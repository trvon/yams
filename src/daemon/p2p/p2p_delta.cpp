// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// pi-lens-ignore: fatal error
#include <yams/daemon/p2p/p2p_delta.h>

#include "p2p_json.h"

#include <limits>
#include <string_view>
#include <utility>

namespace yams::daemon::p2p {

namespace {

using detail::Json;
using detail::readJson;
using detail::writeJson;

struct DeltaAcknowledgement {
    memory_sync::VersionVector version;
    std::size_t quarantined{0};
};

Result<void> validateOptions(const DeltaExchangeOptions& options) {
    if (options.maxDeltasPerBatch == 0 || options.maxBatches == 0 ||
        options.maxDeltasPerSession == 0 || options.maxWireBytesPerSession == 0 ||
        options.timeout <= std::chrono::milliseconds::zero()) {
        return Error{ErrorCode::InvalidArgument, "invalid p2p delta exchange limits"};
    }
    return {};
}

Json batchJson(const memory_sync::MemoryDeltaBatch& batch) {
    return {{"type", "delta_batch"}, {"count", batch.deltas.size()}, {"has_more", batch.hasMore}};
}

Result<std::size_t> estimatedWireBytes(const memory_sync::MemoryDeltaBatch& batch) {
    constexpr std::size_t kFramePrefixBytes = 4;
    std::size_t total = batchJson(batch).dump().size() + kFramePrefixBytes;
    for (const auto& delta : batch.deltas) {
        Json recordJson;
        try {
            recordJson = {{"type", "delta_record"},
                          {"logical_key", delta.logicalKey},
                          {"record", delta.record},
                          {"payload_size", delta.payload.size()}};
        } catch (const std::exception& error) {
            return Error{ErrorCode::SerializationError, error.what()};
        }
        const auto controlBytes = recordJson.dump().size() + kFramePrefixBytes;
        const auto payloadBytes =
            delta.payload.empty() ? 0 : delta.payload.size() + kFramePrefixBytes;
        if (controlBytes > std::numeric_limits<std::size_t>::max() - total ||
            payloadBytes > std::numeric_limits<std::size_t>::max() - total - controlBytes) {
            return Error{ErrorCode::ResourceExhausted, "p2p delta wire size overflow"};
        }
        total += controlBytes + payloadBytes;
    }
    return total;
}

Result<void> admitBatch(const memory_sync::MemoryDeltaBatch& batch,
                        const DeltaExchangeOptions& options, std::size_t acceptedDeltas,
                        std::size_t acceptedBytes, std::size_t& batchBytes) {
    if (acceptedDeltas > options.maxDeltasPerSession ||
        batch.deltas.size() > options.maxDeltasPerSession - acceptedDeltas) {
        return Error{ErrorCode::ResourceExhausted,
                     "p2p delta exchange exceeded aggregate delta limit"};
    }
    auto estimated = estimatedWireBytes(batch);
    if (!estimated) {
        return estimated.error();
    }
    batchBytes = estimated.value();
    if (acceptedBytes > options.maxWireBytesPerSession ||
        batchBytes > options.maxWireBytesPerSession - acceptedBytes) {
        return Error{ErrorCode::ResourceExhausted,
                     "p2p delta exchange exceeded aggregate byte limit"};
    }
    return {};
}

Result<void> sendBatch(P2pConnection& connection, const memory_sync::MemoryDeltaBatch& batch,
                       std::chrono::milliseconds timeout) {
    if (auto written = writeJson(connection, batchJson(batch), timeout); !written) {
        return written.error();
    }
    for (const auto& delta : batch.deltas) {
        Json recordJson;
        try {
            recordJson = {{"type", "delta_record"},
                          {"logical_key", delta.logicalKey},
                          {"record", delta.record},
                          {"payload_size", delta.payload.size()}};
        } catch (const std::exception& error) {
            return Error{ErrorCode::SerializationError, error.what()};
        }
        if (auto written = writeJson(connection, recordJson, timeout); !written) {
            return written.error();
        }
        if (!delta.payload.empty()) {
            if (auto written =
                    connection.writeFrame(delta.payload, timeout, kP2pMaxValuePayloadBytes);
                !written) {
                return written.error();
            }
        }
    }
    return {};
}

struct BatchHeader {
    std::size_t count{0};
    bool hasMore{false};
};

Result<BatchHeader> parseBatchHeader(const Json& control, const DeltaExchangeOptions& options) {
    try {
        if (!control.is_object() || control.value("type", std::string{}) != "delta_batch") {
            return Error{ErrorCode::ValidationError, "unexpected p2p delta batch message"};
        }
        BatchHeader header{.count = control.at("count").get<std::size_t>(),
                           .hasMore = control.at("has_more").get<bool>()};
        if (header.count > options.maxDeltasPerBatch || (header.hasMore && header.count == 0)) {
            return Error{ErrorCode::ValidationError, "p2p delta batch violates configured limits"};
        }
        return header;
    } catch (const std::exception& error) {
        return Error{ErrorCode::ValidationError,
                     std::string("invalid p2p delta batch: ") + error.what()};
    }
}

struct ReceivedDelta {
    memory_sync::MemoryDelta delta;
    std::size_t wireBytes{0};
};

Result<ReceivedDelta> receiveDeltaRecord(P2pConnection& connection,
                                         const DeltaExchangeOptions& options,
                                         std::size_t remainingBytes) {
    constexpr std::size_t kFramePrefixBytes = 4;
    auto control = readJson(connection, options.timeout);
    if (!control) {
        return control.error();
    }
    memory_sync::MemoryDelta delta;
    std::size_t payloadSize = 0;
    try {
        if (!control.value().is_object() ||
            control.value().value("type", std::string{}) != "delta_record") {
            return Error{ErrorCode::ValidationError, "unexpected p2p delta record message"};
        }
        delta.logicalKey = control.value().at("logical_key").get<std::string>();
        delta.record = control.value().at("record").get<memory_sync::MemoryIndexRecord>();
        payloadSize = control.value().at("payload_size").get<std::size_t>();
    } catch (const std::exception& error) {
        return Error{ErrorCode::ValidationError,
                     std::string("invalid p2p delta record: ") + error.what()};
    }
    if (payloadSize > kP2pMaxValuePayloadBytes ||
        (delta.record.isTombstone() && payloadSize != 0)) {
        return Error{ErrorCode::ValidationError, "p2p delta payload size is invalid"};
    }
    const auto controlBytes = control.value().dump().size() + kFramePrefixBytes;
    const auto payloadBytes = payloadSize == 0 ? 0 : payloadSize + kFramePrefixBytes;
    if (controlBytes > remainingBytes || payloadBytes > remainingBytes - controlBytes) {
        return Error{ErrorCode::ResourceExhausted,
                     "p2p delta exchange exceeded aggregate byte limit"};
    }
    if (payloadSize != 0) {
        auto payload = connection.readFrame(options.timeout, kP2pMaxValuePayloadBytes);
        if (!payload) {
            return payload.error();
        }
        if (payload.value().size() != payloadSize) {
            return Error{ErrorCode::ValidationError,
                         "p2p delta payload length does not match its record"};
        }
        delta.payload = std::move(payload.value());
    }
    return ReceivedDelta{.delta = std::move(delta), .wireBytes = controlBytes + payloadBytes};
}

struct ReceivedBatch {
    memory_sync::MemoryDeltaBatch batch;
    std::size_t wireBytes{0};
};

Result<ReceivedBatch> receiveBatch(P2pConnection& connection, const DeltaExchangeOptions& options,
                                   std::size_t remainingDeltas, std::size_t remainingBytes) {
    constexpr std::size_t kFramePrefixBytes = 4;
    auto control = readJson(connection, options.timeout);
    if (!control) {
        return control.error();
    }
    auto header = parseBatchHeader(control.value(), options);
    if (!header) {
        return header.error();
    }
    const auto headerBytes = control.value().dump().size() + kFramePrefixBytes;
    if (header.value().count > remainingDeltas) {
        return Error{ErrorCode::ResourceExhausted,
                     "p2p delta exchange exceeded aggregate delta limit"};
    }
    if (headerBytes > remainingBytes) {
        return Error{ErrorCode::ResourceExhausted,
                     "p2p delta exchange exceeded aggregate byte limit"};
    }
    ReceivedBatch received;
    received.batch.hasMore = header.value().hasMore;
    received.batch.deltas.reserve(header.value().count);
    received.wireBytes = headerBytes;
    for (std::size_t index = 0; index < header.value().count; ++index) {
        auto delta = receiveDeltaRecord(connection, options, remainingBytes - received.wireBytes);
        if (!delta) {
            return delta.error();
        }
        received.wireBytes += delta.value().wireBytes;
        received.batch.deltas.push_back(std::move(delta.value().delta));
    }
    return received;
}

Json acknowledgementJson(const memory_sync::DeltaApplyResult& result) {
    return {{"type", "delta_ack"},
            {"received", result.received},
            {"merged", result.merged},
            {"replayed", result.replayed},
            {"quarantined", result.quarantined},
            {"vv", result.version}};
}

Result<DeltaAcknowledgement> receiveAcknowledgement(P2pConnection& connection,
                                                    std::chrono::milliseconds timeout) {
    auto json = readJson(connection, timeout);
    if (!json) {
        return json.error();
    }
    try {
        if (!json.value().is_object() || json.value().value("type", std::string{}) != "delta_ack") {
            return Error{ErrorCode::ValidationError, "unexpected p2p delta acknowledgement"};
        }
        const auto quarantined =
            json.value().at("quarantined").get<std::map<std::string, std::string>>();
        return DeltaAcknowledgement{.version =
                                        json.value().at("vv").get<memory_sync::VersionVector>(),
                                    .quarantined = quarantined.size()};
    } catch (const std::exception& error) {
        return Error{ErrorCode::ValidationError,
                     std::string("invalid p2p delta acknowledgement: ") + error.what()};
    }
}

Result<DeltaExchangeStats> sendAll(P2pConnection& connection,
                                   memory_sync::MemorySyncService& service,
                                   memory_sync::VersionVector peerVersion,
                                   const DeltaExchangeOptions& options) {
    DeltaExchangeStats stats;
    std::size_t sessionBytes = 0;
    for (std::size_t batchIndex = 0; batchIndex < options.maxBatches; ++batchIndex) {
        auto batch = service.exportLocalDeltasAfter(peerVersion, options.maxDeltasPerBatch);
        if (!batch) {
            return batch.error();
        }
        std::size_t batchBytes = 0;
        if (auto admitted =
                admitBatch(batch.value(), options, stats.deltasSent, sessionBytes, batchBytes);
            !admitted) {
            return admitted.error();
        }
        if (auto sent = sendBatch(connection, batch.value(), options.timeout); !sent) {
            return sent.error();
        }
        ++stats.batchesSent;
        stats.deltasSent += batch.value().deltas.size();
        sessionBytes += batchBytes;
        auto acknowledgement = receiveAcknowledgement(connection, options.timeout);
        if (!acknowledgement) {
            return acknowledgement.error();
        }
        stats.peerQuarantined += acknowledgement.value().quarantined;
        const auto before = peerVersion.get(connection.localNodeId());
        const auto after = acknowledgement.value().version.get(connection.localNodeId());
        if (!batch.value().deltas.empty() && after <= before) {
            return Error{ErrorCode::ValidationError,
                         "p2p peer made no causal progress applying direct deltas"};
        }
        peerVersion = std::move(acknowledgement.value().version);
        if (!batch.value().hasMore) {
            return stats;
        }
    }
    return Error{ErrorCode::ResourceExhausted, "p2p delta exchange exceeded batch limit"};
}

Result<DeltaExchangeStats> receiveAll(P2pConnection& connection,
                                      memory_sync::MemorySyncService& service,
                                      std::string_view expectedPeerNodeId,
                                      const DeltaExchangeOptions& options) {
    DeltaExchangeStats stats;
    std::size_t sessionBytes = 0;
    for (std::size_t batchIndex = 0; batchIndex < options.maxBatches; ++batchIndex) {
        auto batch =
            receiveBatch(connection, options, options.maxDeltasPerSession - stats.deltasReceived,
                         options.maxWireBytesPerSession - sessionBytes);
        if (!batch) {
            return batch.error();
        }
        const bool forgedOrigin = std::ranges::any_of(
            batch.value().batch.deltas, [&](const memory_sync::MemoryDelta& delta) {
                return delta.record.origin != expectedPeerNodeId;
            });
        if (forgedOrigin) {
            return Error{ErrorCode::Unauthorized,
                         "p2p delta origin does not match authenticated peer"};
        }
        auto applied = service.applyDeltas(batch.value().batch.deltas);
        if (!applied) {
            return applied.error();
        }
        if (auto acknowledged =
                writeJson(connection, acknowledgementJson(applied.value()), options.timeout);
            !acknowledged) {
            return acknowledged.error();
        }
        ++stats.batchesReceived;
        stats.deltasReceived += applied.value().received;
        sessionBytes += batch.value().wireBytes;
        stats.merged += applied.value().merged;
        stats.replayed += applied.value().replayed;
        stats.quarantined += applied.value().quarantined.size();
        if (!batch.value().batch.hasMore) {
            return stats;
        }
    }
    return Error{ErrorCode::ResourceExhausted, "p2p delta exchange exceeded batch limit"};
}

DeltaExchangeStats combine(DeltaExchangeStats first, const DeltaExchangeStats& second) {
    first.batchesSent += second.batchesSent;
    first.batchesReceived += second.batchesReceived;
    first.deltasSent += second.deltasSent;
    first.deltasReceived += second.deltasReceived;
    first.merged += second.merged;
    first.replayed += second.replayed;
    first.quarantined += second.quarantined;
    first.peerQuarantined += second.peerQuarantined;
    return first;
}

enum class ExchangeRole : std::uint8_t { Initiator, Acceptor };

Result<DeltaExchangeStats> runDeltaExchange(P2pConnection& connection,
                                            memory_sync::MemorySyncService& service,
                                            const PeerHandshakeResult& handshake,
                                            const DeltaExchangeOptions& options,
                                            ExchangeRole role) {
    const auto fail = [&](Error error) -> Result<DeltaExchangeStats> {
        connection.close();
        return error;
    };
    if (auto valid = validateOptions(options); !valid) {
        return fail(valid.error());
    }
    auto first = role == ExchangeRole::Initiator
                     ? sendAll(connection, service, handshake.peerVersion, options)
                     : receiveAll(connection, service, handshake.peerNodeId, options);
    if (!first) {
        return fail(first.error());
    }
    auto second = role == ExchangeRole::Initiator
                      ? receiveAll(connection, service, handshake.peerNodeId, options)
                      : sendAll(connection, service, handshake.peerVersion, options);
    if (!second) {
        return fail(second.error());
    }
    return combine(first.value(), second.value());
}

} // namespace

Result<DeltaExchangeStats> initiateDeltaExchange(P2pConnection& connection,
                                                 memory_sync::MemorySyncService& service,
                                                 const PeerHandshakeResult& handshake,
                                                 const DeltaExchangeOptions& options) {
    return runDeltaExchange(connection, service, handshake, options, ExchangeRole::Initiator);
}

Result<DeltaExchangeStats> acceptDeltaExchange(P2pConnection& connection,
                                               memory_sync::MemorySyncService& service,
                                               const PeerHandshakeResult& handshake,
                                               const DeltaExchangeOptions& options) {
    return runDeltaExchange(connection, service, handshake, options, ExchangeRole::Acceptor);
}

} // namespace yams::daemon::p2p
