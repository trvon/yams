// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// pi-lens-ignore: fatal error
#include <yams/daemon/p2p/p2p_delta.h>

#include "p2p_json.h"

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
        options.timeout <= std::chrono::milliseconds::zero()) {
        return Error{ErrorCode::InvalidArgument, "invalid p2p delta exchange limits"};
    }
    return {};
}

Json batchJson(const memory_sync::MemoryDeltaBatch& batch) {
    return {{"type", "delta_batch"}, {"count", batch.deltas.size()}, {"has_more", batch.hasMore}};
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

Result<memory_sync::MemoryDelta> receiveDeltaRecord(P2pConnection& connection,
                                                    const DeltaExchangeOptions& options) {
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
    if (payloadSize == 0) {
        return delta;
    }
    auto payload = connection.readFrame(options.timeout, kP2pMaxValuePayloadBytes);
    if (!payload) {
        return payload.error();
    }
    if (payload.value().size() != payloadSize) {
        return Error{ErrorCode::ValidationError,
                     "p2p delta payload length does not match its record"};
    }
    delta.payload = std::move(payload.value());
    return delta;
}

Result<memory_sync::MemoryDeltaBatch> receiveBatch(P2pConnection& connection,
                                                   const DeltaExchangeOptions& options) {
    auto control = readJson(connection, options.timeout);
    if (!control) {
        return control.error();
    }
    auto header = parseBatchHeader(control.value(), options);
    if (!header) {
        return header.error();
    }
    memory_sync::MemoryDeltaBatch batch;
    batch.hasMore = header.value().hasMore;
    batch.deltas.reserve(header.value().count);
    for (std::size_t index = 0; index < header.value().count; ++index) {
        auto delta = receiveDeltaRecord(connection, options);
        if (!delta) {
            return delta.error();
        }
        batch.deltas.push_back(std::move(delta.value()));
    }
    return batch;
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
    for (std::size_t batchIndex = 0; batchIndex < options.maxBatches; ++batchIndex) {
        auto batch = service.exportLocalDeltasAfter(peerVersion, options.maxDeltasPerBatch);
        if (!batch) {
            return batch.error();
        }
        if (auto sent = sendBatch(connection, batch.value(), options.timeout); !sent) {
            return sent.error();
        }
        ++stats.batchesSent;
        stats.deltasSent += batch.value().deltas.size();
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
    for (std::size_t batchIndex = 0; batchIndex < options.maxBatches; ++batchIndex) {
        auto batch = receiveBatch(connection, options);
        if (!batch) {
            return batch.error();
        }
        const bool forgedOrigin =
            std::ranges::any_of(batch.value().deltas, [&](const memory_sync::MemoryDelta& delta) {
                return delta.record.origin != expectedPeerNodeId;
            });
        if (forgedOrigin) {
            return Error{ErrorCode::Unauthorized,
                         "p2p delta origin does not match authenticated peer"};
        }
        auto applied = service.applyDeltas(batch.value().deltas);
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
        stats.merged += applied.value().merged;
        stats.replayed += applied.value().replayed;
        stats.quarantined += applied.value().quarantined.size();
        if (!batch.value().hasMore) {
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
