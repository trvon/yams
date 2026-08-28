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
    constexpr std::size_t kMaxBatchesPerSession = 4096;
    constexpr std::size_t kMaxWireBytes = std::size_t{64} * 1024 * 1024;
    if (options.maxDeltasPerBatch == 0 || options.maxDeltasPerBatch > kMaxP2pWriterAdvance ||
        options.maxBatches == 0 || options.maxBatches > kMaxBatchesPerSession ||
        options.maxDeltasPerSession == 0 || options.maxDeltasPerSession > kMaxP2pWriterAdvance ||
        options.maxWireBytesPerSession == 0 || options.maxWireBytesPerSession > kMaxWireBytes ||
        options.maxSnapshotRecords == 0 || options.maxSnapshotRecords > kMaxP2pWriterAdvance ||
        options.maxSnapshotWireBytes == 0 || options.maxSnapshotWireBytes > kMaxWireBytes ||
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

struct BootstrapPhaseResult {
    DeltaExchangeStats stats;
    memory_sync::VersionVector peerVersion;
};

Json commitmentArray(
    const std::map<memory_sync::NodeId, memory_sync::WriterHistoryCommitment>& commitments) {
    Json result = Json::array();
    for (const auto& [writer, commitment] : commitments) {
        result.push_back({{"writer_id", writer},
                          {"counter", commitment.counter},
                          {"digest", commitment.digest}});
    }
    return result;
}

Result<std::map<memory_sync::NodeId, memory_sync::WriterHistoryCommitment>>
parseCommitments(const Json& entries) {
    try {
        if (!entries.is_array() || entries.size() > kMaxP2pReplicationWriters) {
            return Error{ErrorCode::ValidationError,
                         "cold bootstrap commitment list violates bounds"};
        }
        std::map<memory_sync::NodeId, memory_sync::WriterHistoryCommitment> result;
        for (const auto& entry : entries) {
            auto writer = entry.at("writer_id").get<std::string>();
            memory_sync::WriterHistoryCommitment commitment{
                .counter = entry.at("counter").get<std::uint64_t>(),
                .digest = entry.at("digest").get<std::string>()};
            if (writer.empty() || writer.size() > kMaxP2pIdentityBytes || commitment.counter == 0 ||
                !memory_sync::isSha256Digest(commitment.digest) ||
                !result.emplace(std::move(writer), std::move(commitment)).second) {
                return Error{ErrorCode::ValidationError, "cold bootstrap commitment is invalid"};
            }
        }
        return result;
    } catch (const std::exception& error) {
        return Error{ErrorCode::ValidationError,
                     std::string("invalid cold bootstrap commitments: ") + error.what()};
    }
}

Result<BootstrapPhaseResult> sendBootstrapPhase(P2pConnection& connection,
                                                memory_sync::MemorySyncService& service,
                                                const PeerHandshakeResult& handshake,
                                                const DeltaExchangeOptions& options) {
    BootstrapPhaseResult result{.peerVersion = handshake.peerVersion};
    const bool hasForeignWriter =
        std::ranges::any_of(handshake.localVersion.counters(), [&](const auto& entry) {
            return entry.first != connection.localNodeId() && entry.second != 0;
        });
    const bool offer = handshake.peerVersion.empty() && hasForeignWriter;
    if (auto written = writeJson(
            connection, Json{{"type", "replication_mode"}, {"mode", offer ? "snapshot" : "delta"}},
            options.timeout);
        !written) {
        return written.error();
    }
    if (!offer) {
        return result;
    }
    if (handshake.firstContactPinned) {
        return Error{ErrorCode::Unauthorized,
                     "cold bootstrap requires a previously operator-pinned peer"};
    }
    const auto current = service.replicationState();
    memory_sync::ReplicationState frozen{.version = handshake.localVersion,
                                         .commitments = current.commitments,
                                         .quarantinedWriters = current.quarantinedWriters};
    auto snapshot = service.exportColdBootstrap(frozen, options.maxSnapshotRecords,
                                                options.maxSnapshotWireBytes);
    if (!snapshot) {
        return snapshot.error();
    }
    std::size_t payloadBytes = 0;
    for (const auto& winner : snapshot.value().winners) {
        if (payloadBytes > std::numeric_limits<std::size_t>::max() - winner.payload.size()) {
            return Error{ErrorCode::ResourceExhausted, "cold bootstrap payload total overflow"};
        }
        payloadBytes += winner.payload.size();
    }
    const Json beginJson{{"type", "snapshot_begin"},
                         {"witness", connection.localNodeId()},
                         {"frontier", snapshot.value().frontier},
                         {"commitments", commitmentArray(snapshot.value().commitments)},
                         {"record_count", snapshot.value().winners.size()},
                         {"payload_bytes", payloadBytes},
                         {"root_digest", snapshot.value().rootDigest},
                         {"witness_key_id", snapshot.value().witnessSignature.keyId},
                         {"witness_algorithm", snapshot.value().witnessSignature.algorithm},
                         {"witness_signature", snapshot.value().witnessSignature.signature}};
    constexpr std::size_t kFramePrefixBytes = 4;
    std::size_t wireBytes = beginJson.dump().size() + kFramePrefixBytes;
    for (const auto& delta : snapshot.value().winners) {
        memory_sync::MemoryDeltaBatch one{.deltas = {delta}};
        auto estimated = estimatedWireBytes(one);
        if (!estimated || wireBytes > options.maxSnapshotWireBytes ||
            estimated.value() > options.maxSnapshotWireBytes - wireBytes) {
            return Error{ErrorCode::ResourceExhausted,
                         "cold bootstrap snapshot exceeds aggregate wire bound"};
        }
        wireBytes += estimated.value();
    }
    if (auto written = writeJson(connection, beginJson, options.timeout); !written) {
        return written.error();
    }
    for (const auto& delta : snapshot.value().winners) {
        memory_sync::MemoryDeltaBatch one{.deltas = {delta}};
        if (auto sent = sendBatch(connection, one, options.timeout); !sent) {
            return sent.error();
        }
    }
    auto ack = readJson(connection, options.timeout);
    if (!ack) {
        return ack.error();
    }
    try {
        if (!ack.value().is_object() ||
            ack.value().value("type", std::string{}) != "snapshot_ack" ||
            ack.value().at("root_digest").get<std::string>() != snapshot.value().rootDigest) {
            return Error{ErrorCode::ValidationError, "cold bootstrap acknowledgement mismatch"};
        }
        const auto acknowledged = ack.value().at("vv").get<memory_sync::VersionVector>();
        if (acknowledged.counters() != snapshot.value().frontier.counters()) {
            return Error{ErrorCode::ValidationError,
                         "cold bootstrap acknowledgement frontier mismatch"};
        }
        result.peerVersion = std::move(acknowledged);
        result.stats.snapshotsSent = 1;
        return result;
    } catch (const std::exception& error) {
        return Error{ErrorCode::ValidationError,
                     std::string("invalid cold bootstrap acknowledgement: ") + error.what()};
    }
}

Result<BootstrapPhaseResult> receiveBootstrapPhase(P2pConnection& connection,
                                                   memory_sync::MemorySyncService& service,
                                                   const PeerHandshakeResult& handshake,
                                                   const DeltaExchangeOptions& options) {
    BootstrapPhaseResult result{.peerVersion = handshake.localVersion};
    auto mode = readJson(connection, options.timeout);
    if (!mode) {
        return mode.error();
    }
    try {
        if (!mode.value().is_object() ||
            mode.value().value("type", std::string{}) != "replication_mode") {
            return Error{ErrorCode::ValidationError, "missing p2p replication mode"};
        }
        const auto selected = mode.value().at("mode").get<std::string>();
        if (selected == "delta") {
            return result;
        }
        if (selected != "snapshot") {
            return Error{ErrorCode::ValidationError, "unknown p2p replication mode"};
        }
    } catch (const std::exception& error) {
        return Error{ErrorCode::ValidationError,
                     std::string("invalid p2p replication mode: ") + error.what()};
    }
    if (!handshake.localVersion.empty() || !service.currentVersion().empty() ||
        handshake.firstContactPinned) {
        return Error{ErrorCode::Unauthorized,
                     "cold bootstrap requires a durable zero state and prior peer pin"};
    }
    auto begin = readJson(connection, options.timeout);
    if (!begin) {
        return begin.error();
    }
    try {
        if (!begin.value().is_object() ||
            begin.value().value("type", std::string{}) != "snapshot_begin" ||
            begin.value().at("witness").get<std::string>() != handshake.peerNodeId) {
            return Error{ErrorCode::Unauthorized, "cold bootstrap witness mismatch"};
        }
        auto frontier = begin.value().at("frontier").get<memory_sync::VersionVector>();
        auto commitments = parseCommitments(begin.value().at("commitments"));
        if (!commitments) {
            return commitments.error();
        }
        const auto count = begin.value().at("record_count").get<std::size_t>();
        const auto declaredPayload = begin.value().at("payload_bytes").get<std::size_t>();
        const auto root = begin.value().at("root_digest").get<std::string>();
        if (frontier.counters() != handshake.peerVersion.counters() ||
            commitments.value() != handshake.peerCommitments ||
            count > options.maxSnapshotRecords || declaredPayload > options.maxSnapshotWireBytes ||
            !memory_sync::isSha256Digest(root)) {
            return Error{ErrorCode::ValidationError,
                         "cold bootstrap manifest differs from authenticated frontier"};
        }
        memory_sync::ColdBootstrapSnapshot snapshot{
            .frontier = std::move(frontier),
            .commitments = std::move(commitments.value()),
            .rootDigest = root,
            .witnessSignature = memory_sync::DetachedWriterSignature{
                .writerId = handshake.peerNodeId,
                .keyId = begin.value().at("witness_key_id").get<std::string>(),
                .algorithm = begin.value().at("witness_algorithm").get<std::string>(),
                .signature = begin.value().at("witness_signature").get<std::string>()}};
        snapshot.winners.reserve(count);
        constexpr std::size_t kFramePrefixBytes = 4;
        std::size_t wireBytes = begin.value().dump().size() + kFramePrefixBytes;
        if (wireBytes > options.maxSnapshotWireBytes) {
            return Error{ErrorCode::ResourceExhausted,
                         "cold bootstrap manifest exceeds aggregate wire bound"};
        }
        std::size_t payloadBytes = 0;
        for (std::size_t index = 0; index < count; ++index) {
            auto received =
                receiveBatch(connection, options, 1, options.maxSnapshotWireBytes - wireBytes);
            if (!received || received.value().batch.deltas.size() != 1 ||
                received.value().batch.hasMore) {
                return Error{ErrorCode::ValidationError,
                             "cold bootstrap record framing is invalid"};
            }
            wireBytes += received.value().wireBytes;
            const auto payloadSize = received.value().batch.deltas.front().payload.size();
            if (payloadBytes > std::numeric_limits<std::size_t>::max() - payloadSize) {
                return Error{ErrorCode::ResourceExhausted, "cold bootstrap payload total overflow"};
            }
            payloadBytes += payloadSize;
            snapshot.winners.push_back(std::move(received.value().batch.deltas.front()));
        }
        if (payloadBytes != declaredPayload) {
            return Error{ErrorCode::ValidationError,
                         "cold bootstrap payload total differs from manifest"};
        }
        auto applied =
            service.applyColdBootstrap(snapshot, handshake.peerNodeId, options.maxSnapshotRecords,
                                       options.maxSnapshotWireBytes);
        if (!applied) {
            return applied.error();
        }
        if (auto written = writeJson(connection,
                                     Json{{"type", "snapshot_ack"},
                                          {"root_digest", root},
                                          {"vv", applied.value().version}},
                                     options.timeout);
            !written) {
            return written.error();
        }
        result.peerVersion = applied.value().version;
        result.stats.snapshotsReceived = 1;
        result.stats.merged = applied.value().merged;
        return result;
    } catch (const std::exception& error) {
        return Error{ErrorCode::ValidationError,
                     std::string("invalid cold bootstrap manifest: ") + error.what()};
    }
}

Result<DeltaExchangeStats> sendAll(P2pConnection& connection,
                                   memory_sync::MemorySyncService& service,
                                   memory_sync::VersionVector peerVersion,
                                   std::uint64_t maxWriterCounter,
                                   const DeltaExchangeOptions& options) {
    DeltaExchangeStats stats;
    std::size_t sessionBytes = 0;
    for (std::size_t batchIndex = 0; batchIndex < options.maxBatches; ++batchIndex) {
        auto batch = service.exportLocalDeltasAfter(peerVersion, options.maxDeltasPerBatch,
                                                    maxWriterCounter);
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
                                      const PeerHandshakeResult& handshake,
                                      const DeltaExchangeOptions& options) {
    DeltaExchangeStats stats;
    std::size_t sessionBytes = 0;
    std::vector<memory_sync::MemoryDelta> staged;
    staged.reserve(options.maxDeltasPerSession);
    auto projectedVersion = service.currentVersion();
    for (std::size_t batchIndex = 0; batchIndex < options.maxBatches; ++batchIndex) {
        auto batch =
            receiveBatch(connection, options, options.maxDeltasPerSession - stats.deltasReceived,
                         options.maxWireBytesPerSession - sessionBytes);
        if (!batch) {
            return batch.error();
        }
        const bool forgedOrigin = std::ranges::any_of(
            batch.value().batch.deltas, [&](const memory_sync::MemoryDelta& delta) {
                return delta.record.origin != handshake.peerNodeId;
            });
        if (forgedOrigin) {
            return Error{ErrorCode::Unauthorized,
                         "p2p delta origin does not match authenticated peer"};
        }
        ++stats.batchesReceived;
        stats.deltasReceived += batch.value().batch.deltas.size();
        sessionBytes += batch.value().wireBytes;
        for (auto& delta : batch.value().batch.deltas) {
            projectedVersion.merge(delta.record.vv);
            staged.push_back(std::move(delta));
        }
        if (batch.value().batch.hasMore) {
            memory_sync::DeltaApplyResult stagedAck;
            stagedAck.received = stats.deltasReceived;
            stagedAck.version = projectedVersion;
            if (auto acknowledged =
                    writeJson(connection, acknowledgementJson(stagedAck), options.timeout);
                !acknowledged) {
                return acknowledged.error();
            }
            continue;
        }
        const auto expected = handshake.peerCommitments.find(handshake.peerNodeId);
        const auto expectedCounter = handshake.peerVersion.get(handshake.peerNodeId);
        if (staged.empty()) {
            const auto local = service.replicationState();
            const auto localCommitment = local.commitments.find(handshake.peerNodeId);
            const bool matches =
                local.version.get(handshake.peerNodeId) == expectedCounter &&
                (expectedCounter == 0 ? expected == handshake.peerCommitments.end() &&
                                            localCommitment == local.commitments.end()
                                      : expected != handshake.peerCommitments.end() &&
                                            expected->second.counter == expectedCounter &&
                                            localCommitment != local.commitments.end() &&
                                            localCommitment->second == expected->second);
            if (!matches) {
                auto quarantined =
                    service.quarantineWriter(handshake.peerNodeId, connection.localNodeId());
                if (!quarantined) {
                    return quarantined.error();
                }
                return Error{ErrorCode::ValidationError,
                             "empty delta window does not match authenticated frontier"};
            }
            memory_sync::DeltaApplyResult emptyAck;
            emptyAck.version = projectedVersion;
            if (auto acknowledged =
                    writeJson(connection, acknowledgementJson(emptyAck), options.timeout);
                !acknowledged) {
                return acknowledged.error();
            }
            return stats;
        }
        if (expected == handshake.peerCommitments.end() ||
            expected->second.counter != expectedCounter) {
            return Error{ErrorCode::ValidationError,
                         "authenticated peer frontier commitment is unavailable"};
        }
        auto validated = service.validateHistoryExtension(staged, expected->second);
        if (!validated) {
            auto quarantined =
                service.quarantineWriter(handshake.peerNodeId, connection.localNodeId());
            if (!quarantined) {
                return quarantined.error();
            }
            return validated.error();
        }
        auto applied = service.applyDeltas(staged);
        if (!applied) {
            return applied.error();
        }
        if (auto acknowledged =
                writeJson(connection, acknowledgementJson(applied.value()), options.timeout);
            !acknowledged) {
            return acknowledged.error();
        }
        stats.merged += applied.value().merged;
        stats.replayed += applied.value().replayed;
        stats.quarantined += applied.value().quarantined.size();
        return stats;
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
    first.snapshotsSent += second.snapshotsSent;
    first.snapshotsReceived += second.snapshotsReceived;
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
    const auto localState = service.replicationState();
    if (!handshake.peerPrefixVerified || !handshake.peerPrefixMatches ||
        localState.quarantinedWriters.contains(handshake.peerNodeId)) {
        return fail(Error{ErrorCode::InvalidData,
                          "p2p delta exchange rejected unverified or quarantined peer history"});
    }
    auto quarantine = requiresPeerWriterQuarantine(localState, handshake);
    if (!quarantine) {
        return fail(quarantine.error());
    }
    if (quarantine.value()) {
        return fail(
            Error{ErrorCode::InvalidData, "p2p delta exchange rejected forked peer history"});
    }
    const auto localWriterCounter = handshake.localVersion.get(connection.localNodeId());
    DeltaExchangeStats total;
    if (role == ExchangeRole::Initiator) {
        auto bootstrapSent = sendBootstrapPhase(connection, service, handshake, options);
        if (!bootstrapSent) {
            return fail(bootstrapSent.error());
        }
        total = combine(total, bootstrapSent.value().stats);
        auto sent = sendAll(connection, service, bootstrapSent.value().peerVersion,
                            localWriterCounter, options);
        if (!sent) {
            return fail(sent.error());
        }
        total = combine(total, sent.value());
        auto bootstrapReceived = receiveBootstrapPhase(connection, service, handshake, options);
        if (!bootstrapReceived) {
            return fail(bootstrapReceived.error());
        }
        total = combine(total, bootstrapReceived.value().stats);
        auto received = receiveAll(connection, service, handshake, options);
        if (!received) {
            return fail(received.error());
        }
        return combine(total, received.value());
    }

    auto bootstrapReceived = receiveBootstrapPhase(connection, service, handshake, options);
    if (!bootstrapReceived) {
        return fail(bootstrapReceived.error());
    }
    total = combine(total, bootstrapReceived.value().stats);
    auto received = receiveAll(connection, service, handshake, options);
    if (!received) {
        return fail(received.error());
    }
    total = combine(total, received.value());
    auto bootstrapSent = sendBootstrapPhase(connection, service, handshake, options);
    if (!bootstrapSent) {
        return fail(bootstrapSent.error());
    }
    total = combine(total, bootstrapSent.value().stats);
    auto sent = sendAll(connection, service, bootstrapSent.value().peerVersion, localWriterCounter,
                        options);
    if (!sent) {
        return fail(sent.error());
    }
    return combine(total, sent.value());
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
