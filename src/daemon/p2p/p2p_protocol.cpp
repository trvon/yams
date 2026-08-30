// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// pi-lens-ignore: fatal error
#include <yams/daemon/p2p/p2p_protocol.h>

#include "p2p_json.h"

#include <algorithm>
#include <cctype>
#include <utility>

namespace yams::daemon::p2p {

namespace {

using detail::Json;
using detail::readJson;
using detail::writeJson;

struct WireHello {
    std::uint32_t protocol{0};
    std::uint32_t schemaVersion{0};
    std::string nodeId;
    std::string corpusId;
    std::uint64_t corpusEpoch{0};
    std::size_t maxWriterAdvance{0};
    std::size_t maxWriterWindowBytes{0};
};

struct WireState {
    memory_sync::VersionVector version;
    std::map<memory_sync::NodeId, std::uint64_t> seen;
    std::map<memory_sync::NodeId, memory_sync::WriterHistoryCommitment> commitments;
    std::set<memory_sync::NodeId> quarantinedWriters;
};

struct WireWindowFrontier {
    std::string writerId;
    std::uint64_t counter{0};
    std::string digest;
};

struct WireHistoryProof {
    std::string writerId;
    std::uint64_t counter{0};
    std::string digest;
};

struct ValidatedHistoryProof {
    WireHistoryProof proof;
    bool matchesLocalPrefix{false};
};

Result<void> validateConfig(const PeerHandshakeConfig& config) {
    if (config.nodeId.empty() || config.nodeId == "default" || config.corpusId.empty() ||
        config.nodeId.size() > kMaxP2pIdentityBytes ||
        config.corpusId.size() > kMaxP2pIdentityBytes || config.corpusEpoch == 0) {
        return Error{ErrorCode::InvalidArgument,
                     "p2p handshake requires node, corpus, and epoch identity"};
    }
    if (config.timeout <= std::chrono::milliseconds::zero() || config.maxWriterAdvance == 0 ||
        config.maxWriterAdvance > kMaxP2pWriterAdvance || config.maxWriterWindowBytes == 0 ||
        config.maxWriterWindowBytes > kMaxP2pWriterWindowBytes) {
        return Error{ErrorCode::InvalidArgument,
                     "p2p handshake timeout or writer-window bound is invalid"};
    }
    if (config.localVersion.get(config.nodeId) != 0 && !config.resolveLocalWindow) {
        return Error{ErrorCode::InvalidArgument,
                     "p2p local writer requires a bounded window resolver"};
    }
    if (config.localVersion.counters().size() > kMaxP2pReplicationWriters ||
        config.localCommitments.size() > kMaxP2pReplicationWriters ||
        config.localQuarantinedWriters.size() > kMaxP2pReplicationWriters) {
        return Error{ErrorCode::ResourceExhausted, "p2p replication state exceeds writer limit"};
    }
    for (const auto& [writer, commitment] : config.localCommitments) {
        if (writer.empty() || writer.size() > kMaxP2pIdentityBytes || commitment.counter == 0 ||
            commitment.counter != config.localVersion.get(writer) ||
            !memory_sync::isSha256Digest(commitment.digest)) {
            return Error{ErrorCode::InvalidArgument, "p2p local history commitment is invalid"};
        }
    }
    return {};
}

Json helloJson(const PeerHandshakeConfig& config) {
    return {{"type", "hello"},
            {"protocol", kP2pProtocolVersion},
            {"schema_version", kP2pEnvelopeSchemaVersion},
            {"node_id", config.nodeId},
            {"corpus_id", config.corpusId},
            {"corpus_epoch", config.corpusEpoch},
            {"max_writer_advance", config.maxWriterAdvance},
            {"max_writer_window_bytes", config.maxWriterWindowBytes}};
}

Result<WireHello> parseHello(const Json& json, std::string_view expectedType) {
    try {
        if (!json.is_object() || json.value("type", std::string{}) != expectedType) {
            return Error{ErrorCode::ValidationError, "unexpected p2p handshake message type"};
        }
        return WireHello{.protocol = json.at("protocol").get<std::uint32_t>(),
                         .schemaVersion = json.at("schema_version").get<std::uint32_t>(),
                         .nodeId = json.at("node_id").get<std::string>(),
                         .corpusId = json.at("corpus_id").get<std::string>(),
                         .corpusEpoch = json.at("corpus_epoch").get<std::uint64_t>(),
                         .maxWriterAdvance = json.at("max_writer_advance").get<std::size_t>(),
                         .maxWriterWindowBytes =
                             json.at("max_writer_window_bytes").get<std::size_t>()};
    } catch (const std::exception& error) {
        return Error{ErrorCode::ValidationError, std::string("invalid p2p hello: ") + error.what()};
    }
}

Result<void> validatePeerHello(const WireHello& peer, const PeerHandshakeConfig& local,
                               const P2pConnection& connection) {
    if (peer.protocol != kP2pProtocolVersion) {
        return Error{ErrorCode::NotSupported, "p2p protocol version mismatch"};
    }
    if (peer.schemaVersion != kP2pEnvelopeSchemaVersion) {
        return Error{ErrorCode::NotSupported, "p2p envelope schema version mismatch"};
    }
    if (peer.nodeId.empty() || peer.nodeId.size() > kMaxP2pIdentityBytes ||
        peer.corpusId.size() > kMaxP2pIdentityBytes || peer.nodeId != connection.peerCertCn() ||
        peer.maxWriterAdvance == 0 || peer.maxWriterAdvance > kMaxP2pWriterAdvance ||
        peer.maxWriterWindowBytes == 0 || peer.maxWriterWindowBytes > kMaxP2pWriterWindowBytes) {
        return Error{ErrorCode::Unauthorized, "p2p claimed node id does not match TLS certificate"};
    }
    if (peer.corpusId != local.corpusId) {
        return Error{ErrorCode::ValidationError, "p2p corpus id mismatch"};
    }
    if (peer.corpusEpoch != local.corpusEpoch) {
        return Error{ErrorCode::ValidationError, "p2p corpus epoch mismatch"};
    }
    return {};
}

Json acknowledgementJson(const PeerHandshakeConfig& config, bool accepted,
                         std::string_view reason = {}) {
    Json json{{"type", "hello_ack"},
              {"accepted", accepted},
              {"protocol", kP2pProtocolVersion},
              {"schema_version", kP2pEnvelopeSchemaVersion},
              {"node_id", config.nodeId},
              {"corpus_id", config.corpusId},
              {"corpus_epoch", config.corpusEpoch},
              {"max_writer_advance", config.maxWriterAdvance},
              {"max_writer_window_bytes", config.maxWriterWindowBytes}};
    if (!reason.empty()) {
        json["reason"] = reason;
    }
    return json;
}

Json stateJson(const PeerHandshakeConfig& config) {
    Json commitments = Json::array();
    for (const auto& [writer, commitment] : config.localCommitments) {
        commitments.push_back({{"writer_id", writer},
                               {"counter", commitment.counter},
                               {"digest", commitment.digest}});
    }
    return {{"type", "state"},
            {"vv", config.localVersion},
            {"seen", config.localVersion.counters()},
            {"commitments", std::move(commitments)},
            {"quarantined_writers", config.localQuarantinedWriters}};
}

Result<WireState> parseState(const Json& json) {
    try {
        if (!json.is_object() || json.value("type", std::string{}) != "state") {
            return Error{ErrorCode::ValidationError, "unexpected p2p state message type"};
        }
        WireState state{.version = json.at("vv").get<memory_sync::VersionVector>(),
                        .seen =
                            json.at("seen").get<std::map<memory_sync::NodeId, std::uint64_t>>()};
        if (state.seen != state.version.counters()) {
            return Error{ErrorCode::ValidationError,
                         "p2p state watermark does not match version vector"};
        }
        if (state.seen.size() > kMaxP2pReplicationWriters ||
            std::ranges::any_of(state.seen, [](const auto& entry) {
                return entry.first.empty() || entry.first.size() > kMaxP2pIdentityBytes;
            })) {
            return Error{ErrorCode::ValidationError, "p2p version vector violates writer bounds"};
        }
        const auto& commitments = json.at("commitments");
        const auto& quarantined = json.at("quarantined_writers");
        if (!commitments.is_array() || !quarantined.is_array() ||
            commitments.size() > kMaxP2pReplicationWriters ||
            quarantined.size() > kMaxP2pReplicationWriters) {
            return Error{ErrorCode::ValidationError,
                         "p2p replication state violates writer bounds"};
        }
        for (const auto& entry : commitments) {
            const auto writer = entry.at("writer_id").get<std::string>();
            memory_sync::WriterHistoryCommitment commitment{
                .counter = entry.at("counter").get<std::uint64_t>(),
                .digest = entry.at("digest").get<std::string>()};
            if (writer.empty() || writer.size() > kMaxP2pIdentityBytes || commitment.counter == 0 ||
                commitment.counter != state.version.get(writer) ||
                !memory_sync::isSha256Digest(commitment.digest) ||
                !state.commitments.emplace(writer, std::move(commitment)).second) {
                return Error{ErrorCode::ValidationError, "p2p history commitment is invalid"};
            }
        }
        for (const auto& entry : quarantined) {
            const auto writer = entry.get<std::string>();
            if (writer.empty() || writer.size() > kMaxP2pIdentityBytes ||
                !state.quarantinedWriters.insert(writer).second) {
                return Error{ErrorCode::ValidationError, "p2p writer quarantine set is invalid"};
            }
        }
        return state;
    } catch (const std::exception& error) {
        return Error{ErrorCode::ValidationError, std::string("invalid p2p state: ") + error.what()};
    }
}

Json windowFrontierJson(const WireWindowFrontier& frontier) {
    return {{"type", "writer_window"},
            {"writer_id", frontier.writerId},
            {"counter", frontier.counter},
            {"digest", frontier.digest}};
}

Result<WireWindowFrontier> parseWindowFrontier(const Json& json) {
    try {
        if (!json.is_object() || json.value("type", std::string{}) != "writer_window") {
            return Error{ErrorCode::ValidationError, "unexpected p2p writer-window message type"};
        }
        WireWindowFrontier frontier{.writerId = json.at("writer_id").get<std::string>(),
                                    .counter = json.at("counter").get<std::uint64_t>(),
                                    .digest = json.at("digest").get<std::string>()};
        if (frontier.writerId.empty() || frontier.writerId.size() > kMaxP2pIdentityBytes ||
            (frontier.counter == 0 ? !frontier.digest.empty()
                                   : !memory_sync::isSha256Digest(frontier.digest))) {
            return Error{ErrorCode::ValidationError, "p2p writer-window frontier is invalid"};
        }
        return frontier;
    } catch (const std::exception& error) {
        return Error{ErrorCode::ValidationError,
                     std::string("invalid p2p writer-window frontier: ") + error.what()};
    }
}

bool requiresFullBootstrapWindow(const WireState& source, const WireState& receiver,
                                 std::string_view sourceNodeId) {
    return receiver.version.empty() &&
           std::ranges::any_of(source.version.counters(), [&](const auto& entry) {
               return entry.first != sourceNodeId && entry.second != 0;
           });
}

std::uint64_t boundedWriterCounter(std::uint64_t receiverCounter, std::uint64_t sourceCounter,
                                   std::size_t maxAdvance, bool fullBootstrap) {
    if (fullBootstrap || sourceCounter <= receiverCounter) {
        return sourceCounter;
    }
    const auto remaining = sourceCounter - receiverCounter;
    return remaining <= maxAdvance ? sourceCounter
                                   : receiverCounter + static_cast<std::uint64_t>(maxAdvance);
}

Result<WireWindowFrontier> makeLocalWindowFrontier(const PeerHandshakeConfig& config,
                                                   const WireState& peerState,
                                                   const WireHello& peerHello) {
    const auto localCounter = config.localVersion.get(config.nodeId);
    const auto peerCounter = peerState.version.get(config.nodeId);
    if (peerCounter > localCounter) {
        return Error{ErrorCode::ValidationError,
                     "peer writer watermark is ahead of authenticated local history"};
    }
    WireState localState{.version = config.localVersion,
                         .seen = config.localVersion.counters(),
                         .commitments = config.localCommitments,
                         .quarantinedWriters = config.localQuarantinedWriters};
    const bool fullBootstrap = requiresFullBootstrapWindow(localState, peerState, config.nodeId);
    const auto negotiatedRecords = std::min(config.maxWriterAdvance, peerHello.maxWriterAdvance);
    const auto negotiatedBytes =
        std::min(config.maxWriterWindowBytes, peerHello.maxWriterWindowBytes);
    auto counter =
        boundedWriterCounter(peerCounter, localCounter, negotiatedRecords, fullBootstrap);
    if (!fullBootstrap && counter > peerCounter) {
        auto selected = config.resolveLocalWindow(peerCounter, negotiatedRecords, negotiatedBytes);
        if (!selected) {
            return selected.error();
        }
        if (selected.value().counter <= peerCounter || selected.value().counter > counter ||
            !memory_sync::isSha256Digest(selected.value().digest)) {
            return Error{ErrorCode::InvalidState,
                         "local writer-window resolver violated negotiated bounds"};
        }
        counter = selected.value().counter;
    }
    if (counter == 0) {
        return WireWindowFrontier{.writerId = config.nodeId};
    }
    memory_sync::WriterHistoryCommitment commitment;
    if (counter == localCounter) {
        const auto current = config.localCommitments.find(config.nodeId);
        if (current == config.localCommitments.end()) {
            return Error{ErrorCode::InvalidState,
                         "local writer commitment is unavailable for bounded handshake"};
        }
        commitment = current->second;
    } else {
        if (!config.resolveLocalCommitment) {
            return Error{ErrorCode::InvalidState,
                         "historical writer commitment resolver is unavailable"};
        }
        auto resolved = config.resolveLocalCommitment(counter);
        if (!resolved) {
            return resolved.error();
        }
        commitment = std::move(resolved.value());
    }
    if (commitment.counter != counter || !memory_sync::isSha256Digest(commitment.digest)) {
        return Error{ErrorCode::InvalidState, "bounded local writer commitment is inconsistent"};
    }
    return WireWindowFrontier{
        .writerId = config.nodeId, .counter = counter, .digest = std::move(commitment.digest)};
}

WireState replaceWriterFrontier(WireState state, const WireWindowFrontier& frontier) {
    memory_sync::VersionVector version;
    for (const auto& [writer, counter] : state.version.counters()) {
        if (writer != frontier.writerId) {
            version.observe(writer, counter);
        }
    }
    if (frontier.counter != 0) {
        version.observe(frontier.writerId, frontier.counter);
        state.commitments[frontier.writerId] = memory_sync::WriterHistoryCommitment{
            .counter = frontier.counter, .digest = frontier.digest};
    } else {
        state.commitments.erase(frontier.writerId);
    }
    state.version = std::move(version);
    state.seen = state.version.counters();
    return state;
}

Result<WireState> validatePeerWindowFrontier(const PeerHandshakeConfig& config,
                                             const WireState& peerState, const WireHello& peerHello,
                                             const WireWindowFrontier& frontier) {
    if (frontier.writerId != peerHello.nodeId) {
        return Error{ErrorCode::Unauthorized, "p2p writer-window identity differs from TLS peer"};
    }
    const auto sourceCounter = peerState.version.get(peerHello.nodeId);
    const auto receiverCounter = config.localVersion.get(peerHello.nodeId);
    if (receiverCounter > sourceCounter) {
        return Error{ErrorCode::ValidationError,
                     "peer writer frontier rolls back local durable history"};
    }
    WireState receiver{.version = config.localVersion,
                       .seen = config.localVersion.counters(),
                       .commitments = config.localCommitments,
                       .quarantinedWriters = config.localQuarantinedWriters};
    const bool fullBootstrap = requiresFullBootstrapWindow(peerState, receiver, peerHello.nodeId);
    const auto negotiatedRecords = std::min(config.maxWriterAdvance, peerHello.maxWriterAdvance);
    const auto maximumCounter =
        boundedWriterCounter(receiverCounter, sourceCounter, negotiatedRecords, fullBootstrap);
    const bool exactRequired = fullBootstrap || sourceCounter <= receiverCounter;
    if ((exactRequired && frontier.counter != maximumCounter) ||
        (!exactRequired &&
         (frontier.counter <= receiverCounter || frontier.counter > maximumCounter))) {
        return Error{ErrorCode::ValidationError,
                     "peer writer-window counter violates the negotiated bound"};
    }
    if (frontier.counter == sourceCounter && sourceCounter != 0) {
        const auto full = peerState.commitments.find(peerHello.nodeId);
        if (full == peerState.commitments.end() || full->second.digest != frontier.digest) {
            return Error{ErrorCode::HashMismatch,
                         "peer writer-window differs from its full frontier"};
        }
    }
    return replaceWriterFrontier(peerState, frontier);
}

Result<bool> preflightTrust(IPeerTrustStore& trustStore, std::string_view nodeId,
                            std::string_view spkiPin, bool allowFirstContact) {
    auto verified = trustStore.verifyOrPin(nodeId, spkiPin, false);
    if (verified) {
        return false;
    }
    if (verified.error().code != ErrorCode::Unauthorized) {
        return verified.error(); // Storage/config failures always fail closed.
    }
    const auto pinned = trustStore.pinnedPin(nodeId);
    if (pinned || !allowFirstContact) {
        return verified.error(); // Existing mismatch or unknown peer without TOFU.
    }
    return true; // Defer first-contact pin commit until state validation succeeds.
}

Result<bool> validatePeerAndPreflightTrust(IPeerTrustStore& trustStore,
                                           const P2pConnection& connection,
                                           const PeerHandshakeConfig& local,
                                           const WireHello& peer) {
    if (auto valid = validatePeerHello(peer, local, connection); !valid) {
        return valid.error();
    }
    return preflightTrust(trustStore, peer.nodeId, connection.peerSpkiPin(),
                          local.allowFirstContact);
}

Result<PeerTrustDecision> commitTrust(IPeerTrustStore& trustStore, std::string_view nodeId,
                                      std::string_view spkiPin, bool pendingFirstContact) {
    if (!pendingFirstContact) {
        return PeerTrustDecision{};
    }
    return trustStore.verifyOrPin(nodeId, spkiPin, true);
}

Result<PeerHandshakeResult> makeHandshakeResult(const P2pConnection& connection,
                                                WireState localState, WireState peerState,
                                                const ValidatedHistoryProof& proof,
                                                PeerTrustDecision trust) {
    return PeerHandshakeResult{.peerNodeId = connection.peerCertCn(),
                               .peerSpkiPin = connection.peerSpkiPin(),
                               .localVersion = std::move(localState.version),
                               .peerVersion = std::move(peerState.version),
                               .peerSeen = std::move(peerState.seen),
                               .peerCommitments = std::move(peerState.commitments),
                               .peerQuarantinedWriters = std::move(peerState.quarantinedWriters),
                               .peerPrefixCounter = proof.proof.counter,
                               .peerPrefixVerified = true,
                               .peerPrefixMatches = proof.matchesLocalPrefix,
                               .firstContactPinned = trust.firstContactPinned};
}

Result<void> validateLocalHandshake(const P2pConnection& connection,
                                    const PeerHandshakeConfig& config) {
    if (auto valid = validateConfig(config); !valid) {
        return valid.error();
    }
    if (config.nodeId != connection.localNodeId()) {
        return Error{ErrorCode::InvalidState, "p2p local node id does not match TLS identity"};
    }
    return {};
}

Result<WireState> readStateFrame(P2pConnection& connection, std::chrono::milliseconds timeout) {
    auto stateJsonValue = readJson(connection, timeout);
    if (!stateJsonValue) {
        return stateJsonValue.error();
    }
    return parseState(stateJsonValue.value());
}

Result<PeerTrustDecision> finalizeTrust(IPeerTrustStore& trustStore,
                                        const P2pConnection& connection,
                                        std::string_view peerNodeId, bool pendingFirstContact) {
    return commitTrust(trustStore, peerNodeId, connection.peerSpkiPin(), pendingFirstContact);
}

Json historyProofJson(const WireHistoryProof& proof) {
    return {{"type", "history_proof"},
            {"writer_id", proof.writerId},
            {"counter", proof.counter},
            {"digest", proof.digest}};
}

Result<WireHistoryProof> parseHistoryProof(const Json& json) {
    try {
        if (!json.is_object() || json.value("type", std::string{}) != "history_proof") {
            return Error{ErrorCode::ValidationError, "unexpected p2p history proof message type"};
        }
        WireHistoryProof proof{.writerId = json.at("writer_id").get<std::string>(),
                               .counter = json.at("counter").get<std::uint64_t>(),
                               .digest = json.at("digest").get<std::string>()};
        if (proof.writerId.empty() || proof.writerId.size() > kMaxP2pIdentityBytes ||
            (proof.counter == 0 ? !proof.digest.empty()
                                : !memory_sync::isSha256Digest(proof.digest))) {
            return Error{ErrorCode::ValidationError, "p2p history proof is invalid"};
        }
        return proof;
    } catch (const std::exception& error) {
        return Error{ErrorCode::ValidationError,
                     std::string("invalid p2p history proof: ") + error.what()};
    }
}

Result<WireHistoryProof> makeLocalHistoryProof(const PeerHandshakeConfig& config,
                                               const WireState& peerState) {
    const auto requestedCounter = peerState.version.get(config.nodeId);
    if (requestedCounter == 0) {
        return WireHistoryProof{.writerId = config.nodeId};
    }
    const auto localCounter = config.localVersion.get(config.nodeId);
    if (requestedCounter > localCounter) {
        return Error{ErrorCode::ValidationError,
                     "peer history is ahead of the authenticated local writer"};
    }
    memory_sync::WriterHistoryCommitment commitment;
    if (requestedCounter == localCounter) {
        const auto current = config.localCommitments.find(config.nodeId);
        if (current == config.localCommitments.end()) {
            return Error{ErrorCode::InvalidState,
                         "local writer commitment is unavailable for history proof"};
        }
        commitment = current->second;
    } else {
        if (!config.resolveLocalCommitment) {
            return Error{ErrorCode::InvalidState,
                         "historical writer commitment resolver is unavailable"};
        }
        auto resolved = config.resolveLocalCommitment(requestedCounter);
        if (!resolved) {
            return resolved.error();
        }
        commitment = std::move(resolved.value());
    }
    if (commitment.counter != requestedCounter || !memory_sync::isSha256Digest(commitment.digest)) {
        return Error{ErrorCode::InvalidState, "local writer history proof is inconsistent"};
    }
    return WireHistoryProof{.writerId = config.nodeId,
                            .counter = requestedCounter,
                            .digest = std::move(commitment.digest)};
}

Result<ValidatedHistoryProof> readAndValidatePeerHistoryProof(P2pConnection& connection,
                                                              const PeerHandshakeConfig& config,
                                                              std::string_view peerNodeId) {
    auto json = readJson(connection, config.timeout);
    if (!json) {
        return json.error();
    }
    auto proof = parseHistoryProof(json.value());
    if (!proof) {
        return proof.error();
    }
    const auto expectedCounter = config.localVersion.get(std::string(peerNodeId));
    if (proof.value().writerId != peerNodeId || proof.value().counter != expectedCounter) {
        return Error{ErrorCode::ValidationError,
                     "peer history proof does not match authenticated writer prefix"};
    }
    if (expectedCounter == 0) {
        return ValidatedHistoryProof{.proof = std::move(proof.value()), .matchesLocalPrefix = true};
    }
    const auto expected = config.localCommitments.find(std::string(peerNodeId));
    if (expected == config.localCommitments.end() || expected->second.counter != expectedCounter ||
        !memory_sync::isSha256Digest(expected->second.digest)) {
        return Error{ErrorCode::InvalidState, "local peer prefix commitment is unavailable"};
    }
    const bool matches = expected->second.digest == proof.value().digest;
    return ValidatedHistoryProof{.proof = std::move(proof.value()), .matchesLocalPrefix = matches};
}

} // namespace

Result<std::string> normalizePeerSpkiPin(std::string_view spkiPin) {
    if (!memory_sync::isSha256Digest(spkiPin)) {
        return Error{ErrorCode::InvalidArgument, "p2p SPKI pin must be a SHA-256 digest"};
    }
    std::string normalizedPin(spkiPin);
    std::ranges::transform(normalizedPin, normalizedPin.begin(),
                           [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return normalizedPin;
}

Result<PeerTrustDecision> evaluatePeerPin(const std::optional<std::string>& existingPin,
                                          std::string_view normalizedPin, bool allowFirstContact) {
    if (!existingPin) {
        return allowFirstContact
                   ? Result<PeerTrustDecision>{PeerTrustDecision{.firstContactPinned = true}}
                   : Result<PeerTrustDecision>{
                         Error{ErrorCode::Unauthorized,
                               "p2p peer is unknown and first-contact trust is disabled"}};
    }
    return *existingPin == normalizedPin
               ? Result<PeerTrustDecision>{PeerTrustDecision{}}
               : Result<PeerTrustDecision>{
                     Error{ErrorCode::Unauthorized, "p2p peer certificate pin changed"}};
}

Result<PeerTrustDecision> InMemoryPeerTrustStore::verifyOrPin(std::string_view nodeId,
                                                              std::string_view spkiPin,
                                                              bool allowFirstContact) {
    if (nodeId.empty()) {
        return Error{ErrorCode::InvalidArgument, "p2p trust node id is empty"};
    }
    auto normalized = normalizePeerSpkiPin(spkiPin);
    if (!normalized) {
        return normalized.error();
    }
    const auto& normalizedPin = normalized.value();
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = pins_.find(std::string(nodeId));
    const std::optional<std::string> existing =
        found == pins_.end() ? std::nullopt : std::optional<std::string>{found->second};
    auto decision = evaluatePeerPin(existing, normalizedPin, allowFirstContact);
    if (decision && decision.value().firstContactPinned) {
        pins_.emplace(nodeId, normalizedPin);
    }
    return decision;
}

std::optional<std::string> InMemoryPeerTrustStore::pinnedPin(std::string_view nodeId) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = pins_.find(std::string(nodeId));
    return found == pins_.end() ? std::optional<std::string>{}
                                : std::optional<std::string>{found->second};
}

std::uint64_t PeerHandshakeResult::peerWatermark(std::string_view writerId) const {
    const auto found = peerSeen.find(std::string(writerId));
    return found == peerSeen.end() ? 0 : found->second;
}

Result<bool> requiresPeerWriterQuarantine(const memory_sync::ReplicationState& local,
                                          const PeerHandshakeResult& peer) {
    if (peer.peerNodeId.empty()) {
        return Error{ErrorCode::ValidationError, "authenticated peer writer identity is empty"};
    }
    if (peer.peerQuarantinedWriters.contains(peer.peerNodeId)) {
        return true;
    }
    if (peer.peerPrefixVerified && !peer.peerPrefixMatches) {
        return true;
    }
    const auto localCounter = local.version.get(peer.peerNodeId);
    const auto peerCounter = peer.peerVersion.get(peer.peerNodeId);
    if (localCounter > peerCounter) {
        return Error{ErrorCode::ValidationError,
                     "authenticated peer writer rolled back below the local prefix"};
    }
    if (localCounter < peerCounter) {
        if (!peer.peerPrefixVerified || peer.peerPrefixCounter != localCounter) {
            return Error{ErrorCode::ValidationError,
                         "authenticated peer writer prefix was not verified"};
        }
        return false;
    }
    if (localCounter == 0) {
        return false;
    }
    const auto localCommitment = local.commitments.find(peer.peerNodeId);
    const auto peerCommitment = peer.peerCommitments.find(peer.peerNodeId);
    if (localCommitment == local.commitments.end() ||
        peerCommitment == peer.peerCommitments.end() ||
        localCommitment->second.counter != localCounter ||
        peerCommitment->second.counter != peerCounter ||
        !memory_sync::isSha256Digest(localCommitment->second.digest) ||
        !memory_sync::isSha256Digest(peerCommitment->second.digest)) {
        return Error{ErrorCode::ValidationError,
                     "equal-counter peer history commitment is missing or invalid"};
    }
    return localCommitment->second.digest != peerCommitment->second.digest;
}

Result<PeerHandshakeResult> initiatePeerHandshake(P2pConnection& connection,
                                                  const PeerHandshakeConfig& config,
                                                  IPeerTrustStore& trustStore) {
    const auto fail = [&](Error error) -> Result<PeerHandshakeResult> {
        connection.close();
        return error;
    };
    if (auto valid = validateLocalHandshake(connection, config); !valid) {
        return fail(valid.error());
    }
    if (auto written = writeJson(connection, helloJson(config), config.timeout); !written) {
        return fail(written.error());
    }
    auto acknowledgement = readJson(connection, config.timeout);
    if (!acknowledgement) {
        return fail(acknowledgement.error());
    }
    if (!acknowledgement.value().is_object() ||
        acknowledgement.value().value("type", std::string{}) != "hello_ack") {
        return fail(Error{ErrorCode::ValidationError, "unexpected p2p hello acknowledgement"});
    }
    if (!acknowledgement.value().value("accepted", false)) {
        return fail(Error{ErrorCode::Unauthorized,
                          acknowledgement.value().value("reason", "p2p handshake rejected")});
    }
    auto peerHello = parseHello(acknowledgement.value(), "hello_ack");
    if (!peerHello) {
        return fail(peerHello.error());
    }
    auto pendingTrust =
        validatePeerAndPreflightTrust(trustStore, connection, config, peerHello.value());
    if (!pendingTrust) {
        return fail(pendingTrust.error());
    }
    if (auto written = writeJson(connection, stateJson(config), config.timeout); !written) {
        return fail(written.error());
    }
    auto peerState = readStateFrame(connection, config.timeout);
    if (!peerState) {
        return fail(peerState.error());
    }
    auto localWindow = makeLocalWindowFrontier(config, peerState.value(), peerHello.value());
    if (!localWindow) {
        return fail(localWindow.error());
    }
    if (auto written =
            writeJson(connection, windowFrontierJson(localWindow.value()), config.timeout);
        !written) {
        return fail(written.error());
    }
    auto peerWindowJson = readJson(connection, config.timeout);
    if (!peerWindowJson) {
        return fail(peerWindowJson.error());
    }
    auto peerWindow = parseWindowFrontier(peerWindowJson.value());
    if (!peerWindow) {
        return fail(peerWindow.error());
    }
    auto boundedPeer = validatePeerWindowFrontier(config, peerState.value(), peerHello.value(),
                                                  peerWindow.value());
    if (!boundedPeer) {
        return fail(boundedPeer.error());
    }
    WireState localState{.version = config.localVersion,
                         .seen = config.localVersion.counters(),
                         .commitments = config.localCommitments,
                         .quarantinedWriters = config.localQuarantinedWriters};
    auto boundedLocal = replaceWriterFrontier(std::move(localState), localWindow.value());
    auto localProof = makeLocalHistoryProof(config, peerState.value());
    if (!localProof) {
        return fail(localProof.error());
    }
    if (auto written = writeJson(connection, historyProofJson(localProof.value()), config.timeout);
        !written) {
        return fail(written.error());
    }
    auto peerProof = readAndValidatePeerHistoryProof(connection, config, peerHello.value().nodeId);
    if (!peerProof) {
        return fail(peerProof.error());
    }
    if (!peerProof.value().matchesLocalPrefix && pendingTrust.value()) {
        return fail(
            Error{ErrorCode::Unauthorized, "first-contact peer failed writer prefix verification"});
    }
    auto trust =
        finalizeTrust(trustStore, connection, peerHello.value().nodeId, pendingTrust.value());
    if (!trust) {
        return fail(trust.error());
    }
    return makeHandshakeResult(connection, std::move(boundedLocal), std::move(boundedPeer.value()),
                               peerProof.value(), trust.value());
}

Result<PeerHandshakeResult> acceptPeerHandshake(P2pConnection& connection,
                                                const PeerHandshakeConfig& config,
                                                IPeerTrustStore& trustStore) {
    const auto fail = [&](Error error) -> Result<PeerHandshakeResult> {
        connection.close();
        return error;
    };
    const auto reject = [&](Error error) -> Result<PeerHandshakeResult> {
        (void)writeJson(connection, acknowledgementJson(config, false, error.message),
                        config.timeout);
        return fail(std::move(error));
    };
    if (auto valid = validateLocalHandshake(connection, config); !valid) {
        return fail(valid.error());
    }
    auto helloJsonValue = readJson(connection, config.timeout);
    if (!helloJsonValue) {
        return fail(helloJsonValue.error());
    }
    auto hello = parseHello(helloJsonValue.value(), "hello");
    if (!hello) {
        return reject(hello.error());
    }
    auto pendingTrust =
        validatePeerAndPreflightTrust(trustStore, connection, config, hello.value());
    if (!pendingTrust) {
        return reject(pendingTrust.error());
    }
    if (auto written = writeJson(connection, acknowledgementJson(config, true), config.timeout);
        !written) {
        return fail(written.error());
    }
    auto peerState = readStateFrame(connection, config.timeout);
    if (!peerState) {
        return fail(peerState.error());
    }
    if (auto written = writeJson(connection, stateJson(config), config.timeout); !written) {
        return fail(written.error());
    }
    auto peerWindowJson = readJson(connection, config.timeout);
    if (!peerWindowJson) {
        return fail(peerWindowJson.error());
    }
    auto peerWindow = parseWindowFrontier(peerWindowJson.value());
    if (!peerWindow) {
        return fail(peerWindow.error());
    }
    auto boundedPeer =
        validatePeerWindowFrontier(config, peerState.value(), hello.value(), peerWindow.value());
    if (!boundedPeer) {
        return fail(boundedPeer.error());
    }
    auto localWindow = makeLocalWindowFrontier(config, peerState.value(), hello.value());
    if (!localWindow) {
        return fail(localWindow.error());
    }
    if (auto written =
            writeJson(connection, windowFrontierJson(localWindow.value()), config.timeout);
        !written) {
        return fail(written.error());
    }
    WireState localState{.version = config.localVersion,
                         .seen = config.localVersion.counters(),
                         .commitments = config.localCommitments,
                         .quarantinedWriters = config.localQuarantinedWriters};
    auto boundedLocal = replaceWriterFrontier(std::move(localState), localWindow.value());
    auto peerProof = readAndValidatePeerHistoryProof(connection, config, hello.value().nodeId);
    if (!peerProof) {
        return fail(peerProof.error());
    }
    if (!peerProof.value().matchesLocalPrefix && pendingTrust.value()) {
        return fail(
            Error{ErrorCode::Unauthorized, "first-contact peer failed writer prefix verification"});
    }
    auto localProof = makeLocalHistoryProof(config, peerState.value());
    if (!localProof) {
        return fail(localProof.error());
    }
    if (auto written = writeJson(connection, historyProofJson(localProof.value()), config.timeout);
        !written) {
        return fail(written.error());
    }
    auto trust = finalizeTrust(trustStore, connection, hello.value().nodeId, pendingTrust.value());
    if (!trust) {
        return fail(trust.error());
    }
    return makeHandshakeResult(connection, std::move(boundedLocal), std::move(boundedPeer.value()),
                               peerProof.value(), trust.value());
}

} // namespace yams::daemon::p2p
