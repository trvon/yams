// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// pi-lens-ignore: fatal error
#include <yams/daemon/p2p/p2p_protocol.h>

// pi-lens-ignore: fatal error
#include <nlohmann/json.hpp>

#include <cctype>
#include <cstring>
#include <utility>

namespace yams::daemon::p2p {

namespace {

using Json = nlohmann::json;

struct WireHello {
    std::uint32_t protocol{0};
    std::uint32_t schemaVersion{0};
    std::string nodeId;
    std::string corpusId;
    std::uint64_t corpusEpoch{0};
};

struct WireState {
    memory_sync::VersionVector version;
    std::map<memory_sync::NodeId, std::uint64_t> seen;
};

Result<void> validateConfig(const PeerHandshakeConfig& config) {
    if (config.nodeId.empty() || config.nodeId == "default" || config.corpusId.empty() ||
        config.corpusEpoch == 0) {
        return Error{ErrorCode::InvalidArgument,
                     "p2p handshake requires node, corpus, and epoch identity"};
    }
    if (config.timeout <= std::chrono::milliseconds::zero()) {
        return Error{ErrorCode::InvalidArgument, "p2p handshake timeout must be positive"};
    }
    return {};
}

Result<void> writeJson(P2pConnection& connection, const Json& json,
                       std::chrono::milliseconds timeout) {
    std::string text;
    try {
        text = json.dump();
    } catch (const std::exception& error) {
        return Error{ErrorCode::SerializationError, error.what()};
    }
    return connection.writeFrame(
        std::span<const std::byte>(reinterpret_cast<const std::byte*>(text.data()), text.size()),
        timeout);
}

Result<Json> readJson(P2pConnection& connection, std::chrono::milliseconds timeout) {
    auto frame = connection.readFrame(timeout);
    if (!frame) {
        return frame.error();
    }
    try {
        const std::string_view text(reinterpret_cast<const char*>(frame.value().data()),
                                    frame.value().size());
        return Json::parse(text);
    } catch (const std::exception& error) {
        return Error{ErrorCode::SerializationError,
                     std::string("invalid p2p handshake JSON: ") + error.what()};
    }
}

Json helloJson(const PeerHandshakeConfig& config) {
    return {{"type", "hello"},
            {"protocol", kP2pProtocolVersion},
            {"schema_version", kP2pEnvelopeSchemaVersion},
            {"node_id", config.nodeId},
            {"corpus_id", config.corpusId},
            {"corpus_epoch", config.corpusEpoch}};
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
                         .corpusEpoch = json.at("corpus_epoch").get<std::uint64_t>()};
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
    if (peer.nodeId.empty() || peer.nodeId != connection.peerCertCn()) {
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
              {"corpus_epoch", config.corpusEpoch}};
    if (!reason.empty()) {
        json["reason"] = reason;
    }
    return json;
}

Json stateJson(const PeerHandshakeConfig& config) {
    return {
        {"type", "state"}, {"vv", config.localVersion}, {"seen", config.localVersion.counters()}};
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
        return state;
    } catch (const std::exception& error) {
        return Error{ErrorCode::ValidationError, std::string("invalid p2p state: ") + error.what()};
    }
}

Result<bool> preflightTrust(IPeerTrustStore& trustStore, std::string_view nodeId,
                            std::string_view spkiPin, bool allowFirstContact) {
    const auto pinned = trustStore.pinnedPin(nodeId);
    if (!pinned) {
        if (!allowFirstContact) {
            return Error{ErrorCode::Unauthorized,
                         "p2p peer is unknown and first-contact trust is disabled"};
        }
        return true; // Defer first-contact pin commit until state validation succeeds.
    }
    auto verified = trustStore.verifyOrPin(nodeId, spkiPin, false);
    if (!verified) {
        return verified.error();
    }
    return false;
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

Result<PeerHandshakeResult> makeHandshakeResult(const P2pConnection& connection, WireState state,
                                                PeerTrustDecision trust) {
    return PeerHandshakeResult{.peerNodeId = connection.peerCertCn(),
                               .peerSpkiPin = connection.peerSpkiPin(),
                               .peerVersion = std::move(state.version),
                               .peerSeen = std::move(state.seen),
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

} // namespace

Result<PeerTrustDecision> InMemoryPeerTrustStore::verifyOrPin(std::string_view nodeId,
                                                              std::string_view spkiPin,
                                                              bool allowFirstContact) {
    if (nodeId.empty() || !memory_sync::isSha256Digest(spkiPin)) {
        return Error{ErrorCode::InvalidArgument, "p2p trust identity is invalid"};
    }
    std::string normalizedPin(spkiPin);
    std::ranges::transform(normalizedPin, normalizedPin.begin(),
                           [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = pins_.find(std::string(nodeId));
    if (found == pins_.end()) {
        if (!allowFirstContact) {
            return Error{ErrorCode::Unauthorized,
                         "p2p peer is unknown and first-contact trust is disabled"};
        }
        pins_.emplace(nodeId, normalizedPin);
        return PeerTrustDecision{.firstContactPinned = true};
    }
    if (found->second != normalizedPin) {
        return Error{ErrorCode::Unauthorized, "p2p peer certificate pin changed"};
    }
    return PeerTrustDecision{.firstContactPinned = false};
}

std::optional<std::string> InMemoryPeerTrustStore::pinnedPin(std::string_view nodeId) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = pins_.find(std::string(nodeId));
    return found == pins_.end() ? std::nullopt : std::optional<std::string>{found->second};
}

std::uint64_t PeerHandshakeResult::peerWatermark(std::string_view writerId) const {
    const auto found = peerSeen.find(std::string(writerId));
    return found == peerSeen.end() ? 0 : found->second;
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
    auto state = readStateFrame(connection, config.timeout);
    if (!state) {
        return fail(state.error());
    }
    auto trust =
        finalizeTrust(trustStore, connection, peerHello.value().nodeId, pendingTrust.value());
    if (!trust) {
        return fail(trust.error());
    }
    return makeHandshakeResult(connection, std::move(state.value()), trust.value());
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
    auto state = readStateFrame(connection, config.timeout);
    if (!state) {
        return fail(state.error());
    }
    auto trust = finalizeTrust(trustStore, connection, hello.value().nodeId, pendingTrust.value());
    if (!trust) {
        return fail(trust.error());
    }
    if (auto written = writeJson(connection, stateJson(config), config.timeout); !written) {
        return fail(written.error());
    }
    return makeHandshakeResult(connection, std::move(state.value()), trust.value());
}

} // namespace yams::daemon::p2p
