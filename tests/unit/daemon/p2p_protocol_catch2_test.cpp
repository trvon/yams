// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// pi-lens-ignore: fatal error
#include <catch2/catch_test_macros.hpp>
// pi-lens-ignore: fatal error
#include <nlohmann/json.hpp>

#include <yams/daemon/p2p/p2p_protocol.h>
#include <yams/memory_sync/writer_auth.h>

#include <chrono>
#include <future>
#include <string>
#include <utility>

using namespace std::chrono_literals;
using yams::daemon::p2p::InMemoryPeerTrustStore;
using yams::daemon::p2p::P2pClientOptions;
using yams::daemon::p2p::P2pConnection;
using yams::daemon::p2p::P2pListener;
using yams::daemon::p2p::PeerHandshakeConfig;
using yams::daemon::p2p::PeerHandshakeResult;
using yams::daemon::p2p::TlsIdentity;

namespace {

TlsIdentity identity(std::string nodeId, const yams::memory_sync::WriterKeyPair& keyPair) {
    auto result = TlsIdentity::fromPrivateKeyPem(std::move(nodeId), keyPair.privateKeyPem);
    REQUIRE(result.has_value());
    return std::move(result.value());
}

PeerHandshakeConfig config(std::string nodeId, std::string corpusId = "shared-corpus",
                           std::uint64_t epoch = 1) {
    return PeerHandshakeConfig{.nodeId = std::move(nodeId),
                               .corpusId = std::move(corpusId),
                               .corpusEpoch = epoch,
                               .localVersion = {},
                               .allowFirstContact = true,
                               .timeout = 2s};
}

struct PairResult {
    yams::Result<PeerHandshakeResult> client;
    yams::Result<PeerHandshakeResult> server;
    std::string clientPin;
    std::string serverPin;
};

PairResult runHandshake(PeerHandshakeConfig serverConfig, PeerHandshakeConfig clientConfig,
                        InMemoryPeerTrustStore& serverTrust, InMemoryPeerTrustStore& clientTrust,
                        std::string clientCertificateNodeId = {}) {
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(serverKey.has_value());
    REQUIRE(clientKey.has_value());

    auto serverIdentity = identity(serverConfig.nodeId, serverKey.value());
    auto clientIdentity =
        identity(clientCertificateNodeId.empty() ? clientConfig.nodeId : clientCertificateNodeId,
                 clientKey.value());
    const std::string serverPin = serverIdentity.spkiPin();
    const std::string clientPin = clientIdentity.spkiPin();

    P2pListener listener(P2pListener::Options{.host = "127.0.0.1",
                                              .port = 0,
                                              .identity = std::move(serverIdentity),
                                              .allowedPeerPins = {},
                                              .allowUnpinnedPeers = true,
                                              .handshakeTimeout = 2s,
                                              .maxConcurrentSessions = 1});
    std::promise<yams::Result<PeerHandshakeResult>> serverDone;
    auto serverFuture = serverDone.get_future();
    listener.setHandler([&](P2pConnection channel) {
        serverDone.set_value(
            yams::daemon::p2p::acceptPeerHandshake(channel, serverConfig, serverTrust));
    });
    REQUIRE(listener.start().has_value());

    auto channel =
        yams::daemon::p2p::p2pConnect(P2pClientOptions{.host = "127.0.0.1",
                                                       .port = listener.boundPort(),
                                                       .identity = std::move(clientIdentity),
                                                       .expectedPeerPin = {},
                                                       .allowUnpinnedPeer = true,
                                                       .connectTimeout = 2s,
                                                       .handshakeTimeout = 2s});
    REQUIRE(channel.has_value());
    auto clientResult =
        yams::daemon::p2p::initiatePeerHandshake(channel.value(), clientConfig, clientTrust);
    REQUIRE(serverFuture.wait_for(3s) == std::future_status::ready);
    auto serverResult = serverFuture.get();
    listener.stop();
    return PairResult{.client = std::move(clientResult),
                      .server = std::move(serverResult),
                      .clientPin = clientPin,
                      .serverPin = serverPin};
}

} // namespace

TEST_CASE("P2P peer trust store pins first contact and rejects key changes",
          "[daemon][p2p][handshake][trust]") {
    InMemoryPeerTrustStore trust;
    const std::string firstPin(64, 'a');
    const std::string changedPin(64, 'b');

    auto unknown = trust.verifyOrPin("peer", firstPin, false);
    REQUIRE_FALSE(unknown.has_value());
    CHECK(unknown.error().code == yams::ErrorCode::Unauthorized);

    auto first = trust.verifyOrPin("peer", firstPin, true);
    REQUIRE(first.has_value());
    CHECK(first.value().firstContactPinned);
    CHECK(trust.pinnedPin("peer") == firstPin);

    auto known = trust.verifyOrPin("peer", firstPin, false);
    REQUIRE(known.has_value());
    CHECK_FALSE(known.value().firstContactPinned);

    auto changed = trust.verifyOrPin("peer", changedPin, true);
    REQUIRE_FALSE(changed.has_value());
    CHECK(changed.error().code == yams::ErrorCode::Unauthorized);
    CHECK(trust.pinnedPin("peer") == firstPin);
}

TEST_CASE("P2P hello binds TLS identities and exchanges causal watermarks",
          "[daemon][p2p][handshake][network]") {
    auto serverConfig = config("server-node");
    auto clientConfig = config("client-node");
    REQUIRE(serverConfig.localVersion.increment("server-node"));
    REQUIRE(serverConfig.localVersion.increment("server-node"));
    REQUIRE(serverConfig.localVersion.increment("server-node"));
    REQUIRE(clientConfig.localVersion.increment("client-node"));
    REQUIRE(clientConfig.localVersion.increment("client-node"));
    serverConfig.localCommitments["server-node"] = {.counter = 3, .digest = std::string(64, 'a')};
    clientConfig.localCommitments["client-node"] = {.counter = 2, .digest = std::string(64, 'b')};
    clientConfig.localQuarantinedWriters.insert("retired-client-writer");

    InMemoryPeerTrustStore serverTrust;
    InMemoryPeerTrustStore clientTrust;
    auto pair = runHandshake(serverConfig, clientConfig, serverTrust, clientTrust);

    REQUIRE(pair.client.has_value());
    REQUIRE(pair.server.has_value());
    CHECK(pair.client.value().peerNodeId == "server-node");
    CHECK(pair.server.value().peerNodeId == "client-node");
    CHECK(pair.client.value().firstContactPinned);
    CHECK(pair.server.value().firstContactPinned);
    CHECK(pair.client.value().peerWatermark("server-node") == 3);
    CHECK(pair.server.value().peerWatermark("client-node") == 2);
    CHECK(pair.client.value().peerVersion.get("server-node") == 3);
    CHECK(pair.server.value().peerVersion.get("client-node") == 2);
    REQUIRE(pair.client.value().peerCommitments.contains("server-node"));
    CHECK(pair.client.value().peerCommitments.at("server-node").counter == 3);
    CHECK(pair.client.value().peerCommitments.at("server-node").digest == std::string(64, 'a'));
    CHECK(pair.server.value().peerQuarantinedWriters.contains("retired-client-writer"));
    CHECK(clientTrust.pinnedPin("server-node") == pair.serverPin);
    CHECK(serverTrust.pinnedPin("client-node") == pair.clientPin);
}

TEST_CASE("P2P history comparison binds only the authenticated peer writer",
          "[daemon][p2p][handshake][commitment]") {
    yams::memory_sync::ReplicationState local;
    REQUIRE(local.version.increment("peer-node"));
    local.commitments["peer-node"] = {.counter = 1, .digest = std::string(64, 'a')};
    local.commitments["unrelated"] = {.counter = 7, .digest = std::string(64, 'b')};

    PeerHandshakeResult peer;
    peer.peerNodeId = "peer-node";
    REQUIRE(peer.peerVersion.increment("peer-node"));
    peer.peerCommitments["peer-node"] = {.counter = 1, .digest = std::string(64, 'a')};
    peer.peerCommitments["unrelated"] = {.counter = 7, .digest = std::string(64, 'c')};
    auto unrelatedMismatch = yams::daemon::p2p::requiresPeerWriterQuarantine(local, peer);
    REQUIRE(unrelatedMismatch.has_value());
    CHECK_FALSE(unrelatedMismatch.value());

    peer.peerCommitments["peer-node"].digest = std::string(64, 'd');
    auto authenticatedMismatch = yams::daemon::p2p::requiresPeerWriterQuarantine(local, peer);
    REQUIRE(authenticatedMismatch.has_value());
    CHECK(authenticatedMismatch.value());

    peer.peerCommitments.erase("peer-node");
    auto missing = yams::daemon::p2p::requiresPeerWriterQuarantine(local, peer);
    REQUIRE_FALSE(missing.has_value());
    CHECK(missing.error().code == yams::ErrorCode::ValidationError);
}

TEST_CASE("P2P handshake verifies a nonzero authenticated writer prefix before exchange",
          "[daemon][p2p][handshake][commitment][prefix]") {
    auto serverConfig = config("server-node");
    auto clientConfig = config("client-node");
    REQUIRE(serverConfig.localVersion.increment("server-node"));
    REQUIRE(serverConfig.localVersion.increment("server-node"));
    serverConfig.localCommitments["server-node"] = {.counter = 2, .digest = std::string(64, 'b')};
    serverConfig.resolveLocalCommitment =
        [](std::uint64_t counter) -> yams::Result<yams::memory_sync::WriterHistoryCommitment> {
        if (counter != 1) {
            return yams::Error{yams::ErrorCode::InvalidState, "unexpected proof counter"};
        }
        return yams::memory_sync::WriterHistoryCommitment{.counter = 1,
                                                          .digest = std::string(64, 'a')};
    };
    REQUIRE(clientConfig.localVersion.increment("server-node"));
    clientConfig.localCommitments["server-node"] = {.counter = 1, .digest = std::string(64, 'a')};

    InMemoryPeerTrustStore serverTrust;
    InMemoryPeerTrustStore clientTrust;
    auto pair = runHandshake(serverConfig, clientConfig, serverTrust, clientTrust);
    REQUIRE(pair.client.has_value());
    REQUIRE(pair.server.has_value());
    CHECK(pair.client.value().peerPrefixVerified);
    CHECK(pair.client.value().peerPrefixCounter == 1);
    auto verified = yams::daemon::p2p::requiresPeerWriterQuarantine(
        yams::memory_sync::ReplicationState{.version = clientConfig.localVersion,
                                            .commitments = clientConfig.localCommitments},
        pair.client.value());
    REQUIRE(verified.has_value());
    CHECK_FALSE(verified.value());
}

TEST_CASE("P2P handshake verifies an acceptor prefix of the connector writer",
          "[daemon][p2p][handshake][commitment][prefix]") {
    auto serverConfig = config("server-node");
    auto clientConfig = config("client-node");
    REQUIRE(clientConfig.localVersion.increment("client-node"));
    REQUIRE(clientConfig.localVersion.increment("client-node"));
    clientConfig.localCommitments["client-node"] = {.counter = 2, .digest = std::string(64, 'd')};
    clientConfig.resolveLocalCommitment =
        [](std::uint64_t counter) -> yams::Result<yams::memory_sync::WriterHistoryCommitment> {
        if (counter != 1) {
            return yams::Error{yams::ErrorCode::InvalidState, "unexpected proof counter"};
        }
        return yams::memory_sync::WriterHistoryCommitment{.counter = 1,
                                                          .digest = std::string(64, 'c')};
    };
    REQUIRE(serverConfig.localVersion.increment("client-node"));
    serverConfig.localCommitments["client-node"] = {.counter = 1, .digest = std::string(64, 'c')};

    InMemoryPeerTrustStore serverTrust;
    InMemoryPeerTrustStore clientTrust;
    auto pair = runHandshake(serverConfig, clientConfig, serverTrust, clientTrust);
    REQUIRE(pair.client.has_value());
    REQUIRE(pair.server.has_value());
    CHECK(pair.server.value().peerPrefixVerified);
    CHECK(pair.server.value().peerPrefixMatches);
    CHECK(pair.server.value().peerPrefixCounter == 1);
}

TEST_CASE("P2P handshake fails closed when writer prefix proof is unavailable",
          "[daemon][p2p][handshake][commitment][prefix][security]") {
    auto serverConfig = config("server-node");
    auto clientConfig = config("client-node");
    REQUIRE(serverConfig.localVersion.increment("server-node"));
    serverConfig.localCommitments["server-node"] = {.counter = 1, .digest = std::string(64, 'a')};

    SECTION("historical prefix cannot be derived") {
        REQUIRE(serverConfig.localVersion.increment("server-node"));
        serverConfig.localCommitments["server-node"] = {.counter = 2,
                                                        .digest = std::string(64, 'b')};
        REQUIRE(clientConfig.localVersion.increment("server-node"));
        clientConfig.localCommitments["server-node"] = {.counter = 1,
                                                        .digest = std::string(64, 'a')};
    }
    SECTION("peer claims history ahead of the authenticated writer") {
        REQUIRE(clientConfig.localVersion.increment("server-node"));
        REQUIRE(clientConfig.localVersion.increment("server-node"));
        clientConfig.localCommitments["server-node"] = {.counter = 2,
                                                        .digest = std::string(64, 'b')};
    }

    InMemoryPeerTrustStore serverTrust;
    InMemoryPeerTrustStore clientTrust;
    auto pair = runHandshake(serverConfig, clientConfig, serverTrust, clientTrust);
    REQUIRE_FALSE(pair.client.has_value());
    REQUIRE_FALSE(pair.server.has_value());
    CHECK_FALSE(serverTrust.pinnedPin("client-node").has_value());
    CHECK_FALSE(clientTrust.pinnedPin("server-node").has_value());
}

TEST_CASE("P2P handshake rejects unbounded local replication state",
          "[daemon][p2p][handshake][commitment][limits]") {
    auto serverConfig = config("server-node");
    auto clientConfig = config("client-node");
    for (std::size_t index = 0; index <= yams::daemon::p2p::kMaxP2pReplicationWriters; ++index) {
        const auto writer = "writer-" + std::to_string(index);
        REQUIRE(clientConfig.localVersion.increment(writer));
        clientConfig.localCommitments[writer] = {.counter = 1, .digest = std::string(64, 'a')};
    }

    InMemoryPeerTrustStore serverTrust;
    InMemoryPeerTrustStore clientTrust;
    auto pair = runHandshake(serverConfig, clientConfig, serverTrust, clientTrust);
    REQUIRE_FALSE(pair.client.has_value());
    CHECK(pair.client.error().code == yams::ErrorCode::ResourceExhausted);
    REQUIRE_FALSE(pair.server.has_value());
    CHECK_FALSE(serverTrust.pinnedPin("client-node").has_value());
}

TEST_CASE("P2P hello rejects incompatible replication identities",
          "[daemon][p2p][handshake][network]") {
    auto serverConfig = config("server-node");
    auto clientConfig = config("client-node");
    std::string certificateNode;
    yams::ErrorCode expectedClientCode = yams::ErrorCode::Unauthorized;
    yams::ErrorCode expectedServerCode = yams::ErrorCode::ValidationError;
    bool clientClosedBeforeHello = false;

    SECTION("corpus mismatch") {
        clientConfig.corpusId = "other-corpus";
    }
    SECTION("epoch mismatch") {
        clientConfig.corpusEpoch = 2;
    }
    SECTION("local claimed node differs from TLS certificate") {
        certificateNode = "certificate-node";
        expectedClientCode = yams::ErrorCode::InvalidState;
        clientClosedBeforeHello = true;
    }

    InMemoryPeerTrustStore serverTrust;
    InMemoryPeerTrustStore clientTrust;
    auto pair = runHandshake(serverConfig, clientConfig, serverTrust, clientTrust, certificateNode);

    REQUIRE_FALSE(pair.client.has_value());
    REQUIRE_FALSE(pair.server.has_value());
    CHECK(pair.client.error().code == expectedClientCode);
    if (clientClosedBeforeHello) {
        const auto code = pair.server.error().code;
        CHECK((code == yams::ErrorCode::NetworkError || code == yams::ErrorCode::NotFound ||
               code == yams::ErrorCode::OperationCancelled));
    } else {
        CHECK(pair.server.error().code == expectedServerCode);
    }
    CHECK_FALSE(serverTrust.pinnedPin("client-node").has_value());
    CHECK_FALSE(clientTrust.pinnedPin("server-node").has_value());
}

TEST_CASE("P2P hello rejects unsupported wire protocol and schema versions",
          "[daemon][p2p][handshake][network]") {
    std::uint32_t protocol = yams::daemon::p2p::kP2pProtocolVersion;
    std::uint32_t schema = yams::daemon::p2p::kP2pEnvelopeSchemaVersion;
    SECTION("protocol mismatch") {
        ++protocol;
    }
    SECTION("schema mismatch") {
        --schema;
    }

    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(serverKey.has_value());
    REQUIRE(clientKey.has_value());
    auto serverIdentity = identity("server-node", serverKey.value());
    auto clientIdentity = identity("client-node", clientKey.value());
    const std::string serverPin = serverIdentity.spkiPin();
    const std::string clientPin = clientIdentity.spkiPin();

    auto serverConfig = config("server-node");
    InMemoryPeerTrustStore serverTrust;
    std::promise<yams::Result<PeerHandshakeResult>> serverDone;
    auto serverFuture = serverDone.get_future();
    P2pListener listener(P2pListener::Options{.host = "127.0.0.1",
                                              .port = 0,
                                              .identity = std::move(serverIdentity),
                                              .allowedPeerPins = {clientPin},
                                              .allowUnpinnedPeers = false,
                                              .handshakeTimeout = 2s,
                                              .maxConcurrentSessions = 1});
    listener.setHandler([&](P2pConnection channel) {
        serverDone.set_value(
            yams::daemon::p2p::acceptPeerHandshake(channel, serverConfig, serverTrust));
    });
    REQUIRE(listener.start().has_value());

    auto channel =
        yams::daemon::p2p::p2pConnect(P2pClientOptions{.host = "127.0.0.1",
                                                       .port = listener.boundPort(),
                                                       .identity = std::move(clientIdentity),
                                                       .expectedPeerPin = serverPin,
                                                       .allowUnpinnedPeer = false,
                                                       .connectTimeout = 2s,
                                                       .handshakeTimeout = 2s});
    REQUIRE(channel.has_value());
    const nlohmann::json hello{
        {"type", "hello"},          {"protocol", protocol},         {"schema_version", schema},
        {"node_id", "client-node"}, {"corpus_id", "shared-corpus"}, {"corpus_epoch", 1}};
    const std::string helloText = hello.dump();
    REQUIRE(
        channel.value()
            .writeFrame(std::span<const std::byte>(
                            reinterpret_cast<const std::byte*>(helloText.data()), helloText.size()),
                        2s)
            .has_value());
    auto acknowledgement = channel.value().readFrame(2s);
    REQUIRE(acknowledgement.has_value());
    const std::string ackText(reinterpret_cast<const char*>(acknowledgement.value().data()),
                              acknowledgement.value().size());
    const auto ack = nlohmann::json::parse(ackText);
    CHECK_FALSE(ack.at("accepted").get<bool>());

    REQUIRE(serverFuture.wait_for(3s) == std::future_status::ready);
    auto serverResult = serverFuture.get();
    REQUIRE_FALSE(serverResult.has_value());
    CHECK(serverResult.error().code == yams::ErrorCode::NotSupported);
    CHECK_FALSE(serverTrust.pinnedPin("client-node").has_value());
    listener.stop();
}

TEST_CASE("P2P first contact is not pinned until state validation succeeds",
          "[daemon][p2p][handshake][network][trust]") {
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(serverKey.has_value());
    REQUIRE(clientKey.has_value());
    auto serverIdentity = identity("server-node", serverKey.value());
    auto clientIdentity = identity("client-node", clientKey.value());
    const std::string serverPin = serverIdentity.spkiPin();
    const std::string clientPin = clientIdentity.spkiPin();

    auto serverConfig = config("server-node");
    InMemoryPeerTrustStore serverTrust;
    std::promise<yams::Result<PeerHandshakeResult>> serverDone;
    auto serverFuture = serverDone.get_future();
    P2pListener listener(P2pListener::Options{.host = "127.0.0.1",
                                              .port = 0,
                                              .identity = std::move(serverIdentity),
                                              .allowedPeerPins = {clientPin},
                                              .allowUnpinnedPeers = false,
                                              .handshakeTimeout = 2s,
                                              .maxConcurrentSessions = 1});
    listener.setHandler([&](P2pConnection channel) {
        serverDone.set_value(
            yams::daemon::p2p::acceptPeerHandshake(channel, serverConfig, serverTrust));
    });
    REQUIRE(listener.start().has_value());

    auto channel =
        yams::daemon::p2p::p2pConnect(P2pClientOptions{.host = "127.0.0.1",
                                                       .port = listener.boundPort(),
                                                       .identity = std::move(clientIdentity),
                                                       .expectedPeerPin = serverPin,
                                                       .allowUnpinnedPeer = false,
                                                       .connectTimeout = 2s,
                                                       .handshakeTimeout = 2s});
    REQUIRE(channel.has_value());
    const nlohmann::json hello{{"type", "hello"},
                               {"protocol", yams::daemon::p2p::kP2pProtocolVersion},
                               {"schema_version", yams::daemon::p2p::kP2pEnvelopeSchemaVersion},
                               {"node_id", "client-node"},
                               {"corpus_id", "shared-corpus"},
                               {"corpus_epoch", 1}};
    const std::string helloText = hello.dump();
    REQUIRE(
        channel.value()
            .writeFrame(std::span<const std::byte>(
                            reinterpret_cast<const std::byte*>(helloText.data()), helloText.size()),
                        2s)
            .has_value());
    REQUIRE(channel.value().readFrame(2s).has_value()); // accepted hello_ack

    const nlohmann::json malformedState{{"type", "state"},
                                        {"vv", {{"counters_", {{"client-node", 1}}}}},
                                        {"seen", {{"client-node", 2}}}};
    const std::string stateText = malformedState.dump();
    REQUIRE(
        channel.value()
            .writeFrame(std::span<const std::byte>(
                            reinterpret_cast<const std::byte*>(stateText.data()), stateText.size()),
                        2s)
            .has_value());

    REQUIRE(serverFuture.wait_for(3s) == std::future_status::ready);
    auto serverResult = serverFuture.get();
    REQUIRE_FALSE(serverResult.has_value());
    CHECK(serverResult.error().code == yams::ErrorCode::ValidationError);
    CHECK_FALSE(serverTrust.pinnedPin("client-node").has_value());
    listener.stop();
}

TEST_CASE("P2P hello rejects a previously pinned peer with a changed key",
          "[daemon][p2p][handshake][network][trust]") {
    auto serverConfig = config("server-node");
    auto clientConfig = config("client-node");
    serverConfig.allowFirstContact = false;

    InMemoryPeerTrustStore serverTrust;
    InMemoryPeerTrustStore clientTrust;
    REQUIRE(serverTrust.verifyOrPin("client-node", std::string(64, '0'), true).has_value());
    auto pair = runHandshake(serverConfig, clientConfig, serverTrust, clientTrust);

    REQUIRE_FALSE(pair.client.has_value());
    REQUIRE_FALSE(pair.server.has_value());
    CHECK(pair.client.error().code == yams::ErrorCode::Unauthorized);
    CHECK(pair.server.error().code == yams::ErrorCode::Unauthorized);
    CHECK(serverTrust.pinnedPin("client-node") == std::string(64, '0'));
}
