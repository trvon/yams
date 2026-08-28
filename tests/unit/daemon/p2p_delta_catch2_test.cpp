// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// pi-lens-ignore: fatal error
#include <catch2/catch_test_macros.hpp>
// pi-lens-ignore: fatal error
#include <nlohmann/json.hpp>

#include <yams/crypto/hasher.h>
#include <yams/daemon/p2p/p2p_delta.h>
#include <yams/memory_sync/writer_auth.h>
#include <yams/storage/storage_backend.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <utility>
#include <vector>

using namespace std::chrono_literals;
using yams::daemon::p2p::DeltaExchangeOptions;
using yams::daemon::p2p::DeltaExchangeStats;
using yams::daemon::p2p::InMemoryPeerTrustStore;
using yams::daemon::p2p::P2pClientOptions;
using yams::daemon::p2p::P2pConnection;
using yams::daemon::p2p::P2pListener;
using yams::daemon::p2p::PeerHandshakeConfig;
using yams::daemon::p2p::TlsIdentity;
using yams::memory_sync::MemorySyncConfig;
using yams::memory_sync::MemorySyncService;

namespace {

std::vector<std::byte> bytes(std::string_view text) {
    std::vector<std::byte> result(text.size());
    std::memcpy(result.data(), text.data(), text.size());
    return result;
}

std::string text(const std::vector<std::byte>& data) {
    return std::string(reinterpret_cast<const char*>(data.data()), data.size());
}

std::string digest(std::span<const std::byte> data) {
    yams::crypto::SHA256Hasher hasher;
    hasher.init();
    hasher.update(data);
    return hasher.finalize();
}

struct TempDir {
    explicit TempDir(std::string name) {
        path = std::filesystem::temp_directory_path() /
               ("yams-p2p-delta-" + std::move(name) + "-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(path);
    }
    ~TempDir() {
        std::error_code error;
        std::filesystem::remove_all(path, error);
    }
    std::filesystem::path path;
};

std::unique_ptr<yams::storage::FilesystemBackend> backend(const std::filesystem::path& path) {
    yams::storage::BackendConfig config;
    config.type = "filesystem";
    config.localPath = path;
    auto result = std::make_unique<yams::storage::FilesystemBackend>();
    REQUIRE(result->initialize(config).has_value());
    return result;
}

TlsIdentity identity(std::string nodeId, const yams::memory_sync::WriterKeyPair& keyPair) {
    auto result = TlsIdentity::fromPrivateKeyPem(std::move(nodeId), keyPair.privateKeyPem);
    REQUIRE(result.has_value());
    return std::move(result.value());
}

PeerHandshakeConfig handshakeConfig(std::string nodeId, const MemorySyncService& service,
                                    bool allowFirstContact) {
    return PeerHandshakeConfig{.nodeId = std::move(nodeId),
                               .corpusId = "wire-delta-corpus",
                               .corpusEpoch = 1,
                               .localVersion = service.currentVersion(),
                               .allowFirstContact = allowFirstContact,
                               .timeout = 3s};
}

struct ExchangePair {
    yams::Result<DeltaExchangeStats> client;
    yams::Result<DeltaExchangeStats> server;
};

ExchangePair runExchange(MemorySyncService& clientService, MemorySyncService& serverService,
                         const yams::memory_sync::WriterKeyPair& clientKey,
                         const yams::memory_sync::WriterKeyPair& serverKey,
                         InMemoryPeerTrustStore& clientTrust, InMemoryPeerTrustStore& serverTrust,
                         bool allowFirstContact,
                         DeltaExchangeOptions exchangeOptions = {
                             .maxDeltasPerBatch = 2, .maxBatches = 16, .timeout = 3s}) {
    auto clientIdentity = identity("client-node", clientKey);
    auto serverIdentity = identity("server-node", serverKey);
    P2pListener listener(P2pListener::Options{.host = "127.0.0.1",
                                              .port = 0,
                                              .identity = std::move(serverIdentity),
                                              .allowedPeerPins = {},
                                              .allowUnpinnedPeers = true,
                                              .handshakeTimeout = 3s,
                                              .maxConcurrentSessions = 1});
    std::promise<yams::Result<DeltaExchangeStats>> serverDone;
    auto serverFuture = serverDone.get_future();
    const auto serverHandshake = handshakeConfig("server-node", serverService, allowFirstContact);
    listener.setHandler([&](P2pConnection channel) {
        auto handshake =
            yams::daemon::p2p::acceptPeerHandshake(channel, serverHandshake, serverTrust);
        if (!handshake) {
            serverDone.set_value(handshake.error());
            return;
        }
        serverDone.set_value(yams::daemon::p2p::acceptDeltaExchange(
            channel, serverService, handshake.value(), exchangeOptions));
    });
    REQUIRE(listener.start().has_value());

    auto channel =
        yams::daemon::p2p::p2pConnect(P2pClientOptions{.host = "127.0.0.1",
                                                       .port = listener.boundPort(),
                                                       .identity = std::move(clientIdentity),
                                                       .expectedPeerPin = {},
                                                       .allowUnpinnedPeer = true,
                                                       .connectTimeout = 3s,
                                                       .handshakeTimeout = 3s});
    REQUIRE(channel.has_value());
    auto clientHandshake = yams::daemon::p2p::initiatePeerHandshake(
        channel.value(), handshakeConfig("client-node", clientService, allowFirstContact),
        clientTrust);
    REQUIRE(clientHandshake.has_value());
    auto clientResult = yams::daemon::p2p::initiateDeltaExchange(
        channel.value(), clientService, clientHandshake.value(), exchangeOptions);
    REQUIRE(serverFuture.wait_for(5s) == std::future_status::ready);
    auto serverResult = serverFuture.get();
    listener.stop();
    return ExchangePair{.client = std::move(clientResult), .server = std::move(serverResult)};
}

} // namespace

TEST_CASE("direct P2P delta exchange rejects origin not bound to TLS peer",
          "[daemon][p2p][delta][network][security]") {
    TempDir clientDir{"forged-client"};
    TempDir serverDir{"forged-server"};
    MemorySyncService clientService{
        backend(clientDir.path), MemorySyncConfig{"client-node", 60'000, "wire-delta-corpus", 1}};
    MemorySyncService serverService{
        backend(serverDir.path), MemorySyncConfig{"server-node", 60'000, "wire-delta-corpus", 1}};
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(clientKey.has_value());
    REQUIRE(serverKey.has_value());
    auto clientIdentity = identity("client-node", clientKey.value());
    auto serverIdentity = identity("server-node", serverKey.value());
    const std::string serverPin = serverIdentity.spkiPin();
    const std::string clientPin = clientIdentity.spkiPin();

    InMemoryPeerTrustStore clientTrust;
    InMemoryPeerTrustStore serverTrust;
    std::promise<yams::Result<DeltaExchangeStats>> serverDone;
    auto serverFuture = serverDone.get_future();
    P2pListener listener(P2pListener::Options{.host = "127.0.0.1",
                                              .port = 0,
                                              .identity = std::move(serverIdentity),
                                              .allowedPeerPins = {clientPin},
                                              .allowUnpinnedPeers = false,
                                              .handshakeTimeout = 3s,
                                              .maxConcurrentSessions = 1});
    listener.setHandler([&](P2pConnection channel) {
        auto handshake = yams::daemon::p2p::acceptPeerHandshake(
            channel, handshakeConfig("server-node", serverService, true), serverTrust);
        if (!handshake) {
            serverDone.set_value(handshake.error());
            return;
        }
        serverDone.set_value(yams::daemon::p2p::acceptDeltaExchange(
            channel, serverService, handshake.value(),
            DeltaExchangeOptions{.maxDeltasPerBatch = 2, .maxBatches = 4, .timeout = 3s}));
    });
    REQUIRE(listener.start().has_value());

    auto channel =
        yams::daemon::p2p::p2pConnect(P2pClientOptions{.host = "127.0.0.1",
                                                       .port = listener.boundPort(),
                                                       .identity = std::move(clientIdentity),
                                                       .expectedPeerPin = serverPin,
                                                       .allowUnpinnedPeer = false,
                                                       .connectTimeout = 3s,
                                                       .handshakeTimeout = 3s});
    REQUIRE(channel.has_value());
    auto handshake = yams::daemon::p2p::initiatePeerHandshake(
        channel.value(), handshakeConfig("client-node", clientService, true), clientTrust);
    REQUIRE(handshake.has_value());

    const auto payload = bytes("forged");
    yams::memory_sync::MemoryIndexRecord record;
    record.entryHash = digest(payload);
    record.origin = "other-writer";
    REQUIRE(record.vv.increment(record.origin));
    record.corpusId = "wire-delta-corpus";
    record.corpusEpoch = 1;
    record.operationId = yams::memory_sync::makeOperationId(record.origin, 1);
    record.logicalKey = "user/forged";
    record.recordKind = std::string(yams::memory_sync::kMemoryValueRecordKind);
    const nlohmann::json batch{{"type", "delta_batch"}, {"count", 1}, {"has_more", false}};
    const nlohmann::json wireRecord{{"type", "delta_record"},
                                    {"logical_key", record.logicalKey},
                                    {"record", record},
                                    {"payload_size", payload.size()}};
    for (const auto& json : {batch, wireRecord}) {
        const std::string encoded = json.dump();
        REQUIRE(
            channel.value()
                .writeFrame(std::span<const std::byte>(
                                reinterpret_cast<const std::byte*>(encoded.data()), encoded.size()),
                            3s)
                .has_value());
    }
    REQUIRE(channel.value()
                .writeFrame(payload, 3s, yams::daemon::p2p::kP2pMaxValuePayloadBytes)
                .has_value());

    REQUIRE(serverFuture.wait_for(5s) == std::future_status::ready);
    auto serverResult = serverFuture.get();
    REQUIRE_FALSE(serverResult.has_value());
    CHECK(serverResult.error().code == yams::ErrorCode::Unauthorized);
    CHECK(serverService.currentVersion().empty());
    CHECK_FALSE(serverService.readCached("user/forged").has_value());
    listener.stop();
}

TEST_CASE("direct P2P delta receiver rejects aggregate bytes before reading payload",
          "[daemon][p2p][delta][network][security]") {
    TempDir clientDir{"byte-cap-client"};
    TempDir serverDir{"byte-cap-server"};
    MemorySyncService clientService{
        backend(clientDir.path), MemorySyncConfig{"client-node", 60'000, "wire-delta-corpus", 1}};
    MemorySyncService serverService{
        backend(serverDir.path), MemorySyncConfig{"server-node", 60'000, "wire-delta-corpus", 1}};
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(clientKey.has_value());
    REQUIRE(serverKey.has_value());
    auto clientIdentity = identity("client-node", clientKey.value());
    auto serverIdentity = identity("server-node", serverKey.value());
    const std::string serverPin = serverIdentity.spkiPin();
    const std::string clientPin = clientIdentity.spkiPin();

    const auto payload = bytes(std::string(1024, 'x'));
    yams::memory_sync::MemoryIndexRecord record;
    record.entryHash = digest(payload);
    record.origin = "client-node";
    REQUIRE(record.vv.increment(record.origin));
    record.corpusId = "wire-delta-corpus";
    record.corpusEpoch = 1;
    record.operationId = yams::memory_sync::makeOperationId(record.origin, 1);
    record.logicalKey = "user/oversized-session";
    record.recordKind = std::string(yams::memory_sync::kMemoryValueRecordKind);
    const nlohmann::json batch{{"type", "delta_batch"}, {"count", 1}, {"has_more", false}};
    const nlohmann::json wireRecord{{"type", "delta_record"},
                                    {"logical_key", record.logicalKey},
                                    {"record", record},
                                    {"payload_size", payload.size()}};
    const auto byteLimit = batch.dump().size() + 4 + wireRecord.dump().size() + 4 + 100;

    InMemoryPeerTrustStore clientTrust;
    InMemoryPeerTrustStore serverTrust;
    std::promise<yams::Result<DeltaExchangeStats>> serverDone;
    auto serverFuture = serverDone.get_future();
    P2pListener listener(P2pListener::Options{.host = "127.0.0.1",
                                              .port = 0,
                                              .identity = std::move(serverIdentity),
                                              .allowedPeerPins = {clientPin},
                                              .allowUnpinnedPeers = false,
                                              .handshakeTimeout = 3s,
                                              .maxConcurrentSessions = 1});
    listener.setHandler([&](P2pConnection channel) {
        auto handshake = yams::daemon::p2p::acceptPeerHandshake(
            channel, handshakeConfig("server-node", serverService, true), serverTrust);
        if (!handshake) {
            serverDone.set_value(handshake.error());
            return;
        }
        serverDone.set_value(yams::daemon::p2p::acceptDeltaExchange(
            channel, serverService, handshake.value(),
            DeltaExchangeOptions{.maxDeltasPerBatch = 2,
                                 .maxBatches = 2,
                                 .maxDeltasPerSession = 2,
                                 .maxWireBytesPerSession = byteLimit,
                                 .timeout = 3s}));
    });
    REQUIRE(listener.start().has_value());

    auto channel =
        yams::daemon::p2p::p2pConnect(P2pClientOptions{.host = "127.0.0.1",
                                                       .port = listener.boundPort(),
                                                       .identity = std::move(clientIdentity),
                                                       .expectedPeerPin = serverPin,
                                                       .allowUnpinnedPeer = false,
                                                       .connectTimeout = 3s,
                                                       .handshakeTimeout = 3s});
    REQUIRE(channel.has_value());
    auto handshake = yams::daemon::p2p::initiatePeerHandshake(
        channel.value(), handshakeConfig("client-node", clientService, true), clientTrust);
    REQUIRE(handshake.has_value());
    for (const auto& json : {batch, wireRecord}) {
        const std::string encoded = json.dump();
        REQUIRE(
            channel.value()
                .writeFrame(std::span<const std::byte>(
                                reinterpret_cast<const std::byte*>(encoded.data()), encoded.size()),
                            3s)
                .has_value());
    }

    REQUIRE(serverFuture.wait_for(1s) == std::future_status::ready);
    auto serverResult = serverFuture.get();
    REQUIRE_FALSE(serverResult.has_value());
    CHECK(serverResult.error().code == yams::ErrorCode::ResourceExhausted);
    CHECK(serverService.currentVersion().empty());
    listener.stop();
}

TEST_CASE("direct P2P delta exchange enforces aggregate session limits",
          "[daemon][p2p][delta][network][security]") {
    TempDir clientDir{"bounded-client"};
    TempDir serverDir{"bounded-server"};
    MemorySyncService clientService{
        backend(clientDir.path), MemorySyncConfig{"client-node", 60'000, "wire-delta-corpus", 1}};
    MemorySyncService serverService{
        backend(serverDir.path), MemorySyncConfig{"server-node", 60'000, "wire-delta-corpus", 1}};
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(clientKey.has_value());
    REQUIRE(serverKey.has_value());
    REQUIRE(clientService.publish("user/one", bytes("one")).has_value());
    REQUIRE(clientService.publish("user/two", bytes("two")).has_value());

    InMemoryPeerTrustStore clientTrust;
    InMemoryPeerTrustStore serverTrust;
    auto exchange = runExchange(clientService, serverService, clientKey.value(), serverKey.value(),
                                clientTrust, serverTrust, true,
                                DeltaExchangeOptions{.maxDeltasPerBatch = 2,
                                                     .maxBatches = 2,
                                                     .maxDeltasPerSession = 1,
                                                     .maxWireBytesPerSession = 1024,
                                                     .timeout = 3s});

    REQUIRE_FALSE(exchange.client.has_value());
    CHECK(exchange.client.error().code == yams::ErrorCode::ResourceExhausted);
    CHECK(serverService.currentVersion().empty());
    CHECK_FALSE(serverService.readCached("user/one").has_value());
    CHECK_FALSE(serverService.readCached("user/two").has_value());
}

TEST_CASE("direct P2P delta exchange converges offline corpora and resumes from watermarks",
          "[daemon][p2p][delta][network]") {
    TempDir clientDir{"client"};
    TempDir serverDir{"server"};
    MemorySyncService clientService{
        backend(clientDir.path), MemorySyncConfig{"client-node", 60'000, "wire-delta-corpus", 1}};
    MemorySyncService serverService{
        backend(serverDir.path), MemorySyncConfig{"server-node", 60'000, "wire-delta-corpus", 1}};
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(clientKey.has_value());
    REQUIRE(serverKey.has_value());

    for (int index = 0; index < 5; ++index) {
        REQUIRE(clientService
                    .publish("user/client-" + std::to_string(index),
                             bytes("client-value-" + std::to_string(index)))
                    .has_value());
    }
    for (int index = 0; index < 3; ++index) {
        REQUIRE(serverService
                    .publish("user/server-" + std::to_string(index),
                             bytes("server-value-" + std::to_string(index)))
                    .has_value());
    }

    InMemoryPeerTrustStore clientTrust;
    InMemoryPeerTrustStore serverTrust;
    auto first = runExchange(clientService, serverService, clientKey.value(), serverKey.value(),
                             clientTrust, serverTrust, true);
    REQUIRE(first.client.has_value());
    REQUIRE(first.server.has_value());
    CHECK(first.client.value().deltasSent == 5);
    CHECK(first.client.value().deltasReceived == 3);
    CHECK(first.server.value().deltasReceived == 5);
    CHECK(first.server.value().deltasSent == 3);
    CHECK(first.client.value().batchesSent == 3);
    CHECK(first.server.value().batchesSent == 2);

    for (int index = 0; index < 5; ++index) {
        auto value = serverService.readCached("user/client-" + std::to_string(index));
        REQUIRE(value.has_value());
        CHECK(text(value.value()) == "client-value-" + std::to_string(index));
    }
    for (int index = 0; index < 3; ++index) {
        auto value = clientService.readCached("user/server-" + std::to_string(index));
        REQUIRE(value.has_value());
        CHECK(text(value.value()) == "server-value-" + std::to_string(index));
    }

    auto replay = runExchange(clientService, serverService, clientKey.value(), serverKey.value(),
                              clientTrust, serverTrust, false);
    REQUIRE(replay.client.has_value());
    REQUIRE(replay.server.has_value());
    CHECK(replay.client.value().deltasSent == 0);
    CHECK(replay.client.value().deltasReceived == 0);
    CHECK(replay.server.value().deltasSent == 0);
    CHECK(replay.server.value().deltasReceived == 0);

    REQUIRE(clientService.erase("user/client-0", "client-0").has_value());
    auto tombstone = runExchange(clientService, serverService, clientKey.value(), serverKey.value(),
                                 clientTrust, serverTrust, false);
    REQUIRE(tombstone.client.has_value());
    REQUIRE(tombstone.server.has_value());
    CHECK(tombstone.client.value().deltasSent == 1);
    CHECK_FALSE(serverService.readCached("user/client-0").has_value());
}
