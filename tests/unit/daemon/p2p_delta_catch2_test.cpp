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

#include "../../../src/daemon/p2p/p2p_fuzz.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <functional>
#include <future>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
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

yams::Result<void> validateDeltaJson(const nlohmann::json& json,
                                     const DeltaExchangeOptions& options = {}) {
    const auto encoded = json.dump();
    return yams::daemon::p2p::detail::validateDeltaControlFrame(
        std::span<const std::byte>(reinterpret_cast<const std::byte*>(encoded.data()),
                                   encoded.size()),
        options);
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

class InMemoryBackend final : public yams::storage::IStorageBackend {
public:
    yams::Result<void> initialize(const yams::storage::BackendConfig&) override { return {}; }

    yams::Result<void> store(std::string_view key, std::span<const std::byte> data) override {
        std::lock_guard<std::mutex> lock(mutex_);
        objects_[std::string(key)] = std::vector<std::byte>(data.begin(), data.end());
        return {};
    }

    yams::Result<std::vector<std::byte>> retrieve(std::string_view key) const override {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto found = objects_.find(std::string(key));
        if (found == objects_.end()) {
            return yams::Error{yams::ErrorCode::NotFound, "test object is absent"};
        }
        return found->second;
    }

    yams::Result<bool> exists(std::string_view key) const override {
        std::lock_guard<std::mutex> lock(mutex_);
        return objects_.contains(std::string(key));
    }

    yams::Result<void> remove(std::string_view key) override {
        std::lock_guard<std::mutex> lock(mutex_);
        objects_.erase(std::string(key));
        return {};
    }

    yams::Result<std::vector<std::string>> list(std::string_view prefix = "") const override {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::string> result;
        for (const auto& [key, _] : objects_) {
            if (key.starts_with(prefix)) {
                result.push_back(key);
            }
        }
        return result;
    }

    yams::Result<yams::StorageStats> getStats() const override { return yams::StorageStats{}; }

    std::future<yams::Result<void>> storeAsync(std::string_view key,
                                               std::span<const std::byte> data) override {
        auto ownedKey = std::string(key);
        auto ownedData = std::vector<std::byte>(data.begin(), data.end());
        return std::async(std::launch::deferred,
                          [this, key = std::move(ownedKey), data = std::move(ownedData)] {
                              return store(key, data);
                          });
    }

    std::future<yams::Result<std::vector<std::byte>>>
    retrieveAsync(std::string_view key) const override {
        auto ownedKey = std::string(key);
        return std::async(std::launch::deferred,
                          [this, key = std::move(ownedKey)] { return retrieve(key); });
    }

    std::string getType() const override { return "test-memory"; }
    bool isRemote() const override { return false; }
    yams::Result<void> flush() override { return {}; }

private:
    mutable std::mutex mutex_;
    std::map<std::string, std::vector<std::byte>> objects_;
};

std::unique_ptr<yams::storage::FilesystemBackend> backend(const std::filesystem::path& path) {
    yams::storage::BackendConfig config;
    config.type = "filesystem";
    config.localPath = path;
    auto result = std::make_unique<yams::storage::FilesystemBackend>();
    REQUIRE(result->initialize(config).has_value());
    return result;
}

std::shared_ptr<const yams::memory_sync::WriterAuthenticator>
writerAuth(std::string nodeId, std::string keyId, const yams::memory_sync::WriterKeyPair& keyPair,
           std::vector<yams::memory_sync::TrustedWriterKey> trusted) {
    yams::memory_sync::WriterAuthConfig config;
    config.required = true;
    config.localWriterId = std::move(nodeId);
    config.localKeyId = std::move(keyId);
    config.localPrivateKeyPem = keyPair.privateKeyPem;
    config.trustedKeys = std::move(trusted);
    auto result =
        yams::memory_sync::WriterAuthenticator::create(std::move(config), "wire-delta-corpus", 1);
    REQUIRE(result.has_value());
    return result.value();
}

TlsIdentity identity(std::string nodeId, const yams::memory_sync::WriterKeyPair& keyPair) {
    auto result = TlsIdentity::fromPrivateKeyPem(std::move(nodeId), keyPair.privateKeyPem);
    REQUIRE(result.has_value());
    return std::move(result.value());
}

PeerHandshakeConfig handshakeConfig(std::string nodeId, MemorySyncService& service,
                                    bool allowFirstContact) {
    const auto state = service.replicationState();
    return PeerHandshakeConfig{
        .nodeId = std::move(nodeId),
        .corpusId = "wire-delta-corpus",
        .corpusEpoch = 1,
        .localVersion = state.version,
        .localCommitments = state.commitments,
        .localQuarantinedWriters = state.quarantinedWriters,
        .resolveLocalCommitment =
            [&service](std::uint64_t counter) { return service.localHistoryCommitmentAt(counter); },
        .resolveLocalWindow =
            [&service](std::uint64_t peerCounter, std::size_t maxRecords,
                       std::size_t maxWireBytes) {
                return service.localHistoryWindowAfter(peerCounter, maxRecords, maxWireBytes);
            },
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
                         DeltaExchangeOptions exchangeOptions = {.maxDeltasPerBatch = 2,
                                                                 .maxBatches = 16,
                                                                 .timeout = 3s},
                         std::function<void(PeerHandshakeConfig&)> mutateServerHandshake = {}) {
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
    const auto batchCapacity = exchangeOptions.maxBatches > exchangeOptions.maxDeltasPerSession /
                                                                exchangeOptions.maxDeltasPerBatch
                                   ? exchangeOptions.maxDeltasPerSession
                                   : exchangeOptions.maxBatches * exchangeOptions.maxDeltasPerBatch;
    const auto writerAdvance = std::min({exchangeOptions.maxDeltasPerSession, batchCapacity,
                                         yams::daemon::p2p::kMaxP2pWriterAdvance});
    auto serverHandshake = handshakeConfig("server-node", serverService, allowFirstContact);
    serverHandshake.maxWriterAdvance = writerAdvance;
    serverHandshake.maxWriterWindowBytes = exchangeOptions.maxWireBytesPerSession;
    if (mutateServerHandshake) {
        mutateServerHandshake(serverHandshake);
    }
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
    auto clientConfig = handshakeConfig("client-node", clientService, allowFirstContact);
    clientConfig.maxWriterAdvance = writerAdvance;
    clientConfig.maxWriterWindowBytes = exchangeOptions.maxWireBytesPerSession;
    auto clientHandshake =
        yams::daemon::p2p::initiatePeerHandshake(channel.value(), clientConfig, clientTrust);
    REQUIRE(clientHandshake.has_value());
    auto clientResult = yams::daemon::p2p::initiateDeltaExchange(
        channel.value(), clientService, clientHandshake.value(), exchangeOptions);
    REQUIRE(serverFuture.wait_for(5s) == std::future_status::ready);
    auto serverResult = serverFuture.get();
    listener.stop();
    return ExchangePair{.client = std::move(clientResult), .server = std::move(serverResult)};
}

} // namespace

TEST_CASE("direct P2P delta fuzz seam enforces bounded control messages",
          "[daemon][p2p][delta][fuzz]") {
    CHECK(validateDeltaJson(
              nlohmann::json{{"type", "delta_batch"}, {"count", 1}, {"has_more", false}})
              .has_value());
    CHECK(validateDeltaJson(nlohmann::json{{"type", "replication_mode"}, {"mode", "delta"}})
              .has_value());
    CHECK_FALSE(
        validateDeltaJson(nlohmann::json{{"type", "delta_batch"}, {"count", 0}, {"has_more", true}})
            .has_value());
    CHECK_FALSE(
        validateDeltaJson(nlohmann::json{{"type", "delta_record"}, {"payload_size", 1ULL << 40}})
            .has_value());
    nlohmann::json snapshot{{"type", "snapshot_begin"},
                            {"witness", "node-a"},
                            {"frontier", {{"counters_", nlohmann::json::object()}}},
                            {"commitments", nlohmann::json::array()},
                            {"record_count", 0},
                            {"payload_bytes", 0},
                            {"root_digest", std::string(64, '0')},
                            {"witness_key_id", "node-a-v1"},
                            {"witness_algorithm", "Ed25519"},
                            {"witness_signature", "AA=="}};
    CHECK(validateDeltaJson(snapshot).has_value());
    snapshot["record_count"] = DeltaExchangeOptions{}.maxSnapshotRecords + 1;
    CHECK_FALSE(validateDeltaJson(snapshot).has_value());
    CHECK_FALSE(validateDeltaJson(nlohmann::json{{"type", "unknown"}}).has_value());
}

TEST_CASE("direct P2P cold bootstrap installs an authenticated multiwriter snapshot",
          "[daemon][p2p][delta][cold-bootstrap][network][security]") {
    TempDir clientDir{"cold-client"};
    TempDir serverDir{"cold-server"};
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    auto foreignKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(clientKey.has_value());
    REQUIRE(serverKey.has_value());
    REQUIRE(foreignKey.has_value());
    const std::vector<yams::memory_sync::TrustedWriterKey> trust = {
        {"client-node", "client-v1", clientKey.value().publicKeyPem, false},
        {"server-node", "server-v1", serverKey.value().publicKeyPem, false},
        {"foreign-node", "foreign-v1", foreignKey.value().publicKeyPem, false},
    };

    auto foreignBackend = backend(serverDir.path);
    yams::memory_sync::MemorySyncLoop foreign{
        *foreignBackend,
        "foreign-node",
        "wire-delta-corpus",
        1,
        false,
        {},
        {},
        {},
        writerAuth("foreign-node", "foreign-v1", foreignKey.value(), trust)};
    REQUIRE(foreign.publish("user/foreign", bytes("foreign-value")).has_value());

    MemorySyncService serverService{
        backend(serverDir.path),
        MemorySyncConfig{"server-node",
                         60'000,
                         "wire-delta-corpus",
                         1,
                         {},
                         false,
                         writerAuth("server-node", "server-v1", serverKey.value(), trust)}};
    REQUIRE(serverService.syncFully().has_value());
    REQUIRE(serverService.publish("user/server", bytes("server-value")).has_value());
    REQUIRE(serverService.syncFully().has_value());
    MemorySyncService clientService{
        backend(clientDir.path),
        MemorySyncConfig{"client-node",
                         60'000,
                         "wire-delta-corpus",
                         1,
                         {},
                         false,
                         writerAuth("client-node", "client-v1", clientKey.value(), trust)}};

    InMemoryPeerTrustStore clientTrust;
    InMemoryPeerTrustStore serverTrust;
    const auto refused = runExchange(clientService, serverService, clientKey.value(),
                                     serverKey.value(), clientTrust, serverTrust, true);
    CHECK_FALSE((refused.client.has_value() && refused.server.has_value()));
    CHECK(clientService.currentVersion().empty());

    // A prior TOFU session remains legacy first-contact trust and must never become operator
    // enrollment merely because a later handshake recognizes the same pin.
    const auto exchange = runExchange(clientService, serverService, clientKey.value(),
                                      serverKey.value(), clientTrust, serverTrust, false);
    CHECK_FALSE((exchange.client.has_value() && exchange.server.has_value()));
    CHECK(clientService.currentVersion().empty());

    const auto serverPin = identity("server-node", serverKey.value()).spkiPin();
    const auto clientPin = identity("client-node", clientKey.value()).spkiPin();
    REQUIRE(clientTrust.enrollOperatorPeer("server-node", serverPin).has_value());
    REQUIRE(serverTrust.enrollOperatorPeer("client-node", clientPin).has_value());
    const auto enrolled = runExchange(clientService, serverService, clientKey.value(),
                                      serverKey.value(), clientTrust, serverTrust, false);
    REQUIRE(enrolled.client.has_value());
    REQUIRE(enrolled.server.has_value());
    CHECK(enrolled.client.value().snapshotsReceived == 1);
    CHECK(enrolled.server.value().snapshotsSent == 1);
    REQUIRE(clientService.readCached("user/foreign").has_value());
    REQUIRE(clientService.readCached("user/server").has_value());
    CHECK(text(clientService.readCached("user/foreign").value()) == "foreign-value");
    CHECK(text(clientService.readCached("user/server").value()) == "server-value");
    CHECK(clientService.currentVersion().counters() == serverService.currentVersion().counters());

    REQUIRE(serverService.publish("user/continuation", bytes("after-snapshot")).has_value());
    const auto continued = runExchange(clientService, serverService, clientKey.value(),
                                       serverKey.value(), clientTrust, serverTrust, false);
    REQUIRE(continued.client.has_value());
    REQUIRE(continued.server.has_value());
    CHECK(continued.client.value().snapshotsReceived == 0);
    REQUIRE(clientService.readCached("user/continuation").has_value());
    CHECK(text(clientService.readCached("user/continuation").value()) == "after-snapshot");
}

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
    const nlohmann::json mode{{"type", "replication_mode"}, {"mode", "delta"}};
    const nlohmann::json batch{{"type", "delta_batch"}, {"count", 1}, {"has_more", false}};
    const nlohmann::json wireRecord{{"type", "delta_record"},
                                    {"logical_key", record.logicalKey},
                                    {"record", record},
                                    {"payload_size", payload.size()}};
    for (const auto& json : {mode, batch, wireRecord}) {
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
    const nlohmann::json mode{{"type", "replication_mode"}, {"mode", "delta"}};
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
    for (const auto& json : {mode, batch, wireRecord}) {
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

TEST_CASE("direct P2P delta exchange rejects options above protocol hard bounds",
          "[daemon][p2p][delta][network][limits]") {
    TempDir clientDir{"invalid-limits-client"};
    TempDir serverDir{"invalid-limits-server"};
    MemorySyncService clientService{
        backend(clientDir.path), MemorySyncConfig{"client-node", 60'000, "wire-delta-corpus", 1}};
    MemorySyncService serverService{
        backend(serverDir.path), MemorySyncConfig{"server-node", 60'000, "wire-delta-corpus", 1}};
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(clientKey.has_value());
    REQUIRE(serverKey.has_value());

    InMemoryPeerTrustStore clientTrust;
    InMemoryPeerTrustStore serverTrust;
    auto exchanged = runExchange(
        clientService, serverService, clientKey.value(), serverKey.value(), clientTrust,
        serverTrust, true,
        DeltaExchangeOptions{.maxDeltasPerBatch = 1,
                             .maxBatches = 1,
                             .maxDeltasPerSession = std::numeric_limits<std::size_t>::max(),
                             .maxWireBytesPerSession = 1024,
                             .timeout = 3s});
    REQUIRE_FALSE(exchanged.client.has_value());
    CHECK(exchanged.client.error().code == yams::ErrorCode::InvalidArgument);
}

TEST_CASE("direct P2P delta exchange resumes across aggregate session limits",
          "[daemon][p2p][delta][network][security][continuation]") {
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
    REQUIRE(clientService.publish("user/three", bytes("three")).has_value());

    InMemoryPeerTrustStore clientTrust;
    InMemoryPeerTrustStore serverTrust;
    const DeltaExchangeOptions options{.maxDeltasPerBatch = 2,
                                       .maxBatches = 2,
                                       .maxDeltasPerSession = 1,
                                       .maxWireBytesPerSession = 2048,
                                       .timeout = 3s};

    for (std::uint64_t expectedCounter = 1; expectedCounter <= 3; ++expectedCounter) {
        auto exchange =
            runExchange(clientService, serverService, clientKey.value(), serverKey.value(),
                        clientTrust, serverTrust, expectedCounter == 1, options);
        REQUIRE(exchange.client.has_value());
        REQUIRE(exchange.server.has_value());
        CHECK(exchange.client.value().deltasSent == 1);
        CHECK(exchange.server.value().deltasReceived == 1);
        CHECK(serverService.currentVersion().get("client-node") == expectedCounter);
    }

    REQUIRE(serverService.readCached("user/one").has_value());
    REQUIRE(serverService.readCached("user/two").has_value());
    REQUIRE(serverService.readCached("user/three").has_value());
    auto converged = runExchange(clientService, serverService, clientKey.value(), serverKey.value(),
                                 clientTrust, serverTrust, false, options);
    REQUIRE(converged.client.has_value());
    REQUIRE(converged.server.has_value());
    CHECK(converged.client.value().deltasSent == 0);
    CHECK(converged.server.value().deltasReceived == 0);
}

TEST_CASE("direct P2P catch-up advances beyond 1024 retained operations",
          "[daemon][p2p][delta][network][continuation][large-history][.slow]") {
    MemorySyncService clientService{
        std::make_unique<InMemoryBackend>(),
        MemorySyncConfig{"client-node", 60'000, "wire-delta-corpus", 1}};
    MemorySyncService serverService{
        std::make_unique<InMemoryBackend>(),
        MemorySyncConfig{"server-node", 60'000, "wire-delta-corpus", 1}};
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(clientKey.has_value());
    REQUIRE(serverKey.has_value());
    for (int index = 0; index < 1025; ++index) {
        REQUIRE(
            clientService
                .publish("bulk/" + std::to_string(index), bytes("value-" + std::to_string(index)))
                .has_value());
    }

    InMemoryPeerTrustStore clientTrust;
    InMemoryPeerTrustStore serverTrust;
    const DeltaExchangeOptions options{.maxDeltasPerBatch = 128,
                                       .maxBatches = 8,
                                       .maxDeltasPerSession = 1024,
                                       .maxWireBytesPerSession = 8 * 1024 * 1024,
                                       .timeout = 10s};
    auto first = runExchange(clientService, serverService, clientKey.value(), serverKey.value(),
                             clientTrust, serverTrust, true, options);
    REQUIRE(first.client.has_value());
    REQUIRE(first.server.has_value());
    CHECK(first.client.value().deltasSent == 1024);
    CHECK(serverService.currentVersion().get("client-node") == 1024);
    CHECK_FALSE(serverService.readCached("bulk/1024").has_value());

    auto resumed = runExchange(clientService, serverService, clientKey.value(), serverKey.value(),
                               clientTrust, serverTrust, false, options);
    REQUIRE(resumed.client.has_value());
    REQUIRE(resumed.server.has_value());
    CHECK(resumed.client.value().deltasSent == 1);
    CHECK(serverService.currentVersion().get("client-node") == 1025);
    REQUIRE(serverService.readCached("bulk/1024").has_value());
}

TEST_CASE("direct P2P bounded continuation survives receiver restart",
          "[daemon][p2p][delta][network][continuation][restart]") {
    TempDir clientDir{"restart-client"};
    TempDir serverDir{"restart-server"};
    MemorySyncService clientService{
        backend(clientDir.path), MemorySyncConfig{"client-node", 60'000, "wire-delta-corpus", 1}};
    auto serverService = std::make_unique<MemorySyncService>(
        backend(serverDir.path), MemorySyncConfig{"server-node", 60'000, "wire-delta-corpus", 1});
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(clientKey.has_value());
    REQUIRE(serverKey.has_value());
    REQUIRE(clientService.publish("user/one", bytes("one")).has_value());
    REQUIRE(clientService.publish("user/two", bytes("two")).has_value());

    InMemoryPeerTrustStore clientTrust;
    InMemoryPeerTrustStore serverTrust;
    const DeltaExchangeOptions options{.maxDeltasPerBatch = 1,
                                       .maxBatches = 1,
                                       .maxDeltasPerSession = 1,
                                       .maxWireBytesPerSession = 2048,
                                       .timeout = 3s};
    auto first = runExchange(clientService, *serverService, clientKey.value(), serverKey.value(),
                             clientTrust, serverTrust, true, options);
    REQUIRE(first.client.has_value());
    REQUIRE(first.server.has_value());
    CHECK(serverService->currentVersion().get("client-node") == 1);

    serverService.reset();
    serverService = std::make_unique<MemorySyncService>(
        backend(serverDir.path), MemorySyncConfig{"server-node", 60'000, "wire-delta-corpus", 1});
    REQUIRE(serverService->syncFully().has_value());
    auto resumed = runExchange(clientService, *serverService, clientKey.value(), serverKey.value(),
                               clientTrust, serverTrust, false, options);
    REQUIRE(resumed.client.has_value());
    REQUIRE(resumed.server.has_value());
    CHECK(serverService->currentVersion().get("client-node") == 2);
    REQUIRE(serverService->readCached("user/one").has_value());
    REQUIRE(serverService->readCached("user/two").has_value());
}

TEST_CASE("direct P2P delta stages a false advertised frontier before visibility",
          "[daemon][p2p][delta][network][commitment][security]") {
    TempDir clientDir{"false-frontier-client"};
    TempDir serverDir{"false-frontier-server"};
    MemorySyncService clientService{
        backend(clientDir.path), MemorySyncConfig{"client-node", 60'000, "wire-delta-corpus", 1}};
    MemorySyncService serverService{
        backend(serverDir.path), MemorySyncConfig{"server-node", 60'000, "wire-delta-corpus", 1}};
    REQUIRE(serverService.publish("user/unverified", bytes("must-stay-staged")).has_value());
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(clientKey.has_value());
    REQUIRE(serverKey.has_value());

    InMemoryPeerTrustStore clientTrust;
    InMemoryPeerTrustStore serverTrust;
    auto exchanged =
        runExchange(clientService, serverService, clientKey.value(), serverKey.value(), clientTrust,
                    serverTrust, true,
                    DeltaExchangeOptions{.maxDeltasPerBatch = 2, .maxBatches = 16, .timeout = 3s},
                    [](PeerHandshakeConfig& handshake) {
                        handshake.localCommitments.at("server-node").digest = std::string(64, 'f');
                    });
    REQUIRE_FALSE(exchanged.client.has_value());
    REQUIRE_FALSE(exchanged.server.has_value());
    CHECK(clientService.replicationState().quarantinedWriters.contains("server-node"));
    CHECK_FALSE(clientService.readCached("user/unverified").has_value());
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
