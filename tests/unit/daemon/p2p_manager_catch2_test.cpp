// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// pi-lens-ignore: fatal error
#include <catch2/catch_test_macros.hpp>

// pi-lens-ignore: fatal error
#include <yams/daemon/p2p/p2p_manager.h>
#include <yams/daemon/p2p/p2p_transport.h>
#include <yams/memory_sync/memory_sync_service.h>
#include <yams/memory_sync/writer_auth.h>
#include <yams/storage/storage_backend.h>

#include "common/p2p_test_helpers.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using yams::daemon::p2p::P2pManager;
using yams::daemon::p2p::P2pManagerOptions;
using yams::memory_sync::MemorySyncService;

namespace {
using namespace yams::p2p_test;

// Distinct key pair per writer identity so tests catch origin-binding regressions: a
// record claiming origin "server-node" but signed with "client-node"'s key must fail
// verification. The fixed set mirrors the writer ids used across these tests.
struct TestWriterKeys {
    yams::memory_sync::WriterKeyPair node;
    yams::memory_sync::WriterKeyPair clientNode;
    yams::memory_sync::WriterKeyPair serverNode;
};

const TestWriterKeys& testWriterKeys() {
    static const TestWriterKeys keys = [] {
        auto generate = [] {
            auto generated = yams::memory_sync::generateWriterKeyPair();
            if (!generated) {
                throw std::runtime_error(generated.error().message);
            }
            return generated.value();
        };
        return TestWriterKeys{generate(), generate(), generate()};
    }();
    return keys;
}

const yams::memory_sync::WriterKeyPair* keyForWriter(const std::string& writerId) {
    const auto& keys = testWriterKeys();
    if (writerId == "node") {
        return &keys.node;
    }
    if (writerId == "client-node") {
        return &keys.clientNode;
    }
    if (writerId == "server-node") {
        return &keys.serverNode;
    }
    return nullptr;
}

std::shared_ptr<const yams::memory_sync::WriterAuthenticator>
testWriterAuthenticator(const std::string& writerId, const std::string& corpusId,
                        std::uint64_t corpusEpoch) {
    const auto& keys = testWriterKeys();
    const auto* selfKey = keyForWriter(writerId);
    if (selfKey == nullptr) {
        throw std::runtime_error("test writer id not in the fixed test key set: " + writerId);
    }
    yams::memory_sync::WriterAuthConfig config;
    config.required = true;
    config.localWriterId = writerId;
    config.localKeyId = writerId + "-v1";
    config.localPrivateKeyPem = selfKey->privateKeyPem;
    // Trust every known writer with its own distinct key; each record's origin is bound to
    // the key that signed it, so cross-writer forgery fails verification.
    const auto trust = [&](const std::string& id, const yams::memory_sync::WriterKeyPair& key) {
        config.trustedKeys.push_back(
            yams::memory_sync::TrustedWriterKey{id, id + "-v1", key.publicKeyPem, false});
    };
    trust("node", keys.node);
    trust("client-node", keys.clientNode);
    trust("server-node", keys.serverNode);
    auto authenticator =
        yams::memory_sync::WriterAuthenticator::create(std::move(config), corpusId, corpusEpoch);
    if (!authenticator) {
        throw std::runtime_error(authenticator.error().message);
    }
    return authenticator.value();
}

struct MemorySyncConfig : yams::memory_sync::MemorySyncConfig {
    explicit MemorySyncConfig(std::string writerId, std::uint32_t intervalMs = 5000,
                              std::string corpus = "local-test-corpus", std::uint64_t epoch = 1)
        : yams::memory_sync::MemorySyncConfig(writerId, intervalMs, corpus, epoch, {}, false,
                                              testWriterAuthenticator(writerId, corpus, epoch),
                                              "manager-test-control-scope") {}
};

std::unique_ptr<yams::storage::FilesystemBackend> backend(const std::filesystem::path& path) {
    yams::storage::BackendConfig config;
    config.type = "filesystem";
    config.localPath = path;
    auto result = std::make_unique<yams::storage::FilesystemBackend>();
    REQUIRE(result->initialize(config).has_value());
    return result;
}

P2pManagerOptions options(std::string nodeId, const std::filesystem::path& root,
                          std::string privateKeyPem,
                          std::chrono::milliseconds reconnectInterval = std::chrono::seconds(60),
                          bool allowFirstContact = true) {
    return P2pManagerOptions{.nodeId = std::move(nodeId),
                             .corpusId = "manager-corpus",
                             .corpusEpoch = 1,
                             .privateKeyPem = std::move(privateKeyPem),
                             .databasePath = root / "yams.db",
                             .listenHost = "127.0.0.1",
                             .listenPort = 0,
                             .allowFirstContact = allowFirstContact,
                             .maxPeers = 8,
                             .reconnectInterval = reconnectInterval,
                             .timeout = 3s,
                             .sessionTimeout = 10s};
}

#if YAMS_DAEMON_TEST_HOOKS_ENABLED
struct ReconnectLoopHookGuard {
    ~ReconnectLoopHookGuard() { P2pManager::testing_setReconnectLoopHook({}); }
};
#endif

} // namespace

TEST_CASE("P2P connection strings are strict and IPv6 aware", "[daemon][p2p][manager]") {
    auto basic = yams::daemon::p2p::parseP2pConnectionString("ghost.example:9721");
    REQUIRE(basic.has_value());
    CHECK(basic.value().host == "ghost.example");
    CHECK(basic.value().port == 9721);
    CHECK(basic.value().remember);
    CHECK(basic.value().endpoint() == "ghost.example:9721");

    const std::string pin(64, 'a');
    auto complete = yams::daemon::p2p::parseP2pConnectionString(
        "yams://[fd00::1]:443?corpus=my-corpus&epoch=7&pin=" + pin + "&remember=false");
    REQUIRE(complete.has_value());
    CHECK(complete.value().host == "fd00::1");
    CHECK(complete.value().endpoint() == "[fd00::1]:443");
    CHECK(complete.value().corpusId == "my-corpus");
    CHECK(complete.value().corpusEpoch == 7);
    CHECK(complete.value().peerPin == pin);
    CHECK_FALSE(complete.value().remember);

    for (const auto invalid : {"ghost", "ghost:0", "ghost:65536", "::1:443", "ghost:443?unknown=x",
                               "ghost:443?remember=maybe"}) {
        CAPTURE(invalid);
        CHECK_FALSE(yams::daemon::p2p::parseP2pConnectionString(invalid).has_value());
    }
}

TEST_CASE("P2P manager defaults to rejecting first contact", "[daemon][p2p][security]") {
    CHECK_FALSE(P2pManagerOptions{}.allowFirstContact);
    CHECK(P2pManagerOptions{}.sessionTimeout > P2pManagerOptions{}.timeout);
}

TEST_CASE("P2P manager rejects unauthenticated memory sync services",
          "[daemon][p2p][manager][security]") {
    TempDir dir{"unauthenticated"};
    MemorySyncService service{backend(dir.path / "ops"), yams::memory_sync::MemorySyncConfig{
                                                             "node", 60'000, "manager-corpus", 1}};
    REQUIRE(service.syncFully().has_value());
    auto key = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(key.has_value());
    auto manager =
        P2pManager::create(options("node", dir.path, key.value().privateKeyPem), service);
    REQUIRE_FALSE(manager.has_value());
    CHECK(manager.error().code == yams::ErrorCode::InvalidArgument);
}

TEST_CASE("P2P manager requires full recovery and exact memory sync identity",
          "[daemon][p2p][manager][security]") {
    TempDir dir{"readiness"};
    MemorySyncService service{backend(dir.path / "ops"),
                              MemorySyncConfig{"node", 60'000, "manager-corpus", 1}};
    auto key = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(key.has_value());
    auto managerOptions = options("node", dir.path, key.value().privateKeyPem);
    auto unrecovered = P2pManager::create(managerOptions, service);
    REQUIRE_FALSE(unrecovered.has_value());
    REQUIRE(service.syncFully().has_value());
    managerOptions.corpusId = "different-corpus";
    auto mismatched = P2pManager::create(managerOptions, service);
    REQUIRE_FALSE(mismatched.has_value());
    CHECK(mismatched.error().code == yams::ErrorCode::InvalidArgument);
}

TEST_CASE("P2P manager rejects reconnect intervals above the hard ceiling",
          "[daemon][p2p][manager][capacity]") {
    TempDir dir{"reconnect-limit"};
    MemorySyncService service{backend(dir.path / "ops"),
                              MemorySyncConfig{"node", 60'000, "manager-corpus", 1}};
    REQUIRE(service.syncFully().has_value());
    auto key = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(key.has_value());
    auto managerOptions = options("node", dir.path, key.value().privateKeyPem);
    managerOptions.reconnectInterval = yams::daemon::p2p::kP2pMaxReconnectInterval + 1ms;
    auto manager = P2pManager::create(std::move(managerOptions), service);
    REQUIRE_FALSE(manager.has_value());
    CHECK(manager.error().code == yams::ErrorCode::InvalidArgument);
}

#if YAMS_DAEMON_TEST_HOOKS_ENABLED
TEST_CASE("P2P manager contains reconnect-thread exceptions and continues",
          "[daemon][p2p][manager][reconnect][lifecycle]") {
    ReconnectLoopHookGuard guard;
    std::atomic<unsigned> calls{0};
    std::promise<void> recovered;
    auto recoveredFuture = recovered.get_future();
    std::atomic<bool> signalled{false};
    P2pManager::testing_setReconnectLoopHook([&] {
        if (calls.fetch_add(1, std::memory_order_relaxed) == 0) {
            throw std::runtime_error("injected reconnect-loop failure");
        }
        if (!signalled.exchange(true, std::memory_order_relaxed)) {
            recovered.set_value();
        }
    });

    TempDir dir{"reconnect-exception"};
    MemorySyncService service{backend(dir.path / "ops"),
                              MemorySyncConfig{"node", 60'000, "manager-corpus", 1}};
    REQUIRE(service.syncFully().has_value());
    auto key = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(key.has_value());
    auto manager =
        P2pManager::create(options("node", dir.path, key.value().privateKeyPem, 10ms), service);
    REQUIRE(manager.has_value());
    REQUIRE(manager.value()->start().has_value());
    REQUIRE(recoveredFuture.wait_for(2s) == std::future_status::ready);
    CHECK(calls.load(std::memory_order_relaxed) >= 2);
    manager.value()->stop();
}

TEST_CASE("P2P manager serializes concurrent stop callers",
          "[daemon][p2p][manager][reconnect][lifecycle]") {
    ReconnectLoopHookGuard guard;
    std::promise<void> reconnectEntered;
    auto reconnectEnteredFuture = reconnectEntered.get_future();
    std::promise<void> releaseReconnect;
    auto releaseReconnectFuture = releaseReconnect.get_future().share();
    std::atomic<bool> signalled{false};
    P2pManager::testing_setReconnectLoopHook([&] {
        if (!signalled.exchange(true, std::memory_order_relaxed)) {
            reconnectEntered.set_value();
        }
        releaseReconnectFuture.wait();
    });

    TempDir dir{"concurrent-stop"};
    MemorySyncService service{backend(dir.path / "ops"),
                              MemorySyncConfig{"node", 60'000, "manager-corpus", 1}};
    REQUIRE(service.syncFully().has_value());
    auto key = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(key.has_value());
    auto manager =
        P2pManager::create(options("node", dir.path, key.value().privateKeyPem, 10ms), service);
    REQUIRE(manager.has_value());
    REQUIRE(manager.value()->start().has_value());
    REQUIRE(reconnectEnteredFuture.wait_for(1s) == std::future_status::ready);

    auto firstStop = std::async(std::launch::async, [&] { manager.value()->stop(); });
    auto secondStop = std::async(std::launch::async, [&] { manager.value()->stop(); });
    releaseReconnect.set_value();
    firstStop.get();
    secondStop.get();
    CHECK_FALSE(manager.value()->started());
}
#endif

TEST_CASE("P2P manager rejects unknown inbound peers before delta exchange",
          "[daemon][p2p][manager][network][sqlite][security]") {
    TempDir clientDir{"unknown-client"};
    TempDir serverDir{"unknown-server"};
    MemorySyncService clientService{backend(clientDir.path / "ops"),
                                    MemorySyncConfig{"client-node", 60'000, "manager-corpus", 1}};
    MemorySyncService serverService{backend(serverDir.path / "ops"),
                                    MemorySyncConfig{"server-node", 60'000, "manager-corpus", 1}};
    REQUIRE(clientService.syncFully().has_value());
    REQUIRE(serverService.syncFully().has_value());
    REQUIRE(clientService.publish("user/from-client", bytes("client-value")).has_value());
    REQUIRE(serverService.publish("user/from-server", bytes("server-value")).has_value());

    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(clientKey.has_value());
    REQUIRE(serverKey.has_value());
    auto serverIdentity = yams::daemon::p2p::TlsIdentity::fromPrivateKeyPem(
        "server-node", serverKey.value().privateKeyPem);
    REQUIRE(serverIdentity.has_value());
    const auto serverPin = serverIdentity.value().spkiPin();

    auto client = P2pManager::create(
        options("client-node", clientDir.path, clientKey.value().privateKeyPem, 60s, false),
        clientService);
    auto server = P2pManager::create(
        options("server-node", serverDir.path, serverKey.value().privateKeyPem, 60s, false),
        serverService);
    REQUIRE(client.has_value());
    REQUIRE(server.has_value());
    REQUIRE(client.value()->enrollPeer("server-node", serverPin).has_value());
    REQUIRE(client.value()->start().has_value());
    REQUIRE(server.value()->start().has_value());

    const std::string endpoint =
        "127.0.0.1:" + std::to_string(server.value()->boundPort()) + "?pin=" + serverPin;
    auto rejected = client.value()->connect(endpoint);
    REQUIRE_FALSE(rejected.has_value());
    CHECK_FALSE(serverService.readCached("user/from-client").has_value());
    CHECK_FALSE(clientService.readCached("user/from-server").has_value());
    auto serverPeers = server.value()->peers();
    REQUIRE(serverPeers.has_value());
    CHECK(serverPeers.value().empty());

    client.value()->stop();
    server.value()->stop();
}

TEST_CASE("P2P manager converges after mutual operator enrollment",
          "[daemon][p2p][manager][network][sqlite][security]") {
    TempDir clientDir{"enrolled-client"};
    TempDir serverDir{"enrolled-server"};
    MemorySyncService clientService{backend(clientDir.path / "ops"),
                                    MemorySyncConfig{"client-node", 60'000, "manager-corpus", 1}};
    MemorySyncService serverService{backend(serverDir.path / "ops"),
                                    MemorySyncConfig{"server-node", 60'000, "manager-corpus", 1}};
    REQUIRE(clientService.syncFully().has_value());
    REQUIRE(serverService.syncFully().has_value());
    REQUIRE(clientService.publish("user/from-client", bytes("client-value")).has_value());
    REQUIRE(serverService.publish("user/from-server", bytes("server-value")).has_value());

    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(clientKey.has_value());
    REQUIRE(serverKey.has_value());
    auto clientIdentity = yams::daemon::p2p::TlsIdentity::fromPrivateKeyPem(
        "client-node", clientKey.value().privateKeyPem);
    auto serverIdentity = yams::daemon::p2p::TlsIdentity::fromPrivateKeyPem(
        "server-node", serverKey.value().privateKeyPem);
    REQUIRE(clientIdentity.has_value());
    REQUIRE(serverIdentity.has_value());

    auto client = P2pManager::create(
        options("client-node", clientDir.path, clientKey.value().privateKeyPem, 60s, false),
        clientService);
    auto server = P2pManager::create(
        options("server-node", serverDir.path, serverKey.value().privateKeyPem, 60s, false),
        serverService);
    REQUIRE(client.has_value());
    REQUIRE(server.has_value());
    REQUIRE(client.value()->start().has_value());
    REQUIRE(server.value()->start().has_value());
    REQUIRE(
        client.value()->enrollPeer("server-node", serverIdentity.value().spkiPin()).has_value());
    REQUIRE(
        server.value()->enrollPeer("client-node", clientIdentity.value().spkiPin()).has_value());
    auto localIdentity = client.value()->localIdentity();
    REQUIRE(localIdentity.has_value());
    CHECK(localIdentity.value().nodeId == "client-node");
    CHECK(localIdentity.value().spkiPin == clientIdentity.value().spkiPin());

    const std::string unpinnedEndpoint = "127.0.0.1:" + std::to_string(server.value()->boundPort());
    auto unpinned = client.value()->connect(unpinnedEndpoint);
    REQUIRE_FALSE(unpinned.has_value());
    CHECK(unpinned.error().code == yams::ErrorCode::InvalidArgument);

    const std::string endpoint = unpinnedEndpoint + "?pin=" + serverIdentity.value().spkiPin();
    auto synced = client.value()->connect(endpoint);
    REQUIRE(synced.has_value());
    CHECK(clientService.readCached("user/from-server").has_value());
    CHECK(serverService.readCached("user/from-client").has_value());
    auto serverPeers = server.value()->peers();
    REQUIRE(serverPeers.has_value());
    REQUIRE(serverPeers.value().size() == 1);
    CHECK(serverPeers.value().front().pinnedByOperator);

    client.value()->stop();
    server.value()->stop();
}

TEST_CASE("P2P manager quarantines a mismatched nonzero writer prefix before payloads",
          "[daemon][p2p][manager][network][security][commitment]") {
    TempDir clientDir{"history-client"};
    TempDir serverDir{"history-server"};
    TempDir rogueDir{"history-rogue"};
    MemorySyncService clientService{backend(clientDir.path / "ops"),
                                    MemorySyncConfig{"client-node", 60'000, "manager-corpus", 1}};
    MemorySyncService serverService{backend(serverDir.path / "ops"),
                                    MemorySyncConfig{"server-node", 60'000, "manager-corpus", 1}};
    MemorySyncService rogueWriter{backend(rogueDir.path / "ops"),
                                  MemorySyncConfig{"server-node", 60'000, "manager-corpus", 1}};
    REQUIRE(clientService.syncFully().has_value());
    REQUIRE(serverService.syncFully().has_value());
    REQUIRE(rogueWriter.syncFully().has_value());
    REQUIRE(serverService.publish("user/history", bytes("canonical-first")).has_value());
    REQUIRE(serverService.publish("user/latest", bytes("canonical-second")).has_value());
    REQUIRE(rogueWriter.publish("user/history", bytes("forked")).has_value());
    auto rogueDelta = rogueWriter.exportLocalDeltasAfter({});
    REQUIRE(rogueDelta.has_value());
    REQUIRE(clientService.applyDeltas(rogueDelta.value().deltas).has_value());
    REQUIRE(clientService.currentVersion().get("server-node") == 1);
    REQUIRE(serverService.currentVersion().get("server-node") == 2);

    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(clientKey.has_value());
    REQUIRE(serverKey.has_value());
    auto clientIdentity = yams::daemon::p2p::TlsIdentity::fromPrivateKeyPem(
        "client-node", clientKey.value().privateKeyPem);
    auto serverIdentity = yams::daemon::p2p::TlsIdentity::fromPrivateKeyPem(
        "server-node", serverKey.value().privateKeyPem);
    REQUIRE(clientIdentity.has_value());
    REQUIRE(serverIdentity.has_value());

    auto client = P2pManager::create(
        options("client-node", clientDir.path, clientKey.value().privateKeyPem, 60s, false),
        clientService);
    auto server = P2pManager::create(
        options("server-node", serverDir.path, serverKey.value().privateKeyPem, 60s, false),
        serverService);
    REQUIRE(client.has_value());
    REQUIRE(server.has_value());
    REQUIRE(
        client.value()->enrollPeer("server-node", serverIdentity.value().spkiPin()).has_value());
    REQUIRE(
        server.value()->enrollPeer("client-node", clientIdentity.value().spkiPin()).has_value());
    REQUIRE(client.value()->start().has_value());
    REQUIRE(server.value()->start().has_value());

    const std::string endpoint = "127.0.0.1:" + std::to_string(server.value()->boundPort()) +
                                 "?pin=" + serverIdentity.value().spkiPin();
    auto rejected = client.value()->connect(endpoint);
    REQUIRE_FALSE(rejected.has_value());
    CHECK(rejected.error().code == yams::ErrorCode::InvalidData);
    CHECK(clientService.replicationState().quarantinedWriters.contains("server-node"));
    CHECK_FALSE(clientService.readCached("user/history").has_value());

    REQUIRE(clientService.publish("user/after-quarantine", bytes("must-not-send")).has_value());
    auto repeated = client.value()->connect(endpoint);
    REQUIRE_FALSE(repeated.has_value());
    CHECK_FALSE(serverService.readCached("user/after-quarantine").has_value());

    client.value()->stop();
    server.value()->stop();
}

TEST_CASE("P2P manager connects, converges, and remembers peers",
          "[daemon][p2p][manager][network][sqlite]") {
    TempDir clientDir{"client"};
    TempDir serverDir{"server"};
    MemorySyncService clientService{backend(clientDir.path / "ops"),
                                    MemorySyncConfig{"client-node", 60'000, "manager-corpus", 1}};
    MemorySyncService serverService{backend(serverDir.path / "ops"),
                                    MemorySyncConfig{"server-node", 60'000, "manager-corpus", 1}};
    REQUIRE(clientService.syncFully().has_value());
    REQUIRE(serverService.syncFully().has_value());
    REQUIRE(clientService.publish("user/from-client", bytes("client-value")).has_value());
    REQUIRE(serverService.publish("user/from-server", bytes("server-value")).has_value());

    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(clientKey.has_value());
    REQUIRE(serverKey.has_value());
    auto client = P2pManager::create(
        options("client-node", clientDir.path, clientKey.value().privateKeyPem), clientService);
    auto server = P2pManager::create(
        options("server-node", serverDir.path, serverKey.value().privateKeyPem), serverService);
    REQUIRE(client.has_value());
    REQUIRE(server.has_value());
    REQUIRE(client.value()->start().has_value());
    REQUIRE(server.value()->start().has_value());

    const std::string endpoint = "127.0.0.1:" + std::to_string(server.value()->boundPort());
    auto synced = client.value()->connect(endpoint + "?corpus=manager-corpus&epoch=1");
    REQUIRE(synced.has_value());
    CHECK(synced.value().peerNodeId == "server-node");
    CHECK(synced.value().deltasSent == 1);
    CHECK(synced.value().deltasReceived == 1);
    REQUIRE(clientService.readCached("user/from-server").has_value());
    REQUIRE(serverService.readCached("user/from-client").has_value());

    auto peers = client.value()->peers();
    REQUIRE(peers.has_value());
    REQUIRE(peers.value().size() == 1);
    CHECK(peers.value().front().nodeId == "server-node");
    CHECK(peers.value().front().endpoint == endpoint);
    CHECK(peers.value().front().remembered);

    SECTION("symmetric reconnect does not deadlock") {
        REQUIRE(clientService.publish("user/client-second", bytes("client-2")).has_value());
        REQUIRE(serverService.publish("user/server-second", bytes("server-2")).has_value());
        const std::string clientEndpoint =
            "127.0.0.1:" + std::to_string(client.value()->boundPort());
        auto clientConnect = std::async(std::launch::async, [&] {
            return client.value()->connect(endpoint + "?remember=false");
        });
        auto serverConnect = std::async(std::launch::async, [&] {
            return server.value()->connect(clientEndpoint + "?remember=false");
        });
        REQUIRE(clientConnect.wait_for(10s) == std::future_status::ready);
        REQUIRE(serverConnect.wait_for(10s) == std::future_status::ready);
        CHECK(clientConnect.get().has_value());
        CHECK(serverConnect.get().has_value());
        CHECK(clientService.readCached("user/server-second").has_value());
        CHECK(serverService.readCached("user/client-second").has_value());
    }

    REQUIRE(client.value()->disconnect("server-node").has_value());
    peers = client.value()->peers();
    REQUIRE(peers.has_value());
    REQUIRE(peers.value().size() == 1);
    CHECK_FALSE(peers.value().front().remembered);

    client.value()->stop();
    server.value()->stop();
}

TEST_CASE("P2P manager resumes operator-enrolled peers after restart",
          "[daemon][p2p][manager][network][sqlite][reconnect][security]") {
    TempDir clientDir{"resume-client"};
    TempDir serverDir{"resume-server"};
    MemorySyncService clientService{backend(clientDir.path / "ops"),
                                    MemorySyncConfig{"client-node", 60'000, "manager-corpus", 1}};
    MemorySyncService serverService{backend(serverDir.path / "ops"),
                                    MemorySyncConfig{"server-node", 60'000, "manager-corpus", 1}};
    REQUIRE(clientService.syncFully().has_value());
    REQUIRE(serverService.syncFully().has_value());
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(clientKey.has_value());
    REQUIRE(serverKey.has_value());
    auto clientIdentity = yams::daemon::p2p::TlsIdentity::fromPrivateKeyPem(
        "client-node", clientKey.value().privateKeyPem);
    auto serverIdentity = yams::daemon::p2p::TlsIdentity::fromPrivateKeyPem(
        "server-node", serverKey.value().privateKeyPem);
    REQUIRE(clientIdentity.has_value());
    REQUIRE(serverIdentity.has_value());

    auto server = P2pManager::create(
        options("server-node", serverDir.path, serverKey.value().privateKeyPem, 60s, false),
        serverService);
    auto client = P2pManager::create(
        options("client-node", clientDir.path, clientKey.value().privateKeyPem, 50ms, false),
        clientService);
    REQUIRE(server.has_value());
    REQUIRE(client.has_value());
    REQUIRE(server.value()->start().has_value());
    REQUIRE(client.value()->start().has_value());
    REQUIRE(
        client.value()->enrollPeer("server-node", serverIdentity.value().spkiPin()).has_value());
    REQUIRE(
        server.value()->enrollPeer("client-node", clientIdentity.value().spkiPin()).has_value());
    const std::string endpoint = "127.0.0.1:" + std::to_string(server.value()->boundPort()) +
                                 "?pin=" + serverIdentity.value().spkiPin();
    REQUIRE(client.value()->connect(endpoint).has_value());
    client.value()->stop();
    client.value().reset();

    REQUIRE(serverService.publish("user/after-restart", bytes("resumed")).has_value());
    client = P2pManager::create(
        options("client-node", clientDir.path, clientKey.value().privateKeyPem, 50ms, false),
        clientService);
    REQUIRE(client.has_value());
    REQUIRE(client.value()->start().has_value());
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!clientService.readCached("user/after-restart") &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(25ms);
    }
    CHECK(clientService.readCached("user/after-restart").has_value());

    client.value()->stop();
    server.value()->stop();
}
