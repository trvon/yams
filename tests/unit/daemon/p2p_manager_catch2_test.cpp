// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// pi-lens-ignore: fatal error
#include <catch2/catch_test_macros.hpp>

// pi-lens-ignore: fatal error
#include <yams/daemon/p2p/p2p_manager.h>
#include <yams/memory_sync/memory_sync_service.h>
#include <yams/memory_sync/writer_auth.h>
#include <yams/storage/storage_backend.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using yams::daemon::p2p::P2pManager;
using yams::daemon::p2p::P2pManagerOptions;
using yams::memory_sync::MemorySyncConfig;
using yams::memory_sync::MemorySyncService;

namespace {

struct TempDir {
    explicit TempDir(std::string_view label) {
        path = std::filesystem::temp_directory_path() /
               ("yams-p2p-manager-" + std::string(label) + "-" +
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

std::vector<std::byte> bytes(std::string_view text) {
    std::vector<std::byte> result(text.size());
    std::memcpy(result.data(), text.data(), text.size());
    return result;
}

P2pManagerOptions options(std::string nodeId, const std::filesystem::path& root,
                          std::string privateKeyPem,
                          std::chrono::milliseconds reconnectInterval = std::chrono::seconds(60)) {
    return P2pManagerOptions{.nodeId = std::move(nodeId),
                             .corpusId = "manager-corpus",
                             .corpusEpoch = 1,
                             .privateKeyPem = std::move(privateKeyPem),
                             .databasePath = root / "yams.db",
                             .listenHost = "127.0.0.1",
                             .listenPort = 0,
                             .allowFirstContact = true,
                             .maxPeers = 8,
                             .reconnectInterval = reconnectInterval,
                             .timeout = 3s};
}

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

TEST_CASE("P2P manager resumes remembered peers after restart",
          "[daemon][p2p][manager][network][sqlite][reconnect]") {
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

    auto server = P2pManager::create(
        options("server-node", serverDir.path, serverKey.value().privateKeyPem), serverService);
    auto client = P2pManager::create(
        options("client-node", clientDir.path, clientKey.value().privateKeyPem, 50ms),
        clientService);
    REQUIRE(server.has_value());
    REQUIRE(client.has_value());
    REQUIRE(server.value()->start().has_value());
    REQUIRE(client.value()->start().has_value());
    const std::string endpoint = "127.0.0.1:" + std::to_string(server.value()->boundPort());
    REQUIRE(client.value()->connect(endpoint).has_value());
    client.value()->stop();
    client.value().reset();

    REQUIRE(serverService.publish("user/after-restart", bytes("resumed")).has_value());
    client = P2pManager::create(
        options("client-node", clientDir.path, clientKey.value().privateKeyPem, 50ms),
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
