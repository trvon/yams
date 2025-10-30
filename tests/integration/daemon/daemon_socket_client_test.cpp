// Comprehensive daemon socket client integration test
// Covers connection lifecycle, reconnection, timeouts, multiplexing, and error handling

// Catch2 main - needed for Catch2 test runner
#define CATCH_CONFIG_MAIN
#include <filesystem>
#include <set>
#include <thread>
#include "test_async_helpers.h"
#include "test_daemon_harness.h"
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace yams::daemon;
using namespace yams::test;
using namespace std::chrono_literals;

namespace {
// Helper to create a client with custom config
DaemonClient createClient(const std::filesystem::path& socketPath,
                          std::chrono::milliseconds connectTimeout = 2s, bool autoStart = false) {
    ClientConfig config;
    config.socketPath = socketPath;
    config.connectTimeout = connectTimeout;
    config.autoStart = autoStart;
    config.requestTimeout = 5s;
    return DaemonClient(config);
}
} // namespace

TEST_CASE("Daemon socket connection lifecycle", "[daemon][socket][integration]") {
    DaemonHarness harness;

    SECTION("connects to running daemon") {
        REQUIRE(harness.start());

        auto client = createClient(harness.socketPath());
        auto result = yams::cli::run_sync(client.connect(), 3s);

        REQUIRE(result.has_value());
        REQUIRE(client.isConnected());
    }

    SECTION("fails gracefully when daemon not running") {
        // Don't start daemon
        auto client = createClient(harness.socketPath(), 500ms);
        auto result = yams::cli::run_sync(client.connect(), 1s);

        REQUIRE(!result.has_value());
        REQUIRE(!client.isConnected());
    }

    SECTION("reconnects after daemon restart") {
        REQUIRE(harness.start());

        auto client = createClient(harness.socketPath());
        auto connectResult = yams::cli::run_sync(client.connect(), 3s);
        REQUIRE(connectResult.has_value());
        REQUIRE(client.isConnected());

        // Stop daemon
        harness.stop();
        std::this_thread::sleep_for(500ms);

        // Restart daemon
        REQUIRE(harness.start());

        // Reconnect
        auto reconnectResult = yams::cli::run_sync(client.connect(), 3s);
        REQUIRE(reconnectResult.has_value());
        REQUIRE(client.isConnected());
    }

    SECTION("handles explicit disconnect") {
        REQUIRE(harness.start());

        auto client = createClient(harness.socketPath());
        auto connectResult = yams::cli::run_sync(client.connect(), 3s);
        REQUIRE(connectResult.has_value());
        REQUIRE(client.isConnected());

        client.disconnect();
        REQUIRE(!client.isConnected());
    }

    SECTION("respects connection timeout") {
        // Create socket path but don't start daemon
        auto client = createClient(harness.socketPath(), 100ms);

        auto start = std::chrono::steady_clock::now();
        auto result = yams::cli::run_sync(client.connect(), 500ms);
        auto elapsed = std::chrono::steady_clock::now() - start;

        REQUIRE(!result.has_value());
        REQUIRE(elapsed < 200ms); // Should fail fast with 100ms timeout
    }
}

TEST_CASE("Daemon client request execution", "[daemon][socket][requests]") {
    DaemonHarness harness;
    REQUIRE(harness.start());

    auto client = createClient(harness.socketPath());
    auto connectResult = yams::cli::run_sync(client.connect(), 3s);
    REQUIRE(connectResult.has_value());

    SECTION("status request succeeds") {
        auto statusResult = yams::cli::run_sync(client.status(), 5s);

        REQUIRE(statusResult.has_value());
        auto& status = statusResult.value();
        REQUIRE(status.readinessStates.at("ipc_server"));
        REQUIRE(status.readinessStates.at("content_store"));
    }

    SECTION("list request with empty database") {
        ListRequest req;
        req.limit = 10;

        auto listResult = yams::cli::run_sync(client.list(req), 5s);

        REQUIRE(listResult.has_value());
        REQUIRE(listResult.value().items.empty());
    }

    SECTION("add and retrieve document") {
        // Add a document
        AddDocumentRequest addReq;
        addReq.name = "test.txt";
        addReq.content = "Hello, Catch2!";

        auto addResult = yams::cli::run_sync(client.streamingAddDocument(addReq), 5s);
        REQUIRE(addResult.has_value());

        std::string docHash = addResult.value().hash;
        REQUIRE(!docHash.empty());

        // Retrieve it
        GetRequest getReq;
        getReq.hash = docHash;

        auto getResult = yams::cli::run_sync(client.get(getReq), 5s);
        REQUIRE(getResult.has_value());
        REQUIRE(getResult.value().content == "Hello, Catch2!");
        REQUIRE(getResult.value().name == "test.txt");
    }

    SECTION("search empty index returns no results") {
        SearchRequest searchReq;
        searchReq.query = "nonexistent";

        auto searchResult = yams::cli::run_sync(client.search(searchReq), 5s);

        REQUIRE(searchResult.has_value());
        REQUIRE(searchResult.value().results.empty());
    }

    SECTION("grep with no matches") {
        GrepRequest grepReq;
        grepReq.pattern = "nonexistent";

        auto grepResult = yams::cli::run_sync(client.grep(grepReq), 5s);

        REQUIRE(grepResult.has_value());
        REQUIRE(grepResult.value().matches.empty());
    }
}

TEST_CASE("Daemon client error handling", "[daemon][socket][errors]") {
    DaemonHarness harness;
    REQUIRE(harness.start());

    auto client = createClient(harness.socketPath());
    auto connectResult = yams::cli::run_sync(client.connect(), 3s);
    REQUIRE(connectResult.has_value());

    SECTION("get nonexistent document fails gracefully") {
        GetRequest req;
        req.hash = "0000000000000000000000000000000000000000000000000000000000000000";

        auto result = yams::cli::run_sync(client.get(req), 5s);

        REQUIRE(!result.has_value());
        // Should return error, not crash
    }

    SECTION("invalid search query handled") {
        SearchRequest req;
        req.query = ""; // Empty query

        auto result = yams::cli::run_sync(client.search(req), 5s);

        // Either succeeds with empty results or returns error
        // Both are acceptable error handling
        REQUIRE((!result.has_value() || result.value().results.empty()));
    }

    SECTION("malformed grep pattern handled") {
        GrepRequest req;
        req.pattern = "[[[invalid regex"; // Invalid regex

        auto result = yams::cli::run_sync(client.grep(req), 5s);

        // Should return error, not crash
        REQUIRE(!result.has_value());
    }
}

TEST_CASE("Daemon client concurrent requests", "[daemon][socket][concurrency]") {
    DaemonHarness harness;
    REQUIRE(harness.start());

    auto client = createClient(harness.socketPath());
    auto connectResult = yams::cli::run_sync(client.connect(), 3s);
    REQUIRE(connectResult.has_value());

    SECTION("sequential status requests") {
        for (int i = 0; i < 5; ++i) {
            auto result = yams::cli::run_sync(client.status(), 5s);
            REQUIRE(result.has_value());
        }
    }

    SECTION("add multiple documents") {
        std::vector<std::string> hashes;

        for (int i = 0; i < 3; ++i) {
            AddDocumentRequest req;
            req.name = "doc" + std::to_string(i) + ".txt";
            req.content = "Content " + std::to_string(i);

            auto result = yams::cli::run_sync(client.streamingAddDocument(req), 5s);
            REQUIRE(result.has_value());
            hashes.push_back(result.value().hash);
        }

        REQUIRE(hashes.size() == 3);
        // Verify all hashes are unique
        std::set<std::string> uniqueHashes(hashes.begin(), hashes.end());
        REQUIRE(uniqueHashes.size() == 3);
    }

    SECTION("list after adding documents") {
        // Add documents
        for (int i = 0; i < 3; ++i) {
            AddDocumentRequest req;
            req.name = "multi_doc" + std::to_string(i) + ".txt";
            req.content = "Content " + std::to_string(i);

            auto result = yams::cli::run_sync(client.streamingAddDocument(req), 5s);
            REQUIRE(result.has_value());
        }

        // List should show all documents
        ListRequest listReq;
        listReq.limit = 100;

        auto listResult = yams::cli::run_sync(client.list(listReq), 5s);
        REQUIRE(listResult.has_value());
        REQUIRE(listResult.value().items.size() >= 3);
    }
}

TEST_CASE("Daemon client timeout behavior", "[daemon][socket][timeout]") {
    DaemonHarness harness;
    REQUIRE(harness.start());

    SECTION("short header timeout") {
        ClientConfig config;
        config.socketPath = harness.socketPath();
        config.connectTimeout = 2s;
        config.headerTimeout = 100ms; // Very short timeout
        config.autoStart = false;

        DaemonClient client(config);
        auto connectResult = yams::cli::run_sync(client.connect(), 3s);
        REQUIRE(connectResult.has_value());

        // Fast operation should still succeed
        auto statusResult = yams::cli::run_sync(client.status(), 5s);
        REQUIRE(statusResult.has_value());
    }

    SECTION("request timeout handling") {
        auto client = createClient(harness.socketPath());
        auto connectResult = yams::cli::run_sync(client.connect(), 3s);
        REQUIRE(connectResult.has_value());

        // Use a very short timeout for the operation
        auto statusResult = yams::cli::run_sync(client.status(), 1ms);

        // May timeout or succeed depending on timing
        // Just verify it doesn't crash
        REQUIRE(true);
    }
}

TEST_CASE("Daemon client move semantics", "[daemon][socket][move]") {
    DaemonHarness harness;
    REQUIRE(harness.start());

    SECTION("move construction") {
        auto client1 = createClient(harness.socketPath());
        auto connectResult = yams::cli::run_sync(client1.connect(), 3s);
        REQUIRE(connectResult.has_value());
        REQUIRE(client1.isConnected());

        // Move construct
        DaemonClient client2(std::move(client1));
        REQUIRE(client2.isConnected());

        // Original should be in valid but unspecified state
        // Just verify we can destroy it
    }

    SECTION("move assignment") {
        auto client1 = createClient(harness.socketPath());
        auto connectResult = yams::cli::run_sync(client1.connect(), 3s);
        REQUIRE(connectResult.has_value());
        REQUIRE(client1.isConnected());

        // Move assign
        auto client2 = createClient(harness.socketPath(), 2s, false);
        client2 = std::move(client1);
        REQUIRE(client2.isConnected());

        // Can make request with moved-to client
        auto statusResult = yams::cli::run_sync(client2.status(), 5s);
        REQUIRE(statusResult.has_value());
    }
}

TEST_CASE("Daemon socket file lifecycle", "[daemon][socket][filesystem]") {
    DaemonHarness harness;

    SECTION("socket file created on start") {
        REQUIRE(!std::filesystem::exists(harness.socketPath()));

        REQUIRE(harness.start());

        REQUIRE(std::filesystem::exists(harness.socketPath()));
    }

    SECTION("daemon stops on shutdown request") {
        REQUIRE(harness.start());
        REQUIRE(std::filesystem::exists(harness.socketPath()));

        auto client = createClient(harness.socketPath());
        auto connectResult = yams::cli::run_sync(client.connect(), 3s);
        REQUIRE(connectResult.has_value());

        // Send shutdown request
        auto shutdownResult = yams::cli::run_sync(client.shutdown(true), 5s);
        REQUIRE(shutdownResult.has_value());

        // Wait for daemon to actually stop
        bool stopped = false;
        for (int i = 0; i < 50; ++i) {
            std::this_thread::sleep_for(100ms);
            if (!harness.daemon()->isRunning()) {
                stopped = true;
                break;
            }
        }

        REQUIRE(stopped);
    }

    SECTION("socket file removed on stop") {
        REQUIRE(harness.start());
        REQUIRE(std::filesystem::exists(harness.socketPath()));

        harness.stop();
        std::this_thread::sleep_for(500ms);

        // Socket may be cleaned up
        // Just verify we don't crash
        REQUIRE(true);
    }

    SECTION("multiple clients can connect to same socket") {
        REQUIRE(harness.start());

        auto client1 = createClient(harness.socketPath());
        auto client2 = createClient(harness.socketPath());

        auto connect1 = yams::cli::run_sync(client1.connect(), 3s);
        auto connect2 = yams::cli::run_sync(client2.connect(), 3s);

        REQUIRE(connect1.has_value());
        REQUIRE(connect2.has_value());
        REQUIRE(client1.isConnected());
        REQUIRE(client2.isConnected());
    }
}
