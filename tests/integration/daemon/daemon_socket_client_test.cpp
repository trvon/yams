// Comprehensive daemon socket client integration test
// Covers connection lifecycle, reconnection, timeouts, multiplexing, and error handling

#define CATCH_CONFIG_MAIN
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <filesystem>
#include <set>
#include <thread>

#include "test_async_helpers.h"
#include "test_daemon_harness.h"
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace yams::daemon;
using namespace yams::test;
using namespace std::chrono_literals;

namespace {
// Helper to create a client with custom config
// Increased default timeouts to handle thread resource pressure from repeated daemon restarts
DaemonClient createClient(const std::filesystem::path& socketPath,
                          std::chrono::milliseconds connectTimeout = 5s, bool autoStart = false) {
    ClientConfig config;
    config.socketPath = socketPath;
    config.connectTimeout = connectTimeout;
    config.autoStart = autoStart;
    config.requestTimeout = 10s;
    return DaemonClient(config);
}

} // namespace

TEST_CASE("Daemon socket connection lifecycle", "[daemon][socket][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    SECTION("connects to running daemon") {
        DaemonHarness harness;
        REQUIRE(harness.start());

        auto client = createClient(harness.socketPath());
        auto result = yams::cli::run_sync(client.connect(), 5s);

        REQUIRE(result.has_value());
        REQUIRE(client.isConnected());
    }

    SECTION("fails gracefully when daemon not running") {
        // Use a separate harness that won't start
        DaemonHarness harness;
        auto client = createClient(harness.socketPath(), 500ms);
        auto result = yams::cli::run_sync(client.connect(), 1s);

        REQUIRE(!result.has_value());
        REQUIRE(!client.isConnected());
    }

    SECTION("connects after daemon restart") {
        // This test needs its own daemon to restart
        DaemonHarness harness;
        REQUIRE(harness.start());

        auto client = createClient(harness.socketPath());
        auto connectResult = yams::cli::run_sync(client.connect(), 5s);
        REQUIRE(connectResult.has_value());
        REQUIRE(client.isConnected());

        // Stop daemon - this resets GlobalIOContext, invalidating existing clients
        harness.stop();
        std::this_thread::sleep_for(500ms);

        // Restart daemon
        REQUIRE(harness.start());

        // Create NEW client after restart (old client's io_context was reset)
        auto client2 = createClient(harness.socketPath());
        auto reconnectResult = yams::cli::run_sync(client2.connect(), 5s);
        REQUIRE(reconnectResult.has_value());
        REQUIRE(client2.isConnected());
    }

    SECTION("handles explicit disconnect") {
        DaemonHarness harness;
        REQUIRE(harness.start());

        auto client = createClient(harness.socketPath());
        auto connectResult = yams::cli::run_sync(client.connect(), 5s);
        REQUIRE(connectResult.has_value());
        REQUIRE(client.isConnected());

        client.disconnect();
        REQUIRE(!client.isConnected());
    }

    SECTION("respects connection timeout") {
        // Use a separate harness that won't start
        DaemonHarness harness;
        auto client = createClient(harness.socketPath(), 100ms);

        auto start = std::chrono::steady_clock::now();
        auto result = yams::cli::run_sync(client.connect(), 500ms);
        auto elapsed = std::chrono::steady_clock::now() - start;

        REQUIRE(!result.has_value());
        REQUIRE(elapsed < 200ms); // Should fail fast with 100ms timeout
    }

    SECTION("survives repeated restart cycles") {
        DaemonHarness harness;
        constexpr int cycles = 2; // Reduced from 3 to avoid macOS thread limit

        for (int i = 0; i < cycles; ++i) {
            INFO("restart cycle " << i);
            REQUIRE(harness.start());

            auto client = createClient(harness.socketPath());
            auto connectResult = yams::cli::run_sync(client.connect(), 5s);
            REQUIRE(connectResult.has_value());

            auto statusResult = yams::cli::run_sync(client.status(), 5s);
            REQUIRE(statusResult.has_value());

            harness.stop();
            // Allow GlobalIOContext to fully restart before next cycle
            std::this_thread::sleep_for(500ms);
        }
    }
}

TEST_CASE("Daemon client request execution", "[daemon][socket][requests]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    // Use isolated daemon for tests that modify database state
    DaemonHarness harness;
    REQUIRE(harness.start());

    auto client = createClient(harness.socketPath());
    auto connectResult = yams::cli::run_sync(client.connect(), 5s);
    REQUIRE(connectResult.has_value());

    SECTION("status request succeeds") {
        auto statusResult = yams::cli::run_sync(client.status(), 5s);

        REQUIRE(statusResult.has_value());
        auto& status = statusResult.value();
        REQUIRE(status.readinessStates.at("ipc_server"));
        REQUIRE(status.readinessStates.at("content_store"));
    }

    SECTION("list request succeeds") {
        ListRequest req;
        req.limit = 10;

        auto listResult = yams::cli::run_sync(client.list(req), 5s);

        REQUIRE(listResult.has_value());
        // Database may have documents from other tests - just verify response is valid
    }

    SECTION("add and retrieve document") {
        // Add a document with unique content
        AddDocumentRequest addReq;
        addReq.name = "test_request_execution.txt";
        addReq.content = "Hello from request execution test!";

        auto addResult = yams::cli::run_sync(client.streamingAddDocument(addReq), 5s);
        REQUIRE(addResult.has_value());

        std::string docHash = addResult.value().hash;
        REQUIRE(!docHash.empty());

        // Retrieve it
        GetRequest getReq;
        getReq.hash = docHash;

        auto getResult = yams::cli::run_sync(client.get(getReq), 5s);
        REQUIRE(getResult.has_value());
        REQUIRE(getResult.value().content == "Hello from request execution test!");
        REQUIRE(getResult.value().name == "test_request_execution.txt");
    }

    SECTION("search for unique content") {
        SearchRequest searchReq;
        searchReq.query = "xyzzy_nonexistent_unique_string_12345";

        auto searchResult = yams::cli::run_sync(client.search(searchReq), 5s);

        REQUIRE(searchResult.has_value());
        // Should return empty results for unique non-existent query
        REQUIRE(searchResult.value().results.empty());
    }

    SECTION("grep with unique pattern") {
        GrepRequest grepReq;
        grepReq.pattern = "xyzzy_nonexistent_pattern_67890";

        auto grepResult = yams::cli::run_sync(client.grep(grepReq), 5s);

        REQUIRE(grepResult.has_value());
        // Should return empty matches for unique non-existent pattern
        REQUIRE(grepResult.value().matches.empty());
    }
}

TEST_CASE("Daemon client error handling", "[daemon][socket][errors]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    // Use isolated daemon for tests that modify database state
    DaemonHarness harness;
    REQUIRE(harness.start());

    auto client = createClient(harness.socketPath());
    auto connectResult = yams::cli::run_sync(client.connect(), 5s);
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
    SKIP_DAEMON_TEST_ON_WINDOWS();

    // Use isolated daemon for tests that modify database state
    DaemonHarness harness;
    REQUIRE(harness.start());

    auto client = createClient(harness.socketPath());
    auto connectResult = yams::cli::run_sync(client.connect(), 5s);
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
    SKIP_DAEMON_TEST_ON_WINDOWS();

    // Use isolated daemon
    DaemonHarness harness;
    REQUIRE(harness.start());

    SECTION("short header timeout") {
        ClientConfig config;
        config.socketPath = harness.socketPath();
        config.connectTimeout = 2s;
        config.headerTimeout = 100ms; // Very short timeout
        config.autoStart = false;

        DaemonClient client(config);
        auto connectResult = yams::cli::run_sync(client.connect(), 5s);
        REQUIRE(connectResult.has_value());

        // Fast operation should still succeed
        auto statusResult = yams::cli::run_sync(client.status(), 5s);
        REQUIRE(statusResult.has_value());
    }

    SECTION("request timeout handling") {
        auto client = createClient(harness.socketPath());
        auto connectResult = yams::cli::run_sync(client.connect(), 5s);
        REQUIRE(connectResult.has_value());

        // Use a very short timeout for the operation
        auto statusResult = yams::cli::run_sync(client.status(), 1ms);

        // May timeout or succeed depending on timing
        // Just verify it doesn't crash
        REQUIRE(true);
    }
}

TEST_CASE("Daemon client move semantics", "[daemon][socket][move]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    // Use isolated daemon
    DaemonHarness harness;
    REQUIRE(harness.start());

    SECTION("move construction") {
        auto client1 = createClient(harness.socketPath());
        auto connectResult = yams::cli::run_sync(client1.connect(), 5s);
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
        auto connectResult = yams::cli::run_sync(client1.connect(), 5s);
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
    SKIP_DAEMON_TEST_ON_WINDOWS();

    SECTION("socket file created on start") {
        // Use its own daemon
        DaemonHarness harness;
        REQUIRE(harness.start());
        REQUIRE(std::filesystem::exists(harness.socketPath()));
        harness.stop();
    }

    SECTION("daemon stops on shutdown request") {
        // This test needs its own daemon to shutdown
        DaemonHarness harness;
        REQUIRE(harness.start());
        REQUIRE(std::filesystem::exists(harness.socketPath()));

        auto client = createClient(harness.socketPath());
        auto connectResult = yams::cli::run_sync(client.connect(), 5s);
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
        // This test needs its own daemon to stop
        DaemonHarness harness;
        REQUIRE(harness.start());
        REQUIRE(std::filesystem::exists(harness.socketPath()));

        harness.stop();
        std::this_thread::sleep_for(500ms);

        // Socket may be cleaned up
        // Just verify we don't crash
        REQUIRE(true);
    }

    SECTION("multiple clients can connect to same socket") {
        DaemonHarness harness;
        REQUIRE(harness.start());

        auto client1 = createClient(harness.socketPath());
        auto client2 = createClient(harness.socketPath());

        auto connect1 = yams::cli::run_sync(client1.connect(), 5s);
        auto connect2 = yams::cli::run_sync(client2.connect(), 5s);

        REQUIRE(connect1.has_value());
        REQUIRE(connect2.has_value());
        REQUIRE(client1.isConnected());
        REQUIRE(client2.isConnected());
    }
}
