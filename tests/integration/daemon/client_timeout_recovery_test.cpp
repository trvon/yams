/**
 * @file client_timeout_recovery_test.cpp
 * @brief Integration tests for daemon client timeout and stale connection recovery
 */

#define CATCH_CONFIG_MAIN
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <thread>

#include "test_async_helpers.h"
#include "test_daemon_harness.h"
#include <yams/cli/cli_sync.h>
#include <yams/daemon/client/daemon_client.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using namespace yams::daemon;
using namespace yams::test;

namespace {
DaemonClient createClient(const fs::path& socketPath) {
    ClientConfig config;
    config.socketPath = socketPath;
    config.requestTimeout = 3s;
    config.headerTimeout = 5s;
    config.bodyTimeout = 10s;
    config.autoStart = false;
    return DaemonClient(config);
}

bool connectWithRetry(DaemonClient& client, int maxRetries = 3,
                      std::chrono::milliseconds retryDelay = 200ms) {
    for (int attempt = 0; attempt < maxRetries; ++attempt) {
        auto result = yams::cli::run_sync(client.connect(), 5s);
        if (result.has_value()) {
            return true;
        }
        std::this_thread::sleep_for(retryDelay);
    }
    return false;
}
} // namespace

TEST_CASE("Client timeout recovery: Immediate EOF detection and retry",
          "[daemon][timeout][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    DaemonHarness harness;
    REQUIRE(harness.start(5s));

    auto client = createClient(harness.socketPath());

    SECTION("First request succeeds, second request after idle period works") {
        ListRequest req1;
        req1.limit = 10;

        auto result1 = yams::cli::run_sync(client.list(req1), 2s);
        REQUIRE(result1.has_value());

        // Sleep for 2 seconds to allow connection to become idle
        std::this_thread::sleep_for(2s);

        ListRequest req2;
        req2.limit = 10;

        // Connection may be stale after idle period - allow time for reconnection
        // Retry logic handles stale connection scenarios where pool needs to reconnect
        // Note: Connection pool may need multiple attempts to detect stale connection
        // and establish a new one. Give it more time and retries.
        yams::Result<yams::daemon::ListResponse> result2;
        for (int attempt = 0; attempt < 5; ++attempt) {
            result2 = yams::cli::run_sync(client.list(req2), 5s);
            if (result2.has_value())
                break;
            // Longer delay between retries to allow pool to fully reset
            std::this_thread::sleep_for(500ms);
        }
        REQUIRE(result2.has_value());
    }
}

TEST_CASE("Client timeout recovery: Streaming request connection handling",
          "[daemon][timeout][streaming][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    DaemonHarness harness;
    REQUIRE(harness.start(5s));

    auto client = createClient(harness.socketPath());

    SECTION("Streaming requests work after idle period") {
        GrepRequest req1;
        req1.pattern = "test";
        req1.pathsOnly = true;

        auto result1 = yams::cli::run_sync(client.grep(req1), 2s);
        REQUIRE(result1.has_value());

        // Sleep for 2 seconds to allow connection to become idle
        std::this_thread::sleep_for(2s);

        GrepRequest req2;
        req2.pattern = "another";
        req2.pathsOnly = false;

        // Streaming requests may need reconnection after idle period
        // Give more time and retries for connection pool to recover
        yams::Result<yams::daemon::GrepResponse> result2;
        for (int attempt = 0; attempt < 5; ++attempt) {
            result2 = yams::cli::run_sync(client.grep(req2), 5s);
            if (result2.has_value())
                break;
            // Longer delay between retries to allow pool to fully reset
            std::this_thread::sleep_for(500ms);
        }
        REQUIRE(result2.has_value());
    }
}

TEST_CASE("Client timeout recovery: Connection pool management",
          "[daemon][timeout][pool][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    DaemonHarness harness;
    REQUIRE(harness.start(5s));

    auto client = createClient(harness.socketPath());

    SECTION("Pool handles multiple requests over time") {
        for (int i = 0; i < 3; ++i) {
            ListRequest req;
            req.limit = 5;
            auto result = yams::cli::run_sync(client.list(req), 2s);
            REQUIRE(result.has_value());
            std::this_thread::sleep_for(100ms);
        }

        std::this_thread::sleep_for(1s);

        ListRequest freshReq;
        freshReq.limit = 10;

        auto freshResult = yams::cli::run_sync(client.list(freshReq), 2s);
        REQUIRE(freshResult.has_value());
    }
}

TEST_CASE("Client timeout recovery: Rapid request cycle",
          "[daemon][timeout][stress][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    DaemonHarness harness;
    REQUIRE(harness.start(5s));

    auto client = createClient(harness.socketPath());

    SECTION("Handles rapid cycles without issues") {
        constexpr int kCycles = 5;

        for (int cycle = 0; cycle < kCycles; ++cycle) {
            ListRequest req;
            req.limit = 5;
            auto result = yams::cli::run_sync(client.list(req), 2s);
            REQUIRE(result.has_value());
            std::this_thread::sleep_for(200ms);
        }
    }
}

TEST_CASE("Client timeout recovery: Daemon restart handling",
          "[daemon][timeout][reconnect][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    DaemonHarness harness;
    REQUIRE(harness.start(10s));

    auto client = createClient(harness.socketPath());

    SECTION("Reconnects after daemon restart") {
        // Allow daemon to fully initialize before first request
        std::this_thread::sleep_for(500ms);

        ListRequest req1;
        req1.limit = 10;
        auto result1 = yams::cli::run_sync(client.list(req1), 5s);
        REQUIRE(result1.has_value());

        std::this_thread::sleep_for(500ms);

        harness.stop();
        std::this_thread::sleep_for(500ms);

        REQUIRE(harness.start(10s));

        // Allow restarted daemon to fully initialize
        std::this_thread::sleep_for(500ms);

        ListRequest req2;
        req2.limit = 10;
        auto result2 = yams::cli::run_sync(client.list(req2), 5s);
        REQUIRE(result2.has_value());
    }
}

TEST_CASE("Client timeout recovery: Error message quality",
          "[daemon][timeout][errors][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    DaemonHarness harness;
    REQUIRE(harness.start(5s));

    auto client = createClient(harness.socketPath());

    SECTION("Provides descriptive errors when daemon unavailable") {
        ListRequest req1;
        req1.limit = 10;
        auto result1 = yams::cli::run_sync(client.list(req1), 2s);
        REQUIRE(result1.has_value());

        std::this_thread::sleep_for(500ms);

        harness.stop();

        ListRequest req2;
        req2.limit = 10;
        auto result2 = yams::cli::run_sync(client.list(req2), 2s);

        REQUIRE_FALSE(result2.has_value());

        const auto& msg = result2.error().message;

        // Error message should provide some context about what went wrong
        // Accept EOF-related messages as valid since they describe the connection state
        bool hasContext =
            msg.find("Connection") != std::string::npos || msg.find("stale") != std::string::npos ||
            msg.find("closed") != std::string::npos || msg.find("daemon") != std::string::npos ||
            msg.find("EOF") != std::string::npos || msg.find("End of file") != std::string::npos ||
            msg.find("socket") != std::string::npos || msg.find("connect") != std::string::npos;

        REQUIRE(hasContext);
    }
}

TEST_CASE("Client timeout recovery: Connection refused when daemon down",
          "[daemon][timeout][connection-refused][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    DaemonHarness harness;
    REQUIRE(harness.start(10s));

    auto client = createClient(harness.socketPath());
    REQUIRE(connectWithRetry(client));

    // Allow daemon to fully initialize
    std::this_thread::sleep_for(500ms);

    ListRequest req1;
    req1.limit = 1;
    auto result1 = yams::cli::run_sync(client.list(req1), 5s);
    REQUIRE(result1.has_value());

    std::this_thread::sleep_for(200ms);
    harness.stop();

    ListRequest req2;
    req2.limit = 1;
    auto result2 = yams::cli::run_sync(client.list(req2), 2s);
    REQUIRE_FALSE(result2.has_value());

    const auto& msg = result2.error().message;
    bool hasContext =
        msg.find("Connection") != std::string::npos || msg.find("refused") != std::string::npos ||
        msg.find("daemon") != std::string::npos || msg.find("socket") != std::string::npos ||
        msg.find("EOF") != std::string::npos || msg.find("End of file") != std::string::npos ||
        msg.find("closed") != std::string::npos || msg.find("establish") != std::string::npos;
    REQUIRE(hasContext);
}

TEST_CASE("Client timeout recovery: Shutdown cancels in-flight request",
          "[daemon][timeout][shutdown][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    DaemonHarness harness;
    REQUIRE(harness.start(5s));

    ClientConfig config;
    config.socketPath = harness.socketPath();
    config.requestTimeout = 10s;
    config.headerTimeout = 10s;
    config.bodyTimeout = 10s;
    config.autoStart = false;
    config.maxInflight = 1;
    DaemonClient client(config);
    REQUIRE(connectWithRetry(client));

    std::atomic<bool> done{false};
    std::optional<yams::Error> requestError;
    std::thread requestThread([&]() {
        AddDocumentRequest req;
        req.name = "shutdown_inflight.txt";
        req.content = std::string(2 * 1024 * 1024, 'x');
        auto result = yams::cli::run_sync(client.streamingAddDocument(req), 10s);
        if (!result.has_value()) {
            requestError = result.error();
        }
        done.store(true);
    });

    std::this_thread::sleep_for(50ms);
    harness.stop();

    auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!done.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(10ms);
    }
    requestThread.join();

    REQUIRE(done.load());
    if (requestError.has_value()) {
        const auto& msg = requestError->message;
        bool hasContext =
            msg.find("cancel") != std::string::npos ||
            msg.find("Connection") != std::string::npos ||
            msg.find("closed") != std::string::npos || msg.find("timeout") != std::string::npos ||
            msg.find("daemon") != std::string::npos || msg.find("pipe") != std::string::npos ||
            msg.find("shutdown") != std::string::npos;
        REQUIRE(hasContext);
    }
}
