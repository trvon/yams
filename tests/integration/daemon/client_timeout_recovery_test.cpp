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

        std::this_thread::sleep_for(2s);

        ListRequest req2;
        req2.limit = 10;

        auto result2 = yams::cli::run_sync(client.list(req2), 2s);
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

        std::this_thread::sleep_for(2s);

        GrepRequest req2;
        req2.pattern = "another";
        req2.pathsOnly = false;

        auto result2 = yams::cli::run_sync(client.grep(req2), 2s);
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
    REQUIRE(harness.start(5s));

    auto client = createClient(harness.socketPath());

    SECTION("Reconnects after daemon restart") {
        ListRequest req1;
        req1.limit = 10;
        auto result1 = yams::cli::run_sync(client.list(req1), 2s);
        REQUIRE(result1.has_value());

        std::this_thread::sleep_for(500ms);

        harness.stop();
        std::this_thread::sleep_for(100ms);

        REQUIRE(harness.start(3s));

        ListRequest req2;
        req2.limit = 10;
        auto result2 = yams::cli::run_sync(client.list(req2), 2s);
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
        REQUIRE(msg != "End of file");

        bool hasContext =
            msg.find("Connection") != std::string::npos || msg.find("stale") != std::string::npos ||
            msg.find("closed") != std::string::npos || msg.find("daemon") != std::string::npos;

        REQUIRE(hasContext);
    }
}
