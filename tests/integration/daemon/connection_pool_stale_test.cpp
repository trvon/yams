// AsioConnectionPool stale connection detection integration test
// Validates that connection pool properly detects and handles stale connections
// after daemon restart, ensuring CLI requests don't hang

#define CATCH_CONFIG_MAIN
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <thread>

#include "test_async_helpers.h"
#include "test_daemon_harness.h"
#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace yams::daemon;
using namespace yams::test;
using namespace std::chrono_literals;

namespace {
DaemonClient createClient(const std::filesystem::path& socketPath) {
    ClientConfig config;
    config.socketPath = socketPath;
    config.connectTimeout = 2s;
    config.autoStart = false;
    config.requestTimeout = 5s;
    return DaemonClient(config);
}
} // namespace

TEST_CASE("AsioConnectionPool: Stale connection detection after daemon restart",
          "[daemon][connection-pool][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    SECTION("detects stale connections and creates new ones after daemon restart") {
        DaemonHarness harness;
        REQUIRE(harness.start());

        auto client = createClient(harness.socketPath());
        auto connectResult = yams::cli::run_sync(client.connect(), 3s);
        REQUIRE(connectResult.has_value());

        GetStatsRequest statsReq;
        auto statsResult = yams::cli::run_sync(client.getStats(statsReq), 3s);
        REQUIRE(statsResult.has_value());

        harness.stop();
        std::this_thread::sleep_for(500ms);

        REQUIRE(harness.start());

        auto reconnectResult = yams::cli::run_sync(client.connect(), 3s);
        REQUIRE(reconnectResult.has_value());

        auto statsResult2 = yams::cli::run_sync(client.getStats(statsReq), 3s);
        REQUIRE(statsResult2.has_value());
    }

    SECTION("handles multiple requests after daemon has been running") {
        DaemonHarness harness;
        REQUIRE(harness.start());

        auto client = createClient(harness.socketPath());
        auto connectResult = yams::cli::run_sync(client.connect(), 3s);
        REQUIRE(connectResult.has_value());

        GetStatsRequest statsReq;

        for (int i = 0; i < 10; ++i) {
            auto statsResult = yams::cli::run_sync(client.getStats(statsReq), 3s);
            REQUIRE(statsResult.has_value());
            std::this_thread::sleep_for(100ms);
        }
    }

    SECTION("connection pool survives daemon restart with concurrent requests") {
        DaemonHarness harness;
        REQUIRE(harness.start());

        // Clear any stale connections from harness's internal verify check
        AsioConnectionPool::shutdown_all(100ms);
        std::this_thread::sleep_for(50ms);

        std::vector<DaemonClient> clients;
        for (int i = 0; i < 3; ++i) {
            clients.push_back(createClient(harness.socketPath()));
            auto connectResult = yams::cli::run_sync(clients.back().connect(), 3s);
            REQUIRE(connectResult.has_value());
        }

        // Small delay to ensure daemon's request processing loop is fully initialized
        std::this_thread::sleep_for(100ms);

        GetStatsRequest statsReq;
        for (auto& client : clients) {
            auto statsResult = yams::cli::run_sync(client.getStats(statsReq), 3s);
            REQUIRE(statsResult.has_value());
        }

        harness.stop();
        std::this_thread::sleep_for(500ms);
        REQUIRE(harness.start());

        for (auto& client : clients) {
            auto reconnectResult = yams::cli::run_sync(client.connect(), 3s);
            REQUIRE(reconnectResult.has_value());

            auto statsResult = yams::cli::run_sync(client.getStats(statsReq), 3s);
            REQUIRE(statsResult.has_value());
        }
    }
}
