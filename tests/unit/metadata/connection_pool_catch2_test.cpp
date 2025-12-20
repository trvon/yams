// Catch2 migration of connection pool tests
// Migration: yams-3s4 / yams-aqc (metadata tests)
// Covers:
//   - connection_pool_maintenance_smoke_test.cpp
//   - connection_pool_waiting_guard_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <thread>
#include <yams/metadata/connection_pool.h>

using namespace std::chrono_literals;
using namespace yams::metadata;

namespace {
std::filesystem::path make_db_path(const std::string& prefix) {
    const char* t = std::getenv("YAMS_TEST_TMPDIR");
    auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = base / (prefix + std::to_string(ts) + ".db");
    std::filesystem::remove(p, ec);
    return p;
}
} // namespace

// ============================================================================
// ConnectionPoolMaintenance - pruneIdleConnections
// ============================================================================
TEST_CASE("Connection pool prunes idle connections to min", "[metadata][connection_pool][slow]") {
    ConnectionPoolConfig cfg;
    cfg.minConnections = 1;
    cfg.maxConnections = 4;
    cfg.idleTimeout = std::chrono::seconds(1);
    cfg.enableWAL = false;

    ConnectionPool pool(make_db_path("pool_mt_").string(), cfg);
    REQUIRE(pool.initialize().has_value());

    // Acquire and release to create extra connections
    {
        auto c1 = pool.acquire();
        REQUIRE(c1.has_value());
    }
    {
        auto c2 = pool.acquire();
        REQUIRE(c2.has_value());
    }

    // Let them become idle
    std::this_thread::sleep_for(1500ms);
    pool.pruneIdleConnections();

    auto stats = pool.getStats();
    // After prune, available connections should not exceed minConnections by much
    CHECK(stats.availableConnections <= cfg.minConnections);

    pool.shutdown();
}

// ============================================================================
// ConnectionPoolWaitingGuard - timeout behavior
// ============================================================================
TEST_CASE("Connection pool waiting counter resets on timeout", "[metadata][connection_pool]") {
    ConnectionPoolConfig cfg;
    cfg.minConnections = 1;
    cfg.maxConnections = 1;
    cfg.enableWAL = false;
    cfg.busyTimeout = 50ms;

    ConnectionPool pool(make_db_path("pool_wait_guard_").string(), cfg);
    REQUIRE(pool.initialize().has_value());

    auto first = pool.acquire();
    REQUIRE(first.has_value());

    auto second = pool.acquire(100ms);
    REQUIRE_FALSE(second.has_value());

    auto stats = pool.getStats();
    CHECK(stats.waitingRequests == 0u);
    CHECK(stats.maxObservedWaiting >= 1u);
    CHECK(stats.totalWaitMicros > 0u);
    CHECK(stats.timeoutCount == 1u);

    pool.shutdown();
}

TEST_CASE("Connection pool immediate acquire does not count waiting",
          "[metadata][connection_pool]") {
    ConnectionPoolConfig cfg;
    cfg.minConnections = 1;
    cfg.maxConnections = 2;
    cfg.enableWAL = false;

    ConnectionPool pool(make_db_path("pool_wait_guard_").string(), cfg);
    REQUIRE(pool.initialize().has_value());

    {
        auto conn = pool.acquire();
        REQUIRE(conn.has_value());
    }

    auto stats = pool.getStats();
    CHECK(stats.waitingRequests == 0u);
    CHECK(stats.maxObservedWaiting == 0u);
    CHECK(stats.totalWaitMicros == 0u);
    CHECK(stats.timeoutCount == 0u);

    pool.shutdown();
}
