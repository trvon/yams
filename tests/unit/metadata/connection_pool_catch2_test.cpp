// Catch2 migration of connection pool tests
// Migration: yams-3s4 / yams-aqc (metadata tests)
// Covers:
//   - connection_pool_maintenance_smoke_test.cpp
//   - connection_pool_waiting_guard_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <sqlite3.h>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <string>
#include <thread>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>

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

TEST_CASE("Connection pool histograms holder durations and warns on slow holds",
          "[metadata][connection_pool]") {
    ConnectionPoolConfig cfg;
    cfg.minConnections = 1;
    cfg.maxConnections = 1;
    cfg.enableWAL = false;

    ConnectionPool pool(make_db_path("pool_holder_hist_").string(), cfg);
    REQUIRE(pool.initialize().has_value());

    pool.setSlowHolderThreshold(std::chrono::milliseconds(20));

    {
        auto conn =
            pool.acquire(std::chrono::milliseconds(1000), ConnectionPriority::Normal, "fast_path");
        REQUIRE(conn.has_value());
        CHECK(conn.value()->holderTag() == "fast_path");
    }

    {
        auto conn =
            pool.acquire(std::chrono::milliseconds(1000), ConnectionPriority::Normal, "slow_path");
        REQUIRE(conn.has_value());
        CHECK(conn.value()->holderTag() == "slow_path");
        std::this_thread::sleep_for(std::chrono::milliseconds(60));
    }

    auto stats = pool.getStats();
    std::uint64_t total = 0;
    for (auto v : stats.holderDurationBuckets) {
        total += v;
    }
    CHECK(total >= 2u);
    CHECK(stats.slowHolderCount >= 1u);
    CHECK(stats.maxHolderMicros >= 50'000u);

    pool.shutdown();
}

TEST_CASE("Connection pool records source location for untagged acquire",
          "[metadata][connection_pool][holder-tag]") {
    ConnectionPoolConfig cfg;
    cfg.minConnections = 1;
    cfg.maxConnections = 1;
    cfg.enableWAL = false;

    ConnectionPool pool(make_db_path("pool_holder_tag_").string(), cfg);
    REQUIRE(pool.initialize().has_value());

    {
        auto conn = pool.acquire();
        REQUIRE(conn.has_value());
        const auto& tag = conn.value()->holderTag();
        CHECK_FALSE(tag.empty());
        CHECK(tag.find("connection_pool_catch2_test.cpp") != std::string::npos);
        CHECK(tag.find("<untagged>") == std::string::npos);
    }

    pool.shutdown();
}

TEST_CASE("Connection pool timeout holder summary includes withConnection call site",
          "[metadata][connection_pool][holder-tag]") {
    ConnectionPoolConfig cfg;
    cfg.minConnections = 1;
    cfg.maxConnections = 1;
    cfg.enableWAL = false;

    ConnectionPool pool(make_db_path("pool_with_connection_tag_").string(), cfg);
    REQUIRE(pool.initialize().has_value());

    std::promise<void> entered;
    std::promise<void> release;
    auto enteredFuture = entered.get_future();
    auto releaseFuture = release.get_future();
    std::atomic<bool> holderSucceeded{false};

    std::thread holder([&] {
        auto result = pool.withConnection([&](Database&) -> yams::Result<void> {
            entered.set_value();
            releaseFuture.wait();
            return yams::Result<void>();
        });
        holderSucceeded.store(result.has_value(), std::memory_order_relaxed);
    });

    if (enteredFuture.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
        release.set_value();
        holder.join();
        FAIL("withConnection holder did not acquire the connection");
    }

    auto blocked = pool.acquire(std::chrono::milliseconds(50));
    if (blocked.has_value()) {
        release.set_value();
        holder.join();
        FAIL("second acquire unexpectedly succeeded while withConnection held the only connection");
    }
    CHECK(blocked.error().message.find("connection_pool_catch2_test.cpp") != std::string::npos);
    CHECK(blocked.error().message.find("holders=[<untagged>") == std::string::npos);

    release.set_value();
    holder.join();
    CHECK(holderSucceeded.load(std::memory_order_relaxed));
    pool.shutdown();
}

TEST_CASE("Read-only connection pool opens SQLite read-only",
          "[metadata][connection_pool][readonly]") {
    const auto dbPath = make_db_path("pool_readonly_");

    {
        Database seed;
        REQUIRE(seed.open(dbPath.string(), ConnectionMode::Create).has_value());
        REQUIRE(seed.execute("CREATE TABLE readonly_probe(id INTEGER PRIMARY KEY)").has_value());
    }

    ConnectionPoolConfig cfg;
    cfg.minConnections = 1;
    cfg.maxConnections = 1;
    cfg.readOnly = true;

    ConnectionPool pool(dbPath.string(), cfg);
    REQUIRE(pool.initialize().has_value());

    {
        auto conn = pool.acquire();
        REQUIRE(conn.has_value());
        auto pooled = std::move(conn).value();
        auto& db = **pooled;
        REQUIRE(db.rawHandle() != nullptr);
        CHECK(sqlite3_db_readonly(db.rawHandle(), "main") == 1);

        auto write = db.execute("CREATE TABLE should_not_write(id INTEGER)");
        CHECK_FALSE(write.has_value());
    }

    pool.shutdown();
}

TEST_CASE("Connection pool shutdown while connection is leased does not crash",
          "[metadata][connection_pool][shutdown]") {
    ConnectionPoolConfig cfg;
    cfg.minConnections = 1;
    cfg.maxConnections = 1;
    cfg.enableWAL = false;

    ConnectionPool pool(make_db_path("pool_shutdown_leased_").string(), cfg);
    REQUIRE(pool.initialize().has_value());

    auto conn = pool.acquire();
    REQUIRE(conn.has_value());

    auto statsBefore = pool.getStats();
    CHECK(statsBefore.activeConnections == 1u);

    pool.shutdown();

    auto statsAfter = pool.getStats();
    CHECK(statsAfter.activeConnections == 0u);
    CHECK(statsAfter.totalConnections == 0u);

    auto afterShutdown = pool.acquire(std::chrono::milliseconds(0));
    REQUIRE_FALSE(afterShutdown.has_value());
    CHECK(afterShutdown.error().code == yams::ErrorCode::InvalidState);
}
