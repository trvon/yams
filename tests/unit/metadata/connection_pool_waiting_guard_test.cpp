#include <filesystem>
#include <thread>
#include <gtest/gtest.h>
#include <yams/metadata/connection_pool.h>

using namespace std::chrono_literals;
using namespace yams::metadata;

namespace {
std::filesystem::path make_pool_path() {
    const char* t = std::getenv("YAMS_TEST_TMPDIR");
    auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = base / (std::string("pool_wait_guard_") + std::to_string(ts) + ".db");
    std::filesystem::remove(p, ec);
    return p;
}
} // namespace

TEST(ConnectionPoolWaitingGuard, WaitingCounterResetsOnTimeout) {
    ConnectionPoolConfig cfg;
    cfg.minConnections = 1;
    cfg.maxConnections = 1;
    cfg.enableWAL = false;
    cfg.busyTimeout = 50ms;

    ConnectionPool pool(make_pool_path().string(), cfg);
    ASSERT_TRUE(pool.initialize().has_value());

    auto first = pool.acquire();
    ASSERT_TRUE(first.has_value());

    auto second = pool.acquire(100ms);
    ASSERT_FALSE(second.has_value());
    auto stats = pool.getStats();
    EXPECT_EQ(stats.waitingRequests, 0u);
    EXPECT_GE(stats.maxObservedWaiting, 1u);
    EXPECT_GT(stats.totalWaitMicros, 0u);
    EXPECT_EQ(stats.timeoutCount, 1u);

    pool.shutdown();
}

TEST(ConnectionPoolWaitingGuard, ImmediateAcquireDoesNotCountWaiting) {
    ConnectionPoolConfig cfg;
    cfg.minConnections = 1;
    cfg.maxConnections = 2;
    cfg.enableWAL = false;

    ConnectionPool pool(make_pool_path().string(), cfg);
    ASSERT_TRUE(pool.initialize().has_value());

    {
        auto conn = pool.acquire();
        ASSERT_TRUE(conn.has_value());
    }

    auto stats = pool.getStats();
    EXPECT_EQ(stats.waitingRequests, 0u);
    EXPECT_EQ(stats.maxObservedWaiting, 0u);
    EXPECT_EQ(stats.totalWaitMicros, 0u);
    EXPECT_EQ(stats.timeoutCount, 0u);

    pool.shutdown();
}
