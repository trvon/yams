// Smoke test that pruneIdleConnections reduces available connections beyond minConnections
#include <filesystem>
#include <thread>
#include <gtest/gtest.h>
#include <yams/metadata/connection_pool.h>

using namespace std::chrono_literals;
using namespace yams::metadata;

static std::filesystem::path make_db_path() {
    const char* t = std::getenv("YAMS_TEST_TMPDIR");
    auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = base / (std::string("pool_mt_") + std::to_string(ts) + ".db");
    std::filesystem::remove(p, ec);
    return p;
}

TEST(ConnectionPoolMaintenance, PrunesIdleToMinConnections) {
    ConnectionPoolConfig cfg;
    cfg.minConnections = 1;
    cfg.maxConnections = 4;
    cfg.idleTimeout = std::chrono::seconds(1);
    cfg.enableWAL = false;

    ConnectionPool pool(make_db_path().string(), cfg);
    ASSERT_TRUE(pool.initialize().has_value());

    // Acquire and release to create an extra connection
    {
        auto c1 = pool.acquire();
        ASSERT_TRUE(c1.has_value());
    }
    {
        auto c2 = pool.acquire();
        ASSERT_TRUE(c2.has_value());
    }

    // Let them become idle
    std::this_thread::sleep_for(1500ms);
    pool.pruneIdleConnections();

    auto stats = pool.getStats();
    // After prune, available connections should not exceed minConnections by much
    EXPECT_LE(stats.availableConnections, cfg.minConnections);

    pool.shutdown();
}
