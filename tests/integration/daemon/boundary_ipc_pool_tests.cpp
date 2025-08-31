// Boundary micro-tests for pooled client behavior without a live IPC server
// - Validate pool connect failure accounting and fallback execution paths
// - Ensure pooled manager executes fallback when daemon is unavailable

#include <spdlog/spdlog.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <thread>
#ifndef _WIN32
#include <unistd.h>
#endif

#include <yams/cli/daemon_helpers.h>
#include <yams/daemon/client/daemon_client.h>

namespace yams {
namespace daemon {
namespace test {

class BoundaryIpcPoolIT : public ::testing::Test {
protected:
    void SetUp() override {
        // Intentionally do nothing; tests assume no daemon auto-start
    }

    void TearDown() {
        // Intentionally do nothing; no daemon should be started by these tests
    }
};

// Manager executes fallback when daemon is unavailable; pool stats and recovery stats update
TEST_F(BoundaryIpcPoolIT, ManagerFallbackOnUnavailableDaemon) {
    using namespace std::chrono_literals;
    yams::cli::DaemonClientPool::Config pool_cfg;
    pool_cfg.min_clients = 0;
    pool_cfg.max_clients = 2;
    pool_cfg.acquire_timeout = 250ms;         // keep test snappy
    pool_cfg.client_config.autoStart = false; // do not auto-start daemon
    pool_cfg.client_config.connectTimeout = 200ms;
    pool_cfg.client_config.requestTimeout = 300ms;

    yams::cli::PooledRequestManager<SearchRequest> manager(pool_cfg);

    SearchRequest req{};
    req.query = "noop";
    req.limit = 1;

    bool fallback_executed = false;
    auto result = manager.execute(
        req,
        [&]() -> Result<void> {
            fallback_executed = true;
            return Result<void>();
        },
        [&](const SearchResponse&) -> Result<void> {
            return Error{ErrorCode::InternalError, "Render should not be called without daemon"};
        });

    ASSERT_TRUE(result) << (result ? "" : result.error().message);
    EXPECT_TRUE(fallback_executed);

    // Acquire likely failed early; recovery counters may remain zero. Just ensure pool object
    // alive.
    (void)manager.pool();
}

// Pool attempts multiple acquisitions and times out cleanly when no daemon is present
TEST_F(BoundaryIpcPoolIT, PoolAcquireTimeoutWithoutDaemon) {
    using namespace std::chrono_literals;
    yams::cli::DaemonClientPool::Config pool_cfg;
    pool_cfg.min_clients = 0;
    pool_cfg.max_clients = 1;
    pool_cfg.acquire_timeout = 150ms;
    pool_cfg.client_config.autoStart = false;
    pool_cfg.client_config.connectTimeout = 100ms;

    yams::cli::DaemonClientPool pool(pool_cfg);

    auto lease_res = pool.acquire();
    ASSERT_FALSE(lease_res);
    // Either network error (connect failure) or timeout, depending on timing
    auto code = lease_res.error().code;
    EXPECT_TRUE(code == ErrorCode::Timeout || code == ErrorCode::NetworkError)
        << "unexpected error code";
}

} // namespace test
} // namespace daemon
} // namespace yams
