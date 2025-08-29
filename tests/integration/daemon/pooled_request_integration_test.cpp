#include <spdlog/spdlog.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <thread>
#include <vector>

#include <yams/cli/daemon_helpers.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace std::chrono_literals;

namespace yams::daemon::test {

class PooledRequestIT : public ::testing::Test {
protected:
    void SetUp() override {
        // Try to ensure the daemon is running before tests execute.
        // It's OK if this fails; tests will exercise fallback paths.
        ClientConfig cfg{};
        auto started = DaemonClient::startDaemon(cfg);
        if (!started) {
            spdlog::warn("PooledRequestIT: could not auto-start daemon: {}",
                         started.error().message);
        }

        // Small delay to allow the daemon to initialize its socket.
        std::this_thread::sleep_for(200ms);
    }
};

TEST_F(PooledRequestIT, Search_Succeeds_With_Render_Or_Fallback) {
    // Small pool to exercise gating
    yams::cli::DaemonClientPool::Config pool_cfg;
    pool_cfg.max_clients = 2;

    yams::cli::PooledRequestManager<SearchRequest, SearchResponse> mgr(pool_cfg);

    SearchRequest req{};
    req.query = "test";
    req.limit = 5;

    bool rendered = false;
    bool fellback = false;

    auto render = [&](const SearchResponse& resp) -> yams::Result<void> {
        rendered = true;
        spdlog::debug("Search response: results={}, total={}", resp.results.size(),
                      resp.totalCount);
        return yams::Result<void>();
    };

    auto fallback = [&]() -> yams::Result<void> {
        fellback = true;
        spdlog::warn("Search fallback executed: daemon unavailable");
        return yams::Result<void>();
    };

    auto r = mgr.execute(req, fallback, render);
    EXPECT_TRUE(r) << "execute() returns ok for both render/fallback paths";
    EXPECT_TRUE(rendered || fellback) << "Either render or fallback must run";
}

TEST_F(PooledRequestIT, Grep_Succeeds_With_Render_Or_Fallback) {
    yams::cli::DaemonClientPool::Config pool_cfg;
    pool_cfg.max_clients = 1;

    yams::cli::PooledRequestManager<GrepRequest, GrepResponse> mgr(pool_cfg);

    GrepRequest req{};
    req.pattern = "YAMS";
    req.paths = {};             // Let the daemon choose default scope
    req.caseInsensitive = true; // Non-strict search
    req.recursive = true;       // Common default
    req.maxMatches = 0;         // Unlimited per file

    bool rendered = false;
    bool fellback = false;

    auto render = [&](const GrepResponse& resp) -> yams::Result<void> {
        rendered = true;
        spdlog::debug("Grep response: filesSearched={}, totalMatches={}", resp.filesSearched,
                      resp.totalMatches);
        return yams::Result<void>();
    };

    auto fallback = [&]() -> yams::Result<void> {
        fellback = true;
        spdlog::warn("Grep fallback executed: daemon unavailable");
        return yams::Result<void>();
    };

    auto r = mgr.execute(req, fallback, render);
    EXPECT_TRUE(r);
    EXPECT_TRUE(rendered || fellback);
}

TEST_F(PooledRequestIT, Pool_Unavailable_Still_Proceeds_With_DaemonClient) {
    // Force pool acquisition failure (0 slots)
    yams::cli::DaemonClientPool::Config pool_cfg;
    pool_cfg.max_clients = 0;
    pool_cfg.acquire_timeout = std::chrono::milliseconds{5};

    yams::cli::PooledRequestManager<SearchRequest, SearchResponse> mgr(pool_cfg);

    SearchRequest req{};
    req.query = "test";
    req.limit = 1;

    bool rendered = false;
    bool fellback = false;

    auto render = [&](const SearchResponse&) -> yams::Result<void> {
        rendered = true;
        return yams::Result<void>();
    };

    auto fallback = [&]() -> yams::Result<void> {
        fellback = true;
        return yams::Result<void>();
    };

    auto r = mgr.execute(req, fallback, render);
    EXPECT_TRUE(r);
    EXPECT_TRUE(rendered || fellback)
        << "Should either render or fallback even if pool is unavailable";
}

TEST_F(PooledRequestIT, Concurrent_Execute_Basic) {
    // Modest gating to allow some contention
    yams::cli::DaemonClientPool::Config pool_cfg;
    pool_cfg.max_clients = 2;

    yams::cli::PooledRequestManager<SearchRequest, SearchResponse> mgr(pool_cfg);

    std::atomic<int> renders{0};
    std::atomic<int> fallbacks{0};

    constexpr int kThreads = 4;
    std::vector<std::thread> threads;
    threads.reserve(kThreads);

    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&]() {
            SearchRequest req{};
            req.query = "concurrent";
            req.limit = 3;

            auto render = [&](const SearchResponse&) -> yams::Result<void> {
                renders.fetch_add(1);
                return yams::Result<void>();
            };

            auto fallback = [&]() -> yams::Result<void> {
                fallbacks.fetch_add(1);
                return yams::Result<void>();
            };

            auto r = mgr.execute(req, fallback, render);
            ASSERT_TRUE(r);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // We don't assert all renders since daemon may be temporarily unavailable on CI machines.
    EXPECT_GT(renders.load() + fallbacks.load(), 0) << "At least one request should complete";
}

} // namespace yams::daemon::test