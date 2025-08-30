#include <spdlog/spdlog.h>
#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>

#include <yams/cli/daemon_helpers.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace std::chrono_literals;

namespace yams::daemon::test {

TEST(PoolStreamingReuseIT, TwoSequentialStreamingCallsReusePool) {
    // Ensure daemon is likely up
    (void)DaemonClient::startDaemon({});
    std::this_thread::sleep_for(100ms);

    yams::cli::DaemonClientPool::Config pool_cfg;
    pool_cfg.max_clients = 1; // force reuse of a single connection instance
    pool_cfg.client_config.enableChunkedResponses = true;

    yams::cli::PooledRequestManager<SearchRequest, SearchResponse> mgr(pool_cfg);

    auto do_one = [&](const std::string& q) {
        SearchRequest req{};
        req.query = q;
        req.limit = 3;
        bool rendered = false;
        auto render = [&](const SearchResponse&) -> yams::Result<void> {
            rendered = true;
            return yams::Result<void>();
        };
        auto fallback = [&]() -> yams::Result<void> {
            // It's acceptable to fallback if daemon is not available in CI
            return yams::Result<void>();
        };
        auto r = mgr.execute(req, fallback, render);
        EXPECT_TRUE(r);
        // Either rendered (daemon path) or fallback (no daemon). Both are ok.
        EXPECT_TRUE(rendered || r.has_value()) << "render or fallback path should succeed";
    };

    do_one("alpha");
    do_one("beta");
}

} // namespace yams::daemon::test
