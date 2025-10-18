#include <gtest/gtest.h>
// Validates multiplexing over a single socket does not hang

#include <spdlog/spdlog.h>
#include <atomic>
#include <chrono>
#include <future>
#include <vector>

// Ensure Boost.Asio awaitable/co_spawn are visible before including client header
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

#include "integration/daemon/test_daemon_harness.h"
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>

using namespace yams;
using namespace yams::daemon;

TEST(MCPSocketStressTest, ManyConcurrentRequestsSingleSocket) {
    yams::test::DaemonHarness harness;
    ASSERT_TRUE(harness.start()) << "Failed to start daemon harness";

    ClientConfig cfg;
    cfg.socketPath = harness.socketPath();
    cfg.singleUseConnections = false;
    cfg.maxInflight = 256;
    cfg.requestTimeout = std::chrono::milliseconds(10000);
    cfg.headerTimeout = std::chrono::milliseconds(5000);
    cfg.bodyTimeout = std::chrono::milliseconds(15000);
    cfg.autoStart = false;

    DaemonClient client(cfg);

    // Fire a burst of concurrent lightweight requests over the single socket
    const int concurrency = 64;
    const int rounds = 3;

    for (int r = 0; r < rounds; ++r) {
        auto results =
            std::make_shared<std::vector<std::optional<Result<StatusResponse>>>>(concurrency);
        std::vector<std::future<void>> futs;
        futs.reserve(concurrency);
        for (int i = 0; i < concurrency; ++i) {
            futs.emplace_back(boost::asio::co_spawn(
                GlobalIOContext::instance().get_io_context(),
                [&client, results, i]() -> boost::asio::awaitable<void> {
                    auto res = co_await client.status();
                    (*results)[i] = std::move(res);
                    co_return;
                },
                boost::asio::use_future));
        }
        for (auto& f : futs)
            f.get();
        int ok = 0;
        for (auto& v : *results)
            if (v && v->has_value())
                ++ok;
        spdlog::info("MCPSocketStress round {}: {}/{} OK", r, ok, concurrency);
        ASSERT_EQ(ok, concurrency);
    }
}
