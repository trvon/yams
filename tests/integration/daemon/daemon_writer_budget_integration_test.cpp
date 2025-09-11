#include <chrono>
#include <cstdlib>
#include "test_async_helpers.h"
#include <gtest/gtest.h>
#include <yams/daemon/client/daemon_client.h>

using namespace std::chrono_literals;

namespace {
bool daemon_available() {
    return yams::daemon::DaemonClient::isDaemonRunning();
}
} // namespace

// Optional test: requires YAMS_TEST_WRITER_BUDGET=1 and server to read
// YAMS_SERVER_WRITER_BUDGET_BYTES before daemon start.
TEST(ServerMultiplexIntegrationTest, WriterBudgetAllowsFastUnary) {
    if (!daemon_available()) {
        GTEST_SKIP() << "Daemon not available for integration tests";
    }
    const char* enabled = std::getenv("YAMS_TEST_WRITER_BUDGET");
    if (!(enabled && std::string(enabled) == "1")) {
        GTEST_SKIP() << "Set YAMS_TEST_WRITER_BUDGET=1 and start daemon with "
                        "YAMS_SERVER_WRITER_BUDGET_BYTES";
    }

    yams::daemon::ClientConfig cfg;
    cfg.requestTimeout = 5s;
    cfg.headerTimeout = 3s;
    cfg.bodyTimeout = 5s;
    cfg.maxInflight = 64;
    yams::daemon::DaemonClient client(cfg);

    // Start long-running streaming search (coarse), then issue Status and assert latency bound
    auto long_stream = std::async(std::launch::async, [&]() {
        yams::daemon::SearchRequest req;
        req.query = "test";
        req.limit = 1000; // heavy response
        auto res = yams::cli::run_sync(client.streamingSearch(req), 30s);
        return res;
    });

    std::this_thread::sleep_for(200ms);

    auto t0 = std::chrono::steady_clock::now();
    auto sres = yams::cli::run_sync(client.status(), 5s);
    auto dt_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0)
            .count();

    ASSERT_TRUE(sres);
    (void)sres.value();
    EXPECT_LT(dt_ms, 2000) << "StatusRequest exceeded expected latency under budget: " << dt_ms
                           << " ms";

    (void)long_stream.get();
}
