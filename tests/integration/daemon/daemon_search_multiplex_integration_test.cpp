#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

#include <yams/cli/async_bridge.h>
#include <yams/daemon/client/daemon_client.h>

using namespace std::chrono_literals;

namespace {
bool daemon_available() {
    return yams::daemon::DaemonClient::isDaemonRunning();
}
} // namespace

TEST(ServerMultiplexIntegrationTest, ManyParallelStreamingSearches) {
    if (!daemon_available()) {
        GTEST_SKIP() << "Daemon not available for integration tests";
    }

    yams::daemon::ClientConfig cfg;
    cfg.requestTimeout = 10s;
    cfg.headerTimeout = 5s;
    cfg.bodyTimeout = 10s;
    cfg.maxInflight = 128; // client-side cap
    yams::daemon::DaemonClient client(cfg);

    // Wait until Ready/Degraded
    for (int i = 0; i < 50; ++i) {
        auto st = yams::cli::run_sync(client.status(), 1s);
        if (st && (st.value().ready || st.value().overallStatus == "Ready" ||
                   st.value().overallStatus == "Degraded"))
            break;
        std::this_thread::sleep_for(100ms);
    }

    const int N = 50;
    std::vector<std::future<bool>> futs;
    futs.reserve(N);

    for (int i = 0; i < N; ++i) {
        futs.emplace_back(std::async(std::launch::async, [&client]() {
            yams::daemon::SearchRequest req;
            req.query = "test"; // benign query
            req.limit = 5;
            auto res = yams::cli::run_sync(client.streamingSearch(req), 10s);
            return static_cast<bool>(res);
        }));
    }

    int ok = 0;
    for (auto& f : futs) {
        if (f.get())
            ok++;
    }
    EXPECT_EQ(ok, N);
}
