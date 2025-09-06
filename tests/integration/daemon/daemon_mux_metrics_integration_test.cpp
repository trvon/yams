#include <chrono>
#include <gtest/gtest.h>
#include <yams/cli/async_bridge.h>
#include <yams/daemon/client/daemon_client.h>

using namespace std::chrono_literals;

namespace {
bool daemon_available() {
    return yams::daemon::DaemonClient::isDaemonRunning();
}
} // namespace

TEST(ServerMultiplexIntegrationTest, MuxMetricsExposedInStatus) {
    if (!daemon_available()) {
        GTEST_SKIP() << "Daemon not available for integration tests";
    }
    yams::daemon::ClientConfig cfg;
    cfg.requestTimeout = 2s;
    cfg.headerTimeout = 2s;
    cfg.bodyTimeout = 2s;
    cfg.maxInflight = 8;
    yams::daemon::DaemonClient client(cfg);

    // Wait until Ready/Degraded
    for (int i = 0; i < 50; ++i) {
        auto st = yams::cli::run_sync(client.status(), 1s);
        if (st && (st.value().ready || st.value().overallStatus == "Ready" ||
                   st.value().overallStatus == "Degraded"))
            break;
        std::this_thread::sleep_for(100ms);
    }
    auto res = yams::cli::run_sync(client.status(), 3s);
    ASSERT_TRUE(res);
    const auto& s = res.value();
    // Sanity checks
    EXPECT_GE(s.muxWriterBudgetBytes, 4096u);
    // Active handlers may be zero at idle; ensure queued bytes are consistent
    EXPECT_GE(s.muxQueuedBytes, 0);
}
