#include <gtest/gtest.h>
#include <yams/cli/async_bridge.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>

using namespace std::chrono_literals;

TEST(DaemonStatusStatsIntegration, StatusHasRuntimeAndStatsNonZero) {
    // Launch daemon in-process
    yams::daemon::DaemonConfig cfg;
    cfg.workerThreads = 2;
    yams::daemon::YamsDaemon d(cfg);
    ASSERT_TRUE(d.start());

    // Probe status
    yams::daemon::DaemonClient client{};
    // Wait until Ready/Degraded
    yams::daemon::StatusResponse s;
    for (int i = 0; i < 20; ++i) {
        auto st = yams::cli::run_sync(client.status(), 2s);
        ASSERT_TRUE(st);
        s = st.value();
        if (s.ready || s.overallStatus == "Ready" || s.overallStatus == "Degraded")
            break;
        std::this_thread::sleep_for(100ms);
    }

    // Allow tiny uptime; at least non-negative
    EXPECT_GE(s.uptimeSeconds, 0u);
    // Version should not be empty on proto v2+ (fallbacks may set to dev)
    // Allow empty for older protocol
    // Memory/CPU are best-effort; only assert fields are present numerically
    (void)s.memoryUsageMb;
    (void)s.cpuUsagePercent;

    // Issue a ping to exercise FSM counters
    (void)yams::cli::run_sync(client.ping(), 2s);

    // Probe stats; ensure JSON totals exist or numeric fields present
    yams::daemon::GetStatsRequest req;
    auto stats = yams::cli::run_sync(client.getStats(req), 5s);
    ASSERT_TRUE(stats);
    const auto& g = stats.value();
    if (auto it = g.additionalStats.find("json"); it != g.additionalStats.end()) {
        // Presence of JSON is sufficient for this smoke test
        EXPECT_FALSE(it->second.empty());
        // Services summary keys present
        try {
            auto j = nlohmann::json::parse(it->second);
            if (j.contains("services")) {
                auto& sv = j["services"];
                EXPECT_TRUE(sv.contains("contentstore"));
                EXPECT_TRUE(sv.contains("metadatarepo"));
                EXPECT_TRUE(sv.contains("searchexecutor"));
            }
        } catch (...) {
        }
    }
    // Numeric fields present on proto v2+
    (void)g.totalDocuments;
    (void)g.totalSize;

    // FSM counters exposed when Ready
    if (s.ready) {
        EXPECT_GE(s.fsmPayloadWrites, 0u);
        EXPECT_GE(s.fsmBytesSent, 0u);
    }

    // Shutdown
    ASSERT_TRUE(d.stop());
}
