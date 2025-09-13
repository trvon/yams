#include "test_daemon_harness.h"
#include <gtest/gtest.h>
#include <yams/daemon/client/daemon_client.h>

using namespace std::chrono_literals;

TEST(DaemonSearchNoEmbeddings, ProgressCompletesAndVectorsDisabled) {
    // Force vectors disabled
#ifdef __APPLE__
    ::setenv("YAMS_DISABLE_VECTORS", "1", 1);
#else
    setenv("YAMS_DISABLE_VECTORS", "1", 1);
#endif
    yams::test::DaemonHarness h;
    ASSERT_TRUE(h.start(3s));

    yams::daemon::ClientConfig cc;
    cc.socketPath = h.socketPath();
    cc.autoStart = false;
    yams::daemon::DaemonClient client(cc);

    // Wait for search engine ready
    bool ready = false;
    for (int i = 0; i < 100; ++i) {
        auto s = yams::cli::run_sync(client.status(), 500ms);
        if (s && s->readinessStates["search_engine"]) {
            ready = true;
            break;
        }
        std::this_thread::sleep_for(50ms);
    }
    ASSERT_TRUE(ready) << "search engine not ready";

    auto s = yams::cli::run_sync(client.status(), 1s);
    ASSERT_TRUE(s);
    // Vector scoring should be disabled explicitly
    ASSERT_TRUE(s->readinessStates.count("vector_scoring_enabled"));
    EXPECT_FALSE(s->readinessStates.at("vector_scoring_enabled"));

    // GetStats should echo the same flag for CLI tooling
    yams::daemon::GetStatsRequest gs;
    auto stats = yams::cli::run_sync(client.getStats(gs), 1s);
    ASSERT_TRUE(stats);
    auto it = stats->additionalStats.find("vector_scoring_enabled");
    ASSERT_TRUE(it != stats->additionalStats.end());
    EXPECT_EQ(it->second, std::string("false"));
}
