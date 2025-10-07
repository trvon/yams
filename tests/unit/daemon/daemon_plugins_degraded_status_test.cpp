// Intentionally DISABLED_ until daemon test environment is stable on this host.
#include <chrono>
#include <thread>
#include "../../integration/daemon/test_async_helpers.h"
#include <boost/asio/awaitable.hpp>
#include <gtest/gtest.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>

using namespace std::chrono_literals;
using namespace yams::daemon;

TEST(DaemonPluginsDegradedStatusTest, DISABLED_StatusIncludesPluginsDegradedWhenTrustFails) {
    // Bypass vector/sqlite-vec paths to avoid flakiness in CI/macOS
    setenv("YAMS_DISABLE_VECTORS", "1", 1);
    setenv("YAMS_SQLITE_VEC_SKIP_INIT", "1", 1);
    setenv("YAMS_SQLITE_MINIMAL_PRAGMAS", "1", 1);
    setenv("YAMS_VDB_IN_MEMORY", "1", 1);

    // Force plugin trust failure by using a world-writable directory (/tmp) as pluginDir.
    DaemonConfig cfg;
    cfg.workerThreads = 2;
    cfg.pluginDir = "/tmp";      // parent is world-writable; trustAdd should fail
    cfg.autoLoadPlugins = false; // trust check occurs during ServiceManager construction

    YamsDaemon daemon(cfg);
    ASSERT_TRUE(daemon.start());

    DaemonClient client{};
    StatusResponse s{};
    bool sawFlag = false;
    for (int i = 0; i < 20; ++i) {
        auto st = yams::cli::run_sync(client.status(), 2s);
        ASSERT_TRUE(st);
        s = st.value();
        auto it = s.readinessStates.find("plugins_degraded");
        if (it != s.readinessStates.end() && it->second) {
            sawFlag = true;
            break;
        }
        std::this_thread::sleep_for(100ms);
    }
    EXPECT_TRUE(sawFlag);

    ASSERT_TRUE(daemon.stop());
}
