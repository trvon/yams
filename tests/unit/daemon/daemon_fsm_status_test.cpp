#include <cstdlib>
#include "../../integration/daemon/test_async_helpers.h"
#include <boost/asio/awaitable.hpp>
#include <gtest/gtest.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>

using namespace std::chrono_literals;

TEST(DaemonFsmStatusTest, DISABLED_StatusExportsFsmFieldsWhenDetailed) {
    // Ensure vector subsystem is bypassed so sqlite-vec module is not required in tests
    setenv("YAMS_DISABLE_VECTORS", "1", 1);
    setenv("YAMS_SQLITE_VEC_SKIP_INIT", "1", 1);
    setenv("YAMS_SQLITE_MINIMAL_PRAGMAS", "1", 1);
    setenv("YAMS_VDB_IN_MEMORY", "1", 1);
    yams::daemon::DaemonConfig cfg;
    cfg.workerThreads = 2;
    yams::daemon::YamsDaemon d(cfg);
    ASSERT_TRUE(d.start());

    yams::daemon::DaemonClient client{};

    // Poll status until at least Degraded/Ready
    yams::daemon::StatusResponse s{};
    for (int i = 0; i < 30; ++i) {
        auto st = yams::cli::run_sync(client.status(), 2s);
        ASSERT_TRUE(st);
        s = st.value();
        if (s.overallStatus == "Ready" || s.overallStatus == "Degraded" || s.ready)
            break;
        std::this_thread::sleep_for(100ms);
    }

    // FSM numeric fields should be present in requestCounts
    ASSERT_NE(s.requestCounts.find("service_fsm_state"), s.requestCounts.end());
    ASSERT_NE(s.requestCounts.find("embedding_state"), s.requestCounts.end());
    ASSERT_NE(s.requestCounts.find("plugin_host_state"), s.requestCounts.end());
    // Readiness booleans should be present
    ASSERT_NE(s.readinessStates.find("embedding_ready"), s.readinessStates.end());
    ASSERT_NE(s.readinessStates.find("plugins_ready"), s.readinessStates.end());

    ASSERT_TRUE(d.stop());
}
