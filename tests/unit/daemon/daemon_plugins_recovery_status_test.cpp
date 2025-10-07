// DISABLED_: Exercises daemon status plugins_degraded recovery using ServiceManager test hooks.
#include <chrono>
#include <thread>
#include <boost/asio/awaitable.hpp>
#include <gtest/gtest.h>

#include "../../integration/daemon/test_async_helpers.h"
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>

using namespace std::chrono_literals;
using namespace yams::daemon;

TEST(DaemonPluginsRecoveryStatus, DISABLED_RecoveryClearsPluginsDegradedFlag) {
    setenv("YAMS_DISABLE_VECTORS", "1", 1);
    setenv("YAMS_SQLITE_VEC_SKIP_INIT", "1", 1);
    setenv("YAMS_SQLITE_MINIMAL_PRAGMAS", "1", 1);
    setenv("YAMS_VDB_IN_MEMORY", "1", 1);

    DaemonConfig cfg;
    cfg.workerThreads = 2;
    YamsDaemon daemon(cfg);
    ASSERT_TRUE(daemon.start());

    // Induce plugin host failure via test hook, then verify status shows plugins_degraded
    auto* sm = daemon.getServiceManager();
    ASSERT_NE(sm, nullptr);
    sm->__test_pluginLoadFailed("unit-test failure");

    DaemonClient client{};
    auto st1 = yams::cli::run_sync(client.status(), 2s);
    ASSERT_TRUE(st1);
    auto s1 = st1.value();
    auto it1 = s1.readinessStates.find("plugins_degraded");
    ASSERT_NE(it1, s1.readinessStates.end());
    EXPECT_TRUE(it1->second);

    // Now recover by marking scan complete with >0 loaded
    sm->__test_pluginScanComplete(1);
    auto st2 = yams::cli::run_sync(client.status(), 2s);
    ASSERT_TRUE(st2);
    auto s2 = st2.value();
    auto it2 = s2.readinessStates.find("plugins_degraded");
    ASSERT_NE(it2, s2.readinessStates.end());
    EXPECT_FALSE(it2->second);

    ASSERT_TRUE(daemon.stop());
}
