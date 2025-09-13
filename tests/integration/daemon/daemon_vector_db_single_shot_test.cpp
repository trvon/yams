#include "test_daemon_harness.h"
#include <gtest/gtest.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>

using namespace std::chrono_literals;

TEST(DaemonVectorDbInit, SingleShotSkipsSecondAttempt) {
    yams::test::DaemonHarness h;
    ASSERT_TRUE(h.start(3s));

    // Invoke the init guard a second time; should skip in-process and return false
    auto* sm = h.daemon()->getServiceManager();
    ASSERT_NE(sm, nullptr);
    auto r = sm->__test_forceVectorDbInitOnce(h.dataDir());
    ASSERT_TRUE(r);
    EXPECT_FALSE(r.value());
}

TEST(DaemonVectorDbInit, LockBusyAcrossProcessesSkips) {
    // Start the first daemon (holds the advisory lock)
    yams::test::DaemonHarness h1;
    ASSERT_TRUE(h1.start(3s));

    // Fork a child process that starts a second daemon pointed at the same data dir
    pid_t pid = fork();
    ASSERT_GE(pid, 0) << "fork failed";
    if (pid == 0) {
        // Child: start a second daemon with distinct IPC paths but same dataDir
        yams::daemon::DaemonConfig cfg;
        cfg.dataDir = h1.dataDir();
        cfg.enableModelProvider = true;
        cfg.autoLoadPlugins = false;
        // Unique socket/pid/log files
        auto p = h1.dataDir().parent_path();
        auto sock = p / "child.sock";
        auto pidf = p / "child.pid";
        auto logf = p / "child.log";
        cfg.socketPath = sock;
        cfg.pidFile = pidf;
        cfg.logFile = logf;
        yams::daemon::YamsDaemon d(cfg);
        auto ok = d.start();
        if (!ok)
            _exit(2);
        // Poll once for status (ensures it came up despite lock busy)
        yams::daemon::ClientConfig cc;
        cc.socketPath = sock;
        cc.autoStart = false;
        yams::daemon::DaemonClient c(cc);
        auto s = yams::cli::run_sync(c.status(), 1500ms);
        if (!s)
            _exit(3);
        (void)d.stop();
        _exit(0);
    }
    int status = 0;
    waitpid(pid, &status, 0);
    EXPECT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}
