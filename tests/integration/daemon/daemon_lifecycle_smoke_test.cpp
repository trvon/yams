#include "test_daemon_harness.h"
#include <gtest/gtest.h>
#include <yams/cli/yams_cli.h>

using namespace std::chrono_literals;

TEST(DaemonLifecycleSmoke, StartStatusStop) {
    yams::test::DaemonHarness h;
    ASSERT_TRUE(h.start(2500ms));

    yams::daemon::ClientConfig cc;
    cc.socketPath = h.socketPath();
    cc.autoStart = false;
    yams::daemon::DaemonClient client(cc);

    auto s = yams::cli::run_sync(client.status(), 1s);
    ASSERT_TRUE(s);
    // IPC server up
    EXPECT_TRUE(s->readinessStates["ipc_server"]);
    // Content store usually ready immediately
    EXPECT_TRUE(s->readinessStates["content_store"]);
    // Embedding service may be unavailable in mock mode; acceptable

    h.stop();
}
