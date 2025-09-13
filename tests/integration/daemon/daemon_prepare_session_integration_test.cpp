#include "test_async_helpers.h"
#include "test_daemon_harness.h"
#include <gtest/gtest.h>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/daemon_client.h>

// Minimal integration: ensure the request object can be sent and a response decoded.
// This test expects the daemon to be available via the configured socket.

TEST(DaemonPrepareSession, RequestResponseRoundTrip) {
    yams::test::DaemonHarness h;
    ASSERT_TRUE(h.start(std::chrono::seconds(2)));
    yams::daemon::ClientConfig cc;
    cc.socketPath = h.socketPath();
    cc.autoStart = false;
    yams::daemon::DaemonClient client(cc);
    auto c = yams::cli::run_sync(client.connect(), std::chrono::seconds(1));
    if (!c) {
        GTEST_SKIP() << "Daemon not running; skipping prepare-session roundtrip";
    }
    yams::daemon::PrepareSessionRequest req;
    req.sessionName = ""; // current session
    req.cores = -1;
    req.memoryGb = -1;
    req.timeMs = -1;
    req.aggressive = false;
    req.limit = 1;
    req.snippetLen = 80;

    auto resp = yams::cli::run_sync(client.call<yams::daemon::PrepareSessionRequest>(req));
    ASSERT_TRUE(resp);
    EXPECT_GE(resp.value().warmedCount, 0u);
}
