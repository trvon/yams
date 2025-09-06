#include <gtest/gtest.h>
#include <yams/cli/async_bridge.h>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/daemon_client.h>

// Minimal integration: ensure the request object can be sent and a response decoded.
// This test expects the daemon to be available via the configured socket.

TEST(DaemonPrepareSession, RequestResponseRoundTrip) {
    using namespace yams::daemon;
    DaemonClient client;
    auto c = client.connect();
    if (!c) {
        GTEST_SKIP() << "Daemon not running; skipping prepare-session roundtrip";
    }
    PrepareSessionRequest req;
    req.sessionName = ""; // current session
    req.cores = -1;
    req.memoryGb = -1;
    req.timeMs = -1;
    req.aggressive = false;
    req.limit = 1;
    req.snippetLen = 80;

    auto resp = yams::cli::run_sync(client.call<PrepareSessionRequest>(req));
    ASSERT_TRUE(resp);
    EXPECT_GE(resp.value().warmedCount, 0u);
}
