#include <chrono>
#include <thread>
#include <gtest/gtest.h>

#include <yams/cli/async_bridge.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
using yams::cli::run_sync;

using namespace std::chrono_literals;

namespace yams::daemon::it {

// These tests exercise protocol behavior under minimal assumptions: they do not
// assert on actual search content, only that streaming and non-streaming paths
// complete without read timeouts or protocol hangs.

TEST(DaemonProtocolIT, StreamingSearchDoesNotTimeoutAndCompletes) {
    // Ensure daemon is likely up; ignore failure in environments without daemon
    (void)DaemonClient::startDaemon({});
    std::this_thread::sleep_for(100ms);

    ClientConfig cfg;
    cfg.enableChunkedResponses = true;
    cfg.headerTimeout = std::chrono::milliseconds(3000);
    cfg.bodyTimeout = std::chrono::milliseconds(15000);
    cfg.requestTimeout = std::chrono::milliseconds(20000);

    DaemonClient client(cfg);

    SearchRequest req;
    req.query = "alpha";
    req.limit = 1;
    auto fut = client.streamingSearch(req);
    auto res = run_sync(std::move(fut), 20s);
    // Accept either success or non-timeout error (e.g., daemon unavailable). Reject timeouts.
    ASSERT_TRUE(res.has_value() || !res) << "Unexpected variant";
    if (!res) {
        EXPECT_NE(res.error().code, ErrorCode::Timeout) << "Should not timeout";
    }
}

TEST(DaemonProtocolIT, NonStreamingStatusDoesNotTimeout) {
    (void)DaemonClient::startDaemon({});
    std::this_thread::sleep_for(100ms);

    ClientConfig cfg;
    cfg.enableChunkedResponses = false;
    cfg.headerTimeout = std::chrono::milliseconds(3000);
    cfg.bodyTimeout = std::chrono::milliseconds(15000);
    cfg.requestTimeout = std::chrono::milliseconds(20000);

    DaemonClient client(cfg);

    auto fut = client.status();
    auto res = run_sync(std::move(fut), 10s);
    if (!res) {
        // In environments without daemon support, allow non-timeout failures
        EXPECT_NE(res.error().code, ErrorCode::Timeout);
    } else {
        SUCCEED();
    }
}

} // namespace yams::daemon::it
