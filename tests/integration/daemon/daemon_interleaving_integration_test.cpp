#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <thread>

#include <yams/daemon/client/daemon_client.h>
#include <yams/cli/async_bridge.h>

using namespace std::chrono_literals;

namespace {
bool daemon_available() { return yams::daemon::DaemonClient::isDaemonRunning(); }
}

// This test is optional and requires an environment-provided path that will
// produce a long-running streaming grep. Set:
//   YAMS_TEST_LONG_STREAM=1
//   YAMS_TEST_LONG_STREAM_PATH=/path/with/many/text/files
// The test launches a streaming Grep and, while it is in-flight, issues a
// StatusRequest, asserting the unary completes quickly (interleaving works).
TEST(ServerMultiplexIntegrationTest, Interleaving_LongStreamPlusShortUnary) {
    if (!daemon_available()) {
        GTEST_SKIP() << "Daemon not available for interleaving test";
    }
    const char* enabled = std::getenv("YAMS_TEST_LONG_STREAM");
    const char* path = std::getenv("YAMS_TEST_LONG_STREAM_PATH");
    if (!(enabled && std::string(enabled) == "1") || !path) {
        GTEST_SKIP() << "Set YAMS_TEST_LONG_STREAM=1 and YAMS_TEST_LONG_STREAM_PATH to run";
    }

    yams::daemon::ClientConfig cfg;
    cfg.requestTimeout = 10s;
    cfg.headerTimeout = 5s;
    cfg.bodyTimeout = 10s;
    cfg.maxInflight = 128;
    yams::daemon::DaemonClient client(cfg);

    // Start long-running streaming Grep asynchronously
    auto long_stream = std::async(std::launch::async, [&]() {
        yams::daemon::GrepRequest req;
        req.pattern = "."; // match anything
        req.paths.push_back(path);
        req.recursive = true;
        req.pathsOnly = false;
        req.filesOnly = false;
        req.maxMatches = 0; // no per-file cap
        auto res = yams::cli::run_sync(client.streamingGrep(req), 30s);
        return res;
    });

    // Give the stream a moment to start
    std::this_thread::sleep_for(200ms);

    // Issue a short unary Status request and measure latency
    auto t0 = std::chrono::steady_clock::now();
    yams::daemon::StatusRequest sreq;
    auto sres = yams::cli::run_sync(client.status(), 5s);
    auto dt_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();

    ASSERT_TRUE(sres);
    (void)sres.value();
    // Expect the unary to complete well under 2 seconds even with large stream active
    EXPECT_LT(dt_ms, 2000) << "StatusRequest took too long: " << dt_ms << " ms";

    // Finish streaming (or timeout)
    auto stream_res = long_stream.get();
    (void)stream_res;
}

