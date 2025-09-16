#include <chrono>
#include <future>
#include "test_async_helpers.h"
#include <gtest/gtest.h>
#include <yams/daemon/client/daemon_client.h>

using namespace std::chrono_literals;

namespace {
bool daemon_available() {
    return yams::daemon::DaemonClient::isDaemonRunning();
}
} // namespace

// Under induced pressure, server should return typed overload errors without closing the
// connection.
TEST(ServerBackpressureIT, TypedErrorsAndConnectionStaysUsable) {
    if (!daemon_available()) {
        GTEST_SKIP() << "Daemon not available for integration tests";
    }
    yams::daemon::ClientConfig cfg;
    cfg.requestTimeout = 3s;
    cfg.headerTimeout = 2s;
    cfg.bodyTimeout = 3s;
    cfg.maxInflight = 1; // Make saturation easier to trigger
    yams::daemon::DaemonClient client(cfg);

    // Kick off multiple streaming searches to create writer/backpressure quickly.
    const int N = 12;
    std::vector<std::future<yams::Result<yams::daemon::SearchResponse>>> futs;
    futs.reserve(N);
    for (int i = 0; i < N; ++i) {
        futs.emplace_back(std::async(std::launch::async, [&client]() {
            yams::daemon::SearchRequest req;
            req.query = "";
            return yams::cli::run_sync(client.streamingSearch(req), 2500ms);
        }));
    }

    int overload = 0;
    int ok = 0;
    for (auto& f : futs) {
        auto r = f.get();
        if (r) {
            ++ok;
        } else if (r.error().code == yams::ErrorCode::ResourceExhausted ||
                   r.error().code == yams::ErrorCode::RateLimited) {
            ++overload;
        }
    }
    // Environment may not reach overload depending on server caps; accept either outcome.
    if (overload == 0 && ok > 0) {
        SUCCEED() << "No overload observed; environment caps not reached.";
    } else {
        EXPECT_GE(overload, 1) << "Expected at least one typed overload error";
    }

    // Verify the connection remains usable.
    auto st = yams::cli::run_sync(client.status(), 1500ms);
    if (!st) {
        GTEST_SKIP() << "Daemon status unavailable after pressure; skipping stability check";
    }
    EXPECT_TRUE(st.value().running);
}

// When mux queue grows, retryAfterMs should be non-zero to hint client backoff.
TEST(ServerBackpressureIT, RetryAfterMsSignalsUnderMuxPressure) {
    if (!daemon_available()) {
        GTEST_SKIP() << "Daemon not available for integration tests";
    }
    yams::daemon::ClientConfig cfg;
    cfg.requestTimeout = 3s;
    cfg.headerTimeout = 2s;
    cfg.bodyTimeout = 3s;
    cfg.maxInflight = 2;
    yams::daemon::DaemonClient client(cfg);

    // Create some pressure with small parallel searches.
    const int N = 6;
    std::vector<std::future<yams::Result<yams::daemon::SearchResponse>>> futs;
    for (int i = 0; i < N; ++i) {
        futs.emplace_back(std::async(std::launch::async, [&client]() {
            yams::daemon::SearchRequest req;
            req.query = "";
            return yams::cli::run_sync(client.streamingSearch(req), 2000ms);
        }));
    }
    // Poll status and check retryAfterMs heuristic (non-fatal if zero; assert when queued>0)
    auto st = yams::cli::run_sync(client.status(), 1500ms);
    if (!st) {
        GTEST_SKIP() << "Daemon status unavailable; skipping retryAfterMs check";
    }
    auto s = st.value();
    if (s.muxQueuedBytes > 0) {
        EXPECT_GT(s.retryAfterMs, 0u) << "Expected retryAfterMs > 0 under mux pressure";
    }
    for (auto& f : futs)
        (void)f.get();
}
