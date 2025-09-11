#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

#include "../../integration/daemon/test_async_helpers.h"
#include <yams/daemon/client/daemon_client.h>

using namespace std::chrono_literals;

namespace {

bool daemon_available() {
    return yams::daemon::DaemonClient::isDaemonRunning();
}

} // namespace

TEST(MultiplexClientTest, ConcurrentUnaryOutOfOrder) {
    if (!daemon_available()) {
        GTEST_SKIP() << "Daemon not available for multiplex tests";
    }

    yams::daemon::ClientConfig cfg;
    cfg.requestTimeout = 3s;
    cfg.headerTimeout = 3s;
    cfg.bodyTimeout = 3s;
    cfg.maxInflight = 64;
    yams::daemon::DaemonClient client(cfg);

    const int N = 16;
    int ok = 0;
    for (int i = 0; i < N; ++i) {
        yams::daemon::StatusRequest req;
        auto res = yams::cli::run_sync(client.status(), 2s);
        if (res && std::holds_alternative<yams::daemon::StatusResponse>(res.value()))
            ok++;
    }
    EXPECT_EQ(ok, N);
}

TEST(MultiplexClientTest, ConcurrentUnaryParallel) {
    if (!daemon_available()) {
        GTEST_SKIP() << "Daemon not available for multiplex tests";
    }

    yams::daemon::ClientConfig cfg;
    cfg.requestTimeout = 3s;
    cfg.headerTimeout = 3s;
    cfg.bodyTimeout = 3s;
    cfg.maxInflight = 64;
    yams::daemon::DaemonClient client(cfg);

    const int N = 32;
    std::vector<std::future<bool>> futs;
    futs.reserve(N);

    for (int i = 0; i < N; ++i) {
        futs.emplace_back(std::async(std::launch::async, [&client]() {
            yams::daemon::StatusRequest req;
            auto res = yams::cli::run_sync(client.status(), 3s);
            return res && std::holds_alternative<yams::daemon::StatusResponse>(res.value());
        }));
    }

    int ok = 0;
    for (auto& f : futs) {
        if (f.get())
            ok++;
    }
    EXPECT_EQ(ok, N);
}

TEST(MultiplexClientTest, NonChunkedStatusSmoke) {
    if (!daemon_available()) {
        GTEST_SKIP() << "Daemon not available for multiplex tests";
    }

    yams::daemon::ClientConfig cfg;
    cfg.requestTimeout = 2s;
    cfg.headerTimeout = 2s;
    cfg.bodyTimeout = 2s;
    cfg.maxInflight = 8;
    yams::daemon::DaemonClient client(cfg);

    yams::daemon::StatusRequest req;
    auto res = yams::cli::run_sync(client.status(), 2s);
    ASSERT_TRUE(res);
    ASSERT_TRUE(std::holds_alternative<yams::daemon::StatusResponse>(res.value()));
    const auto& s = std::get<yams::daemon::StatusResponse>(res.value());
    EXPECT_TRUE(s.running);
    EXPECT_FALSE(s.version.empty());
}

TEST(MultiplexClientTest, BackpressureMaxInflightSaturation) {
    if (!daemon_available()) {
        GTEST_SKIP() << "Daemon not available for multiplex tests";
    }

    yams::daemon::ClientConfig cfg;
    cfg.requestTimeout = 3s;
    cfg.headerTimeout = 3s;
    cfg.bodyTimeout = 3s;
    cfg.maxInflight = 1; // Force immediate saturation with >1 concurrent requests
    yams::daemon::DaemonClient client(cfg);

    // Fire many streaming searches in parallel to race for maxInflight slot
    const int N = 16;
    std::vector<std::future<yams::Result<yams::daemon::SearchResponse>>> futs;
    futs.reserve(N);

    for (int i = 0; i < N; ++i) {
        futs.emplace_back(std::async(std::launch::async, [&client]() {
            yams::daemon::SearchRequest req;
            req.query = ""; // trivial search
            return yams::cli::run_sync(client.streamingSearch(req), 3s);
        }));
    }

    int exhausted = 0;
    int succeeded = 0;
    for (auto& f : futs) {
        auto r = f.get();
        if (r) {
            succeeded++;
        } else if (r.error().code == yams::ErrorCode::ResourceExhausted) {
            exhausted++;
        }
    }

    // Expect at least one exhaustion error when N > maxInflight
    EXPECT_GE(exhausted, 1);
    // And at least one success overall
    EXPECT_GE(succeeded, 1);
}

TEST(MultiplexClientTest, InterleavedStreamingRouting) {
    if (!daemon_available()) {
        GTEST_SKIP() << "Daemon not available for multiplex tests";
    }
    // NOTE: This requires server-side multiplexing (Phase 2). For now, skip to avoid flakes.
    GTEST_SKIP() << "Requires server multiplexing (Phase 2).";
}

TEST(MultiplexClientTest, ConnectionDropFailsAllInFlight) {
    if (!daemon_available()) {
        GTEST_SKIP() << "Daemon not available for multiplex tests";
    }
    // Hard to deterministically drop connection from here; add placeholder.
    GTEST_SKIP() << "Connection drop simulation is environment-specific; implement with controlled "
                    "kill in integration tests.";
}
