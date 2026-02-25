// Catch2 migration of multiplex_client_test.cpp
// Migration: yams-3s4 (daemon unit tests)

#include <catch2/catch_test_macros.hpp>

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

TEST_CASE("MultiplexClient concurrent unary out of order",
          "[daemon][multiplex][.requires_daemon]") {
    if (!daemon_available()) {
        SKIP("Daemon not available for multiplex tests");
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
        auto res = yams::cli::run_sync(client.status(), 2s);
        if (res)
            ok++;
    }
    REQUIRE(ok == N);
}

TEST_CASE("MultiplexClient concurrent unary parallel", "[daemon][multiplex][.requires_daemon]") {
    if (!daemon_available()) {
        SKIP("Daemon not available for multiplex tests");
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
            auto res = yams::cli::run_sync(client.status(), 3s);
            return res.has_value();
        }));
    }

    int ok = 0;
    for (auto& f : futs) {
        if (f.get())
            ok++;
    }
    REQUIRE(ok == N);
}

TEST_CASE("MultiplexClient non-chunked status smoke", "[daemon][multiplex][.requires_daemon]") {
    if (!daemon_available()) {
        SKIP("Daemon not available for multiplex tests");
    }

    yams::daemon::ClientConfig cfg;
    cfg.requestTimeout = 2s;
    cfg.headerTimeout = 2s;
    cfg.bodyTimeout = 2s;
    cfg.maxInflight = 8;
    yams::daemon::DaemonClient client(cfg);

    auto res = yams::cli::run_sync(client.status(), 2s);
    REQUIRE(res);
    REQUIRE(res.value().running);
    REQUIRE_FALSE(res.value().version.empty());
}

TEST_CASE("MultiplexClient backpressure max inflight saturation",
          "[daemon][multiplex][.requires_daemon]") {
    if (!daemon_available()) {
        SKIP("Daemon not available for multiplex tests");
    }

    yams::daemon::ClientConfig cfg;
    cfg.requestTimeout = 3s;
    cfg.headerTimeout = 3s;
    cfg.bodyTimeout = 3s;
    cfg.maxInflight = 1; // Force immediate saturation
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
    int rate_limited = 0;
    int succeeded = 0;
    for (auto& f : futs) {
        auto r = f.get();
        if (r) {
            succeeded++;
        } else if (r.error().code == yams::ErrorCode::ResourceExhausted) {
            exhausted++;
        } else if (r.error().code == yams::ErrorCode::RateLimited) {
            rate_limited++;
        }
    }

    // Expect at least one exhaustion error when N > maxInflight
    REQUIRE(exhausted + rate_limited >= 1);
    // And at least one success overall
    REQUIRE(succeeded >= 1);
}

TEST_CASE("MultiplexClient interleaved streaming routing",
          "[daemon][multiplex][.requires_daemon][.skip]") {
    if (!daemon_available()) {
        SKIP("Daemon not available for multiplex tests");
    }
    // NOTE: This requires server-side multiplexing (Phase 2)
    SKIP("Requires server multiplexing (Phase 2)");
}

TEST_CASE("MultiplexClient connection drop fails all in flight",
          "[daemon][multiplex][.requires_daemon][.skip]") {
    if (!daemon_available()) {
        SKIP("Daemon not available for multiplex tests");
    }
    SKIP("Connection drop simulation is environment-specific; implement with controlled kill in "
         "integration tests");
}
