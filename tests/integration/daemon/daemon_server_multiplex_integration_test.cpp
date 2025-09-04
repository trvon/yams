// Integration tests for server-side multiplexing on a single AF_UNIX connection.
// Ensure placement-new and string APIs are visible and not macro-polluted
#include <new>
#include <string>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

// Defensively undef potential stray macros that could break STL
#ifdef size
#undef size
#endif
#ifdef resize
#undef resize
#endif

#include <yams/cli/async_bridge.h>
#include <yams/daemon/client/daemon_client.h>

using namespace std::chrono_literals;

namespace {

bool daemon_available() {
    return yams::daemon::DaemonClient::isDaemonRunning();
}

} // namespace

TEST(ServerMultiplexIntegrationTest, ManyParallelStatusRequests) {
    if (!daemon_available()) {
        GTEST_SKIP() << "Daemon not available for integration tests";
    }

    yams::daemon::ClientConfig cfg;
    cfg.requestTimeout = 3s;
    cfg.headerTimeout = 3s;
    cfg.bodyTimeout = 3s;
    cfg.maxInflight = 128; // client cap
    yams::daemon::DaemonClient client(cfg);

    const int N = 50;
    std::vector<std::future<bool>> futs;
    futs.reserve(N);

    for (int i = 0; i < N; ++i) {
        futs.emplace_back(std::async(std::launch::async, [&client]() {
            auto res = yams::cli::run_sync(client.status(), 3s);
            return static_cast<bool>(res);
        }));
    }

    int ok = 0;
    for (auto& f : futs) {
        if (f.get())
            ok++;
    }
    EXPECT_EQ(ok, N);
}
