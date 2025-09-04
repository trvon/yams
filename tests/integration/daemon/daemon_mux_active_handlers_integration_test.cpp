#include <gtest/gtest.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/cli/async_bridge.h>
#include <future>
#include <chrono>

using namespace std::chrono_literals;
namespace { bool daemon_available() { return yams::daemon::DaemonClient::isDaemonRunning(); } }

TEST(ServerMultiplexIntegrationTest, MuxActiveHandlersDuringStream) {
    if (!daemon_available()) {
        GTEST_SKIP() << "Daemon not available for integration tests";
    }
    // Optional: only run when enabled to reduce flakiness
    const char* en = std::getenv("YAMS_TEST_MUX_ACTIVE");
    if (!(en && std::string(en) == "1")) {
        GTEST_SKIP() << "Set YAMS_TEST_MUX_ACTIVE=1 to run";
    }

    yams::daemon::ClientConfig cfg;
    cfg.requestTimeout = 10s; cfg.headerTimeout = 5s; cfg.bodyTimeout = 10s; cfg.maxInflight = 64;
    yams::daemon::DaemonClient client(cfg);

    // Start a streaming search
    auto fut = std::async(std::launch::async, [&]() {
        yams::daemon::SearchRequest req; req.query = "test"; req.limit = 2000;
        return yams::cli::run_sync(client.streamingSearch(req), 30s);
    });

    std::this_thread::sleep_for(200ms);
    auto st = yams::cli::run_sync(client.status(), 3s);
    ASSERT_TRUE(st);
    const auto& s = st.value();
    EXPECT_GE(s.muxActiveHandlers, 1u);
    (void)fut.get();
}
