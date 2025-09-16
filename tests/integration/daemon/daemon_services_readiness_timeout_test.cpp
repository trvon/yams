#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <thread>

#include "test_async_helpers.h"
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

static void set_default_env() {
    // Stabilize daemon bring-up on CI/macOS
    ::setenv("YAMS_DISABLE_STORE_STATS", "1", 0);
    // Keep search engine build bounded so the daemon surfaces readiness quickly
    ::setenv("YAMS_SEARCH_BUILD_TIMEOUT_MS", "2000", 0); // 2s budget
}

TEST(DaemonServicesReadiness, ManagedServicesReachReadyWithinTimeouts) {
    if (std::getenv("GTEST_DISCOVERY_MODE")) {
        GTEST_SKIP() << "Skipping during test discovery";
    }

    set_default_env();

    fs::path tmp =
        fs::temp_directory_path() / ("yams_services_ready_" + std::to_string(::getpid()));
    fs::create_directories(tmp);
    auto cleanup = [&]() {
        std::error_code ec;
        fs::remove_all(tmp, ec);
    };

    fs::path sock = tmp / "d.sock";
    fs::path pid = tmp / "d.pid";
    fs::path log = tmp / "d.log";
    fs::path data = tmp / "data";
    fs::create_directories(data);

    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = data;
    cfg.socketPath = sock;
    cfg.pidFile = pid;
    cfg.logFile = log;
    cfg.enableModelProvider = true; // mock acceptable
    cfg.useMockModelProvider = true;
    cfg.autoLoadPlugins = false; // avoid dlopen

    yams::daemon::YamsDaemon daemon(cfg);
    ASSERT_TRUE(daemon.start());
    auto guard = std::unique_ptr<void, void (*)(void*)>{
        &daemon, +[](void* d) { (void)static_cast<yams::daemon::YamsDaemon*>(d)->stop(); }};

    yams::daemon::ClientConfig ccfg;
    ccfg.socketPath = sock;
    ccfg.requestTimeout = 10s;
    ccfg.headerTimeout = 10s;
    ccfg.bodyTimeout = 10s;
    ccfg.autoStart = false;
    yams::daemon::DaemonClient client(ccfg);

    // Poll readiness map with explicit caps; assert each subsystem reaches ready within budget.
    auto wait_ready = [&](const std::string& key, std::chrono::milliseconds budget) {
        for (auto i = 0ms; i <= budget; i += 100ms) {
            auto st = yams::cli::run_sync(client.status(), 500ms);
            if (st && st.value().readinessStates.count(key) && st.value().readinessStates.at(key))
                return true;
            std::this_thread::sleep_for(100ms);
        }
        return false;
    };

    EXPECT_TRUE(wait_ready("content_store", 3s));
    EXPECT_TRUE(wait_ready("metadata_repo", 5s));
    EXPECT_TRUE(wait_ready("vector_index", 5s));
    // With bounded search build timeout, search engine either becomes ready or the daemon keeps
    // responding; require ready within a relaxed budget to catch hangs.
    EXPECT_TRUE(wait_ready("search_engine", 8s)) << "search engine did not report ready";
    // Model provider should be available in mock mode
    EXPECT_TRUE(wait_ready("model_provider", 5s));

    cleanup();
}
