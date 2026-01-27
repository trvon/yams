// Data-dir single-instance enforcement integration tests
// Validates that two daemons cannot share the same data-dir

#define CATCH_CONFIG_MAIN
#include <spdlog/spdlog.h>
#include <filesystem>
#include <fstream>
#include <memory>
#include <thread>
#include "test_async_helpers.h"
#include "test_daemon_harness.h"
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>
#include <yams/compat/unistd.h>
#include <yams/daemon/client/daemon_client.h>

using namespace yams::daemon;
using namespace yams::test;
using namespace std::chrono_literals;

namespace {

// Custom harness that allows specifying a shared data directory
class SharedDataDirHarness {
public:
    explicit SharedDataDirHarness(const std::filesystem::path& sharedDataDir,
                                  const std::string& suffix = "")
        : sharedDataDir_(sharedDataDir) {
        namespace fs = std::filesystem;
        auto id = random_id() + suffix;

        // Use shared data-dir but unique socket/pid paths
#ifdef _WIN32
        sock_ = fs::temp_directory_path() / ("daemon_shared_" + id + ".sock");
#else
        sock_ = std::filesystem::path("/tmp") / ("daemon_shared_" + id + ".sock");
#endif
        auto root = fs::temp_directory_path() / ("yams_shared_" + id);
        fs::create_directories(root);
        pid_ = root / ("daemon_" + id + ".pid");
        log_ = root / ("daemon_" + id + ".log");
        root_ = root;

        // Ensure shared data dir exists
        fs::create_directories(sharedDataDir_);
    }

    ~SharedDataDirHarness() {
        stop();
        cleanup();
    }

    bool start(std::chrono::milliseconds timeout = 10s) {
        DaemonConfig cfg;
        cfg.dataDir = sharedDataDir_; // Use shared data directory
        cfg.socketPath = sock_;
        cfg.pidFile = pid_;
        cfg.logFile = log_;
        cfg.enableModelProvider = true;
        cfg.useMockModelProvider = true;
        cfg.autoLoadPlugins = false;

        daemon_ = std::make_unique<YamsDaemon>(cfg);

        auto s = daemon_->start();
        if (!s) {
            spdlog::error("[SharedDataDirHarness] daemon_->start() failed: {}", s.error().message);
            startError_ = s.error().message;
            return false;
        }

        runLoopThread_ = std::thread([this]() { daemon_->runLoop(); });
        std::this_thread::sleep_for(50ms);

        // Poll for Ready state
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(100ms);
            auto lifecycle = daemon_->getLifecycle().snapshot();
            if (lifecycle.state == LifecycleState::Ready) {
                spdlog::info("[SharedDataDirHarness] Daemon ready at socket: {}", sock_.string());
                return true;
            } else if (lifecycle.state == LifecycleState::Failed) {
                spdlog::error("[SharedDataDirHarness] Daemon failed: {}", lifecycle.lastError);
                return false;
            }
        }
        spdlog::error("[SharedDataDirHarness] Timeout waiting for Ready state");
        return false;
    }

    void stop() {
        if (daemon_) {
            daemon_->stop();
            if (runLoopThread_.joinable()) {
                runLoopThread_.join();
            }
            yams::daemon::AsioConnectionPool::shutdown_all(500ms);
#ifndef _WIN32
            yams::daemon::GlobalIOContext::safe_restart();
#endif
            daemon_.reset();
            std::this_thread::sleep_for(500ms);
        }
    }

    const std::filesystem::path& socketPath() const { return sock_; }
    const std::filesystem::path& dataDir() const { return sharedDataDir_; }
    const std::string& startError() const { return startError_; }
    YamsDaemon* daemon() const { return daemon_.get(); }

private:
    static std::string random_id() {
        static const char* cs = "abcdefghijklmnopqrstuvwxyz0123456789";
        thread_local std::mt19937_64 rng{std::random_device{}()};
        std::uniform_int_distribution<size_t> dist(0, 35);
        std::string out;
        out.reserve(8);
        for (int i = 0; i < 8; ++i)
            out.push_back(cs[dist(rng)]);
        return out;
    }

    void cleanup() {
        namespace fs = std::filesystem;
        std::error_code ec;
        if (!root_.empty())
            fs::remove_all(root_, ec);
    }

    std::unique_ptr<YamsDaemon> daemon_;
    std::thread runLoopThread_;
    std::filesystem::path sharedDataDir_;
    std::filesystem::path root_, sock_, pid_, log_;
    std::string startError_;
};

} // namespace

TEST_CASE("Data-dir lock prevents concurrent access", "[daemon][lifecycle][single-instance]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    // Create a shared data directory for both daemons
    auto sharedDataDir =
        std::filesystem::temp_directory_path() / ("yams_shared_data_" + std::to_string(getpid()));
    std::filesystem::create_directories(sharedDataDir);

    // Cleanup on exit
    struct Cleanup {
        std::filesystem::path dir;
        ~Cleanup() {
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
        }
    } cleanup{sharedDataDir};

    SECTION("second daemon takes over from first") {
        // Use a scope-managed pointer to allow early cleanup
        auto harness1 = std::make_unique<SharedDataDirHarness>(sharedDataDir, "_first");
        REQUIRE(harness1->start());

        // Verify first daemon is running
        REQUIRE(std::filesystem::exists(harness1->socketPath()));

        // Start second daemon with same data-dir (should trigger takeover)
        SharedDataDirHarness harness2(sharedDataDir, "_second");

        // Second daemon should successfully start (after taking over from first)
        // This tests the "newer daemon takes precedence" behavior
        bool started = harness2.start(20s);

        // Clean up harness1 before assertions - it was already terminated by takeover
        // so its internal state may be inconsistent
        harness1.reset();

        // Wait for resources to settle
        std::this_thread::sleep_for(500ms);

        // Second daemon should have started successfully
        REQUIRE(started);

        // First daemon should have been shut down
        // (We can't easily verify this directly, but the lock acquisition succeeded)

        harness2.stop();
    }

    SECTION("lock file is created in data-dir") {
        SharedDataDirHarness harness(sharedDataDir, "_lock_test");
        REQUIRE(harness.start());

        // Verify lock file exists
        auto lockFile = sharedDataDir / ".yams-lock";
        REQUIRE(std::filesystem::exists(lockFile));

        // Verify lock file contains valid JSON with our PID
        std::ifstream f(lockFile);
        std::string content((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
        REQUIRE(!content.empty());
        REQUIRE(content.find("\"pid\"") != std::string::npos);
        REQUIRE(content.find("\"socket\"") != std::string::npos);

        harness.stop();
    }
}

TEST_CASE("Data-dir lock released on shutdown", "[daemon][lifecycle][single-instance]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    auto sharedDataDir =
        std::filesystem::temp_directory_path() / ("yams_release_test_" + std::to_string(getpid()));
    std::filesystem::create_directories(sharedDataDir);

    struct Cleanup {
        std::filesystem::path dir;
        ~Cleanup() {
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
        }
    } cleanup{sharedDataDir};

    SECTION("new daemon can start after previous shutdown") {
        {
            SharedDataDirHarness harness1(sharedDataDir, "_release1");
            REQUIRE(harness1.start());
            harness1.stop();
        }

        // Allow time for cleanup
        std::this_thread::sleep_for(500ms);

        {
            SharedDataDirHarness harness2(sharedDataDir, "_release2");
            // Should start immediately without needing takeover
            REQUIRE(harness2.start(10s));
            harness2.stop();
        }
    }
}
