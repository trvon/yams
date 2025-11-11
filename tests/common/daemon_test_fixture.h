// Reusable test fixture for daemon-based integration tests with proper isolation.
#pragma once

#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>
#include <gtest/gtest.h>

#include "integration/daemon/test_async_helpers.h"
#include <yams/daemon/daemon.h>
#include <yams/daemon/client/daemon_client.h>
#include "daemon_preflight.h"
#include "fixture_manager.h"

namespace yams::test {

/// Base fixture for tests that require a daemon instance with proper isolation.
/// Provides:
/// - Unique temp directories per test
/// - Daemon lifecycle management with RAII cleanup
/// - Socket path management (short paths for AF_UNIX)
/// - Fixture file management
/// - Automatic environment setup/teardown
class DaemonTestFixture : public ::testing::Test {
protected:
    std::filesystem::path root_;
    std::filesystem::path storageDir_;
    std::filesystem::path runtimeRoot_;
    std::filesystem::path socketPath_;
    std::unique_ptr<FixtureManager> fixtures_;
    std::unique_ptr<yams::daemon::YamsDaemon> daemon_;

    void SetUp() override {
        // Create unique temp directories
        auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        root_ = std::filesystem::temp_directory_path() / ("yams_test_" + unique);
        storageDir_ = root_ / "storage";
        runtimeRoot_ = root_ / "runtime";
        
        std::error_code ec;
        std::filesystem::create_directories(storageDir_, ec);
        ASSERT_FALSE(ec) << "Failed to create storage dir: " << ec.message();
        std::filesystem::create_directories(runtimeRoot_, ec);
        ASSERT_FALSE(ec) << "Failed to create runtime dir: " << ec.message();

        // Initialize fixture manager
        fixtures_ = std::make_unique<FixtureManager>(root_ / "fixtures");

        // Setup daemon environment with preflight
        yams::tests::harnesses::DaemonPreflight::ensure_environment({
            .runtime_dir = runtimeRoot_,
            .socket_name_prefix = "yams-test-",
            .kill_others = false,
        });

        // Get socket path from environment (preflight may have shortened it)
        if (const char* s = std::getenv("YAMS_SOCKET_PATH")) {
            socketPath_ = s;
        } else {
            socketPath_ = runtimeRoot_ / ("yams-test-" + std::to_string(::getpid()) + ".sock");
        }
    }

    void TearDown() override {
        // Stop daemon if running
        if (daemon_) {
            daemon_->stop();
            daemon_.reset();
        }

        // Reset fixture manager
        fixtures_.reset();

        // Cleanup runtime and temp directories
        yams::tests::harnesses::DaemonPreflight::post_test_cleanup(runtimeRoot_);
        
        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
        if (ec) {
            // Log but don't fail test on cleanup error
            std::cerr << "Warning: Failed to cleanup test dir " << root_ 
                      << ": " << ec.message() << std::endl;
        }

        // Clear socket path from environment
        ::unsetenv("YAMS_SOCKET_PATH");
        ::unsetenv("YAMS_DAEMON_SOCKET");
    }

    /// Start daemon with default configuration.
    /// Returns true on success, sets GTEST failure on error.
    /// Waits for daemon to be fully ready before returning.
    bool startDaemon(std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
        yams::daemon::DaemonConfig cfg;
        cfg.dataDir = storageDir_;
        cfg.socketPath = socketPath_;
        cfg.pidFile = root_ / "daemon.pid";
        cfg.logFile = root_ / "daemon.log";
        
        daemon_ = std::make_unique<yams::daemon::YamsDaemon>(cfg);
        auto started = daemon_->start();
        
        if (!started) {
            ADD_FAILURE() << "Failed to start daemon: " << started.error().message;
            return false;
        }
        
        // Poll for daemon to be fully ready by attempting to connect
        auto client = yams::daemon::DaemonClient(
            yams::daemon::ClientConfig{.socketPath = socketPath_,
                                       .connectTimeout = std::chrono::milliseconds(500),
                                       .autoStart = false});

        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            // Try to connect - successful connection means daemon is ready
            auto connectResult =
                yams::cli::run_sync(client.connect(), std::chrono::milliseconds(300));
            if (connectResult) {
                return true;
            }
        }

        ADD_FAILURE() << "Daemon failed to become ready within timeout";
        return false;
    }

    /// Stop daemon and wait for clean shutdown.
    void stopDaemon() {
        if (daemon_) {
            daemon_->stop();
            daemon_.reset();
        }
    }

    /// Get the daemon socket path for client connections.
    const std::filesystem::path& socketPath() const { return socketPath_; }

    /// Get the storage directory path.
    const std::filesystem::path& storageDir() const { return storageDir_; }

    /// Get the fixture manager for creating test files.
    FixtureManager& fixtures() { return *fixtures_; }
};

} // namespace yams::test
