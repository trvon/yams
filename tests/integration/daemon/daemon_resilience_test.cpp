#include <atomic>
#include <csignal>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <signal.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include <gtest/gtest.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <yams/daemon/daemon.h>

namespace yams::daemon::integration::test {

namespace fs = std::filesystem;
using namespace std::chrono_literals;

class DaemonResilienceTest : public ::testing::Test {
protected:
    void SetUp() override {
        testDir_ = fs::temp_directory_path() /
                   ("resilience_test_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(testDir_);

        config_.socketPath = testDir_ / "daemon.sock";
        config_.pidFile = testDir_ / "daemon.pid";
        config_.logFile = testDir_ / "daemon.log";
    }

    void TearDown() override {
        if (daemon_) {
            daemon_->stop();
        }

        killOrphanedProcesses();

        std::error_code ec;
        fs::remove_all(testDir_, ec);
    }

    void killOrphanedProcesses() {
        if (fs::exists(config_.pidFile)) {
            std::ifstream pf(config_.pidFile);
            pid_t pid;
            if (pf >> pid && pid > 0) {
                kill(pid, SIGKILL);
                waitpid(pid, nullptr, WNOHANG);
            }
        }
    }

    size_t countOpenFileDescriptors() {
        size_t count = 0;
        fs::path fdDir = "/proc/self/fd";

        if (fs::exists(fdDir)) {
            // Linux
            for ([[maybe_unused]] const auto& entry : fs::directory_iterator(fdDir)) {
                count++;
            }
        } else {
            // macOS fallback - use resource limits
            struct rlimit rlim;
            if (getrlimit(RLIMIT_NOFILE, &rlim) == 0) {
                // Try to count by checking if FDs are valid
                for (int fd = 0; fd < 1024; ++fd) {
                    if (fcntl(fd, F_GETFD) != -1) {
                        count++;
                    }
                }
            }
        }

        return count;
    }

    size_t getMemoryUsage() {
        // Simple memory usage check
        struct rusage usage;
        if (getrusage(RUSAGE_SELF, &usage) == 0) {
            return usage.ru_maxrss; // In KB on Linux, bytes on macOS
        }
        return 0;
    }

    fs::path testDir_;
    DaemonConfig config_;
    std::unique_ptr<YamsDaemon> daemon_;
};

// Test recovery from SIGKILL focusing on PID file lifecycle (no in-process IPC server)
TEST_F(DaemonResilienceTest, KillRecoveryPidFileLifecycle) {
    // Start daemon in child process
    pid_t daemonPid = fork();
    ASSERT_GE(daemonPid, 0) << "Fork failed";

    if (daemonPid == 0) {
        // Child process - run daemon
        daemon_ = std::make_unique<YamsDaemon>(config_);
        if (daemon_->start()) {
            while (true) {
                std::this_thread::sleep_for(100ms);
            }
        }
        _exit(1);
    }

    // Parent process - wait briefly and verify PID file is created
    std::this_thread::sleep_for(300ms);
    EXPECT_TRUE(fs::exists(config_.pidFile)) << "PID file should exist while daemon runs";

    // Kill daemon with SIGKILL (non-graceful)
    kill(daemonPid, SIGKILL);
    waitpid(daemonPid, nullptr, 0);

    // Stale PID file may exist; starting a new daemon should clean it up
    daemon_ = std::make_unique<YamsDaemon>(config_);
    auto result = daemon_->start();
    ASSERT_TRUE(result) << "Failed to recover after SIGKILL: " << result.error().message;

    // PID file should exist again under new process
    EXPECT_TRUE(fs::exists(config_.pidFile));

    // Stop and ensure PID file is removed
    ASSERT_TRUE(daemon_->stop());
    EXPECT_FALSE(fs::exists(config_.pidFile));
}

// Test file descriptor leak detection (no IPC server, focuses on daemon lifecycle only)
TEST_F(DaemonResilienceTest, StartStopCycles_NoFdLeak) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    ASSERT_TRUE(daemon_->start());
    size_t initialFds = countOpenFileDescriptors();
    ASSERT_TRUE(daemon_->stop());

    // Perform multiple start/stop cycles and ensure FD count is stable
    const int cycles = 20;
    for (int i = 0; i < cycles; ++i) {
        ASSERT_TRUE(daemon_->start());
        ASSERT_TRUE(daemon_->stop());
    }

    size_t finalFds = countOpenFileDescriptors();
    size_t fdGrowth = (finalFds > initialFds) ? (finalFds - initialFds) : 0;
    EXPECT_LT(fdGrowth, 10) << "Possible file descriptor leak detected across start/stop cycles";
}

// Test memory growth over time
TEST_F(DaemonResilienceTest, MemoryGrowth) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    ASSERT_TRUE(daemon_->start());

    size_t initialMemory = getMemoryUsage();

    // No IPC operations here since in-process server is removed; just wait to simulate work
    std::this_thread::sleep_for(500ms);

    size_t finalMemory = getMemoryUsage();

    // Allow some growth but flag excessive growth
    if (initialMemory > 0 && finalMemory > 0) {
        double growthRatio = static_cast<double>(finalMemory) / initialMemory;
        EXPECT_LT(growthRatio, 2.0) << "Memory grew more than 2x";
    }
}

// Test stop() returns promptly
TEST_F(DaemonResilienceTest, ShutdownIsPrompt) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    ASSERT_TRUE(daemon_->start());
    auto start = std::chrono::steady_clock::now();
    ASSERT_TRUE(daemon_->stop());
    auto elapsed = std::chrono::steady_clock::now() - start;
    // With no in-process IPC server, shutdown should still be prompt but allow
    // modest teardown time for background components.
    EXPECT_LT(elapsed, 3s) << "Shutdown should be quick";
}

// Test signal handling
TEST_F(DaemonResilienceTest, RunningFlagToggles) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    ASSERT_FALSE(daemon_->isRunning());
    ASSERT_TRUE(daemon_->start());
    EXPECT_TRUE(daemon_->isRunning());
    ASSERT_TRUE(daemon_->stop());
    EXPECT_FALSE(daemon_->isRunning());
}

// Resource pressure simulation without IPC: create/delete many small files while daemon runs
TEST_F(DaemonResilienceTest, ResourcePressure_FileChurn) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    ASSERT_TRUE(daemon_->start());

    std::vector<fs::path> files;
    for (int i = 0; i < 200; ++i) {
        auto p = testDir_ / ("tmp_" + std::to_string(i));
        {
            std::ofstream ofs(p);
            ofs << "data";
        }
        files.push_back(p);
    }

    // Remove them
    for (auto& p : files) {
        std::error_code ec;
        fs::remove(p, ec);
    }

    EXPECT_TRUE(daemon_->isRunning());
    ASSERT_TRUE(daemon_->stop());
}

// Test "upgrade" scenario: restart resets uptime (no IPC clients involved)
TEST_F(DaemonResilienceTest, UpgradeRestartResetsUptime) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    ASSERT_TRUE(daemon_->start());

    auto startTimeBefore = daemon_->getState().stats.startTime;
    ASSERT_TRUE(daemon_->stop());

    std::this_thread::sleep_for(200ms);

    daemon_ = std::make_unique<YamsDaemon>(config_);
    ASSERT_TRUE(daemon_->start());
    auto startTimeAfter = daemon_->getState().stats.startTime;

    EXPECT_GT(startTimeAfter, startTimeBefore);
}

// Test concurrent state checks while daemon runs
TEST_F(DaemonResilienceTest, ConcurrentStateChecks) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    ASSERT_TRUE(daemon_->start());

    const int numThreads = 8;
    std::atomic<bool> stopFlag{false};
    std::vector<std::thread> threads;
    threads.reserve(numThreads);
    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([this, &stopFlag]() {
            while (!stopFlag) {
                (void)daemon_->isRunning();
                std::this_thread::sleep_for(1ms);
            }
        });
    }

    std::this_thread::sleep_for(500ms);
    stopFlag = true;
    for (auto& th : threads)
        th.join();

    EXPECT_TRUE(daemon_->isRunning());
    ASSERT_TRUE(daemon_->stop());
}

// Error injection over IPC is not applicable without an in-process server; covered elsewhere.

// Test long-running daemon stability
TEST_F(DaemonResilienceTest, LongRunningStability) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    ASSERT_TRUE(daemon_->start());

    // Simulate long-running daemon with periodic checks
    const int numIterations = 50;
    for (int i = 0; i < numIterations; ++i) {
        EXPECT_TRUE(daemon_->isRunning());

        std::this_thread::sleep_for(50ms);
    }
    EXPECT_TRUE(daemon_->isRunning());
}

// Abrupt client disconnect test not applicable without in-process IPC server.

} // namespace yams::daemon::integration::test
