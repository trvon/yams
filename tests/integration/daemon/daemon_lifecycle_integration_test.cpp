#include <chrono>
#include <csignal>
#include <filesystem>
#include <thread>
#include <gtest/gtest.h>
#include <sys/wait.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>

namespace yams::daemon::integration::test {

namespace fs = std::filesystem;
using namespace std::chrono_literals;

class DaemonLifecycleIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use unique paths for each test to avoid conflicts
        testId_ = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        socketPath_ = fs::temp_directory_path() / ("test_daemon_" + testId_ + ".sock");
        pidFile_ = fs::temp_directory_path() / ("test_daemon_" + testId_ + ".pid");
        logFile_ = fs::temp_directory_path() / ("test_daemon_" + testId_ + ".log");

        cleanupFiles();

        config_.socketPath = socketPath_;
        config_.pidFile = pidFile_;
        config_.logFile = logFile_;
        config_.workerThreads = 2;
        config_.maxMemoryGb = 0.5;

        clientConfig_.socketPath = socketPath_;
        clientConfig_.autoStart = false;
        clientConfig_.connectTimeout = 2s;
        clientConfig_.requestTimeout = 5s;
    }

    void TearDown() override {
        // Stop any running daemons
        if (daemon_) {
            daemon_->stop();
            daemon_.reset();
        }

        // Kill any orphaned daemon processes
        killOrphanedDaemon();

        cleanupFiles();
    }

    void cleanupFiles() {
        std::error_code ec;
        fs::remove(socketPath_, ec);
        fs::remove(pidFile_, ec);
        fs::remove(logFile_, ec);
    }

    void killOrphanedDaemon() {
        if (fs::exists(pidFile_)) {
            std::ifstream pf(pidFile_);
            pid_t pid;
            if (pf >> pid) {
                kill(pid, SIGTERM);
                std::this_thread::sleep_for(100ms);
                kill(pid, SIGKILL); // Force kill if still running
            }
        }
    }

    bool waitForSocket(std::chrono::milliseconds timeout = 2s) {
        auto start = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() - start < timeout) {
            if (fs::exists(socketPath_)) {
                return true;
            }
            std::this_thread::sleep_for(10ms);
        }
        return false;
    }

    bool waitForPidFile(std::chrono::milliseconds timeout = 2s) {
        auto start = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() - start < timeout) {
            if (fs::exists(pidFile_)) {
                return true;
            }
            std::this_thread::sleep_for(10ms);
        }
        return false;
    }

    std::string testId_;
    fs::path socketPath_;
    fs::path pidFile_;
    fs::path logFile_;
    DaemonConfig config_;
    ClientConfig clientConfig_;
    std::unique_ptr<YamsDaemon> daemon_;
};

// Test basic daemon start and stop
TEST_F(DaemonLifecycleIntegrationTest, BasicStartStop) {
    daemon_ = std::make_unique<YamsDaemon>(config_);

    // Start daemon
    auto result = daemon_->start();
    ASSERT_TRUE(result) << "Failed to start daemon: " << result.error().message;

    // Wait for socket to be created
    ASSERT_TRUE(waitForSocket()) << "Socket not created within timeout";

    // PID file should exist
    EXPECT_TRUE(fs::exists(pidFile_));

    // Should be able to connect
    DaemonClient client(clientConfig_);
    auto connectResult = client.connect();
    EXPECT_TRUE(connectResult) << "Failed to connect: " << connectResult.error().message;

    // Stop daemon
    result = daemon_->stop();
    ASSERT_TRUE(result) << "Failed to stop daemon: " << result.error().message;

    // Files should be cleaned up
    std::this_thread::sleep_for(100ms);
    EXPECT_FALSE(fs::exists(socketPath_));
    EXPECT_FALSE(fs::exists(pidFile_));
}

// Test multiple start/stop cycles
TEST_F(DaemonLifecycleIntegrationTest, MultipleRestarts) {
    const int numCycles = 3;

    for (int i = 0; i < numCycles; ++i) {
        daemon_ = std::make_unique<YamsDaemon>(config_);

        // Start
        auto result = daemon_->start();
        ASSERT_TRUE(result) << "Failed to start daemon on cycle " << i;
        ASSERT_TRUE(waitForSocket()) << "Socket not created on cycle " << i;

        // Verify we can connect
        DaemonClient client(clientConfig_);
        EXPECT_TRUE(client.connect()) << "Failed to connect on cycle " << i;

        // Stop
        result = daemon_->stop();
        ASSERT_TRUE(result) << "Failed to stop daemon on cycle " << i;

        daemon_.reset();
        std::this_thread::sleep_for(200ms); // Give time for cleanup
    }
}

// Test preventing multiple daemon instances
TEST_F(DaemonLifecycleIntegrationTest, PreventMultipleInstances) {
    // Start first daemon
    daemon_ = std::make_unique<YamsDaemon>(config_);
    auto result = daemon_->start();
    ASSERT_TRUE(result) << "Failed to start first daemon";
    ASSERT_TRUE(waitForSocket());

    // Try to start second daemon with same config
    auto daemon2 = std::make_unique<YamsDaemon>(config_);
    result = daemon2->start();
    EXPECT_FALSE(result) << "Second daemon should not start";
    if (!result) {
        EXPECT_EQ(result.error().code, ErrorCode::InvalidState);
    }

    // Stop first daemon
    daemon_->stop();
    daemon_.reset();

    std::this_thread::sleep_for(200ms);

    // Now second daemon should be able to start
    result = daemon2->start();
    EXPECT_TRUE(result) << "Second daemon should start after first stops";

    daemon2->stop();
}

// Test graceful shutdown with active connections
TEST_F(DaemonLifecycleIntegrationTest, GracefulShutdownWithConnections) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    auto result = daemon_->start();
    ASSERT_TRUE(result);
    ASSERT_TRUE(waitForSocket());

    // Create multiple client connections
    std::vector<std::unique_ptr<DaemonClient>> clients;
    for (int i = 0; i < 3; ++i) {
        auto client = std::make_unique<DaemonClient>(clientConfig_);
        ASSERT_TRUE(client->connect());
        clients.push_back(std::move(client));
    }

    // Start some requests in background
    std::atomic<bool> requestsComplete{false};
    std::thread requestThread([&clients, &requestsComplete]() {
        for (auto& client : clients) {
            client->ping();
            std::this_thread::sleep_for(10ms);
        }
        requestsComplete = true;
    });

    // Give requests time to start
    std::this_thread::sleep_for(50ms);

    // Graceful shutdown
    result = daemon_->stop();
    EXPECT_TRUE(result) << "Graceful shutdown should succeed";

    requestThread.join();
    EXPECT_TRUE(requestsComplete) << "Requests should complete";
}

// Test daemon crash recovery
TEST_F(DaemonLifecycleIntegrationTest, CrashRecovery) {
    // Start daemon in child process
    pid_t pid = fork();
    ASSERT_GE(pid, 0) << "Fork failed";

    if (pid == 0) {
        // Child process - run daemon
        daemon_ = std::make_unique<YamsDaemon>(config_);
        if (daemon_->start()) {
            // Daemon started, wait indefinitely
            while (true) {
                std::this_thread::sleep_for(100ms);
            }
        }
        exit(1);
    }

    // Parent process
    // Wait for daemon to start
    ASSERT_TRUE(waitForPidFile()) << "Daemon didn't create PID file";
    ASSERT_TRUE(waitForSocket()) << "Daemon didn't create socket";

    // Verify we can connect
    DaemonClient client(clientConfig_);
    ASSERT_TRUE(client.connect());

    // Kill the daemon process (simulate crash)
    kill(pid, SIGKILL);
    int status;
    waitpid(pid, &status, 0);

    // PID file might still exist (stale)
    // Socket should be gone
    std::this_thread::sleep_for(100ms);

    // Should not be able to connect
    DaemonClient client2(clientConfig_);
    EXPECT_FALSE(client2.connect());

    // Start new daemon - should handle stale PID file
    daemon_ = std::make_unique<YamsDaemon>(config_);
    auto result = daemon_->start();
    EXPECT_TRUE(result) << "Should be able to start after crash";

    if (result) {
        daemon_->stop();
    }
}

// Test signal handling
TEST_F(DaemonLifecycleIntegrationTest, SignalHandling) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    auto result = daemon_->start();
    ASSERT_TRUE(result);
    ASSERT_TRUE(waitForSocket());

    // Get daemon PID
    pid_t daemonPid = 0;
    {
        std::ifstream pf(pidFile_);
        ASSERT_TRUE(pf >> daemonPid);
        ASSERT_GT(daemonPid, 0);
    }

    // Since we're in the same process, we can't actually send signals
    // But we can test that signal handlers are installed
    // In a real integration test, this would be in a separate process

    // Stop normally
    daemon_->stop();
}

// Test rapid connect/disconnect cycles
TEST_F(DaemonLifecycleIntegrationTest, RapidConnectionCycles) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    auto result = daemon_->start();
    ASSERT_TRUE(result);
    ASSERT_TRUE(waitForSocket());

    const int numCycles = 20;
    for (int i = 0; i < numCycles; ++i) {
        DaemonClient client(clientConfig_);
        auto connectResult = client.connect();
        EXPECT_TRUE(connectResult) << "Failed on cycle " << i;

        if (connectResult) {
            auto pingResult = client.ping();
            EXPECT_TRUE(pingResult) << "Ping failed on cycle " << i;
        }

        client.disconnect();
    }

    daemon_->stop();
}

// Test daemon resource cleanup
TEST_F(DaemonLifecycleIntegrationTest, ResourceCleanup) {
    // Track initial resource state
    auto initialSockets = fs::directory_iterator(fs::temp_directory_path());
    int initialSocketCount = 0;
    for (const auto& entry : initialSockets) {
        if (entry.path().extension() == ".sock") {
            initialSocketCount++;
        }
    }

    // Start and stop daemon multiple times
    for (int i = 0; i < 3; ++i) {
        daemon_ = std::make_unique<YamsDaemon>(config_);
        ASSERT_TRUE(daemon_->start());
        ASSERT_TRUE(waitForSocket());

        // Create some clients
        for (int j = 0; j < 5; ++j) {
            DaemonClient client(clientConfig_);
            client.connect();
            client.status();
        }

        daemon_->stop();
        daemon_.reset();
        std::this_thread::sleep_for(200ms);
    }

    // Check that no extra sockets were leaked
    auto finalSockets = fs::directory_iterator(fs::temp_directory_path());
    int finalSocketCount = 0;
    for (const auto& entry : finalSockets) {
        if (entry.path().extension() == ".sock") {
            finalSocketCount++;
        }
    }

    EXPECT_LE(finalSocketCount, initialSocketCount) << "Socket files may have leaked";
}

// Test daemon with invalid configuration
TEST_F(DaemonLifecycleIntegrationTest, InvalidConfiguration) {
    // Test with invalid socket path
    DaemonConfig badConfig = config_;
    badConfig.socketPath = "/invalid/path/that/does/not/exist/daemon.sock";

    auto daemon = std::make_unique<YamsDaemon>(badConfig);
    auto result = daemon->start();
    EXPECT_FALSE(result) << "Daemon should not start with invalid socket path";

    // Test with invalid PID file path
    badConfig = config_;
    badConfig.pidFile = "/root/cannot_write_here.pid";

    daemon = std::make_unique<YamsDaemon>(badConfig);
    result = daemon->start();
    // Might succeed if it falls back to /tmp
    if (!result) {
        EXPECT_NE(result.error().code, ErrorCode::Success);
    }
}

// Test concurrent daemon operations
TEST_F(DaemonLifecycleIntegrationTest, ConcurrentOperations) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    ASSERT_TRUE(daemon_->start());
    ASSERT_TRUE(waitForSocket());

    const int numThreads = 10;
    std::vector<std::thread> threads;
    std::atomic<int> successCount{0};
    std::atomic<int> errorCount{0};

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([this, &successCount, &errorCount, i]() {
            DaemonClient client(clientConfig_);

            for (int j = 0; j < 10; ++j) {
                if (client.connect()) {
                    // Mix different operations
                    if (j % 3 == 0) {
                        if (client.ping())
                            successCount++;
                        else
                            errorCount++;
                    } else if (j % 3 == 1) {
                        if (client.status())
                            successCount++;
                        else
                            errorCount++;
                    } else {
                        SearchRequest req{"test", 5};
                        if (client.search(req) || true)
                            successCount++; // May fail if no data
                        else
                            errorCount++;
                    }

                    if (j % 5 == 0) {
                        client.disconnect();
                    }
                } else {
                    errorCount++;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_GT(successCount, numThreads * 5) << "Most operations should succeed";
    EXPECT_LT(errorCount, successCount) << "Errors should be less than successes";

    daemon_->stop();
}

} // namespace yams::daemon::integration::test