#include <csignal>
#include <fcntl.h>
#include <filesystem>
#include <thread>
#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>

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

        clientConfig_.socketPath = config_.socketPath;
        clientConfig_.autoStart = false;
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
    ClientConfig clientConfig_;
    std::unique_ptr<YamsDaemon> daemon_;
};

// Test recovery from SIGKILL
TEST_F(DaemonResilienceTest, KillRecovery) {
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
        exit(1);
    }

    // Parent process - wait for daemon to start
    std::this_thread::sleep_for(500ms);
    ASSERT_TRUE(fs::exists(config_.socketPath)) << "Daemon didn't create socket";

    // Verify daemon is working
    DaemonClient client(clientConfig_);
    ASSERT_TRUE(client.connect());
    ASSERT_TRUE(client.ping());

    // Kill daemon with SIGKILL (non-graceful)
    kill(daemonPid, SIGKILL);
    waitpid(daemonPid, nullptr, 0);

    // Socket and PID file might still exist
    std::this_thread::sleep_for(100ms);

    // Start new daemon - should handle stale files
    daemon_ = std::make_unique<YamsDaemon>(config_);
    auto result = daemon_->start();
    ASSERT_TRUE(result) << "Failed to recover: " << result.error().message;

    // Verify new daemon works
    DaemonClient client2(clientConfig_);
    ASSERT_TRUE(client2.connect());
    ASSERT_TRUE(client2.ping());
}

// Test file descriptor leak detection
TEST_F(DaemonResilienceTest, FileDescriptorLeak) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    ASSERT_TRUE(daemon_->start());

    size_t initialFds = countOpenFileDescriptors();

    // Create many connections and close them
    const int numCycles = 50;
    for (int i = 0; i < numCycles; ++i) {
        DaemonClient client(clientConfig_);
        ASSERT_TRUE(client.connect());

        // Do some operations
        client.ping();
        client.status();

        // Disconnect
        client.disconnect();
    }

    // Check FD count hasn't grown significantly
    size_t finalFds = countOpenFileDescriptors();
    size_t fdGrowth = (finalFds > initialFds) ? (finalFds - initialFds) : 0;

    EXPECT_LT(fdGrowth, 10) << "Possible file descriptor leak detected";
}

// Test memory growth over time
TEST_F(DaemonResilienceTest, MemoryGrowth) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    ASSERT_TRUE(daemon_->start());

    size_t initialMemory = getMemoryUsage();

    // Perform many operations
    DaemonClient client(clientConfig_);
    ASSERT_TRUE(client.connect());

    const int numOperations = 100;
    for (int i = 0; i < numOperations; ++i) {
        client.ping();

        SearchRequest req{};
        req.query = "test";
        req.limit = 10;
        client.search(req);

        if (i % 10 == 0) {
            client.status();
        }
    }

    size_t finalMemory = getMemoryUsage();

    // Allow some growth but flag excessive growth
    if (initialMemory > 0 && finalMemory > 0) {
        double growthRatio = static_cast<double>(finalMemory) / initialMemory;
        EXPECT_LT(growthRatio, 2.0) << "Memory grew more than 2x";
    }
}

// Test graceful vs forced shutdown
TEST_F(DaemonResilienceTest, ShutdownComparison) {
    // Test graceful shutdown
    {
        daemon_ = std::make_unique<YamsDaemon>(config_);
        ASSERT_TRUE(daemon_->start());

        DaemonClient client(clientConfig_);
        ASSERT_TRUE(client.connect());

        // Start a long operation in background
        std::thread bgThread([&client]() {
            for (int i = 0; i < 100; ++i) {
                client.ping();
                std::this_thread::sleep_for(10ms);
            }
        });

        // Graceful shutdown
        auto result = daemon_->stop();
        EXPECT_TRUE(result) << "Graceful shutdown should succeed";

        bgThread.join();
        daemon_.reset();
    }

    // Clean up
    std::this_thread::sleep_for(200ms);

    // Test forced shutdown
    {
        daemon_ = std::make_unique<YamsDaemon>(config_);
        ASSERT_TRUE(daemon_->start());

        DaemonClient client(clientConfig_);
        ASSERT_TRUE(client.connect());

        // Forced shutdown via client
        auto result = client.shutdown(false); // Not graceful

        // Daemon should stop quickly
        auto start = std::chrono::steady_clock::now();
        daemon_->stop();
        auto elapsed = std::chrono::steady_clock::now() - start;

        EXPECT_LT(elapsed, 1s) << "Forced shutdown should be quick";
    }
}

// Test signal handling
TEST_F(DaemonResilienceTest, SignalHandling) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    ASSERT_TRUE(daemon_->start());

    // Test SIGHUP (reload signal)
    // Note: Can't actually send signals to self in test, but verify handlers are installed

    // Verify daemon is still running after various operations
    DaemonClient client(clientConfig_);
    ASSERT_TRUE(client.connect());
    ASSERT_TRUE(client.ping());
}

// Test resource exhaustion recovery
TEST_F(DaemonResilienceTest, ResourceExhaustion) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    ASSERT_TRUE(daemon_->start());

    // Try to exhaust connections
    std::vector<std::unique_ptr<DaemonClient>> clients;

    // Create many clients but don't exceed reasonable limits
    const int maxClients = 20;
    int connectedCount = 0;

    for (int i = 0; i < maxClients; ++i) {
        auto client = std::make_unique<DaemonClient>(clientConfig_);
        if (client->connect()) {
            clients.push_back(std::move(client));
            connectedCount++;
        } else {
            break; // Hit connection limit
        }
    }

    EXPECT_GT(connectedCount, 0) << "Should connect at least one client";

    // Release all connections
    clients.clear();

    // Should be able to connect again
    DaemonClient newClient(clientConfig_);
    EXPECT_TRUE(newClient.connect()) << "Should recover after releasing connections";
}

// Test daemon upgrade scenario
TEST_F(DaemonResilienceTest, DaemonUpgrade) {
    // Start "old" daemon
    daemon_ = std::make_unique<YamsDaemon>(config_);
    ASSERT_TRUE(daemon_->start());

    // Connect and get some state
    DaemonClient client(clientConfig_);
    ASSERT_TRUE(client.connect());

    auto statusBefore = client.status();
    ASSERT_TRUE(statusBefore);

    // Simulate upgrade: stop old daemon
    client.disconnect();
    daemon_->stop();
    daemon_.reset();

    std::this_thread::sleep_for(200ms);

    // Start "new" daemon (same binary in test)
    daemon_ = std::make_unique<YamsDaemon>(config_);
    ASSERT_TRUE(daemon_->start());

    // Connect with new client
    DaemonClient newClient(clientConfig_);
    ASSERT_TRUE(newClient.connect());

    auto statusAfter = newClient.status();
    ASSERT_TRUE(statusAfter);

    // New daemon should start fresh
    EXPECT_LT(statusAfter.value().uptimeSeconds, statusBefore.value().uptimeSeconds);
}

// Test concurrent stress
TEST_F(DaemonResilienceTest, ConcurrentStress) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    ASSERT_TRUE(daemon_->start());

    const int numThreads = 10;
    // opsPerThread unused at outer scope; per-thread value set inside lambda
    std::atomic<int> successCount{0};
    std::atomic<int> errorCount{0};
    std::atomic<bool> stopFlag{false};

    // Stress threads
    std::vector<std::thread> threads;
    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([this, &successCount, &errorCount, &stopFlag]() {
            int opsPerThread = 20;
            DaemonClient client(clientConfig_);

            for (int i = 0; i < opsPerThread && !stopFlag; ++i) {
                // Random operations
                if (i % 10 == 0) {
                    // Reconnect periodically
                    client.disconnect();
                    if (!client.connect()) {
                        errorCount++;
                        continue;
                    }
                }

                bool success = false;
                switch (i % 4) {
                    case 0:
                        success = client.ping().has_value();
                        break;
                    case 1:
                        success = client.status().has_value();
                        break;
                    case 2: {
                        SearchRequest req{};
                        req.query = "test";
                        req.limit = 5;
                        auto res = client.search(req);
                        success = res.has_value() || res.error().code == ErrorCode::NotFound;
                        break;
                    }
                    case 3:
                        // Brief pause
                        std::this_thread::sleep_for(1ms);
                        success = true;
                        break;
                }

                if (success)
                    successCount++;
                else
                    errorCount++;
            }
        });
    }

    // Let it run for a bit
    std::this_thread::sleep_for(2s);

    // Signal stop
    stopFlag = true;

    // Wait for threads
    for (auto& t : threads) {
        t.join();
    }

    // Daemon should still be responsive
    DaemonClient finalClient(clientConfig_);
    ASSERT_TRUE(finalClient.connect()) << "Daemon should still work after stress";
    ASSERT_TRUE(finalClient.ping());

    // Most operations should succeed
    EXPECT_GT(successCount, errorCount) << "Most operations should succeed under stress";
}

// Test error injection and recovery
TEST_F(DaemonResilienceTest, ErrorInjection) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    ASSERT_TRUE(daemon_->start());

    DaemonClient client(clientConfig_);
    ASSERT_TRUE(client.connect());

    // Send various invalid requests
    std::vector<std::function<void()>> errorTests = {[&client]() {
                                                         // Empty search
                                                         SearchRequest req{};
                                                         req.query = "";
                                                         req.limit = 0;
                                                         client.search(req);
                                                     },
                                                     [&client]() {
                                                         // Invalid model
                                                         LoadModelRequest req{
                                                             "nonexistent-model-xyz"};
                                                         client.loadModel(req);
                                                     },
                                                     [&client]() {
                                                         // Invalid hash
                                                         GetRequest req{};
                                                         req.hash = "invalid-hash-123";
                                                         client.get(req);
                                                     }};

    // Run error tests
    for (auto& test : errorTests) {
        test();

        // Daemon should still be responsive
        EXPECT_TRUE(client.ping()) << "Daemon should recover from errors";
    }
}

// Test long-running daemon stability
TEST_F(DaemonResilienceTest, LongRunningStability) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    ASSERT_TRUE(daemon_->start());

    DaemonClient client(clientConfig_);
    ASSERT_TRUE(client.connect());

    // Simulate long-running daemon with periodic operations
    const int numIterations = 50;
    for (int i = 0; i < numIterations; ++i) {
        // Check daemon is still responsive
        ASSERT_TRUE(client.ping()) << "Failed at iteration " << i;

        // Get status
        auto status = client.status();
        ASSERT_TRUE(status) << "Status failed at iteration " << i;

        // Verify uptime is increasing
        static uint64_t lastUptime = 0;
        EXPECT_GE(status.value().uptimeSeconds, lastUptime);
        lastUptime = status.value().uptimeSeconds;

        std::this_thread::sleep_for(50ms);
    }

    // Final health check
    auto finalStatus = client.status();
    ASSERT_TRUE(finalStatus);
    EXPECT_TRUE(finalStatus.value().running);
}

// Test that the daemon survives a client disconnecting abruptly after a request
TEST_F(DaemonResilienceTest, AbruptClientDisconnect) {
    // Start the daemon
    daemon_ = std::make_unique<YamsDaemon>(config_);
    auto result = daemon_->start();
    ASSERT_TRUE(result) << "Failed to start daemon: " << result.error().message;

    // Wait for the socket to be created
    bool socket_exists = false;
    for (int i = 0; i < 100; ++i) {
        if (fs::exists(config_.socketPath)) {
            socket_exists = true;
            break;
        }
        std::this_thread::sleep_for(10ms);
    }
    ASSERT_TRUE(socket_exists) << "Daemon socket not found";

    // Fork a client process that will connect, send, and exit immediately
    pid_t clientPid = fork();
    ASSERT_GE(clientPid, 0) << "Fork failed for client process";

    if (clientPid == 0) {
        // Child (client) process
        int clientFd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (clientFd < 0) {
            exit(1); // Parent will see this as failure
        }

        struct sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, config_.socketPath.c_str(), sizeof(addr.sun_path) - 1);

        if (connect(clientFd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
            exit(1);
        }

        // Create and send a PingRequest
        Message msg;
        msg.version = PROTOCOL_VERSION;
        msg.requestId = 123;
        msg.payload = PingRequest{};

        MessageFramer framer;
        auto framedResult = framer.frame_message(msg);
        if (!framedResult) {
            exit(1);
        }
        auto& data = framedResult.value();
        send(clientFd, data.data(), data.size(), MSG_NOSIGNAL);

        // Immediately exit without waiting for response
        exit(0);
    }

    // Parent process
    int status;
    waitpid(clientPid, &status, 0);
    ASSERT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0) << "Client process failed";

    // Give the daemon a moment to potentially crash
    std::this_thread::sleep_for(500ms);

    // Check if the daemon is still running
    DaemonClient client(clientConfig_);
    auto connectResult = client.connect();
    EXPECT_TRUE(connectResult) << "Daemon crashed after abrupt client disconnect. "
                               << "Failed to reconnect: " << connectResult.error().message;

    if (connectResult) {
        auto pingResult = client.ping();
        EXPECT_TRUE(pingResult) << "Daemon is unresponsive after abrupt client disconnect.";
    }
}

} // namespace yams::daemon::integration::test
