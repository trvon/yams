// Benchmark/stress test for daemon socket server accept performance
// Tests: connection accept latency, concurrent connections, metrics polling overhead

#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include "../integration/daemon/test_async_helpers.h"
#include <boost/asio/awaitable.hpp>
#include <gtest/gtest.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

class SocketAcceptBench : public ::testing::Test {
protected:
    fs::path testRoot_;
    fs::path socketPath_;
    std::unique_ptr<yams::daemon::YamsDaemon> daemon_;

    void SetUp() override {
        auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        testRoot_ = fs::temp_directory_path() / ("yams_socket_bench_" + unique);
        fs::create_directories(testRoot_);

        // Short socket path for AF_UNIX
        socketPath_ = fs::path("/tmp") / ("yams-bench-" + unique + ".sock");

        yams::daemon::DaemonConfig cfg;
        cfg.dataDir = testRoot_ / "data";
        cfg.socketPath = socketPath_;
        cfg.pidFile = testRoot_ / "daemon.pid";
        cfg.logFile = testRoot_ / "daemon.log";
        cfg.useMockModelProvider = true;
        cfg.autoLoadPlugins = false;

        fs::create_directories(cfg.dataDir);

        daemon_ = std::make_unique<yams::daemon::YamsDaemon>(cfg);
        auto started = daemon_->start();
        ASSERT_TRUE(started) << started.error().message;

        // Wait for daemon ready
        std::this_thread::sleep_for(500ms);
    }

    void TearDown() override {
        if (daemon_) {
            daemon_->stop();
        }
        std::error_code ec;
        fs::remove_all(testRoot_, ec);
    }
};

// Test: Measure time from daemon start to first successful connection
TEST_F(SocketAcceptBench, FirstConnectionLatency) {
    auto startTime = std::chrono::steady_clock::now();

    yams::daemon::ClientConfig ccfg;
    ccfg.socketPath = socketPath_;
    ccfg.autoStart = false;
    ccfg.requestTimeout = 10s;
    yams::daemon::DaemonClient client(ccfg);

    // First status request - measures connection + request time
    auto st = yams::test_async::res(client.status(), 10s);
    auto elapsed = std::chrono::steady_clock::now() - startTime;
    auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

    ASSERT_TRUE(st) << st.error().message;
    std::cout << "First connection latency: " << elapsedMs << "ms" << std::endl;

    // Should be fast (< 1 second for local daemon)
    EXPECT_LT(elapsedMs, 1000) << "First connection took too long";
}

// Test: Rapid sequential connections to measure accept throughput
TEST_F(SocketAcceptBench, SequentialConnectionThroughput) {
    constexpr int NUM_REQUESTS = 100;
    std::vector<int64_t> latencies;
    latencies.reserve(NUM_REQUESTS);

    yams::daemon::ClientConfig ccfg;
    ccfg.socketPath = socketPath_;
    ccfg.autoStart = false;
    ccfg.requestTimeout = 5s;

    auto overallStart = std::chrono::steady_clock::now();

    for (int i = 0; i < NUM_REQUESTS; ++i) {
        // New client each iteration to test connection overhead
        yams::daemon::DaemonClient client(ccfg);

        auto start = std::chrono::steady_clock::now();
        auto st = yams::test_async::res(client.status(), 5s);
        auto elapsed = std::chrono::steady_clock::now() - start;
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

        ASSERT_TRUE(st) << "Request " << i << " failed: " << st.error().message;
        latencies.push_back(ms);
    }

    auto totalElapsed = std::chrono::steady_clock::now() - overallStart;
    auto totalMs = std::chrono::duration_cast<std::chrono::milliseconds>(totalElapsed).count();

    // Calculate statistics
    int64_t sum = 0, min = latencies[0], max = latencies[0];
    for (auto lat : latencies) {
        sum += lat;
        min = std::min(min, lat);
        max = std::max(max, lat);
    }
    double avg = static_cast<double>(sum) / NUM_REQUESTS;
    double throughput = (NUM_REQUESTS * 1000.0) / totalMs;

    std::cout << "Sequential throughput:" << std::endl;
    std::cout << "  Total: " << totalMs << "ms for " << NUM_REQUESTS << " requests" << std::endl;
    std::cout << "  Throughput: " << throughput << " req/s" << std::endl;
    std::cout << "  Latency - avg: " << avg << "ms, min: " << min << "ms, max: " << max << "ms"
              << std::endl;

    // Reasonable expectations for local daemon
    EXPECT_GT(throughput, 10.0) << "Throughput too low";
    EXPECT_LT(avg, 500.0) << "Average latency too high";
}

// Test: Concurrent connections to stress accept queue
TEST_F(SocketAcceptBench, ConcurrentConnectionStress) {
    constexpr int NUM_THREADS = 10;
    constexpr int REQUESTS_PER_THREAD = 10;

    // Wait for daemon to be fully ready before stress test (avoid startup race)
    {
        yams::daemon::ClientConfig ccfg;
        ccfg.socketPath = socketPath_;
        ccfg.autoStart = false;
        ccfg.requestTimeout = 10s;

        auto waitStart = std::chrono::steady_clock::now();
        bool ready = false;
        while (!ready && std::chrono::steady_clock::now() - waitStart < 15s) {
            try {
                yams::daemon::DaemonClient client(ccfg);
                auto st = yams::test_async::res(client.status(), 10s);
                if (st && st.value().ready) {
                    ready = true;
                    std::cout << "Daemon ready after "
                              << std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::steady_clock::now() - waitStart)
                                     .count()
                              << "ms" << std::endl;
                } else {
                    std::this_thread::sleep_for(100ms);
                }
            } catch (...) {
                std::this_thread::sleep_for(100ms);
            }
        }
        ASSERT_TRUE(ready) << "Daemon failed to become ready within 15 seconds";
    }

    std::atomic<int> successCount{0};
    std::atomic<int> failCount{0};
    std::vector<std::thread> threads;
    threads.reserve(NUM_THREADS);

    auto overallStart = std::chrono::steady_clock::now();

    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&]() {
            yams::daemon::ClientConfig ccfg;
            ccfg.socketPath = socketPath_;
            ccfg.autoStart = false;
            ccfg.requestTimeout = 10s;

            for (int i = 0; i < REQUESTS_PER_THREAD; ++i) {
                try {
                    yams::daemon::DaemonClient client(ccfg);
                    auto st = yams::test_async::res(client.status(), 10s);
                    if (st) {
                        successCount.fetch_add(1);
                    } else {
                        failCount.fetch_add(1);
                    }
                } catch (...) {
                    failCount.fetch_add(1);
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Wait for all async operations to complete before shutdown
    // Check daemon status to ensure all connections have cleaned up
    {
        yams::daemon::ClientConfig ccfg;
        ccfg.socketPath = socketPath_;
        ccfg.autoStart = false;
        ccfg.requestTimeout = 5s;

        auto waitStart = std::chrono::steady_clock::now();
        bool allComplete = false;
        while (!allComplete && std::chrono::steady_clock::now() - waitStart < 10s) {
            try {
                yams::daemon::DaemonClient client(ccfg);
                auto st = yams::test_async::res(client.status(), 5s);
                if (st && st.value().ready) {
                    // Check if active connections dropped to (near) zero
                    // Some overhead connection might exist, accept <= 2 as "done"
                    auto active = st.value().activeConnections;
                    if (active <= 2) {
                        allComplete = true;
                        std::cout << "All async operations complete after "
                                  << std::chrono::duration_cast<std::chrono::milliseconds>(
                                         std::chrono::steady_clock::now() - waitStart)
                                         .count()
                                  << "ms (active_conn=" << active << ")" << std::endl;
                    } else {
                        std::this_thread::sleep_for(100ms);
                    }
                }
            } catch (...) {
                std::this_thread::sleep_for(100ms);
            }
        }
        if (!allComplete) {
            std::cout << "Warning: Timed out waiting for async operations to complete" << std::endl;
        }
    }

    auto totalElapsed = std::chrono::steady_clock::now() - overallStart;
    auto totalMs = std::chrono::duration_cast<std::chrono::milliseconds>(totalElapsed).count();
    int totalRequests = NUM_THREADS * REQUESTS_PER_THREAD;
    double throughput = (totalRequests * 1000.0) / totalMs;

    std::cout << "Concurrent stress:" << std::endl;
    std::cout << "  Total: " << totalMs << "ms for " << totalRequests << " requests ("
              << NUM_THREADS << " threads)" << std::endl;
    std::cout << "  Success: " << successCount.load() << ", Failed: " << failCount.load()
              << std::endl;
    std::cout << "  Throughput: " << throughput << " req/s" << std::endl;

    EXPECT_EQ(successCount.load(), totalRequests) << "Some requests failed under load";
    EXPECT_GT(throughput, 5.0) << "Concurrent throughput too low";
}

// Test: Metrics polling doesn't block request handling
TEST_F(SocketAcceptBench, MetricsPollingNoBlockage) {
    constexpr int NUM_REQUESTS = 50;
    std::vector<int64_t> latencies;
    latencies.reserve(NUM_REQUESTS);

    yams::daemon::ClientConfig ccfg;
    ccfg.socketPath = socketPath_;
    ccfg.autoStart = false;
    ccfg.requestTimeout = 5s;
    yams::daemon::DaemonClient client(ccfg);

    // Let metrics polling run for a bit
    std::this_thread::sleep_for(1s);

    // Rapid-fire status requests while metrics polling is active
    for (int i = 0; i < NUM_REQUESTS; ++i) {
        auto start = std::chrono::steady_clock::now();
        auto st = yams::test_async::res(client.status(), 5s);
        auto elapsed = std::chrono::steady_clock::now() - start;
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

        ASSERT_TRUE(st) << "Request " << i << " failed: " << st.error().message;
        latencies.push_back(ms);
    }

    // Calculate p50, p95, p99
    std::sort(latencies.begin(), latencies.end());
    auto p50 = latencies[NUM_REQUESTS / 2];
    auto p95 = latencies[static_cast<size_t>(NUM_REQUESTS * 0.95)];
    auto p99 = latencies[static_cast<size_t>(NUM_REQUESTS * 0.99)];

    std::cout << "Metrics polling impact:" << std::endl;
    std::cout << "  p50: " << p50 << "ms, p95: " << p95 << "ms, p99: " << p99 << "ms" << std::endl;

    // With background polling, all requests should be fast (< 100ms)
    EXPECT_LT(p50, 100) << "p50 latency too high - metrics blocking?";
    EXPECT_LT(p95, 200) << "p95 latency too high - metrics blocking?";
}

// Test: Measure daemon readiness time
TEST_F(SocketAcceptBench, DaemonReadinessLatency) {
    // This test measures full startup time - already covered by SetUp()
    // But let's verify status reflects ready state
    yams::daemon::ClientConfig ccfg;
    ccfg.socketPath = socketPath_;
    ccfg.autoStart = false;
    ccfg.requestTimeout = 10s;
    yams::daemon::DaemonClient client(ccfg);

    // Poll until ready or timeout
    auto deadline = std::chrono::steady_clock::now() + 30s;
    bool becameReady = false;
    int64_t readyTimeMs = 0;
    auto startTime = std::chrono::steady_clock::now();

    while (std::chrono::steady_clock::now() < deadline) {
        auto st = yams::test_async::res(client.status(), 2s);
        if (st && st.value().ready) {
            becameReady = true;
            auto elapsed = std::chrono::steady_clock::now() - startTime;
            readyTimeMs = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
            break;
        }
        std::this_thread::sleep_for(100ms);
    }

    ASSERT_TRUE(becameReady) << "Daemon did not become ready within timeout";
    std::cout << "Daemon ready in: " << readyTimeMs << "ms" << std::endl;

    // Should be ready quickly (< 10 seconds for mock provider)
    EXPECT_LT(readyTimeMs, 10000) << "Daemon took too long to become ready";
}

// Main function for running benchmarks
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    std::cout << "\n";
    std::cout << "====================================\n";
    std::cout << "YAMS Daemon Socket Accept Benchmarks\n";
    std::cout << "====================================\n\n";

    int result = RUN_ALL_TESTS();

    std::cout << "\n====================================\n";
    std::cout << "Benchmarks complete.\n";

    return result;
}
