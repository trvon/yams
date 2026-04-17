// Benchmark/stress test for daemon socket server accept performance
// Tests: connection accept latency, concurrent connections, metrics polling overhead

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include "../integration/daemon/test_async_helpers.h"
#include <boost/asio/awaitable.hpp>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace {

struct SocketAcceptBenchFixture {
    fs::path testRoot_;
    fs::path socketPath_;
    std::unique_ptr<yams::daemon::YamsDaemon> daemon_;
    std::thread runLoopThread_;

    SocketAcceptBenchFixture() {
        auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        testRoot_ = fs::temp_directory_path() / ("yams_socket_bench_" + unique);
        fs::create_directories(testRoot_);

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
        REQUIRE(started);

        runLoopThread_ = std::thread([this]() { daemon_->runLoop(); });

        auto deadline = std::chrono::steady_clock::now() + 10s;
        while (std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(100ms);
            yams::daemon::ClientConfig probeCfg;
            probeCfg.socketPath = socketPath_;
            probeCfg.autoStart = false;
            probeCfg.requestTimeout = 1s;
            yams::daemon::DaemonClient probe(probeCfg);
            auto st = yams::test_async::res(probe.status(), 1s);
            if (st && st.value().ready) {
                break;
            }
        }
    }

    ~SocketAcceptBenchFixture() {
        if (daemon_) {
            daemon_->stop();
        }
        if (runLoopThread_.joinable()) {
            runLoopThread_.join();
        }
        std::error_code ec;
        fs::remove_all(testRoot_, ec);
    }

    SocketAcceptBenchFixture(const SocketAcceptBenchFixture&) = delete;
    SocketAcceptBenchFixture& operator=(const SocketAcceptBenchFixture&) = delete;
};

} // namespace

TEST_CASE_METHOD(SocketAcceptBenchFixture, "FirstConnectionLatency",
                 "[!benchmark][daemon][socket]") {
    auto startTime = std::chrono::steady_clock::now();

    yams::daemon::ClientConfig ccfg;
    ccfg.socketPath = socketPath_;
    ccfg.autoStart = false;
    ccfg.requestTimeout = 10s;
    yams::daemon::DaemonClient client(ccfg);

    auto st = yams::test_async::res(client.status(), 10s);
    auto elapsed = std::chrono::steady_clock::now() - startTime;
    auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

    REQUIRE(st);
    std::cout << "First connection latency: " << elapsedMs << "ms" << std::endl;

    CHECK(elapsedMs < 1000);
}

TEST_CASE_METHOD(SocketAcceptBenchFixture, "SequentialConnectionThroughput",
                 "[!benchmark][daemon][socket]") {
    constexpr int NUM_REQUESTS = 100;
    std::vector<int64_t> latencies;
    latencies.reserve(NUM_REQUESTS);

    yams::daemon::ClientConfig ccfg;
    ccfg.socketPath = socketPath_;
    ccfg.autoStart = false;
    ccfg.requestTimeout = 5s;

    auto overallStart = std::chrono::steady_clock::now();

    for (int i = 0; i < NUM_REQUESTS; ++i) {
        yams::daemon::DaemonClient client(ccfg);

        auto start = std::chrono::steady_clock::now();
        auto st = yams::test_async::res(client.status(), 5s);
        auto elapsed = std::chrono::steady_clock::now() - start;
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

        REQUIRE(st);
        latencies.push_back(ms);
    }

    auto totalElapsed = std::chrono::steady_clock::now() - overallStart;
    auto totalMs = std::chrono::duration_cast<std::chrono::milliseconds>(totalElapsed).count();

    int64_t sum = 0, min = latencies[0], max = latencies[0];
    for (auto lat : latencies) {
        sum += lat;
        min = std::min(min, lat);
        max = std::max(max, lat);
    }
    double avg = static_cast<double>(sum) / NUM_REQUESTS;
    double throughput = (NUM_REQUESTS * 1000.0) / static_cast<double>(totalMs);

    std::cout << "Sequential throughput:" << std::endl;
    std::cout << "  Total: " << totalMs << "ms for " << NUM_REQUESTS << " requests" << std::endl;
    std::cout << "  Throughput: " << throughput << " req/s" << std::endl;
    std::cout << "  Latency - avg: " << avg << "ms, min: " << min << "ms, max: " << max << "ms"
              << std::endl;

    CHECK(throughput > 10.0);
    CHECK(avg < 500.0);
}

TEST_CASE_METHOD(SocketAcceptBenchFixture, "ConcurrentConnectionStress",
                 "[!benchmark][daemon][socket]") {
    constexpr int NUM_THREADS = 10;
    constexpr int REQUESTS_PER_THREAD = 10;

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
        REQUIRE(ready);
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
    double throughput = (totalRequests * 1000.0) / static_cast<double>(totalMs);

    std::cout << "Concurrent stress:" << std::endl;
    std::cout << "  Total: " << totalMs << "ms for " << totalRequests << " requests ("
              << NUM_THREADS << " threads)" << std::endl;
    std::cout << "  Success: " << successCount.load() << ", Failed: " << failCount.load()
              << std::endl;
    std::cout << "  Throughput: " << throughput << " req/s" << std::endl;

    CHECK(successCount.load() == totalRequests);
    CHECK(throughput > 5.0);
}

TEST_CASE_METHOD(SocketAcceptBenchFixture, "MetricsPollingNoBlockage",
                 "[!benchmark][daemon][socket]") {
    constexpr int NUM_REQUESTS = 50;
    std::vector<int64_t> latencies;
    latencies.reserve(NUM_REQUESTS);

    yams::daemon::ClientConfig ccfg;
    ccfg.socketPath = socketPath_;
    ccfg.autoStart = false;
    ccfg.requestTimeout = 5s;
    yams::daemon::DaemonClient client(ccfg);

    std::this_thread::sleep_for(1s);

    for (int i = 0; i < NUM_REQUESTS; ++i) {
        auto start = std::chrono::steady_clock::now();
        auto st = yams::test_async::res(client.status(), 5s);
        auto elapsed = std::chrono::steady_clock::now() - start;
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

        REQUIRE(st);
        latencies.push_back(ms);
    }

    std::sort(latencies.begin(), latencies.end());
    auto p50 = latencies[NUM_REQUESTS / 2];
    auto p95 = latencies[static_cast<size_t>(NUM_REQUESTS * 0.95)];
    auto p99 = latencies[static_cast<size_t>(NUM_REQUESTS * 0.99)];

    std::cout << "Metrics polling impact:" << std::endl;
    std::cout << "  p50: " << p50 << "ms, p95: " << p95 << "ms, p99: " << p99 << "ms" << std::endl;

    CHECK(p50 < 100);
    CHECK(p95 < 200);
}

TEST_CASE_METHOD(SocketAcceptBenchFixture, "DaemonReadinessLatency",
                 "[!benchmark][daemon][socket]") {
    yams::daemon::ClientConfig ccfg;
    ccfg.socketPath = socketPath_;
    ccfg.autoStart = false;
    ccfg.requestTimeout = 10s;
    yams::daemon::DaemonClient client(ccfg);

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

    REQUIRE(becameReady);
    std::cout << "Daemon ready in: " << readyTimeMs << "ms" << std::endl;

    CHECK(readyTimeMs < 10000);
}
