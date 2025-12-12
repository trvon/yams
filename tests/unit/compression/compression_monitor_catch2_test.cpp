// Catch2 tests for Compression Monitor
// Migrated from GTest: compression_monitor_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>
#include <yams/compression/compression_monitor.h>
#include <yams/compression/compressor_interface.h>

using namespace yams::compression;
using namespace std::chrono_literals;

namespace {
struct CompressionMonitorFixture {
    CompressionMonitorFixture() {
        // Reset global stats before each test to avoid cross-test interference
        MonitorConfig testConfig;
        testConfig.metricsInterval = std::chrono::seconds(1);
        testConfig.historyRetention = std::chrono::seconds(10);
        monitor_ = std::make_unique<CompressionMonitor>(testConfig);
        auto& stats = CompressionMonitor::getGlobalStats();
        auto& mutex = CompressionMonitor::getGlobalStatsMutex();
        std::lock_guard lock(mutex);
        stats = CompressionStats{};
    }

    ~CompressionMonitorFixture() {
        if (monitor_ && monitor_->isRunning()) {
            monitor_->stop();
        }
    }

    std::unique_ptr<CompressionMonitor> monitor_;
};
} // namespace

TEST_CASE_METHOD(CompressionMonitorFixture, "CompressionMonitor - StartStop",
                 "[compression][monitor][catch2]") {
    CHECK_FALSE(monitor_->isRunning());

    auto result = monitor_->start();
    REQUIRE(result.has_value());
    CHECK(monitor_->isRunning());

    monitor_->stop();
    CHECK_FALSE(monitor_->isRunning());
}

TEST_CASE_METHOD(CompressionMonitorFixture, "CompressionMonitor - ConfigurationUpdate",
                 "[compression][monitor][catch2]") {
    MonitorConfig config;
    config.metricsInterval = 5s;
    config.compressionRatioThreshold = 0.2;
    config.errorRateThreshold = 0.1;

    monitor_->updateConfig(config);

    auto retrieved = monitor_->getConfig();
    CHECK(retrieved.metricsInterval == 5s);
    CHECK(retrieved.compressionRatioThreshold == 0.2);
    CHECK(retrieved.errorRateThreshold == 0.1);
}

TEST_CASE_METHOD(CompressionMonitorFixture, "CompressionMonitor - AlertCallback",
                 "[compression][monitor][catch2]") {
    std::atomic<int> alertCount{0};
    Alert lastAlert;

    monitor_->registerAlertCallback([&alertCount, &lastAlert](const Alert& alert) {
        alertCount++;
        lastAlert = alert;
    });

    // Configure to trigger alerts
    MonitorConfig config;
    config.compressionRatioThreshold = 10.0; // Impossible ratio
    monitor_->updateConfig(config);

    // Simulate some compression operations
    {
        CompressionTracker tracker(CompressionAlgorithm::Zstandard, 1000);
        CompressionResult result;
        result.originalSize = 1000;
        result.compressedSize = 900; // 1.11 ratio (below 10.0)
        result.algorithm = CompressionAlgorithm::Zstandard;
        result.duration = 10ms;
        tracker.complete(result);
    }

    // Manually trigger metric check
    monitor_->checkMetrics();
    monitor_->checkAlgorithmHealth(CompressionAlgorithm::Zstandard);

    // Wait a bit for alert processing
    std::this_thread::sleep_for(100ms);

    CHECK(alertCount.load() > 0);
    // Allow either compression ratio or performance alert depending on platform timing
    CHECK((lastAlert.type == AlertType::LowCompressionRatio ||
           lastAlert.type == AlertType::SlowPerformance));
}

TEST_CASE_METHOD(CompressionMonitorFixture, "CompressionMonitor - TrackingOperations",
                 "[compression][monitor][catch2]") {
    auto stats1 = monitor_->getCurrentStats();
    auto initialCompressed = stats1.totalCompressedFiles.load();

    // Track successful compression
    {
        CompressionTracker tracker(CompressionAlgorithm::Zstandard, 1000);
        CompressionResult result;
        result.originalSize = 1000;
        result.compressedSize = 500;
        result.algorithm = CompressionAlgorithm::Zstandard;
        result.duration = 5ms;
        tracker.complete(result);
    }

    auto stats2 = monitor_->getCurrentStats();
    CHECK(stats2.totalCompressedFiles.load() == initialCompressed + 1);
    CHECK(stats2.totalSpaceSaved.load() == 500u);

    // Prime the LZMA entry
    {
        CompressionTracker tracker(CompressionAlgorithm::LZMA, 2000);
        CompressionResult result;
        result.originalSize = 2000;
        result.compressedSize = 1500;
        result.algorithm = CompressionAlgorithm::LZMA;
        result.duration = 5ms;
        tracker.complete(result);
    }

    // Track failed compression
    {
        CompressionTracker tracker(CompressionAlgorithm::LZMA, 2000);
        tracker.failed();
    }

    auto stats3 = monitor_->getCurrentStats();
    CHECK(stats3.algorithmStats[CompressionAlgorithm::LZMA].compressionErrors.load() > 0u);
}

TEST_CASE_METHOD(CompressionMonitorFixture, "CompressionMonitor - DecompressionTracking",
                 "[compression][monitor][catch2]") {
    // Track decompression
    {
        DecompressionTracker tracker(CompressionAlgorithm::Zstandard, 500);
        tracker.complete(1000);
    }

    auto stats = monitor_->getCurrentStats();
    CHECK(stats.algorithmStats[CompressionAlgorithm::Zstandard].filesDecompressed.load() > 0u);
}

TEST_CASE_METHOD(CompressionMonitorFixture, "CompressionMonitor - ExportFormats",
                 "[compression][monitor][catch2]") {
    // Add some data
    {
        CompressionTracker tracker(CompressionAlgorithm::Zstandard, 1000);
        CompressionResult result;
        result.originalSize = 1000;
        result.compressedSize = 400;
        result.algorithm = CompressionAlgorithm::Zstandard;
        result.duration = 5ms;
        tracker.complete(result);
    }

    SECTION("Report export") {
        auto report = monitor_->exportReport();
        CHECK_FALSE(report.empty());
        CHECK(report.find("Compression Statistics") != std::string::npos);
    }

    SECTION("JSON export") {
        auto json = monitor_->exportJSON();
        CHECK_FALSE(json.empty());
        CHECK(json.find("totalCompressedFiles") != std::string::npos);
        CHECK(json.find("Zstandard") != std::string::npos);
    }
}

TEST_CASE_METHOD(CompressionMonitorFixture, "CompressionMonitor - MetricsHistory",
                 "[compression][monitor][catch2]") {
    MonitorConfig config = monitor_->getConfig();
    config.metricsInterval = std::chrono::seconds(1);
    config.historyRetention = std::chrono::seconds(10);
    monitor_->updateConfig(config);

    auto result = monitor_->start();
    REQUIRE(result.has_value());

    // Manually record a few snapshots
    monitor_->checkMetrics();
    monitor_->checkMetrics();

    auto history = monitor_->getHistory(std::chrono::seconds(2));
    CHECK(history.size() >= 2u);
}

TEST_CASE_METHOD(CompressionMonitorFixture, "CompressionMonitor - ConcurrentOperations",
                 "[compression][monitor][concurrent][catch2][.]") {
    const int numThreads = 4;
    const int operationsPerThread = 100;

    std::vector<std::thread> threads;

    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([t, operationsPerThread]() {
            for (int i = 0; i < operationsPerThread; ++i) {
                CompressionTracker tracker(t % 2 == 0 ? CompressionAlgorithm::Zstandard
                                                      : CompressionAlgorithm::LZMA,
                                           static_cast<size_t>(1000 + i));

                CompressionResult result;
                result.originalSize = static_cast<size_t>(1000 + i);
                result.compressedSize = static_cast<size_t>(500 + i / 2);
                result.algorithm =
                    t % 2 == 0 ? CompressionAlgorithm::Zstandard : CompressionAlgorithm::LZMA;
                result.duration = std::chrono::milliseconds(1);

                tracker.complete(result);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto stats = monitor_->getCurrentStats();
    CHECK(stats.totalCompressedFiles.load() >=
          static_cast<uint64_t>(numThreads * operationsPerThread));
}

TEST_CASE_METHOD(CompressionMonitorFixture, "CompressionMonitor - CustomMetricCollector",
                 "[compression][monitor][catch2]") {
    std::atomic<int> collectorCalls{0};

    monitor_->setMetricCollector([&collectorCalls](MetricsSnapshot& snapshot) {
        collectorCalls++;
        snapshot.cpuUsage = 50.0;
        snapshot.memoryUsage = 1024 * 1024;
        snapshot.activeThreads = 4;
    });

    monitor_->checkMetrics();

    CHECK(collectorCalls.load() > 0);
}
