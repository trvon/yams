#include <gtest/gtest.h>
#include <yams/compression/compression_monitor.h>
#include <yams/compression/compressor_interface.h>
#include <thread>
#include <chrono>

using namespace yams::compression;
using namespace std::chrono_literals;

class CompressionMonitorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Reset global stats before each test
        monitor_ = std::make_unique<CompressionMonitor>();
    }
    
    void TearDown() override {
        if (monitor_ && monitor_->isRunning()) {
            monitor_->stop();
        }
    }
    
    std::unique_ptr<CompressionMonitor> monitor_;
};

TEST_F(CompressionMonitorTest, StartStop) {
    EXPECT_FALSE(monitor_->isRunning());
    
    auto result = monitor_->start();
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(monitor_->isRunning());
    
    monitor_->stop();
    EXPECT_FALSE(monitor_->isRunning());
}

TEST_F(CompressionMonitorTest, ConfigurationUpdate) {
    MonitorConfig config;
    config.metricsInterval = 5s;
    config.compressionRatioThreshold = 0.2;
    config.errorRateThreshold = 0.1;
    
    monitor_->updateConfig(config);
    
    auto retrieved = monitor_->getConfig();
    EXPECT_EQ(retrieved.metricsInterval, 5s);
    EXPECT_DOUBLE_EQ(retrieved.compressionRatioThreshold, 0.2);
    EXPECT_DOUBLE_EQ(retrieved.errorRateThreshold, 0.1);
}

TEST_F(CompressionMonitorTest, AlertCallback) {
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
        result.compressedSize = 900;  // 1.11 ratio (below 10.0)
        result.algorithm = CompressionAlgorithm::Zstandard;
        result.duration = 10ms;
        tracker.complete(result);
    }
    
    // Manually trigger metric check
    monitor_->checkMetrics();
    monitor_->checkAlgorithmHealth(CompressionAlgorithm::Zstandard);
    
    // Wait a bit for alert processing
    std::this_thread::sleep_for(100ms);
    
    EXPECT_GT(alertCount.load(), 0);
    EXPECT_EQ(lastAlert.type, AlertType::LowCompressionRatio);
}

TEST_F(CompressionMonitorTest, TrackingOperations) {
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
    EXPECT_EQ(stats2.totalCompressedFiles.load(), initialCompressed + 1);
    EXPECT_EQ(stats2.totalSpaceSaved.load(), 500u);
    
    // Track failed compression
    {
        CompressionTracker tracker(CompressionAlgorithm::LZMA, 2000);
        tracker.failed();
    }
    
    auto stats3 = monitor_->getCurrentStats();
    EXPECT_GT(stats3.algorithmStats[CompressionAlgorithm::LZMA].compressionErrors.load(), 0u);
}

TEST_F(CompressionMonitorTest, DecompressionTracking) {
    // Track decompression
    {
        DecompressionTracker tracker(CompressionAlgorithm::Zstandard, 500);
        tracker.complete(1000);
    }
    
    auto stats = monitor_->getCurrentStats();
    EXPECT_GT(stats.algorithmStats[CompressionAlgorithm::Zstandard].filesDecompressed.load(), 0u);
}

TEST_F(CompressionMonitorTest, ExportFormats) {
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
    
    // Test report export
    auto report = monitor_->exportReport();
    EXPECT_FALSE(report.empty());
    EXPECT_NE(report.find("Compression Statistics"), std::string::npos);
    
    // Test JSON export
    auto json = monitor_->exportJSON();
    EXPECT_FALSE(json.empty());
    EXPECT_NE(json.find("totalCompressedFiles"), std::string::npos);
    EXPECT_NE(json.find("Zstandard"), std::string::npos);
}

TEST_F(CompressionMonitorTest, MetricsHistory) {
    auto result = monitor_->start();
    ASSERT_TRUE(result.has_value());
    
    // Configure short interval for testing
    MonitorConfig config;
    config.metricsInterval = std::chrono::seconds(1);
    monitor_->updateConfig(config);
    
    // Wait for a few intervals
    std::this_thread::sleep_for(350ms);
    
    // Get history
    auto history = monitor_->getHistory(1s);
    EXPECT_GE(history.size(), 2u); // Should have at least 2 snapshots
}

TEST_F(CompressionMonitorTest, ConcurrentOperations) {
    const int numThreads = 4;
    const int operationsPerThread = 100;
    
    std::vector<std::thread> threads;
    
    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([t, operationsPerThread]() {
            for (int i = 0; i < operationsPerThread; ++i) {
                CompressionTracker tracker(
                    t % 2 == 0 ? CompressionAlgorithm::Zstandard : CompressionAlgorithm::LZMA,
                    static_cast<size_t>(1000 + i)
                );
                
                CompressionResult result;
                result.originalSize = static_cast<size_t>(1000 + i);
                result.compressedSize = static_cast<size_t>(500 + i/2);
                result.algorithm = t % 2 == 0 ? 
                    CompressionAlgorithm::Zstandard : CompressionAlgorithm::LZMA;
                result.duration = std::chrono::milliseconds(1);
                
                tracker.complete(result);
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    auto stats = monitor_->getCurrentStats();
    EXPECT_EQ(stats.totalCompressedFiles.load(), 
              static_cast<uint64_t>(numThreads * operationsPerThread));
}

TEST_F(CompressionMonitorTest, CustomMetricCollector) {
    std::atomic<int> collectorCalls{0};
    
    monitor_->setMetricCollector([&collectorCalls](MetricsSnapshot& snapshot) {
        collectorCalls++;
        snapshot.cpuUsage = 50.0;
        snapshot.memoryUsage = 1024 * 1024;
        snapshot.activeThreads = 4;
    });
    
    monitor_->checkMetrics();
    
    EXPECT_GT(collectorCalls.load(), 0);
}