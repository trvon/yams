#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <thread>
#include <vector>
#include <yams/compression/compression_stats.h>
#include <yams/compression/compressor_interface.h>
#include <yams/core/types.h>

namespace yams::compression {

/**
 * @brief Monitoring configuration
 */
struct MonitorConfig {
    bool enableMonitoring = true;
    bool enableAlerts = true;
    std::chrono::seconds metricsInterval{60};    // How often to check metrics
    double compressionRatioThreshold = 0.1;      // Alert if ratio < 10%
    double errorRateThreshold = 0.05;            // Alert if error rate > 5%
    double performanceThreshold = 100.0;         // Alert if MB/s < 100
    size_t samplingRate = 100;                   // Sample every N operations
    std::chrono::seconds historyRetention{3600}; // Keep 1 hour of history
};

/**
 * @brief Alert types for monitoring
 */
enum class AlertType {
    LowCompressionRatio,
    HighErrorRate,
    SlowPerformance,
    UnusualDataPattern,
    ResourceExhaustion,
    CompressionFailure
};

/**
 * @brief Alert information
 */
struct Alert {
    AlertType type;
    CompressionAlgorithm algorithm;
    std::string message;
    double value;
    double threshold;
    std::chrono::system_clock::time_point timestamp;
};

/**
 * @brief Alert callback function type
 */
using AlertCallback = std::function<void(const Alert&)>;

/**
 * @brief Metrics snapshot for a specific time
 */
struct MetricsSnapshot {
    std::chrono::system_clock::time_point timestamp;
    CompressionStats stats;
    double cpuUsage;
    size_t memoryUsage;
    size_t activeThreads;
};

/**
 * @brief Compression monitoring service
 */
class CompressionMonitor {
public:
    /**
     * @brief Construct with configuration
     * @param config Monitor configuration
     */
    explicit CompressionMonitor(MonitorConfig config = {});
    ~CompressionMonitor();

    // Disable copy, enable move
    CompressionMonitor(const CompressionMonitor&) = delete;
    CompressionMonitor& operator=(const CompressionMonitor&) = delete;
    CompressionMonitor(CompressionMonitor&&) noexcept;
    CompressionMonitor& operator=(CompressionMonitor&&) noexcept;

    /**
     * @brief Start monitoring service
     * @return Success or error
     */
    [[nodiscard]] Result<void> start();

    /**
     * @brief Stop monitoring service
     */
    void stop();

    /**
     * @brief Check if monitoring is running
     * @return True if active
     */
    [[nodiscard]] bool isRunning() const noexcept;

    /**
     * @brief Register alert callback
     * @param callback Function to call on alerts
     */
    void registerAlertCallback(AlertCallback callback);

    /**
     * @brief Clear all alert callbacks
     */
    void clearAlertCallbacks();

    /**
     * @brief Manually check metrics and trigger alerts
     */
    void checkMetrics();

    /**
     * @brief Check health of specific algorithm
     * @param algo Algorithm to check
     */
    void checkAlgorithmHealth(CompressionAlgorithm algo);

    /**
     * @brief Update monitoring configuration
     * @param config New configuration
     */
    void updateConfig(MonitorConfig config);

    /**
     * @brief Get current configuration
     * @return Current config
     */
    [[nodiscard]] MonitorConfig getConfig() const;

    /**
     * @brief Get current statistics
     * @return Current compression stats
     */
    [[nodiscard]] CompressionStats getCurrentStats() const;

    /**
     * @brief Get metrics history
     * @param duration How far back to retrieve
     * @return Vector of snapshots
     */
    [[nodiscard]] std::vector<MetricsSnapshot> getHistory(std::chrono::seconds duration) const;

    /**
     * @brief Get active alerts
     * @return Vector of current alerts
     */
    [[nodiscard]] std::vector<Alert> getActiveAlerts() const;

    /**
     * @brief Clear alert history
     */
    void clearAlerts();

    /**
     * @brief Export monitoring report
     * @return Formatted report string
     */
    [[nodiscard]] std::string exportReport() const;

    /**
     * @brief Export metrics in JSON format
     * @return JSON string
     */
    [[nodiscard]] std::string exportJSON() const;

    /**
     * @brief Set custom metric collector
     * @param collector Function to collect custom metrics
     */
    void setMetricCollector(std::function<void(MetricsSnapshot&)> collector);

    /**
     * @brief Get global stats for tracking operations
     * @return Reference to global stats
     */
    static CompressionStats& getGlobalStats();

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;

    // Allow tracker classes to access global stats
    friend class CompressionTracker;
    friend class DecompressionTracker;
};

/**
 * @brief RAII helper to automatically track compression operations
 */
class CompressionTracker {
public:
    /**
     * @brief Start tracking compression operation
     * @param algo Algorithm being used
     * @param originalSize Size before compression
     */
    CompressionTracker(CompressionAlgorithm algo, size_t originalSize);

    /**
     * @brief Complete tracking with result
     * @param result Compression result
     */
    void complete(const CompressionResult& result);

    /**
     * @brief Mark operation as failed
     */
    void failed();

    ~CompressionTracker();

private:
    CompressionAlgorithm algorithm_;
    size_t originalSize_;
    std::chrono::steady_clock::time_point startTime_;
    bool completed_{false};
};

/**
 * @brief Decompression operation tracker
 */
class DecompressionTracker {
public:
    /**
     * @brief Start tracking decompression operation
     * @param algo Algorithm being used
     * @param compressedSize Size of compressed data
     */
    DecompressionTracker(CompressionAlgorithm algo, size_t compressedSize);

    /**
     * @brief Complete tracking with result
     * @param decompressedSize Size after decompression
     */
    void complete(size_t decompressedSize);

    /**
     * @brief Mark operation as failed
     */
    void failed();

    ~DecompressionTracker();

private:
    CompressionAlgorithm algorithm_;
    size_t compressedSize_;
    std::chrono::steady_clock::time_point startTime_;
    bool completed_{false};
};

} // namespace yams::compression