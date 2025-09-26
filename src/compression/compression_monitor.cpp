#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <deque>
#include <numeric>
#include <yams/compression/compression_monitor.h>

namespace yams::compression {

//-----------------------------------------------------------------------------
// CompressionMonitor::Impl
//-----------------------------------------------------------------------------

class CompressionMonitor::Impl {
public:
    explicit Impl(MonitorConfig cfg) : config_(std::move(cfg)) {}

    ~Impl() { stop(); }

    Result<void> start() {
        if (running_.load()) {
            return Error{ErrorCode::InvalidState, "Monitor already running"};
        }

        running_.store(true);
        monitorThread_ = std::thread([this] { monitorLoop(); });

        spdlog::info("Compression monitor started with {} second interval",
                     config_.metricsInterval.count());
        return {};
    }

    void stop() {
        if (running_.exchange(false)) {
            cv_.notify_all();
            if (monitorThread_.joinable()) {
                monitorThread_.join();
            }
            spdlog::info("Compression monitor stopped");
        }
    }

    bool isRunning() const noexcept { return running_.load(); }

    void registerAlertCallback(AlertCallback callback) {
        std::lock_guard lock(callbacksMutex_);
        alertCallbacks_.push_back(std::move(callback));
    }

    void clearAlertCallbacks() {
        std::lock_guard lock(callbacksMutex_);
        alertCallbacks_.clear();
    }

    void checkMetrics() {
        if (!config_.enableMonitoring) {
            return;
        }

        // Take snapshot
        MetricsSnapshot snapshot;
        snapshot.timestamp = std::chrono::system_clock::now();
        snapshot.stats = getCurrentStatsInternal();
        snapshot.cpuUsage = 0.0;
        snapshot.memoryUsage = 0;
        snapshot.activeThreads = 0;

        // Collect custom metrics if available
        if (metricCollector_) {
            metricCollector_(snapshot);
        }

        // Store in history
        {
            std::lock_guard lock(historyMutex_);
            history_.push_back(snapshot);

            // Prune old history
            auto cutoff = snapshot.timestamp - config_.historyRetention;
            history_.erase(std::remove_if(history_.begin(), history_.end(),
                                          [cutoff](const auto& s) { return s.timestamp < cutoff; }),
                           history_.end());
        }

        // Check for alerts
        if (config_.enableAlerts) {
            checkAlerts(snapshot.stats);
        }
    }

    void checkAlgorithmHealth(CompressionAlgorithm algo) {
        auto stats = getCurrentStatsInternal();

        auto it = stats.algorithmStats.find(algo);
        if (it == stats.algorithmStats.end()) {
            return; // No data for this algorithm
        }

        const auto& algoStats = it->second;

        // Check compression ratio
        double ratio = algoStats.averageRatio();
        if (ratio > 0 && ratio < config_.compressionRatioThreshold) {
            triggerAlert(
                Alert{.type = AlertType::LowCompressionRatio,
                      .algorithm = algo,
                      .message = fmt::format("Low compression ratio for {}", algorithmName(algo)),
                      .value = ratio,
                      .threshold = config_.compressionRatioThreshold,
                      .timestamp = std::chrono::system_clock::now()});
        }

        // Check error rate
        uint64_t totalOps = algoStats.filesCompressed.load() + algoStats.filesDecompressed.load();
        uint64_t totalErrors =
            algoStats.compressionErrors.load() + algoStats.decompressionErrors.load();

        if (totalOps > 0) {
            double errorRate = static_cast<double>(totalErrors) / static_cast<double>(totalOps);
            if (errorRate > config_.errorRateThreshold) {
                triggerAlert(
                    Alert{.type = AlertType::HighErrorRate,
                          .algorithm = algo,
                          .message = fmt::format("High error rate for {}", algorithmName(algo)),
                          .value = errorRate,
                          .threshold = config_.errorRateThreshold,
                          .timestamp = std::chrono::system_clock::now()});
            }
        }

        // Check performance
        double throughput = algoStats.compressionThroughputMBps();
        if (throughput > 0 && throughput < config_.performanceThreshold) {
            triggerAlert(Alert{
                .type = AlertType::SlowPerformance,
                .algorithm = algo,
                .message = fmt::format("Slow compression performance for {}", algorithmName(algo)),
                .value = throughput,
                .threshold = config_.performanceThreshold,
                .timestamp = std::chrono::system_clock::now()});
        }
    }

    void updateConfig(MonitorConfig cfg) {
        std::lock_guard lock(configMutex_);
        config_ = std::move(cfg);
        cv_.notify_all(); // Wake monitor thread to pick up changes
    }

    MonitorConfig getConfig() const {
        std::lock_guard lock(configMutex_);
        return config_;
    }

    CompressionStats getCurrentStats() const { return getCurrentStatsInternal(); }

    std::vector<MetricsSnapshot> getHistory(std::chrono::seconds duration) const {
        std::lock_guard lock(historyMutex_);

        auto cutoff = std::chrono::system_clock::now() - duration;
        std::vector<MetricsSnapshot> result;

        for (const auto& snapshot : history_) {
            if (snapshot.timestamp >= cutoff) {
                result.push_back(snapshot);
            }
        }

        return result;
    }

    std::vector<Alert> getActiveAlerts() const {
        std::lock_guard lock(alertsMutex_);
        return activeAlerts_;
    }

    void clearAlerts() {
        std::lock_guard lock(alertsMutex_);
        activeAlerts_.clear();
    }

    std::string exportReport() const {
        std::ostringstream oss;

        auto stats = getCurrentStatsInternal();
        oss << stats.formatReport() << "\n";

        // Add monitoring information
        oss << "=== Monitoring Status ===\n";
        oss << "Status: " << (running_.load() ? "Active" : "Stopped") << "\n";
        oss << "Interval: " << config_.metricsInterval.count() << " seconds\n";
        oss << "History entries: " << history_.size() << "\n";
        oss << "Active alerts: " << activeAlerts_.size() << "\n\n";

        // Add alerts
        if (!activeAlerts_.empty()) {
            oss << "=== Active Alerts ===\n";
            for (const auto& alert : activeAlerts_) {
                oss << formatAlert(alert) << "\n";
            }
        }

        return oss.str();
    }

    std::string exportJSON() const {
        // Simple JSON export (could use a JSON library for production)
        std::ostringstream oss;
        oss << "{\n";

        auto stats = getCurrentStatsInternal();
        oss << "  \"totalCompressedFiles\": " << stats.totalCompressedFiles.load() << ",\n";
        oss << "  \"totalSpaceSaved\": " << stats.totalSpaceSaved.load() << ",\n";
        oss << "  \"compressionRatio\": " << stats.overallCompressionRatio() << ",\n";
        oss << "  \"cacheHitRate\": " << stats.cacheHitRate() << ",\n";

        oss << "  \"algorithms\": {\n";
        bool first = true;
        for (const auto& [algo, algoStats] : stats.algorithmStats) {
            if (!first)
                oss << ",\n";
            first = false;

            oss << "    \"" << algorithmName(algo) << "\": {\n";
            oss << "      \"filesCompressed\": " << algoStats.filesCompressed.load() << ",\n";
            oss << "      \"averageRatio\": " << algoStats.averageRatio() << ",\n";
            oss << "      \"compressionSpeed\": " << algoStats.compressionThroughputMBps() << ",\n";
            oss << "      \"decompressionSpeed\": " << algoStats.decompressionThroughputMBps()
                << "\n";
            oss << "    }";
        }
        oss << "\n  },\n";

        oss << "  \"monitoring\": {\n";
        oss << "    \"running\": " << (running_.load() ? "true" : "false") << ",\n";
        oss << "    \"alertCount\": " << activeAlerts_.size() << "\n";
        oss << "  }\n";

        oss << "}\n";
        return oss.str();
    }

    void setMetricCollector(std::function<void(MetricsSnapshot&)> collector) {
        std::lock_guard lock(configMutex_);
        metricCollector_ = std::move(collector);
    }

    static CompressionStats& getGlobalStats() {
        static CompressionStats stats;
        return stats;
    }

    static std::mutex& getGlobalStatsMutex() {
        static std::mutex mutex;
        return mutex;
    }

private:
    // Configuration
    mutable std::mutex configMutex_;
    MonitorConfig config_;

    // Running state
    std::atomic<bool> running_{false};
    std::thread monitorThread_;
    std::condition_variable cv_;
    std::mutex cvMutex_;

    // Alert management
    mutable std::mutex callbacksMutex_;
    std::vector<AlertCallback> alertCallbacks_;
    mutable std::mutex alertsMutex_;
    std::vector<Alert> activeAlerts_;

    // History
    mutable std::mutex historyMutex_;
    std::deque<MetricsSnapshot> history_;

    // Custom metrics
    std::function<void(MetricsSnapshot&)> metricCollector_;

    // Reference to global stats (would be injected in production)
    CompressionStats& globalStats_{getGlobalStats()};

    void monitorLoop() {
        while (running_.load()) {
            checkMetrics();

            // Wait for interval or shutdown
            std::unique_lock lock(cvMutex_);
            cv_.wait_for(lock, config_.metricsInterval, [this] { return !running_.load(); });
        }
    }

    void checkAlerts(const CompressionStats& stats) {
        // Check overall compression ratio
        double ratio = stats.overallCompressionRatio();
        if (ratio > 0 && ratio < config_.compressionRatioThreshold) {
            triggerAlert(Alert{.type = AlertType::LowCompressionRatio,
                               .algorithm = CompressionAlgorithm::None,
                               .message = "Overall compression ratio below threshold",
                               .value = ratio,
                               .threshold = config_.compressionRatioThreshold,
                               .timestamp = std::chrono::system_clock::now()});
        }

        // Check each algorithm
        for (const auto& [algo, _] : stats.algorithmStats) {
            checkAlgorithmHealth(algo);
        }

        // Check resource usage
        if (stats.peakMemoryUsageBytes.load() > 1024ULL * 1024ULL * 1024ULL) { // 1GB
            triggerAlert(Alert{.type = AlertType::ResourceExhaustion,
                               .algorithm = CompressionAlgorithm::None,
                               .message = "High memory usage detected",
                               .value = static_cast<double>(stats.peakMemoryUsageBytes.load()),
                               .threshold = 1024.0 * 1024.0 * 1024.0,
                               .timestamp = std::chrono::system_clock::now()});
        }
    }

    void triggerAlert(Alert alert) {
        // Check if this alert is already active
        {
            std::lock_guard lock(alertsMutex_);
            auto it =
                std::find_if(activeAlerts_.begin(), activeAlerts_.end(), [&alert](const Alert& a) {
                    return a.type == alert.type && a.algorithm == alert.algorithm;
                });

            if (it != activeAlerts_.end()) {
                // Update existing alert
                *it = alert;
            } else {
                // Add new alert
                activeAlerts_.push_back(alert);
            }
        }

        // Notify callbacks
        std::lock_guard lock(callbacksMutex_);
        for (const auto& callback : alertCallbacks_) {
            try {
                callback(alert);
            } catch (const std::exception& e) {
                spdlog::error("Alert callback error: {}", e.what());
            }
        }

        // Log alert
        spdlog::warn("Compression alert: {}", formatAlert(alert));
    }

    static std::string formatAlert(const Alert& alert) {
        return fmt::format("[{}] {} - Value: {:.2f}, Threshold: {:.2f}", alertTypeName(alert.type),
                           alert.message, alert.value, alert.threshold);
    }

    static std::string alertTypeName(AlertType type) {
        switch (type) {
            case AlertType::LowCompressionRatio:
                return "LOW_RATIO";
            case AlertType::HighErrorRate:
                return "HIGH_ERROR";
            case AlertType::SlowPerformance:
                return "SLOW_PERF";
            case AlertType::UnusualDataPattern:
                return "UNUSUAL_DATA";
            case AlertType::ResourceExhaustion:
                return "RESOURCE_LIMIT";
            case AlertType::CompressionFailure:
                return "FAILURE";
            default:
                return "UNKNOWN";
        }
    }

    static std::string algorithmName(CompressionAlgorithm algo) {
        switch (algo) {
            case CompressionAlgorithm::None:
                return "None";
            case CompressionAlgorithm::Zstandard:
                return "Zstandard";
            case CompressionAlgorithm::LZMA:
                return "LZMA";
            default:
                return "Unknown";
        }
    }

    CompressionStats getCurrentStatsInternal() const {
        // In production, this would get stats from the actual system
        // For now, return a copy of global stats
        std::lock_guard lock(getGlobalStatsMutex());
        return globalStats_;
    }
};

//-----------------------------------------------------------------------------
// CompressionMonitor
//-----------------------------------------------------------------------------

CompressionMonitor::CompressionMonitor(MonitorConfig config)
    : pImpl(std::make_unique<Impl>(std::move(config))) {}

CompressionMonitor::~CompressionMonitor() = default;

CompressionMonitor::CompressionMonitor(CompressionMonitor&&) noexcept = default;
CompressionMonitor& CompressionMonitor::operator=(CompressionMonitor&&) noexcept = default;

CompressionStats& CompressionMonitor::getGlobalStats() {
    return Impl::getGlobalStats();
}

std::mutex& CompressionMonitor::getGlobalStatsMutex() {
    return Impl::getGlobalStatsMutex();
}

Result<void> CompressionMonitor::start() {
    return pImpl->start();
}

void CompressionMonitor::stop() {
    pImpl->stop();
}

bool CompressionMonitor::isRunning() const noexcept {
    return pImpl->isRunning();
}

void CompressionMonitor::registerAlertCallback(AlertCallback callback) {
    pImpl->registerAlertCallback(std::move(callback));
}

void CompressionMonitor::clearAlertCallbacks() {
    pImpl->clearAlertCallbacks();
}

void CompressionMonitor::checkMetrics() {
    pImpl->checkMetrics();
}

void CompressionMonitor::checkAlgorithmHealth(CompressionAlgorithm algo) {
    pImpl->checkAlgorithmHealth(algo);
}

void CompressionMonitor::updateConfig(MonitorConfig config) {
    pImpl->updateConfig(std::move(config));
}

MonitorConfig CompressionMonitor::getConfig() const {
    return pImpl->getConfig();
}

CompressionStats CompressionMonitor::getCurrentStats() const {
    return pImpl->getCurrentStats();
}

std::vector<MetricsSnapshot> CompressionMonitor::getHistory(std::chrono::seconds duration) const {
    return pImpl->getHistory(duration);
}

std::vector<Alert> CompressionMonitor::getActiveAlerts() const {
    return pImpl->getActiveAlerts();
}

void CompressionMonitor::clearAlerts() {
    pImpl->clearAlerts();
}

std::string CompressionMonitor::exportReport() const {
    return pImpl->exportReport();
}

std::string CompressionMonitor::exportJSON() const {
    return pImpl->exportJSON();
}

void CompressionMonitor::setMetricCollector(std::function<void(MetricsSnapshot&)> collector) {
    pImpl->setMetricCollector(std::move(collector));
}

//-----------------------------------------------------------------------------
// CompressionTracker
//-----------------------------------------------------------------------------

CompressionTracker::CompressionTracker(CompressionAlgorithm algo, size_t originalSize)
    : algorithm_(algo), originalSize_(originalSize), startTime_(std::chrono::steady_clock::now()) {}

void CompressionTracker::complete(const CompressionResult& result) {
    if (completed_)
        return;
    completed_ = true;

    auto& stats = CompressionMonitor::getGlobalStats();
    auto& mutex = CompressionMonitor::getGlobalStatsMutex();
    std::lock_guard lock(mutex);

    auto& algoStats = stats.algorithmStats[algorithm_];
    algoStats.recordCompression(result);

    stats.totalCompressedFiles++;
    stats.totalCompressedBytes += result.compressedSize;
    stats.totalSpaceSaved += (originalSize_ - result.compressedSize);
}

void CompressionTracker::failed() {
    if (completed_)
        return;
    completed_ = true;

    auto& stats = CompressionMonitor::getGlobalStats();
    auto& mutex = CompressionMonitor::getGlobalStatsMutex();
    std::lock_guard lock(mutex);

    auto it = stats.algorithmStats.find(algorithm_);
    if (it != stats.algorithmStats.end()) {
        it->second.compressionErrors++;
    }
}

CompressionTracker::~CompressionTracker() {
    if (!completed_) {
        failed();
    }
}

//-----------------------------------------------------------------------------
// DecompressionTracker
//-----------------------------------------------------------------------------

DecompressionTracker::DecompressionTracker(CompressionAlgorithm algo, size_t compressedSize)
    : algorithm_(algo), compressedSize_(compressedSize),
      startTime_(std::chrono::steady_clock::now()) {}

void DecompressionTracker::complete(size_t decompressedSize) {
    if (completed_)
        return;
    completed_ = true;

    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - startTime_);

    auto& stats = CompressionMonitor::getGlobalStats();
    auto& mutex = CompressionMonitor::getGlobalStatsMutex();
    std::lock_guard lock(mutex);

    auto& algoStats = stats.algorithmStats[algorithm_];
    algoStats.recordDecompression(compressedSize_, decompressedSize, duration);
}

void DecompressionTracker::failed() {
    if (completed_)
        return;
    completed_ = true;

    auto& stats = CompressionMonitor::getGlobalStats();
    auto& mutex = CompressionMonitor::getGlobalStatsMutex();
    std::lock_guard lock(mutex);

    auto it = stats.algorithmStats.find(algorithm_);
    if (it != stats.algorithmStats.end()) {
        it->second.decompressionErrors++;
    }
}

DecompressionTracker::~DecompressionTracker() {
    if (!completed_) {
        failed();
    }
}

} // namespace yams::compression