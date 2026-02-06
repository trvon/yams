#pragma once

#include <atomic>
#include <chrono>
#include <string>
#include <mutex>
#include <optional>

namespace yams::daemon {

/// Configuration for GradientLimiter
struct GradientLimiterConfig {
    double initialLimit = 4.0;       ///< Starting concurrency
    double minLimit = 1.0;           ///< Floor - never below this
    double maxLimit = 32.0;          ///< Ceiling - never above this
    double smoothingAlpha = 0.2;     ///< Short-window EMA alpha
    double longWindowAlpha = 0.05;   ///< Long-window EMA alpha (drift correction)
    double tolerance = 1.5;          ///< Queue tolerance multiplier
    uint32_t warmupSamples = 10;     ///< Samples before adjusting limits
    bool enableProbing = true;       ///< Enable limit increase probing
};

/// Netflix Gradient2-style adaptive concurrency limiter
/// Based on TCP Vegas congestion control principles
/// Automatically learns optimal concurrency by measuring latency feedback
class GradientLimiter {
public:
    using Config = GradientLimiterConfig;

    struct Metrics {
        double limit = 0.0;
        double minRtt = 0.0;
        double smoothedRtt = 0.0;
        double longRtt = 0.0;
        double gradient = 0.0;
        uint32_t inFlight = 0;
        uint64_t acquireCount = 0;
        uint64_t rejectCount = 0;
    };

    explicit GradientLimiter(std::string name);
    explicit GradientLimiter(std::string name, Config config);

    /// Check if a new job can start
    /// @return true if slot acquired, false if at limit
    bool tryAcquire();

    /// Called when a job completes
    /// @param rtt End-to-end latency (including queue wait time)
    /// @param success Whether job completed successfully
    void onJobComplete(std::chrono::nanoseconds rtt, bool success);

    /// Called when a job starts (after acquire)
    void onJobStart();

    /// Called when a job ends (before onJobComplete)
    void onJobEnd();

    /// Get current effective concurrency limit
    uint32_t effectiveLimit() const {
        return static_cast<uint32_t>(limit_.load(std::memory_order_relaxed));
    }

    /// Get current in-flight count
    uint32_t inFlight() const {
        return inFlight_.load(std::memory_order_relaxed);
    }

    /// Get metrics snapshot
    Metrics metrics() const;

    /// Apply back-pressure from ResourceGovernor.
    /// Warning  → clamp limit to 75% of maxLimit
    /// Critical → force limit to minLimit
    /// Emergency→ force limit to 0 (full stop)
    /// Normal   → no-op (limit recovers organically via gradient)
    /// @param level 0=Normal, 1=Warning, 2=Critical, 3=Emergency
    void applyPressure(uint8_t level);

    /// Reset limiter state (for testing)
    void reset();

private:
    std::string name_;
    Config config_;

    // Core state (atomic for thread safety)
    std::atomic<double> limit_;
    std::atomic<uint32_t> inFlight_{0};

    // RTT tracking
    std::atomic<double> minRtt_{0.0};       ///< Baseline RTT (no queuing)
    std::atomic<double> smoothedRtt_{0.0};  ///< Short-window EMA
    std::atomic<double> longRtt_{0.0};      ///< Long-window EMA (drift correction)
    std::atomic<double> gradient_{1.0};     ///< Current gradient value

    // Sample tracking
    std::atomic<uint64_t> sampleCount_{0};
    std::atomic<bool> inWarmup_{true};

    // Metrics
    mutable std::mutex metricsMutex_;
    std::atomic<uint64_t> acquireCount_{0};
    std::atomic<uint64_t> rejectCount_{0};

    void updateLimit(double rttNanos);
    void enterCooldown();
};

} // namespace yams::daemon
