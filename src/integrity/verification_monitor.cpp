#include <yams/integrity/verifier.h>

#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <deque>
#include <iomanip>
#include <mutex>

namespace yams::integrity {

/**
 * Rate tracking helper class
 */
class RateTracker {
private:
    struct Event {
        std::chrono::steady_clock::time_point timestamp;
        uint64_t count;
    };

    std::deque<Event> events;
    std::chrono::seconds windowSize;
    mutable std::mutex mutex;

public:
    explicit RateTracker(std::chrono::seconds window = std::chrono::seconds{60})
        : windowSize(window) {}

    void recordEvent(uint64_t count = 1) {
        std::lock_guard lock(mutex);

        auto now = std::chrono::steady_clock::now();
        events.push_back({now, count});

        // Remove old events outside the window
        auto cutoff = now - windowSize;
        while (!events.empty() && events.front().timestamp < cutoff) {
            events.pop_front();
        }
    }

    double getRate() const {
        std::lock_guard lock(mutex);

        if (events.empty()) {
            return 0.0;
        }

        auto now = std::chrono::steady_clock::now();
        auto cutoff = now - windowSize;

        uint64_t totalCount = 0;
        auto firstEventTime = now;

        for (const auto& event : events) {
            if (event.timestamp >= cutoff) {
                totalCount += event.count;
                firstEventTime = std::min(firstEventTime, event.timestamp);
            }
        }

        if (totalCount == 0) {
            return 0.0;
        }

        auto actualWindow =
            std::chrono::duration_cast<std::chrono::milliseconds>(now - firstEventTime);
        if (actualWindow.count() == 0) {
            return 0.0;
        }

        return static_cast<double>(totalCount) * 1000.0 / actualWindow.count(); // per second
    }

    uint64_t getTotalCount() const {
        std::lock_guard lock(mutex);

        uint64_t total = 0;
        for (const auto& event : events) {
            total += event.count;
        }
        return total;
    }
};

/**
 * Implementation details for VerificationMonitor
 */
struct VerificationMonitor::Impl {
    // Rate trackers
    RateTracker verificationTracker;
    RateTracker errorTracker;

    // Counters
    std::atomic<uint64_t> totalVerifications{0};
    std::atomic<uint64_t> totalErrors{0};
    std::atomic<uint64_t> repairAttempts{0};
    std::atomic<uint64_t> successfulRepairs{0};

    // Alerting
    double errorRateThreshold = 0.1; // 10% error rate threshold
    mutable std::mutex alertMutex;

    // Recent verification results for analysis
    std::deque<VerificationResult> recentResults;
    mutable std::mutex resultsMutex;
    static constexpr size_t MAX_RECENT_RESULTS = 1000;

    // Global mutex for thread-safe operations
    // Protects rate trackers and coordinate access between different operations
    mutable std::mutex globalMutex;

    void recordVerificationInternal(const VerificationResult& result) {
        // Use global mutex to ensure thread-safe access to rate trackers
        std::lock_guard globalLock(globalMutex);

        totalVerifications++;
        verificationTracker.recordEvent();

        if (!result.isSuccess()) {
            totalErrors++;
            errorTracker.recordEvent();
        }

        // Store recent result for analysis (using separate mutex to avoid deadlock)
        {
            std::lock_guard lock(resultsMutex);
            recentResults.push_back(result);
            if (recentResults.size() > MAX_RECENT_RESULTS) {
                recentResults.pop_front();
            }
        }

        spdlog::debug("Recorded verification result for block {}: {}",
                      result.blockHash.substr(0, 8),
                      result.status == VerificationStatus::Passed ? "PASS" : "FAIL");
    }

    void recordRepairAttemptInternal(const std::string& hash, bool success) {
        repairAttempts++;
        if (success) {
            successfulRepairs++;
        }

        spdlog::info("Recorded repair attempt for block {}: {}", hash.substr(0, 8),
                     success ? "SUCCESS" : "FAILED");
    }

    bool shouldAlertInternal() const {
        std::lock_guard globalLock(globalMutex);
        std::lock_guard alertLock(alertMutex);

        auto currentErrorRate = errorTracker.getRate();
        auto currentVerificationRate = verificationTracker.getRate();

        // Only alert if we have sufficient verification activity
        if (currentVerificationRate < 1.0) {
            return false;
        }

        double errorRatio =
            currentVerificationRate > 0 ? currentErrorRate / currentVerificationRate : 0.0;

        bool shouldAlert = errorRatio > errorRateThreshold;

        if (shouldAlert) {
            spdlog::warn("Error rate threshold exceeded: {:.2f}% > {:.2f}%", errorRatio * 100,
                         errorRateThreshold * 100);
        }

        return shouldAlert;
    }

    void setErrorRateThresholdInternal(double threshold) {
        std::lock_guard lock(alertMutex);
        errorRateThreshold = threshold;
        spdlog::info("Set error rate threshold to {:.2f}%", threshold * 100);
    }

    Metrics getCurrentMetricsInternal() const {
        std::lock_guard globalLock(globalMutex);

        Metrics metrics;

        metrics.verificationsPerSecond = static_cast<uint64_t>(verificationTracker.getRate());
        metrics.errorsPerSecond = static_cast<uint64_t>(errorTracker.getRate());
        metrics.totalVerifications = totalVerifications.load();
        metrics.totalErrors = totalErrors.load();
        metrics.repairAttempts = repairAttempts.load();
        metrics.successfulRepairs = successfulRepairs.load();

        // Calculate current error rate
        if (metrics.verificationsPerSecond > 0) {
            metrics.currentErrorRate =
                static_cast<double>(metrics.errorsPerSecond) / metrics.verificationsPerSecond;
        } else {
            metrics.currentErrorRate = 0.0;
        }

        return metrics;
    }

    void exportMetricsInternal(std::ostream& out) const {
        auto metrics = getCurrentMetricsInternal();

        out << "# Integrity Verification Metrics\n";
        out << "# Generated at: " << std::chrono::system_clock::now().time_since_epoch().count()
            << "\n";
        out << "\n";

        // Rate metrics
        out << "kronos_verification_rate " << metrics.verificationsPerSecond << "\n";
        out << "kronos_verification_error_rate " << metrics.errorsPerSecond << "\n";
        out << "kronos_verification_current_error_ratio " << std::fixed << std::setprecision(4)
            << metrics.currentErrorRate << "\n";

        // Counter metrics
        out << "kronos_verification_total " << metrics.totalVerifications << "\n";
        out << "kronos_verification_errors_total " << metrics.totalErrors << "\n";
        out << "kronos_verification_repairs_attempted_total " << metrics.repairAttempts << "\n";
        out << "kronos_verification_repairs_successful_total " << metrics.successfulRepairs << "\n";

        // Derived metrics
        if (metrics.totalVerifications > 0) {
            double overallErrorRate =
                static_cast<double>(metrics.totalErrors) / metrics.totalVerifications;
            out << "kronos_verification_overall_error_rate " << std::fixed << std::setprecision(4)
                << overallErrorRate << "\n";
        }

        if (metrics.repairAttempts > 0) {
            double repairSuccessRate =
                static_cast<double>(metrics.successfulRepairs) / metrics.repairAttempts;
            out << "kronos_verification_repair_success_rate " << std::fixed << std::setprecision(4)
                << repairSuccessRate << "\n";
        }

        // Status breakdown
        {
            std::lock_guard lock(resultsMutex);

            size_t passed = 0, failed = 0, missing = 0, corrupted = 0, repaired = 0;

            for (const auto& result : recentResults) {
                switch (result.status) {
                    case VerificationStatus::Passed:
                        passed++;
                        break;
                    case VerificationStatus::Failed:
                        failed++;
                        break;
                    case VerificationStatus::Missing:
                        missing++;
                        break;
                    case VerificationStatus::Corrupted:
                        corrupted++;
                        break;
                    case VerificationStatus::Repaired:
                        repaired++;
                        break;
                }
            }

            out << "kronos_verification_status{status=\"passed\"} " << passed << "\n";
            out << "kronos_verification_status{status=\"failed\"} " << failed << "\n";
            out << "kronos_verification_status{status=\"missing\"} " << missing << "\n";
            out << "kronos_verification_status{status=\"corrupted\"} " << corrupted << "\n";
            out << "kronos_verification_status{status=\"repaired\"} " << repaired << "\n";
        }

        out << std::flush;
    }
};

// VerificationMonitor implementation

VerificationMonitor::VerificationMonitor() : pImpl(std::make_unique<Impl>()) {}

VerificationMonitor::~VerificationMonitor() = default;

void VerificationMonitor::recordVerification(const VerificationResult& result) {
    pImpl->recordVerificationInternal(result);
}

void VerificationMonitor::recordRepairAttempt(const std::string& hash, bool success) {
    pImpl->recordRepairAttemptInternal(hash, success);
}

void VerificationMonitor::setErrorRateThreshold(double threshold) {
    pImpl->setErrorRateThresholdInternal(threshold);
}

bool VerificationMonitor::shouldAlert() const {
    return pImpl->shouldAlertInternal();
}

VerificationMonitor::Metrics VerificationMonitor::getCurrentMetrics() const {
    return pImpl->getCurrentMetricsInternal();
}

void VerificationMonitor::exportMetrics(std::ostream& out) const {
    pImpl->exportMetricsInternal(out);
}

} // namespace yams::integrity