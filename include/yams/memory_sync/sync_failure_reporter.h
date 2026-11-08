#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

namespace yams::memory_sync {

/// Tracks failed sync cycles and decides which ones to log. A cycle can fail every few seconds
/// for hours (a sweep that never completes), so a new error logs at once, a repeat of the same
/// error at most once per interval with a count of the cycles it covers, and the first success
/// after failures logs a recovery line. Thread-safe: the worker records, status readers read.
class SyncFailureReporter {
public:
    using Clock = std::chrono::steady_clock;

    struct Snapshot {
        std::string lastError;    /// empty when the last cycle succeeded
        Clock::time_point lastAt; /// when lastError was recorded
        std::uint64_t consecutive{0};
    };

    explicit SyncFailureReporter(Clock::duration repeatInterval = std::chrono::minutes(10))
        : repeatInterval_(repeatInterval) {}

    /// Records a failed cycle. Returns the line to log, or nullopt when it is a suppressed repeat.
    std::optional<std::string> failed(std::string error, Clock::time_point now) {
        std::scoped_lock lock(mutex_);
        ++consecutive_;
        ++sinceLastReport_;
        const bool sameError = error == lastError_;
        lastError_ = std::move(error);
        lastAt_ = now;
        if (sameError && now - lastReportAt_ < repeatInterval_) {
            return std::nullopt;
        }
        std::string line = "memory sync cycle failed: " + lastError_;
        if (sameError && sinceLastReport_ > 1) {
            line += " (" + std::to_string(sinceLastReport_) + " cycles since the last report, " +
                    std::to_string(consecutive_) + " in a row)";
        }
        lastReportAt_ = now;
        sinceLastReport_ = 0;
        return line;
    }

    /// Records a successful cycle. Returns a recovery line when failures preceded it.
    std::optional<std::string> succeeded() {
        std::scoped_lock lock(mutex_);
        if (consecutive_ == 0) {
            return std::nullopt;
        }
        std::string line =
            "memory sync recovered after " + std::to_string(consecutive_) + " failed cycles";
        consecutive_ = 0;
        sinceLastReport_ = 0;
        lastError_.clear();
        lastReportAt_ = {};
        return line;
    }

    Snapshot snapshot() const {
        std::scoped_lock lock(mutex_);
        return {.lastError = lastError_, .lastAt = lastAt_, .consecutive = consecutive_};
    }

private:
    const Clock::duration repeatInterval_;
    mutable std::mutex mutex_;
    std::string lastError_;
    Clock::time_point lastAt_;
    Clock::time_point lastReportAt_;
    std::uint64_t consecutive_{0};
    std::uint64_t sinceLastReport_{0};
};

/// Writes a SyncFailureReporter line to the daemon log (warning for failures, info for
/// recovery). Defined in memory_sync.cpp so this header stays free of the logging library.
void logSyncCycleReport(std::string_view line, bool failure);

} // namespace yams::memory_sync
