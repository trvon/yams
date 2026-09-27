#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>

namespace yams::common {

/// Throttles a repeating log message to one line per interval and counts what it held back,
/// so a hot failure path (a full channel, a watcher retrying every pass) cannot flood the log.
///
///     static yams::common::LogRateLimiter limiter{std::chrono::seconds(30)};
///     if (auto held = limiter.admit()) {
///         spdlog::warn("queue full{}", LogRateLimiter::suppressedSuffix(*held));
///     }
///
/// Thread-safe and lock-free; exactly one caller wins each interval.
class LogRateLimiter {
public:
    using Clock = std::chrono::steady_clock;

    explicit LogRateLimiter(Clock::duration interval) noexcept : interval_(interval) {}

    LogRateLimiter(const LogRateLimiter&) = delete;
    LogRateLimiter& operator=(const LogRateLimiter&) = delete;

    /// Returns the number of occurrences suppressed since the last admitted one when this
    /// occurrence should be logged, or nullopt when it should be dropped.
    [[nodiscard]] std::optional<std::uint64_t>
    admit(Clock::time_point now = Clock::now()) noexcept {
        const auto nowTicks = now.time_since_epoch().count();
        auto next = nextAllowedTicks_.load(std::memory_order_relaxed);
        while (nowTicks >= next) {
            if (nextAllowedTicks_.compare_exchange_weak(next, nowTicks + interval_.count(),
                                                        std::memory_order_acq_rel)) {
                return suppressed_.exchange(0, std::memory_order_acq_rel);
            }
        }
        suppressed_.fetch_add(1, std::memory_order_relaxed);
        return std::nullopt;
    }

    /// " (N similar messages suppressed)" for N > 0, otherwise empty.
    [[nodiscard]] static std::string suppressedSuffix(std::uint64_t suppressed) {
        if (suppressed == 0) {
            return {};
        }
        return " (" + std::to_string(suppressed) + " similar message" +
               (suppressed == 1 ? "" : "s") + " suppressed)";
    }

private:
    Clock::duration interval_;
    std::atomic<Clock::rep> nextAllowedTicks_{std::numeric_limits<Clock::rep>::min()};
    std::atomic<std::uint64_t> suppressed_{0};
};

} // namespace yams::common
