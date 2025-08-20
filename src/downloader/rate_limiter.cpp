/*
 * yams/src/downloader/rate_limiter.cpp
 *
 * Basic token-bucket RateLimiter (MVP)
 * - Honors limits.globalBps and limits.perConnectionBps if > 0
 * - No-ops when both limits are zero (unlimited)
 * - Cooperative cancellation via ShouldCancel
 *
 * Notes:
 * - This implementation is intentionally simple and portable.
 * - It uses a token-bucket with capacity = rate (i.e., burst <= 1 second of allowance).
 * - Tokens are doubles to allow partial-byte accumulation between sleeps.
 * - Thread-safe for concurrent acquire() calls.
 */

#include <yams/downloader/downloader.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <thread>

namespace yams::downloader {

namespace {

using clock_t = std::chrono::steady_clock;

struct Bucket {
    // rate in bytes per second (0 => unlimited)
    double rate_bps{0.0};
    // capacity in bytes (burst); set to rate_bps (1 second worth)
    double capacity{0.0};
    // current tokens (bytes)
    double tokens{0.0};
    // last refill time
    clock_t::time_point last_refill{clock_t::now()};
};

class TokenBucketLimiter final : public IRateLimiter {
public:
    TokenBucketLimiter() = default;
    ~TokenBucketLimiter() override = default;

    void setLimits(const RateLimit& limit) override {
        std::lock_guard<std::mutex> lk(mutex_);
        limits_ = limit;
        const auto now = clock_t::now();

        initBucket(global_, static_cast<double>(limits_.globalBps), now);
        initBucket(per_conn_, static_cast<double>(limits_.perConnectionBps), now);
    }

    void acquire(std::uint64_t bytes, const ShouldCancel& shouldCancel) override {
        if (bytes == 0)
            return;

        // Fast-path: if both unlimited, return immediately
        {
            std::lock_guard<std::mutex> lk(mutex_);
            if ((global_.rate_bps <= 0.0) && (per_conn_.rate_bps <= 0.0)) {
                return;
            }
        }

        // Loop until we can deduct 'bytes' from both buckets (if limited)
        while (true) {
            if (shouldCancel && shouldCancel()) {
                return; // cooperative cancel
            }

            double need_global = 0.0;
            double need_conn = 0.0;
            double wait_seconds = 0.0;
            bool ready = false;

            {
                std::lock_guard<std::mutex> lk(mutex_);
                const auto now = clock_t::now();
                refill(global_, now);
                refill(per_conn_, now);

                // Determine deficits (only for limited buckets)
                need_global = needFor(global_, bytes);
                need_conn = needFor(per_conn_, bytes);

                if (need_global <= 0.0 && need_conn <= 0.0) {
                    // We can deduct now from both buckets (where limited)
                    deduct(global_, bytes);
                    deduct(per_conn_, bytes);
                    ready = true;
                } else {
                    // Compute time to accumulate enough tokens
                    const double t_global = timeToAccumulate(global_, need_global);
                    const double t_conn = timeToAccumulate(per_conn_, need_conn);
                    wait_seconds = std::max(t_global, t_conn);
                }
            }

            if (ready) {
                return;
            }

            // Sleep in small increments to allow cancel checks
            constexpr auto max_slice = std::chrono::milliseconds(50);
            auto sleep_for = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::duration<double>(wait_seconds));
            if (sleep_for.count() <= 0) {
                // Yield if computed wait is extremely small
                std::this_thread::yield();
            } else {
                if (sleep_for > max_slice)
                    sleep_for = max_slice;
                std::this_thread::sleep_for(sleep_for);
            }
        }
    }

private:
    static void initBucket(Bucket& b, double rate_bps, clock_t::time_point now) {
        b.rate_bps = rate_bps;
        if (b.rate_bps > 0.0) {
            b.capacity = b.rate_bps; // 1 second burst
            // Initialize tokens to full capacity to allow immediate burst up to 1s allowance
            b.tokens = b.capacity;
        } else {
            b.capacity = 0.0;
            b.tokens = 0.0;
        }
        b.last_refill = now;
    }

    static void refill(Bucket& b, clock_t::time_point now) {
        if (b.rate_bps <= 0.0) {
            // Unlimited bucket: nothing to do
            b.last_refill = now;
            return;
        }

        const auto dt = std::chrono::duration<double>(now - b.last_refill).count();
        if (dt <= 0.0)
            return;

        const double add = b.rate_bps * dt;
        b.tokens = std::min(b.capacity, b.tokens + add);
        b.last_refill = now;
    }

    static double needFor(const Bucket& b, std::uint64_t bytes) {
        if (b.rate_bps <= 0.0)
            return 0.0; // unlimited
        const double need = static_cast<double>(bytes) - b.tokens;
        return (need > 0.0) ? need : 0.0;
    }

    static double timeToAccumulate(const Bucket& b, double need) {
        if (b.rate_bps <= 0.0 || need <= 0.0)
            return 0.0;
        return need / b.rate_bps;
    }

    static void deduct(Bucket& b, std::uint64_t bytes) {
        if (b.rate_bps <= 0.0)
            return; // unlimited
        const double d = static_cast<double>(bytes);
        // Clamp to non-negative
        b.tokens = (b.tokens > d) ? (b.tokens - d) : 0.0;
    }

private:
    std::mutex mutex_;
    RateLimit limits_{};
    Bucket global_{};
    Bucket per_conn_{};
};

} // namespace

// Optional factory (useful for wiring without exposing class)
std::unique_ptr<IRateLimiter> makeRateLimiter() {
    return std::make_unique<TokenBucketLimiter>();
}

} // namespace yams::downloader