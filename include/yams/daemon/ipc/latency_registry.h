#pragma once

#include <array>
#include <atomic>
#include <cstdint>

namespace yams::daemon {

class LatencyRegistry {
public:
    static inline LatencyRegistry& instance() {
        static LatencyRegistry g;
        return g;
    }

    void record(uint64_t ms) noexcept {
        const uint32_t b = bucket(ms);
        buckets_[b].fetch_add(1, std::memory_order_relaxed);
        count_.fetch_add(1, std::memory_order_relaxed);
        sum_.fetch_add(ms, std::memory_order_relaxed);
    }

    struct Snapshot {
        uint64_t count{0};
        uint64_t sum_ms{0};
        std::array<uint64_t, 13> buckets{};
        uint64_t p50_ms{0};
        uint64_t p95_ms{0};
    };

    Snapshot snapshot() const noexcept {
        Snapshot s;
        s.count = count_.load(std::memory_order_acquire);
        s.sum_ms = sum_.load(std::memory_order_acquire);
        for (size_t i = 0; i < buckets_.size(); ++i) {
            s.buckets[i] = buckets_[i].load(std::memory_order_acquire);
        }
        if (s.count > 0) {
            s.p50_ms = percentile(s, 0.50);
            s.p95_ms = percentile(s, 0.95);
        }
        return s;
    }

private:
    static inline uint32_t bucket(uint64_t ms) noexcept {
        if (ms <= 1)
            return 0;
        if (ms <= 2)
            return 1;
        if (ms <= 4)
            return 2;
        if (ms <= 8)
            return 3;
        if (ms <= 16)
            return 4;
        if (ms <= 32)
            return 5;
        if (ms <= 64)
            return 6;
        if (ms <= 128)
            return 7;
        if (ms <= 256)
            return 8;
        if (ms <= 512)
            return 9;
        if (ms <= 1024)
            return 10;
        if (ms <= 2048)
            return 11;
        return 12;
    }

    static inline uint64_t bucket_upper_ms(uint32_t b) noexcept {
        static const uint64_t uppers[] = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096};
        return uppers[b < 13 ? b : 12];
    }

    static inline uint64_t percentile(const Snapshot& s, double q) noexcept {
        uint64_t target = static_cast<uint64_t>(q * s.count);
        if (target == 0)
            target = 1;
        uint64_t acc = 0;
        for (size_t i = 0; i < s.buckets.size(); ++i) {
            acc += s.buckets[i];
            if (acc >= target)
                return bucket_upper_ms(static_cast<uint32_t>(i));
        }
        return bucket_upper_ms(static_cast<uint32_t>(s.buckets.size() - 1));
    }

    LatencyRegistry() = default;
    std::array<std::atomic<uint64_t>, 13> buckets_{};
    std::atomic<uint64_t> count_{0};
    std::atomic<uint64_t> sum_{0};
};

} // namespace yams::daemon
