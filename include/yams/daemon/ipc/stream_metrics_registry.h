#pragma once

#include <atomic>
#include <cstdint>

namespace yams {
namespace daemon {

class StreamMetricsRegistry {
public:
    struct Snapshot {
        uint64_t totalStreams{0};
        uint64_t batchesEmitted{0};
        uint64_t keepalives{0};
        uint64_t ttfbCount{0};
        uint64_t ttfbSumMs{0};
    };

    static inline StreamMetricsRegistry& instance() noexcept {
        static StreamMetricsRegistry g;
        return g;
    }

    inline void incStreams(uint64_t n = 1) noexcept {
        totalStreams_.fetch_add(n, std::memory_order_relaxed);
    }
    inline void incBatches(uint64_t n = 1) noexcept {
        batchesEmitted_.fetch_add(n, std::memory_order_relaxed);
    }
    inline void incKeepalive(uint64_t n = 1) noexcept {
        keepalives_.fetch_add(n, std::memory_order_relaxed);
    }
    inline void addTtfb(uint64_t ms) noexcept {
        ttfbSumMs_.fetch_add(ms, std::memory_order_relaxed);
        ttfbCount_.fetch_add(1, std::memory_order_relaxed);
    }

    inline Snapshot snapshot() const noexcept {
        Snapshot s;
        s.totalStreams = totalStreams_.load(std::memory_order_acquire);
        s.batchesEmitted = batchesEmitted_.load(std::memory_order_acquire);
        s.keepalives = keepalives_.load(std::memory_order_acquire);
        s.ttfbCount = ttfbCount_.load(std::memory_order_acquire);
        s.ttfbSumMs = ttfbSumMs_.load(std::memory_order_acquire);
        return s;
    }

private:
    std::atomic<uint64_t> totalStreams_{0};
    std::atomic<uint64_t> batchesEmitted_{0};
    std::atomic<uint64_t> keepalives_{0};
    std::atomic<uint64_t> ttfbCount_{0};
    std::atomic<uint64_t> ttfbSumMs_{0};
};

} // namespace daemon
} // namespace yams
