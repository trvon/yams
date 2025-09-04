#pragma once

#include <atomic>
#include <cstdint>

namespace yams {
namespace daemon {

class MuxMetricsRegistry {
public:
    struct Snapshot {
        uint64_t activeHandlers{0};
        int64_t queuedBytes{0};
        uint64_t writerBudgetBytes{0};
    };

    static inline MuxMetricsRegistry& instance() noexcept {
        static MuxMetricsRegistry g;
        return g;
    }

    void incrementActiveHandlers(int64_t delta) noexcept {
        activeHandlers_.fetch_add(delta, std::memory_order_relaxed);
    }
    void addQueuedBytes(int64_t delta) noexcept {
        queuedBytes_.fetch_add(delta, std::memory_order_relaxed);
    }
    void setWriterBudget(uint64_t bytes) noexcept {
        writerBudgetBytes_.store(bytes, std::memory_order_relaxed);
    }

    Snapshot snapshot() const noexcept {
        Snapshot s;
        s.activeHandlers = static_cast<uint64_t>(activeHandlers_.load(std::memory_order_acquire));
        s.queuedBytes = queuedBytes_.load(std::memory_order_acquire);
        s.writerBudgetBytes = writerBudgetBytes_.load(std::memory_order_acquire);
        return s;
    }

private:
    std::atomic<int64_t> activeHandlers_{0};
    std::atomic<int64_t> queuedBytes_{0};
    std::atomic<uint64_t> writerBudgetBytes_{0};
};

} // namespace daemon
} // namespace yams
