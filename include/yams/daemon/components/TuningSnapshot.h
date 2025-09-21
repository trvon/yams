#pragma once

#include <atomic>
#include <cstdint>
#include <memory>

namespace yams::daemon {

// Lightweight, precomputed view of tuning knobs used frequently by hot paths.
// Produced periodically by TuningManager and consumed read-mostly by other components.
struct TuningSnapshot {
    // Cadence and timers
    uint32_t workerPollMs{150};
    uint32_t backpressureReadPauseMs{10};

    // Idle/pressure thresholds
    double idleCpuPct{10.0};
    std::uint64_t idleMuxLowBytes{4ull * 1024ull * 1024ull};
    uint32_t idleShrinkHoldMs{5000};
    int poolScaleStep{1};
    uint32_t poolCooldownMs{500};

    // Pool bounds
    uint32_t poolIpcMin{1};
    uint32_t poolIpcMax{32};
    uint32_t poolIoMin{1};
    uint32_t poolIoMax{32};

    // Writer/mux basics
    std::size_t writerBudgetBytesPerTurn{3072u * 1024u};
};

// Registry exposing the latest snapshot using atomic shared_ptr for zero-copy reads.
class TuningSnapshotRegistry {
public:
    static TuningSnapshotRegistry& instance() {
        static TuningSnapshotRegistry reg;
        return reg;
    }

    void set(std::shared_ptr<const TuningSnapshot> snap) {
        // Use C++11 atomic free functions for shared_ptr to support libc++
        std::atomic_store_explicit(&snap_, std::move(snap), std::memory_order_release);
    }

    std::shared_ptr<const TuningSnapshot> get() const {
        return std::atomic_load_explicit(&snap_, std::memory_order_acquire);
    }

private:
    TuningSnapshotRegistry() = default;
    // Plain shared_ptr storage; synchronization via atomic_load/store free functions above
    std::shared_ptr<const TuningSnapshot> snap_{nullptr};
};

} // namespace yams::daemon
