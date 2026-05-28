#pragma once

#include <cstdint>

#include <yams/daemon/components/AtomicSnapshotRegistry.h>

namespace yams::daemon {

// Lightweight, precomputed view of tuning knobs used frequently by hot paths.
// Produced periodically by TuningManager and consumed read-mostly by other components.
struct TuningSnapshot {
    // Cadence and timers
    uint32_t workerPollMs{150};
    uint32_t backpressureReadPauseMs{10};
    bool daemonIdle{false};

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

    uint32_t repairDegradeHoldMs{750};
    uint32_t repairReadyHoldMs{1500};

    std::size_t serverMaxInflightPerConn{64};
    std::size_t serverQueueFramesCap{1024};
    std::size_t serverQueueBytesCap{128ull * 1024ull * 1024ull};
    std::size_t serverWriterBudgetBytesPerTurn{8ull * 1024ull * 1024ull};
    std::size_t serverWriterBudgetMaxBytesPerTurn{8ull * 1024ull * 1024ull};
};

// Registry exposing the latest snapshot using atomic shared_ptr for zero-copy reads.
using TuningSnapshotRegistry = AtomicSnapshotRegistry<TuningSnapshot>;

} // namespace yams::daemon
