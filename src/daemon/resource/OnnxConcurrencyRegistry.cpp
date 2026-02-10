#include <yams/daemon/resource/OnnxConcurrencyRegistry.h>

#include <spdlog/spdlog.h>
#include <algorithm>

namespace yams::daemon {

// ============================================================================
// Singleton
// ============================================================================

OnnxConcurrencyRegistry& OnnxConcurrencyRegistry::instance() noexcept {
    static OnnxConcurrencyRegistry instance;
    return instance;
}

// ============================================================================
// Constructor
// ============================================================================

OnnxConcurrencyRegistry::OnnxConcurrencyRegistry()
    : sharedSlots_(4) // Default: 4 shared slots
{
    // Default reserved slots by lane (conservative defaults)
    // These can be overridden by TuneAdvisor env vars
    laneStates_[static_cast<std::size_t>(OnnxLane::Gliner)].reserved.store(1);
    laneStates_[static_cast<std::size_t>(OnnxLane::Embedding)].reserved.store(1);
    laneStates_[static_cast<std::size_t>(OnnxLane::Reranker)].reserved.store(1);
    laneStates_[static_cast<std::size_t>(OnnxLane::Other)].reserved.store(0);

    // Default max slots: 8 (4 shared + 4 reserved across lanes)
    maxSlots_.store(8);

    spdlog::debug("[OnnxConcurrencyRegistry] Initialized with {} max slots", maxSlots_.load());
}

// ============================================================================
// Slot Acquisition
// ============================================================================

bool OnnxConcurrencyRegistry::acquireSlot(OnnxLane lane, std::chrono::milliseconds timeout) {
    auto laneIdx = static_cast<std::size_t>(lane);
    auto& state = laneStates_[laneIdx];

    // Track queued count for metrics
    state.queued.fetch_add(1, std::memory_order_relaxed);

    // First, try to use the lane's reserved capacity via CAS loop (no TOCTOU)
    {
        std::uint32_t currentUsed = state.used.load(std::memory_order_acquire);
        std::uint32_t reserved = state.reserved.load(std::memory_order_acquire);

        while (currentUsed < reserved) {
            if (state.used.compare_exchange_weak(currentUsed, currentUsed + 1,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
                state.queued.fetch_sub(1, std::memory_order_relaxed);
                totalUsed_.fetch_add(1, std::memory_order_relaxed);
                return true;
            }
            // currentUsed reloaded by CAS failure; re-check against reserved
            reserved = state.reserved.load(std::memory_order_acquire);
        }
    }

    // Check global budget before trying shared pool
    {
        std::uint32_t total = totalUsed_.load(std::memory_order_acquire);
        std::uint32_t max = maxSlots_.load(std::memory_order_acquire);
        if (total >= max) {
            // Over budget — don't even try the semaphore
            state.queued.fetch_sub(1, std::memory_order_relaxed);
            state.timeouts.fetch_add(1, std::memory_order_relaxed);
            spdlog::debug("[OnnxConcurrencyRegistry] Over budget ({}/{}) for lane {}", total, max,
                          laneIdx);
            return false;
        }
    }

    // Reserved slots exhausted, try shared pool
    bool acquired = sharedSlots_.try_acquire_for(timeout);

    state.queued.fetch_sub(1, std::memory_order_relaxed);

    if (acquired) {
        state.used.fetch_add(1, std::memory_order_release);
        totalUsed_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    // Timeout
    state.timeouts.fetch_add(1, std::memory_order_relaxed);
    spdlog::debug("[OnnxConcurrencyRegistry] Slot acquisition timeout for lane {}", laneIdx);
    return false;
}

void OnnxConcurrencyRegistry::releaseSlot(OnnxLane lane) {
    auto laneIdx = static_cast<std::size_t>(lane);
    auto& state = laneStates_[laneIdx];

    std::uint32_t prevUsed = state.used.fetch_sub(1, std::memory_order_release);
    totalUsed_.fetch_sub(1, std::memory_order_relaxed);

    // Determine if this was a reserved slot or shared slot
    // If prevUsed <= reserved, it was using reserved capacity, no semaphore release needed
    std::uint32_t reserved = state.reserved.load(std::memory_order_acquire);

    if (prevUsed > reserved) {
        // Was using a shared slot - release it back
        sharedSlots_.release();
    }
    // else: was using reserved capacity, nothing to release
}

// ============================================================================
// Configuration
// ============================================================================

void OnnxConcurrencyRegistry::setMaxSlots(std::uint32_t total) {
    std::uint32_t oldMax = maxSlots_.exchange(total, std::memory_order_release);

    // Calculate how many shared slots we should have
    // shared = total - sum(reserved)
    std::uint32_t totalReserved = 0;
    for (std::size_t i = 0; i < kOnnxLaneCount; ++i) {
        totalReserved += laneStates_[i].reserved.load(std::memory_order_acquire);
    }

    // Note: We can't easily resize std::counting_semaphore dynamically
    // The semaphore was initialized with a max count, and we work within that
    // For now, log the change and let natural slot recycling handle it

    if (total != oldMax) {
        spdlog::info("[OnnxConcurrencyRegistry] Max slots changed: {} -> {} (reserved={})", oldMax,
                     total, totalReserved);
    }
}

void OnnxConcurrencyRegistry::setReservedSlots(OnnxLane lane, std::uint32_t reserved) {
    auto laneIdx = static_cast<std::size_t>(lane);
    std::uint32_t oldReserved =
        laneStates_[laneIdx].reserved.exchange(reserved, std::memory_order_release);
    if (reserved != oldReserved) {
        spdlog::debug("[OnnxConcurrencyRegistry] Lane {} reserved slots: {} -> {}", laneIdx,
                      oldReserved, reserved);
    }

    // Note: callers may update maxSlots and reserved slots in separate operations.
    // Ensure maxSlots never falls below total reserved to preserve accounting invariants.
    std::uint32_t totalReserved = 0;
    for (std::size_t i = 0; i < kOnnxLaneCount; ++i) {
        totalReserved += laneStates_[i].reserved.load(std::memory_order_acquire);
    }
    std::uint32_t currentMax = maxSlots_.load(std::memory_order_acquire);
    if (currentMax < totalReserved) {
        maxSlots_.store(totalReserved, std::memory_order_release);
        spdlog::warn("[OnnxConcurrencyRegistry] maxSlots raised to {} to satisfy reserved={}",
                     totalReserved, totalReserved);
    }
}

// ============================================================================
// Metrics
// ============================================================================

std::uint32_t OnnxConcurrencyRegistry::totalSlots() const noexcept {
    return maxSlots_.load(std::memory_order_acquire);
}

std::uint32_t OnnxConcurrencyRegistry::usedSlots() const noexcept {
    return totalUsed_.load(std::memory_order_acquire);
}

std::uint32_t OnnxConcurrencyRegistry::availableSlots() const noexcept {
    std::uint32_t total = maxSlots_.load(std::memory_order_acquire);
    std::uint32_t used = totalUsed_.load(std::memory_order_acquire);
    return (used < total) ? (total - used) : 0;
}

OnnxLaneMetrics OnnxConcurrencyRegistry::laneMetrics(OnnxLane lane) const noexcept {
    auto laneIdx = static_cast<std::size_t>(lane);
    const auto& state = laneStates_[laneIdx];

    OnnxLaneMetrics metrics;
    metrics.reserved = state.reserved.load(std::memory_order_acquire);
    metrics.used = state.used.load(std::memory_order_acquire);
    metrics.queued = state.queued.load(std::memory_order_acquire);
    metrics.timeouts = state.timeouts.load(std::memory_order_acquire);
    return metrics;
}

OnnxConcurrencyRegistry::Snapshot OnnxConcurrencyRegistry::snapshot() const noexcept {
    Snapshot snap;
    snap.totalSlots = maxSlots_.load(std::memory_order_acquire);
    snap.usedSlots = totalUsed_.load(std::memory_order_acquire);
    snap.availableSlots =
        (snap.usedSlots < snap.totalSlots) ? (snap.totalSlots - snap.usedSlots) : 0;

    for (std::size_t i = 0; i < kOnnxLaneCount; ++i) {
        const auto& state = laneStates_[i];
        snap.lanes[i].reserved = state.reserved.load(std::memory_order_acquire);
        snap.lanes[i].used = state.used.load(std::memory_order_acquire);
        snap.lanes[i].queued = state.queued.load(std::memory_order_acquire);
        snap.lanes[i].timeouts = state.timeouts.load(std::memory_order_acquire);
    }

    return snap;
}

} // namespace yams::daemon
