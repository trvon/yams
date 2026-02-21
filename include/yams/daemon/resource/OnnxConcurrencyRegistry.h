#pragma once

// OnnxConcurrencyRegistry
// -----------------------
// Global ONNX concurrency coordination with reserved lanes per component.
// Prevents any single component from starving others of ONNX execution slots.
//
// Design principles (C++20):
// - Uses counting_semaphore for efficient slot management
// - Lane-based reservation ensures fairness between GLiNER, embeddings, reranker
// - RAII SlotGuard pattern for exception-safe slot management
// - Singleton pattern with thread-safe access
//
// Integration:
// - GLiNER wraps inference with SlotGuard(OnnxLane::Gliner)
// - Embeddings wrap acquireModel with SlotGuard(OnnxLane::Embedding)
// - Reranker wraps scoring with SlotGuard(OnnxLane::Reranker)
// - TuningManager adjusts total slots under memory pressure

#include <yams/daemon/resource/onnx_resource_export.h>

// MSVC C4251: private members of dllexport class don't need dll-interface
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4251)
#endif

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <semaphore>

namespace yams::daemon {

// ============================================================================
// ONNX Execution Lanes
// ============================================================================

/// Execution lanes for ONNX operations, each with optional reserved slots
enum class OnnxLane : std::uint8_t {
    Gliner = 0,    // GLiNER title/NL extraction
    Embedding = 1, // Embedding generation
    Reranker = 2,  // Search reranking
    Other = 3      // Generic ONNX operations
};

constexpr std::uint8_t kOnnxLaneCount = 4;

// ============================================================================
// Lane Metrics
// ============================================================================

/// Per-lane metrics for observability
struct OnnxLaneMetrics {
    std::uint32_t reserved{0}; // Reserved slots for this lane
    std::uint32_t used{0};     // Currently in-flight
    std::uint32_t queued{0};   // Waiting for slot (approximate)
    std::uint32_t timeouts{0}; // Acquisition timeouts
};

// ============================================================================
// ONNX Concurrency Registry
// ============================================================================

/// Global ONNX concurrency coordination with reserved lanes per component.
/// Prevents any component from being completely starved.
class YAMS_ONNX_RESOURCE_API OnnxConcurrencyRegistry {
public:
    /// Singleton accessor
    static OnnxConcurrencyRegistry& instance() noexcept;

    // Non-copyable, non-movable
    OnnxConcurrencyRegistry(const OnnxConcurrencyRegistry&) = delete;
    OnnxConcurrencyRegistry& operator=(const OnnxConcurrencyRegistry&) = delete;
    OnnxConcurrencyRegistry(OnnxConcurrencyRegistry&&) = delete;
    OnnxConcurrencyRegistry& operator=(OnnxConcurrencyRegistry&&) = delete;

    // ========================================================================
    // Slot Acquisition (blocking with timeout)
    // ========================================================================

    /// Acquire a slot for the specified lane.
    /// First tries to acquire from the lane's reserved pool, then from shared pool.
    /// @param lane The execution lane requesting a slot
    /// @param timeout Maximum time to wait for a slot
    /// @return true if slot acquired, false on timeout
    [[nodiscard]] bool acquireSlot(OnnxLane lane,
                                   std::chrono::milliseconds timeout = std::chrono::seconds(30));

    /// Release a slot back to the appropriate pool.
    /// @param lane The execution lane releasing the slot
    void releaseSlot(OnnxLane lane);

    // ========================================================================
    // RAII Slot Guard
    // ========================================================================

    /// RAII guard for automatic slot release
    class SlotGuard {
    public:
        SlotGuard(OnnxConcurrencyRegistry& reg, OnnxLane lane,
                  std::chrono::milliseconds timeout = std::chrono::seconds(30))
            : reg_(reg), lane_(lane), acquired_(reg.acquireSlot(lane, timeout)) {}

        ~SlotGuard() {
            if (acquired_) {
                reg_.releaseSlot(lane_);
            }
        }

        /// Check if slot was successfully acquired
        [[nodiscard]] bool acquired() const noexcept { return acquired_; }

        /// Explicit conversion to bool for if-statement usage
        explicit operator bool() const noexcept { return acquired_; }

        // Non-copyable, non-movable
        SlotGuard(const SlotGuard&) = delete;
        SlotGuard& operator=(const SlotGuard&) = delete;
        SlotGuard(SlotGuard&&) = delete;
        SlotGuard& operator=(SlotGuard&&) = delete;

    private:
        OnnxConcurrencyRegistry& reg_;
        OnnxLane lane_;
        bool acquired_;
    };

    // ========================================================================
    // Configuration (called by TuningManager)
    // ========================================================================

    /// Set total maximum concurrent ONNX operations
    /// @param total New total slot count (shared + all reserved)
    void setMaxSlots(std::uint32_t total);

    /// Set reserved slots for a specific lane
    /// @param lane The lane to configure
    /// @param reserved Number of slots reserved for this lane
    void setReservedSlots(OnnxLane lane, std::uint32_t reserved);

    // ========================================================================
    // Metrics and Observability
    // ========================================================================

    /// Get total configured slot count
    [[nodiscard]] std::uint32_t totalSlots() const noexcept;

    /// Get currently used slot count (across all lanes)
    [[nodiscard]] std::uint32_t usedSlots() const noexcept;

    /// Get available slot count
    [[nodiscard]] std::uint32_t availableSlots() const noexcept;

    /// Get metrics for a specific lane
    [[nodiscard]] OnnxLaneMetrics laneMetrics(OnnxLane lane) const noexcept;

    /// Full snapshot for FSM integration
    struct Snapshot {
        std::uint32_t totalSlots{0};
        std::uint32_t usedSlots{0};
        std::uint32_t availableSlots{0};
        std::array<OnnxLaneMetrics, kOnnxLaneCount> lanes{};
    };

    /// Get complete metrics snapshot
    [[nodiscard]] Snapshot snapshot() const noexcept;

private:
    OnnxConcurrencyRegistry();
    ~OnnxConcurrencyRegistry() = default;

    // Shared pool semaphore (slots not reserved for any lane)
    // Note: Max semaphore count must accommodate dynamic resizing
    std::counting_semaphore<64> sharedSlots_;

    // Per-lane state
    struct LaneState {
        std::atomic<std::uint32_t> reserved{0}; // Reserved slots for this lane
        std::atomic<std::uint32_t> used{0};     // Currently in-flight
        std::atomic<std::uint32_t> queued{0};   // Waiting (approximate)
        std::atomic<std::uint32_t> timeouts{0}; // Timeout count
        std::atomic<bool> usingReserved{false}; // Whether current slot is from reserved pool
    };

    std::array<LaneState, kOnnxLaneCount> laneStates_;

    // Global configuration
    std::atomic<std::uint32_t> maxSlots_{8};
    std::atomic<std::uint32_t> totalUsed_{0};
};

} // namespace yams::daemon

#ifdef _MSC_VER
#pragma warning(pop)
#endif
