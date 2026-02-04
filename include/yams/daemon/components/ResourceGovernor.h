#pragma once

// ResourceGovernor
// ----------------
// Centralized daemon-scoped resource pressure management. Monitors cumulative
// memory usage across all scaling dimensions and coordinates graduated responses
// to prevent OOM conditions.
//
// Design principles (C++20):
// - Uses concepts for type constraints
// - Designated initializers for snapshot structs
// - constexpr for compile-time constants
// - Singleton pattern with thread-safe access
//
// Integration:
// - Called from TuningManager::tick_once() every ~250ms
// - Gates scaling decisions via canScaleUp(), canLoadModel(), canAdmitWork()
// - Triggers pressure responses via onWarningLevel(), onCriticalLevel(), etc.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <shared_mutex>
#include <string>
#include <string_view>

namespace yams::daemon {

class ServiceManager;

// ============================================================================
// Pressure Levels
// ============================================================================

/// Resource pressure levels with graduated responses
enum class ResourcePressureLevel : std::uint8_t {
    Normal = 0,   // < warning threshold - full operations allowed
    Warning = 1,  // warning-critical range - cap scaling, reduce concurrency
    Critical = 2, // critical-emergency range - evict models, shrink pools
    Emergency = 3 // > emergency threshold - halt new work, aggressive eviction
};

/// Convert pressure level to string for logging
constexpr std::string_view pressureLevelName(ResourcePressureLevel level) noexcept {
    switch (level) {
        case ResourcePressureLevel::Normal:
            return "Normal";
        case ResourcePressureLevel::Warning:
            return "Warning";
        case ResourcePressureLevel::Critical:
            return "Critical";
        case ResourcePressureLevel::Emergency:
            return "Emergency";
    }
    return "Unknown";
}

// ============================================================================
// Resource Snapshot
// ============================================================================

/// Snapshot of ALL managed scaling dimensions at a point in time.
/// Uses designated initializers for clarity at construction sites.
struct ResourceSnapshot {
    // Memory metrics
    std::uint64_t rssBytes{0};          // Current resident set size
    std::uint64_t memoryBudgetBytes{0}; // Configured memory budget
    double memoryPressure{0.0};         // rssBytes / memoryBudgetBytes (0.0-1.0+)

    // CPU metrics
    double cpuUsagePercent{0.0}; // Process CPU % across all cores

    // Model dimension
    std::uint32_t loadedModels{0};
    std::uint64_t modelMemoryBytes{0};

    // Worker pool dimensions
    std::uint32_t ingestWorkers{0};
    std::uint32_t searchConcurrency{0};
    std::uint32_t extractionConcurrency{0};
    std::uint32_t kgConcurrency{0};
    std::uint32_t embedConcurrency{0};

    // Queue dimensions
    std::uint64_t postIngestQueued{0};
    std::uint64_t embedQueued{0};
    std::uint64_t kgQueued{0};
    std::uint64_t muxQueuedBytes{0};

    // Connection pool dimensions
    std::uint32_t dbConnections{0};
    std::uint32_t activeIpcHandlers{0};

    // Computed state
    ResourcePressureLevel level{ResourcePressureLevel::Normal};
    std::uint32_t consecutiveWarningTicks{0};
    std::uint32_t consecutiveCriticalTicks{0};
    std::uint32_t consecutiveEmergencyTicks{0};

    // Scaling budget remaining (governor can veto scale-up if exhausted)
    // 0.0 = no headroom (at/over budget), 1.0 = full headroom (well under budget)
    double scalingHeadroom{1.0};

    // Timestamp of this snapshot
    std::chrono::steady_clock::time_point timestamp{};
};

// ============================================================================
// Scaling Caps
// ============================================================================

/// Current scaling caps enforced by the governor based on pressure level.
/// These are dynamic limits that TuningManager should respect.
struct ScalingCaps {
    std::uint32_t ingestWorkers{0}; // 0 = use TuneAdvisor default
    std::uint32_t searchConcurrency{0};
    std::uint32_t extractionConcurrency{0};
    std::uint32_t kgConcurrency{0};
    std::uint32_t embedConcurrency{0};
    bool allowModelLoads{true};
    bool allowNewIngest{true};
};

// ============================================================================
// Resource Governor
// ============================================================================

/// Singleton resource governor that monitors cumulative resource usage
/// and coordinates scaling decisions across all daemon subsystems.
class ResourceGovernor {
public:
    /// Singleton accessor (daemon-scoped)
    static ResourceGovernor& instance() noexcept;

    // Non-copyable, non-movable
    ResourceGovernor(const ResourceGovernor&) = delete;
    ResourceGovernor& operator=(const ResourceGovernor&) = delete;
    ResourceGovernor(ResourceGovernor&&) = delete;
    ResourceGovernor& operator=(ResourceGovernor&&) = delete;

    // ========================================================================
    // Tick Interface (called by TuningManager)
    // ========================================================================

    /// Collect metrics from all dimensions and compute pressure level.
    /// Called every tick (~250ms) by TuningManager.
    /// @param sm ServiceManager for accessing subsystem metrics
    /// @return Current resource snapshot
    ResourceSnapshot tick(ServiceManager* sm);

    // ========================================================================
    // Admission Control (called BEFORE scaling decisions)
    // ========================================================================

    /// Check if scaling up a dimension is allowed under current pressure.
    /// @param dimension Name of the scaling dimension (for logging)
    /// @param delta Amount of scale-up requested
    /// @return true if scale-up is allowed
    [[nodiscard]] bool canScaleUp(std::string_view dimension, std::uint32_t delta = 1) const;

    /// Check if loading a model of given size is allowed.
    /// @param modelSizeBytes Estimated memory for the model
    /// @return true if load is allowed
    [[nodiscard]] bool canLoadModel(std::uint64_t modelSizeBytes) const;

    /// Check if new work (ingest items, requests) should be admitted.
    /// @return true if new work is allowed
    [[nodiscard]] bool canAdmitWork() const;

    // ========================================================================
    // Pressure Response Actions (called based on level transitions)
    // ========================================================================

    /// Called when entering Warning level - cap scaling, reduce concurrency
    void onWarningLevel(ServiceManager* sm);

    /// Called when entering Critical level - evict models, shrink pools
    void onCriticalLevel(ServiceManager* sm);

    /// Called when entering Emergency level - halt new work, aggressive eviction
    void onEmergencyLevel(ServiceManager* sm);

    /// Called when returning to Normal level - restore full scaling
    void onNormalLevel(ServiceManager* sm);

    // ========================================================================
    // Scaling Caps (queried by TuningManager for gating)
    // ========================================================================

    /// Get maximum allowed ingest workers under current pressure
    [[nodiscard]] std::uint32_t maxIngestWorkers() const;

    /// Get maximum allowed search concurrency under current pressure
    [[nodiscard]] std::uint32_t maxSearchConcurrency() const;

    /// Get maximum allowed embed concurrency under current pressure
    [[nodiscard]] std::uint32_t maxEmbedConcurrency() const;

    /// Get maximum allowed extraction concurrency under current pressure
    [[nodiscard]] std::uint32_t maxExtractionConcurrency() const;

    /// Get maximum allowed KG concurrency under current pressure
    [[nodiscard]] std::uint32_t maxKgConcurrency() const;

    // ========================================================================
    // Observability
    // ========================================================================

    /// Get the most recent snapshot (thread-safe copy)
    [[nodiscard]] ResourceSnapshot getSnapshot() const;

    /// Get current pressure level (fast, atomic read)
    [[nodiscard]] ResourcePressureLevel getPressureLevel() const noexcept;

    /// Get current scaling caps (thread-safe copy)
    [[nodiscard]] ScalingCaps getScalingCaps() const;

    // ========================================================================
    // Test Hooks (YAMS_TESTING only)
    // ========================================================================

#ifdef YAMS_TESTING
    /// Test hook: expose updateScalingCaps for deterministic unit testing
    void testing_updateScalingCaps(ResourcePressureLevel level) {
        updateScalingCaps(level);
    }
#endif

private:
    ResourceGovernor();
    ~ResourceGovernor() = default;

    /// Collect metrics from ServiceManager into snapshot
    void collectMetrics(ServiceManager* sm, ResourceSnapshot& snap);

    /// Compute pressure level from snapshot with hysteresis
    ResourcePressureLevel computeLevel(const ResourceSnapshot& snap);

    /// Update CPU-based admission control with hysteresis
    void updateCpuAdmissionControl(const ResourceSnapshot& snap);

    /// Update scaling caps based on pressure level
    void updateScalingCaps(ResourcePressureLevel level);

    /// Check if eviction cooldown has elapsed
    bool canEvict() const;

    /// Record eviction timestamp for cooldown
    void recordEviction();

    // Thread safety
    mutable std::shared_mutex mutex_;

    // State
    ResourceSnapshot lastSnapshot_{};
    ScalingCaps scalingCaps_{};
    std::atomic<ResourcePressureLevel> currentLevel_{ResourcePressureLevel::Normal};

    // Hysteresis tracking (time-based, decoupled from tick interval)
    std::chrono::steady_clock::time_point proposedLevelSince_{};
    ResourcePressureLevel proposedLevel_{ResourcePressureLevel::Normal};

    // Eviction cooldown
    std::chrono::steady_clock::time_point lastEvictionTime_{};

    // CPU utilization sampling state: deltas for calculating process CPU usage
    mutable std::uint64_t lastProcJiffies_{0};
    mutable std::uint64_t lastTotalJiffies_{0};

    // CPU admission control state (time-based hysteresis)
    std::atomic<bool> cpuAdmissionBlocked_{false};
    std::chrono::steady_clock::time_point cpuHighSince_{};
    std::chrono::steady_clock::time_point cpuLowSince_{};

    // Startup grace period: prevent false Emergency during early init
    std::chrono::steady_clock::time_point startupTime_{std::chrono::steady_clock::now()};
    static constexpr auto kStartupGracePeriod = std::chrono::seconds(10);
    [[nodiscard]] bool startupGraceActive() const noexcept {
        return (std::chrono::steady_clock::now() - startupTime_) < kStartupGracePeriod;
    }
};

} // namespace yams::daemon
