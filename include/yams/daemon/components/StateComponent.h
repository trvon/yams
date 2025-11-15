#pragma once

#include <atomic>
#include <chrono>
#include <map>
#include <string>

namespace yams::daemon {

/**
 * @struct DaemonReadiness
 * @brief A passive component holding the readiness state of various daemon subsystems.
 */
struct DaemonReadiness {
    std::atomic<bool> ipcServerReady{false};
    std::atomic<bool> contentStoreReady{false};
    std::atomic<bool> databaseReady{false};
    std::atomic<bool> metadataRepoReady{false};
    std::atomic<bool> searchEngineReady{false};
    std::atomic<bool> modelProviderReady{false};
    std::atomic<bool> vectorIndexReady{false};
    std::atomic<bool> pluginsReady{false};
    // Vector database guard/health fields
    std::atomic<bool> vectorDbInitAttempted{false};
    std::atomic<bool> vectorDbReady{false};
    std::atomic<uint32_t> vectorDbDim{0};

    // Progress tracking for long-running initializations (0-100)
    std::atomic<int> searchProgress{0};
    std::atomic<int> vectorIndexProgress{0};
    std::atomic<int> modelLoadProgress{0};

    // DEPRECATED: not an authoritative lifecycle signal. Use DaemonLifecycleFsm state instead.
    bool fullyReady() const {
        return ipcServerReady && contentStoreReady && databaseReady && metadataRepoReady &&
               searchEngineReady && modelProviderReady && vectorIndexReady && pluginsReady;
    }

    // DEPRECATED: for legacy display only; not a lifecycle source of truth.
    std::string overallStatus() const {
        if (fullyReady())
            return "Ready";
        if (ipcServerReady)
            return "Initializing";
        return "Starting";
    }
};

/**
 * @struct DaemonStats
 * @brief A passive component holding runtime statistics for the daemon.
 */
struct DaemonStats {
    std::chrono::steady_clock::time_point startTime;
    std::atomic<uint64_t> requestsProcessed{0};
    std::atomic<uint64_t> activeConnections{0};
    std::atomic<uint64_t> maxConnections{0};
    std::atomic<uint64_t> connectionSlotsFree{0};
    std::atomic<uint64_t> oldestConnectionAge{0};
    std::atomic<uint64_t> forcedCloseCount{0};
    std::atomic<uint64_t> ipcTasksPending{0};
    std::atomic<uint64_t> ipcTasksActive{0};
    // macOS AF_UNIX acceptor recovery counter
    std::atomic<uint64_t> ipcEinvalRebuilds{0};
    // Accept loop backpressure metrics
    std::atomic<uint64_t> acceptBackpressureDelays{0};
    std::atomic<uint64_t> acceptCapacityDelays{0};

    // Background repair metrics (idle-only coordinator)
    std::atomic<uint64_t> repairIdleTicks{0};
    std::atomic<uint64_t> repairBusyTicks{0};
    std::atomic<uint64_t> repairBatchesAttempted{0};
    std::atomic<uint64_t> repairEmbeddingsGenerated{0};
    std::atomic<uint64_t> repairEmbeddingsSkipped{0};
    std::atomic<uint64_t> repairFailedOperations{0};
    std::atomic<uint64_t> repairQueueDepth{0};
};

/**
 * @struct StateComponent
 * @brief A container for all passive state components of the daemon.
 */
struct StateComponent {
    DaemonReadiness readiness;
    DaemonStats stats;
    // Initialization durations (ms since daemon start) per component once ready
    // Example keys: "ipc_server", "content_store", "database", "metadata_repo",
    //               "search_engine", "model_provider", "vector_index", "plugins"
    std::map<std::string, uint64_t> initDurationsMs;
};

} // namespace yams::daemon
