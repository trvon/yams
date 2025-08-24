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

    // Progress tracking for long-running initializations (0-100)
    std::atomic<int> searchProgress{0};
    std::atomic<int> vectorIndexProgress{0};
    std::atomic<int> modelLoadProgress{0};

    bool fullyReady() const {
        return ipcServerReady && contentStoreReady && databaseReady && metadataRepoReady &&
               searchEngineReady && modelProviderReady && vectorIndexReady && pluginsReady;
    }

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
};

/**
 * @struct StateComponent
 * @brief A container for all passive state components of the daemon.
 */
struct StateComponent {
    DaemonReadiness readiness;
    DaemonStats stats;
};

} // namespace yams::daemon
