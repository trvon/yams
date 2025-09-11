#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <string>

namespace yams::daemon {

class ServiceManager;
class DaemonLifecycleFsm;
struct StateComponent;

struct EmbeddingServiceInfo {
    bool available{false};
    int modelsLoaded{0};
    bool onnxRuntimeEnabled{false};
};

struct MetricsSnapshot {
    bool running{true};
    bool ready{false};
    std::string version;
    std::string overallStatus;
    std::string lifecycleState;
    std::string lastError;

    std::size_t uptimeSeconds{0};
    std::size_t requestsProcessed{0};
    std::size_t activeConnections{0};
    double memoryUsageMb{0.0};
    double cpuUsagePercent{0.0};

    // Optional memory breakdown (bytes) for deeper diagnostics
    // Keys: rss_bytes, pss_bytes (if available), provider_bytes, vector_index_bytes
    std::map<std::string, std::uint64_t> memoryBreakdownBytes;

    // FSM/MUX
    uint64_t fsmTransitions{0};
    uint64_t fsmHeaderReads{0};
    uint64_t fsmPayloadReads{0};
    uint64_t fsmPayloadWrites{0};
    uint64_t fsmBytesSent{0};
    uint64_t fsmBytesReceived{0};
    uint64_t muxActiveHandlers{0};
    int64_t muxQueuedBytes{0};
    uint64_t muxWriterBudgetBytes{0};
    uint32_t retryAfterMs{0};

    // Worker pool
    std::size_t workerThreads{0};
    std::size_t workerActive{0};
    std::size_t workerQueued{0};

    // Readiness and init progress
    std::map<std::string, bool> readinessStates; // subsystem -> ready
    std::map<std::string, uint8_t> initProgress; // subsystem -> 0-100

    // Vector DB snapshot (best-effort)
    std::size_t vectorDbSizeBytes{0};
    std::size_t vectorRowsExact{0};

    // Service states (centralized)
    std::string serviceContentStore;   // "running"|"unavailable"
    std::string serviceMetadataRepo;   // "running"|"unavailable"
    std::string serviceSearchExecutor; // "available"|"unavailable"
    std::string searchExecutorReason;  // optional reason when unavailable

    // Content store & compression snapshot (best-effort)
    std::uint64_t storeObjects{0};
    std::uint64_t uniqueBlocks{0};
    std::uint64_t deduplicatedBytes{0};
    double compressionRatio{0.0};

    // Resolved data directory
    std::string dataDir;
};

class DaemonMetrics {
public:
    DaemonMetrics(const DaemonLifecycleFsm* lifecycle, const StateComponent* state,
                  const ServiceManager* services);

    MetricsSnapshot getSnapshot() const;
    // Optional: force refresh cache now (used by periodic ticker)
    void refresh();

    // Centralized embedding/ONNX provider status for stats/status callers
    EmbeddingServiceInfo getEmbeddingServiceInfo() const;

private:
    const DaemonLifecycleFsm* lifecycle_;
    const StateComponent* state_;
    const ServiceManager* services_;
    // Lightweight memoization to avoid repeated /proc reads; 0 disables caching
    uint32_t cacheMs_{200};
    mutable std::mutex cacheMutex_;
    mutable std::chrono::steady_clock::time_point lastUpdate_{};
    mutable MetricsSnapshot cached_{};

    // CPU utilization sampling state (Linux): deltas over /proc since last snapshot
    mutable std::uint64_t lastProcJiffies_{0};
    mutable std::uint64_t lastTotalJiffies_{0};
};

} // namespace yams::daemon
