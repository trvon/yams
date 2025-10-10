#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <thread>

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
    // Tuning pool sizes (via FSM metrics)
    uint32_t ipcPoolSize{0};
    uint32_t ioPoolSize{0};
    std::uint32_t searchActive{0};
    std::uint32_t searchQueued{0};
    std::uint64_t searchExecuted{0};
    double searchCacheHitRate{0.0};
    std::uint64_t searchAvgLatencyUs{0};
    std::uint32_t searchConcurrencyLimit{0};

    // Worker pool
    std::size_t workerThreads{0};
    std::size_t workerActive{0};
    std::size_t workerQueued{0};

    // Post-ingest queue metrics
    std::size_t postIngestThreads{0};
    std::size_t postIngestQueued{0};
    std::size_t postIngestInflight{0};
    std::size_t postIngestCapacity{0};
    // Optional per-queue sizes (for diagnostics only)
    std::size_t postIngestQMeta{0};
    std::size_t postIngestQKg{0};
    std::size_t postIngestQEmb{0};
    std::size_t postIngestProcessed{0};
    std::size_t postIngestFailed{0};
    double postIngestLatencyMsEma{0.0};
    double postIngestRateSecEma{0.0};

    // Readiness and init progress
    std::map<std::string, bool> readinessStates; // subsystem -> ready
    std::map<std::string, uint8_t> initProgress; // subsystem -> 0-100

    // Vector DB snapshot (best-effort)
    std::size_t vectorDbSizeBytes{0};
    std::size_t vectorRowsExact{0};
    bool vectorDbInitAttempted{false};
    bool vectorDbReady{false};
    uint32_t vectorDbDim{0};

    // Service states (centralized)
    std::string serviceContentStore;   // "running"|"unavailable"
    std::string serviceMetadataRepo;   // "running"|"unavailable"
    std::string serviceSearchExecutor; // "available"|"unavailable"
    std::string searchExecutorReason;  // optional reason when unavailable

    // Document counts (cached from metadata repo, avoid live DB queries on hot path)
    std::uint64_t documentsTotal{0};
    std::uint64_t documentsIndexed{0};
    std::uint64_t documentsContentExtracted{0};

    // Content store & compression snapshot (best-effort)
    std::uint64_t storeObjects{0};
    std::uint64_t uniqueBlocks{0};
    std::uint64_t deduplicatedBytes{0};
    double compressionRatio{0.0};
    // Storage size summary (best-effort)
    std::uint64_t logicalBytes{0};  // total logical bytes from content store
    std::uint64_t physicalBytes{0}; // on-disk (scanned) bytes for storage dir (TTL-cached)

    // Storage breakdown (best-effort; populated when detailed=true)
    std::uint64_t casPhysicalBytes{0};      // storage/objects (filesystem blocks)
    std::uint64_t casUniqueRawBytes{0};     // sum of unique raw bytes entering CAS
    std::uint64_t casDedupSavedBytes{0};    // bytes avoided via dedup (duplicate chunks)
    std::uint64_t casCompressSavedBytes{0}; // bytes saved via compression (global monitor)
    std::uint64_t metadataPhysicalBytes{0}; // yams.db + WAL/SHM + refs.db
    std::uint64_t indexPhysicalBytes{0};    // text/search index files (if externalized)
    std::uint64_t vectorPhysicalBytes{0};   // vector DB + index files
    std::uint64_t logsTmpPhysicalBytes{0};  // logs + temp files under data dir
    std::uint64_t physicalTotalBytes{0};    // sum of above components

    // Resolved data directory
    std::string dataDir;

    // Content store diagnostics
    std::string contentStoreRoot;  // absolute path to storage root
    std::string contentStoreError; // last initialization error (if any)

    // Embedding/model provider snapshot (best-effort)
    bool embeddingAvailable{false};
    std::string embeddingBackend;   // provider|local|unknown
    std::string embeddingModel;     // preferred/active model name if known
    std::string embeddingModelPath; // resolved model path when known
    uint32_t embeddingDim{0};
};

class DaemonMetrics {
public:
    DaemonMetrics(const DaemonLifecycleFsm* lifecycle, const StateComponent* state,
                  const ServiceManager* services);
    ~DaemonMetrics();

    // Start background polling thread to keep metrics cache hot
    void startPolling();
    // Stop background polling thread
    void stopPolling();

    // Retrieve metrics snapshot. When detailed is true, include deep store stats
    // (may perform additional I/O) without poisoning the basic cache.
    // Returns a shared_ptr for zero-copy, thread-safe access.
    std::shared_ptr<const MetricsSnapshot> getSnapshot(bool detailed) const;
    // Backward-compatible basic snapshot (no deep store stats)
    std::shared_ptr<const MetricsSnapshot> getSnapshot() const { return getSnapshot(false); }
    // Optional: force refresh cache now (used by periodic ticker)
    void refresh();

    // Centralized embedding/ONNX provider status for stats/status callers
    EmbeddingServiceInfo getEmbeddingServiceInfo() const;

private:
    void pollingLoop(); // Background thread loop

    const DaemonLifecycleFsm* lifecycle_;
    const StateComponent* state_;
    const ServiceManager* services_;
    // Lightweight memoization to avoid repeated /proc reads; 0 disables caching
    uint32_t cacheMs_{200};
    // Shared mutex for concurrent reads, exclusive writes
    mutable std::shared_mutex cacheMutex_;
    mutable std::shared_ptr<MetricsSnapshot> cachedSnapshot_{nullptr};
    mutable std::chrono::steady_clock::time_point lastUpdate_{};
    // Physical storage breakdown (updated separately with TTL, reuses cacheMutex_)
    mutable MetricsSnapshot cached_{}; // Only physical storage breakdown fields used

    // Background polling thread
    std::atomic<bool> pollingActive_{false};
    std::thread pollingThread_;

    // CPU utilization sampling state (Linux): deltas over /proc since last snapshot
    mutable std::uint64_t lastProcJiffies_{0};
    mutable std::uint64_t lastTotalJiffies_{0};

    // TTL cache for physical storage scan
    mutable std::chrono::steady_clock::time_point lastPhysicalAt_{};
    mutable std::uint64_t lastPhysicalBytes_{0};
    uint32_t physicalTtlMs_{60000}; // default 60s; may be tuned via env later

    // TTL cache for expensive document counts (avoid blocking DB queries on hot path)
    mutable std::chrono::steady_clock::time_point lastDocCountsAt_{};
    mutable std::uint64_t cachedDocumentsTotal_{0};
    mutable std::uint64_t cachedDocumentsIndexed_{0};
    mutable std::uint64_t cachedDocumentsExtracted_{0};
    mutable std::uint64_t cachedVectorRows_{0};
    uint32_t docCountsTtlMs_{5000}; // 5s TTL for document counts
};

} // namespace yams::daemon
