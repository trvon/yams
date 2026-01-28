#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>

namespace yams::daemon {

class ServiceManager;
class WorkCoordinator;
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
    std::size_t maxConnections{0};      // Connection slot limit
    std::size_t connectionSlotsFree{0}; // Available connection slots
    uint64_t oldestConnectionAge{0};    // Age of oldest active connection (seconds)
    uint64_t forcedCloseCount{0};       // Connections closed due to lifetime exceeded
    std::size_t ipcTasksPending{0};     // IPC handlers spawned but not yet started
    std::size_t ipcTasksActive{0};      // IPC handlers currently executing
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
    // File/directory add tracking
    std::uint64_t filesAdded{0};
    std::uint64_t directoriesAdded{0};
    std::uint64_t filesProcessed{0};
    std::uint64_t directoriesProcessed{0};
    double postIngestLatencyMsEma{0.0};
    double postIngestRateSecEma{0.0};
    // Pipeline stage metrics (extraction → KG → symbol → entity → embedding)
    std::size_t extractionInFlight{0};
    std::size_t kgQueued{0};
    std::size_t kgDropped{0};
    std::size_t kgConsumed{0};
    std::size_t kgInFlight{0};
    std::size_t kgQueueDepth{0}; // Current channel queue depth
    std::size_t symbolInFlight{0};
    std::size_t symbolQueueDepth{0}; // Current channel queue depth
    // Entity extraction metrics (external plugins like Ghidra)
    std::size_t entityQueued{0};
    std::size_t entityDropped{0};
    std::size_t entityConsumed{0};
    std::size_t entityInFlight{0};
    std::size_t entityQueueDepth{0}; // Current channel queue depth
    // Title extraction metrics
    std::size_t titleQueueDepth{0}; // Current channel queue depth
    // Dynamic concurrency limits (PBI-05a)
    std::size_t postExtractionLimit{4};
    std::size_t postKgLimit{8};
    std::size_t postSymbolLimit{4};
    std::size_t postEntityLimit{2};

    // Session watch status
    bool watchEnabled{false};
    uint32_t watchIntervalMs{0};
    std::string watchSession;
    std::string watchRoot;

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
    std::string serviceContentStore; // "running"|"unavailable"
    std::string serviceMetadataRepo; // "running"|"unavailable"
    std::string serviceSearchEngine; // "available"|"unavailable"
    std::string searchEngineReason;  // optional reason when unavailable

    // Document counts (cached from metadata repo, avoid live DB queries on hot path)
    std::uint64_t documentsTotal{0};
    std::uint64_t documentsIndexed{0};
    std::uint64_t documentsContentExtracted{0};

    // FTS5 orphan scan metrics (from InternalEventBus)
    std::uint64_t fts5OrphansDetected{0}; // total orphans found since daemon start
    std::uint64_t fts5OrphansRemoved{0};  // total orphans removed since daemon start
    std::string lastOrphanScanTime;       // ISO8601 timestamp of last scan (empty if never)

    // FTS5 indexing failure breakdown (from InternalEventBus)
    std::uint64_t fts5FailNoDoc{0};      // document not found in metadata
    std::uint64_t fts5FailExtraction{0}; // text extraction failed or empty
    std::uint64_t fts5FailIndex{0};      // FTS5 indexing failed (DB error)
    std::uint64_t fts5FailException{0};  // unexpected exceptions

    // Checkpoint manager stats (PBI-090)
    std::uint64_t checkpointVectorCount{0};
    std::uint64_t checkpointHotzoneCount{0};
    std::uint64_t checkpointErrorCount{0};
    std::string lastVectorCheckpointTime;  // ISO8601 timestamp
    std::string lastHotzoneCheckpointTime; // ISO8601 timestamp

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

    // Vector diagnostics (collected in background to avoid blocking status requests)
    bool vectorEmbeddingsAvailable{false};
    bool vectorScoringEnabled{false};
    std::string searchEngineBuildReason; // "initial"|"rebuild"|"degraded"|"unknown"

    // Search tuning state (from SearchTuner FSM - epic yams-7ez4)
    std::string searchTuningState;  // e.g., "SMALL_CODE", "SCIENTIFIC", "MIXED"
    std::string searchTuningReason; // Human-readable explanation of state selection
    std::map<std::string, double> searchTuningParams; // e.g., {"textWeight": 0.55, ...}

    // ResourceGovernor metrics (memory pressure management)
    uint64_t governorRssBytes{0};     // Current process RSS
    uint64_t governorBudgetBytes{0};  // Memory budget limit
    uint8_t governorPressureLevel{0}; // 0=Normal, 1=Warning, 2=Critical, 3=Emergency
    uint8_t governorHeadroomPct{100}; // Scaling headroom (0-100%)

    // ONNX concurrency metrics
    uint32_t onnxTotalSlots{0};
    uint32_t onnxUsedSlots{0};
    uint32_t onnxGlinerUsed{0};
    uint32_t onnxEmbedUsed{0};
    uint32_t onnxRerankerUsed{0};

    // DatabaseManager metrics
    uint64_t dbOpenDurationMs{0};
    uint64_t dbMigrationDurationMs{0};
    uint64_t dbOpenErrors{0};
    uint64_t dbMigrationErrors{0};
    uint64_t dbRepositoryInitErrors{0};

    // WorkCoordinator metrics
    std::size_t workCoordinatorActiveWorkers{0};
    bool workCoordinatorRunning{false};

    // Stream metrics (from StreamMetricsRegistry)
    uint64_t streamTotal{0};
    uint64_t streamBatches{0};
    uint64_t streamKeepalives{0};
    uint64_t streamTtfbAvgMs{0};

    // Title extraction metrics (from InternalEventBus)
    uint64_t titleQueued{0};
    uint64_t titleDropped{0};
    uint64_t titleConsumed{0};

    // FTS5 indexing metrics (full picture from InternalEventBus)
    uint64_t fts5Queued{0};
    uint64_t fts5Dropped{0};
    uint64_t fts5Consumed{0};

    // Symbol extraction metrics (from InternalEventBus)
    uint64_t symbolQueued{0};
    uint64_t symbolDropped{0};
    uint64_t symbolConsumed{0};
};

class SocketServer; // Forward declaration

class DaemonMetrics {
public:
    DaemonMetrics(const DaemonLifecycleFsm* lifecycle, const StateComponent* state,
                  const ServiceManager* services, WorkCoordinator* coordinator,
                  const SocketServer* socketServer = nullptr);
    ~DaemonMetrics();

    // Start background polling loop to keep metrics cache hot
    void startPolling();
    // Stop background polling loop
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

    // Set SocketServer (called after SocketServer is created, which happens after DaemonMetrics)
    void setSocketServer(const SocketServer* socketServer) { socketServer_ = socketServer; }

private:
    boost::asio::awaitable<void> pollingLoop(); // Background polling loop

    const DaemonLifecycleFsm* lifecycle_;
    const StateComponent* state_;
    const ServiceManager* services_;
    [[maybe_unused]] WorkCoordinator* coordinator_; // Retained for future metrics expansion
    const SocketServer* socketServer_;
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;

    // Lightweight memoization to avoid repeated /proc reads; 0 disables caching
    uint32_t cacheMs_{250};
    // Shared mutex for concurrent reads, exclusive writes
    mutable std::shared_mutex cacheMutex_;
    mutable std::shared_ptr<MetricsSnapshot> cachedSnapshot_{nullptr};
    mutable std::chrono::steady_clock::time_point lastUpdate_{};
    // Physical storage breakdown (updated separately with TTL, reuses cacheMutex_)
    mutable MetricsSnapshot cached_{}; // Only physical storage breakdown fields used

    // Background polling control
    std::atomic<bool> pollingActive_{false};
    // Timer pointer for cancellation from stopPolling()
    boost::asio::steady_timer* pollingTimer_{nullptr};
    // Shutdown synchronization - signaled when polling loop exits
    std::mutex pollingMutex_;
    std::condition_variable pollingCv_;
    bool pollingStopped_{true};

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

    // TTL cache for SearchTuner state (avoid re-instantiation on every poll - yams-fbtq)
    mutable std::shared_mutex tunerCacheMutex_; // protects tuner cache state
    mutable std::chrono::steady_clock::time_point lastTunerStateAt_{};
    mutable std::string cachedTuningState_;
    mutable std::string cachedTuningReason_;
    mutable std::map<std::string, double> cachedTuningParams_;
    mutable std::uint64_t cachedTunerDocCount_{0}; // docCount when tuner was cached
    uint32_t tunerStateTtlMs_{30000}; // 30s TTL for tuner state (matches CorpusStats cache)
};

} // namespace yams::daemon
