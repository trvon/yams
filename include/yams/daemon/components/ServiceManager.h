#pragma once

#include <atomic>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include "IComponent.h"
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <yams/app/services/services.hpp>
#include <yams/compat/thread_stop_compat.h>
#include <yams/core/types.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/DatabaseManager.h>
#include <yams/daemon/components/EmbeddingProviderFsm.h>
#include <yams/daemon/components/EmbeddingService.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/KGWriteQueue.h>
#include <yams/daemon/components/PluginHostFsm.h>
#include <yams/daemon/components/PluginManager.h>
#include <yams/daemon/components/PoolManager.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/SearchComponent.h>
#include <yams/daemon/components/SearchEngineFsm.h>
#include <yams/daemon/components/SearchEngineManager.h>
#include <yams/daemon/components/ServiceManagerFsm.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningConfig.h>
#include <yams/daemon/components/VectorSystemManager.h>
#include <yams/daemon/components/WalMetricsProvider.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/retrieval_session.h>
#include <yams/daemon/resource/abi_entity_extractor_adapter.h>
#include <yams/daemon/resource/abi_plugin_loader.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>
#include <yams/daemon/resource/plugin_host.h>
#include <yams/extraction/content_extractor.h>
#include <yams/profiling.h>
#include <yams/wal/wal_manager.h>

// Forward declarations for services
namespace yams::api {
class IContentStore;
}
namespace yams::metadata {
class Database;
class ConnectionPool;
class MetadataRepository;
} // namespace yams::metadata
namespace yams::integrity {
class RepairManager;
} // namespace yams::integrity
namespace yams::search {
class SearchEngine;
class SearchEngineBuilder;
class IReranker;
} // namespace yams::search
namespace yams::vector {
class EmbeddingGenerator;
class VectorDatabase;
} // namespace yams::vector
namespace yams::daemon {

class AbiPluginLoader;
class ExternalPluginHost;
class IModelProvider;
class RetrievalSessionManager;
class WorkerPool;
class TuningManager;
class CheckpointManager;
class OnnxRerankerSession;
} // namespace yams::daemon

namespace yams::daemon {

class IngestService;
class GraphComponent;

class ServiceManager : public IComponent, public std::enable_shared_from_this<ServiceManager> {
public:
    ServiceManager(const DaemonConfig& config, StateComponent& state,
                   DaemonLifecycleFsm& lifecycleFsm);
    ~ServiceManager() override;

    // IComponent interface
    const char* getName() const override { return "ServiceManager"; }

    /// Compute ETA remaining seconds from expected duration and progress percentage.
    /// Uses round-up division to avoid integer truncation on small values.
    /// Extracted as a static helper for testability.
    static int computeEtaRemaining(int expectedSeconds, int progressPercent) {
        if (expectedSeconds <= 0)
            return 0;
        int completed = (expectedSeconds * progressPercent + 99) / 100;
        return std::max(0, expectedSeconds - completed);
    }

    /// Synchronous initialization - validates config, creates directories, prepares resources.
    /// Does NOT start async initialization - call startAsyncInit() after main loop is running.
    Result<void> initialize() override;

    void startAsyncInit(std::promise<void>* barrierPromise = nullptr,
                        std::atomic<bool>* barrierSet = nullptr);

    void shutdown() override;

    /// Reset FSM state for daemon restart (call before next start cycle)
    void prepareForRestart() {
        serviceFsm_.reset();
        asyncInitStopSource_ = yams::compat::stop_source{};
    }

    // Start background task coroutines (must be called after shared_ptr construction)
    void startBackgroundTasks();

    // Service Accessors
    // Use atomic_load for thread-safe read of shared_ptr (written by async init coroutine)
    std::shared_ptr<api::IContentStore> getContentStore() const {
        return std::atomic_load_explicit(&contentStore_, std::memory_order_acquire);
    }
    std::shared_ptr<metadata::MetadataRepository> getMetadataRepo() const {
        // PBI-088: Delegate to DatabaseManager if available
        if (databaseManager_) {
            auto repo = databaseManager_->getMetadataRepo();
            if (repo)
                return repo;
        }
        return metadataRepo_; // Fallback to old member
    }
    std::shared_ptr<IModelProvider> getModelProvider() const { return modelProvider_; }
    std::shared_ptr<yams::search::SearchEngine> getSearchEngineSnapshot() const;
    std::string getEmbeddingModelName() const { return embeddingModelName_; }
    std::shared_ptr<vector::VectorDatabase> getVectorDatabase() const {
        // PBI-088: Delegate to VectorSystemManager if available
        if (vectorSystemManager_) {
            auto db = vectorSystemManager_->getVectorDatabase();
            if (db)
                return db;
        }
        return vectorDatabase_; // Fallback to old member
    }
    std::shared_ptr<WorkerPool> getWorkerPool() const { return nullptr; }
    WorkCoordinator* getWorkCoordinator() const { return workCoordinator_.get(); }
    boost::asio::any_io_executor getCliExecutor() const;
    // Resize the worker pool to a target size; creates pool on demand.
    bool resizeWorkerPool(std::size_t target);
    PostIngestQueue* getPostIngestQueue() const { return postIngest_.get(); }
    struct SearchLoadMetrics {
        std::uint32_t active{0};
        std::uint32_t queued{0};
        std::uint64_t executed{0};
        double cacheHitRate{0.0};
        std::uint64_t avgLatencyUs{0};
        std::uint32_t concurrencyLimit{0};
    };
    SearchLoadMetrics getSearchLoadMetrics() const;
    struct IngestMetricsSnapshot {
        std::size_t queued;
        std::size_t active;
        std::size_t target;
    };

    // Plugin status snapshot structures (PBI-046: non-blocking status)
    struct PluginStatusRecord {
        std::string name;
        bool isProvider{false};
        bool ready{false};
        bool degraded{false};
        std::string error;
        std::uint32_t modelsLoaded{0};
        std::vector<std::string> interfaces;   // Plugin interfaces (e.g., content_extractor_v1)
        std::vector<std::string> capabilities; // Capability categories (e.g., content_extraction)
        std::string healthJson;                // Full health response as JSON string
    };

    struct PluginStatusSnapshot {
        PluginHostSnapshot host;
        std::vector<PluginStatusRecord> records;
    };

    void publishIngestMetrics(std::size_t queued, std::size_t active) {
        ingestQueued_.store(queued, std::memory_order_relaxed);
        ingestActive_.store(active, std::memory_order_relaxed);
    }
    void publishIngestQueued(std::size_t queued) {
        ingestQueued_.store(queued, std::memory_order_relaxed);
    }
    void publishIngestActive(std::size_t active) {
        ingestActive_.store(active, std::memory_order_relaxed);
    }
    void setIngestWorkerTarget(std::size_t target) {
        if (target < 1)
            target = 1;
        ingestWorkerTarget_.store(target, std::memory_order_relaxed);
    }
    std::size_t ingestWorkerTarget() const {
        auto v = ingestWorkerTarget_.load(std::memory_order_relaxed);
        return v == 0 ? 1 : v;
    }
    IngestMetricsSnapshot getIngestMetricsSnapshot() const {
        return {ingestQueued_.load(std::memory_order_relaxed),
                ingestActive_.load(std::memory_order_relaxed),
                ingestWorkerTarget_.load(std::memory_order_relaxed)};
    }
    void enqueuePostIngest(const std::string& hash, const std::string& mime);
    // Phase 2.4: Delegate to SearchEngineManager
    SearchEngineSnapshot getSearchEngineFsmSnapshot() const {
        return searchEngineManager_.getSnapshot();
    }
    yams::search::SearchEngine* getCachedSearchEngine() const {
        return searchEngineManager_.getCachedEngine();
    }

    // Plugin status snapshot API (PBI-046: non-blocking status)
    PluginStatusSnapshot getPluginStatusSnapshot() const;
    void setCachedModelProviderModelCount(std::uint32_t count) {
        cachedModelProviderModelCount_.store(count, std::memory_order_relaxed);
    }
    void refreshPluginStatusSnapshot();
    boost::asio::any_io_executor getWorkerExecutor() const;
    std::function<void(bool)> getWorkerJobSignal();
    // Best-effort queue depth estimation for backpressure/telemetry
    std::size_t getWorkerQueueDepth() const;

    // Tuning configuration (no envs): getter/setter with live application where applicable.
    const TuningConfig& getTuningConfig() const { return tuningConfig_; }
    void setTuningConfig(const TuningConfig& cfg) {
        tuningConfig_ = cfg;
        if (postIngest_ && cfg.postIngestCapacity > 0)
            postIngest_->setCapacity(cfg.postIngestCapacity);
    }
    const std::vector<std::shared_ptr<yams::extraction::IContentExtractor>>&
    getContentExtractors() const {
        return contentExtractors_;
    }
    // Symbol extractors (ABI adapters) - delegate to PluginManager
    const std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>>& getSymbolExtractors() const {
        if (pluginManager_) {
            return pluginManager_->getSymbolExtractors();
        }
        return symbolExtractors_; // Empty fallback
    }

    // NL Entity extractors (ABI adapters, e.g., Glint) - delegate to PluginManager
    const std::vector<std::shared_ptr<AbiEntityExtractorAdapter>>& getEntityExtractors() const {
        if (pluginManager_) {
            return pluginManager_->getEntityExtractors();
        }
        static const std::vector<std::shared_ptr<AbiEntityExtractorAdapter>> kEmpty;
        return kEmpty;
    }

    // Knowledge Graph Store (PBI-059)
    std::shared_ptr<metadata::KnowledgeGraphStore> getKgStore() const {
        // PBI-088: Delegate to DatabaseManager if available
        if (databaseManager_) {
            auto store = databaseManager_->getKgStore();
            if (store)
                return store;
        }
        return kgStore_; // Fallback to old member
    }

    // KG Write Queue - serializes KG writes to eliminate lock contention
    KGWriteQueue* getKgWriteQueue() const { return kgWriteQueue_.get(); }

    // Graph Component (PBI-009)
    std::shared_ptr<GraphComponent> getGraphComponent() const { return graphComponent_; }

    // ContentStore diagnostics
    std::string getContentStoreError() const { return contentStoreError_; }

    // WAL metrics provider (may return zeros until a WALManager is attached)
    std::shared_ptr<WalMetricsProvider> getWalMetricsProvider() const {
        return walMetricsProvider_;
    }
    std::shared_ptr<yams::integrity::RepairManager> getRepairManager() const {
        return repairManager_;
    }
    void attachWalManager(std::shared_ptr<yams::wal::WALManager> wal) {
        if (!walMetricsProvider_)
            walMetricsProvider_ = std::make_shared<WalMetricsProvider>();
        walMetricsProvider_->setManager(std::move(wal));
    }

    // Session watchers: polling mtime/size for pinned directories
    struct SessionWatchState {
        std::unordered_map<std::string,
                           std::unordered_map<std::string, std::pair<std::uint64_t, std::uint64_t>>>
            dirFiles; // dir -> (file -> (mtime,size))
        std::unordered_map<std::string, std::vector<std::string>> gitignorePatterns;
        std::unordered_map<std::string, std::uint64_t> gitignoreMtime;
    };
    SessionWatchState sessionWatch_;
    yams::compat::stop_source sessionWatchStopSource_;
    std::future<void> sessionWatcherFuture_;

    // Stop source for cancelling async initialization coroutine during shutdown
    yams::compat::stop_source asyncInitStopSource_;

    // ABI plugin loader access
    AbiPluginLoader* getAbiPluginLoader() const { return abiPluginLoader_.get(); }
    // Plugin host (C‑ABI)
    AbiPluginHost* getAbiPluginHost() const { return abiHost_.get(); }
    // Plugin host (external Python/JS plugins) - delegates to PluginManager
    ExternalPluginHost* getExternalPluginHost() const {
        return pluginManager_ ? pluginManager_->getExternalPluginHost() : nullptr;
    }

    // PBI-088: New component accessors
    PluginManager* getPluginManager() const { return pluginManager_.get(); }
    VectorSystemManager* getVectorSystemManager() const { return vectorSystemManager_.get(); }
    DatabaseManager* getDatabaseManager() const { return databaseManager_.get(); }
    SearchComponent* getSearchComponent() const { return searchComponent_.get(); }

    // Worker pool metrics accessors
    std::size_t getWorkerActive() const { return poolActive_.load(std::memory_order_relaxed); }
    std::size_t getWorkerPosted() const { return poolPosted_.load(std::memory_order_relaxed); }
    std::size_t getWorkerCompleted() const {
        return poolCompleted_.load(std::memory_order_relaxed);
    }
    std::size_t getWorkerThreads() const {
        return workCoordinator_ ? workCoordinator_->getWorkerCount() : 0;
    }

    // Embedding service metrics accessors
    std::size_t getEmbeddingInFlightJobs() const;
    std::size_t getEmbeddingQueuedJobs() const;

    RetrievalSessionManager* getRetrievalSessionManager() const { return retrievalSessions_.get(); }

    CheckpointManager* getCheckpointManager() const { return checkpointManager_.get(); }

    // Get AppContext for app services
    app::services::AppContext getAppContext() const;

    // FSM snapshots (read-only) for status/diagnostics
    ServiceManagerSnapshot getServiceManagerFsmSnapshot() const { return serviceFsm_.snapshot(); }

    ServiceManagerSnapshot waitForServiceManagerTerminalState(int timeoutSeconds = 60) {
        return serviceFsm_.waitForTerminalState(timeoutSeconds);
    }

    void cancelServiceManagerWait() { serviceFsm_.cancelWait(); }
    // PBI-088: Delegate to PluginManager FSM when available (it owns the provider lifecycle)
    ProviderSnapshot getEmbeddingProviderFsmSnapshot() const {
        if (pluginManager_) {
            return pluginManager_->getEmbeddingProviderFsmSnapshot();
        }
        return embeddingFsm_.snapshot();
    }
    // PBI-088: Delegate to PluginManager FSM (it owns the plugin host lifecycle)
    PluginHostSnapshot getPluginHostFsmSnapshot() const {
        if (pluginManager_) {
            return pluginManager_->getPluginHostFsmSnapshot();
        }
        // Fallback: return NotInitialized state if PluginManager not available
        return PluginHostSnapshot{};
    }

    // Expose resolved daemon configuration for components that need paths
    const DaemonConfig& getConfig() const { return config_; }
    // Resolved data directory used for storage (may derive from env/config)
    const std::filesystem::path& getResolvedDataDir() const { return resolvedDataDir_; }

    // Persist trusted plugin path updates back to config.toml when available.
    void persistTrustedPluginPath(const std::filesystem::path& path, bool remove) const;

    // Try to adopt a model provider from loaded plugin hosts at runtime.
    // If preferredName is non-empty, attempts that plugin first.
    // Returns true on success.
    Result<bool> adoptModelProviderFromHosts(const std::string& preferredName = "");
    Result<size_t> adoptContentExtractorsFromHosts();
    Result<size_t> adoptSymbolExtractorsFromHosts();
    Result<size_t> adoptEntityExtractorsFromHosts();
    bool isEmbeddingsAutoOnAdd() const { return embeddingsAutoOnAdd_; }

    // Explicit, on-demand plugin autoload: scans trusted roots and default directories,
    // loads plugins via ABI hosts, and attempts to adopt model providers and content extractors.
    // Returns number of plugins loaded during this invocation.
    boost::asio::awaitable<Result<size_t>> autoloadPluginsNow();
    // Attempt to preload preferred model if a model provider is available.
    // Preferred model is resolved from env (YAMS_PREFERRED_MODEL) or by scanning ~/.yams/models.
    boost::asio::awaitable<void> preloadPreferredModelIfConfigured();

    // Helper method to resolve the preferred model from env, config, or auto-detection
    std::string resolvePreferredModel() const;

    // Helper method to align vector component dimensions after embedding generator is initialized
    void alignVectorComponentDimensions();

    // Model provider degraded state accessors (FSM-first)
    bool isModelProviderDegraded() const {
        try {
            auto snap = embeddingFsm_.snapshot();
            return snap.state == EmbeddingProviderState::Degraded ||
                   snap.state == EmbeddingProviderState::Failed;
        } catch (...) {
            return false;
        }
    }
    // Deprecated: Use lifecycleFsm_.degradationReason("embeddings") instead
    std::string lastModelError() const;
    const std::string& adoptedProviderPluginName() const { return adoptedProviderPluginName_; }
    // Clear embedding subsystem degradation
    void clearModelProviderError();

    // Combined embedding initialization and search engine rebuild coroutine.
    // This method provides idempotent, race-safe initialization of embedding capabilities
    // followed by search engine rebuild to enable vector search. Uses atomic guards to
    // prevent duplicate execution and ensures proper lifetime management.
    // Should be called via co_spawn with shared_from_this() for safety.
    boost::asio::awaitable<void> co_enableEmbeddingsAndRebuild();

    // Check if search engine needs rebuild due to corpus growth and trigger if needed.
    // Safe to call frequently (e.g., from status handler) - uses atomic guards.
    // Returns true if a rebuild was triggered (async, not awaited).
    bool triggerSearchEngineRebuildIfNeeded();

    /// Request a search engine rebuild via the FSM. If waitForDrain is true (default),
    /// the rebuild will be deferred until PostIngestQueue drains.
    /// Returns true if the request was accepted (not already building/awaiting).
    bool requestSearchEngineRebuild(const std::string& reason, bool includeVector = true,
                                    bool waitForDrain = true) {
        return searchEngineManager_.requestRebuild(reason, includeVector, waitForDrain);
    }

    /// Check if search engine is awaiting indexing drain before rebuild.
    bool isSearchEngineAwaitingDrain() const { return searchEngineManager_.isAwaitingDrain(); }

    // Ensure embedding generator is initialized for a specific model name (already loaded in
    // provider). Returns success if generator is ready or initialized; schedules no rebuild by
    // itself.
    Result<void> ensureEmbeddingGeneratorFor(const std::string& modelName);

    // Coroutine-based initialization wrapper (awaitable). Uses the same phase logic as
    // initialization but integrates with Boost.Asio coroutine flow.
    boost::asio::awaitable<Result<void>> initializeAsyncAwaitable(yams::compat::stop_token token);

    // Test helpers: inject mock provider and tweak provider state/name
    void __test_setModelProvider(std::shared_ptr<IModelProvider> provider) {
        modelProvider_ = std::move(provider);
    }
    void __test_setAdoptedProviderPluginName(const std::string& name) {
        adoptedProviderPluginName_ = name;
    }
    void __test_setModelProviderDegraded(bool degraded, const std::string& error = {});

#ifdef YAMS_TESTING
    AbiPluginHost* __test_getAbiHost() const { return abiHost_.get(); }
    AbiPluginLoader* __test_getAbiPluginLoader() const { return abiPluginLoader_.get(); }
#endif
    // Test helpers for plugin host FSM transitions (status recovery tests)
    // PBI-088: Delegate to PluginManager which owns the FSM
    void __test_pluginLoadFailed(const std::string& error) {
        if (pluginManager_) {
            pluginManager_->dispatchPluginLoadFailed(error);
        }
    }
    void __test_pluginScanComplete(std::size_t count) {
        if (pluginManager_) {
            pluginManager_->dispatchAllPluginsLoaded(count);
        }
    }
    // Force a vector DB initialization attempt and return whether work was performed
    // (skipped=false indicates already attempted or lock-busy/disabled).
    Result<bool> __test_forceVectorDbInitOnce(const std::filesystem::path& dataDir) {
        return initializeVectorDatabaseOnce(dataDir);
    }

private:
    size_t getEmbeddingDimension() const;

    boost::asio::awaitable<yams::Result<void>>
    co_initContentStore(boost::asio::any_io_executor exec,
                        const boost::asio::cancellation_state& token);

    boost::asio::awaitable<yams::Result<void>>
    co_initDatabase(boost::asio::any_io_executor exec,
                    const boost::asio::cancellation_state& token);

    boost::asio::awaitable<yams::Result<void>>
    co_initSearchEngine(boost::asio::any_io_executor exec,
                        const boost::asio::cancellation_state& token);

    boost::asio::awaitable<yams::Result<void>>
    co_initVectorSystem(boost::asio::any_io_executor exec,
                        const boost::asio::cancellation_state& token);

    boost::asio::awaitable<yams::Result<void>>
    co_initPluginSystem(boost::asio::any_io_executor exec,
                        const boost::asio::cancellation_state& token);

    boost::asio::awaitable<void> co_runSessionWatcher(yams::compat::stop_token token);

    // Awaitable phase helpers for modern architecture
    boost::asio::awaitable<bool> co_openDatabase(const std::filesystem::path& dbPath,
                                                 int timeout_ms, yams::compat::stop_token token);
    boost::asio::awaitable<bool> co_migrateDatabase(int timeout_ms, yams::compat::stop_token token);
    boost::asio::awaitable<std::shared_ptr<yams::search::SearchEngine>>
    co_buildEngine(int timeout_ms, const boost::asio::cancellation_state& token,
                   bool includeEmbeddingGenerator = true);
    bool detectEmbeddingPreloadFlag() const;

    const DaemonConfig& config_;
    StateComponent& state_;
    mutable std::mutex configPersistMutex_{};

    // All the services managed by this component
    std::shared_ptr<api::IContentStore> contentStore_;
    std::shared_ptr<metadata::Database> database_;
    std::shared_ptr<metadata::ConnectionPool> connectionPool_;
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore_;
    std::shared_ptr<GraphComponent> graphComponent_;
    std::shared_ptr<vector::VectorDatabase> vectorDatabase_;
    std::shared_ptr<IModelProvider> modelProvider_;

    // Thread pools: declared early so they destruct LAST (after threads that use them)
    // (reverse order), ensuring coroutines are cancelled before executor dies
    // Deprecated legacy pools removed – now using a single io_context with strands.

    // Legacy members retained for compatibility during transition
    std::unique_ptr<IngestService> ingestService_;
    yams::compat::jthread initThread_; // Retained for legacy async init (will be removed later)

    std::unique_ptr<AbiPluginLoader> abiPluginLoader_;
    std::unique_ptr<AbiPluginHost> abiHost_;
    // NOTE: ExternalPluginHost moved to PluginManager (PBI-093)
    std::unique_ptr<RetrievalSessionManager> retrievalSessions_;
    std::unique_ptr<CheckpointManager> checkpointManager_;

    // Phase 1 (PBI-002): Background task coordination
    // CRITICAL: Must be declared BEFORE jthreads so it destructs AFTER threads
    // (reverse order), ensuring coroutines are cancelled before executor dies
    std::unique_ptr<class BackgroundTaskManager> backgroundTaskManager_;

    // Worker pool metrics
    std::atomic<std::size_t> poolActive_{0};
    std::atomic<std::size_t> poolPosted_{0};
    std::atomic<std::size_t> poolCompleted_{0};

    std::atomic<std::size_t> ingestQueued_{0};
    std::atomic<std::size_t> ingestActive_{0};
    std::atomic<std::size_t> ingestWorkerTarget_{1};

    bool embeddingPreloadOnStartup_{false};

    std::shared_ptr<yams::search::SearchEngine> searchEngine_;
    mutable YAMS_SHARED_LOCKABLE(std::shared_mutex, searchEngineMutex_); // Allow concurrent reads

    // Cross-encoder reranker for improved search ranking
    std::shared_ptr<yams::search::IReranker> rerankerAdapter_;

    // Modern async architecture (Phase 0c): WorkCoordinator delegates threading complexity
    // Member declaration order is CRITICAL for correct destruction
    // 1. Cancellation signal (destructs last among these) - signals all async ops to cancel
    boost::asio::cancellation_signal shutdownSignal_;

    // 2. WorkCoordinator (destructs after cancellation) - owns io_context + worker threads
    //    Replaces ioContext_, workGuard_, workers_ (extracted for reusability and testability)
    std::unique_ptr<WorkCoordinator> workCoordinator_;
    std::unique_ptr<WorkCoordinator> entityWorkCoordinator_;
    std::unique_ptr<boost::asio::thread_pool> cliRequestPool_;

    // 3. Execution domains for logical separation (lightweight strands) - optional for lazy init
    std::optional<boost::asio::strand<boost::asio::any_io_executor>> initStrand_;
    std::optional<boost::asio::strand<boost::asio::any_io_executor>> pluginStrand_;
    std::optional<boost::asio::strand<boost::asio::any_io_executor>> modelStrand_;

    std::atomic<bool> asyncInitStarted_{false};

    std::filesystem::path resolvedDataDir_;

    std::shared_ptr<WalMetricsProvider> walMetricsProvider_;
    std::shared_ptr<yams::wal::WALManager> walManager_;
    std::shared_ptr<yams::integrity::RepairManager> repairManager_;
    std::unique_ptr<PostIngestQueue> postIngest_;
    std::unique_ptr<EmbeddingService> embeddingService_;
    std::unique_ptr<KGWriteQueue> kgWriteQueue_;
    std::vector<std::shared_ptr<yams::extraction::IContentExtractor>> contentExtractors_;
    std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>> symbolExtractors_;
    bool embeddingsAutoOnAdd_{false};
    // Centralized tuning config (persistable via config file; avoids envs).
    TuningConfig tuningConfig_{};

    // Atomic guards retained:
    //  - vectorDbInitAttempted_ (cross-process guard)
    //  - shutdownInvoked_ (safety backstop)

    // Guard to ensure vector database initialization executes at most once per daemon lifetime
    std::atomic<bool> vectorDbInitAttempted_{false};

    // Idempotent vector database initialization entry point
    // Returns true when this call performed initialization work, false when skipped
    // because it was already attempted elsewhere. On failure, returns Error.
    Result<bool> initializeVectorDatabaseOnce(const std::filesystem::path& dataDir);

    std::string adoptedProviderPluginName_;

    // Diagnostics: embedding model name
    std::string embeddingModelName_;

    // Diagnostics: track last content store init error (empty when none)
    std::string contentStoreError_;

    // Idempotence: guard against double shutdown (stop() plus destructor)
    std::atomic<bool> shutdownInvoked_{false};

    // Reference to parent daemon's lifecycle FSM (for subsystem degradation tracking)
    DaemonLifecycleFsm& lifecycleFsm_;

    // FSMs introduced by PBI-046 (initially advisory; will replace atomic flags incrementally)
    // NOTE: pluginHostFsm_ removed - delegated to PluginManager (PBI-088)
    ServiceManagerFsm serviceFsm_{};
    EmbeddingProviderFsm embeddingFsm_{};

    // jthreads: deprecated – replaced by std::thread workers using ioContext_
    // They are kept for compatibility but not used in the new shutdown flow.
    // yams::compat::jthread initThread_;      // Retained for legacy async init (will be removed
    // later) yams::compat::jthread poolReconThread_; // Retained for legacy pool reconciliation

    // Phase 2.4: Extracted managers (consolidate lifecycle management)
    SearchEngineManager searchEngineManager_;
    std::unique_ptr<SearchComponent> searchComponent_;

    std::unique_ptr<PluginManager> pluginManager_;
    std::unique_ptr<VectorSystemManager> vectorSystemManager_;
    std::unique_ptr<DatabaseManager> databaseManager_;

    // Cached GLiNER query concept extraction function (initialized once when plugins ready)
    mutable std::once_flag queryConceptExtractorOnce_;
    mutable search::EntityExtractionFunc cachedQueryConceptExtractor_;

    mutable std::shared_mutex pluginStatusMutex_;
    PluginStatusSnapshot pluginStatusSnapshot_{};
    std::atomic<std::uint32_t> cachedModelProviderModelCount_{0};
};

} // namespace yams::daemon
