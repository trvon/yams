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
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/this_coro.hpp>
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
class RepairService;

class ServiceManager : public IComponent, public std::enable_shared_from_this<ServiceManager> {
public:
    ServiceManager(const DaemonConfig& config, StateComponent& state,
                   DaemonLifecycleFsm& lifecycleFsm);
    ~ServiceManager() override;

    // IComponent interface
    const char* getName() const override { return "ServiceManager"; }

    static int computeEtaRemaining(int expectedSeconds, int progressPercent) {
        if (expectedSeconds <= 0)
            return 0;
        int completed = (expectedSeconds * progressPercent + 99) / 100;
        return std::max(0, expectedSeconds - completed);
    }

    Result<void> initialize() override;

    void startAsyncInit(std::promise<void>* barrierPromise = nullptr,
                        std::atomic<bool>* barrierSet = nullptr);

    void shutdown() override;

    void prepareForRestart() {
        serviceFsm_.reset();
        asyncInitStopSource_ = yams::compat::stop_source{};
    }

    // Start background task coroutines (must be called after shared_ptr construction)
    void startBackgroundTasks();

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
    std::shared_ptr<metadata::ConnectionPool> getWriteConnectionPool() const {
        std::lock_guard<std::mutex> lk(poolMutex_);
        return connectionPool_;
    }
    std::shared_ptr<metadata::ConnectionPool> getReadConnectionPool() const {
        std::lock_guard<std::mutex> lk(poolMutex_);
        return readConnectionPool_;
    }
    void onSearchRequestQueued() { searchQueued_.fetch_add(1, std::memory_order_relaxed); }
    bool tryStartSearchRequest(std::uint32_t concurrencyCap) {
        std::uint32_t queued = searchQueued_.load(std::memory_order_relaxed);
        while (queued > 0 && !searchQueued_.compare_exchange_weak(queued, queued - 1,
                                                                  std::memory_order_relaxed)) {
        }
        if (concurrencyCap == 0) {
            searchActive_.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
        std::uint32_t active = searchActive_.load(std::memory_order_relaxed);
        while (true) {
            if (active >= concurrencyCap) {
                return false;
            }
            if (searchActive_.compare_exchange_weak(active, active + 1,
                                                    std::memory_order_relaxed)) {
                return true;
            }
        }
    }
    void onSearchRequestFinished() {
        std::uint32_t active = searchActive_.load(std::memory_order_relaxed);
        while (active > 0 && !searchActive_.compare_exchange_weak(active, active - 1,
                                                                  std::memory_order_relaxed)) {
        }
    }
    void onSearchRequestRejected() {
        std::uint32_t queued = searchQueued_.load(std::memory_order_relaxed);
        while (queued > 0 && !searchQueued_.compare_exchange_weak(queued, queued - 1,
                                                                  std::memory_order_relaxed)) {
        }
    }
    std::uint32_t getSearchActiveRequests() const {
        return searchActive_.load(std::memory_order_relaxed);
    }
    void onSnapshotPersisted() { snapshotsPersisted_.fetch_add(1, std::memory_order_relaxed); }
    std::uint64_t getSnapshotsPersistedCount() const {
        return snapshotsPersisted_.load(std::memory_order_relaxed);
    }
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
    void enqueuePostIngestBatch(const std::vector<std::string>& hashes, const std::string& mime);
    SearchEngineSnapshot getSearchEngineFsmSnapshot() const {
        return searchEngineManager_.getSnapshot();
    }
    yams::search::SearchEngine* getCachedSearchEngine() const {
        return searchEngineManager_.getCachedEngine();
    }

    PluginStatusSnapshot getPluginStatusSnapshot() const;
    void setCachedModelProviderModelCount(std::uint32_t count) {
        cachedModelProviderModelCount_.store(count, std::memory_order_relaxed);
    }
    void refreshPluginStatusSnapshot();
    boost::asio::any_io_executor getWorkerExecutor() const;
    std::function<void(bool)> getWorkerJobSignal();
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

    std::shared_ptr<RepairService> getRepairServiceShared() const {
        return std::atomic_load_explicit(&repairService_, std::memory_order_acquire);
    }
    RepairService* getRepairService() const { return getRepairServiceShared().get(); }
    void startRepairService(std::function<size_t()> activeConnFn);
    void stopRepairService();

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

    AbiPluginLoader* getAbiPluginLoader() const { return abiPluginLoader_.get(); }
    AbiPluginHost* getAbiPluginHost() const { return abiHost_.get(); }
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
    std::size_t getEmbeddingActiveInferSubBatches() const;
    uint64_t getEmbeddingInferOldestMs() const;
    uint64_t getEmbeddingInferStartedCount() const;
    uint64_t getEmbeddingInferCompletedCount() const;
    uint64_t getEmbeddingInferLastMs() const;
    uint64_t getEmbeddingInferMaxMs() const;
    uint64_t getEmbeddingInferWarnCount() const;

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
    ProviderSnapshot getEmbeddingProviderFsmSnapshot() const {
        if (pluginManager_) {
            return pluginManager_->getEmbeddingProviderFsmSnapshot();
        }
        return embeddingFsm_.snapshot();
    }
    PluginHostSnapshot getPluginHostFsmSnapshot() const {
        if (pluginManager_) {
            return pluginManager_->getPluginHostFsmSnapshot();
        }
        // Fallback: return NotInitialized state if PluginManager not available
        return PluginHostSnapshot{};
    }

    // Expose resolved daemon configuration for components that need paths
    const DaemonConfig& getConfig() const { return config_; }
    const std::filesystem::path& getResolvedDataDir() const { return resolvedDataDir_; }
    void persistTrustedPluginPath(const std::filesystem::path& path, bool remove) const;

    Result<bool> adoptModelProviderFromHosts(const std::string& preferredName = "");
    Result<size_t> adoptContentExtractorsFromHosts();
    Result<size_t> adoptSymbolExtractorsFromHosts();
    Result<size_t> adoptEntityExtractorsFromHosts();
    bool isEmbeddingsAutoOnAdd() const { return embeddingsAutoOnAdd_; }

    boost::asio::awaitable<Result<size_t>> autoloadPluginsNow();
    boost::asio::awaitable<void> preloadPreferredModelIfConfigured();

    std::string resolvePreferredModel() const;

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

    boost::asio::awaitable<void> co_enableEmbeddingsAndRebuild();

    bool triggerSearchEngineRebuildIfNeeded();

    bool requestSearchEngineRebuild(const std::string& reason, bool includeVector = true,
                                    bool waitForDrain = true) {
        return searchEngineManager_.requestRebuild(reason, includeVector, waitForDrain);
    }

    bool isSearchEngineAwaitingDrain() const { return searchEngineManager_.isAwaitingDrain(); }

    Result<void> ensureEmbeddingGeneratorFor(const std::string& modelName);

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
    mutable std::mutex poolMutex_{}; // Guards connectionPool_ / readConnectionPool_

    // All the services managed by this component
    std::shared_ptr<api::IContentStore> contentStore_;
    std::shared_ptr<metadata::Database> database_;
    std::shared_ptr<metadata::ConnectionPool> connectionPool_;     // GUARDED_BY(poolMutex_)
    std::shared_ptr<metadata::ConnectionPool> readConnectionPool_; // GUARDED_BY(poolMutex_)
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore_;
    std::shared_ptr<GraphComponent> graphComponent_;
    std::shared_ptr<vector::VectorDatabase> vectorDatabase_;
    std::shared_ptr<IModelProvider> modelProvider_;

    std::unique_ptr<IngestService> ingestService_;
    yams::compat::jthread initThread_; // Retained for legacy async init (will be removed later)

    std::unique_ptr<AbiPluginLoader> abiPluginLoader_;
    std::unique_ptr<AbiPluginHost> abiHost_;
    // NOTE: ExternalPluginHost moved to PluginManager (PBI-093)
    std::unique_ptr<RetrievalSessionManager> retrievalSessions_;
    std::unique_ptr<CheckpointManager> checkpointManager_;
    std::unique_ptr<class BackgroundTaskManager> backgroundTaskManager_;

    // Worker pool metrics
    std::atomic<std::size_t> poolActive_{0};
    std::atomic<std::size_t> poolPosted_{0};
    std::atomic<std::size_t> poolCompleted_{0};

    std::atomic<std::size_t> ingestQueued_{0};
    std::atomic<std::size_t> ingestActive_{0};
    std::atomic<std::size_t> ingestWorkerTarget_{1};
    std::atomic<std::uint32_t> searchActive_{0};
    std::atomic<std::uint32_t> searchQueued_{0};
    std::atomic<std::uint64_t> snapshotsPersisted_{0};

    bool embeddingPreloadOnStartup_{false};

    std::shared_ptr<yams::search::SearchEngine> searchEngine_;
    mutable YAMS_SHARED_LOCKABLE(std::shared_mutex, searchEngineMutex_); // Allow concurrent reads

    // Cross-encoder reranker for improved search ranking
    std::shared_ptr<yams::search::IReranker> rerankerAdapter_;

    boost::asio::cancellation_signal shutdownSignal_;

    std::unique_ptr<WorkCoordinator> workCoordinator_;
    std::unique_ptr<WorkCoordinator> entityWorkCoordinator_;
    std::unique_ptr<boost::asio::thread_pool> cliRequestPool_;

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
    std::shared_ptr<RepairService> repairService_;
    mutable std::mutex repairServiceMutex_;
    std::vector<std::shared_ptr<yams::extraction::IContentExtractor>> contentExtractors_;
    std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>> symbolExtractors_;
    bool embeddingsAutoOnAdd_{false};
    // Centralized tuning config (persistable via config file; avoids envs).
    TuningConfig tuningConfig_{};

    // Guard to ensure vector database initialization executes at most once per daemon lifetime
    std::atomic<bool> vectorDbInitAttempted_{false};

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

    ServiceManagerFsm serviceFsm_{};
    EmbeddingProviderFsm embeddingFsm_{};

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
