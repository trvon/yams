#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stop_token>
#include "IComponent.h"
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/thread_pool.hpp>
#include <yams/app/services/services.hpp>
#include <yams/compat/thread_stop_compat.h>
#include <yams/core/types.h>
#include <yams/daemon/components/EmbeddingProviderFsm.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/PluginHostFsm.h>
#include <yams/daemon/components/PoolManager.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/ServiceManagerFsm.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningConfig.h>
#include <yams/daemon/components/WalMetricsProvider.h>
#include <yams/daemon/daemon.h> // For DaemonConfig
#include <yams/daemon/ipc/retrieval_session.h>
#include <yams/daemon/resource/abi_plugin_loader.h>
#include <yams/daemon/resource/plugin_host.h>
#include <yams/daemon/resource/plugin_loader.h>
#include <yams/extraction/content_extractor.h>

// Forward declarations for services
namespace yams::api {
class IContentStore;
}
namespace yams::metadata {
class Database;
class ConnectionPool;
class MetadataRepository;
} // namespace yams::metadata
namespace yams::search {
class SearchExecutor;
class HybridSearchEngine;
class SearchEngineBuilder;
} // namespace yams::search
namespace yams::vector {
class VectorIndexManager;
class EmbeddingGenerator;
class VectorDatabase;
} // namespace yams::vector
namespace yams::daemon {
class PluginLoader;
class AbiPluginLoader;
class IModelProvider;
class RetrievalSessionManager;
class WorkerPool;
class TuningManager;
} // namespace yams::daemon

namespace yams::daemon {

class IngestService;

class ServiceManager : public IComponent, public std::enable_shared_from_this<ServiceManager> {
public:
    using InitCompleteCallback = std::function<void(bool success, const std::string& error)>;

    ServiceManager(const DaemonConfig& config, StateComponent& state);
    ~ServiceManager() override;

    // IComponent interface
    const char* getName() const override { return "ServiceManager"; }
    Result<void> initialize() override;
    void shutdown() override;

    // Set callback to be invoked when async initialization completes
    void setInitCompleteCallback(InitCompleteCallback callback) {
        initCompleteCallback_ = callback;
    }

    // Service Accessors
    std::shared_ptr<api::IContentStore> getContentStore() const { return contentStore_; }
    std::shared_ptr<metadata::MetadataRepository> getMetadataRepo() const { return metadataRepo_; }
    std::shared_ptr<search::SearchExecutor> getSearchExecutor() const { return searchExecutor_; }
    std::shared_ptr<IModelProvider> getModelProvider() const { return modelProvider_; }
    std::shared_ptr<yams::search::HybridSearchEngine> getSearchEngineSnapshot() const;
    std::shared_ptr<vector::VectorIndexManager> getVectorIndexManager() const {
        return vectorIndexManager_;
    }
    std::shared_ptr<vector::EmbeddingGenerator> getEmbeddingGenerator() const {
        return embeddingGenerator_;
    }
    std::string getEmbeddingModelName() const { return embeddingModelName_; }
    std::shared_ptr<vector::VectorDatabase> getVectorDatabase() const { return vectorDatabase_; }
    std::shared_ptr<WorkerPool> getWorkerPool() const { return workerPool_; }
    // Resize the worker pool to a target size; creates pool on demand.
    bool resizeWorkerPool(std::size_t target);
    PostIngestQueue* getPostIngestQueue() const { return postIngest_.get(); }
    // Resize PostIngestQueue worker threads; returns false if unchanged/missing.
    bool resizePostIngestThreads(std::size_t target);
    struct SearchLoadMetrics {
        std::uint32_t active{0};
        std::uint32_t queued{0};
        std::uint64_t executed{0};
        double cacheHitRate{0.0};
        std::uint64_t avgLatencyUs{0};
        std::uint32_t concurrencyLimit{0};
    };
    SearchLoadMetrics getSearchLoadMetrics() const;
    bool applySearchConcurrencyTarget(std::size_t target);
    struct IngestMetricsSnapshot {
        std::size_t queued;
        std::size_t active;
        std::size_t target;
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
    void enqueuePostIngest(const std::string& hash, const std::string& mime) {
        bool routedViaBus = false;
        if (yams::daemon::TuneAdvisor::useInternalBusForPostIngest()) {
            yams::daemon::InternalEventBus::PostIngestTask t{hash, mime};
            static std::shared_ptr<
                yams::daemon::SpscQueue<yams::daemon::InternalEventBus::PostIngestTask>>
                q = yams::daemon::InternalEventBus::instance()
                        .get_or_create_channel<yams::daemon::InternalEventBus::PostIngestTask>(
                            "post_ingest", 4096);
            if (q && q->try_push(std::move(t))) {
                yams::daemon::InternalEventBus::instance().incPostQueued();
                routedViaBus = true;
                if (postIngest_)
                    postIngest_->notifyWorkers();
            } else {
                yams::daemon::InternalEventBus::instance().incPostDropped();
            }
        }
        if (routedViaBus)
            return;
        // Direct blocking enqueue for predictable latency and throughput
        if (postIngest_) {
            PostIngestQueue::Task t{
                hash, mime, /*session*/ "", {}, PostIngestQueue::Task::Stage::Metadata};
            postIngest_->enqueue(std::move(t));
            postIngest_->notifyWorkers();
        }
    }
    // Last search engine build metadata (for diagnostics/status)
    std::string getLastSearchBuildReason() const { return lastSearchBuildReason_; }
    bool getLastVectorEnabled() const { return lastVectorEnabled_; }
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
        if (cfg.postIngestThreadsMin > 0)
            (void)resizePostIngestThreads(cfg.postIngestThreadsMin);
        // Align PoolManager bounds for post_ingest with new config
        try {
            PoolManager::Config piCfg{};
            piCfg.min_size =
                static_cast<uint32_t>(std::max<std::size_t>(1, cfg.postIngestThreadsMin));
            piCfg.max_size =
                static_cast<uint32_t>(std::max(cfg.postIngestThreadsMin, cfg.postIngestThreadsMax));
            piCfg.cooldown_ms = TuneAdvisor::poolCooldownMs();
            piCfg.low_watermark = TuneAdvisor::poolLowWatermarkPercent();
            piCfg.high_watermark = TuneAdvisor::poolHighWatermarkPercent();
            PoolManager::instance().configure("post_ingest", piCfg);
        } catch (...) {
        }
    }
    const std::vector<std::shared_ptr<yams::extraction::IContentExtractor>>&
    getContentExtractors() const {
        return contentExtractors_;
    }

    // ContentStore diagnostics
    std::string getContentStoreError() const { return contentStoreError_; }

    // WAL metrics provider (may return zeros until a WALManager is attached)
    std::shared_ptr<WalMetricsProvider> getWalMetricsProvider() const {
        return walMetricsProvider_;
    }
    void attachWalManager(std::shared_ptr<yams::wal::WALManager> wal) {
        if (!walMetricsProvider_)
            walMetricsProvider_ = std::make_shared<WalMetricsProvider>();
        walMetricsProvider_->setManager(std::move(wal));
    }

    // Plugins
    std::vector<PluginInfo> getLoadedPlugins() const {
        // Prefer ABI host view of loaded plugins; adapt to PluginInfo for stats
        if (abiHost_) {
            std::vector<PluginInfo> out;
            try {
                for (const auto& d : abiHost_->listLoaded()) {
                    PluginInfo pi;
                    pi.name = d.name;
                    pi.path = d.path;
                    pi.loaded = true;
                    out.push_back(std::move(pi));
                }
                return out;
            } catch (...) {
            }
        }
        if (pluginLoader_)
            return pluginLoader_->getLoadedPlugins();
        return {};
    }

    // ABI plugin loader access
    AbiPluginLoader* getAbiPluginLoader() const { return abiPluginLoader_.get(); }
    // Plugin host (C‑ABI)
    AbiPluginHost* getAbiPluginHost() const { return abiHost_.get(); }
    WasmPluginHost* getWasmPluginHost() const { return wasmHost_.get(); }
    ExternalPluginHost* getExternalPluginHost() const { return externalHost_.get(); }

    // Worker pool metrics accessors
    std::size_t getWorkerActive() const { return poolActive_.load(std::memory_order_relaxed); }
    std::size_t getWorkerPosted() const { return poolPosted_.load(std::memory_order_relaxed); }
    std::size_t getWorkerCompleted() const {
        return poolCompleted_.load(std::memory_order_relaxed);
    }
    std::size_t getWorkerThreads() const { return poolThreads_; }

    RetrievalSessionManager* getRetrievalSessionManager() const { return retrievalSessions_.get(); }

    // Get AppContext for app services
    app::services::AppContext getAppContext() const;

    // FSM snapshots (read-only) for status/diagnostics
    ServiceManagerSnapshot getServiceManagerFsmSnapshot() const { return serviceFsm_.snapshot(); }
    ProviderSnapshot getEmbeddingProviderFsmSnapshot() const { return embeddingFsm_.snapshot(); }
    PluginHostSnapshot getPluginHostFsmSnapshot() const { return pluginHostFsm_.snapshot(); }

    // PBI-008-11: FSM hook scaffolds for session preparation lifecycle (no-op for now)
    void onPrepareSessionRequested() {};
    void onPrepareSessionCompleted() {};

    // Expose resolved daemon configuration for components that need paths
    const DaemonConfig& getConfig() const { return config_; }
    // Resolved data directory used for storage (may derive from env/config)
    const std::filesystem::path& getResolvedDataDir() const { return resolvedDataDir_; }

    // Try to adopt a model provider from loaded plugin hosts at runtime.
    // If preferredName is non-empty, attempts that plugin first.
    // Returns true on success.
    Result<bool> adoptModelProviderFromHosts(const std::string& preferredName = "");
    Result<size_t> adoptContentExtractorsFromHosts();
    bool isEmbeddingsAutoOnAdd() const { return embeddingsAutoOnAdd_; }

    // Ensure an embedding generator is available by adopting a provider and loading
    // the preferred model if necessary. Preferred model is resolved from config or
    // environment (YAMS_PREFERRED_MODEL), with a fallback to the first installed
    // local model under ~/.yams/models.
    Result<void> ensureEmbeddingGeneratorReady();
    bool shouldPreloadEmbeddings() const;
    void scheduleEmbeddingWarmup();

    // Explicit, on-demand plugin autoload: scans trusted roots and default directories,
    // loads plugins via ABI hosts, and attempts to adopt model providers and content extractors.
    // Returns number of plugins loaded during this invocation.
    Result<size_t> autoloadPluginsNow();
    // Attempt to preload preferred model if a model provider is available.
    // Preferred model is resolved from env (YAMS_PREFERRED_MODEL) or by scanning ~/.yams/models.
    void preloadPreferredModelIfConfigured();

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
    const std::string& lastModelError() const { return lastModelError_; }
    const std::string& adoptedProviderPluginName() const { return adoptedProviderPluginName_; }
    void clearModelProviderError() { lastModelError_.clear(); }

    // Combined embedding initialization and search engine rebuild coroutine.
    // This method provides idempotent, race-safe initialization of embedding capabilities
    // followed by search engine rebuild to enable vector search. Uses atomic guards to
    // prevent duplicate execution and ensures proper lifetime management.
    // Should be called via co_spawn with shared_from_this() for safety.
    boost::asio::awaitable<void> co_enableEmbeddingsAndRebuild();

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
    void __test_setModelProviderDegraded(bool degraded, const std::string& error = {}) {
        lastModelError_ = error;
        try {
            if (degraded) {
                embeddingFsm_.dispatch(
                    ProviderDegradedEvent{error.empty() ? std::string{"test"} : error});
            } else {
                // Treat as recovered to a ready-ish state without asserting a model; use
                // ModelLoadedEvent with dimension 0 to clear degraded state in FSM.
                embeddingFsm_.dispatch(ModelLoadedEvent{embeddingModelName_, 0});
            }
        } catch (...) {
        }
    }
#ifdef YAMS_TESTING
    AbiPluginHost* __test_getAbiHost() const { return abiHost_.get(); }
    AbiPluginLoader* __test_getAbiPluginLoader() const { return abiPluginLoader_.get(); }
#endif
    // Test helpers for plugin host FSM transitions (status recovery tests)
    void __test_pluginLoadFailed(const std::string& error) {
        try {
            pluginHostFsm_.dispatch(PluginLoadFailedEvent{error});
        } catch (...) {
        }
    }
    void __test_pluginScanComplete(std::size_t count) {
        try {
            pluginHostFsm_.dispatch(AllPluginsLoadedEvent{count});
        } catch (...) {
        }
    }
    // Force a vector DB initialization attempt and return whether work was performed
    // (skipped=false indicates already attempted or lock-busy/disabled).
    Result<bool> __test_forceVectorDbInitOnce(const std::filesystem::path& dataDir) {
        return initializeVectorDatabaseOnce(dataDir);
    }

private:
    // Invoke init completion callback exactly once in a thread-safe manner.
    // Returns true if this call fired the callback; false if it was already invoked.
    bool invokeInitCompleteOnce(bool success, const std::string& error);

    // Awaitable phase helpers (coroutine-based)
    boost::asio::awaitable<bool> co_openDatabase(const std::filesystem::path& dbPath,
                                                 int timeout_ms, yams::compat::stop_token token);
    boost::asio::awaitable<bool> co_migrateDatabase(int timeout_ms, yams::compat::stop_token token);
    boost::asio::awaitable<std::shared_ptr<yams::search::HybridSearchEngine>>
    co_buildEngine(int timeout_ms, yams::compat::stop_token token,
                   bool includeEmbeddingGenerator = true);
    bool detectEmbeddingPreloadFlag() const;

    const DaemonConfig& config_;
    StateComponent& state_;
    yams::compat::jthread initThread_;

    // All the services managed by this component
    std::shared_ptr<api::IContentStore> contentStore_;
    std::shared_ptr<metadata::Database> database_;
    std::shared_ptr<metadata::ConnectionPool> connectionPool_;
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore_; // PBI-043: tree diff KG integration
    std::shared_ptr<search::SearchExecutor> searchExecutor_;
    std::shared_ptr<vector::VectorIndexManager> vectorIndexManager_;
    std::shared_ptr<vector::EmbeddingGenerator> embeddingGenerator_;
    std::shared_ptr<vector::VectorDatabase> vectorDatabase_;
    std::shared_ptr<search::SearchEngineBuilder> searchBuilder_;
    std::shared_ptr<IModelProvider> modelProvider_;
    std::unique_ptr<boost::asio::thread_pool> initPool_;
    std::unique_ptr<boost::asio::thread_pool>
        modelLoadPool_; // Dedicated pool for model loading operations
    std::unique_ptr<PluginLoader> pluginLoader_;
    std::unique_ptr<AbiPluginLoader> abiPluginLoader_;
    std::unique_ptr<AbiPluginHost> abiHost_;
    std::unique_ptr<WasmPluginHost> wasmHost_;
    std::unique_ptr<ExternalPluginHost> externalHost_;
    std::unique_ptr<RetrievalSessionManager> retrievalSessions_;
    std::shared_ptr<WorkerPool> workerPool_;
    // Phase 6: background reconciler for PoolManager stats (logging only)
    yams::compat::jthread poolReconThread_;
    // Removed: lifecycleReadyWatchdog_ (1200ms defensive timeout eliminated)
    // Worker pool metrics
    std::atomic<std::size_t> poolActive_{0};
    std::atomic<std::size_t> poolPosted_{0};
    std::atomic<std::size_t> poolCompleted_{0};
    std::size_t poolThreads_{0};

    std::atomic<std::size_t> ingestQueued_{0};
    std::atomic<std::size_t> ingestActive_{0};
    std::atomic<std::size_t> ingestWorkerTarget_{1};

    std::atomic<bool> embeddingWarmupScheduled_{false};
    bool embeddingPreloadOnStartup_{false};

    std::unique_ptr<IngestService> ingestService_;
    std::shared_ptr<yams::search::HybridSearchEngine> searchEngine_;
    mutable std::shared_mutex searchEngineMutex_; // Allow concurrent reads

    InitCompleteCallback initCompleteCallback_;
    std::atomic<bool> initCompleteInvoked_{false};
    std::filesystem::path resolvedDataDir_;

    std::shared_ptr<WalMetricsProvider> walMetricsProvider_;
    std::unique_ptr<PostIngestQueue> postIngest_;
    std::vector<std::shared_ptr<yams::extraction::IContentExtractor>> contentExtractors_;
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

    // Degraded provider tracking (FSM-first). Atomic retained only for ABI/back-compat during
    // transition, but not written by new code paths.
    std::atomic<bool> modelProviderDegraded_{false};
    std::string lastModelError_;
    std::string adoptedProviderPluginName_;

    // Diagnostics: track last search build reason and vector enablement
    std::string lastSearchBuildReason_{"unknown"};
    bool lastVectorEnabled_{false};
    std::string embeddingModelName_;

    // Diagnostics: track last content store init error (empty when none)
    std::string contentStoreError_;

    // Idempotence: guard against double shutdown (stop() plus destructor)
    std::atomic<bool> shutdownInvoked_{false};

    // FSMs introduced by PBI-046 (initially advisory; will replace atomic flags incrementally)
    ServiceManagerFsm serviceFsm_{};
    EmbeddingProviderFsm embeddingFsm_{};
    PluginHostFsm pluginHostFsm_{};
};

} // namespace yams::daemon
