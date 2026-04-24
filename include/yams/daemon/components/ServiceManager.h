#pragma once

#include <atomic>
#include <condition_variable>
#include <exception>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
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
#include <yams/daemon/components/AsyncInitOrchestrator.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/DatabaseManager.h>
#include <yams/daemon/components/EmbeddingLifecycleManager.h>
#include <yams/daemon/components/EmbeddingProviderFsm.h>
#include <yams/daemon/components/EmbeddingService.h>
#include <yams/daemon/components/IngestMetricsPublisher.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/KGWriteQueue.h>
#include <yams/daemon/components/PluginHostFsm.h>
#include <yams/daemon/components/PluginManager.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/RepairServiceHost.h>
#include <yams/daemon/components/RequestExecutor.h>
#include <yams/daemon/components/SearchAdmissionController.h>
#include <yams/daemon/components/SearchComponent.h>
#include <yams/daemon/components/SearchEngineFsm.h>
#include <yams/daemon/components/SearchEngineManager.h>
#include <yams/daemon/components/ServiceManagerFsm.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TopologyManager.h>
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
#include <yams/daemon/resource/external_plugin_host.h>
#include <yams/daemon/resource/plugin_host.h>
#include <yams/extraction/content_extractor.h>
#include <yams/profiling.h>
#include <yams/search/search_engine.h>
#include <yams/search/search_execution_context.h>
#include <yams/vector/vector_database.h>
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
class VectorIndexCoordinator;
} // namespace yams::daemon

namespace yams::daemon {

class IngestService;
class GraphComponent;
class RepairService;
struct ModelLoadEvent;

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
        asyncInit_.resetForRestart();
    }

    // Start background task coroutines (must be called after shared_ptr construction)
    void startBackgroundTasks();
    void startDeferredMetadataWarmup();

    std::shared_ptr<api::IContentStore> getContentStore() const {
        if (databaseManager_) {
            auto store = databaseManager_->getContentStore();
            if (store)
                return store;
        }
        return nullptr;
    }
    std::shared_ptr<metadata::MetadataRepository> getMetadataRepo() const {
        return databaseManager_ ? databaseManager_->getMetadataRepo() : nullptr;
    }
    std::shared_ptr<IModelProvider> getModelProvider() const { return loadModelProvider(); }
    Result<std::string>
    ensureEmbeddingModelReadySync(const std::string& requestedModel,
                                  std::function<void(const ModelLoadEvent&)> progress = {},
                                  int timeoutMs = 0, bool keepHot = true, bool warmup = true) {
        return embeddingLifecycle_.ensureModelReadySync(requestedModel, std::move(progress),
                                                        timeoutMs, keepHot, warmup);
    }
    boost::asio::awaitable<Result<std::string>>
    co_ensureEmbeddingModelReady(const std::string& requestedModel,
                                 std::function<void(const ModelLoadEvent&)> progress = {},
                                 int timeoutMs = 0, bool keepHot = true, bool warmup = true);
    bool startEmbeddingWarmupIfConfigured();
    std::shared_ptr<yams::search::SearchEngine> getSearchEngineSnapshot() const;
    SearchEngineSnapshot getSearchEngineStatusSnapshot() const {
        return searchEngineManager_.getSnapshot();
    }
    const std::string& getEmbeddingModelName() const { return embeddingLifecycle_.modelName(); }
    std::shared_ptr<vector::VectorDatabase> getVectorDatabase() const {
        return vectorSystemManager_ ? vectorSystemManager_->getVectorDatabase() : nullptr;
    }
    std::shared_ptr<WorkerPool> getWorkerPool() const { return nullptr; }
    WorkCoordinator* getWorkCoordinator() const { return workCoordinator_.get(); }
    boost::asio::any_io_executor getCliExecutor() const;
    // Resize the worker pool to a target size; creates pool on demand.
    bool resizeWorkerPool(std::size_t target);
    std::shared_ptr<PostIngestQueue> getPostIngestQueue() const {
        return std::atomic_load_explicit(&postIngest_, std::memory_order_acquire);
    }
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
        if (databaseManager_) {
            return databaseManager_->getConnectionPool();
        }
        return nullptr;
    }
    std::shared_ptr<metadata::ConnectionPool> getReadConnectionPool() const {
        if (databaseManager_) {
            return databaseManager_->getReadConnectionPool();
        }
        return nullptr;
    }
    void onSearchRequestQueued() { searchAdmission_.onQueued(); }
    bool tryStartSearchRequest(std::uint32_t concurrencyCap) {
        return searchAdmission_.tryStart(concurrencyCap);
    }
    void onSearchRequestFinished() { searchAdmission_.onFinished(); }
    void onSearchRequestRejected() { searchAdmission_.onRejected(); }
    std::uint32_t getSearchActiveRequests() const { return searchAdmission_.active(); }
    SearchAdmissionController& getSearchAdmissionController() { return searchAdmission_; }
    void onSnapshotPersisted() { metricsPublisher_.onSnapshotPersisted(); }
    std::uint64_t getSnapshotsPersistedCount() const {
        return metricsPublisher_.snapshotsPersistedCount();
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
        metricsPublisher_.publishIngest(queued, active);
    }
    void publishIngestQueued(std::size_t queued) { metricsPublisher_.publishQueued(queued); }
    void publishIngestActive(std::size_t active) { metricsPublisher_.publishActive(active); }
    void setIngestWorkerTarget(std::size_t target) { metricsPublisher_.setWorkerTarget(target); }
    std::size_t ingestWorkerTarget() const { return metricsPublisher_.workerTarget(); }
    IngestMetricsSnapshot getIngestMetricsSnapshot() const {
        auto s = metricsPublisher_.snapshot();
        return {s.queued, s.active, s.target};
    }
    IngestMetricsPublisher& getIngestMetricsPublisher() { return metricsPublisher_; }
    void enqueuePostIngest(const std::string& hash, const std::string& mime);
    void enqueuePostIngestBatch(const std::vector<std::string>& hashes, const std::string& mime);
    SearchEngineSnapshot getSearchEngineFsmSnapshot() const {
        return searchEngineManager_.getSnapshot();
    }
    yams::search::IndexFreshnessSnapshot getIndexFreshnessSnapshot() const {
        yams::search::IndexFreshnessSnapshot snapshot;
        const auto ingest = getIngestMetricsSnapshot();
        snapshot.ingestQueued = static_cast<std::uint32_t>(
            std::min<std::size_t>(ingest.queued, std::numeric_limits<std::uint32_t>::max()));
        snapshot.ingestInFlight = static_cast<std::uint32_t>(
            std::min<std::size_t>(ingest.active, std::numeric_limits<std::uint32_t>::max()));
        if (auto postIngest = getPostIngestQueue()) {
            snapshot.postIngestQueued = static_cast<std::uint32_t>(std::min<std::size_t>(
                postIngest->size(), std::numeric_limits<std::uint32_t>::max()));
            snapshot.postIngestInFlight = static_cast<std::uint32_t>(std::min<std::size_t>(
                postIngest->totalInFlight(), std::numeric_limits<std::uint32_t>::max()));
        }
        const auto lexicalDelta = searchEngineManager_.getLexicalDeltaSnapshot();
        snapshot.lexicalDeltaQueuedEpoch = lexicalDelta.queuedEpoch;
        snapshot.lexicalDeltaPublishedEpoch = lexicalDelta.publishedEpoch;
        snapshot.lexicalDeltaPendingDocs = static_cast<std::uint32_t>(std::min<std::uint64_t>(
            lexicalDelta.pendingDocs, std::numeric_limits<std::uint32_t>::max()));
        snapshot.lexicalDeltaPublishedDocs = lexicalDelta.publishedDocs;
        snapshot.lexicalDeltaRecentDocs = static_cast<std::uint32_t>(std::min<std::uint64_t>(
            lexicalDelta.recentDocs, std::numeric_limits<std::uint32_t>::max()));
        snapshot.topologyEpoch = topologyManager_.publishedEpoch();
        const auto searchSnapshot = searchEngineManager_.getSnapshot();
        snapshot.awaitingDrain = searchSnapshot.state == SearchEngineState::AwaitingDrain;
        snapshot.lexicalReady = searchSnapshot.state == SearchEngineState::Ready;
        snapshot.vectorReady = searchSnapshot.vectorEnabled && snapshot.lexicalReady;
        snapshot.kgReady = snapshot.lexicalReady && snapshot.postIngestQueued == 0 &&
                           snapshot.postIngestInFlight == 0;
        snapshot.topologyReady = snapshot.kgReady && !snapshot.awaitingDrain;
        if (auto* engine = searchEngineManager_.getCachedEngine()) {
            const auto lexical = engine->getSimeonLexicalStatus();
            snapshot.simeonLexicalConfigured = lexical.configured;
            snapshot.simeonLexicalReady = lexical.ready;
            snapshot.simeonLexicalBuilding = lexical.building;
            snapshot.simeonFragmentGeometryReady = lexical.fragmentGeometryReady;
        }
        return snapshot;
    }
    std::vector<std::string> getRecentLexicalDeltaHashes() const {
        return searchEngineManager_.getRecentLexicalDeltaHashes();
    }
    std::vector<std::string> getTopologyOverlayHashes(std::size_t limit = 64) const {
        return topologyManager_.getOverlayHashes(limit);
    }
    yams::search::SearchEngine* getCachedSearchEngine() const {
        return searchEngineManager_.getCachedEngine();
    }

    PluginStatusSnapshot getPluginStatusSnapshot() const;
    std::shared_ptr<const PluginStatusSnapshot> getPluginStatusSnapshotPtr() const;
    void refreshPluginStatusSnapshot();
    boost::asio::any_io_executor getWorkerExecutor() const;
    std::function<void(bool)> getWorkerJobSignal();
    std::size_t getWorkerQueueDepth() const;

    // Tuning configuration (no envs): getter/setter with live application where applicable.
    const TuningConfig& getTuningConfig() const { return tuningConfig_; }
    void setTuningConfig(const TuningConfig& cfg) {
        tuningConfig_ = cfg;
        if (cfg.postIngestCapacity > 0) {
            auto piq = std::atomic_load_explicit(&postIngest_, std::memory_order_acquire);
            if (piq) {
                piq->setCapacity(cfg.postIngestCapacity);
            }
        }
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

    bool hasTitleExtractor() const {
        auto piq = std::atomic_load_explicit(&postIngest_, std::memory_order_acquire);
        return piq && piq->hasTitleExtractor();
    }

    // Knowledge Graph Store (PBI-059)
    std::shared_ptr<metadata::KnowledgeGraphStore> getKgStore() const {
        return databaseManager_ ? databaseManager_->getKgStore() : nullptr;
    }

    // KG Write Queue - serializes KG writes to eliminate lock contention
    KGWriteQueue* getKgWriteQueue() const { return kgWriteQueue_.get(); }

    // Graph Component (PBI-009)
    std::shared_ptr<GraphComponent> getGraphComponent() const { return loadGraphComponent(); }

    // ContentStore diagnostics
    std::string getContentStoreError() const {
        if (databaseManager_) {
            return databaseManager_->getContentStoreError();
        }
        return std::string{};
    }

    // WAL metrics provider (may return zeros until a WALManager is attached)
    std::shared_ptr<WalMetricsProvider> getWalMetricsProvider() const {
        return databaseManager_ ? databaseManager_->getWalMetricsProvider()
                                : std::shared_ptr<WalMetricsProvider>{};
    }
    std::shared_ptr<yams::integrity::RepairManager> getRepairManager() const {
        return repairManager_;
    }

    std::shared_ptr<RepairService> getRepairServiceShared() const {
        return repairServiceHost_.get();
    }
    void startRepairService(std::function<size_t()> activeConnFn);
    void stopRepairService();

    std::shared_ptr<VectorIndexCoordinator> getVectorIndexCoordinator() const {
        return vectorIndexCoordinator_;
    }

    using TopologyRebuildStats = TopologyManager::RebuildStats;
    using TopologyTelemetrySnapshot = TopologyManager::TelemetrySnapshot;

    Result<TopologyRebuildStats>
    rebuildTopologyArtifacts(const std::string& reason, bool dryRun = false,
                             const std::vector<std::string>& documentHashes = {});

    // Deterministic corpus-wide rebuild of the semantic_neighbor edges in the KG.
    // Clears existing edges then rebuilds against every doc-level vector in vdb.
    // Returns the number of new edges created.
    Result<std::size_t> rebuildSemanticNeighborGraph(const std::string& reason);
    void requestTopologyRebuild(const std::string& reason,
                                const std::vector<std::string>& documentHashes = {});
    TopologyTelemetrySnapshot getTopologyTelemetrySnapshot() const {
        return topologyManager_.getTelemetrySnapshot();
    }
    bool isTopologyRebuildInProgress() const { return topologyManager_.isRebuildInProgress(); }
    TopologyManager& getTopologyManager() { return topologyManager_; }

    void attachWalManager(std::shared_ptr<yams::wal::WALManager> wal) {
        if (databaseManager_) {
            databaseManager_->attachWalManager(std::move(wal));
        }
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
    AsyncInitOrchestrator asyncInit_;

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

    std::size_t getWorkerActive() const { return metricsPublisher_.workerActive(); }
    std::size_t getWorkerPosted() const { return metricsPublisher_.workerPosted(); }
    std::size_t getWorkerCompleted() const { return metricsPublisher_.workerCompleted(); }
    std::size_t getWorkerThreads() const {
        return workCoordinator_ ? workCoordinator_->getWorkerCount() : 0;
    }

    std::size_t getEmbeddingInFlightJobs() const { return embeddingLifecycle_.inFlightJobs(); }
    std::size_t getEmbeddingQueuedJobs() const { return embeddingLifecycle_.queuedJobs(); }
    std::size_t getEmbeddingActiveInferSubBatches() const {
        return embeddingLifecycle_.activeInferSubBatches();
    }
    uint64_t getEmbeddingInferOldestMs() const { return embeddingLifecycle_.inferOldestMs(); }
    uint64_t getEmbeddingInferStartedCount() const {
        return embeddingLifecycle_.inferStartedCount();
    }
    uint64_t getEmbeddingInferCompletedCount() const {
        return embeddingLifecycle_.inferCompletedCount();
    }
    uint64_t getEmbeddingInferLastMs() const { return embeddingLifecycle_.inferLastMs(); }
    uint64_t getEmbeddingInferMaxMs() const { return embeddingLifecycle_.inferMaxMs(); }
    uint64_t getEmbeddingInferWarnCount() const { return embeddingLifecycle_.inferWarnCount(); }
    uint64_t getEmbeddingSemanticEdgesCreated() const {
        return embeddingLifecycle_.semanticEdgesCreated();
    }
    uint64_t getEmbeddingSemanticDocsProcessed() const {
        return embeddingLifecycle_.semanticDocsProcessed();
    }
    uint64_t getEmbeddingSemanticUpdateErrors() const {
        return embeddingLifecycle_.semanticUpdateErrors();
    }

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
        return embeddingLifecycle_.fsmSnapshot();
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
    const StateComponent& getState() const { return state_; }
    const std::filesystem::path& getResolvedDataDir() const { return resolvedDataDir_; }
    std::string getMetadataDatabasePath() const {
        auto db = database_;
        return db ? db->path() : std::string{};
    }
    std::string getVectorDatabasePath() const {
        auto vectorDb = getVectorDatabase();
        return vectorDb ? vectorDb->getConfig().database_path : std::string{};
    }
    Result<bool> adoptModelProviderFromHosts(const std::string& preferredName = "");
    Result<size_t> adoptContentExtractorsFromHosts();
    Result<size_t> adoptSymbolExtractorsFromHosts();
    Result<size_t> adoptEntityExtractorsFromHosts();
    bool isEmbeddingsAutoOnAdd() const { return embeddingLifecycle_.isAutoOnAdd(); }

    boost::asio::awaitable<Result<size_t>> autoloadPluginsNow();
    boost::asio::awaitable<void> preloadPreferredModelIfConfigured();

    std::string resolvePreferredModel() const {
        return embeddingLifecycle_.resolvePreferredModel();
    }

    bool isModelProviderDegraded() const { return embeddingLifecycle_.isDegraded(); }
    std::string lastModelError() const;
    const std::string& adoptedProviderPluginName() const {
        return embeddingLifecycle_.adoptedPluginName();
    }
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
        storeModelProvider(std::move(provider));
    }
    void __test_setMetadataRepo(std::shared_ptr<metadata::MetadataRepository> repo) {
        if (databaseManager_) {
            databaseManager_->setMetadataRepo(std::move(repo));
        }
    }
    void __test_setContentStore(std::shared_ptr<api::IContentStore> store) {
        if (databaseManager_) {
            databaseManager_->setContentStore(std::move(store));
        }
    }
    void __test_setRetrievalSessionManager(std::unique_ptr<RetrievalSessionManager> sessions) {
        retrievalSessions_ = std::move(sessions);
    }
    void __test_setGraphQueryService(
        std::shared_ptr<app::services::IGraphQueryService> graphQueryService) {
        graphQueryServiceOverride_ = std::move(graphQueryService);
    }
    void __test_setAdoptedProviderPluginName(const std::string& name) {
        embeddingLifecycle_.setAdoptedPluginName(name);
    }
    void __test_setModelProviderDegraded(bool degraded, const std::string& error = {});

#ifdef YAMS_TESTING
    void __test_setAbiHost(std::unique_ptr<AbiPluginHost> host) {
        abiHost_ = std::move(host);
        if (pluginManager_) {
            pluginManager_->__test_setSharedPluginHost(abiHost_.get());
        }
    }
    void __test_setExternalPluginHost(std::unique_ptr<ExternalPluginHost> host) {
        if (pluginManager_) {
            pluginManager_->__test_setExternalPluginHost(std::move(host));
        }
    }
    void __test_setCachedSearchEngine(const std::shared_ptr<yams::search::SearchEngine>& engine,
                                      bool vectorEnabled) {
        searchEngineManager_.setEngine(engine, vectorEnabled);
    }
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
    void __test_pluginScanStarted(std::size_t directoryCount) {
        if (pluginManager_) {
            pluginManager_->dispatchPluginScanStarted(directoryCount);
        }
    }
    void __test_pluginLoaded(const std::string& name) {
        if (pluginManager_) {
            pluginManager_->dispatchPluginLoaded(name);
        }
    }
    Result<bool> __test_forceVectorDbInitOnce(const std::filesystem::path& dataDir) {
        if (vectorSystemManager_) {
            return vectorSystemManager_->initializeOnce(dataDir);
        }
        return Result<bool>(false);
    }

private:
    std::shared_ptr<GraphComponent> loadGraphComponent() const {
        return std::atomic_load_explicit(&graphComponent_, std::memory_order_acquire);
    }

    void storeGraphComponent(std::shared_ptr<GraphComponent> component) {
        std::atomic_store_explicit(&graphComponent_, std::move(component),
                                   std::memory_order_release);
    }

    std::shared_ptr<IModelProvider> loadModelProvider() const {
        return std::atomic_load_explicit(&modelProvider_, std::memory_order_acquire);
    }

    void storeModelProvider(std::shared_ptr<IModelProvider> provider) {
        std::atomic_store_explicit(&modelProvider_, std::move(provider), std::memory_order_release);
    }

    size_t getEmbeddingDimension() const { return embeddingLifecycle_.getEmbeddingDimension(); }
    void wireSearchEngineRuntimeAdapters(const std::shared_ptr<search::SearchEngine>& engine,
                                         const char* contextLabel);

    boost::asio::awaitable<void> co_runSessionWatcher(const yams::compat::stop_token& token);

    boost::asio::awaitable<bool> co_openDatabase(const std::filesystem::path& dbPath,
                                                 int timeout_ms, yams::compat::stop_token token);
    boost::asio::awaitable<bool> co_migrateDatabase(int timeout_ms, yams::compat::stop_token token);
    bool detectEmbeddingPreloadFlag() const { return embeddingLifecycle_.detectPreloadFlag(); }

    const DaemonConfig& config_;
    StateComponent& state_;

    // All the services managed by this component
    std::shared_ptr<metadata::Database> database_;
    std::shared_ptr<GraphComponent> graphComponent_;
    std::shared_ptr<app::services::IGraphQueryService> graphQueryServiceOverride_;
    std::shared_ptr<IModelProvider> modelProvider_;

    std::unique_ptr<AbiPluginLoader> abiPluginLoader_;
    std::unique_ptr<AbiPluginHost> abiHost_;
    // NOTE: ExternalPluginHost moved to PluginManager (PBI-093)
    std::unique_ptr<RetrievalSessionManager> retrievalSessions_;
    std::unique_ptr<CheckpointManager> checkpointManager_;
    std::unique_ptr<class BackgroundTaskManager> backgroundTaskManager_;

    SearchAdmissionController searchAdmission_;
    IngestMetricsPublisher metricsPublisher_;

    EmbeddingLifecycleManager embeddingLifecycle_;

    boost::asio::cancellation_signal shutdownSignal_;

    std::unique_ptr<WorkCoordinator> workCoordinator_;
    /// Dedicated thread pool for blocking I/O (database open, migrations).
    /// Kept separate from the event-loop pool so long-running SQLite ops never stall async work.
    std::unique_ptr<boost::asio::thread_pool> blockingPool_;
    std::unique_ptr<IngestService> ingestService_;
    std::unique_ptr<RequestExecutor> requestExecutor_;

    std::optional<boost::asio::strand<boost::asio::any_io_executor>> initStrand_;
    std::optional<boost::asio::strand<boost::asio::any_io_executor>> pluginStrand_;
    std::optional<boost::asio::strand<boost::asio::any_io_executor>> modelStrand_;
    std::filesystem::path resolvedDataDir_;
    std::shared_ptr<yams::integrity::RepairManager> repairManager_;
    std::shared_ptr<PostIngestQueue> postIngest_;
    std::shared_ptr<EmbeddingService> embeddingService_;
    std::unique_ptr<KGWriteQueue> kgWriteQueue_;
    RepairServiceHost repairServiceHost_;
    TopologyManager topologyManager_;
    std::vector<std::shared_ptr<yams::extraction::IContentExtractor>> contentExtractors_;
    std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>> symbolExtractors_;
    TuningConfig tuningConfig_{};

    std::atomic<bool> shutdownInvoked_{false};

    DaemonLifecycleFsm& lifecycleFsm_;

    ServiceManagerFsm serviceFsm_{};

    SearchEngineManager searchEngineManager_;
    std::unique_ptr<SearchComponent> searchComponent_;

    std::unique_ptr<PluginManager> pluginManager_;
    std::unique_ptr<VectorSystemManager> vectorSystemManager_;
    std::shared_ptr<VectorIndexCoordinator> vectorIndexCoordinator_;
    std::unique_ptr<DatabaseManager> databaseManager_;

    // Cached GLiNER query concept extraction function.
    mutable search::EntityExtractionFunc cachedQueryConceptExtractor_;

    std::shared_ptr<PluginStatusSnapshot> pluginStatusSnapshot_{};
};

} // namespace yams::daemon
