#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <stop_token>
#include "IComponent.h"
#include <boost/asio/any_io_executor.hpp>
#include <yams/app/services/services.hpp>
#include <yams/compat/thread_stop_compat.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/WalMetricsProvider.h>
#include <yams/daemon/daemon.h> // For DaemonConfig
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
} // namespace yams::daemon

namespace yams::daemon {

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
    std::shared_ptr<vector::VectorDatabase> getVectorDatabase() const { return vectorDatabase_; }
    std::shared_ptr<WorkerPool> getWorkerPool() const { return workerPool_; }
    boost::asio::any_io_executor getWorkerExecutor() const;
    std::function<void(bool)> getWorkerJobSignal();
    // Best-effort queue depth estimation for backpressure/telemetry
    std::size_t getWorkerQueueDepth() const;
    const std::vector<std::shared_ptr<yams::extraction::IContentExtractor>>&
    getContentExtractors() const {
        return contentExtractors_;
    }

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

    // Combined embedding initialization and search engine rebuild coroutine.
    // This method provides idempotent, race-safe initialization of embedding capabilities
    // followed by search engine rebuild to enable vector search. Uses atomic guards to
    // prevent duplicate execution and ensures proper lifetime management.
    // Should be called via co_spawn with shared_from_this() for safety.
    boost::asio::awaitable<void> co_enableEmbeddingsAndRebuild();

    // Check if embedding initialization has started
    bool isEmbeddingInitStarted() const {
        return embedInitStarted_.load(std::memory_order_acquire);
    }

    // Check if embedding initialization has completed
    bool isEmbeddingInitCompleted() const {
        return embedInitCompleted_.load(std::memory_order_acquire);
    }

    // Check if search engine rebuild is in progress
    bool isRebuildInProgress() const { return rebuildInProgress_.load(std::memory_order_acquire); }

    // Coroutine-based initialization wrapper (awaitable). Uses the same phase logic as
    // initialization but integrates with Boost.Asio coroutine flow.
    boost::asio::awaitable<Result<void>> initializeAsyncAwaitable(yams::compat::stop_token token);

#ifdef YAMS_TESTING
    // Additional test-only accessors can go here if needed
#endif

private:
    // Awaitable phase helpers (coroutine-based)
    boost::asio::awaitable<bool> co_openDatabase(const std::filesystem::path& dbPath,
                                                 int timeout_ms, yams::compat::stop_token token);
    boost::asio::awaitable<bool> co_migrateDatabase(int timeout_ms, yams::compat::stop_token token);
    boost::asio::awaitable<std::shared_ptr<yams::search::HybridSearchEngine>>
    co_buildEngine(int timeout_ms, yams::compat::stop_token token);

    const DaemonConfig& config_;
    StateComponent& state_;
    yams::compat::jthread initThread_;

    // All the services managed by this component
    std::shared_ptr<api::IContentStore> contentStore_;
    std::shared_ptr<metadata::Database> database_;
    std::shared_ptr<metadata::ConnectionPool> connectionPool_;
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<search::SearchExecutor> searchExecutor_;
    std::shared_ptr<vector::VectorIndexManager> vectorIndexManager_;
    std::shared_ptr<vector::EmbeddingGenerator> embeddingGenerator_;
    std::shared_ptr<vector::VectorDatabase> vectorDatabase_;
    std::shared_ptr<search::SearchEngineBuilder> searchBuilder_;
    std::shared_ptr<IModelProvider> modelProvider_;
    std::unique_ptr<PluginLoader> pluginLoader_;
    std::unique_ptr<AbiPluginLoader> abiPluginLoader_;
    std::unique_ptr<AbiPluginHost> abiHost_;
    std::unique_ptr<WasmPluginHost> wasmHost_;
    std::unique_ptr<ExternalPluginHost> externalHost_;
    std::unique_ptr<RetrievalSessionManager> retrievalSessions_;
    std::shared_ptr<WorkerPool> workerPool_;
    // Phase 6: background reconciler for PoolManager stats (logging only)
    yams::compat::jthread poolReconThread_;
    // Worker pool metrics
    std::atomic<std::size_t> poolActive_{0};
    std::atomic<std::size_t> poolPosted_{0};
    std::atomic<std::size_t> poolCompleted_{0};
    std::size_t poolThreads_{0};

    std::shared_ptr<yams::search::HybridSearchEngine> searchEngine_;
    mutable std::mutex searchEngineMutex_;

    InitCompleteCallback initCompleteCallback_;
    std::filesystem::path resolvedDataDir_;

    std::shared_ptr<WalMetricsProvider> walMetricsProvider_;
    std::vector<std::shared_ptr<yams::extraction::IContentExtractor>> contentExtractors_;
    bool embeddingsAutoOnAdd_{false};
    // Prevent duplicate plugin autoload scheduling
    std::atomic<bool> pluginsAutoloadScheduled_{false};

    // Atomic guards for idempotent operations
    std::atomic<bool> embedInitStarted_{false};
    std::atomic<bool> embedInitCompleted_{false};
    std::atomic<bool> rebuildInProgress_{false};
    std::atomic<bool> preferredPreloadStarted_{false};
};

} // namespace yams::daemon
