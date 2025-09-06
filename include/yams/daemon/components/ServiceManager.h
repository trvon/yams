#pragma once

#include <memory>
#include <mutex>
#include <stop_token>
#include "IComponent.h"
#include <yams/app/services/services.hpp>
#include <yams/compat/thread_stop_compat.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/daemon.h> // For DaemonConfig

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
} // namespace yams::vector
namespace yams::daemon {
class PluginLoader;
class IModelProvider;
class RetrievalSessionManager;
} // namespace yams::daemon

namespace yams::daemon {

class ServiceManager : public IComponent {
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

#ifdef YAMS_TESTING
    // Additional test-only accessors can go here if needed
#endif

private:
    Result<void> initializeAsync(yams::compat::stop_token token);

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
    std::shared_ptr<search::SearchEngineBuilder> searchBuilder_;
    std::shared_ptr<IModelProvider> modelProvider_;
    std::unique_ptr<PluginLoader> pluginLoader_;
    std::unique_ptr<RetrievalSessionManager> retrievalSessions_;

    std::shared_ptr<yams::search::HybridSearchEngine> searchEngine_;
    mutable std::mutex searchEngineMutex_;

    InitCompleteCallback initCompleteCallback_;
    std::filesystem::path resolvedDataDir_;
};

} // namespace yams::daemon
