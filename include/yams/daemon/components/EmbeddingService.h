#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include <yams/daemon/components/IComponent.h>
#include <yams/daemon/components/InternalEventBus.h>

namespace yams {
namespace api {
class IContentStore;
}
namespace metadata {
class MetadataRepository;
}
namespace vector {
class VectorDatabase;
}
namespace daemon {

class IModelProvider;

/**
 * @brief Dedicated service for asynchronous embedding generation
 *
 * Consumes EmbedJob from InternalBus and generates embeddings without blocking
 * the PostIngestQueue workers. This architectural separation ensures:
 * - Documents become searchable immediately after FTS5 indexing
 * - Embedding generation happens asynchronously in background
 * - Better resource isolation and independent tuning
 * - No blocking of ingest pipeline on slow embedding models
 */
class EmbeddingService : public IComponent {
public:
    EmbeddingService(std::shared_ptr<api::IContentStore> store,
                     std::shared_ptr<metadata::MetadataRepository> meta, std::size_t threads = 2);
    ~EmbeddingService() override;

    // IComponent interface
    const char* getName() const override { return "EmbeddingService"; }
    Result<void> initialize() override;
    void shutdown() override;

    // Configure providers (called by ServiceManager during initialization)
    void setProviders(std::function<std::shared_ptr<IModelProvider>()> providerGetter,
                      std::function<std::string()> modelNameGetter,
                      std::function<std::shared_ptr<yams::vector::VectorDatabase>()> dbGetter);

    // Metrics
    std::size_t processed() const { return processed_.load(); }
    std::size_t failed() const { return failed_.load(); }
    std::size_t queuedJobs() const;

private:
    void workerLoop();
    void processEmbedJob(const InternalEventBus::EmbedJob& job);

    std::shared_ptr<api::IContentStore> store_;
    std::shared_ptr<metadata::MetadataRepository> meta_;

    // Provider getters (set by ServiceManager)
    std::function<std::shared_ptr<IModelProvider>()> getModelProvider_;
    std::function<std::string()> getPreferredModel_;
    std::function<std::shared_ptr<yams::vector::VectorDatabase>()> getVectorDatabase_;

    // Worker threads
    std::vector<std::thread> workers_;
    std::atomic<bool> stop_{false};

    // Metrics
    std::atomic<std::size_t> processed_{0};
    std::atomic<std::size_t> failed_{0};

    // Channel for consuming EmbedJob from InternalBus
    std::shared_ptr<SpscQueue<InternalEventBus::EmbedJob>> embedChannel_;
};

} // namespace daemon
} // namespace yams
