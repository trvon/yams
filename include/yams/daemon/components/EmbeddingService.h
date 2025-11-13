#pragma once

#include <atomic>
#include <memory>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
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
class WorkCoordinator;

class EmbeddingService : public IComponent {
public:
    EmbeddingService(std::shared_ptr<api::IContentStore> store,
                     std::shared_ptr<metadata::MetadataRepository> meta,
                     WorkCoordinator* coordinator);
    ~EmbeddingService() override;

    const char* getName() const override { return "EmbeddingService"; }
    Result<void> initialize() override;
    void shutdown() override;

    void setProviders(std::function<std::shared_ptr<IModelProvider>()> providerGetter,
                      std::function<std::string()> modelNameGetter,
                      std::function<std::shared_ptr<yams::vector::VectorDatabase>()> dbGetter);

    std::size_t processed() const { return processed_.load(); }
    std::size_t failed() const { return failed_.load(); }
    std::size_t queuedJobs() const;

    void start();

private:
    boost::asio::awaitable<void> channelPoller();
    boost::asio::awaitable<void> processEmbedJob(const InternalEventBus::EmbedJob& job);

    std::shared_ptr<api::IContentStore> store_;
    std::shared_ptr<metadata::MetadataRepository> meta_;
    WorkCoordinator* coordinator_;
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;

    std::function<std::shared_ptr<IModelProvider>()> getModelProvider_;
    std::function<std::string()> getPreferredModel_;
    std::function<std::shared_ptr<yams::vector::VectorDatabase>()> getVectorDatabase_;

    std::atomic<bool> stop_{false};
    std::atomic<std::size_t> processed_{0};
    std::atomic<std::size_t> failed_{0};
    std::shared_ptr<SpscQueue<InternalEventBus::EmbedJob>> embedChannel_;
};

} // namespace daemon
} // namespace yams
