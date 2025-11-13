#pragma once

#include <atomic>
#include <string>
#include <unordered_map>
#include <vector>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <yams/metadata/knowledge_graph_store.h>

namespace yams {
namespace api {
class IContentStore;
}
namespace metadata {
class MetadataRepository;
}
namespace extraction {
class IContentExtractor;
}
namespace vector {
class VectorDatabase;
} // namespace vector
} // namespace yams

namespace yams::daemon {
class IModelProvider;
class WorkCoordinator;

class PostIngestQueue {
public:
    struct Task {
        std::string hash;
        std::string mime;
        std::string session;
        std::chrono::steady_clock::time_point enqueuedAt;
        enum class Stage : uint8_t { Metadata = 0, KnowledgeGraph = 1 } stage;
    };

    PostIngestQueue(std::shared_ptr<api::IContentStore> store,
                    std::shared_ptr<metadata::MetadataRepository> meta,
                    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors,
                    std::shared_ptr<metadata::KnowledgeGraphStore> kg, WorkCoordinator* coordinator,
                    std::size_t capacity = 1000);
    ~PostIngestQueue();

    void start();
    void stop();

    void enqueue(Task t);
    bool tryEnqueue(const Task& t);
    bool tryEnqueue(Task&& t);

    std::size_t size() const;
    std::size_t processed() const { return processed_.load(); }
    std::size_t failed() const { return failed_.load(); }
    double latencyMsEma() const { return latencyMsEma_.load(); }
    double ratePerSecEma() const { return ratePerSecEma_.load(); }
    std::size_t capacity() const { return capacity_; }

    void setCapacity(std::size_t cap) { capacity_ = cap > 0 ? cap : capacity_; }

private:
    boost::asio::awaitable<void> channelPoller();
    boost::asio::awaitable<void> processMetadataStage(const std::string& hash,
                                                      const std::string& mime);
    boost::asio::awaitable<void> processKnowledgeGraphStage(const std::string& hash,
                                                            const std::string& mime);
    boost::asio::awaitable<void> processEmbeddingStage(const std::string& hash,
                                                       const std::string& mime);

    std::shared_ptr<api::IContentStore> store_;
    std::shared_ptr<metadata::MetadataRepository> meta_;
    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors_;
    std::shared_ptr<metadata::KnowledgeGraphStore> kg_;
    WorkCoordinator* coordinator_;
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;

    std::atomic<bool> stop_{false};
    std::atomic<std::size_t> processed_{0};
    std::atomic<std::size_t> failed_{0};
    std::atomic<double> latencyMsEma_{0.0};
    std::atomic<double> ratePerSecEma_{0.0};
    std::size_t capacity_{1000};

    std::chrono::steady_clock::time_point lastCompleteTs_{};
    static constexpr double kAlpha_ = 0.2; // EMA smoothing
};

} // namespace yams::daemon
