#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>
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
} // namespace yams

namespace yams::daemon {

class PostIngestQueue {
public:
    struct Task {
        std::string hash;
        std::string mime;
        std::chrono::steady_clock::time_point enqueuedAt;
    };

    PostIngestQueue(std::shared_ptr<api::IContentStore> store,
                    std::shared_ptr<metadata::MetadataRepository> meta,
                    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors,
                    std::shared_ptr<metadata::KnowledgeGraphStore> kg, std::size_t threads = 2,
                    std::size_t capacity = 1000);
    ~PostIngestQueue();

    void enqueue(Task t);
    std::size_t size() const;
    std::size_t processed() const { return processed_.load(); }
    std::size_t failed() const { return failed_.load(); }
    std::size_t threads() const { return threads_.size(); }
    double latencyMsEma() const { return latencyMsEma_.load(); }
    double ratePerSecEma() const { return ratePerSecEma_.load(); }

    // Dynamically resize the worker thread pool (grow or shrink)
    // Returns true if a change was applied.
    bool resize(std::size_t target);

private:
    void workerLoop();

    std::shared_ptr<api::IContentStore> store_;
    std::shared_ptr<metadata::MetadataRepository> meta_;
    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors_;
    std::shared_ptr<metadata::KnowledgeGraphStore> kg_;
    struct Worker {
        std::thread th;
        std::shared_ptr<std::atomic<bool>> exit;
    };
    std::vector<Worker> threads_;
    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::queue<Task> q_;
    std::unordered_set<std::string> inflight_;
    std::size_t capacity_{1000};
    std::atomic<bool> stop_{false};
    std::atomic<std::size_t> processed_{0};
    std::atomic<std::size_t> failed_{0};
    std::atomic<double> latencyMsEma_{0.0};
    std::atomic<double> ratePerSecEma_{0.0};
    std::chrono::steady_clock::time_point lastCompleteTs_{};
    static constexpr double kAlpha_ = 0.2; // EMA smoothing
};

} // namespace yams::daemon
