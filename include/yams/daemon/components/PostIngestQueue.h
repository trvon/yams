#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
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
        std::string session; // optional client/session identifier for fairness (empty => default)
        std::chrono::steady_clock::time_point enqueuedAt;
        enum class Stage : uint8_t { Metadata = 0, KnowledgeGraph = 1, Embeddings = 2 } stage;
    };

    PostIngestQueue(std::shared_ptr<api::IContentStore> store,
                    std::shared_ptr<metadata::MetadataRepository> meta,
                    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors,
                    std::shared_ptr<metadata::KnowledgeGraphStore> kg, std::size_t threads = 2,
                    std::size_t capacity = 1000);
    ~PostIngestQueue();

    void enqueue(Task t);
    // Non-blocking admission: returns false if queue is at capacity (caller may retry/backoff).
    bool tryEnqueue(const Task& t);
    std::size_t size() const;
    std::size_t processed() const { return processed_.load(); }
    std::size_t failed() const { return failed_.load(); }
    std::size_t threads() const { return threads_.size(); }
    double latencyMsEma() const { return latencyMsEma_.load(); }
    double ratePerSecEma() const { return ratePerSecEma_.load(); }
    std::size_t capacity() const { return capacity_; }

    // Dynamically resize the worker thread pool (grow or shrink)
    // Returns true if a change was applied.
    bool resize(std::size_t target);

    // Adaptive tuning hooks
    void setCapacity(std::size_t cap) { capacity_ = cap > 0 ? cap : capacity_; }
    void setWeights(uint32_t wMeta, uint32_t wKg, uint32_t wEmb) {
        std::lock_guard<std::mutex> lk(mtx_);
        wMeta_ = std::max<uint32_t>(1, wMeta);
        wKg_ = std::max<uint32_t>(1, wKg);
        wEmb_ = std::max<uint32_t>(1, wEmb);
        schedCounter_ = 0;
    }
    void setTokenBucket(uint32_t ratePerSec, uint32_t burst) {
        std::lock_guard<std::mutex> lk(mtx_);
        tokenRatePerSec_ = std::max<uint32_t>(1, ratePerSec);
        tokenBurst_ = std::max<uint32_t>(1, burst);
    }
    struct QueueGauges {
        std::size_t queued{0};
        std::size_t inflight{0};
        std::size_t cap{0};
    };
    QueueGauges gauges() const {
        std::lock_guard<std::mutex> lk(mtx_);
        return QueueGauges{qMeta_.size() + qKg_.size() + qEmb_.size(), inflight_.size(), capacity_};
    }

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
    // Multi-queue buffers (front-of-queue fairness respected by WFS scheduler)
    std::deque<Task> qMeta_;
    std::deque<Task> qKg_;
    std::deque<Task> qEmb_;
    std::unordered_set<std::string> inflight_;
    std::size_t capacity_{1000};
    std::atomic<bool> stop_{false};
    std::atomic<std::size_t> processed_{0};
    std::atomic<std::size_t> failed_{0};
    std::atomic<double> latencyMsEma_{0.0};
    std::atomic<double> ratePerSecEma_{0.0};
    std::chrono::steady_clock::time_point lastCompleteTs_{};
    static constexpr double kAlpha_ = 0.2; // EMA smoothing

    // Weighted-fair scheduler
    uint32_t wMeta_{3}, wKg_{2}, wEmb_{1};
    uint32_t schedCounter_{0};
    // Per-session token buckets
    struct Bucket {
        double tokens;
        std::chrono::steady_clock::time_point last;
    };
    std::unordered_map<std::string, Bucket> buckets_;
    uint32_t tokenRatePerSec_{50}; // defaults tuned for small-burst adds
    uint32_t tokenBurst_{100};

    // Internal helpers
    bool admitSessionLocked(const std::string& session);
    bool popNextTaskLocked(Task& out);
};

} // namespace yams::daemon
