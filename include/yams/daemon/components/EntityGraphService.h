/**
 * EntityGraphService
 *
 * A daemon-owned service that completes the graph stage of knowledge-graph enrichment for
 * post-ingest and repair flows. Code-symbol extraction was removed in v0.20; NL entities are
 * written by the PostIngestQueue title+NL stage.
 *
 * Jobs are routed through InternalEventBus ("entity_graph_jobs" channel) for
 * centralized backpressure and observability, then consumed by a channel poller
 * coroutine running on WorkCoordinator's executor.
 */
#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include <boost/asio/awaitable.hpp>

#include <yams/core/types.h>
#include <yams/daemon/components/knowledge_graph_completion.h>

namespace yams {
namespace daemon {
class ServiceManager;

/**
 * EntityGraphService facade.
 * - Thread-safe submit; background worker consumes jobs.
 */
class EntityGraphService {
public:
    /// Lightweight job descriptor for extraction requests
    struct Job {
        std::string documentHash; ///< SHA256 document hash
        std::string filePath;     ///< Absolute or repo-relative path
        std::string contentUtf8;  ///< UTF-8 document content
        std::string language;     ///< Language hint (e.g., "cpp", "python")
        std::string mimeType;     ///< MIME type for content routing (e.g., "text/plain")
        int64_t documentDbId = 0;
        std::string knowledgeGraphToken;
        std::shared_ptr<KnowledgeGraphCompletion> knowledgeGraphCompletion;
    };

    /**
     * Construct a service bound to the daemon's ServiceManager. Dependencies
     * (plugins, KG, metadata) are resolved lazily at use-time.
     */
    explicit EntityGraphService(ServiceManager* services, std::size_t workers = 1);

    /// Graceful shutdown. Joins worker threads.
    ~EntityGraphService();

    /**
     * Start worker threads (idempotent). Safe to call multiple times.
     */
    void start();

    /**
     * Stop worker threads (idempotent).
     */
    void stop();

    /**
     * Enqueue an extraction job. Returns ok on acceptance.
     */
    yams::Result<void> submitExtraction(Job job);

    /**
     * Stats snapshot for diagnostics.
     */
    struct Stats {
        std::uint64_t accepted{0};
        std::uint64_t processed{0};
        std::uint64_t failed{0};
    };
    Stats getStats() const;

#ifdef YAMS_TESTING
    bool testing_process(Job& job) { return process(job); }
#endif

private:
    bool process(Job& job);
    boost::asio::awaitable<void> channelPoller();

    ServiceManager* services_{};
    std::atomic<bool> stop_{false};
    std::atomic<std::uint64_t> accepted_{0}, processed_{0}, failed_{0};
};

} // namespace daemon
} // namespace yams
