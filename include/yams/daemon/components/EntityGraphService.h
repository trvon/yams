/**
 * EntityGraphService
 *
 * A single daemon-owned service that executes plugin-based entity/symbol extraction
 * and updates the canonical Knowledge Graph (KG) plus a materialized symbol index
 * for fast grep/search. Provides a small facade API used by post-ingest pipeline
 * and repair flows.
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
#include <yams/metadata/knowledge_graph_store.h>

// Forward declarations for C ABI types
struct yams_symbol_extraction_result_v1;
struct yams_entity_extraction_result_v2;

namespace yams {
namespace daemon {
class ServiceManager;
class AbiSymbolExtractorAdapter;
class AbiEntityExtractorAdapter;
class KGWriteQueue;

/**
 * EntityGraphService facade.
 * - Thread-safe submit; background worker consumes jobs.
 * - Uses symbol extractor plugins when available; otherwise no-ops safely.
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


private:
    bool process(Job& job);
    boost::asio::awaitable<void> channelPoller();
    // KG population helper: builds rich multi-layered symbol graph
    bool populateKnowledgeGraph(const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg,
                                const Job& job, const yams_symbol_extraction_result_v1* result);

    /**
     * Deferred symbol KG population using batched write queue.
     * Eliminates lock contention by queuing writes instead of immediate commits.
     */
    bool
    populateKnowledgeGraphDeferred(const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg,
                                   const Job& job, const yams_symbol_extraction_result_v1* result,
                                   KGWriteQueue* kgQueue);

    /**
     * Check if content type should use NL entity extraction instead of code symbol extraction.
     * Returns true for text/plain, text/markdown, application/json, and similar NL content.
     */
    static bool isNaturalLanguageContent(const Job& job);

    ServiceManager* services_{};
    std::atomic<bool> stop_{false};
    std::atomic<std::uint64_t> accepted_{0}, processed_{0}, failed_{0};
};

} // namespace daemon
} // namespace yams
