/**
 * EntityGraphService
 *
 * A single daemon-owned service that executes plugin-based entity/symbol extraction
 * and updates the canonical Knowledge Graph (KG) plus a materialized symbol index
 * for fast grep/search. Provides a small facade API used by post-ingest pipeline
 * and repair flows.
 *
 * Jobs are dispatched to WorkCoordinator's shared thread pool for parallel processing.
 */
#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <yams/core/types.h>

// Forward declarations for C ABI types
struct yams_symbol_extraction_result_v1;

namespace yams {
namespace metadata {
class KnowledgeGraphStore;
class MetadataRepository;
} // namespace metadata
namespace daemon {
class ServiceManager;
class AbiSymbolExtractorAdapter;

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
     * Retroactive scheduling helper (stub). Queues re-extraction across a scope.
     */
    yams::Result<void> reextractRange(const std::string& scope);

    /**
     * Stats snapshot for diagnostics.
     */
    struct Stats {
        std::uint64_t accepted{0};
        std::uint64_t processed{0};
        std::uint64_t failed{0};
    };
    Stats getStats() const;

    /**
     * Materialize derived indices from KG for grep/search (stub; no-op if stores missing).
     */
    yams::Result<void> materializeSymbolIndex();

    /**
     * Minimal read adapters for grep/search (fast path wrappers; stubs for now).
     */
    std::vector<std::string> findSymbolsByName(const std::string& name) const;

private:
    bool process(Job& job);
    // KG population helper: builds rich multi-layered symbol graph
    bool populateKnowledgeGraph(const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg,
                                const Job& job, const yams_symbol_extraction_result_v1* result);

    struct ContextNodes {
        std::optional<std::int64_t> documentNodeId;
        std::optional<std::int64_t> fileNodeId;
        std::optional<std::int64_t> directoryNodeId;
    };

    yams::Result<ContextNodes>
    resolveContextNodes(const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg,
                        const Job& job, std::optional<std::int64_t>& documentDbId);

    yams::Result<std::vector<std::int64_t>>
    createSymbolNodes(const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg,
                      const Job& job, const yams_symbol_extraction_result_v1* result,
                      std::vector<std::string>& outSymbolKeys);

    yams::Result<void> createSymbolEdges(
        const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg, const Job& job,
        const yams_symbol_extraction_result_v1* result, const ContextNodes& contextNodes,
        const std::vector<std::int64_t>& symbolNodeIds, const std::vector<std::string>& symbolKeys);

    yams::Result<void>
    createDocEntities(const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg,
                      std::optional<std::int64_t> documentDbId,
                      const yams_symbol_extraction_result_v1* result,
                      const std::vector<std::int64_t>& symbolNodeIds);

    ServiceManager* services_{};
    std::atomic<bool> stop_{false};
    std::atomic<std::uint64_t> accepted_{0}, processed_{0}, failed_{0};
};

} // namespace daemon
} // namespace yams
