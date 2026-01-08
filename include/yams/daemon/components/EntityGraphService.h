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
struct yams_entity_extraction_result_v2;

namespace yams {
namespace metadata {
class KnowledgeGraphStore;
class MetadataRepository;
} // namespace metadata
namespace daemon {
class ServiceManager;
class AbiSymbolExtractorAdapter;
class AbiEntityExtractorAdapter;

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
        std::optional<std::int64_t> pathNodeId;
    };

    struct SymbolNodeBatch {
        std::vector<std::int64_t> canonicalNodeIds;
        std::vector<std::int64_t> versionNodeIds;
        std::vector<std::string> symbolKeys;
    };

    yams::Result<ContextNodes>
    resolveContextNodes(const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg,
                        const Job& job, std::optional<std::int64_t>& documentDbId);

    yams::Result<SymbolNodeBatch>
    createSymbolNodes(const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg,
                      const Job& job, const yams_symbol_extraction_result_v1* result);

    yams::Result<void>
    createSymbolEdges(const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg,
                      const Job& job, const yams_symbol_extraction_result_v1* result,
                      const ContextNodes& contextNodes, const SymbolNodeBatch& nodes);

    yams::Result<void>
    createDocEntities(const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg,
                      std::optional<std::int64_t> documentDbId,
                      const yams_symbol_extraction_result_v1* result,
                      const std::vector<std::int64_t>& symbolNodeIds);

    /**
     * Generate and store entity embeddings for extracted symbols.
     * Creates EntityVectorRecords with SIGNATURE embedding type for semantic search.
     * Non-blocking: skips gracefully if model provider or vector DB is not available.
     */
    yams::Result<void> generateEntityEmbeddings(const Job& job,
                                                const yams_symbol_extraction_result_v1* result,
                                                const SymbolNodeBatch& symbolNodes);

    /**
     * Build text representation for a symbol (for embedding generation).
     * Combines kind, qualified name, parameters, return type, and documentation.
     */
    static std::string buildSymbolText(const yams_symbol_extraction_result_v1* result,
                                       size_t index);

    /**
     * Check if content type should use NL entity extraction instead of code symbol extraction.
     * Returns true for text/plain, text/markdown, application/json, and similar NL content.
     */
    static bool isNaturalLanguageContent(const Job& job);

    /**
     * Process NL content using entity extractor plugins (e.g., Glint for GLiNER).
     * Extracts named entities (person, organization, location, etc.) from text.
     */
    bool processNaturalLanguage(Job& job);

    /**
     * Populate KG with NL entities (person, organization, location, etc.)
     */
    bool populateKnowledgeGraphNL(const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg,
                                  const Job& job, const yams_entity_extraction_result_v2* result,
                                  const AbiEntityExtractorAdapter* adapter);

    /**
     * Create KG nodes for NL entities.
     */
    struct EntityNodeBatch {
        std::vector<std::int64_t> nodeIds;
        std::vector<std::string> entityKeys;
    };

    yams::Result<EntityNodeBatch>
    createEntityNodes(const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg,
                      const Job& job, const yams_entity_extraction_result_v2* result);

    /**
     * Generate embeddings for NL entities.
     */
    yams::Result<void> generateNLEntityEmbeddings(const Job& job,
                                                  const yams_entity_extraction_result_v2* result,
                                                  const EntityNodeBatch& entityNodes);

    ServiceManager* services_{};
    std::atomic<bool> stop_{false};
    std::atomic<std::uint64_t> accepted_{0}, processed_{0}, failed_{0};
};

} // namespace daemon
} // namespace yams
