#pragma once

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <yams/core/types.h>
#include <yams/vector/vector_types.h>

namespace yams::vector {

/**
 * @brief Narrow interface for vector storage CRUD, search, and lifecycle.
 *
 * Separated from the monolithic IVectorBackend so backends can implement
 * only the surfaces they actually support. Index lifecycle, entity vectors,
 * and quantizer persistence each have their own narrow contracts.
 */
class IVectorStore {
public:
    virtual ~IVectorStore() = default;

    // ---- lifecycle --------------------------------------------------------
    virtual Result<void> initialize(const std::string& db_path) = 0;
    virtual void close() = 0;
    virtual bool isInitialized() const = 0;

    // ---- schema -----------------------------------------------------------
    virtual Result<void> createTables(size_t embedding_dim) = 0;
    virtual bool tablesExist() const = 0;

    // ---- CRUD -------------------------------------------------------------
    virtual Result<void> insertVector(const VectorRecord& record) = 0;
    virtual Result<void> insertVectorsBatch(const std::vector<VectorRecord>& records) = 0;
    virtual Result<void> updateVector(const std::string& chunk_id, const VectorRecord& record) = 0;
    virtual Result<void> deleteVector(const std::string& chunk_id) = 0;
    virtual Result<void> deleteVectorsByDocument(const std::string& document_hash) = 0;

    // ---- search -----------------------------------------------------------
    virtual Result<std::vector<VectorRecord>>
    searchSimilar(const std::vector<float>& query_embedding, size_t k,
                  float similarity_threshold = 0.0f,
                  const std::optional<std::string>& document_hash = std::nullopt,
                  const std::unordered_set<std::string>& candidate_hashes = {},
                  const std::map<std::string, std::string>& metadata_filters = {}) = 0;

    virtual Result<std::vector<std::vector<VectorRecord>>>
    searchSimilarBatch(const std::vector<std::vector<float>>& query_embeddings, size_t k,
                       float similarity_threshold = 0.0f, size_t num_threads = 0) = 0;

    // ---- retrieval --------------------------------------------------------
    virtual Result<std::optional<VectorRecord>> getVector(const std::string& chunk_id) = 0;
    virtual Result<std::map<std::string, VectorRecord>>
    getVectorsBatch(const std::vector<std::string>& chunk_ids) = 0;
    virtual Result<std::vector<VectorRecord>>
    getVectorsByDocument(const std::string& document_hash) = 0;
    virtual Result<std::unordered_map<std::string, VectorRecord>> getDocumentLevelVectorsAll() = 0;
    virtual Result<size_t>
    forEachDocumentLevelVector(const std::function<bool(VectorRecord&&)>& visitor) = 0;

    // ---- existence checks -------------------------------------------------
    virtual Result<bool> hasEmbedding(const std::string& document_hash) = 0;
    virtual Result<std::unordered_set<std::string>> getEmbeddedDocumentHashes() = 0;

    // ---- stats ------------------------------------------------------------
    virtual Result<size_t> getVectorCount() = 0;
    virtual Result<VectorDatabaseStats> getStats() = 0;

    // ---- transactions -----------------------------------------------------
    virtual Result<void> beginTransaction() = 0;
    virtual Result<void> commitTransaction() = 0;
    virtual Result<void> rollbackTransaction() = 0;
};

/// Optional per-call diagnostics seam. Backends implement this when they can
/// report work without global counters or cross-request races.
class IDiagnosticVectorStore {
public:
    virtual ~IDiagnosticVectorStore() = default;
    virtual Result<std::vector<VectorRecord>> searchSimilarWithDiagnostics(
        const std::vector<float>& query_embedding, size_t k, float similarity_threshold,
        const std::optional<std::string>& document_hash,
        const std::unordered_set<std::string>& candidate_hashes,
        const std::map<std::string, std::string>& metadata_filters,
        VectorSearchDiagnostics& diagnostics) = 0;
};

} // namespace yams::vector
