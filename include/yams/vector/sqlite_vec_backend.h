#pragma once

#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>
#include <yams/core/types.h>
#include <yams/vector/vector_backend.h>

// Forward declarations for sqlite-vec-cpp types
struct sqlite3;

namespace yams::vector {

/**
 * @brief SQLite-vec backend implementation using sqlite-vec-cpp
 *
 * Search engines: SimeonPqAdc (default, Product-Quantized ADC) or Vec0L2
 * (sqlite-vec native vec0 virtual table with L2 distance). HNSW has been
 * removed from the daemon data plane.
 *
 * Schema:
 *   vectors: rowid, chunk_id, document_hash, embedding, content, metadata, ...
 *   vectors_simeon_pq_*: PQ codebook/codes for SimeonPqAdc.
 *   vectors_vec0_*: sqlite-vec virtual-table shadow tables for Vec0L2.
 */
class SqliteVecBackend : public IVectorBackend {
public:
    /// Statistics for orphan cleanup operations
    struct OrphanCleanupStats {
        std::size_t metadata_removed = 0;
        std::size_t embeddings_removed = 0;
        std::size_t metadata_backfilled = 0;
    };

    /// Configuration for backend
    struct Config {
        size_t embedding_dim = 1024;       ///< Embedding dimensions
        float compaction_threshold = 0.2f; ///< Compact when >20% deleted
        size_t filter_candidate_chunks_per_doc =
            10; ///< Estimated chunks per document for filtering
        // TurboQuant settings (mirrored from VectorDatabaseConfig)
        bool enable_turboquant_storage = false;
        bool quantized_primary_storage =
            false; ///< Store quantized sidecar only; reconstruct floats on read
        uint8_t turboquant_bits = 4;
        uint64_t turboquant_seed = 42;
        VectorSearchEngine search_engine = VectorSearchEngine::SimeonPqAdc;
        size_t simeon_pq_subquantizers = 32;
        size_t simeon_pq_centroids = 256;
        size_t simeon_pq_train_limit = 4096;
        size_t simeon_pq_rerank_factor = 2;
        uint64_t simeon_pq_seed = 0xC0FFEE5EED5EEDC0ULL;
    };

    SqliteVecBackend();
    explicit SqliteVecBackend(const Config& config);
    ~SqliteVecBackend() override;

    // Non-copyable, movable
    SqliteVecBackend(const SqliteVecBackend&) = delete;
    SqliteVecBackend& operator=(const SqliteVecBackend&) = delete;
    SqliteVecBackend(SqliteVecBackend&&) noexcept;
    SqliteVecBackend& operator=(SqliteVecBackend&&) noexcept;

    // ==================== IVectorBackend Interface ====================

    Result<void> initialize(const std::string& db_path) override;
    void close() override;
    bool isInitialized() const override;

    /// Get raw SQLite handle (for migration/testing only)
    sqlite3* getDbHandle() const;

    Result<void> createTables(size_t embedding_dim) override;
    bool tablesExist() const override;

    Result<void> insertVector(const VectorRecord& record) override;
    Result<void> insertVectorsBatch(const std::vector<VectorRecord>& records) override;
    Result<void> updateVector(const std::string& chunk_id, const VectorRecord& record) override;

    Result<void> deleteVector(const std::string& chunk_id) override;
    Result<void> deleteVectorsByDocument(const std::string& document_hash) override;

    Result<std::vector<VectorRecord>>
    searchSimilar(const std::vector<float>& query_embedding, size_t k,
                  float similarity_threshold = 0.0f,
                  const std::optional<std::string>& document_hash = std::nullopt,
                  const std::unordered_set<std::string>& candidate_hashes = {},
                  const std::map<std::string, std::string>& metadata_filters = {}) override;

    Result<std::vector<std::vector<VectorRecord>>>
    searchSimilarBatch(const std::vector<std::vector<float>>& query_embeddings, size_t k,
                       float similarity_threshold = 0.0f, size_t num_threads = 0) override;

    Result<std::optional<VectorRecord>> getVector(const std::string& chunk_id) override;
    Result<std::map<std::string, VectorRecord>>
    getVectorsBatch(const std::vector<std::string>& chunk_ids) override;
    Result<std::vector<VectorRecord>>
    getVectorsByDocument(const std::string& document_hash) override;
    Result<std::unordered_map<std::string, VectorRecord>> getDocumentLevelVectorsAll() override;

    Result<bool> hasEmbedding(const std::string& document_hash) override;
    Result<std::unordered_set<std::string>> getEmbeddedDocumentHashes() override;
    Result<size_t> getVectorCount() override;
    Result<VectorDatabaseStats> getStats() override;

    Result<void> buildIndex() override;
    Result<void> prepareSearchIndex() override;
    Result<bool> hasReusablePersistedSearchIndex() override;
    Result<void> optimize() override;
    Result<void> persistIndex() override;

    Result<void> beginTransaction() override;
    Result<void> commitTransaction() override;
    Result<void> rollbackTransaction() override;

    // ==================== Entity Vector Operations ====================

    Result<void> insertEntityVector(const EntityVectorRecord& record) override;
    Result<void> insertEntityVectorsBatch(const std::vector<EntityVectorRecord>& records) override;
    Result<void> deleteEntityVectorsByNode(const std::string& node_key) override;
    Result<void> deleteEntityVectorsByDocument(const std::string& document_hash) override;
    std::vector<EntityVectorRecord> searchEntities(const std::vector<float>& query_embedding,
                                                   const EntitySearchParams& params = {}) override;
    std::vector<EntityVectorRecord> getEntityVectorsByNode(const std::string& node_key) override;
    std::vector<EntityVectorRecord>
    getEntityVectorsByDocument(const std::string& document_hash) override;
    bool hasEntityEmbedding(const std::string& node_key) override;
    size_t getEntityVectorCount() override;
    Result<void> markEntityAsStale(const std::string& node_key) override;

    // ==================== Backend-specific helpers ====================

    /// Return the embedding dimension from config/schema
    std::optional<size_t> getStoredEmbeddingDimension() const;

    /// Drop vector tables (safe when reinitializing)
    Result<void> dropTables();

    /// Checkpoint vectors.db WAL to reclaim disk space
    Result<void> checkpointWal();

    /// Defer index load/build/checkpoint work during bulk vector regeneration.
    Result<void> beginBulkLoad();

    /// Finish deferred work after bulk vector regeneration and persist the index.
    Result<void> finalizeBulkLoad();

    /// No-op for V2 schema (unified table has no rowid sync issues)
    Result<void> ensureEmbeddingRowIdColumn();

    /// No-op for V2 schema (unified table has no orphans)
    Result<OrphanCleanupStats> cleanupOrphanRows();

    /// Explicitly ensure sqlite-vec module is loaded (for doctor command)
    Result<void> ensureVecLoaded();

    Result<void> persistTurboQuantPerCoordScales(size_t dim, uint8_t bits, uint64_t seed,
                                                 const std::vector<float>& scales) override;

    Result<void> persistTurboQuantFittedModel(size_t dim, uint8_t bits, uint64_t seed,
                                              const std::vector<float>& scales,
                                              const std::vector<float>& centroids) override;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace yams::vector
