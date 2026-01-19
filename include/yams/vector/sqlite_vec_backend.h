#pragma once

#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>
#include <yams/core/types.h>
#include <yams/vector/vector_backend.h>
#include <yams/vector/vector_database.h>

// Forward declarations for sqlite-vec-cpp types
struct sqlite3;

namespace yams::vector {

/**
 * @brief SQLite-vec backend implementation using sqlite-vec-cpp
 *
 * Features:
 * - Unified schema (single table for embeddings + metadata)
 * - HNSW persistence (loads from disk, not rebuilt on startup)
 * - No in-memory ID maps (uses SQLite rowid as HNSW node ID)
 * - Proper read/write locking (std::shared_mutex)
 * - nlohmann::json for metadata serialization
 *
 * Schema:
 *   vectors: rowid, chunk_id, document_hash, embedding, content, metadata, ...
 *   vectors_hnsw_meta: HNSW configuration and entry point
 *   vectors_hnsw_nodes: HNSW graph structure (node + edges)
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
        size_t embedding_dim = 384;        ///< Embedding dimensions
        size_t hnsw_m = 16;                ///< HNSW connections per node
        size_t hnsw_ef_construction = 200; ///< HNSW build exploration factor
        size_t hnsw_ef_search = 100;       ///< HNSW search exploration factor
        size_t checkpoint_threshold = 100; ///< Inserts before HNSW checkpoint
        float compaction_threshold = 0.2f; ///< Compact when >20% deleted
        size_t filter_candidate_chunks_per_doc =
            10; ///< Estimated chunks per document for filtering
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

    Result<std::optional<VectorRecord>> getVector(const std::string& chunk_id) override;
    Result<std::map<std::string, VectorRecord>>
    getVectorsBatch(const std::vector<std::string>& chunk_ids) override;
    Result<std::vector<VectorRecord>>
    getVectorsByDocument(const std::string& document_hash) override;

    Result<bool> hasEmbedding(const std::string& document_hash) override;
    Result<size_t> getVectorCount() override;
    Result<VectorDatabase::DatabaseStats> getStats() override;

    Result<void> buildIndex() override;
    Result<void> optimize() override;

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

    /// No-op for V2 schema (unified table has no rowid sync issues)
    Result<void> ensureEmbeddingRowIdColumn();

    /// No-op for V2 schema (unified table has no orphans)
    Result<OrphanCleanupStats> cleanupOrphanRows();

    /// Explicitly ensure sqlite-vec module is loaded (for doctor command)
    Result<void> ensureVecLoaded();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace yams::vector
