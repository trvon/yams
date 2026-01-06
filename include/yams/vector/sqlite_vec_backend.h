#pragma once

#include <sqlite3.h>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <yams/profiling.h>
#include <yams/vector/vector_backend.h>

namespace yams::vector {

/**
 * @brief SQLite-vec backend implementation for vector storage
 *
 * Uses the sqlite-vec extension for efficient vector similarity search
 * within SQLite databases.
 */
class SqliteVecBackend : public IVectorBackend {
public:
    struct OrphanCleanupStats {
        std::size_t metadata_removed = 0;
        std::size_t embeddings_removed = 0;
        std::size_t metadata_backfilled = 0;
    };
    SqliteVecBackend();
    ~SqliteVecBackend() override;

    // Disable copy but allow move
    SqliteVecBackend(const SqliteVecBackend&) = delete;
    SqliteVecBackend& operator=(const SqliteVecBackend&) = delete;
    SqliteVecBackend(SqliteVecBackend&&) noexcept;
    SqliteVecBackend& operator=(SqliteVecBackend&&) noexcept;

    // IVectorBackend implementation
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

    // Backend-specific helpers (used opportunistically by higher layers)
    // Return the embedding dimension encoded in the vec0 table DDL, if present.
    std::optional<size_t> getStoredEmbeddingDimension() const;
    // Drop vector and metadata tables (safe when reinitializing an empty DB/schema).
    Result<void> dropTables();
    // Ensure doc_metadata has embedding_rowid column populated and indexed.
    Result<void> ensureEmbeddingRowIdColumn();
    // Remove orphaned metadata/embedding rows (rowid mismatches).
    Result<OrphanCleanupStats> cleanupOrphanRows();

    // Explicitly ensure sqlite-vec module is loaded (doctor can call separately)
    Result<void> ensureVecLoaded();

private:
    /**
     * @brief Internal insert method that optionally manages transactions
     */
    Result<void> insertVectorInternal(const VectorRecord& record, bool manage_transaction);

    // Validate embedding dimension against known schema dimension when available
    Result<void> validateEmbeddingDim(size_t actual_dim) const;

    /**
     * @brief Load sqlite-vec extension into the database
     */
    Result<void> loadSqliteVecExtension();

    /**
     * @brief Prepare SQL statements for common operations
     */
    Result<void> prepareStatements();

    /**
     * @brief Clean up prepared statements
     */
    void finalizeStatements();

    /**
     * @brief Convert float vector to blob for storage
     */
    std::vector<uint8_t> vectorToBlob(const std::vector<float>& vec) const;

    /**
     * @brief Convert blob back to float vector
     */
    std::vector<float> blobToVector(const void* blob, size_t size) const;

    /**
     * @brief Execute a SQL statement with error handling
     */
    Result<void> executeSQL(const std::string& sql);

    /**
     * @brief Get last error message from SQLite
     */
    std::string getLastError() const;

private:
    sqlite3* db_;
    std::string db_path_;
    size_t embedding_dim_;
    bool initialized_;
    bool in_transaction_;
    mutable YAMS_SHARED_LOCKABLE(std::shared_mutex, mutex_);

    // Prepared statements for performance
    struct Statements {
        sqlite3_stmt* insert_vector = nullptr;
        sqlite3_stmt* select_vector = nullptr;
        sqlite3_stmt* update_vector = nullptr;
        sqlite3_stmt* delete_vector = nullptr;
        sqlite3_stmt* search_similar = nullptr;
        sqlite3_stmt* has_embedding = nullptr;
        sqlite3_stmt* count_vectors = nullptr;
    } stmts_;
};

} // namespace yams::vector
