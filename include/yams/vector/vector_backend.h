#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <yams/core/types.h>
#include <yams/vector/vector_database.h>

namespace yams::vector {

/**
 * @brief Abstract interface for vector storage backends
 *
 * This interface allows VectorDatabase to work with different
 * vector storage implementations (sqlite-vec, LanceDB, etc.)
 */
class IVectorBackend {
public:
    virtual ~IVectorBackend() = default;

    /**
     * @brief Initialize the backend storage
     */
    virtual Result<void> initialize(const std::string& db_path) = 0;

    /**
     * @brief Close the backend storage
     */
    virtual void close() = 0;

    /**
     * @brief Check if backend is initialized
     */
    virtual bool isInitialized() const = 0;

    /**
     * @brief Create tables/collections for vector storage
     */
    virtual Result<void> createTables(size_t embedding_dim) = 0;

    /**
     * @brief Check if tables exist
     */
    virtual bool tablesExist() const = 0;

    /**
     * @brief Insert a single vector record
     */
    virtual Result<void> insertVector(const VectorRecord& record) = 0;

    /**
     * @brief Insert multiple vector records in a batch
     */
    virtual Result<void> insertVectorsBatch(const std::vector<VectorRecord>& records) = 0;

    /**
     * @brief Update an existing vector record
     */
    virtual Result<void> updateVector(const std::string& chunk_id, const VectorRecord& record) = 0;

    /**
     * @brief Delete a vector by chunk ID
     */
    virtual Result<void> deleteVector(const std::string& chunk_id) = 0;

    /**
     * @brief Delete all vectors for a document
     */
    virtual Result<void> deleteVectorsByDocument(const std::string& document_hash) = 0;

    /**
     * @brief Search for similar vectors using KNN
     *
     * @param query_embedding The query vector
     * @param k Number of nearest neighbors to return
     * @param similarity_threshold Minimum similarity score
     * @param document_hash Optional filter by document
     * @param metadata_filters Optional metadata key-value filters
     * @return Vector of matching records sorted by similarity
     */
    virtual Result<std::vector<VectorRecord>>
    searchSimilar(const std::vector<float>& query_embedding, size_t k,
                  float similarity_threshold = 0.0f,
                  const std::optional<std::string>& document_hash = std::nullopt,
                  const std::map<std::string, std::string>& metadata_filters = {}) = 0;

    /**
     * @brief Get a specific vector by chunk ID
     */
    virtual Result<std::optional<VectorRecord>> getVector(const std::string& chunk_id) = 0;

    /**
     * @brief Get multiple vectors by chunk IDs (batch lookup)
     * @return Map from chunk_id to VectorRecord for found records
     */
    virtual Result<std::map<std::string, VectorRecord>>
    getVectorsBatch(const std::vector<std::string>& chunk_ids) = 0;

    /**
     * @brief Get all vectors for a document
     */
    virtual Result<std::vector<VectorRecord>>
    getVectorsByDocument(const std::string& document_hash) = 0;

    /**
     * @brief Check if a document has embeddings
     */
    virtual Result<bool> hasEmbedding(const std::string& document_hash) = 0;

    /**
     * @brief Get total number of vectors in storage
     */
    virtual Result<size_t> getVectorCount() = 0;

    /**
     * @brief Get storage statistics
     */
    virtual Result<VectorDatabase::DatabaseStats> getStats() = 0;

    /**
     * @brief Build or rebuild search index
     */
    virtual Result<void> buildIndex() = 0;

    /**
     * @brief Optimize storage and indices
     */
    virtual Result<void> optimize() = 0;

    /**
     * @brief Begin a transaction
     */
    virtual Result<void> beginTransaction() = 0;

    /**
     * @brief Commit the current transaction
     */
    virtual Result<void> commitTransaction() = 0;

    /**
     * @brief Rollback the current transaction
     */
    virtual Result<void> rollbackTransaction() = 0;
};

/**
 * @brief Factory function for creating vector backends
 */
enum class VectorBackendType {
    SqliteVec, // sqlite-vec extension (default)
    LanceDB,   // LanceDB (future)
    InMemory   // In-memory only (testing)
};

std::unique_ptr<IVectorBackend>
createVectorBackend(VectorBackendType type = VectorBackendType::SqliteVec);

} // namespace yams::vector