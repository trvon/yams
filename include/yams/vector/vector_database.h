#pragma once

#include <yams/core/concepts.h>
#include <yams/core/types.h>

#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace yams::vector {

/**
 * Configuration for the vector database
 */
struct VectorDatabaseConfig {
    std::string database_path = "vectors.db"; // SQLite database path
    std::string table_name = "document_embeddings";
    size_t embedding_dim = 384; // all-MiniLM-L6-v2 dimensions
    std::string index_type = "IVF_PQ";
    size_t num_partitions = 256;    // For IVF index
    size_t num_sub_quantizers = 96; // For PQ
    bool enable_checkpoints = true;
    size_t checkpoint_frequency = 1000; // Operations between checkpoints
    size_t max_batch_size = 1000;
    float default_similarity_threshold = 0.7f;
    bool use_in_memory = false; // For testing
};

/**
 * Represents a vector record in the database
 */
struct VectorRecord {
    std::string chunk_id;                        // UUID for chunk
    std::string document_hash;                   // SHA-256 hash of source document
    std::vector<float> embedding;                // Embedding vector
    std::string content;                         // Original text chunk
    size_t start_offset = 0;                     // Character offset in document
    size_t end_offset = 0;                       // End character offset
    std::map<std::string, std::string> metadata; // Document metadata
    std::chrono::system_clock::time_point created_at;
    float relevance_score = 0.0f; // For search results

    // Embedding versioning fields
    std::string model_id;                  // Model identifier (e.g., "all-MiniLM-L6-v2")
    std::string model_version;             // Model version (e.g., "1.0.0")
    uint32_t embedding_version = 1;        // Schema version for embeddings
    std::string content_hash_at_embedding; // SHA-256 of content when embedded
    std::chrono::system_clock::time_point embedded_at; // When embedding was generated
    bool is_stale = false;                             // Mark for re-embedding

    // Default constructor
    VectorRecord() = default;

    // Constructor with basic fields
    VectorRecord(std::string chunk_id, std::string document_hash, std::vector<float> embedding,
                 std::string content)
        : chunk_id(std::move(chunk_id)), document_hash(std::move(document_hash)),
          embedding(std::move(embedding)), content(std::move(content)),
          created_at(std::chrono::system_clock::now()) {}

    // Copy and move constructors
    VectorRecord(const VectorRecord&) = default;
    VectorRecord(VectorRecord&&) = default;
    VectorRecord& operator=(const VectorRecord&) = default;
    VectorRecord& operator=(VectorRecord&&) = default;
};

/**
 * Search parameters for vector similarity queries
 */
struct VectorSearchParams {
    size_t k = 10;                                       // Number of results to return
    float similarity_threshold = 0.7f;                   // Minimum similarity score
    std::optional<std::string> document_hash;            // Filter by document
    std::map<std::string, std::string> metadata_filters; // Metadata filters
    bool include_embeddings = false;                     // Include embedding vectors in results
};

/**
 * Vector database interface for LanceDB operations
 */
class VectorDatabase {
public:
    explicit VectorDatabase(const VectorDatabaseConfig& config = {});
    ~VectorDatabase();

    // Non-copyable but movable
    VectorDatabase(const VectorDatabase&) = delete;
    VectorDatabase& operator=(const VectorDatabase&) = delete;
    VectorDatabase(VectorDatabase&&) noexcept;
    VectorDatabase& operator=(VectorDatabase&&) noexcept;

    // Initialization and management
    bool initialize();
    bool isInitialized() const;
    void close();

    // Table management
    bool createTable();
    bool tableExists() const;
    void dropTable();
    size_t getVectorCount() const;

    // Vector operations
    bool insertVector(const VectorRecord& record);
    bool insertVectorsBatch(const std::vector<VectorRecord>& records);
    bool updateVector(const std::string& chunk_id, const VectorRecord& record);
    bool deleteVector(const std::string& chunk_id);
    bool deleteVectorsByDocument(const std::string& document_hash);

    // Search operations
    std::vector<VectorRecord> searchSimilar(const std::vector<float>& query_embedding,
                                            const VectorSearchParams& params = {}) const;

    std::vector<VectorRecord> searchSimilarToDocument(const std::string& document_hash,
                                                      const VectorSearchParams& params = {}) const;

    // Utility operations
    std::optional<VectorRecord> getVector(const std::string& chunk_id) const;
    std::vector<VectorRecord> getVectorsByDocument(const std::string& document_hash) const;
    bool hasEmbedding(const std::string& document_hash) const;

    // Index management
    bool buildIndex();
    bool optimizeIndex();
    void compactDatabase();
    bool rebuildIndex();

    // Statistics and monitoring
    struct DatabaseStats {
        size_t total_vectors = 0;
        size_t total_documents = 0;
        double avg_embedding_magnitude = 0.0;
        size_t index_size_bytes = 0;
        std::chrono::system_clock::time_point last_optimized;
    };

    DatabaseStats getStats() const;

    // Embedding lifecycle operations
    Result<void> updateEmbeddings(const std::vector<VectorRecord>& records);
    Result<std::vector<std::string>> getStaleEmbeddings(const std::string& model_id,
                                                        const std::string& model_version);
    Result<std::vector<VectorRecord>> getEmbeddingsByVersion(const std::string& model_version,
                                                             size_t limit = 1000);
    Result<void> markAsStale(const std::string& chunk_id);
    Result<void> markAsDeleted(const std::string& chunk_id);
    Result<size_t> purgeDeleted(std::chrono::hours age_threshold);

    // Configuration and error handling
    const VectorDatabaseConfig& getConfig() const;
    std::string getLastError() const;
    bool hasError() const;

    // Validation
    static bool isValidEmbedding(const std::vector<float>& embedding, size_t expected_dim);
    static double computeCosineSimilarity(const std::vector<float>& a, const std::vector<float>& b);

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * Factory function for creating vector database instances
 */
std::unique_ptr<VectorDatabase> createVectorDatabase(const VectorDatabaseConfig& config = {});

/**
 * Utility functions for vector operations
 */
namespace utils {
/**
 * Normalize a vector to unit length
 */
std::vector<float> normalizeVector(const std::vector<float>& vec);

/**
 * Generate a unique chunk ID
 */
std::string generateChunkId(const std::string& document_hash, size_t chunk_index);

/**
 * Validate vector record integrity
 */
bool validateVectorRecord(const VectorRecord& record, size_t expected_dim);

/**
 * Convert similarity score to distance metric
 */
double similarityToDistance(double similarity);

/**
 * Convert distance metric to similarity score
 */
double distanceToSimilarity(double distance);
} // namespace utils

} // namespace yams::vector