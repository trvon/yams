#pragma once

#include <yams/core/concepts.h>
#include <yams/core/types.h>

#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

namespace yams::vector {

enum class EmbeddingLevel { CHUNK, DOCUMENT };

/**
 * Type of entity embedding content
 */
enum class EntityEmbeddingType {
    SIGNATURE,     // Function/method signature: qualified_name + params + return_type
    DOCUMENTATION, // Docstrings, comments
    ALIAS,         // Entity aliases for linking
    CONTEXT        // Surrounding code context
};

/**
 * Configuration for the vector database
 */
struct VectorDatabaseConfig {
    std::string database_path = "vectors.db"; // SQLite database path
    std::string table_name = "document_embeddings";
    size_t embedding_dim = 0; // Must be set explicitly - no default to prevent dimension bugs
    // Create the database file and required tables if missing.
    // Default true to satisfy unit/integration tests that expect automatic
    // creation on first use. Daemon code may override this to false when it
    // wants strictly no side effects.
    bool create_if_missing = true;
    std::string index_type = "IVF_PQ";
    size_t num_partitions = 256;    // For IVF index
    size_t num_sub_quantizers = 96; // For PQ
    bool enable_checkpoints = true;
    size_t checkpoint_frequency = 1000; // Operations between checkpoints
    size_t max_batch_size = 1000;
    float default_similarity_threshold = 0.35f;
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

    EmbeddingLevel level = EmbeddingLevel::CHUNK;
    std::vector<std::string> source_chunk_ids;
    std::string parent_document_hash;
    std::vector<std::string> child_document_hashes;

    // Default constructor
    VectorRecord() = default;

    // Constructor with basic fields
    VectorRecord(std::string chunk_id_param, std::string document_hash_param,
                 std::vector<float> embedding_param, std::string content_param)
        : chunk_id(std::move(chunk_id_param)), document_hash(std::move(document_hash_param)),
          embedding(std::move(embedding_param)), content(std::move(content_param)),
          created_at(std::chrono::system_clock::now()) {}

    // Copy and move constructors
    VectorRecord(const VectorRecord&) = default;
    VectorRecord(VectorRecord&&) = default;
    VectorRecord& operator=(const VectorRecord&) = default;
    VectorRecord& operator=(VectorRecord&&) = default;
};

/**
 * Represents an entity/symbol vector record in the database
 * Used for semantic search over code symbols, functions, classes, etc.
 */
struct EntityVectorRecord {
    int64_t rowid = 0;                  // Database row ID (0 = new record)
    std::string node_key;               // KG node key (e.g., "function:foo@src/bar.cpp")
    EntityEmbeddingType embedding_type; // What was embedded (signature, docs, etc.)
    std::vector<float> embedding;       // Embedding vector
    std::string content;                // Text that was embedded
    std::string model_id;               // Model identifier
    std::string model_version;          // Model version
    std::chrono::system_clock::time_point embedded_at;
    bool is_stale = false;        // Mark for re-embedding
    float relevance_score = 0.0f; // For search results

    // Optional metadata from the KG node
    std::string node_type;      // "function", "class", "method", etc.
    std::string qualified_name; // Fully qualified symbol name
    std::string file_path;      // Source file path
    std::string document_hash;  // Source document hash (for joining)

    EntityVectorRecord() = default;

    EntityVectorRecord(std::string node_key_param, EntityEmbeddingType type,
                       std::vector<float> embedding_param, std::string content_param)
        : node_key(std::move(node_key_param)), embedding_type(type),
          embedding(std::move(embedding_param)), content(std::move(content_param)),
          embedded_at(std::chrono::system_clock::now()) {}

    // Copy and move
    EntityVectorRecord(const EntityVectorRecord&) = default;
    EntityVectorRecord(EntityVectorRecord&&) = default;
    EntityVectorRecord& operator=(const EntityVectorRecord&) = default;
    EntityVectorRecord& operator=(EntityVectorRecord&&) = default;
};

/**
 * Search parameters for entity vector similarity queries
 */
struct EntitySearchParams {
    size_t k = 10;
    float similarity_threshold = 0.5f;
    std::optional<EntityEmbeddingType> embedding_type; // Filter by type
    std::optional<std::string> node_type;              // Filter by KG node type
    std::optional<std::string> document_hash;          // Filter by source document
    bool include_embeddings = false;
};

/**
 * Search parameters for vector similarity queries
 */
struct VectorSearchParams {
    size_t k = 10;                                    // Number of results to return
    float similarity_threshold = 0.7f;                // Minimum similarity score
    std::optional<std::string> document_hash;         // Filter by single document
    std::unordered_set<std::string> candidate_hashes; // Filter to these candidates (tiered search)
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
    void initializeCounter(); // Initialize component-owned metrics (call after initialize())

    // Table management
    bool createTable();
    bool tableExists() const;
    void dropTable();
    size_t getVectorCount() const; // Returns cached count (no DB query)

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

    // Dual-path search: brute-force for small corpus (<10K), HNSW for large corpus (>=10K)
    std::vector<VectorRecord> search(const std::vector<float>& query_embedding,
                                     const VectorSearchParams& params = {}) const;

    // Utility operations
    std::optional<VectorRecord> getVector(const std::string& chunk_id) const;
    std::map<std::string, VectorRecord>
    getVectorsBatch(const std::vector<std::string>& chunk_ids) const;
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

    struct OrphanCleanupStats {
        size_t metadata_removed = 0;
        size_t embeddings_removed = 0;
        size_t metadata_backfilled = 0;
    };

    Result<OrphanCleanupStats> cleanupOrphanRows();

    // Embedding lifecycle operations
    Result<void> updateEmbeddings(const std::vector<VectorRecord>& records);
    Result<std::vector<std::string>> getStaleEmbeddings(const std::string& model_id,
                                                        const std::string& model_version);
    Result<std::vector<VectorRecord>> getEmbeddingsByVersion(const std::string& model_version,
                                                             size_t limit = 1000);
    Result<void> markAsStale(const std::string& chunk_id);
    Result<void> markAsDeleted(const std::string& chunk_id);
    Result<size_t> purgeDeleted(std::chrono::hours age_threshold);

    // =========================================================================
    // Entity Vector Operations (for symbols, functions, classes, etc.)
    // =========================================================================

    /**
     * @brief Insert a single entity vector record
     */
    Result<void> insertEntityVector(const EntityVectorRecord& record);

    /**
     * @brief Insert multiple entity vectors in a batch
     */
    Result<void> insertEntityVectorsBatch(const std::vector<EntityVectorRecord>& records);

    /**
     * @brief Update an entity vector by node_key + embedding_type
     */
    Result<void> updateEntityVector(const std::string& node_key, EntityEmbeddingType type,
                                    const EntityVectorRecord& record);

    /**
     * @brief Delete entity vectors by node_key
     */
    Result<void> deleteEntityVectorsByNode(const std::string& node_key);

    /**
     * @brief Delete all entity vectors for a document
     */
    Result<void> deleteEntityVectorsByDocument(const std::string& document_hash);

    /**
     * @brief Search for similar entity vectors
     */
    std::vector<EntityVectorRecord> searchEntities(const std::vector<float>& query_embedding,
                                                   const EntitySearchParams& params = {}) const;

    /**
     * @brief Get entity vectors by node_key
     */
    std::vector<EntityVectorRecord> getEntityVectorsByNode(const std::string& node_key) const;

    /**
     * @brief Get entity vectors by document
     */
    std::vector<EntityVectorRecord>
    getEntityVectorsByDocument(const std::string& document_hash) const;

    /**
     * @brief Check if an entity has embeddings
     */
    bool hasEntityEmbedding(const std::string& node_key) const;

    /**
     * @brief Get count of entity vectors
     */
    size_t getEntityVectorCount() const;

    /**
     * @brief Mark entity embeddings as stale (e.g., when source code changes)
     */
    Result<void> markEntityAsStale(const std::string& node_key);

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

/**
 * Convert EntityEmbeddingType to string
 */
inline std::string entityEmbeddingTypeToString(EntityEmbeddingType type) {
    switch (type) {
        case EntityEmbeddingType::SIGNATURE:
            return "signature";
        case EntityEmbeddingType::DOCUMENTATION:
            return "documentation";
        case EntityEmbeddingType::ALIAS:
            return "alias";
        case EntityEmbeddingType::CONTEXT:
            return "context";
        default:
            return "unknown";
    }
}

/**
 * Convert string to EntityEmbeddingType
 */
inline EntityEmbeddingType stringToEntityEmbeddingType(const std::string& str) {
    if (str == "signature")
        return EntityEmbeddingType::SIGNATURE;
    if (str == "documentation")
        return EntityEmbeddingType::DOCUMENTATION;
    if (str == "alias")
        return EntityEmbeddingType::ALIAS;
    if (str == "context")
        return EntityEmbeddingType::CONTEXT;
    return EntityEmbeddingType::SIGNATURE; // default
}

} // namespace utils

} // namespace yams::vector
