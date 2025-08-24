#pragma once

#include <yams/core/types.h>

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace yams::vector {

/**
 * Types of vector indices supported
 */
enum class IndexType {
    FLAT,     // Exact brute-force search
    HNSW,     // Hierarchical Navigable Small World
    IVF_FLAT, // Inverted File with Flat quantizer
    IVF_PQ,   // Inverted File with Product Quantization
    LSH,      // Locality Sensitive Hashing
    ANNOY     // Approximate Nearest Neighbors Oh Yeah
};

/**
 * Distance metrics for similarity calculation
 */
enum class DistanceMetric {
    COSINE,        // Cosine similarity
    L2,            // Euclidean distance
    INNER_PRODUCT, // Dot product
    HAMMING        // Hamming distance (for binary vectors)
};

/**
 * Configuration for vector index
 */
struct IndexConfig {
    IndexType type = IndexType::HNSW;
    size_t dimension = 384;
    DistanceMetric distance_metric = DistanceMetric::COSINE;

    // HNSW parameters
    size_t hnsw_m = 16;                // Number of connections per node
    size_t hnsw_ef_construction = 200; // Construction time accuracy
    size_t hnsw_ef_search = 50;        // Search time accuracy
    size_t hnsw_seed = 100;            // Random seed for level assignment

    // IVF parameters
    size_t ivf_nlist = 100; // Number of clusters
    size_t ivf_nprobe = 10; // Clusters to search

    // LSH parameters
    size_t lsh_num_tables = 10; // Number of hash tables
    size_t lsh_key_size = 20;   // Hash key size

    // General parameters
    bool normalize_vectors = true;  // Normalize vectors before indexing
    size_t max_elements = 1000000;  // Maximum capacity
    bool enable_persistence = true; // Enable saving/loading
    std::string index_path = "vector_index.bin";

    // Performance tuning
    size_t batch_size = 1000; // Batch size for operations
    bool use_simd = true;     // Use SIMD instructions if available
    size_t num_threads = 0;   // 0 = auto-detect

    // Memory management
    bool use_mmap = false;      // Use memory-mapped files
    size_t cache_size_mb = 100; // Cache size in MB

    // Delta index for incremental updates
    bool enable_delta_index = true;
    size_t delta_threshold = 1000; // Merge delta when it reaches this size
};

/**
 * Search result from vector index
 */
struct SearchResult {
    std::string id;                              // Vector identifier
    float distance;                              // Distance to query
    float similarity;                            // Similarity score (1.0 - normalized_distance)
    std::map<std::string, std::string> metadata; // Optional metadata

    // Constructors
    SearchResult() : distance(0.0f), similarity(0.0f) {}
    SearchResult(std::string id_, float dist, float sim)
        : id(std::move(id_)), distance(dist), similarity(sim) {}

    // Comparison operators for sorting
    bool operator<(const SearchResult& other) const { return distance < other.distance; }

    bool operator>(const SearchResult& other) const { return distance > other.distance; }
};

/**
 * Filter criteria for search operations
 */
struct SearchFilter {
    // Metadata filters (key-value pairs must match)
    std::map<std::string, std::string> metadata_filters;

    // Date range filters
    std::optional<std::chrono::system_clock::time_point> created_after;
    std::optional<std::chrono::system_clock::time_point> created_before;

    // Document filters
    std::vector<std::string> document_hashes; // Include only these documents
    std::vector<std::string> exclude_ids;     // Exclude these vector IDs

    // Score threshold
    std::optional<float> min_similarity; // Minimum similarity score
    std::optional<float> max_distance;   // Maximum distance

    // Custom filter function
    std::function<bool(const std::string& id, const std::map<std::string, std::string>& metadata)>
        custom_filter;

    // Check if any filters are set
    bool hasFilters() const {
        return !metadata_filters.empty() || created_after.has_value() ||
               created_before.has_value() || !document_hashes.empty() || !exclude_ids.empty() ||
               min_similarity.has_value() || max_distance.has_value() || custom_filter != nullptr;
    }
};

/**
 * Statistics for vector index
 */
struct IndexStats {
    size_t num_vectors = 0;           // Total number of vectors
    size_t index_size_bytes = 0;      // Memory/disk usage
    size_t dimension = 0;             // Vector dimension
    IndexType type = IndexType::FLAT; // Index type
    DistanceMetric metric = DistanceMetric::COSINE;

    // Performance metrics
    double avg_search_time_ms = 0.0; // Average search time
    double avg_insert_time_ms = 0.0; // Average insertion time
    size_t total_searches = 0;       // Total searches performed
    size_t total_inserts = 0;        // Total insertions

    // Index health
    double fragmentation_ratio = 0.0; // Index fragmentation (0-1)
    size_t delta_index_size = 0;      // Size of delta index
    bool needs_optimization = false;  // Whether optimization is recommended
    std::chrono::system_clock::time_point last_optimization;

    // Memory usage
    size_t memory_usage_bytes = 0; // Current memory usage
    size_t cache_hits = 0;         // Cache hit count
    size_t cache_misses = 0;       // Cache miss count

    double getCacheHitRate() const {
        size_t total = cache_hits + cache_misses;
        return total > 0 ? static_cast<double>(cache_hits) / static_cast<double>(total) : 0.0;
    }
};

/**
 * Base class for vector indices
 */
class VectorIndex {
public:
    explicit VectorIndex(const IndexConfig& config) : config_(config) {}
    virtual ~VectorIndex() = default;

    // Non-copyable but movable
    VectorIndex(const VectorIndex&) = delete;
    VectorIndex& operator=(const VectorIndex&) = delete;
    VectorIndex(VectorIndex&&) = default;
    VectorIndex& operator=(VectorIndex&&) = default;

    // Core operations
    virtual Result<void> add(const std::string& id, const std::vector<float>& vector) = 0;
    virtual Result<void> update(const std::string& id, const std::vector<float>& vector) = 0;
    virtual Result<void> remove(const std::string& id) = 0;
    virtual Result<std::vector<SearchResult>> search(const std::vector<float>& query, size_t k,
                                                     const SearchFilter* filter = nullptr) = 0;

    // Batch operations
    virtual Result<void> addBatch(const std::vector<std::string>& ids,
                                  const std::vector<std::vector<float>>& vectors) = 0;

    virtual Result<std::vector<std::vector<SearchResult>>>
    searchBatch(const std::vector<std::vector<float>>& queries, size_t k,
                const SearchFilter* filter = nullptr) = 0;

    // Persistence
    virtual Result<void> save(const std::string& path) = 0;
    virtual Result<void> load(const std::string& path) = 0;

    // Optimization
    virtual Result<void> optimize() = 0;
    virtual bool needsOptimization() const = 0;

    // Info
    virtual size_t size() const = 0;
    virtual size_t dimension() const = 0;
    virtual IndexType type() const = 0;
    virtual IndexStats getStats() const = 0;

    // Serialization interface
    virtual Result<void> serialize(std::ostream& out) const = 0;
    virtual Result<void> deserialize(std::istream& in) = 0;

    // Helper methods for serialization
    virtual std::vector<std::string> getAllIds() const = 0;
    virtual Result<std::vector<float>> getVector(const std::string& id) const = 0;

    // Configuration
    const IndexConfig& getConfig() const { return config_; }

protected:
    IndexConfig config_;
    mutable IndexStats stats_;
};

/**
 * Main vector index manager class
 */
class VectorIndexManager {
public:
    explicit VectorIndexManager(const IndexConfig& config = {});
    ~VectorIndexManager();

    // Non-copyable but movable
    VectorIndexManager(const VectorIndexManager&) = delete;
    VectorIndexManager& operator=(const VectorIndexManager&) = delete;
    VectorIndexManager(VectorIndexManager&&) noexcept;
    VectorIndexManager& operator=(VectorIndexManager&&) noexcept;

    // Initialization
    Result<void> initialize();
    bool isInitialized() const;
    void shutdown();

    // Vector operations
    Result<void> addVector(const std::string& id, const std::vector<float>& vector,
                           const std::map<std::string, std::string>& metadata = {});

    Result<void> addVectors(const std::vector<std::string>& ids,
                            const std::vector<std::vector<float>>& vectors,
                            const std::vector<std::map<std::string, std::string>>& metadata = {});

    Result<void> updateVector(const std::string& id, const std::vector<float>& vector,
                              const std::map<std::string, std::string>& metadata = {});

    Result<void> removeVector(const std::string& id);
    Result<void> removeVectors(const std::vector<std::string>& ids);

    // Search operations
    Result<std::vector<SearchResult>> search(const std::vector<float>& query, size_t k = 10,
                                             const SearchFilter& filter = {});

    Result<std::vector<std::vector<SearchResult>>>
    batchSearch(const std::vector<std::vector<float>>& queries, size_t k = 10,
                const SearchFilter& filter = {});

    // Range search (find all vectors within distance)
    Result<std::vector<SearchResult>> rangeSearch(const std::vector<float>& query,
                                                  float max_distance,
                                                  const SearchFilter& filter = {});

    // Index management
    Result<void> buildIndex();
    Result<void> rebuildIndex();
    Result<void> optimizeIndex();
    Result<void> mergeDeltaIndex();

    // Persistence
    Result<void> saveIndex(const std::string& path = "");
    Result<void> loadIndex(const std::string& path = "");
    Result<void> exportIndex(const std::string& path, const std::string& format = "json");

    // Statistics and monitoring
    IndexStats getStats() const;
    bool needsOptimization() const;
    size_t size() const;
    size_t dimension() const;

    // Configuration
    void setConfig(const IndexConfig& config);
    const IndexConfig& getConfig() const;

    // Index type management
    Result<void> changeIndexType(IndexType new_type);
    IndexType getCurrentIndexType() const;
    IndexType getIndexType() const; // Alias for getCurrentIndexType for compatibility

    // Vector ID management
    Result<std::vector<std::string>> getAllVectorIds() const;

    // Cache management
    void clearCache();
    void warmupCache(const std::vector<std::vector<float>>& frequent_queries);

    // Validation
    Result<bool> validateIndex() const;
    Result<void> repairIndex();

    // Error handling
    std::string getLastError() const;
    bool hasError() const;

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * Factory function for creating specific index types
 */
std::unique_ptr<VectorIndex> createVectorIndex(const IndexConfig& config);

/**
 * Utility functions for vector operations
 */
namespace vector_utils {
/**
 * Calculate distance between two vectors
 */
float calculateDistance(const std::vector<float>& a, const std::vector<float>& b,
                        DistanceMetric metric);

/**
 * Normalize a vector to unit length
 */
std::vector<float> normalize(const std::vector<float>& vector);

/**
 * Batch normalize vectors
 */
std::vector<std::vector<float>> normalizeVectors(const std::vector<std::vector<float>>& vectors);

/**
 * Convert distance to similarity score
 */
float distanceToSimilarity(float distance, DistanceMetric metric);

/**
 * Check if vector dimension is valid
 */
bool isValidDimension(const std::vector<float>& vector, size_t expected_dim);

/**
 * Calculate mean vector from a set
 */
std::vector<float> meanVector(const std::vector<std::vector<float>>& vectors);

/**
 * Calculate centroid of vectors
 */
std::vector<float> centroid(const std::vector<std::vector<float>>& vectors);

/**
 * Quantize vector for compression
 */
std::vector<uint8_t> quantizeVector(const std::vector<float>& vector);

/**
 * Dequantize compressed vector
 */
std::vector<float> dequantizeVector(const std::vector<uint8_t>& quantized, size_t dimension);
} // namespace vector_utils

} // namespace yams::vector