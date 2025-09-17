#pragma once

#include <yams/core/types.h>
#include <yams/vector/vector_index_manager.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

namespace yams::vector {
class EmbeddingGenerator;
}

namespace yams::search {

// Forward declarations
class KeywordSearchEngine;
class KGScorer;

/**
 * Configuration for hybrid search
 */
struct HybridSearchConfig {
    // Weight distribution (should sum to 1.0)
    float vector_weight = 0.7f;  // Weight for vector search results
    float keyword_weight = 0.3f; // Weight for keyword search results
    // KG-related weights (optional; considered when enable_kg=true)
    float kg_entity_weight = 0.0f;  // Weight for KG entity similarity score
    float structural_weight = 0.0f; // Weight for structural prior score (e.g., Node2Vec)

    // Search parameters
    size_t vector_top_k = 50;  // Number of vector results to retrieve
    size_t keyword_top_k = 50; // Number of keyword results to retrieve
    size_t final_top_k = 10;   // Final number of results to return

    // Fusion strategy
    enum class FusionStrategy {
        LINEAR_COMBINATION, // Weighted sum of scores
        RECIPROCAL_RANK,    // Reciprocal Rank Fusion (RRF)
        LEARNED_FUSION,     // ML-based fusion (future)
        CASCADE,            // Use vector search to rerank keyword results
        HYBRID_CASCADE      // Two-stage: keyword filter then vector rerank
    };
    FusionStrategy fusion_strategy = FusionStrategy::LINEAR_COMBINATION;

    // Reciprocal Rank Fusion parameter
    float rrf_k = 60.0f; // RRF constant (typically 60)

    // Re-ranking
    bool enable_reranking = true;
    size_t rerank_top_k = 20;      // Number of results to rerank
    float rerank_threshold = 0.0f; // Minimum score for reranking

    // Query expansion
    bool enable_query_expansion = false;
    size_t expansion_terms = 5;
    float expansion_weight = 0.3f;

    // KG scoring (local-first)
    bool enable_kg = false;                     // Enable KG-based scoring
    size_t kg_max_neighbors = 32;               // Max neighbors to consider per entity
    size_t kg_max_hops = 1;                     // Max hops for structural prior
    std::chrono::milliseconds kg_budget_ms{20}; // Time budget to include KG scoring

    // Score normalization
    bool normalize_scores = true; // Normalize scores to [0, 1]

    // Caching
    bool enable_cache = true;
    size_t cache_size = 1000;
    std::chrono::minutes cache_ttl{60};

    // Explanation
    bool generate_explanations = false;
    bool include_debug_scores = false;

    // Performance
    bool parallel_search = true; // Run vector and keyword search in parallel
    size_t num_threads = 0;      // 0 = auto-detect
    // Guard embedding generation so vector path never stalls queries. When the
    // timeout elapses, fall back to keyword-only for that query.
    std::chrono::milliseconds embed_timeout_ms{350};

    // Validate configuration
    bool isValid() const {
        const float w_sum = vector_weight + keyword_weight + kg_entity_weight + structural_weight;
        return vector_weight >= 0 && keyword_weight >= 0 && kg_entity_weight >= 0 &&
               structural_weight >= 0 && w_sum > 0 && vector_top_k > 0 && keyword_top_k > 0 &&
               final_top_k > 0;
    }

    // Normalize weights to sum to 1.0
    void normalizeWeights() {
        float sum = vector_weight + keyword_weight + kg_entity_weight + structural_weight;
        if (sum > 0) {
            vector_weight = vector_weight / sum;
            keyword_weight = keyword_weight / sum;
            kg_entity_weight = kg_entity_weight / sum;
            structural_weight = structural_weight / sum;
        }
    }
};

/**
 * Result from keyword search
 */
struct KeywordSearchResult {
    std::string id;      // Document/chunk ID
    std::string content; // Result content
    float score = 0.0f;  // BM25 or TF-IDF score
    std::vector<std::string> matched_terms;
    std::map<std::string, float> term_scores;
    std::map<std::string, std::string> metadata;

    // Constructor
    KeywordSearchResult() = default;
    KeywordSearchResult(std::string id_, std::string content_, float score_)
        : id(std::move(id_)), content(std::move(content_)), score(score_) {}
};

/**
 * Combined result from hybrid search
 */
struct HybridSearchResult {
    std::string id;      // Document/chunk ID
    std::string content; // Result content

    // Individual scores
    float vector_score = 0.0f;     // Vector similarity score [0, 1]
    float keyword_score = 0.0f;    // Keyword relevance score [0, 1]
    float kg_entity_score = 0.0f;  // KG entity similarity score [0, 1]
    float structural_score = 0.0f; // Structural prior score [0, 1]
    float hybrid_score = 0.0f;     // Combined score [0, 1]

    // Ranking information
    size_t vector_rank = SIZE_MAX;  // Rank in vector search (0-based)
    size_t keyword_rank = SIZE_MAX; // Rank in keyword search (0-based)
    size_t final_rank = 0;          // Final rank after fusion (0-based)

    // Search method flags
    bool found_by_vector = false;  // Found in vector search
    bool found_by_keyword = false; // Found in keyword search

    // Metadata
    std::map<std::string, std::string> metadata;
    std::vector<std::string> matched_keywords;
    std::vector<std::string> expanded_terms;

    // Explanation of ranking
    struct Explanation {
        std::string primary_method;       // Main method that found this result
        std::vector<std::string> reasons; // Why it's relevant
        std::map<std::string, float> score_breakdown;
        std::string fusion_method; // How scores were combined

        std::string toString() const {
            std::ostringstream oss;
            oss << "Method: " << primary_method << "\n";
            oss << "Fusion: " << fusion_method << "\n";
            oss << "Score breakdown:\n";
            for (const auto& [component, score] : score_breakdown) {
                oss << "  " << component << ": " << score << "\n";
            }
            if (!reasons.empty()) {
                oss << "Reasons:\n";
                for (const auto& reason : reasons) {
                    oss << "  - " << reason << "\n";
                }
            }
            return oss.str();
        }
    };
    std::optional<Explanation> explanation;

    // Comparison for sorting (higher score = better) with deterministic tie-breakers
    bool operator<(const HybridSearchResult& other) const {
        constexpr float eps = 1e-6f;
        if (std::fabs(hybrid_score - other.hybrid_score) > eps) {
            return hybrid_score > other.hybrid_score;
        }
        // Prefer better keyword rank when scores tie (smaller rank is better)
        if (keyword_rank != other.keyword_rank) {
            return keyword_rank < other.keyword_rank;
        }
        // Then prefer better vector rank
        if (vector_rank != other.vector_rank) {
            return vector_rank < other.vector_rank;
        }
        // Next, prefer lexicographically smaller path if available, else id
        auto pathItThis = metadata.find("path");
        auto pathItOther = other.metadata.find("path");
        if (pathItThis != metadata.end() && pathItOther != other.metadata.end() &&
            pathItThis->second != pathItOther->second) {
            return pathItThis->second < pathItOther->second;
        }
        // Fallback to id to ensure total order
        return id < other.id;
    }

    bool operator>(const HybridSearchResult& other) const {
        return hybrid_score < other.hybrid_score;
    }
};

/**
 * Base class for keyword search engines
 */
class KeywordSearchEngine {
public:
    virtual ~KeywordSearchEngine() = default;

    // Search interface
    virtual Result<std::vector<KeywordSearchResult>>
    search(const std::string& query, size_t k, const vector::SearchFilter* filter = nullptr) = 0;

    // Batch search
    virtual Result<std::vector<std::vector<KeywordSearchResult>>>
    batchSearch(const std::vector<std::string>& queries, size_t k,
                const vector::SearchFilter* filter = nullptr) = 0;

    // Index management
    virtual Result<void> addDocument(const std::string& id, const std::string& content,
                                     const std::map<std::string, std::string>& metadata = {}) = 0;

    virtual Result<void> removeDocument(const std::string& id) = 0;

    virtual Result<void>
    updateDocument(const std::string& id, const std::string& content,
                   const std::map<std::string, std::string>& metadata = {}) = 0;

    // Batch operations
    virtual Result<void>
    addDocuments(const std::vector<std::string>& ids, const std::vector<std::string>& contents,
                 const std::vector<std::map<std::string, std::string>>& metadata = {}) = 0;

    // Index operations
    virtual Result<void> buildIndex() = 0;
    virtual Result<void> optimizeIndex() = 0;
    virtual Result<void> clearIndex() = 0;

    // Persistence
    virtual Result<void> saveIndex(const std::string& path) = 0;
    virtual Result<void> loadIndex(const std::string& path) = 0;

    // Statistics
    virtual size_t getDocumentCount() const = 0;
    virtual size_t getTermCount() const = 0;
    virtual size_t getIndexSize() const = 0;

    // Query analysis
    virtual std::vector<std::string> analyzeQuery(const std::string& query) const = 0;
    virtual std::vector<std::string> extractKeywords(const std::string& text) const = 0;
};

/**
 * Main hybrid search engine class
 */
class HybridSearchEngine {
public:
    HybridSearchEngine(std::shared_ptr<vector::VectorIndexManager> vector_index,
                       std::shared_ptr<KeywordSearchEngine> keyword_engine,
                       const HybridSearchConfig& config = {},
                       std::shared_ptr<vector::EmbeddingGenerator> embedding_generator = nullptr);

    ~HybridSearchEngine();

    // Non-copyable but movable
    HybridSearchEngine(const HybridSearchEngine&) = delete;
    HybridSearchEngine& operator=(const HybridSearchEngine&) = delete;
    HybridSearchEngine(HybridSearchEngine&&) noexcept;
    HybridSearchEngine& operator=(HybridSearchEngine&&) noexcept;

    // Initialization
    Result<void> initialize();
    bool isInitialized() const;
    void shutdown();

    // Main search interface
    Result<std::vector<HybridSearchResult>> search(const std::string& query, size_t k = 10,
                                                   const vector::SearchFilter& filter = {});

    // Advanced search with pre-computed vector
    Result<std::vector<HybridSearchResult>>
    searchWithVector(const std::string& text_query, const std::vector<float>& query_vector,
                     size_t k = 10, const vector::SearchFilter& filter = {});

    // Batch search
    Result<std::vector<std::vector<HybridSearchResult>>>
    batchSearch(const std::vector<std::string>& queries, size_t k = 10,
                const vector::SearchFilter& filter = {});

    // Query expansion
    std::vector<std::string> expandQuery(const std::string& query);
    Result<std::vector<std::string>> findSimilarTerms(const std::string& term, size_t k = 5);

    // Re-ranking
    std::vector<HybridSearchResult> rerankResults(const std::vector<HybridSearchResult>& results,
                                                  const std::string& query);

    // Result fusion (public for testing/custom use)
    std::vector<HybridSearchResult>
    fuseResults(const std::vector<vector::SearchResult>& vector_results,
                const std::vector<KeywordSearchResult>& keyword_results,
                const HybridSearchConfig& config);

    // Configuration
    void setConfig(const HybridSearchConfig& config);
    const HybridSearchConfig& getConfig() const;
    void updateWeights(float vector_weight, float keyword_weight);
    // Inject KG scorer implementation (optional; used when config.enable_kg is true)
    void setKGScorer(std::shared_ptr<class KGScorer> kg_scorer);

    // Performance metrics
    struct SearchMetrics {
        // Timing metrics
        double avg_latency_ms = 0.0;
        double vector_search_time_ms = 0.0;
        double keyword_search_time_ms = 0.0;
        double fusion_time_ms = 0.0;
        double rerank_time_ms = 0.0;

        // Cache metrics
        size_t cache_hits = 0;
        size_t cache_misses = 0;
        double cache_hit_rate = 0.0;

        // Quality metrics
        double precision_at_k = 0.0;
        double recall_at_k = 0.0;
        double f1_score = 0.0;
        double ndcg = 0.0; // Normalized Discounted Cumulative Gain

        // Volume metrics
        size_t total_searches = 0;
        size_t total_results_returned = 0;

        // Method distribution
        size_t vector_only_results = 0;
        size_t keyword_only_results = 0;
        size_t both_methods_results = 0;

        void reset() { *this = SearchMetrics{}; }
    };

    SearchMetrics getMetrics() const;
    void resetMetrics();

    // Cache management
    void clearCache();
    size_t getCacheSize() const;
    double getCacheMemoryUsage() const; // In MB
    void preloadCache(const std::vector<std::string>& frequent_queries);

    // Feedback and learning
    void recordClickthrough(const std::string& query, const std::string& result_id,
                            size_t position);

    void recordRelevanceFeedback(const std::string& query, const std::string& result_id,
                                 bool relevant);

    Result<void> updateWeightsFromFeedback();

    // Index management (delegates to underlying engines)
    Result<void> addDocument(const std::string& id, const std::string& content,
                             const std::vector<float>& embedding,
                             const std::map<std::string, std::string>& metadata = {});

    Result<void> removeDocument(const std::string& id);

    Result<void> updateDocument(const std::string& id, const std::string& content,
                                const std::vector<float>& embedding,
                                const std::map<std::string, std::string>& metadata = {});

    // Diagnostics
    std::string getLastError() const;
    bool hasError() const;

    // Statistics
    struct IndexStats {
        size_t vector_index_size = 0;
        size_t keyword_index_size = 0;
        size_t total_documents = 0;
        size_t total_terms = 0;
        double avg_document_length = 0.0;
    };

    IndexStats getIndexStats() const;

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * Result fusion utilities
 */
namespace fusion {
/**
 * Linear combination of scores
 */
float linearCombination(float vector_score, float keyword_score, float vector_weight,
                        float keyword_weight);

/**
 * Reciprocal Rank Fusion
 */
float reciprocalRankFusion(size_t vector_rank, size_t keyword_rank, float k = 60.0f);

/**
 * Normalize scores to [0, 1] range
 */
std::vector<float> normalizeScores(const std::vector<float>& scores);

/**
 * Min-max normalization
 */
float minMaxNormalize(float value, float min, float max);

/**
 * Z-score normalization
 */
float zScoreNormalize(float value, float mean, float stddev);

/**
 * Compute score statistics
 */
struct ScoreStats {
    float min = 0.0f;
    float max = 0.0f;
    float mean = 0.0f;
    float stddev = 0.0f;
    float median = 0.0f;
};

ScoreStats computeScoreStats(const std::vector<float>& scores);
} // namespace fusion

/**
 * Query expansion utilities
 */
namespace expansion {
/**
 * Expand query with synonyms
 */
std::vector<std::string> expandWithSynonyms(const std::string& query, size_t max_terms = 5);

/**
 * Expand query using word embeddings
 */
std::vector<std::string> expandWithEmbeddings(const std::string& query,
                                              const vector::VectorIndexManager& index,
                                              size_t max_terms = 5);

/**
 * Extract key phrases from query
 */
std::vector<std::string> extractKeyPhrases(const std::string& query);

/**
 * Generate query variations
 */
std::vector<std::string> generateQueryVariations(const std::string& query);
} // namespace expansion

} // namespace yams::search
