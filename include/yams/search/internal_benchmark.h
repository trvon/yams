#pragma once

#include <yams/core/types.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine.h>

#include <nlohmann/json.hpp>
#include <chrono>
#include <cstdint>
#include <optional>
#include <random>
#include <string>
#include <vector>

namespace yams::search {

/**
 * @brief Type of synthetic query generated for benchmarking.
 */
enum class QueryType {
    KNOWN_ITEM,  // Exact phrase from document (tests retrieval accuracy)
    TFIDF_TERMS, // Top TF-IDF terms from document (tests term relevance)
    SEMANTIC     // Paraphrased/related terms (tests vector similarity)
};

/**
 * @brief Convert QueryType to string for logging/debugging.
 */
[[nodiscard]] constexpr const char* queryTypeToString(QueryType type) noexcept {
    switch (type) {
        case QueryType::KNOWN_ITEM:
            return "KNOWN_ITEM";
        case QueryType::TFIDF_TERMS:
            return "TFIDF_TERMS";
        case QueryType::SEMANTIC:
            return "SEMANTIC";
    }
    return "UNKNOWN";
}

/**
 * @brief A synthetic query with ground truth for benchmark evaluation.
 *
 * Each query is derived from a known document, allowing us to measure
 * whether the search engine can retrieve the source document.
 */
struct SyntheticQuery {
    std::string text;             // The query text to search for
    std::string expectedDocHash;  // Ground truth: document this query came from
    std::string expectedFilePath; // File path for logging/debugging
    QueryType type;               // Type of query generation used
    std::string sourcePhrase;     // Original phrase extracted (for debugging)

    [[nodiscard]] nlohmann::json toJson() const {
        return nlohmann::json{{"text", text},
                              {"expected_doc_hash", expectedDocHash},
                              {"expected_file_path", expectedFilePath},
                              {"type", queryTypeToString(type)},
                              {"source_phrase", sourcePhrase}};
    }
};

/**
 * @brief Configuration for synthetic query generation.
 */
struct QueryGeneratorConfig {
    size_t queryCount = 100;        // Number of queries to generate
    size_t minPhraseLength = 3;     // Minimum words in extracted phrase
    size_t maxPhraseLength = 8;     // Maximum words in extracted phrase
    size_t minDocumentLength = 100; // Skip documents shorter than this
    float knownItemRatio = 0.5f;    // Fraction of KNOWN_ITEM queries
    float tfidfRatio = 0.3f;        // Fraction of TFIDF_TERMS queries
    float semanticRatio = 0.2f;     // Fraction of SEMANTIC queries
    uint64_t seed = 42;             // Random seed for reproducibility
    bool excludeBinaryFiles = true; // Skip binary content
    bool preferCodeFiles = false;   // Weight towards code files
};

/**
 * @brief Generator for synthetic benchmark queries from corpus.
 *
 * Samples documents from the indexed corpus and extracts distinctive
 * phrases/terms to create queries with known ground truth. This allows
 * us to measure retrieval quality (MRR, Recall@K) without external
 * benchmark datasets.
 *
 * Query Types:
 * - KNOWN_ITEM: Direct phrase extraction (tests exact matching)
 * - TFIDF_TERMS: Top distinctive terms (tests term weighting)
 * - SEMANTIC: Related concepts (tests vector similarity) [future]
 *
 * Usage:
 * @code
 *   SyntheticQueryGenerator gen(metadataRepo);
 *   auto queries = gen.generate(config);
 *   // Use queries for InternalBenchmark
 * @endcode
 */
class SyntheticQueryGenerator {
public:
    /**
     * @brief Construct generator with metadata repository access.
     */
    explicit SyntheticQueryGenerator(std::shared_ptr<metadata::MetadataRepository> metadataRepo);

    /**
     * @brief Generate synthetic queries from the corpus.
     *
     * @param config Generation configuration (count, ratios, seed)
     * @return Vector of synthetic queries with ground truth
     */
    [[nodiscard]] Result<std::vector<SyntheticQuery>>
    generate(const QueryGeneratorConfig& config = {});

    /**
     * @brief Get the number of documents available for sampling.
     */
    [[nodiscard]] Result<size_t> getAvailableDocumentCount() const;

private:
    /**
     * @brief Sample random documents from the corpus.
     */
    Result<std::vector<metadata::DocumentInfo>> sampleDocuments(size_t count, std::mt19937_64& rng,
                                                                const QueryGeneratorConfig& config);

    /**
     * @brief Extract a distinctive phrase from document content.
     *
     * Selects a random contiguous phrase that is likely to be unique
     * and useful for retrieval testing.
     */
    std::optional<std::string> extractPhrase(const std::string& content, size_t minWords,
                                             size_t maxWords, std::mt19937_64& rng);

    /**
     * @brief Extract top TF-IDF-like terms from document content.
     *
     * Uses term frequency and basic filtering to identify distinctive terms.
     */
    std::vector<std::string> extractTopTerms(const std::string& content, size_t count);

    /**
     * @brief Check if content appears to be binary/non-text.
     */
    bool isBinaryContent(const std::string& content) const;

    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
};

/**
 * @brief Latency statistics from benchmark execution.
 */
struct LatencyStats {
    double minMs = 0.0;
    double maxMs = 0.0;
    double meanMs = 0.0;
    double medianMs = 0.0;
    double p95Ms = 0.0;
    double p99Ms = 0.0;
    double stddevMs = 0.0;
    size_t sampleCount = 0;

    [[nodiscard]] nlohmann::json toJson() const {
        return nlohmann::json{{"min_ms", minMs},       {"max_ms", maxMs},
                              {"mean_ms", meanMs},     {"median_ms", medianMs},
                              {"p95_ms", p95Ms},       {"p99_ms", p99Ms},
                              {"stddev_ms", stddevMs}, {"sample_count", sampleCount}};
    }

    /**
     * @brief Compute statistics from a vector of latency samples.
     */
    static LatencyStats compute(const std::vector<double>& samples);
};

/**
 * @brief Results from a single query execution.
 */
struct QueryExecution {
    SyntheticQuery query;
    std::vector<std::string> retrievedHashes; // Top-K retrieved doc hashes
    size_t reciprocalRank = 0;                // Position of expected doc (1-based, 0 if not found)
    bool foundInTopK = false;                 // Did we find expected doc in top-K?
    double latencyMs = 0.0;                   // Query execution time
    std::string error;                        // Error message if query failed
};

/**
 * @brief Aggregated benchmark results.
 */
struct BenchmarkResults {
    // Quality metrics
    float mrr = 0.0f;          // Mean Reciprocal Rank (higher is better, max 1.0)
    float recallAtK = 0.0f;    // Recall@K (fraction of queries with expected doc in top-K)
    float precisionAtK = 0.0f; // Precision@K (not typically meaningful for single-doc ground truth)
    size_t k = 10;             // K value used for Recall@K

    // Latency metrics
    LatencyStats latency;

    // Execution metadata
    size_t queriesRun = 0;
    size_t queriesSucceeded = 0;
    size_t queriesFailed = 0;
    std::chrono::milliseconds totalTime{0};
    std::string timestamp; // ISO8601 timestamp

    // Per-query breakdown (optional, for detailed analysis)
    std::vector<QueryExecution> executions;

    // Tuning state at time of benchmark (for correlation)
    std::optional<std::string> tuningState;
    std::optional<nlohmann::json> tunedParams;

    [[nodiscard]] nlohmann::json toJson() const {
        nlohmann::json j{{"mrr", mrr},
                         {"recall_at_k", recallAtK},
                         {"precision_at_k", precisionAtK},
                         {"k", k},
                         {"latency", latency.toJson()},
                         {"queries_run", queriesRun},
                         {"queries_succeeded", queriesSucceeded},
                         {"queries_failed", queriesFailed},
                         {"total_time_ms", totalTime.count()},
                         {"timestamp", timestamp}};
        if (tuningState) {
            j["tuning_state"] = *tuningState;
        }
        if (tunedParams) {
            j["tuned_params"] = *tunedParams;
        }
        return j;
    }

    /**
     * @brief Create a summary string for console output.
     */
    [[nodiscard]] std::string summary() const;
};

/**
 * @brief Comparison result between two benchmark runs.
 */
struct BenchmarkComparison {
    float mrrDelta = 0.0f;      // Positive = improvement
    float recallDelta = 0.0f;   // Positive = improvement
    float latencyDelta = 0.0f;  // Negative = improvement (faster)
    bool isRegression = false;  // True if quality dropped significantly
    bool isImprovement = false; // True if quality improved significantly
    std::string summary;

    [[nodiscard]] nlohmann::json toJson() const {
        return nlohmann::json{{"mrr_delta", mrrDelta},
                              {"recall_delta", recallDelta},
                              {"latency_delta_ms", latencyDelta},
                              {"is_regression", isRegression},
                              {"is_improvement", isImprovement},
                              {"summary", summary}};
    }
};

/**
 * @brief Configuration for benchmark execution.
 */
struct BenchmarkConfig {
    size_t queryCount = 100;            // Number of queries to run
    size_t k = 10;                      // K for Recall@K
    size_t warmupQueries = 5;           // Warmup queries (not counted in results)
    bool includeExecutions = false;     // Include per-query breakdown in results
    bool verbose = false;               // Log each query execution
    float regressionThreshold = 0.05f;  // MRR drop threshold for regression alert
    float improvementThreshold = 0.05f; // MRR gain threshold for improvement alert
    QueryGeneratorConfig queryConfig;   // Query generation settings
};

/**
 * @brief Internal benchmark runner for search quality measurement.
 *
 * Runs synthetic queries against the search engine and measures retrieval
 * quality using standard IR metrics (MRR, Recall@K) plus latency statistics.
 *
 * This enables:
 * 1. Validation of SearchTuner FSM parameter selections
 * 2. Detection of quality regressions after code changes
 * 3. Comparison of different tuning configurations
 * 4. Continuous quality monitoring
 *
 * Usage:
 * @code
 *   InternalBenchmark bench(searchEngine, metadataRepo);
 *   BenchmarkConfig config;
 *   config.queryCount = 50;
 *
 *   auto results = bench.run(config);
 *   if (results) {
 *       std::cout << results->summary() << "\n";
 *   }
 * @endcode
 */
class InternalBenchmark {
public:
    /**
     * @brief Construct benchmark runner.
     *
     * @param searchEngine Search engine to benchmark
     * @param metadataRepo Metadata repository for query generation and ground truth
     */
    InternalBenchmark(std::shared_ptr<SearchEngine> searchEngine,
                      std::shared_ptr<metadata::MetadataRepository> metadataRepo);

    /**
     * @brief Run the benchmark.
     *
     * @param config Benchmark configuration
     * @return Aggregated benchmark results
     */
    [[nodiscard]] Result<BenchmarkResults> run(const BenchmarkConfig& config = {});

    /**
     * @brief Run benchmark with pre-generated queries.
     *
     * Use this when you want to reuse the same query set across multiple runs
     * (e.g., comparing different configurations).
     *
     * @param queries Pre-generated synthetic queries
     * @param config Benchmark configuration (queryCount is ignored)
     * @return Aggregated benchmark results
     */
    [[nodiscard]] Result<BenchmarkResults>
    runWithQueries(const std::vector<SyntheticQuery>& queries, const BenchmarkConfig& config = {});

    /**
     * @brief Compare two benchmark results.
     *
     * @param baseline Previous/reference results
     * @param current New results to compare
     * @param regressionThreshold MRR drop threshold for regression (default 0.05)
     * @return Comparison summary
     */
    [[nodiscard]] static BenchmarkComparison compare(const BenchmarkResults& baseline,
                                                     const BenchmarkResults& current,
                                                     float regressionThreshold = 0.05f);

private:
    /**
     * @brief Execute a single query and measure results.
     */
    QueryExecution executeQuery(const SyntheticQuery& query, size_t k);

    /**
     * @brief Compute aggregate metrics from query executions.
     */
    BenchmarkResults aggregateResults(const std::vector<QueryExecution>& executions, size_t k,
                                      std::chrono::milliseconds totalTime);

    std::shared_ptr<SearchEngine> searchEngine_;
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
};

} // namespace yams::search
