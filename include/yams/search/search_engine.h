#pragma once

#include <yams/core/types.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_results.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>
#include <yams/vector/vector_index_manager.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// Forward declarations
namespace yams::metadata {
class KnowledgeGraphStore;
}

namespace yams::search {

// Import SearchResult from metadata namespace
using yams::metadata::SearchResult;

/**
 * @brief Search parameters for query execution
 */
struct SearchParams {
    int limit = 100;
    int offset = 0;

    // Tag-based search parameters
    std::vector<std::string> tags; // Tags to search for
    bool matchAllTags = false;     // true = AND, false = OR

    // Metadata filters (boost matching documents, don't exclude)
    std::optional<std::string> mimeType;
    std::optional<std::string> extension;
    std::optional<int64_t> modifiedAfter;
    std::optional<int64_t> modifiedBefore;
};

/**
 * @brief Configuration for SearchEngine
 *
 * Search engine configuration with tunable weights for each component.
 * This engine parallelizes queries across all available metadata structures
 * and fuses results with configurable ranking.
 */
struct SearchEngineConfig {
    /**
     * @brief Corpus content type profile for automatic weight tuning
     *
     * Auto-detects dominant content type and adjusts search weights accordingly.
     * - CODE: Heavy symbol/path emphasis, lower FTS5 weight
     * - PROSE: Heavy FTS5/vector emphasis, minimal symbol weight
     * - DOCS: Balanced FTS5/vector, moderate path weight
     * - MIXED: Default balanced weights
     * - CUSTOM: User-specified weights (ignores profile)
     */
    enum class CorpusProfile {
        CODE,  // Primarily source code (.py, .cpp, .rs, .go, etc.)
        PROSE, // Primarily text documents (.md, .txt, .pdf)
        DOCS,  // Technical documentation (mixed code snippets + prose)
        MIXED, // Balanced corpus (default)
        CUSTOM // User-specified weights
    } corpusProfile = CorpusProfile::MIXED;

    // Component weights (0.0 = disabled, 1.0 = full weight)
    float fts5Weight = 0.30f;     // Full-text search weight
    float pathTreeWeight = 0.15f; // Path tree hierarchical weight
    float symbolWeight = 0.15f;   // Symbol metadata weight
    float kgWeight = 0.10f;       // Knowledge graph weight
    float vectorWeight = 0.15f;   // Vector similarity weight
    float tagWeight = 0.10f;      // Tag-based search weight
    float metadataWeight = 0.05f; // Metadata attribute matching weight

    // Search parameters
    size_t maxResults = 100;                     // Maximum results to return
    float similarityThreshold = 0.65f;           // Minimum similarity threshold
    bool enableParallelExecution = true;         // Parallel component queries
    std::chrono::milliseconds componentTimeout = // Timeout per component (0 = no timeout)
        std::chrono::milliseconds(0);

    // Benchmarking support
    bool enableProfiling = false; // Enable detailed profiling output

    // Result fusion strategy
    enum class FusionStrategy {
        WEIGHTED_SUM,       // Sum of weighted scores
        RECIPROCAL_RANK,    // Reciprocal Rank Fusion
        BORDA_COUNT,        // Borda count voting
        WEIGHTED_RECIPROCAL // Weighted RRF (custom)
    } fusionStrategy = FusionStrategy::WEIGHTED_RECIPROCAL;

    // Component-specific settings
    size_t fts5MaxResults = 150;     // FTS5 candidate limit
    size_t pathTreeMaxResults = 100; // Path tree candidate limit
    size_t symbolMaxResults = 150;   // Symbol search candidate limit
    size_t kgMaxResults = 50;        // KG traversal candidate limit
    size_t vectorMaxResults = 100;   // Vector search k
    size_t tagMaxResults = 200;      // Tag search candidate limit
    size_t metadataMaxResults = 150; // Metadata filter candidate limit

    // Performance tuning
    bool useConnectionPriority = true;   // Use High priority for search queries
    size_t minChunkSizeForParallel = 50; // Min results for parallel processing

    // Symbol ranking
    bool symbolRank = true; // Enable automatic symbol ranking boost for code-like queries

    // Debugging
    bool includeDebugInfo = false; // Include per-component scores in results

    /**
     * @brief Get preset configuration for a corpus profile
     *
     * Returns a SearchEngineConfig with weights tuned for the given profile:
     * - CODE: fts5=0.20, symbol=0.30, path=0.20, vector=0.10, kg=0.10, tag=0.05, meta=0.05
     * - PROSE: fts5=0.40, symbol=0.05, path=0.10, vector=0.25, kg=0.05, tag=0.10, meta=0.05
     * - DOCS: fts5=0.30, symbol=0.15, path=0.15, vector=0.20, kg=0.10, tag=0.05, meta=0.05
     * - MIXED: Default balanced weights
     */
    static SearchEngineConfig forProfile(CorpusProfile profile) {
        SearchEngineConfig config;
        config.corpusProfile = profile;

        switch (profile) {
            case CorpusProfile::CODE:
                // Code-heavy: prioritize symbols, paths, and structured search
                config.fts5Weight = 0.20f;
                config.pathTreeWeight = 0.20f;
                config.symbolWeight = 0.30f;
                config.kgWeight = 0.10f;
                config.vectorWeight = 0.10f;
                config.tagWeight = 0.05f;
                config.metadataWeight = 0.05f;
                break;

            case CorpusProfile::PROSE:
                // Prose-heavy: prioritize FTS5 and semantic vector search
                config.fts5Weight = 0.40f;
                config.pathTreeWeight = 0.10f;
                config.symbolWeight = 0.05f;
                config.kgWeight = 0.05f;
                config.vectorWeight = 0.25f;
                config.tagWeight = 0.10f;
                config.metadataWeight = 0.05f;
                break;

            case CorpusProfile::DOCS:
                // Documentation: balanced FTS5/vector with moderate symbol/path
                config.fts5Weight = 0.30f;
                config.pathTreeWeight = 0.15f;
                config.symbolWeight = 0.15f;
                config.kgWeight = 0.10f;
                config.vectorWeight = 0.20f;
                config.tagWeight = 0.05f;
                config.metadataWeight = 0.05f;
                break;

            case CorpusProfile::MIXED:
            case CorpusProfile::CUSTOM:
            default:
                // Keep default balanced weights
                break;
        }

        return config;
    }

    /**
     * @brief Detect corpus profile from document extension distribution
     *
     * Analyzes the distribution of file extensions to determine the dominant
     * content type. Uses thresholds:
     * - CODE: >60% source code extensions (.py, .cpp, .h, .rs, .go, .js, etc.)
     * - PROSE: >60% text/document extensions (.md, .txt, .pdf, .docx)
     * - DOCS: 30-60% code + significant docs (README, docs/, etc.)
     * - MIXED: No dominant type
     *
     * @param extensionCounts Map of extension -> count from getDocumentCountsByExtension()
     * @return Detected CorpusProfile
     */
    static CorpusProfile
    detectProfile(const std::unordered_map<std::string, int64_t>& extensionCounts) {
        if (extensionCounts.empty()) {
            return CorpusProfile::MIXED;
        }

        // Code file extensions
        static const std::unordered_set<std::string> codeExtensions = {
            ".py", ".cpp", ".c",   ".h",     ".hpp",  ".cc", ".cxx", ".rs",  ".go",
            ".js", ".ts",  ".jsx", ".tsx",   ".java", ".kt", ".rb",  ".cs",  ".swift",
            ".m",  ".mm",  ".php", ".scala", ".lua",  ".pl", ".sh",  ".bash"};

        // Prose/document extensions
        static const std::unordered_set<std::string> proseExtensions = {
            ".md",  ".txt",  ".pdf", ".docx", ".doc", ".rtf",
            ".tex", ".html", ".htm", ".xml",  ".rst", ".adoc"};

        int64_t totalDocs = 0;
        int64_t codeDocs = 0;
        int64_t proseDocs = 0;

        for (const auto& [ext, count] : extensionCounts) {
            totalDocs += count;
            std::string lowerExt = ext;
            std::transform(lowerExt.begin(), lowerExt.end(), lowerExt.begin(), ::tolower);

            if (codeExtensions.count(lowerExt)) {
                codeDocs += count;
            } else if (proseExtensions.count(lowerExt)) {
                proseDocs += count;
            }
        }

        if (totalDocs == 0) {
            return CorpusProfile::MIXED;
        }

        float codeRatio = static_cast<float>(codeDocs) / static_cast<float>(totalDocs);
        float proseRatio = static_cast<float>(proseDocs) / static_cast<float>(totalDocs);

        // Thresholds for classification
        constexpr float kDominantThreshold = 0.60f;
        constexpr float kSignificantThreshold = 0.30f;

        if (codeRatio >= kDominantThreshold) {
            return CorpusProfile::CODE;
        }
        if (proseRatio >= kDominantThreshold) {
            return CorpusProfile::PROSE;
        }
        if (codeRatio >= kSignificantThreshold && proseRatio >= kSignificantThreshold) {
            return CorpusProfile::DOCS; // Mix of code and prose = documentation
        }

        return CorpusProfile::MIXED;
    }
};

/**
 * @brief Component-specific search result
 *
 * Internal structure for tracking results from individual search components.
 */
struct ComponentResult {
    std::string documentHash;
    std::string filePath;
    float score;                                  // Component-specific score [0.0, 1.0]
    std::string source;                           // Component name (e.g., "fts5", "vector")
    size_t rank;                                  // Rank within component results (0-based)
    std::optional<std::string> snippet;           // Optional text snippet
    std::map<std::string, std::string> debugInfo; // Component-specific debug data
};

struct SearchResponse {
    std::vector<SearchResult> results;
    std::vector<std::string> timedOutComponents;
    std::vector<std::string> failedComponents;
    std::vector<std::string> contributingComponents;
    int64_t executionTimeMs = 0;
    bool isDegraded = false;

    [[nodiscard]] bool hasResults() const { return !results.empty(); }
    [[nodiscard]] bool isComplete() const {
        return timedOutComponents.empty() && failedComponents.empty();
    }
};

/**
 * @brief Parallel component query executor
 *
 * Internal class for managing parallel execution of search queries across
 * all components. Uses connection pool with High priority and deterministic
 * error handling (no hangs from timeouts).
 */
class ComponentQueryExecutor {
public:
    explicit ComponentQueryExecutor(yams::metadata::ConnectionPool& pool,
                                    const SearchEngineConfig& config);

    // Execute all component queries in parallel
    Result<std::vector<ComponentResult>>
    executeAll(const std::string& query, const std::optional<std::vector<float>>& queryEmbedding);

private:
    // Individual component query methods (each returns Results ordered by score)
    Result<std::vector<ComponentResult>> queryFTS5(const std::string& query);
    Result<std::vector<ComponentResult>> queryPathTree(const std::string& query);
    Result<std::vector<ComponentResult>> querySymbols(const std::string& query);
    Result<std::vector<ComponentResult>> queryKnowledgeGraph(const std::string& query);
    Result<std::vector<ComponentResult>> queryVectorIndex(const std::vector<float>& embedding);

    yams::metadata::ConnectionPool& pool_;
    const SearchEngineConfig& config_;
};

/**
 * @brief Result fusion and ranking engine
 *
 * Fuses results from multiple components using configurable strategies.
 * All fusion algorithms are deterministic and produce consistent rankings
 * given the same input results.
 */
class ResultFusion {
public:
    explicit ResultFusion(const SearchEngineConfig& config);

    // Fuse component results into final ranked list
    std::vector<SearchResult> fuse(const std::vector<ComponentResult>& componentResults);

private:
    // Fusion strategy implementations
    std::vector<SearchResult> fuseWeightedSum(const std::vector<ComponentResult>& results);
    std::vector<SearchResult> fuseReciprocalRank(const std::vector<ComponentResult>& results);
    std::vector<SearchResult> fuseBordaCount(const std::vector<ComponentResult>& results);
    std::vector<SearchResult> fuseWeightedReciprocal(const std::vector<ComponentResult>& results);

    // Helper: Group component results by document (uses unordered_map for O(1) lookup)
    std::unordered_map<std::string, std::vector<ComponentResult>>
    groupByDocument(const std::vector<ComponentResult>& results) const;

    // Helper: Get weight for component source
    float getComponentWeight(const std::string& source) const;

    const SearchEngineConfig& config_;
};

/**
 * @brief Multi-component parallel search engine
 *
 * This engine:
 * 1. Queries all available metadata structures in parallel:
 *    - FTS5 full-text search (documents_fts)
 *    - Path tree hierarchical search (path_tree_nodes)
 *    - Symbol metadata search (symbol_metadata)
 *    - Knowledge graph traversal (kg_nodes, kg_edges, kg_aliases)
 *    - Vector similarity search (VectorDatabase + VectorIndexManager)
 *
 * 2. Ranks and fuses results from all components using configurable strategies
 *
 * 3. Optimizes for fast database reads (no in-memory loading required)
 *
 * 4. Uses deterministic execution flow (no hangs from complex async patterns)
 *
 * Design Philosophy:
 * - Each component query is independent and returns scored results
 * - Parallel execution uses connection pool with High priority
 * - No complex std::async/future timeout patterns that cause hangs
 * - Fusion happens after all components complete or timeout
 * - Failed components don't block other components
 * - Deterministic ranking for reproducible results
 */
class SearchEngine {
public:
    /**
     * @brief Construct a new SearchEngine
     *
     * @param metadataRepo Metadata repository for database access
     * @param vectorDb Vector database for similarity search
     * @param vectorIndex In-memory HNSW vector index
     * @param embeddingGen Embedding generator for query vectorization
     * @param kgStore Knowledge graph store for entity/alias queries (optional)
     * @param config Search engine configuration
     */
    explicit SearchEngine(std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo,
                          std::shared_ptr<vector::VectorDatabase> vectorDb,
                          std::shared_ptr<vector::VectorIndexManager> vectorIndex,
                          std::shared_ptr<vector::EmbeddingGenerator> embeddingGen,
                          std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore,
                          const SearchEngineConfig& config = {});

    ~SearchEngine();

    // Non-copyable, movable
    SearchEngine(const SearchEngine&) = delete;
    SearchEngine& operator=(const SearchEngine&) = delete;
    SearchEngine(SearchEngine&&) noexcept;
    SearchEngine& operator=(SearchEngine&&) noexcept;

    /**
     * @brief Execute a search query
     *
     * Parallelizes queries across all components, then fuses and ranks results.
     * This is the main entry point for search operations.
     *
     * @param query Search query string
     * @param params Additional search parameters (filters, limits, etc.)
     * @return Ranked search results
     */
    Result<std::vector<SearchResult>> search(const std::string& query,
                                             const SearchParams& params = {});

    /**
     * @brief Execute a search query with full response metadata
     *
     * Like search() but returns additional metadata about which components
     * contributed, timed out, or failed. Use this when you need to know
     * if the search was degraded due to component timeouts.
     *
     * @param query Search query string
     * @param params Additional search parameters (filters, limits, etc.)
     * @return SearchResponse with results and metadata
     */
    Result<SearchResponse> searchWithResponse(const std::string& query,
                                              const SearchParams& params = {});

    /**
     * @brief Update configuration at runtime
     *
     * Allows tuning component weights and fusion strategy without restart.
     *
     * @param config New configuration
     */
    void setConfig(const SearchEngineConfig& config);

    /**
     * @brief Get current configuration
     */
    const SearchEngineConfig& getConfig() const;

    /**
     * @brief Get search engine statistics
     *
     * Returns metrics about search performance and component health.
     */
    struct Statistics {
        // Query metrics
        std::atomic<uint64_t> totalQueries{0};
        std::atomic<uint64_t> successfulQueries{0};
        std::atomic<uint64_t> failedQueries{0};
        std::atomic<uint64_t> timedOutQueries{0}; // Parallel component timeouts

        // Component metrics
        std::atomic<uint64_t> fts5Queries{0};
        std::atomic<uint64_t> pathTreeQueries{0};
        std::atomic<uint64_t> symbolQueries{0};
        std::atomic<uint64_t> kgQueries{0};
        std::atomic<uint64_t> vectorQueries{0};
        std::atomic<uint64_t> tagQueries{0};
        std::atomic<uint64_t> metadataQueries{0};

        // Timing metrics (microseconds)
        std::atomic<uint64_t> totalQueryTimeMicros{0};
        std::atomic<uint64_t> avgQueryTimeMicros{0};

        // Component timing (microseconds)
        std::atomic<uint64_t> avgFts5TimeMicros{0};
        std::atomic<uint64_t> avgPathTreeTimeMicros{0};
        std::atomic<uint64_t> avgSymbolTimeMicros{0};
        std::atomic<uint64_t> avgKgTimeMicros{0};
        std::atomic<uint64_t> avgVectorTimeMicros{0};
        std::atomic<uint64_t> avgTagTimeMicros{0};
        std::atomic<uint64_t> avgMetadataTimeMicros{0};

        // Fusion metrics
        std::atomic<uint64_t> avgResultsPerQuery{0};
        std::atomic<uint64_t> avgComponentsPerResult{0};
    };

    const Statistics& getStatistics() const;
    void resetStatistics();

    /**
     * @brief Health check for all components
     *
     * Verifies that all search components are available and responsive.
     * Useful for startup validation and monitoring.
     */
    Result<void> healthCheck();

private:
    class Impl;
    std::unique_ptr<Impl> pImpl_;
};

/**
 * @brief Factory function for creating SearchEngine
 *
 * Convenience function for constructing the search engine with default config.
 */
std::unique_ptr<SearchEngine>
createSearchEngine(std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo,
                   std::shared_ptr<vector::VectorDatabase> vectorDb,
                   std::shared_ptr<vector::VectorIndexManager> vectorIndex,
                   std::shared_ptr<vector::EmbeddingGenerator> embeddingGen,
                   std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore,
                   const SearchEngineConfig& config = {});

} // namespace yams::search
