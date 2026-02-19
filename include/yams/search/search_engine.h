#pragma once

#include <yams/core/types.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/query_concept_extractor.h>
#include <yams/search/search_results.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>

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

#include <boost/asio/any_io_executor.hpp>

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
 * @brief Interface for cross-encoder document reranking
 *
 * Rerankers use cross-encoder models to score query-document pairs,
 * providing more accurate relevance scores than bi-encoder (embedding) similarity.
 * This is typically used as a second-stage ranker after initial retrieval.
 */
class IReranker {
public:
    virtual ~IReranker() = default;

    /**
     * @brief Score documents against a query using cross-encoder
     *
     * @param query The search query
     * @param documents The document texts/snippets to score
     * @return Vector of relevance scores [0,1] for each document, or error
     */
    virtual Result<std::vector<float>>
    scoreDocuments(const std::string& query, const std::vector<std::string>& documents) = 0;

    /**
     * @brief Check if the reranker is ready to accept requests
     */
    virtual bool isReady() const = 0;
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
    float textWeight = 0.70f;          // Full-text search weight
    float pathTreeWeight = 0.08f;      // Path tree hierarchical weight
    float kgWeight = 0.04f;            // Knowledge graph weight
    float vectorWeight = 0.30f;        // Vector similarity weight (increased for hybrid value)
    float vectorOnlyPenalty = 0.8f;    // Penalty for vector-only results (no text match)
    float vectorOnlyThreshold = 0.90f; // Minimum confidence for vector-only inclusion
    float vectorBoostFactor = 0.10f;   // Boost factor for vector re-ranking
    float entityVectorWeight = 0.05f;  // Entity (symbol) vector similarity weight
    float tagWeight = 0.05f;           // Tag-based search weight (modifier, not standalone)
    float metadataWeight = 0.05f; // Metadata attribute matching weight (modifier, not standalone)

    // Concept-based boosts (GLiNER query concepts)
    float conceptBoostWeight = 0.10f;   // Per-concept boost factor applied to matches
    float conceptMinConfidence = 0.40f; // Minimum confidence for concept inclusion
    size_t conceptMaxCount = 6;         // Maximum number of concepts to apply per query
    float conceptMaxBoost = 0.25f;      // Global boost budget per query (sum of applied boosts)
    size_t conceptMaxScanResults = 200; // Maximum results to scan for concept matches

    // Search parameters
    size_t maxResults = 100;             // Maximum results to return
    float similarityThreshold = 0.75f;   // Higher threshold - only high-confidence vector matches
    bool enableParallelExecution = true; // Parallel component queries
    std::chrono::milliseconds componentTimeout = // Timeout per component (0 = no timeout)
        std::chrono::milliseconds(0);

    // Tiered execution: Run fast components (FTS5 + path) first, use results to narrow vector
    // search Tier 1 results inform which document vectors to search, improving both speed and
    // relevance
    bool enableTieredExecution = true; // Enable two-tier search (text narrows vector candidates)
    bool tieredNarrowVectorSearch =
        false; // DISABLED: Narrowing prevents vector from finding docs FTS5 missed
    size_t tieredMinCandidates =
        10; // Min candidates from Tier 1 before narrowing (fallback to full)
    bool enableAdaptiveVectorFallback =
        false; // Skip embedding/vector tier when Tier 1 already has strong coverage
    size_t adaptiveVectorSkipMinTier1Hits =
        0; // 0=auto (max(maxResults*2,50)); explicit value overrides auto threshold

    // RRF (Reciprocal Rank Fusion) parameter
    // Lower k = more weight on top-ranked items (better precision/MRR)
    // Higher k = smoother ranking across positions (better recall)
    float rrfK = 12.0f; // Lower k for sharper top-rank discrimination

    // BM25 score normalization divisor (typical range: 20-30)
    float bm25NormDivisor = 25.0f;

    // Benchmarking support
    bool enableProfiling = false;

    // Result fusion strategy (4 strategies for simplicity)
    enum class FusionStrategy {
        WEIGHTED_SUM,                            // Sum of weighted scores (simple baseline)
        RECIPROCAL_RANK,                         // Standard Reciprocal Rank Fusion
        WEIGHTED_RECIPROCAL,                     // RRF with score boost (good default)
        COMB_MNZ                                 // CombMNZ: score * num_components (recall-focused)
    } fusionStrategy = FusionStrategy::COMB_MNZ; // Default: recall-focused

    // Hybrid behavior flags for experimentation and tuning
    bool enableIntentAdaptiveWeighting = true; // Apply query-intent scaling at runtime
    bool enableFieldAwareWeightedRrf = true;   // Use source-specific scaling in WEIGHTED_RECIPROCAL
    bool enableLexicalExpansion = false;       // Enable fallback lexical expansion for sparse hits
    size_t lexicalExpansionMinHits = 3;        // Trigger expansion when primary FTS hits below this
    float lexicalExpansionScorePenalty = 0.65f; // Penalty applied to expanded-only FTS matches

    /// Convert FusionStrategy to string for logging/debugging
    [[nodiscard]] static constexpr const char*
    fusionStrategyToString(FusionStrategy strategy) noexcept {
        switch (strategy) {
            case FusionStrategy::WEIGHTED_SUM:
                return "WEIGHTED_SUM";
            case FusionStrategy::RECIPROCAL_RANK:
                return "RECIPROCAL_RANK";
            case FusionStrategy::WEIGHTED_RECIPROCAL:
                return "WEIGHTED_RECIPROCAL";
            case FusionStrategy::COMB_MNZ:
                return "COMB_MNZ";
        }
        return "UNKNOWN";
    }

    // Component-specific settings (increased for better recall)
    size_t textMaxResults = 300;
    size_t pathTreeMaxResults = 150;
    size_t kgMaxResults = 100;
    size_t vectorMaxResults = 150;
    size_t entityVectorMaxResults = 100;
    size_t tagMaxResults = 250;
    size_t metadataMaxResults = 200;

    // Performance tuning
    bool useConnectionPriority = true;   // Use High priority for search queries
    size_t minChunkSizeForParallel = 50; // Min results for parallel processing

    // Symbol ranking
    bool symbolRank = true; // Enable automatic symbol ranking boost for code-like queries

    // Debugging
    bool includeDebugInfo = false;       // Include per-component scores in results
    bool includeComponentTiming = false; // Include per-component execution time in response

    // Reranking configuration
    bool enableReranking = true;           // Enable reranking (score-based by default)
    size_t rerankTopK = 5;                 // Number of top results to rerank
    size_t rerankSnippetMaxChars = 256;    // Max snippet chars sent to reranker
    float rerankScoreGapThreshold = 0.05f; // Skip rerank if top1-top2 gap exceeds this
    float rerankWeight = 0.60f;            // Blend weight for model reranking
    bool rerankReplaceScores = true;       // If true, replace scores entirely; if false, blend

    // Graph reranking (PR1: post-fusion rerank-only, default off)
    bool enableGraphRerank = false;     // Enable KG-based reranking on fused top-N candidates
    size_t graphRerankTopN = 25;        // Number of fused results to rescore with KG
    float graphRerankWeight = 0.15f;    // Multiplicative weight applied to KG signal
    float graphRerankMaxBoost = 0.20f;  // Per-document cap for graph-induced boost
    float graphRerankMinSignal = 0.01f; // Ignore weak KG signals below this threshold

    // KG scorer budget controls used by graph rerank
    size_t graphMaxNeighbors = 16;           // Neighbor cap per node for structural scoring
    size_t graphMaxHops = 1;                 // Hop depth for path-based graph scoring
    int graphScoringBudgetMs = 10;           // Best-effort total budget for graph scoring call
    bool graphEnablePathEnumeration = false; // Optional bounded path enumeration
    size_t graphMaxPaths = 32;               // Soft cap on path enumeration
    float graphHopDecay = 0.90f;             // Per-hop decay factor when path scoring is enabled

    // Model-based reranking (cross-encoder) - opt-in, requires ONNX model
    bool enableModelReranking = false; // Use cross-encoder model (slow, opt-in)

    // Score-based reranking (default) - uses component scores, no model needed
    // Boosts documents that score well in BOTH text AND vector components
    bool useScoreBasedReranking = true; // Use score-based reranking (fast, default)

    /**
     * @brief Get preset configuration for a corpus profile
     *
     * Returns a SearchEngineConfig with weights tuned for the given profile:
     * - CODE: text=0.40, path=0.15, vector=0.10, entityVector=0.15, kg=0.10, tag=0.05, meta=0.05
     * - PROSE: text=0.45, path=0.10, vector=0.25, entityVector=0.00, kg=0.05, tag=0.10, meta=0.05
     * - DOCS: text=0.40, path=0.15, vector=0.20, entityVector=0.05, kg=0.10, tag=0.05, meta=0.05
     * - MIXED: Default balanced weights
     */
    static SearchEngineConfig forProfile(CorpusProfile profile) {
        SearchEngineConfig config;
        config.corpusProfile = profile;

        switch (profile) {
            case CorpusProfile::CODE:
                config.textWeight = 0.40f;
                config.pathTreeWeight = 0.15f;
                config.kgWeight = 0.10f;
                config.vectorWeight = 0.10f;
                config.entityVectorWeight = 0.15f; // High weight for code symbol search
                config.tagWeight = 0.05f;
                config.metadataWeight = 0.05f;
                break;

            case CorpusProfile::PROSE:
                config.textWeight = 0.45f;
                config.pathTreeWeight = 0.10f;
                config.kgWeight = 0.05f;
                config.vectorWeight = 0.25f;
                config.entityVectorWeight = 0.00f; // Disabled for prose (no symbols)
                config.tagWeight = 0.10f;
                config.metadataWeight = 0.05f;
                break;

            case CorpusProfile::DOCS:
                config.textWeight = 0.40f;
                config.pathTreeWeight = 0.15f;
                config.kgWeight = 0.10f;
                config.vectorWeight = 0.20f;
                config.entityVectorWeight = 0.05f; // Low weight for docs with code snippets
                config.tagWeight = 0.05f;
                config.metadataWeight = 0.05f;
                break;

            case CorpusProfile::MIXED:
            case CorpusProfile::CUSTOM:
            default:
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
    float score; // Component-specific score [0.0, 1.0]
    enum class Source {
        Text,
        PathTree,
        KnowledgeGraph,
        Vector,
        EntityVector,
        Tag,
        Metadata,
        Symbol,
        Unknown
    } source = Source::Unknown;
    size_t rank;                                  // Rank within component results (0-based)
    std::optional<std::string> snippet;           // Optional text snippet
    std::map<std::string, std::string> debugInfo; // Component-specific debug data
};

inline constexpr const char* componentSourceToString(ComponentResult::Source source) noexcept {
    switch (source) {
        case ComponentResult::Source::Text:
            return "text";
        case ComponentResult::Source::PathTree:
            return "path_tree";
        case ComponentResult::Source::KnowledgeGraph:
            return "kg";
        case ComponentResult::Source::Vector:
            return "vector";
        case ComponentResult::Source::EntityVector:
            return "entity_vector";
        case ComponentResult::Source::Tag:
            return "tag";
        case ComponentResult::Source::Metadata:
            return "metadata";
        case ComponentResult::Source::Symbol:
            return "symbol";
        case ComponentResult::Source::Unknown:
            return "unknown";
    }
    return "unknown";
}

inline constexpr bool isVectorComponent(ComponentResult::Source source) noexcept {
    return source == ComponentResult::Source::Vector ||
           source == ComponentResult::Source::EntityVector;
}

inline constexpr bool isTextAnchoringComponent(ComponentResult::Source source) noexcept {
    return source == ComponentResult::Source::Text || source == ComponentResult::Source::PathTree ||
           source == ComponentResult::Source::KnowledgeGraph ||
           source == ComponentResult::Source::Tag || source == ComponentResult::Source::Metadata ||
           source == ComponentResult::Source::Symbol;
}

struct SearchResponse {
    std::vector<SearchResult> results;
    std::vector<SearchFacet> facets;
    std::vector<std::string> timedOutComponents;
    std::vector<std::string> failedComponents;
    std::vector<std::string> contributingComponents;
    std::vector<std::string> skippedComponents; // Components skipped due to early termination
    std::map<std::string, int64_t> componentTimingMicros; // Per-component execution time
    int64_t executionTimeMs = 0;
    bool isDegraded = false;
    bool usedEarlyTermination = false; // True if later tiers were skipped

    [[nodiscard]] bool hasResults() const { return !results.empty(); }
    [[nodiscard]] bool isComplete() const {
        return timedOutComponents.empty() && failedComponents.empty();
    }
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
    std::vector<SearchResult> fuseWeightedSum(const std::vector<ComponentResult>& results);
    std::vector<SearchResult> fuseReciprocalRank(const std::vector<ComponentResult>& results);
    std::vector<SearchResult> fuseWeightedReciprocal(const std::vector<ComponentResult>& results);
    std::vector<SearchResult> fuseCombMNZ(const std::vector<ComponentResult>& results);

    template <typename ScoreFunc>
    std::vector<SearchResult> fuseSinglePass(const std::vector<ComponentResult>& results,
                                             ScoreFunc&& scoreFunc);

    float getComponentWeight(ComponentResult::Source source) const;

    const SearchEngineConfig& config_;
};

// Helper to accumulate a component score into the appropriate breakdown field
// Uses a single switch for clarity and maintainability
inline void accumulateComponentScore(SearchResult& r, ComponentResult::Source source,
                                     double contribution) {
    switch (source) {
        case ComponentResult::Source::Vector:
        case ComponentResult::Source::EntityVector:
            r.vectorScore = r.vectorScore.value_or(0.0) + contribution;
            break;
        case ComponentResult::Source::Text:
            r.keywordScore = r.keywordScore.value_or(0.0) + contribution;
            break;
        case ComponentResult::Source::KnowledgeGraph:
            r.kgScore = r.kgScore.value_or(0.0) + contribution;
            break;
        case ComponentResult::Source::PathTree:
            r.pathScore = r.pathScore.value_or(0.0) + contribution;
            break;
        case ComponentResult::Source::Tag:
        case ComponentResult::Source::Metadata:
            r.tagScore = r.tagScore.value_or(0.0) + contribution;
            break;
        case ComponentResult::Source::Symbol:
            r.symbolScore = r.symbolScore.value_or(0.0) + contribution;
            break;
        default:
            break;
    }
}

// Template implementation - must be in header
template <typename ScoreFunc>
std::vector<SearchResult> ResultFusion::fuseSinglePass(const std::vector<ComponentResult>& results,
                                                       ScoreFunc&& scoreFunc) {
    // Single map to accumulate scores directly into SearchResult objects
    std::unordered_map<std::string, SearchResult> resultMap;
    resultMap.reserve(results.size());
    std::unordered_map<std::string, double> maxVectorRawScore;
    maxVectorRawScore.reserve(results.size());
    std::unordered_set<std::string> anchoredDocs;
    anchoredDocs.reserve(results.size());

    // Single pass: accumulate scores directly
    for (const auto& comp : results) {
        auto& r = resultMap[comp.documentHash];
        if (r.document.sha256Hash.empty()) {
            // First time seeing this document - initialize
            r.document.sha256Hash = comp.documentHash;
            r.document.filePath = comp.filePath;
            r.score = 0.0;
        }

        // Calculate this component's contribution
        const double contribution = scoreFunc(comp);

        // Accumulate total score
        r.score += contribution;

        // Track per-component breakdown
        accumulateComponentScore(r, comp.source, contribution);

        if (isVectorComponent(comp.source)) {
            const double clampedRaw = std::clamp(static_cast<double>(comp.score), 0.0, 1.0);
            auto [it, inserted] = maxVectorRawScore.try_emplace(comp.documentHash, clampedRaw);
            if (!inserted) {
                it->second = std::max(it->second, clampedRaw);
            }
        } else if (isTextAnchoringComponent(comp.source)) {
            anchoredDocs.insert(comp.documentHash);
        }

        // Use first available snippet
        if (r.snippet.empty() && comp.snippet.has_value()) {
            r.snippet = comp.snippet.value();
        }
    }

    // Extract results from map
    std::vector<SearchResult> fusedResults;
    fusedResults.reserve(resultMap.size());
    for (auto& [hash, r] : resultMap) {
        const bool hasAnchoring = anchoredDocs.contains(hash);
        const auto vecIt = maxVectorRawScore.find(hash);
        const bool hasVector = vecIt != maxVectorRawScore.end();

        if (hasVector && !hasAnchoring) {
            if (vecIt->second < static_cast<double>(config_.vectorOnlyThreshold)) {
                continue;
            }
            r.score *= static_cast<double>(config_.vectorOnlyPenalty);
        }

        if (hasVector && hasAnchoring && config_.vectorBoostFactor > 0.0f) {
            const double vectorContribution = r.vectorScore.value_or(0.0);
            const double anchorContribution =
                r.keywordScore.value_or(0.0) + r.pathScore.value_or(0.0) + r.kgScore.value_or(0.0) +
                r.tagScore.value_or(0.0) + r.symbolScore.value_or(0.0);

            if (vectorContribution > 0.0 && anchorContribution > 0.0) {
                const double agreement = (2.0 * std::min(vectorContribution, anchorContribution)) /
                                         (vectorContribution + anchorContribution);
                const double boostFactor =
                    std::clamp(static_cast<double>(config_.vectorBoostFactor), 0.0, 0.10);
                r.score *= (1.0 + boostFactor * agreement);
            }
        }

        fusedResults.emplace_back(std::move(r));
    }

    // Single sort at the end
    if (fusedResults.size() > config_.maxResults) {
        std::partial_sort(
            fusedResults.begin(), fusedResults.begin() + static_cast<ptrdiff_t>(config_.maxResults),
            fusedResults.end(),
            [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });
        fusedResults.resize(config_.maxResults);
    } else {
        std::sort(fusedResults.begin(), fusedResults.end(),
                  [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });
    }

    return fusedResults;
}

/**
 * @brief Multi-component parallel search engine
 *
 * This engine:
 * 1. Queries all available metadata structures in parallel:
 *    - FTS5 full-text search (documents_fts)
 *    - Path tree hierarchical search (path_tree_nodes)
 *    - Symbol metadata search (symbol_metadata)
 *    - Knowledge graph traversal (kg_nodes, kg_edges, kg_aliases)
 *    - Vector similarity search (VectorDatabase with sqlite-vec)
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
     * @param vectorDb Vector database for similarity search (sqlite-vec)
     * @param embeddingGen Embedding generator for query vectorization
     * @param kgStore Knowledge graph store for entity/alias queries (optional)
     * @param config Search engine configuration
     */
    explicit SearchEngine(std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo,
                          std::shared_ptr<vector::VectorDatabase> vectorDb,
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
        std::atomic<uint64_t> totalQueries{0};
        std::atomic<uint64_t> successfulQueries{0};
        std::atomic<uint64_t> failedQueries{0};
        std::atomic<uint64_t> timedOutQueries{0};

        std::atomic<uint64_t> textQueries{0};
        std::atomic<uint64_t> pathTreeQueries{0};
        std::atomic<uint64_t> kgQueries{0};
        std::atomic<uint64_t> vectorQueries{0};
        std::atomic<uint64_t> entityVectorQueries{0};
        std::atomic<uint64_t> tagQueries{0};
        std::atomic<uint64_t> metadataQueries{0};

        std::atomic<uint64_t> totalQueryTimeMicros{0};
        std::atomic<uint64_t> avgQueryTimeMicros{0};

        std::atomic<uint64_t> avgTextTimeMicros{0};
        std::atomic<uint64_t> avgPathTreeTimeMicros{0};
        std::atomic<uint64_t> avgKgTimeMicros{0};
        std::atomic<uint64_t> avgVectorTimeMicros{0};
        std::atomic<uint64_t> avgEntityVectorTimeMicros{0};
        std::atomic<uint64_t> avgTagTimeMicros{0};
        std::atomic<uint64_t> avgMetadataTimeMicros{0};

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

    /**
     * @brief Set executor for parallel component queries
     *
     * When an executor is set, SearchEngine uses boost::asio::post instead of
     * std::async for parallel component execution. This allows reuse of a thread pool
     * (e.g., WorkCoordinator's io_context) instead of creating new threads per query.
     *
     * @param executor Optional executor. If empty/nullopt, falls back to std::async.
     */
    void setExecutor(std::optional<boost::asio::any_io_executor> executor);

    /**
     * @brief Set the concept extractor for GLiNER-based entity extraction
     * @param extractor Function to extract concepts from query text
     */
    void setConceptExtractor(EntityExtractionFunc extractor);

    /**
     * @brief Set the reranker for cross-encoder second-stage ranking
     *
     * When a reranker is set and config.enableReranking is true, search results
     * are passed through the cross-encoder for improved relevance scoring.
     *
     * @param reranker The reranker implementation (or nullptr to disable)
     */
    void setReranker(std::shared_ptr<IReranker> reranker);

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
                   std::shared_ptr<vector::EmbeddingGenerator> embeddingGen,
                   std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore,
                   const SearchEngineConfig& config = {});

} // namespace yams::search
