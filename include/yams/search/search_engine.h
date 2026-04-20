#pragma once

#include <yams/core/types.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/query_concept_extractor.h>
#include <yams/search/search_results.h>
#include <yams/topology/topology_artifacts.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cmath>
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

class SearchTuner;

// Import SearchResult from metadata namespace
using yams::metadata::SearchResult;

/**
 * @brief Search parameters for query execution
 */
struct SearchParams {
    int limit = 100;
    int offset = 0;
    bool semanticOnly = false;

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

    /**
     * @brief Navigation zoom level for machine-first retrieval.
     *
     * Auto lets the engine infer an effective zoom from query intent. Explicit zoom levels bias the
     * engine toward high-level topology (Map), balanced dependency exploration (Neighborhood), or
     * detail-preserving retrieval (Street).
     */
    enum class NavigationZoomLevel {
        Auto,
        Map,
        Neighborhood,
        Street,
    } zoomLevel = NavigationZoomLevel::Auto;

    [[nodiscard]] static constexpr const char*
    navigationZoomLevelToString(NavigationZoomLevel level) noexcept {
        switch (level) {
            case NavigationZoomLevel::Auto:
                return "AUTO";
            case NavigationZoomLevel::Map:
                return "MAP";
            case NavigationZoomLevel::Neighborhood:
                return "NEIGHBORHOOD";
            case NavigationZoomLevel::Street:
                return "STREET";
        }
        return "AUTO";
    }

    // Component weights (0.0 = disabled, 1.0 = full weight)
    float textWeight = 0.70f;          // Full-text search weight
    float graphTextWeight = 0.12f;     // Graph-expanded text search weight
    float pathTreeWeight = 0.08f;      // Path tree hierarchical weight
    float kgWeight = 0.04f;            // Knowledge graph weight
    float vectorWeight = 0.30f;        // Vector similarity weight (increased for hybrid value)
    float graphVectorWeight = 0.08f;   // Graph-expanded vector search weight
    float vectorOnlyPenalty = 0.8f;    // Penalty for vector-only results (no text match)
    float vectorOnlyThreshold = 0.90f; // Minimum confidence for vector-only inclusion
    bool enableStrongVectorOnlyRelief =
        false; // Relax vector-only penalty for very strong semantic-only candidates
    float strongVectorOnlyMinScore =
        0.97f; // Raw vector score threshold where relief starts applying
    size_t strongVectorOnlyTopRank =
        0; // Vector rank threshold where relief starts applying (0 disables rank-based relief)
    float strongVectorOnlyPenalty =
        0.95f; // Maximum effective penalty for strong semantic-only candidates
    size_t vectorOnlyNearMissReserve =
        0; // Keep up to N near-threshold vector-only docs (0 disables reserve)
    float vectorOnlyNearMissSlack = 0.05f; // Near-miss band below threshold eligible for reserve
    float vectorOnlyNearMissPenalty =
        0.60f;                        // Additional penalty factor for reserved near-miss docs
    float vectorBoostFactor = 0.10f;  // Boost factor for vector re-ranking
    float entityVectorWeight = 0.05f; // Entity (symbol) vector similarity weight
    float tagWeight = 0.05f;          // Tag-based search weight (modifier, not standalone)
    float metadataWeight = 0.05f; // Metadata attribute matching weight (modifier, not standalone)

    // Concept-based boosts (GLiNER query concepts)
    float conceptBoostWeight = 0.10f;     // Per-concept boost factor applied to matches
    float conceptMinConfidence = 0.40f;   // Minimum confidence for concept inclusion
    size_t conceptMaxCount = 6;           // Maximum number of concepts to apply per query
    float conceptMaxBoost = 0.25f;        // Global boost budget per query (sum of applied boosts)
    size_t conceptMaxScanResults = 200;   // Maximum results to scan for concept matches
    bool waitForConceptExtraction = true; // Wait for concept extraction before applying boosts

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
    bool adaptiveVectorSkipRequireTextSignal =
        true; // Require strong text signal before skipping semantic tier
    size_t adaptiveVectorSkipMinTextHits =
        3; // Minimum text hits required to allow semantic skip when text-signal gating enabled
    float adaptiveVectorSkipMinTopTextScore =
        0.30f; // Minimum top text score required to allow semantic skip
    bool enableWeakQueryFanoutBoost =
        true; // Expand vector/entity candidate fanout when Tier 1 lexical signal is weak
    float weakQueryVectorFanoutMultiplier =
        2.0f; // Multiplier for vectorMaxResults when weak-query boost triggers
    float weakQueryEntityVectorFanoutMultiplier =
        1.5f; // Multiplier for entityVectorMaxResults when weak-query boost triggers
    bool enableTopologyWeakQueryRouting =
        false; // Expand weak-query vector candidates via topology clusters seeded by Tier 1 docs
    size_t topologyWeakQueryMaxClusters = 2; // Maximum topology clusters to route through
    size_t topologyWeakQueryMaxDocs = 64;    // Maximum routed document hashes added to Tier 2
    float topologyRoutedBaseMultiplier = 0.85f;
    float topologyMedoidBoost = 0.05f;
    float topologyBridgeBoost = 0.03f;
    enum class TopologyRoutingVariant {
        Baseline,
        VectorSeed,
        KgWalk,
        ScoreReplace,
        MedoidPromote,
    };
    TopologyRoutingVariant topologyRoutingVariant = TopologyRoutingVariant::Baseline;

    enum class TopologyIntegration {
        Boost,        // Apply medoid/bridge boost (baseline behavior)
        RecallExpand, // Disable boost; use routed-cluster expansion only
        Rrf,          // Keep boost, force RECIPROCAL_RANK fusion with overridden rrfK
        Both,         // RecallExpand + Rrf combined
    };
    TopologyIntegration topologyIntegration = TopologyIntegration::Boost;
    size_t topologyRecallExpandPerCluster = 0;

    enum class TopologyRouteScoring {
        Current,      // persistence + 0.5*cohesion + matchedSeed + memberCount/40
        SizeWeighted, // current * 1/(1+log(size))
        SeedCoverage, // seeds-in-cluster / seeds-total (+ small persistence/size bias)
    };
    TopologyRouteScoring topologyRouteScoring = TopologyRouteScoring::Current;

    bool bypassCorpusWarmingGate = false;

    // RRF (Reciprocal Rank Fusion) parameter
    // Lower k = more weight on top-ranked items (better precision/MRR)
    // Higher k = smoother ranking across positions (better recall)
    float rrfK = 12.0f; // Lower k for sharper top-rank discrimination

    // BM25 score normalization divisor (typical range: 20-30)
    float bm25NormDivisor = 25.0f;

    // Benchmarking support
    bool enableProfiling = false;

    // Result fusion strategy
    enum class FusionStrategy {
        WEIGHTED_SUM,          // Sum of weighted scores (simple baseline)
        RECIPROCAL_RANK,       // Standard Reciprocal Rank Fusion
        WEIGHTED_RECIPROCAL,   // RRF with score boost (good default)
        COMB_MNZ,              // CombMNZ: score * num_components (recall-focused)
        CONVEX,                // Convex combination of per-component normalized scores
        WEIGHTED_LINEAR_ZSCORE // Cascade: BM25 top-K pool, alpha-weighted z-scored
                               // linear combination of lexical+vector components
                               // (training-free recipe; defaults match the simeon
                               // bench's headline cascade row)
    } fusionStrategy = FusionStrategy::COMB_MNZ; // Default: recall-focused

    // WEIGHTED_LINEAR_ZSCORE knobs. Defaults reproduce simeon's
    // bm25_pool500_linear_alpha075 bench row on BEIR scifact.
    size_t weightedLinearZScorePoolSize = 500; // BM25 (lexical) pool depth
    float weightedLinearZScoreAlpha = 0.75f;   // Weight on the lexical leg in [0,1]
    bool weightedLinearZScoreUseZScore = true; // Off => raw scores (still alpha-mixed)

    // P7: When true, SearchEngine overrides fusionStrategy to CONVEX once the
    // SearchTuner reports convergence. Default off preserves existing behavior.
    bool enableAdaptiveFusion = false;

    // Chunk-to-document aggregation strategy for vector search results.
    // Controls how multiple chunk-level vector hits from the same document are combined.
    enum class ChunkAggregation {
        MAX,      // Keep only the highest-scoring chunk per document (default)
        SUM,      // Sum chunk scores per document (capped at 1.0)
        TOP_K_AVG // Average of top-K chunk scores per document (K=3)
    } chunkAggregation = ChunkAggregation::MAX;
    size_t chunkAggregationTopK = 3; // K for TOP_K_AVG strategy

    // Hybrid behavior flags for experimentation and tuning
    bool enableIntentAdaptiveWeighting = true; // Apply query-intent scaling at runtime
    bool enableFieldAwareWeightedRrf = true;   // Use source-specific scaling in WEIGHTED_RECIPROCAL
    bool enableLexicalExpansion = false;       // Enable fallback lexical expansion for sparse hits
    size_t lexicalExpansionMinHits = 3;        // Trigger expansion when primary FTS hits below this
    float lexicalExpansionScorePenalty = 0.65f; // Penalty applied to expanded-only FTS matches
    bool enablePathDedupInFusion = false; // Merge multi-hash results that resolve to same path
    size_t lexicalFloorTopN = 0;          // Apply lexical floor to top-N text ranks (0 disables)
    float lexicalFloorBoost = 0.0f;       // Additive floor boost for protected lexical docs
    bool enableLexicalTieBreak = false; // Prefer lexical evidence when fused scores are near-equal
    float lexicalTieBreakEpsilon =
        0.0f; // Score delta threshold for lexical tie-break (0 = exact ties only)
    size_t semanticRescueSlots =
        0; // Reserve up to N top-k slots for strong vector-only candidates (0 disables)
    float semanticRescueMinVectorScore =
        0.0f; // Minimum vector contribution for semantic rescue eligibility
    size_t fusionEvidenceRescueSlots =
        0; // Reserve up to N top-k slots for strong evidence docs below the fusion cutoff
    float fusionEvidenceRescueMinScore =
        0.0f; // Minimum raw evidence score for fusion evidence rescue eligibility

    // Multi-vector sub-phrase search: decompose query into overlapping sub-phrases,
    // embed each independently, and run separate HNSW searches. Results are merged
    // (max score per document) before entering fusion. Addresses vocabulary mismatch
    // where the full-query embedding misses semantically related documents.
    bool enableMultiVectorQuery = false; // Enable sub-phrase vector search
    size_t multiVectorMaxPhrases = 3;    // Max sub-phrases (in addition to original query)
    float multiVectorScoreDecay = 0.85f; // Score penalty for sub-phrase results vs original

    // Enhanced sub-phrase FTS expansion: when primary FTS hits are sparse, generate
    // overlapping content-word sub-phrases and run OR queries for each. Broader than
    // the basic lexical expansion which only ORs individual tokens.
    bool enableSubPhraseExpansion = false;   // Enable sub-phrase FTS expansion
    size_t subPhraseExpansionMinHits = 5;    // Trigger when primary FTS hits below this
    float subPhraseExpansionPenalty = 0.70f; // Score penalty for sub-phrase FTS results

    // Sub-phrase rescoring: unlike expansion (which adds new docs when hits are sparse),
    // this pass fires unconditionally and updates scores of already-retrieved documents
    // using sub-phrase AND-clause queries. Improves ranking for prose/claim queries where
    // base BM25 ranks relevant docs low due to vocabulary mismatch — the expansion gates
    // (baseFtsHitCount < N) never fire when the full corpus is retrieved at low scores.
    bool enableSubPhraseRescoring = false; // Enable always-on sub-phrase score-update pass
    float subPhraseScoringPenalty = 0.70f; // Score multiplier applied to sub-phrase match signal

    // Graph-expanded retrieval: resolve query concepts/aliases into KG nodes, walk a small
    // neighborhood, and use resulting entity labels as early FTS/vector expansion terms.
    bool enableGraphQueryExpansion = false;   // Enable graph -> FTS/vector expansion pre-fusion
    size_t graphExpansionMinHits = 8;         // Trigger when primary FTS hits below this threshold
    size_t graphExpansionMaxTerms = 8;        // Max graph-derived labels/phrases to use
    size_t graphExpansionMaxSeeds = 6;        // Max resolved query seed nodes
    size_t graphExpansionQueryNeighborK = 12; // Top document-level vector neighbors to inspect
    float graphExpansionQueryNeighborMinScore =
        0.84f; // Minimum document-level similarity for graph neighbor seeding
    bool graphVectorRequireCorroboration =
        true; // Require baseline text/vector/KG evidence before admitting GraphVector docs
    bool graphVectorRequireTextAnchoring =
        true; // Require text-like anchoring before admitting GraphVector docs
    bool graphVectorRequireBaselineTextAnchoring =
        true; // Require baseline non-graph text/KG/symbol anchoring before admitting GraphVector
              // docs
    bool enableGraphFusionWindowGuard =
        false; // Legacy graphless-window replacement guard (too blunt; disabled by default)
    size_t graphFusionGuardDepthMultiplier = 2; // Graph-added docs may intrude only if they are
                                                // within top-(window * multiplier) graphless ranks
    size_t graphMaxAddedInFusionWindow = 0; // Cap graph-added docs in fused window (0 disables cap)
    float graphTextMinAdmissionScore =
        0.0010f; // Minimum GraphText fused contribution to admit a graph-expanded text doc
    float graphExpansionFtsPenalty = 0.78f;    // Penalty applied to graph-expanded FTS matches
    float graphExpansionVectorPenalty = 0.82f; // Penalty applied to graph-expanded vector matches

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
            case FusionStrategy::CONVEX:
                return "CONVEX";
            case FusionStrategy::WEIGHTED_LINEAR_ZSCORE:
                return "WEIGHTED_LINEAR_ZSCORE";
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
    // Minimum score ratio for a multi-source doc to count as "competitive anchored evidence"
    // and override the score-gap guard. 0.0 = any multi-source doc counts (prior behavior).
    // E.g. 0.70 means the competitor must score >= 70% of rank-1's score.
    float rerankAnchoredMinRelativeScore = 0.0f;
    float rerankWeight = 0.60f;        // Blend weight for model reranking
    bool rerankReplaceScores = true;   // If true, replace scores entirely; if false, blend
    bool rerankAdaptiveBlend = false;  // Scale blend weight by reranker confidence
    float rerankAdaptiveFloor = 0.10f; // Minimum effective blend weight when adaptive

    // Fusion candidate limit: how many fused results to keep before reranking
    // 0 = auto: max(userLimit, max(rerankTopK, graphRerankTopN))
    size_t fusionCandidateLimit = 0;

    // Graph reranking (PR1: post-fusion rerank-only, default off)
    bool enableGraphRerank = false;     // Enable KG-based reranking on fused top-N candidates
    size_t graphRerankTopN = 25;        // Number of fused results to rescore with KG
    float graphRerankWeight = 0.15f;    // Multiplicative weight applied to KG signal
    float graphRerankMaxBoost = 0.20f;  // Per-document cap for graph-induced boost
    float graphRerankMinSignal = 0.01f; // Ignore weak KG signals below this threshold
    float graphCommunityWeight = 0.10f; // Share of raw graph signal reserved for reciprocal support
    // Reference "meaningful community size" used to normalize reciprocal-community support.
    // Normalization: clamp((component_size - 1) / (reference - 1), 0, 1). Stable across
    // candidate-set widths so identical structural support produces stable absolute scores.
    // Set <= 1.0f to fall back to the legacy candidate-set-adaptive normalizer.
    float graphCommunityReferenceSize = 8.0f;
    // P8: temporal half-life for edge weight in reciprocal community support, in days.
    // 0.0f (default) disables decay, preserving binary-equivalent behavior. When > 0,
    // decayed_weight = edge.weight * exp(-ln(2) * age_days / halfLifeDays). Edges without
    // createdTime are treated as no-decay.
    float graphCommunityDecayHalfLifeDays = 0.0f;
    // P8: minimum decayed edge weight required to count as a reciprocal link. Guards against
    // arbitrarily-weak edges pulling docs into communities. Default 0 means any reciprocal edge
    // counts (matches pre-P8 binary behavior).
    float graphCommunityMinEdgeWeight = 0.0f;
    bool graphUseQueryConcepts = true; // Enrich graph rerank query with extracted concepts
    bool graphFallbackToTopSignal =
        true; // If no candidate clears minSignal, still boost the top positive graph hit

    // KG scorer budget controls used by graph rerank
    size_t graphMaxNeighbors = 16;           // Neighbor cap per node for structural scoring
    size_t graphMaxHops = 1;                 // Hop depth for path-based graph scoring
    int graphScoringBudgetMs = 10;           // Best-effort total budget for graph scoring call
    bool graphEnablePathEnumeration = false; // Optional bounded path enumeration
    size_t graphMaxPaths = 32;               // Soft cap on path enumeration
    float graphHopDecay = 0.90f;             // Per-hop decay factor when path scoring is enabled

    // Per-profile graph signal weights for reranking composition
    float graphEntitySignalWeight = 0.40F;
    float graphStructuralSignalWeight = 0.20F;
    float graphCoverageSignalWeight = 0.20F;
    float graphPathSignalWeight = 0.10F;
    float graphCorroborationFloor = 0.35F;

    // Model-based reranking (cross-encoder) - opt-in, requires ONNX model
    bool enableModelReranking = false; // Use cross-encoder model (slow, opt-in)

    // Score-based reranking (default) - uses component scores, no model needed
    // Boosts documents that score well in BOTH text AND vector components
    bool useScoreBasedReranking = true; // Use score-based reranking (fast, default)

    // TurboQuant packed-code reranking: stable default for packed-code corpora.
    // Runs after first-stage fusion and before optional cross-encoder reranking.
    // Candidates without packed codes fall through unchanged when
    // turboQuantRerankOnlyWhenPackedAvailable is true.
    bool enableTurboQuantRerank = true;   // Enable TurboQuant vector reranking
    size_t turboQuantRerankWindow = 50;   // Max candidates to rerank with TurboQuant
    float turboQuantRerankWeight = 0.50f; // Blend weight for TurboQuant score (0-1)
    uint8_t turboQuantRerankBits = 4;     // Bits per channel expected in stored packed codes
    size_t turboQuantRerankDim = 384;     // Embedding dimension expected by reranker
    bool turboQuantRerankOnlyWhenPackedAvailable =
        true; // Only rerank candidates with packed codes; others fall through

    // Compressed ANN traversal: opt-in packed-space retrieval path.
    // Runs as an additional vector component; the regular vector path remains available.
    // Callers must keep the bit depth aligned with the stored TurboQuant sidecar.
    bool enableCompressedANN = false;  // Enable compressed ANN traversal
    float compressedAnnWeight = 0.10f; // Fusion weight for compressed ANN candidates
    size_t compressedAnnTopK = 50;     // Number of results from compressed ANN search
    size_t compressedAnnEfSearch = 50; // ef_search parameter for CompressedANNIndex
    uint8_t compressedAnnBits = 4;     // Bits per channel for compressed ANN (must match storage)
    size_t compressedAnnDim = 384;     // Embedding dimension (must match stored vectors)
    bool compressedAnnFallbackToRerank =
        true; // Reserved for explicit fallback orchestration; current default path keeps vector
              // search plus TurboQuant reranking active

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
        GraphText,
        PathTree,
        KnowledgeGraph,
        Vector,
        CompressedANN,
        GraphVector,
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
        case ComponentResult::Source::GraphText:
            return "graph_text";
        case ComponentResult::Source::PathTree:
            return "path_tree";
        case ComponentResult::Source::KnowledgeGraph:
            return "kg";
        case ComponentResult::Source::Vector:
            return "vector";
        case ComponentResult::Source::CompressedANN:
            return "compressed_ann";
        case ComponentResult::Source::GraphVector:
            return "graph_vector";
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

inline float componentSourceWeight(const SearchEngineConfig& config,
                                   ComponentResult::Source source) noexcept {
    switch (source) {
        case ComponentResult::Source::Text:
            return config.textWeight;
        case ComponentResult::Source::GraphText:
            return config.graphTextWeight;
        case ComponentResult::Source::PathTree:
            return config.pathTreeWeight;
        case ComponentResult::Source::KnowledgeGraph:
            return config.kgWeight;
        case ComponentResult::Source::Vector:
            return config.vectorWeight;
        case ComponentResult::Source::CompressedANN:
            return config.compressedAnnWeight;
        case ComponentResult::Source::GraphVector:
            return config.graphVectorWeight;
        case ComponentResult::Source::EntityVector:
            return config.entityVectorWeight;
        case ComponentResult::Source::Tag:
            return config.tagWeight;
        case ComponentResult::Source::Metadata:
            return config.metadataWeight;
        case ComponentResult::Source::Symbol:
        case ComponentResult::Source::Unknown:
            return 0.0f;
    }
    return 0.0f;
}

inline constexpr bool isVectorComponent(ComponentResult::Source source) noexcept {
    return source == ComponentResult::Source::Vector ||
           source == ComponentResult::Source::CompressedANN ||
           source == ComponentResult::Source::GraphVector ||
           source == ComponentResult::Source::EntityVector;
}

inline constexpr bool isTextAnchoringComponent(ComponentResult::Source source) noexcept {
    return source == ComponentResult::Source::Text ||
           source == ComponentResult::Source::GraphText ||
           source == ComponentResult::Source::PathTree ||
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
    std::map<std::string, int64_t> componentTimingMicros;    // Per-component execution time
    std::unordered_map<std::string, std::string> debugStats; // Opt-in diagnostics and traces
    int64_t executionTimeMs = 0;
    bool isDegraded = false;
    bool usedEarlyTermination = false; // True if later tiers were skipped

    [[nodiscard]] bool hasResults() const { return !results.empty(); }
    [[nodiscard]] bool isComplete() const {
        return timedOutComponents.empty() && failedComponents.empty();
    }
};

namespace detail {

/// Unified dedup key for fusion stages. Works with both ComponentResult
/// (has .documentHash/.filePath) and SearchResult (has .document.sha256Hash/.document.filePath).
template <typename T> std::string makeFusionDedupKey(const T& item, bool enablePathDedup) {
    const auto& filePath = [&]() -> const std::string& {
        if constexpr (requires { item.filePath; })
            return item.filePath;
        else
            return item.document.filePath;
    }();
    const auto& hash = [&]() -> const std::string& {
        if constexpr (requires { item.documentHash; })
            return item.documentHash;
        else
            return item.document.sha256Hash;
    }();
    if (enablePathDedup && !filePath.empty()) {
        return "path:" + filePath;
    }
    if (!hash.empty()) {
        return "hash:" + hash;
    }
    if (!filePath.empty()) {
        return "path:" + filePath;
    }
    return "unknown:";
}

} // namespace detail

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
    std::vector<SearchResult> fuseConvex(const std::vector<ComponentResult>& results);
    std::vector<SearchResult> fuseWeightedLinearZScore(const std::vector<ComponentResult>& results);

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
        case ComponentResult::Source::CompressedANN:
        case ComponentResult::Source::EntityVector:
            r.vectorScore = r.vectorScore.value_or(0.0) + contribution;
            break;
        case ComponentResult::Source::GraphVector:
            r.graphVectorScore = r.graphVectorScore.value_or(0.0) + contribution;
            break;
        case ComponentResult::Source::Text:
            r.keywordScore = r.keywordScore.value_or(0.0) + contribution;
            break;
        case ComponentResult::Source::GraphText:
            r.graphTextScore = r.graphTextScore.value_or(0.0) + contribution;
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

inline double componentSourceScoreInResult(const SearchResult& r,
                                           ComponentResult::Source source) noexcept {
    switch (source) {
        case ComponentResult::Source::Vector:
        case ComponentResult::Source::CompressedANN:
        case ComponentResult::Source::EntityVector:
            return r.vectorScore.value_or(0.0);
        case ComponentResult::Source::GraphVector:
            return r.graphVectorScore.value_or(0.0);
        case ComponentResult::Source::Text:
            return r.keywordScore.value_or(0.0);
        case ComponentResult::Source::GraphText:
            return r.graphTextScore.value_or(0.0);
        case ComponentResult::Source::KnowledgeGraph:
            return r.kgScore.value_or(0.0);
        case ComponentResult::Source::PathTree:
            return r.pathScore.value_or(0.0);
        case ComponentResult::Source::Tag:
        case ComponentResult::Source::Metadata:
            return r.tagScore.value_or(0.0);
        case ComponentResult::Source::Symbol:
            return r.symbolScore.value_or(0.0);
        case ComponentResult::Source::Unknown:
            return 0.0;
    }
    return 0.0;
}

/**
 * @brief Extract document ID from component result for deduplication
 *
 * Uses filePath as the primary dedup key (if non-empty), otherwise falls back
 * to documentHash. This mirrors the dedup logic used in ResultFusion.
 *
 * @param comp Component result to extract ID from
 * @return Document ID string, or empty string if neither is available
 */
inline std::string documentIdFromComponent(const ComponentResult& comp) noexcept {
    if (!comp.filePath.empty()) {
        return comp.filePath;
    }
    if (!comp.documentHash.empty()) {
        return comp.documentHash;
    }
    return {};
}

/**
 * @brief Prune duplicate compressed ANN results before fusion
 *
 * This function implements the pre-fusion dedup logic:
 * 1. First pass: collect all exact Vector document IDs
 * 2. Second pass: for each CompressedANN result:
 *    - If doc ID is in exact Vector results -> remove (exact vector takes precedence)
 *    - If doc ID was already seen in compressed ANN -> remove (dedup)
 *    - If doc ID is novel -> keep it
 *
 * This ensures:
 * - compressed-ANN doc duplicated by exact vector is removed
 * - duplicate compressed-ANN docs collapse to one
 * - exact vector docs remain untouched
 * - unique compressed-ANN docs survive
 *
 * @param components Input vector of component results
 * @return Pruned vector with duplicate compressed ANN results removed
 */
inline std::vector<ComponentResult>
pruneDuplicateCompressedAnnResults(std::vector<ComponentResult> components) {
    std::unordered_set<std::string> exactVectorDocIds;
    exactVectorDocIds.reserve(components.size());

    // First pass: collect exact Vector document IDs
    for (const auto& comp : components) {
        if (comp.source == ComponentResult::Source::Vector) {
            const auto docId = documentIdFromComponent(comp);
            if (!docId.empty()) {
                exactVectorDocIds.insert(docId);
            }
        }
    }

    // Second pass: prune duplicate compressed ANN results
    std::unordered_set<std::string> compressedAnnDocIds;
    compressedAnnDocIds.reserve(components.size());

    components.erase(std::remove_if(components.begin(), components.end(),
                                    [&](const ComponentResult& comp) {
                                        if (comp.source != ComponentResult::Source::CompressedANN) {
                                            return false;
                                        }
                                        const auto docId = documentIdFromComponent(comp);
                                        if (docId.empty()) {
                                            return false;
                                        }
                                        // Remove if already covered by exact vector
                                        if (exactVectorDocIds.contains(docId)) {
                                            return true;
                                        }
                                        // Remove if duplicate compressed ANN
                                        return !compressedAnnDocIds.insert(docId).second;
                                    }),
                     components.end());

    return components;
}

inline double strongVectorOnlyReliefStrength(const SearchEngineConfig& config, double rawVector,
                                             size_t bestVectorRank) {
    if (!config.enableStrongVectorOnlyRelief) {
        return 0.0;
    }

    const double minScore =
        std::clamp(static_cast<double>(config.strongVectorOnlyMinScore), 0.0, 1.0);
    double rawStrength = 0.0;
    if (rawVector >= minScore) {
        if (minScore >= 1.0) {
            rawStrength = 1.0;
        } else {
            rawStrength = std::clamp((rawVector - minScore) / (1.0 - minScore), 0.0, 1.0);
        }
    }

    double rankStrength = 0.0;
    if (config.strongVectorOnlyTopRank > 0 &&
        bestVectorRank != std::numeric_limits<size_t>::max() &&
        bestVectorRank < config.strongVectorOnlyTopRank) {
        rankStrength =
            std::clamp(static_cast<double>(config.strongVectorOnlyTopRank - bestVectorRank) /
                           static_cast<double>(config.strongVectorOnlyTopRank),
                       0.0, 1.0);
    }

    return std::max(rawStrength, rankStrength);
}

inline bool strongVectorOnlyReliefEligible(const SearchEngineConfig& config, double rawVector,
                                           size_t bestVectorRank) {
    return strongVectorOnlyReliefStrength(config, rawVector, bestVectorRank) > 0.0;
}

inline double effectiveVectorOnlyPenalty(const SearchEngineConfig& config, double rawVector,
                                         size_t bestVectorRank) {
    const double basePenalty = std::clamp(static_cast<double>(config.vectorOnlyPenalty), 0.0, 1.0);
    if (!config.enableStrongVectorOnlyRelief) {
        return basePenalty;
    }

    const double reliefPenalty =
        std::clamp(static_cast<double>(config.strongVectorOnlyPenalty), basePenalty, 1.0);
    const double t = strongVectorOnlyReliefStrength(config, rawVector, bestVectorRank);
    return basePenalty + (reliefPenalty - basePenalty) * t;
}

inline size_t semanticRescueWindowLimit(const SearchEngineConfig& config) {
    if (config.enableReranking && config.rerankTopK > 0) {
        return std::min(config.maxResults, config.rerankTopK);
    }
    return config.maxResults;
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
    std::unordered_map<std::string, size_t> bestTextRank;
    bestTextRank.reserve(results.size());
    std::unordered_map<std::string, size_t> bestVectorRank;
    bestVectorRank.reserve(results.size());

    // Single pass: accumulate scores directly
    for (const auto& comp : results) {
        const std::string dedupKey =
            detail::makeFusionDedupKey(comp, config_.enablePathDedupInFusion);
        auto& r = resultMap[dedupKey];
        if (r.document.sha256Hash.empty()) {
            // First time seeing this document - initialize
            r.document.sha256Hash = comp.documentHash;
            r.document.filePath = comp.filePath;
            r.score = 0.0;
        } else {
            if (r.document.filePath.empty() && !comp.filePath.empty()) {
                r.document.filePath = comp.filePath;
            }
        }

        // Calculate this component's contribution
        const double contribution = scoreFunc(comp);

        // Accumulate total score
        r.score += contribution;

        // Track per-component breakdown
        accumulateComponentScore(r, comp.source, contribution);

        if (comp.source == ComponentResult::Source::Text) {
            auto [it, inserted] = bestTextRank.try_emplace(dedupKey, comp.rank);
            if (!inserted) {
                it->second = std::min(it->second, comp.rank);
            }
        }

        if (isVectorComponent(comp.source)) {
            const double clampedRaw = std::clamp(static_cast<double>(comp.score), 0.0, 1.0);
            auto [it, inserted] = maxVectorRawScore.try_emplace(dedupKey, clampedRaw);
            if (!inserted) {
                it->second = std::max(it->second, clampedRaw);
            }
            auto [rankIt, rankInserted] = bestVectorRank.try_emplace(dedupKey, comp.rank);
            if (!rankInserted) {
                rankIt->second = std::min(rankIt->second, comp.rank);
            }
        } else if (isTextAnchoringComponent(comp.source)) {
            anchoredDocs.insert(dedupKey);
        }

        // Use first available snippet
        if (r.snippet.empty() && comp.snippet.has_value()) {
            r.snippet = comp.snippet.value();
        }
    }

    // Extract results from map
    std::vector<SearchResult> fusedResults;
    fusedResults.reserve(resultMap.size());
    std::vector<std::pair<double, SearchResult>> nearMissReserve;
    nearMissReserve.reserve(std::min(config_.vectorOnlyNearMissReserve, resultMap.size()));

    const double vectorOnlyThreshold =
        std::clamp(static_cast<double>(config_.vectorOnlyThreshold), 0.0, 1.0);
    const double nearMissSlack =
        std::clamp(static_cast<double>(config_.vectorOnlyNearMissSlack), 0.0, 1.0);
    const double nearMissPenalty =
        std::clamp(static_cast<double>(config_.vectorOnlyNearMissPenalty), 0.0, 1.0);

    for (auto& [hash, r] : resultMap) {
        const bool hasAnchoring = anchoredDocs.contains(hash);
        const auto vecIt = maxVectorRawScore.find(hash);
        const bool hasVector = vecIt != maxVectorRawScore.end();

        if (config_.lexicalFloorBoost > 0.0f) {
            if (auto textRankIt = bestTextRank.find(hash); textRankIt != bestTextRank.end()) {
                const bool floorEnabledForRank = (config_.lexicalFloorTopN == 0) ||
                                                 (textRankIt->second < config_.lexicalFloorTopN);
                if (floorEnabledForRank) {
                    const double floorBoost =
                        std::clamp(static_cast<double>(config_.lexicalFloorBoost), 0.0, 1.0) /
                        (1.0 + static_cast<double>(textRankIt->second));
                    r.score += floorBoost;
                }
            }
        }

        if (hasVector && !hasAnchoring) {
            const double rawVector = vecIt->second;
            const size_t bestSemanticRank = bestVectorRank.contains(hash)
                                                ? bestVectorRank.at(hash)
                                                : std::numeric_limits<size_t>::max();
            const bool strongRelief =
                strongVectorOnlyReliefEligible(config_, rawVector, bestSemanticRank);
            const double vectorOnlyPenalty =
                effectiveVectorOnlyPenalty(config_, rawVector, bestSemanticRank);
            if (rawVector < vectorOnlyThreshold) {
                const bool reserveEnabled = config_.vectorOnlyNearMissReserve > 0;
                const bool isNearMiss = reserveEnabled && vectorOnlyThreshold > 0.0 &&
                                        rawVector + nearMissSlack >= vectorOnlyThreshold;
                if (!isNearMiss && !strongRelief) {
                    continue;
                }

                if (strongRelief) {
                    r.score *= vectorOnlyPenalty;
                } else {
                    if (config_.semanticRescueSlots > 0 &&
                        rawVector >= std::max(0.0, static_cast<double>(
                                                       config_.semanticRescueMinVectorScore)) &&
                        !isNearMiss) {
                        r.score *= vectorOnlyPenalty;
                        nearMissReserve.emplace_back(rawVector, std::move(r));
                        continue;
                    }

                    const double thresholdRatio =
                        vectorOnlyThreshold > 0.0
                            ? std::clamp(rawVector / vectorOnlyThreshold, 0.0, 1.0)
                            : std::clamp(rawVector, 0.0, 1.0);
                    r.score *= (vectorOnlyPenalty * nearMissPenalty * thresholdRatio);
                    nearMissReserve.emplace_back(rawVector, std::move(r));
                    continue;
                }
            }
            r.score *= vectorOnlyPenalty;
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

    if (!nearMissReserve.empty() && config_.vectorOnlyNearMissReserve > 0) {
        std::sort(nearMissReserve.begin(), nearMissReserve.end(), [](const auto& a, const auto& b) {
            if (a.first != b.first) {
                return a.first > b.first;
            }
            return a.second.score > b.second.score;
        });

        const size_t reserveTake =
            std::min(config_.vectorOnlyNearMissReserve, nearMissReserve.size());
        for (size_t i = 0; i < reserveTake; ++i) {
            fusedResults.emplace_back(std::move(nearMissReserve[i].second));
        }
    }

    const auto lexicalAnchorScore = [](const SearchResult& r) {
        return r.keywordScore.value_or(0.0) + r.pathScore.value_or(0.0) + r.tagScore.value_or(0.0) +
               r.symbolScore.value_or(0.0);
    };

    const auto rawVectorScoreForResult = [&maxVectorRawScore](const std::string& dedupKey) {
        if (auto it = maxVectorRawScore.find(dedupKey); it != maxVectorRawScore.end()) {
            return it->second;
        }
        return 0.0;
    };

    const auto isVectorOnlyRescueCandidate =
        [this, &lexicalAnchorScore, &rawVectorScoreForResult](const SearchResult& r) -> bool {
        const double lexical = lexicalAnchorScore(r);
        const std::string dedupKey =
            (config_.enablePathDedupInFusion && !r.document.filePath.empty())
                ? std::string("path:") + r.document.filePath
            : (!r.document.sha256Hash.empty()) ? std::string("hash:") + r.document.sha256Hash
                                               : std::string("path:") + r.document.filePath;
        const double vector = rawVectorScoreForResult(dedupKey);
        return lexical <= 0.0 &&
               vector >= std::max(0.0, static_cast<double>(config_.semanticRescueMinVectorScore));
    };

    const auto evidenceRescueScore = [&lexicalAnchorScore](const SearchResult& r) -> double {
        const double lexical = lexicalAnchorScore(r);
        const double vector = r.vectorScore.value_or(0.0);
        const double kg = r.kgScore.value_or(0.0);
        const double bestSignal = std::max({lexical, vector, kg, r.pathScore.value_or(0.0),
                                            r.symbolScore.value_or(0.0), r.tagScore.value_or(0.0)});
        const double componentBonus = (lexical > 0.0 && vector > 0.0) ? 0.01 : 0.0;
        return bestSignal + componentBonus;
    };

    const auto isEvidenceRescueCandidate = [this, &evidenceRescueScore](const SearchResult& r) {
        return evidenceRescueScore(r) >=
               std::max(0.0, static_cast<double>(config_.fusionEvidenceRescueMinScore));
    };

    const auto lexicalAwareLess = [this, &lexicalAnchorScore](const SearchResult& a,
                                                              const SearchResult& b) {
        const double scoreDiff = a.score - b.score;
        const double tieEpsilon =
            std::max(0.0, static_cast<double>(config_.lexicalTieBreakEpsilon));

        if (!config_.enableLexicalTieBreak || std::abs(scoreDiff) > tieEpsilon) {
            if (a.score != b.score) {
                return a.score > b.score;
            }
        } else {
            const double lexicalA = lexicalAnchorScore(a);
            const double lexicalB = lexicalAnchorScore(b);
            if (lexicalA != lexicalB) {
                return lexicalA > lexicalB;
            }

            const double keywordA = a.keywordScore.value_or(0.0);
            const double keywordB = b.keywordScore.value_or(0.0);
            if (keywordA != keywordB) {
                return keywordA > keywordB;
            }

            const double vectorA = a.vectorScore.value_or(0.0);
            const double vectorB = b.vectorScore.value_or(0.0);
            if (vectorA != vectorB) {
                return vectorA < vectorB;
            }
        }

        if (a.document.filePath != b.document.filePath) {
            return a.document.filePath < b.document.filePath;
        }
        return a.document.sha256Hash < b.document.sha256Hash;
    };

    const auto applySemanticRescueWindow = [&]() {
        if (config_.semanticRescueSlots == 0 || fusedResults.empty()) {
            return;
        }

        const size_t topK = std::min(semanticRescueWindowLimit(config_), fusedResults.size());
        if (topK == 0 || topK >= fusedResults.size()) {
            return;
        }

        const size_t rescueTarget = std::min(config_.semanticRescueSlots, topK);
        size_t rescuePresent = 0;
        for (size_t i = 0; i < topK; ++i) {
            if (isVectorOnlyRescueCandidate(fusedResults[i])) {
                rescuePresent++;
            }
        }

        while (rescuePresent < rescueTarget) {
            size_t bestTailIndex = fusedResults.size();
            for (size_t i = topK; i < fusedResults.size(); ++i) {
                if (!isVectorOnlyRescueCandidate(fusedResults[i])) {
                    continue;
                }
                if (bestTailIndex >= fusedResults.size() ||
                    lexicalAwareLess(fusedResults[i], fusedResults[bestTailIndex])) {
                    bestTailIndex = i;
                }
            }
            if (bestTailIndex >= fusedResults.size()) {
                break;
            }

            size_t victimIndex = topK;
            for (size_t i = topK; i > 0; --i) {
                const size_t idx = i - 1;
                if (!isVectorOnlyRescueCandidate(fusedResults[idx])) {
                    victimIndex = idx;
                    break;
                }
            }
            if (victimIndex >= topK) {
                break;
            }

            std::swap(fusedResults[victimIndex], fusedResults[bestTailIndex]);
            rescuePresent++;
        }

        std::sort(fusedResults.begin(), fusedResults.begin() + static_cast<ptrdiff_t>(topK),
                  lexicalAwareLess);
    };

    // Single sort at the end
    if (fusedResults.size() > config_.maxResults) {
        std::partial_sort(fusedResults.begin(),
                          fusedResults.begin() + static_cast<ptrdiff_t>(config_.maxResults),
                          fusedResults.end(), lexicalAwareLess);
        applySemanticRescueWindow();

        if (config_.fusionEvidenceRescueSlots > 0) {
            const size_t topK = config_.maxResults;
            const size_t rescueTarget = std::min(config_.fusionEvidenceRescueSlots, topK);
            size_t rescuePresent = 0;
            for (size_t i = 0; i < topK; ++i) {
                if (isEvidenceRescueCandidate(fusedResults[i])) {
                    rescuePresent++;
                }
            }

            while (rescuePresent < rescueTarget) {
                size_t bestTailIndex = fusedResults.size();
                double bestTailEvidence = -1.0;
                for (size_t i = topK; i < fusedResults.size(); ++i) {
                    if (!isEvidenceRescueCandidate(fusedResults[i])) {
                        continue;
                    }
                    const double evidence = evidenceRescueScore(fusedResults[i]);
                    if (evidence > bestTailEvidence ||
                        (evidence == bestTailEvidence &&
                         (bestTailIndex >= fusedResults.size() ||
                          lexicalAwareLess(fusedResults[i], fusedResults[bestTailIndex])))) {
                        bestTailIndex = i;
                        bestTailEvidence = evidence;
                    }
                }
                if (bestTailIndex >= fusedResults.size()) {
                    break;
                }

                size_t victimIndex = topK;
                double victimEvidence = std::numeric_limits<double>::max();
                for (size_t i = 0; i < topK; ++i) {
                    const double evidence = evidenceRescueScore(fusedResults[i]);
                    if (evidence < victimEvidence) {
                        victimEvidence = evidence;
                        victimIndex = i;
                    }
                }
                if (victimIndex >= topK || bestTailEvidence <= victimEvidence) {
                    break;
                }

                std::swap(fusedResults[victimIndex], fusedResults[bestTailIndex]);
                rescuePresent++;
            }

            std::sort(fusedResults.begin(), fusedResults.begin() + static_cast<ptrdiff_t>(topK),
                      lexicalAwareLess);
        }

        fusedResults.resize(config_.maxResults);
    } else {
        std::sort(fusedResults.begin(), fusedResults.end(), lexicalAwareLess);
        applySemanticRescueWindow();
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

        // Compressed ANN telemetry (Phase 3, Milestone 9)
        std::atomic<uint64_t> compressedAnnQueries{0};        // total compressed ANN attempts
        std::atomic<uint64_t> compressedAnnSucceeded{0};      // successful searches
        std::atomic<uint64_t> compressedAnnFallback{0};       // fell back to other path
        std::atomic<uint64_t> compressedAnnDecodeEscapes{0};  // times decode was triggered
        std::atomic<uint64_t> compressedAnnCandidateCount{0}; // total candidates scored
        std::atomic<uint64_t> compressedAnnBuildErrors{0};    // index build failures
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

    /**
     * @brief Install a runtime SearchTuner for adaptive per-query tuning.
     */
    void setSearchTuner(std::shared_ptr<SearchTuner> tuner);

    /**
     * @brief Access the installed runtime SearchTuner, if any.
     */
    std::shared_ptr<SearchTuner> getSearchTuner() const;

    /**
     * @brief Invalidate the compressed ANN index so the next query rebuilds it.
     *
     * Call this after inserting or updating vectors in the VectorDatabase to
     * ensure the compressed ANN index stays in sync. The next query will
     * lazily rebuild the index from the current database state.
     *
     * @note This is a no-op if enableCompressedANN is false.
     */
    void invalidateCompressedANNIndex();

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
