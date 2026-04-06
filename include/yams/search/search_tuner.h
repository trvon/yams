#pragma once

#include <yams/search/search_engine.h>
#include <yams/storage/corpus_stats.h>

#include <nlohmann/json.hpp>
#include <map>
#include <mutex>
#include <optional>
#include <string>

namespace yams::search {

/**
 * @brief Tuning state for the SearchTuner FSM.
 *
 * Each state corresponds to a specific corpus profile and has associated
 * optimal search parameters (RRF k, component weights, etc.).
 */
enum class TuningState {
    SMALL_CODE,      // < 1K docs, mostly code (.cpp, .py, .rs, etc.)
    LARGE_CODE,      // >= 1K docs, mostly code
    SMALL_PROSE,     // < 1K docs, mostly prose (.md, .txt, etc.)
    LARGE_PROSE,     // >= 1K docs, mostly prose
    SCIENTIFIC,      // Prose with no path/tag structure (benchmark-like corpora)
    MIXED,           // Balanced content types
    MIXED_PRECISION, // MIXED tuned for lexical precision guardrails
    MINIMAL          // < 100 docs, any type (very small corpus)
};

/**
 * @brief Convert TuningState to string for logging/debugging.
 */
[[nodiscard]] constexpr const char* tuningStateToString(TuningState state) noexcept {
    switch (state) {
        case TuningState::SMALL_CODE:
            return "SMALL_CODE";
        case TuningState::LARGE_CODE:
            return "LARGE_CODE";
        case TuningState::SMALL_PROSE:
            return "SMALL_PROSE";
        case TuningState::LARGE_PROSE:
            return "LARGE_PROSE";
        case TuningState::SCIENTIFIC:
            return "SCIENTIFIC";
        case TuningState::MIXED:
            return "MIXED";
        case TuningState::MIXED_PRECISION:
            return "MIXED_PRECISION";
        case TuningState::MINIMAL:
            return "MINIMAL";
    }
    return "UNKNOWN";
}

/**
 * @brief Tuned parameter set for a specific corpus state.
 *
 * Contains all tunable parameters for the search engine, optimized for
 * a particular corpus profile (size, content type, feature availability).
 */
struct TunedParams {
    SearchEngineConfig::NavigationZoomLevel zoomLevel =
        SearchEngineConfig::NavigationZoomLevel::Auto;

    // RRF constant (k in 1/(k+rank))
    // Lower k = more weight on top-ranked items
    // Higher k = smoother ranking across positions
    int rrfK = 60;

    // Component weights (must sum to ~1.0)
    float textWeight = 0.45f;         // Full-text search (FTS5 + symbol)
    float vectorWeight = 0.15f;       // Vector similarity (document embeddings)
    float entityVectorWeight = 0.05f; // Entity/symbol vector similarity (code search)
    float pathTreeWeight = 0.15f;     // Path tree hierarchical
    float kgWeight = 0.10f;           // Knowledge graph
    float tagWeight = 0.10f;          // Tag-based search
    float metadataWeight = 0.05f;     // Metadata attribute matching

    // Similarity threshold for vector search
    float similarityThreshold = 0.65f;

    // Vector boost factor for TEXT_ANCHOR fusion (multiplied with vectorWeight)
    // Higher values = stronger vector influence in text-anchored results
    // Default 1.0 means full vectorWeight is used; lower values reduce vector impact
    float vectorBoostFactor = 1.0f;

    // Fusion strategy (default: COMB_MNZ, but TEXT_ANCHOR for scientific corpora)
    SearchEngineConfig::FusionStrategy fusionStrategy =
        SearchEngineConfig::FusionStrategy::COMB_MNZ;

    // Hybrid precision guardrails (default to SearchEngineConfig defaults)
    float vectorOnlyThreshold = 0.90f;
    float vectorOnlyPenalty = 0.80f;
    size_t vectorOnlyNearMissReserve = 0;
    float vectorOnlyNearMissSlack = 0.05f;
    float vectorOnlyNearMissPenalty = 0.60f;
    bool enablePathDedupInFusion = false;
    size_t lexicalFloorTopN = 0;
    float lexicalFloorBoost = 0.0f;
    bool enableLexicalTieBreak = false;
    float lexicalTieBreakEpsilon = 0.0f;
    size_t semanticRescueSlots = 0;
    float semanticRescueMinVectorScore = 0.0f;
    size_t fusionEvidenceRescueSlots = 0;
    float fusionEvidenceRescueMinScore = 0.0f;

    // Adaptive semantic skip behavior
    bool enableAdaptiveVectorFallback = false;
    size_t adaptiveVectorSkipMinTier1Hits = 0;
    bool adaptiveVectorSkipRequireTextSignal = true;
    size_t adaptiveVectorSkipMinTextHits = 3;
    float adaptiveVectorSkipMinTopTextScore = 0.30f;

    // Graph reranking controls (dynamically adapted by SearchTuner)
    bool enableGraphRerank = false;
    size_t graphRerankTopN = 25;
    float graphRerankWeight = 0.15f;
    float graphRerankMaxBoost = 0.20f;
    float graphRerankMinSignal = 0.01f;
    float graphCommunityWeight = 0.10f;
    size_t kgMaxResults = 100;
    int graphScoringBudgetMs = 10;

    /**
     * @brief Apply tuned parameters to a SearchEngineConfig.
     */
    void applyTo(SearchEngineConfig& config) const {
        config.zoomLevel = zoomLevel;
        config.textWeight = textWeight;
        config.vectorWeight = vectorWeight;
        config.entityVectorWeight = entityVectorWeight;
        config.pathTreeWeight = pathTreeWeight;
        config.kgWeight = kgWeight;
        config.tagWeight = tagWeight;
        config.metadataWeight = metadataWeight;
        config.similarityThreshold = similarityThreshold;
        config.vectorBoostFactor = vectorBoostFactor;
        config.rrfK = static_cast<float>(rrfK);
        config.fusionStrategy = fusionStrategy;
        config.enableGraphRerank = enableGraphRerank;
        config.graphRerankTopN = graphRerankTopN;
        config.graphRerankWeight = graphRerankWeight;
        config.graphRerankMaxBoost = graphRerankMaxBoost;
        config.graphRerankMinSignal = graphRerankMinSignal;
        config.graphCommunityWeight = graphCommunityWeight;
        config.kgMaxResults = kgMaxResults;
        config.graphScoringBudgetMs = graphScoringBudgetMs;
        config.vectorOnlyThreshold = vectorOnlyThreshold;
        config.vectorOnlyPenalty = vectorOnlyPenalty;
        config.vectorOnlyNearMissReserve = vectorOnlyNearMissReserve;
        config.vectorOnlyNearMissSlack = vectorOnlyNearMissSlack;
        config.vectorOnlyNearMissPenalty = vectorOnlyNearMissPenalty;
        config.enablePathDedupInFusion = enablePathDedupInFusion;
        config.lexicalFloorTopN = lexicalFloorTopN;
        config.lexicalFloorBoost = lexicalFloorBoost;
        config.enableLexicalTieBreak = enableLexicalTieBreak;
        config.lexicalTieBreakEpsilon = lexicalTieBreakEpsilon;
        config.semanticRescueSlots = semanticRescueSlots;
        config.semanticRescueMinVectorScore = semanticRescueMinVectorScore;
        config.fusionEvidenceRescueSlots = fusionEvidenceRescueSlots;
        config.fusionEvidenceRescueMinScore = fusionEvidenceRescueMinScore;
        config.enableAdaptiveVectorFallback = enableAdaptiveVectorFallback;
        config.adaptiveVectorSkipMinTier1Hits = adaptiveVectorSkipMinTier1Hits;
        config.adaptiveVectorSkipRequireTextSignal = adaptiveVectorSkipRequireTextSignal;
        config.adaptiveVectorSkipMinTextHits = adaptiveVectorSkipMinTextHits;
        config.adaptiveVectorSkipMinTopTextScore = adaptiveVectorSkipMinTopTextScore;
    }

    /**
     * @brief Serialize to JSON.
     */
    [[nodiscard]] nlohmann::json toJson() const {
        return nlohmann::json{
            {"zoom_level", SearchEngineConfig::navigationZoomLevelToString(zoomLevel)},
            {"rrf_k", rrfK},
            {"text_weight", textWeight},
            {"vector_weight", vectorWeight},
            {"entity_vector_weight", entityVectorWeight},
            {"path_tree_weight", pathTreeWeight},
            {"kg_weight", kgWeight},
            {"tag_weight", tagWeight},
            {"metadata_weight", metadataWeight},
            {"similarity_threshold", similarityThreshold},
            {"vector_boost_factor", vectorBoostFactor},
            {"enable_graph_rerank", enableGraphRerank},
            {"graph_rerank_topn", graphRerankTopN},
            {"graph_rerank_weight", graphRerankWeight},
            {"graph_rerank_max_boost", graphRerankMaxBoost},
            {"graph_rerank_min_signal", graphRerankMinSignal},
            {"graph_community_weight", graphCommunityWeight},
            {"kg_max_results", kgMaxResults},
            {"graph_scoring_budget_ms", graphScoringBudgetMs},
            {"vector_only_threshold", vectorOnlyThreshold},
            {"vector_only_penalty", vectorOnlyPenalty},
            {"vector_only_near_miss_reserve", vectorOnlyNearMissReserve},
            {"vector_only_near_miss_slack", vectorOnlyNearMissSlack},
            {"vector_only_near_miss_penalty", vectorOnlyNearMissPenalty},
            {"enable_path_dedup_in_fusion", enablePathDedupInFusion},
            {"lexical_floor_topn", lexicalFloorTopN},
            {"lexical_floor_boost", lexicalFloorBoost},
            {"enable_lexical_tie_break", enableLexicalTieBreak},
            {"lexical_tie_break_epsilon", lexicalTieBreakEpsilon},
            {"semantic_rescue_slots", semanticRescueSlots},
            {"semantic_rescue_min_vector_score", semanticRescueMinVectorScore},
            {"fusion_evidence_rescue_slots", fusionEvidenceRescueSlots},
            {"fusion_evidence_rescue_min_score", fusionEvidenceRescueMinScore},
            {"enable_adaptive_vector_fallback", enableAdaptiveVectorFallback},
            {"adaptive_vector_skip_min_tier1_hits", adaptiveVectorSkipMinTier1Hits},
            {"adaptive_vector_skip_require_text_signal", adaptiveVectorSkipRequireTextSignal},
            {"adaptive_vector_skip_min_text_hits", adaptiveVectorSkipMinTextHits},
            {"adaptive_vector_skip_min_top_text_score", adaptiveVectorSkipMinTopTextScore}};
    }
};

/// Get tuned parameters for a specific tuning state.
[[nodiscard]] inline TunedParams getTunedParams(TuningState state) noexcept {
    TunedParams params;

    switch (state) {
        case TuningState::SMALL_CODE:
            params.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Street;
            params.rrfK = 20;
            params.textWeight = 0.45f;
            params.vectorWeight = 0.15f;
            params.entityVectorWeight = 0.15f;
            params.pathTreeWeight = 0.15f;
            params.kgWeight = 0.05f;
            params.tagWeight = 0.03f;
            params.metadataWeight = 0.02f;
            break;

        case TuningState::LARGE_CODE:
            params.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Street;
            params.rrfK = 60;
            params.textWeight = 0.40f;
            params.vectorWeight = 0.20f;
            params.entityVectorWeight = 0.15f;
            params.pathTreeWeight = 0.10f;
            params.kgWeight = 0.05f;
            params.tagWeight = 0.05f;
            params.metadataWeight = 0.05f;
            break;

        case TuningState::SMALL_PROSE:
            params.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Neighborhood;
            params.rrfK = 25;
            params.textWeight = 0.50f;
            params.vectorWeight = 0.40f;
            params.entityVectorWeight = 0.00f;
            params.pathTreeWeight = 0.00f;
            params.kgWeight = 0.00f;
            params.tagWeight = 0.05f;
            params.metadataWeight = 0.05f;
            params.lexicalFloorTopN = 12;
            params.lexicalFloorBoost = 0.20f;
            params.enableLexicalTieBreak = true;
            params.lexicalTieBreakEpsilon = 0.010f;
            params.fusionEvidenceRescueSlots = 1;
            params.fusionEvidenceRescueMinScore = 0.012f;
            break;

        case TuningState::LARGE_PROSE:
            params.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Neighborhood;
            params.rrfK = 60;
            params.textWeight = 0.40f;
            params.vectorWeight = 0.45f;
            params.entityVectorWeight = 0.00f;
            params.pathTreeWeight = 0.05f;
            params.kgWeight = 0.00f;
            params.tagWeight = 0.05f;
            params.metadataWeight = 0.05f;
            break;

        case TuningState::SCIENTIFIC:
            params.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Map;
            // For scientific/benchmark corpora: balanced text + vector fusion
            // Text-dominant because lexical precision is usually stronger than dense-only
            // semantics on benchmark-style scientific corpora.
            // WEIGHTED_RECIPROCAL avoids COMB_MNZ's mnzBoost penalty which demotes
            // documents found by only one component (common when vector search doesn't
            // understand scientific terminology)
            params.rrfK = 12;            // Low k for better top-rank discrimination
            params.textWeight = 0.70f;   // Text as primary signal
            params.vectorWeight = 0.25f; // Vector as semantic assist
            params.entityVectorWeight = 0.00f;
            params.pathTreeWeight = 0.00f;
            params.kgWeight = 0.00f;
            params.tagWeight = 0.00f;
            params.metadataWeight = 0.05f;
            params.similarityThreshold = 0.55f;
            params.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_RECIPROCAL;
            break;

        case TuningState::MIXED:
            params.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Neighborhood;
            params.rrfK = 45;
            params.textWeight = 0.40f;
            params.vectorWeight = 0.25f;
            params.entityVectorWeight = 0.10f;
            params.pathTreeWeight = 0.10f;
            params.kgWeight = 0.05f;
            params.tagWeight = 0.05f;
            params.metadataWeight = 0.05f;
            break;

        case TuningState::MIXED_PRECISION:
            params.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Neighborhood;
            params.rrfK = 45;
            params.textWeight = 0.40f;
            params.vectorWeight = 0.25f;
            params.entityVectorWeight = 0.10f;
            params.pathTreeWeight = 0.10f;
            params.kgWeight = 0.05f;
            params.tagWeight = 0.05f;
            params.metadataWeight = 0.05f;
            params.vectorOnlyThreshold = 0.94f;
            params.vectorOnlyPenalty = 0.70f;
            params.vectorOnlyNearMissReserve = 2;
            params.vectorOnlyNearMissSlack = 0.04f;
            params.vectorOnlyNearMissPenalty = 0.55f;
            params.enablePathDedupInFusion = true;
            params.lexicalFloorTopN = 12;
            params.lexicalFloorBoost = 0.20f;
            params.enableLexicalTieBreak = true;
            params.lexicalTieBreakEpsilon = 0.010f;
            params.semanticRescueSlots = 1;
            params.semanticRescueMinVectorScore = 0.0f;
            params.fusionEvidenceRescueSlots = 1;
            params.fusionEvidenceRescueMinScore = 0.012f;
            params.enableAdaptiveVectorFallback = true;
            params.adaptiveVectorSkipMinTier1Hits = 80;
            params.adaptiveVectorSkipRequireTextSignal = true;
            params.adaptiveVectorSkipMinTextHits = 5;
            params.adaptiveVectorSkipMinTopTextScore = 0.45f;
            break;

        case TuningState::MINIMAL:
            params.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Street;
            params.rrfK = 15;
            params.textWeight = 0.55f;
            params.vectorWeight = 0.30f;
            params.entityVectorWeight = 0.05f;
            params.pathTreeWeight = 0.05f;
            params.kgWeight = 0.00f;
            params.tagWeight = 0.03f;
            params.metadataWeight = 0.02f;
            break;
    }

    return params;
}

/**
 * @brief Adaptive search tuner that selects optimal parameters based on corpus.
 *
 * The SearchTuner implements a finite state machine that observes corpus
 * characteristics and selects optimal search parameters. It replaces the
 * static CorpusProfile system with dynamic, data-driven tuning.
 *
 * Usage:
 * @code
 *   // Get corpus stats (computed or cached)
 *   auto stats = metadataRepo->getCorpusStats();
 *
 *   // Create tuner and get optimal config
 *   SearchTuner tuner(stats.value());
 *   auto config = tuner.getConfig();
 *
 *   // Use tuned config for search
 *   SearchEngine engine(..., config);
 * @endcode
 *
 * The tuner can also be used to observe and log tuning decisions:
 * @code
 *   spdlog::info("Search tuned to {} ({})",
 *                tuner.currentState(),
 *                tuner.stateReason());
 * @endcode
 */
class SearchTuner {
public:
    struct RuntimeStageSignal {
        bool enabled = false;
        bool attempted = false;
        bool contributed = false;
        bool skipped = false;
        double durationMs = 0.0;
        std::size_t rawHitCount = 0;
        std::size_t uniqueDocCount = 0;
    };

    struct RuntimeFusionSignal {
        bool enabled = false;
        bool contributedToFinal = false;
        double configuredWeight = 0.0;
        double finalScoreMass = 0.0;
        std::size_t finalTopDocCount = 0;
        std::size_t rawHitCount = 0;
        std::size_t uniqueDocCount = 0;
    };

    struct RuntimeTelemetry {
        double latencyMs = 0.0;
        std::size_t finalResultCount = 0;
        std::size_t topWindow = 0;
        SearchEngineConfig::NavigationZoomLevel zoomLevel =
            SearchEngineConfig::NavigationZoomLevel::Auto;
        std::map<std::string, RuntimeStageSignal> stages;
        std::map<std::string, RuntimeFusionSignal> fusionSources;
    };

    /**
     * @brief Construct a tuner from corpus statistics.
     *
     * Immediately computes the optimal state and parameters based on
     * the provided corpus metrics.
     *
     * @param stats Corpus statistics (from MetadataRepository::getCorpusStats)
     */
    explicit SearchTuner(const storage::CorpusStats& stats);
    SearchTuner(const storage::CorpusStats& stats, std::optional<TuningState> forcedState);

    /**
     * @brief Get the current tuning state.
     */
    [[nodiscard]] TuningState currentState() const noexcept { return state_; }

    /**
     * @brief Get a human-readable explanation of why this state was selected.
     *
     * Useful for debugging and observability.
     */
    [[nodiscard]] const std::string& stateReason() const noexcept { return stateReason_; }

    /**
     * @brief Get the tuned parameter set for the current state.
     */
    [[nodiscard]] const TunedParams& getParams() const noexcept { return params_; }

    /**
     * @brief Reset adaptive baseline from an externally finalized config.
     *
     * Used when environment overrides or runtime setConfig() mutate the tuned config after
     * SearchTuner selected its baseline. This keeps runtime adaptation anchored to the final
     * effective configuration.
     */
    void seedRuntimeConfig(const SearchEngineConfig& config);

    /**
     * @brief Get a SearchEngineConfig with tuned parameters applied.
     *
     * Returns a config with weights and thresholds optimized for the corpus.
     * This is the main entry point for integrating the tuner with SearchEngine.
     */
    [[nodiscard]] SearchEngineConfig getConfig() const;

    /**
     * @brief Observe one query worth of runtime telemetry.
     */
    void observe(const RuntimeTelemetry& telemetry);

    /**
     * @brief Export current adaptive runtime state for diagnostics.
     */
    [[nodiscard]] nlohmann::json adaptiveStateToJson() const;

    /**
     * @brief Get the RRF k constant for this tuning state.
     *
     * The k value controls how much weight is given to top-ranked results
     * in Reciprocal Rank Fusion. Lower k means top ranks dominate more.
     */
    [[nodiscard]] int getRrfK() const noexcept { return params_.rrfK; }

    /**
     * @brief Serialize tuner state to JSON for logging/debugging.
     */
    [[nodiscard]] nlohmann::json toJson() const;

    /**
     * @brief Compute the optimal state from corpus statistics.
     *
     * Static method for external use (e.g., testing, CLI display).
     *
     * @param stats Corpus statistics
     * @return Computed tuning state
     */
    [[nodiscard]] static TuningState computeState(const storage::CorpusStats& stats);

    /**
     * @brief Compute state with reason string.
     *
     * @param stats Corpus statistics
     * @param outReason Output parameter for state reason explanation
     * @return Computed tuning state
     */
    [[nodiscard]] static TuningState computeState(const storage::CorpusStats& stats,
                                                  std::string& outReason);

private:
    struct AdaptiveRuntimeState {
        std::uint64_t observations = 0;
        std::uint64_t lastAdjustmentObservation = 0;
        double ewmaLatencyMs = 0.0;
        double ewmaKgLatencyShare = 0.0;
        double ewmaKgUtility = 0.0;
        double ewmaKgScoreMassShare = 0.0;
        double ewmaKgFinalDocShare = 0.0;
        double ewmaKgContributionRate = 0.0;
        double ewmaGraphRerankLatencyMs = 0.0;
        double ewmaGraphRerankSkipRate = 0.0;
        double ewmaGraphRerankContributionRate = 0.0;
        double ewmaZoomDepth = 0.0;
        bool lastObservationChanged = false;
        SearchEngineConfig::NavigationZoomLevel lastZoomLevel =
            SearchEngineConfig::NavigationZoomLevel::Auto;
        std::map<std::string, std::uint64_t> zoomLevelCounts;
        std::string lastDecision;
    };

    [[nodiscard]] SearchEngineConfig buildConfigFromParamsLocked() const;
    [[nodiscard]] nlohmann::json adaptiveStateToJsonLocked() const;

    TuningState state_;
    TunedParams baseParams_;
    TunedParams params_;
    std::string stateReason_;
    storage::CorpusStats stats_; // Keep a copy for toJson()
    SearchEngineConfig baseConfig_{};
    mutable std::mutex mutex_;
    AdaptiveRuntimeState adaptive_;
};

} // namespace yams::search
