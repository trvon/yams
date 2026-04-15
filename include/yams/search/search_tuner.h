#pragma once

#include <yams/core/types.h>
#include <yams/search/relevance_label_store.h>
#include <yams/search/search_engine.h>
#include <yams/search/tuning_slot.h>
#include <yams/storage/corpus_stats.h>

#include <nlohmann/json.hpp>
#include <cstddef>
#include <filesystem>
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
    MINIMAL,         // < 100 docs, any type (very small corpus)
    MEDIA            // Binary/media-heavy corpus (images, audio, video)
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
        case TuningState::MEDIA:
            return "MEDIA";
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

    // Component weights (must sum to ~1.0).
    // Stored in WeightSlots for provenance tracking and pinning support.
    WeightSlots weights = []() {
        WeightSlots ws;
        ws.setAll(0.45f, 0.15f, 0.05f, 0.15f, 0.10f, 0.10f, 0.05f, TuningLayer::Default);
        return ws;
    }();

    // Similarity threshold for vector search
    TuningSlot<float> similarityThreshold{0.65f};

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
    TuningSlot<size_t> semanticRescueSlots{0};
    float semanticRescueMinVectorScore = 0.0f;
    size_t fusionEvidenceRescueSlots = 0;
    float fusionEvidenceRescueMinScore = 0.0f;

    // Adaptive semantic skip behavior
    bool enableAdaptiveVectorFallback = false;
    size_t adaptiveVectorSkipMinTier1Hits = 0;
    bool adaptiveVectorSkipRequireTextSignal = true;
    size_t adaptiveVectorSkipMinTextHits = 3;
    float adaptiveVectorSkipMinTopTextScore = 0.30f;

    // Sub-phrase rescoring (always-on score-update pass for prose queries)
    bool enableSubPhraseRescoring = false;
    float subPhraseScoringPenalty = 0.70f;

    // Cross-reranker controls
    size_t rerankTopK = 5;
    // Minimum relative score (vs. rank-1) for a multi-source doc to count as "competitive
    // anchored evidence" and override the score-gap guard. 0.0 = any doc counts (default).
    float rerankAnchoredMinRelativeScore = 0.0f;

    // Chunk-to-document vector aggregation
    SearchEngineConfig::ChunkAggregation chunkAggregation =
        SearchEngineConfig::ChunkAggregation::MAX;

    // Graph reranking controls (dynamically adapted by SearchTuner)
    bool enableGraphRerank = false;
    size_t graphRerankTopN = 25;
    float graphRerankWeight = 0.15F;
    float graphRerankMaxBoost = 0.20F;
    float graphRerankMinSignal = 0.01F;
    float graphCommunityWeight = 0.10F;
    size_t kgMaxResults = 100;
    int graphScoringBudgetMs = 10;
    bool graphEnablePathEnumeration = false;
    bool enableGraphQueryExpansion = false;

    // Per-profile graph signal weights for reranking composition
    float graphEntitySignalWeight = 0.40F;
    float graphStructuralSignalWeight = 0.20F;
    float graphCoverageSignalWeight = 0.20F;
    float graphPathSignalWeight = 0.10F;
    float graphCorroborationFloor = 0.35F;

    /**
     * @brief Apply tuned parameters to a SearchEngineConfig.
     */
    void applyTo(SearchEngineConfig& config) const {
        config.zoomLevel = zoomLevel;
        config.textWeight = weights.text.value;
        config.vectorWeight = weights.vector.value;
        config.entityVectorWeight = weights.entityVector.value;
        config.pathTreeWeight = weights.pathTree.value;
        config.kgWeight = weights.kg.value;
        config.tagWeight = weights.tag.value;
        config.metadataWeight = weights.metadata.value;
        config.similarityThreshold = similarityThreshold.value;
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
        config.graphEnablePathEnumeration = graphEnablePathEnumeration;
        config.enableGraphQueryExpansion = enableGraphQueryExpansion;
        config.graphEntitySignalWeight = graphEntitySignalWeight;
        config.graphStructuralSignalWeight = graphStructuralSignalWeight;
        config.graphCoverageSignalWeight = graphCoverageSignalWeight;
        config.graphPathSignalWeight = graphPathSignalWeight;
        config.graphCorroborationFloor = graphCorroborationFloor;
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
        config.semanticRescueSlots = semanticRescueSlots.value;
        config.semanticRescueMinVectorScore = semanticRescueMinVectorScore;
        config.fusionEvidenceRescueSlots = fusionEvidenceRescueSlots;
        config.fusionEvidenceRescueMinScore = fusionEvidenceRescueMinScore;
        config.enableAdaptiveVectorFallback = enableAdaptiveVectorFallback;
        config.adaptiveVectorSkipMinTier1Hits = adaptiveVectorSkipMinTier1Hits;
        config.adaptiveVectorSkipRequireTextSignal = adaptiveVectorSkipRequireTextSignal;
        config.adaptiveVectorSkipMinTextHits = adaptiveVectorSkipMinTextHits;
        config.adaptiveVectorSkipMinTopTextScore = adaptiveVectorSkipMinTopTextScore;
        config.enableSubPhraseRescoring = enableSubPhraseRescoring;
        config.subPhraseScoringPenalty = subPhraseScoringPenalty;
        config.rerankTopK = rerankTopK;
        config.rerankAnchoredMinRelativeScore = rerankAnchoredMinRelativeScore;
        config.chunkAggregation = chunkAggregation;
    }

    /**
     * @brief Serialize to JSON.
     */
    [[nodiscard]] nlohmann::json toJson() const {
        return nlohmann::json{
            {"zoom_level", SearchEngineConfig::navigationZoomLevelToString(zoomLevel)},
            {"rrf_k", rrfK},
            {"text_weight", weights.text.value},
            {"text_weight_source", tuningLayerToString(weights.text.source)},
            {"vector_weight", weights.vector.value},
            {"vector_weight_source", tuningLayerToString(weights.vector.source)},
            {"entity_vector_weight", weights.entityVector.value},
            {"entity_vector_weight_source", tuningLayerToString(weights.entityVector.source)},
            {"path_tree_weight", weights.pathTree.value},
            {"path_tree_weight_source", tuningLayerToString(weights.pathTree.source)},
            {"kg_weight", weights.kg.value},
            {"kg_weight_source", tuningLayerToString(weights.kg.source)},
            {"tag_weight", weights.tag.value},
            {"tag_weight_source", tuningLayerToString(weights.tag.source)},
            {"metadata_weight", weights.metadata.value},
            {"metadata_weight_source", tuningLayerToString(weights.metadata.source)},
            {"similarity_threshold", similarityThreshold.value},
            {"similarity_threshold_source", tuningLayerToString(similarityThreshold.source)},
            {"vector_boost_factor", vectorBoostFactor},
            {"enable_graph_rerank", enableGraphRerank},
            {"graph_rerank_topn", graphRerankTopN},
            {"graph_rerank_weight", graphRerankWeight},
            {"graph_rerank_max_boost", graphRerankMaxBoost},
            {"graph_rerank_min_signal", graphRerankMinSignal},
            {"graph_community_weight", graphCommunityWeight},
            {"kg_max_results", kgMaxResults},
            {"graph_scoring_budget_ms", graphScoringBudgetMs},
            {"graph_enable_path_enumeration", graphEnablePathEnumeration},
            {"enable_graph_query_expansion", enableGraphQueryExpansion},
            {"graph_entity_signal_weight", graphEntitySignalWeight},
            {"graph_structural_signal_weight", graphStructuralSignalWeight},
            {"graph_coverage_signal_weight", graphCoverageSignalWeight},
            {"graph_path_signal_weight", graphPathSignalWeight},
            {"graph_corroboration_floor", graphCorroborationFloor},
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
            {"semantic_rescue_slots", semanticRescueSlots.value},
            {"semantic_rescue_slots_source", tuningLayerToString(semanticRescueSlots.source)},
            {"semantic_rescue_min_vector_score", semanticRescueMinVectorScore},
            {"fusion_evidence_rescue_slots", fusionEvidenceRescueSlots},
            {"fusion_evidence_rescue_min_score", fusionEvidenceRescueMinScore},
            {"enable_adaptive_vector_fallback", enableAdaptiveVectorFallback},
            {"adaptive_vector_skip_min_tier1_hits", adaptiveVectorSkipMinTier1Hits},
            {"adaptive_vector_skip_require_text_signal", adaptiveVectorSkipRequireTextSignal},
            {"adaptive_vector_skip_min_text_hits", adaptiveVectorSkipMinTextHits},
            {"adaptive_vector_skip_min_top_text_score", adaptiveVectorSkipMinTopTextScore},
            {"enable_sub_phrase_rescoring", enableSubPhraseRescoring},
            {"sub_phrase_scoring_penalty", subPhraseScoringPenalty},
            {"rerank_top_k", rerankTopK},
            {"rerank_anchored_min_relative_score", rerankAnchoredMinRelativeScore}};
    }
};

/// Get tuned parameters for a specific tuning state.
[[nodiscard]] inline TunedParams getTunedParams(TuningState state) noexcept {
    TunedParams params;

    switch (state) {
        case TuningState::SMALL_CODE:
            params.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Street;
            params.rrfK = 20;
            params.weights.setAll(0.45f, 0.15f, 0.15f, 0.15f, 0.05f, 0.03f, 0.02f,
                                  TuningLayer::Profile);
            params.graphEntitySignalWeight = 0.25F;
            params.graphStructuralSignalWeight = 0.35F;
            params.graphCoverageSignalWeight = 0.20F;
            params.graphPathSignalWeight = 0.15F;
            params.graphCorroborationFloor = 0.40F;
            break;

        case TuningState::LARGE_CODE:
            params.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Street;
            params.rrfK = 60;
            params.weights.setAll(0.40f, 0.20f, 0.15f, 0.10f, 0.05f, 0.05f, 0.05f,
                                  TuningLayer::Profile);
            break;

        case TuningState::SMALL_PROSE:
            params.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Neighborhood;
            params.rrfK = 25;
            params.weights.setAll(0.50f, 0.40f, 0.00f, 0.00f, 0.00f, 0.05f, 0.05f,
                                  TuningLayer::Profile);
            params.lexicalFloorTopN = 12;
            params.lexicalFloorBoost = 0.20f;
            params.enableLexicalTieBreak = true;
            params.lexicalTieBreakEpsilon = 0.010f;
            params.fusionEvidenceRescueSlots = 1;
            params.fusionEvidenceRescueMinScore = 0.012f;
            // Re-score already-retrieved documents via sub-phrase AND queries.
            // Helps when base FTS returns the entire corpus at low scores and
            // the standard expansion gates (baseFtsHitCount < N) never fire.
            params.enableSubPhraseRescoring = true;
            params.subPhraseScoringPenalty = 0.70f;
            break;

        case TuningState::LARGE_PROSE:
            params.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Neighborhood;
            params.rrfK = 60;
            params.weights.setAll(0.40f, 0.45f, 0.00f, 0.05f, 0.00f, 0.05f, 0.05f,
                                  TuningLayer::Profile);
            break;

        case TuningState::SCIENTIFIC:
            params.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Auto;
            // For scientific/benchmark corpora: balanced text + vector fusion.
            // WEIGHTED_RECIPROCAL avoids COMB_MNZ's mnzBoost penalty which demotes
            // documents found by only one component.
            params.rrfK = 12; // Low k for better top-rank discrimination
            params.weights.setAll(0.60f, 0.35f, 0.00f, 0.00f, 0.00f, 0.00f, 0.05f,
                                  TuningLayer::Profile);
            // Lower threshold vs default (0.65) to improve recall for claim-style prose
            // queries where query-document cosine similarity is often in the 0.40-0.54
            // range rather than the >0.55 range seen for code/technical queries.
            params.similarityThreshold = TuningSlot<float>(0.40f, TuningLayer::Profile);
            params.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_RECIPROCAL;
            // Sub-phrase rescoring re-scores already-retrieved docs via AND-clause
            // sub-phrase queries. This is the only mechanism that helps when base FTS5
            // returns the entire corpus at low scores (expansion gates never fire).
            params.enableSubPhraseRescoring = true;
            params.subPhraseScoringPenalty = 0.70f;
            // Rescue high-confidence vector matches that fall below the fusion cutoff.
            // Critical for multi-session conversational data where semantic similarity
            // captures relevance that keyword matching misses.
            params.semanticRescueSlots = TuningSlot<size_t>(2, TuningLayer::Profile);
            params.semanticRescueMinVectorScore = 0.65f;
            // Widen rerank window to give cross-encoder more candidates to reorder.
            params.rerankTopK = 12;
            params.rerankAnchoredMinRelativeScore = 0.70f;
            // Use SUM aggregation for chunk-level vector scores. Multi-chunk documents
            // (e.g., multi-turn chat sessions) benefit from accumulating semantic signal
            // across chunks rather than taking only the single best chunk.
            params.chunkAggregation = SearchEngineConfig::ChunkAggregation::SUM;
            params.graphEntitySignalWeight = 0.50F;
            params.graphStructuralSignalWeight = 0.15F;
            params.graphCoverageSignalWeight = 0.15F;
            params.graphPathSignalWeight = 0.10F;
            params.graphCorroborationFloor = 0.25F;
            break;

        case TuningState::MIXED:
            params.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Neighborhood;
            params.rrfK = 45;
            params.weights.setAll(0.40f, 0.25f, 0.10f, 0.10f, 0.05f, 0.05f, 0.05f,
                                  TuningLayer::Profile);
            break;

        case TuningState::MIXED_PRECISION:
            params.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Neighborhood;
            params.rrfK = 45;
            params.weights.setAll(0.40f, 0.25f, 0.10f, 0.10f, 0.05f, 0.05f, 0.05f,
                                  TuningLayer::Profile);
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
            params.semanticRescueSlots = TuningSlot<size_t>(1, TuningLayer::Profile);
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
            params.weights.setAll(0.55f, 0.30f, 0.05f, 0.05f, 0.00f, 0.03f, 0.02f,
                                  TuningLayer::Profile);
            break;

        case TuningState::MEDIA:
            // Binary/media-heavy corpus: titles + tags matter; body text and dense
            // embeddings are less reliable for images/audio/video.
            params.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Neighborhood;
            params.rrfK = 20;
            params.weights.setAll(0.25f, 0.20f, 0.00f, 0.15f, 0.25f, 0.10f, 0.05f,
                                  TuningLayer::Profile);
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
     * @brief Observe a user-labeled relevance session.
     *
     * The per-query reward (position-discounted precision) is aggregated into
     * a second EWMA channel that sits alongside the runtime-telemetry EWMAs.
     * This lets downstream consumers (MAB tuner, convex fusion) see "is the
     * user happy with what we're returning" as a first-class signal.
     *
     * Empty sessions are ignored.
     */
    void observeRelevanceFeedback(const RelevanceSession& session);

    /**
     * @brief Whether the adaptive EWMA state has stabilized.
     *
     * Used to gate opt-in features (P6 MAB, P7 convex fusion) that should only
     * engage once the rule-based tuner has settled. The default threshold of
     * 200 observations roughly corresponds to a few thousand queries of real
     * traffic; callers can pass a lower bound for short-lived sessions.
     *
     * A tuner is "converged" when it has at least `minObservations` observations
     * AND it has not adjusted parameters within the last cooldown window.
     */
    [[nodiscard]] bool hasConverged(std::size_t minObservations = 200) const noexcept;

    /**
     * @brief Pin weight slots that were overridden via environment variables.
     *
     * Call after seedRuntimeConfig(). Pinned slots cannot be modified by
     * downstream layers (zoom, intent, community, mode), guaranteeing that
     * env-var overrides reach the query unchanged.
     */
    void pinEnvOverrides(bool textPinned, bool vectorPinned, bool kgPinned,
                         bool similarityThresholdPinned);

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
     * @brief Enable throttled auto-persistence of adaptive EWMA state to disk.
     *
     * Every `kAdaptivePersistInterval` observations inside `observe()` the tuner
     * writes its adaptive counters to `path` via atomic rename. Pass an empty
     * path to disable.
     */
    void setAdaptivePersistPath(std::filesystem::path path);

    /**
     * @brief Write the current adaptive state to `path` atomically.
     */
    [[nodiscard]] Result<void> saveAdaptiveState(const std::filesystem::path& path) const;

    /**
     * @brief Restore adaptive EWMA counters from `path`.
     *
     * Missing or corrupt files are non-fatal: the tuner keeps its fresh state
     * and a warning is logged. `TunedParams` are NOT restored — they recompute
     * naturally once the adaptive rules reconverge on the replayed EWMAs.
     */
    Result<void> loadAdaptiveState(const std::filesystem::path& path);

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
        // User-labeled reward channel (P6/F1). `ewmaRelevanceReward` is the
        // EWMA of per-query position-discounted precision from `yams tune`
        // sessions. `relevanceSessions` counts how many labeled sessions have
        // fed into the EWMA so far.
        double ewmaRelevanceReward = 0.0;
        std::uint64_t relevanceSessions = 0;
        std::uint64_t relevanceQueries = 0;
        std::string lastRelevanceTimestamp;
        bool lastObservationChanged = false;
        SearchEngineConfig::NavigationZoomLevel lastZoomLevel =
            SearchEngineConfig::NavigationZoomLevel::Auto;
        std::map<std::string, std::uint64_t> zoomLevelCounts;
        std::string lastDecision;
    };

    [[nodiscard]] SearchEngineConfig buildConfigFromParamsLocked() const;
    [[nodiscard]] nlohmann::json adaptiveStateToJsonLocked() const;
    void observeLocked(const RuntimeTelemetry& telemetry);

    TuningState state_;
    TunedParams baseParams_;
    TunedParams params_;
    std::string stateReason_;
    storage::CorpusStats stats_; // Keep a copy for toJson()
    SearchEngineConfig baseConfig_{};
    mutable std::mutex mutex_;
    AdaptiveRuntimeState adaptive_;
    std::filesystem::path persistPath_; // empty => auto-persist disabled
    std::uint64_t lastPersistedObservation_ = 0;
};

} // namespace yams::search
