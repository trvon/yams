#include "search_config_environment_internal.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstdlib>
#include <utility>

namespace yams::search {

LegacySearchConfigEnvironment::LegacySearchConfigEnvironment(SearchEnvironmentLookup lookup)
    : lookup_(std::move(lookup)) {}

LegacySearchConfigEnvironment LegacySearchConfigEnvironment::fromProcess() {
    return LegacySearchConfigEnvironment{[](std::string_view name) -> std::optional<std::string> {
        const std::string key{name};
        if (const char* value = std::getenv(key.c_str())) {
            return std::string{value};
        }
        return std::nullopt;
    }};
}

bool LegacySearchConfigEnvironment::enabled() const {
    const auto value = lookup_("YAMS_ENABLE_ENV_OVERRIDES");
    return value && *value == "1";
}

std::optional<TuningState> LegacySearchConfigEnvironment::tuningStateOverride() const {
    if (!enabled()) {
        return std::nullopt;
    }

    const auto value = lookup_("YAMS_TUNING_OVERRIDE");
    if (!value) {
        return std::nullopt;
    }
    if (*value == "SCIENTIFIC") {
        return TuningState::SCIENTIFIC;
    }
    if (*value == "SMALL_CODE") {
        return TuningState::SMALL_CODE;
    }
    if (*value == "LARGE_CODE") {
        return TuningState::LARGE_CODE;
    }
    if (*value == "SMALL_PROSE") {
        return TuningState::SMALL_PROSE;
    }
    if (*value == "LARGE_PROSE") {
        return TuningState::LARGE_PROSE;
    }
    if (*value == "MIXED") {
        return TuningState::MIXED;
    }
    if (*value == "MIXED_PRECISION") {
        return TuningState::MIXED_PRECISION;
    }
    if (*value == "MINIMAL") {
        return TuningState::MINIMAL;
    }

    spdlog::warn("Unknown YAMS_TUNING_OVERRIDE value '{}', ignoring", *value);
    return std::nullopt;
}

SearchEnvironmentPins LegacySearchConfigEnvironment::applyTo(SearchEngineConfig& config) const {
    SearchEnvironmentPins pins;
    if (!enabled()) {
        return pins;
    }

    const auto getEnvString = [this](std::string_view name) { return lookup_(name); };
    const auto getEnvFloat = [&getEnvString](std::string_view name) -> std::optional<float> {
        if (const auto value = getEnvString(name)) {
            try {
                return std::stof(*value);
            } catch (...) {
            }
        }
        return std::nullopt;
    };
    const auto getEnvInt = [&getEnvString](std::string_view name) -> std::optional<int> {
        if (const auto value = getEnvString(name)) {
            try {
                return std::stoi(*value);
            } catch (...) {
            }
        }
        return std::nullopt;
    };
    const auto getEnvBool = [&getEnvString](std::string_view name) -> std::optional<bool> {
        if (const auto value = getEnvString(name)) {
            if (*value == "1" || *value == "true" || *value == "TRUE" || *value == "on" ||
                *value == "ON") {
                return true;
            }
            if (*value == "0" || *value == "false" || *value == "FALSE" || *value == "off" ||
                *value == "OFF") {
                return false;
            }
        }
        return std::nullopt;
    };

    // Allow environment variable overrides for individual weights (for benchmarking)
    // These take precedence over tuning state weights and are pinned so that
    // downstream layers (zoom, intent, community) cannot override them.
    bool envTextPinned = false;
    bool envSimeonTextPinned = false;
    bool envVectorPinned = false;
    bool envKgPinned = false;
    bool envSimilarityThresholdPinned = false;
    if (auto textWeight = getEnvFloat("YAMS_SEARCH_TEXT_WEIGHT")) {
        config.textWeight = *textWeight;
        envTextPinned = true;
        spdlog::info("SearchEngine textWeight overridden to {:.2f} via env (pinned)",
                     config.textWeight);
    }
    if (auto simeonTextWeight = getEnvFloat("YAMS_SEARCH_SIMEON_TEXT_WEIGHT")) {
        config.simeonTextWeight = *simeonTextWeight;
        envSimeonTextPinned = true;
        spdlog::info("SearchEngine simeonTextWeight overridden to {:.2f} via env (pinned)",
                     config.simeonTextWeight);
    }
    if (auto graphTextWeight = getEnvFloat("YAMS_SEARCH_GRAPH_TEXT_WEIGHT")) {
        config.graphTextWeight = *graphTextWeight;
        spdlog::info("SearchEngine graphTextWeight overridden to {:.2f} via env",
                     config.graphTextWeight);
    }
    if (auto vectorWeight = getEnvFloat("YAMS_SEARCH_VECTOR_WEIGHT")) {
        config.vectorWeight = *vectorWeight;
        envVectorPinned = true;
        spdlog::info("SearchEngine vectorWeight overridden to {:.2f} via env (pinned)",
                     config.vectorWeight);
    }
    if (auto similarityThreshold = getEnvFloat("YAMS_SEARCH_SIMILARITY_THRESHOLD")) {
        config.similarityThreshold = std::clamp(*similarityThreshold, 0.0f, 1.0f);
        envSimilarityThresholdPinned = true;
        spdlog::info("SearchEngine similarityThreshold overridden to {:.3f} via env (pinned)",
                     config.similarityThreshold);
    }
    if (auto kgWeight = getEnvFloat("YAMS_SEARCH_KG_WEIGHT")) {
        config.kgWeight = *kgWeight;
        envKgPinned = true;
        spdlog::info("SearchEngine kgWeight overridden to {:.2f} via env (pinned)",
                     config.kgWeight);
    }
    if (auto graphVectorWeight = getEnvFloat("YAMS_SEARCH_GRAPH_VECTOR_WEIGHT")) {
        config.graphVectorWeight = *graphVectorWeight;
        spdlog::info("SearchEngine graphVectorWeight overridden to {:.2f} via env",
                     config.graphVectorWeight);
    }
    if (auto rrfK = getEnvFloat("YAMS_SEARCH_RRF_K")) {
        config.rrfK = std::clamp(*rrfK, 1.0f, 200.0f);
        spdlog::info("SearchEngine rrfK overridden to {:.2f} via env", config.rrfK);
    }
    if (auto vectorOnlyThreshold = getEnvFloat("YAMS_SEARCH_VECTOR_ONLY_THRESHOLD")) {
        config.vectorOnlyThreshold = std::clamp(*vectorOnlyThreshold, 0.0f, 1.0f);
        spdlog::info("SearchEngine vectorOnlyThreshold overridden to {:.3f} via env",
                     config.vectorOnlyThreshold);
    }
    if (auto vectorOnlyPenalty = getEnvFloat("YAMS_SEARCH_VECTOR_ONLY_PENALTY")) {
        config.vectorOnlyPenalty = std::clamp(*vectorOnlyPenalty, 0.0f, 1.0f);
        spdlog::info("SearchEngine vectorOnlyPenalty overridden to {:.3f} via env",
                     config.vectorOnlyPenalty);
    }
    if (auto strongVectorOnlyRelief = getEnvBool("YAMS_SEARCH_ENABLE_STRONG_VECTOR_ONLY_RELIEF")) {
        config.enableStrongVectorOnlyRelief = *strongVectorOnlyRelief;
        spdlog::info("SearchEngine enableStrongVectorOnlyRelief overridden to {} via env",
                     config.enableStrongVectorOnlyRelief);
    }
    if (auto strongVectorOnlyMinScore = getEnvFloat("YAMS_SEARCH_STRONG_VECTOR_ONLY_MIN_SCORE")) {
        config.strongVectorOnlyMinScore = std::clamp(*strongVectorOnlyMinScore, 0.0f, 1.0f);
        spdlog::info("SearchEngine strongVectorOnlyMinScore overridden to {:.3f} via env",
                     config.strongVectorOnlyMinScore);
    }
    if (auto strongVectorOnlyTopRank = getEnvInt("YAMS_SEARCH_STRONG_VECTOR_ONLY_TOP_RANK")) {
        config.strongVectorOnlyTopRank = static_cast<size_t>(std::max(0, *strongVectorOnlyTopRank));
        spdlog::info("SearchEngine strongVectorOnlyTopRank overridden to {} via env",
                     config.strongVectorOnlyTopRank);
    }
    if (auto strongVectorOnlyPenalty = getEnvFloat("YAMS_SEARCH_STRONG_VECTOR_ONLY_PENALTY")) {
        config.strongVectorOnlyPenalty = std::clamp(*strongVectorOnlyPenalty, 0.0f, 1.0f);
        spdlog::info("SearchEngine strongVectorOnlyPenalty overridden to {:.3f} via env",
                     config.strongVectorOnlyPenalty);
    }
    if (auto nearMissReserve = getEnvInt("YAMS_SEARCH_VECTOR_ONLY_NEAR_MISS_RESERVE")) {
        config.vectorOnlyNearMissReserve = static_cast<size_t>(std::max(0, *nearMissReserve));
        spdlog::info("SearchEngine vectorOnlyNearMissReserve overridden to {} via env",
                     config.vectorOnlyNearMissReserve);
    }
    if (auto nearMissSlack = getEnvFloat("YAMS_SEARCH_VECTOR_ONLY_NEAR_MISS_SLACK")) {
        config.vectorOnlyNearMissSlack = std::clamp(*nearMissSlack, 0.0f, 1.0f);
        spdlog::info("SearchEngine vectorOnlyNearMissSlack overridden to {:.3f} via env",
                     config.vectorOnlyNearMissSlack);
    }
    if (auto nearMissPenalty = getEnvFloat("YAMS_SEARCH_VECTOR_ONLY_NEAR_MISS_PENALTY")) {
        config.vectorOnlyNearMissPenalty = std::clamp(*nearMissPenalty, 0.0f, 1.0f);
        spdlog::info("SearchEngine vectorOnlyNearMissPenalty overridden to {:.3f} via env",
                     config.vectorOnlyNearMissPenalty);
    }
    if (auto conceptBoostWeight = getEnvFloat("YAMS_SEARCH_CONCEPT_BOOST_WEIGHT")) {
        config.conceptBoostWeight = std::clamp(*conceptBoostWeight, 0.0f, 1.0f);
        spdlog::info("SearchEngine conceptBoostWeight overridden to {:.3f} via env",
                     config.conceptBoostWeight);
    }
    if (auto waitForConcepts = getEnvBool("YAMS_SEARCH_WAIT_FOR_CONCEPTS")) {
        config.waitForConceptExtraction = *waitForConcepts;
        spdlog::info("SearchEngine waitForConceptExtraction overridden to {} via env",
                     config.waitForConceptExtraction);
    }

    if (auto zoomLevel = getEnvString("YAMS_SEARCH_ZOOM_LEVEL")) {
        if (*zoomLevel == "AUTO") {
            config.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Auto;
        } else if (*zoomLevel == "MAP") {
            config.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Map;
        } else if (*zoomLevel == "NEIGHBORHOOD") {
            config.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Neighborhood;
        } else if (*zoomLevel == "STREET") {
            config.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Street;
        } else {
            spdlog::warn("Unknown YAMS_SEARCH_ZOOM_LEVEL value '{}', ignoring", *zoomLevel);
        }
        spdlog::info("SearchEngine zoomLevel overridden to {} via env",
                     SearchEngineConfig::navigationZoomLevelToString(config.zoomLevel));
    }

    // Allow semantic rescue, rerank, and chunk aggregation overrides
    if (auto slots = getEnvInt("YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS")) {
        config.semanticRescueSlots = static_cast<size_t>(std::max(0, *slots));
        spdlog::info("SearchEngine semanticRescueSlots overridden to {} via env",
                     config.semanticRescueSlots);
    }
    if (auto minScore = getEnvFloat("YAMS_SEARCH_SEMANTIC_RESCUE_MIN_SCORE")) {
        config.semanticRescueMinVectorScore = std::clamp(*minScore, 0.0f, 1.0f);
        spdlog::info("SearchEngine semanticRescueMinVectorScore overridden to {:.3f} via env",
                     config.semanticRescueMinVectorScore);
    }
    if (auto topK = getEnvInt("YAMS_SEARCH_RERANK_TOP_K")) {
        config.rerankTopK = static_cast<size_t>(std::max(1, *topK));
        spdlog::info("SearchEngine rerankTopK overridden to {} via env", config.rerankTopK);
    }
    if (auto aggEnv = getEnvString("YAMS_SEARCH_CHUNK_AGGREGATION")) {
        if (*aggEnv == "MAX" || *aggEnv == "max") {
            config.chunkAggregation = SearchEngineConfig::ChunkAggregation::MAX;
        } else if (*aggEnv == "SUM" || *aggEnv == "sum") {
            config.chunkAggregation = SearchEngineConfig::ChunkAggregation::SUM;
        } else if (*aggEnv == "TOP_K_AVG" || *aggEnv == "top_k_avg") {
            config.chunkAggregation = SearchEngineConfig::ChunkAggregation::TOP_K_AVG;
        } else if (*aggEnv == "WEIGHTED_TOP_K_AVG" || *aggEnv == "weighted_top_k_avg" ||
                   *aggEnv == "WEIGHTED" || *aggEnv == "weighted") {
            config.chunkAggregation = SearchEngineConfig::ChunkAggregation::WEIGHTED_TOP_K_AVG;
        } else {
            spdlog::warn("Unknown YAMS_SEARCH_CHUNK_AGGREGATION value '{}', ignoring", *aggEnv);
        }
        spdlog::info("SearchEngine chunkAggregation overridden via env");
    }

    // Allow candidate limit overrides for recall benchmarking
    // YAMS_CANDIDATE_MULTIPLIER scales all maxResults values (e.g., 2.0 = 2x candidates)
    if (auto multiplier = getEnvFloat("YAMS_CANDIDATE_MULTIPLIER")) {
        config.textMaxResults = static_cast<size_t>(config.textMaxResults * *multiplier);
        config.vectorMaxResults = static_cast<size_t>(config.vectorMaxResults * *multiplier);
        config.entityVectorMaxResults =
            static_cast<size_t>(config.entityVectorMaxResults * *multiplier);
        config.pathTreeMaxResults = static_cast<size_t>(config.pathTreeMaxResults * *multiplier);
        config.kgMaxResults = static_cast<size_t>(config.kgMaxResults * *multiplier);
        config.tagMaxResults = static_cast<size_t>(config.tagMaxResults * *multiplier);
        config.metadataMaxResults = static_cast<size_t>(config.metadataMaxResults * *multiplier);
        spdlog::info("SearchEngine candidate limits scaled by {:.2f}x via env (text={}, vec={})",
                     *multiplier, config.textMaxResults, config.vectorMaxResults);
    }

    // Individual maxResults overrides
    if (auto textMax = getEnvInt("YAMS_TEXT_MAX_RESULTS")) {
        config.textMaxResults = static_cast<size_t>(*textMax);
        spdlog::info("SearchEngine textMaxResults overridden to {} via env", config.textMaxResults);
    }
    if (auto vectorMax = getEnvInt("YAMS_VECTOR_MAX_RESULTS")) {
        config.vectorMaxResults = static_cast<size_t>(*vectorMax);
        spdlog::info("SearchEngine vectorMaxResults overridden to {} via env",
                     config.vectorMaxResults);
    }
    if (auto kgMax = getEnvInt("YAMS_KG_MAX_RESULTS")) {
        config.kgMaxResults = static_cast<size_t>(*kgMax);
        spdlog::info("SearchEngine kgMaxResults overridden to {} via env", config.kgMaxResults);
    }

    if (auto intentAdaptive = getEnvBool("YAMS_SEARCH_ENABLE_INTENT_ADAPTIVE")) {
        config.enableIntentAdaptiveWeighting = *intentAdaptive;
        spdlog::info("SearchEngine enableIntentAdaptiveWeighting overridden to {} via env",
                     config.enableIntentAdaptiveWeighting);
    }

    if (auto lexicalExpansion = getEnvBool("YAMS_SEARCH_ENABLE_LEXICAL_EXPANSION")) {
        config.enableLexicalExpansion = *lexicalExpansion;
        spdlog::info("SearchEngine enableLexicalExpansion overridden to {} via env",
                     config.enableLexicalExpansion);
    }

    if (auto lexicalMinHits = getEnvInt("YAMS_SEARCH_LEXICAL_EXPANSION_MIN_HITS")) {
        config.lexicalExpansionMinHits = static_cast<size_t>(std::max(0, *lexicalMinHits));
        spdlog::info("SearchEngine lexicalExpansionMinHits overridden to {} via env",
                     config.lexicalExpansionMinHits);
    }

    if (auto lexicalPenalty = getEnvFloat("YAMS_SEARCH_LEXICAL_EXPANSION_PENALTY")) {
        config.lexicalExpansionScorePenalty = std::clamp(*lexicalPenalty, 0.1f, 1.0f);
        spdlog::info("SearchEngine lexicalExpansionScorePenalty overridden to {:.2f} via env",
                     config.lexicalExpansionScorePenalty);
    }

    if (auto lexicalFloorTopN = getEnvInt("YAMS_SEARCH_LEXICAL_FLOOR_TOPN")) {
        config.lexicalFloorTopN = static_cast<size_t>(std::max(0, *lexicalFloorTopN));
        spdlog::info("SearchEngine lexicalFloorTopN overridden to {} via env",
                     config.lexicalFloorTopN);
    }

    if (auto lexicalFloorBoost = getEnvFloat("YAMS_SEARCH_LEXICAL_FLOOR_BOOST")) {
        config.lexicalFloorBoost = std::clamp(*lexicalFloorBoost, 0.0f, 1.0f);
        spdlog::info("SearchEngine lexicalFloorBoost overridden to {:.3f} via env",
                     config.lexicalFloorBoost);
    }

    if (auto lexicalTieBreak = getEnvBool("YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK")) {
        config.enableLexicalTieBreak = *lexicalTieBreak;
        spdlog::info("SearchEngine enableLexicalTieBreak overridden to {} via env",
                     config.enableLexicalTieBreak);
    }

    if (auto lexicalTieBreakEps = getEnvFloat("YAMS_SEARCH_LEXICAL_TIEBREAK_EPS")) {
        config.lexicalTieBreakEpsilon = std::max(0.0f, *lexicalTieBreakEps);
        spdlog::info("SearchEngine lexicalTieBreakEpsilon overridden to {:.4f} via env",
                     config.lexicalTieBreakEpsilon);
    }

    if (auto semanticRescueSlots = getEnvInt("YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS")) {
        config.semanticRescueSlots = static_cast<size_t>(std::max(0, *semanticRescueSlots));
        spdlog::info("SearchEngine semanticRescueSlots overridden to {} via env",
                     config.semanticRescueSlots);
    }

    if (auto semanticRescueMinVector =
            getEnvFloat("YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE")) {
        config.semanticRescueMinVectorScore = std::max(0.0f, *semanticRescueMinVector);
        spdlog::info("SearchEngine semanticRescueMinVectorScore overridden to {:.4f} via env",
                     config.semanticRescueMinVectorScore);
    }

    if (auto rerankingEnabled = getEnvBool("YAMS_SEARCH_ENABLE_RERANKING")) {
        config.enableReranking = *rerankingEnabled;
        spdlog::info("SearchEngine enableReranking overridden to {} via env",
                     config.enableReranking);
    }

    if (auto rerankTopK = getEnvInt("YAMS_SEARCH_RERANK_TOPK")) {
        config.rerankTopK = static_cast<size_t>(std::max(0, *rerankTopK));
        spdlog::info("SearchEngine rerankTopK overridden to {} via env", config.rerankTopK);
    }

    if (auto rerankReplace = getEnvBool("YAMS_SEARCH_RERANK_REPLACE_SCORES")) {
        config.rerankReplaceScores = *rerankReplace;
        spdlog::info("SearchEngine rerankReplaceScores overridden to {} via env",
                     config.rerankReplaceScores);
    }

    if (auto rerankWeight = getEnvFloat("YAMS_SEARCH_RERANK_WEIGHT")) {
        config.rerankBlendWeight = std::clamp(*rerankWeight, 0.0f, 1.0f);
        spdlog::info("SearchEngine rerankBlendWeight overridden to {:.3f} via env",
                     config.rerankBlendWeight);
    }

    if (auto rerankGap = getEnvFloat("YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD")) {
        config.rerankScoreGapThreshold = std::max(0.0f, *rerankGap);
        spdlog::info("SearchEngine rerankScoreGapThreshold overridden to {:.6f} via env",
                     config.rerankScoreGapThreshold);
    }

    if (auto rerankSnippetChars = getEnvInt("YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS")) {
        config.rerankSnippetMaxChars = static_cast<size_t>(std::max(0, *rerankSnippetChars));
        spdlog::info("SearchEngine rerankSnippetMaxChars overridden to {} via env",
                     config.rerankSnippetMaxChars);
    }

    if (auto graphRerankEnabled = getEnvBool("YAMS_SEARCH_ENABLE_GRAPH_RERANK")) {
        config.enableGraphRerank = *graphRerankEnabled;
        spdlog::info("SearchEngine enableGraphRerank overridden to {} via env",
                     config.enableGraphRerank);
    }

    if (auto graphTopN = getEnvInt("YAMS_SEARCH_GRAPH_RERANK_TOPN")) {
        config.graphRerankTopN = static_cast<size_t>(std::max(0, *graphTopN));
        spdlog::info("SearchEngine graphRerankTopN overridden to {} via env",
                     config.graphRerankTopN);
    }

    if (auto includeTiming = getEnvBool("YAMS_SEARCH_INCLUDE_COMPONENT_TIMING")) {
        config.includeComponentTiming = *includeTiming;
        spdlog::info("SearchEngine includeComponentTiming overridden to {} via env",
                     config.includeComponentTiming);
    }

    if (auto graphWeight = getEnvFloat("YAMS_SEARCH_GRAPH_RERANK_WEIGHT")) {
        config.graphRerankWeight = std::max(0.0f, *graphWeight);
        spdlog::info("SearchEngine graphRerankWeight overridden to {:.3f} via env",
                     config.graphRerankWeight);
    }

    if (auto graphMaxBoost = getEnvFloat("YAMS_SEARCH_GRAPH_RERANK_MAX_BOOST")) {
        config.graphRerankMaxBoost = std::max(0.0f, *graphMaxBoost);
        spdlog::info("SearchEngine graphRerankMaxBoost overridden to {:.3f} via env",
                     config.graphRerankMaxBoost);
    }

    if (auto graphMinSignal = getEnvFloat("YAMS_SEARCH_GRAPH_RERANK_MIN_SIGNAL")) {
        config.graphRerankMinSignal = std::max(0.0f, *graphMinSignal);
        spdlog::info("SearchEngine graphRerankMinSignal overridden to {:.3f} via env",
                     config.graphRerankMinSignal);
    }
    if (auto graphCommunityWeight = getEnvFloat("YAMS_SEARCH_GRAPH_COMMUNITY_WEIGHT")) {
        config.graphCommunityWeight = std::clamp(*graphCommunityWeight, 0.0f, 1.0f);
        spdlog::info("SearchEngine graphCommunityWeight overridden to {:.3f} via env",
                     config.graphCommunityWeight);
    }
    if (auto graphUseQueryConcepts = getEnvBool("YAMS_SEARCH_GRAPH_USE_QUERY_CONCEPTS")) {
        config.graphUseQueryConcepts = *graphUseQueryConcepts;
        spdlog::info("SearchEngine graphUseQueryConcepts overridden to {} via env",
                     config.graphUseQueryConcepts);
    }
    if (auto graphFallbackTopSignal = getEnvBool("YAMS_SEARCH_GRAPH_FALLBACK_TOP_SIGNAL")) {
        config.graphFallbackToTopSignal = *graphFallbackTopSignal;
        spdlog::info("SearchEngine graphFallbackToTopSignal overridden to {} via env",
                     config.graphFallbackToTopSignal);
    }

    if (auto graphNeighbors = getEnvInt("YAMS_SEARCH_GRAPH_MAX_NEIGHBORS")) {
        config.graphMaxNeighbors = static_cast<size_t>(std::max(1, *graphNeighbors));
        spdlog::info("SearchEngine graphMaxNeighbors overridden to {} via env",
                     config.graphMaxNeighbors);
    }

    if (auto graphHops = getEnvInt("YAMS_SEARCH_GRAPH_MAX_HOPS")) {
        config.graphMaxHops = static_cast<size_t>(std::clamp(*graphHops, 1, 5));
        spdlog::info("SearchEngine graphMaxHops overridden to {} via env", config.graphMaxHops);
    }

    if (auto graphBudgetMs = getEnvInt("YAMS_SEARCH_GRAPH_BUDGET_MS")) {
        config.graphScoringBudgetMs = std::max(0, *graphBudgetMs);
        spdlog::info("SearchEngine graphScoringBudgetMs overridden to {} via env",
                     config.graphScoringBudgetMs);
    }

    if (auto graphPaths = getEnvBool("YAMS_SEARCH_GRAPH_ENABLE_PATHS")) {
        config.graphEnablePathEnumeration = *graphPaths;
        spdlog::info("SearchEngine graphEnablePathEnumeration overridden to {} via env",
                     config.graphEnablePathEnumeration);
    }

    if (auto graphMaxPaths = getEnvInt("YAMS_SEARCH_GRAPH_MAX_PATHS")) {
        config.graphMaxPaths = static_cast<size_t>(std::max(1, *graphMaxPaths));
        spdlog::info("SearchEngine graphMaxPaths overridden to {} via env", config.graphMaxPaths);
    }

    if (auto graphHopDecay = getEnvFloat("YAMS_SEARCH_GRAPH_HOP_DECAY")) {
        config.graphHopDecay = std::clamp(*graphHopDecay, 0.0f, 1.0f);
        spdlog::info("SearchEngine graphHopDecay overridden to {:.3f} via env",
                     config.graphHopDecay);
    }

    if (auto tieredNarrow = getEnvBool("YAMS_SEARCH_TIERED_NARROW_VECTOR_SEARCH")) {
        config.tieredNarrowVectorSearch = *tieredNarrow;
        spdlog::info("SearchEngine tieredNarrowVectorSearch overridden to {} via env",
                     config.tieredNarrowVectorSearch);
    }

    if (auto tieredMinCandidates = getEnvInt("YAMS_SEARCH_TIERED_MIN_CANDIDATES")) {
        config.tieredMinCandidates = static_cast<size_t>(std::max(0, *tieredMinCandidates));
        spdlog::info("SearchEngine tieredMinCandidates overridden to {} via env",
                     config.tieredMinCandidates);
    }

    if (auto weakFanout = getEnvBool("YAMS_SEARCH_ENABLE_WEAK_QUERY_FANOUT_BOOST")) {
        config.enableWeakQueryFanoutBoost = *weakFanout;
        spdlog::info("SearchEngine enableWeakQueryFanoutBoost overridden to {} via env",
                     config.enableWeakQueryFanoutBoost);
    }
    if (auto weakVectorFanout = getEnvFloat("YAMS_SEARCH_WEAK_QUERY_VECTOR_FANOUT_MULTIPLIER")) {
        config.weakQueryVectorFanoutMultiplier = std::max(1.0f, *weakVectorFanout);
        spdlog::info("SearchEngine weakQueryVectorFanoutMultiplier overridden to {:.2f} via env",
                     config.weakQueryVectorFanoutMultiplier);
    }
    if (auto weakEntityFanout =
            getEnvFloat("YAMS_SEARCH_WEAK_QUERY_ENTITY_VECTOR_FANOUT_MULTIPLIER")) {
        config.weakQueryEntityVectorFanoutMultiplier = std::max(1.0f, *weakEntityFanout);
        spdlog::info(
            "SearchEngine weakQueryEntityVectorFanoutMultiplier overridden to {:.2f} via env",
            config.weakQueryEntityVectorFanoutMultiplier);
    }
    if (auto bypassWarming = getEnvBool("YAMS_SEARCH_BYPASS_CORPUS_WARMING_GATE")) {
        config.bypassCorpusWarmingGate = *bypassWarming;
        spdlog::info("SearchEngine bypassCorpusWarmingGate overridden to {} via env",
                     config.bypassCorpusWarmingGate);
    }
    if (auto evidenceRescueSlots = getEnvInt("YAMS_SEARCH_FUSION_EVIDENCE_RESCUE_SLOTS")) {
        config.fusionEvidenceRescueSlots = static_cast<size_t>(std::max(0, *evidenceRescueSlots));
        spdlog::info("SearchEngine fusionEvidenceRescueSlots overridden to {} via env",
                     config.fusionEvidenceRescueSlots);
    }
    if (auto evidenceRescueMinScore = getEnvFloat("YAMS_SEARCH_FUSION_EVIDENCE_RESCUE_MIN_SCORE")) {
        config.fusionEvidenceRescueMinScore = std::max(0.0f, *evidenceRescueMinScore);
        spdlog::info("SearchEngine fusionEvidenceRescueMinScore overridden to {:.3f} via env",
                     config.fusionEvidenceRescueMinScore);
    }
    if (auto graphQueryExpansion = getEnvBool("YAMS_SEARCH_ENABLE_GRAPH_QUERY_EXPANSION")) {
        config.enableGraphQueryExpansion = *graphQueryExpansion;
        spdlog::info("SearchEngine enableGraphQueryExpansion overridden to {} via env",
                     config.enableGraphQueryExpansion);
    }
    if (auto graphExpansionMinHits = getEnvInt("YAMS_SEARCH_GRAPH_EXPANSION_MIN_HITS")) {
        config.graphExpansionMinHits = static_cast<size_t>(std::max(0, *graphExpansionMinHits));
        spdlog::info("SearchEngine graphExpansionMinHits overridden to {} via env",
                     config.graphExpansionMinHits);
    }
    if (auto graphExpansionMaxTerms = getEnvInt("YAMS_SEARCH_GRAPH_EXPANSION_MAX_TERMS")) {
        config.graphExpansionMaxTerms = static_cast<size_t>(std::max(0, *graphExpansionMaxTerms));
        spdlog::info("SearchEngine graphExpansionMaxTerms overridden to {} via env",
                     config.graphExpansionMaxTerms);
    }
    if (auto graphExpansionMaxSeeds = getEnvInt("YAMS_SEARCH_GRAPH_EXPANSION_MAX_SEEDS")) {
        config.graphExpansionMaxSeeds = static_cast<size_t>(std::max(0, *graphExpansionMaxSeeds));
        spdlog::info("SearchEngine graphExpansionMaxSeeds overridden to {} via env",
                     config.graphExpansionMaxSeeds);
    }
    if (auto graphExpansionQueryNeighborK =
            getEnvInt("YAMS_SEARCH_GRAPH_EXPANSION_QUERY_NEIGHBOR_K")) {
        config.graphExpansionQueryNeighborK =
            static_cast<size_t>(std::max(0, *graphExpansionQueryNeighborK));
        spdlog::info("SearchEngine graphExpansionQueryNeighborK overridden to {} via env",
                     config.graphExpansionQueryNeighborK);
    }
    if (auto graphExpansionQueryNeighborMinScore =
            getEnvFloat("YAMS_SEARCH_GRAPH_EXPANSION_QUERY_NEIGHBOR_MIN_SCORE")) {
        config.graphExpansionQueryNeighborMinScore =
            std::clamp(*graphExpansionQueryNeighborMinScore, 0.0f, 1.0f);
        spdlog::info(
            "SearchEngine graphExpansionQueryNeighborMinScore overridden to {:.3f} via env",
            config.graphExpansionQueryNeighborMinScore);
    }
    if (auto graphVectorRequireCorroboration =
            getEnvBool("YAMS_SEARCH_GRAPH_VECTOR_REQUIRE_CORROBORATION")) {
        config.graphVectorRequireCorroboration = *graphVectorRequireCorroboration;
        spdlog::info("SearchEngine graphVectorRequireCorroboration overridden to {} via env",
                     config.graphVectorRequireCorroboration);
    }
    if (auto graphVectorRequireTextAnchoring =
            getEnvBool("YAMS_SEARCH_GRAPH_VECTOR_REQUIRE_TEXT_ANCHORING")) {
        config.graphVectorRequireTextAnchoring = *graphVectorRequireTextAnchoring;
        spdlog::info("SearchEngine graphVectorRequireTextAnchoring overridden to {} via env",
                     config.graphVectorRequireTextAnchoring);
    }
    if (auto graphVectorRequireBaselineTextAnchoring =
            getEnvBool("YAMS_SEARCH_GRAPH_VECTOR_REQUIRE_BASELINE_TEXT_ANCHORING")) {
        config.graphVectorRequireBaselineTextAnchoring = *graphVectorRequireBaselineTextAnchoring;
        spdlog::info(
            "SearchEngine graphVectorRequireBaselineTextAnchoring overridden to {} via env",
            config.graphVectorRequireBaselineTextAnchoring);
    }
    if (auto graphFusionWindowGuard = getEnvBool("YAMS_SEARCH_ENABLE_GRAPH_FUSION_WINDOW_GUARD")) {
        config.enableGraphFusionWindowGuard = *graphFusionWindowGuard;
        spdlog::info("SearchEngine enableGraphFusionWindowGuard overridden to {} via env",
                     config.enableGraphFusionWindowGuard);
    }
    if (auto graphFusionGuardDepthMultiplier =
            getEnvInt("YAMS_SEARCH_GRAPH_FUSION_GUARD_DEPTH_MULTIPLIER")) {
        config.graphFusionGuardDepthMultiplier =
            static_cast<size_t>(std::max(1, *graphFusionGuardDepthMultiplier));
        spdlog::info("SearchEngine graphFusionGuardDepthMultiplier overridden to {} via env",
                     config.graphFusionGuardDepthMultiplier);
    }
    if (auto graphMaxAddedInFusionWindow =
            getEnvInt("YAMS_SEARCH_GRAPH_MAX_ADDED_IN_FUSION_WINDOW")) {
        config.graphMaxAddedInFusionWindow =
            static_cast<size_t>(std::max(0, *graphMaxAddedInFusionWindow));
        spdlog::info("SearchEngine graphMaxAddedInFusionWindow overridden to {} via env",
                     config.graphMaxAddedInFusionWindow);
    }
    if (auto graphTextMinAdmissionScore =
            getEnvFloat("YAMS_SEARCH_GRAPH_TEXT_MIN_ADMISSION_SCORE")) {
        config.graphTextMinAdmissionScore = std::max(0.0f, *graphTextMinAdmissionScore);
        spdlog::info("SearchEngine graphTextMinAdmissionScore overridden to {:.4f} via env",
                     config.graphTextMinAdmissionScore);
    }
    if (auto graphExpansionFtsPenalty = getEnvFloat("YAMS_SEARCH_GRAPH_EXPANSION_FTS_PENALTY")) {
        config.graphExpansionFtsPenalty = std::clamp(*graphExpansionFtsPenalty, 0.1f, 1.0f);
        spdlog::info("SearchEngine graphExpansionFtsPenalty overridden to {:.3f} via env",
                     config.graphExpansionFtsPenalty);
    }
    if (auto graphExpansionVectorPenalty =
            getEnvFloat("YAMS_SEARCH_GRAPH_EXPANSION_VECTOR_PENALTY")) {
        config.graphExpansionVectorPenalty = std::clamp(*graphExpansionVectorPenalty, 0.1f, 1.0f);
        spdlog::info("SearchEngine graphExpansionVectorPenalty overridden to {:.3f} via env",
                     config.graphExpansionVectorPenalty);
    }

    if (auto weakMinTextHits = getEnvInt("YAMS_SEARCH_WEAK_QUERY_MIN_TEXT_HITS")) {
        config.weakQueryMinTextHits = static_cast<size_t>(std::max(0, *weakMinTextHits));
        spdlog::info("SearchEngine weakQueryMinTextHits overridden to {} via env",
                     config.weakQueryMinTextHits);
    }

    if (auto weakMinTopText = getEnvFloat("YAMS_SEARCH_WEAK_QUERY_MIN_TOP_TEXT_SCORE")) {
        config.weakQueryMinTopTextScore = std::clamp(*weakMinTopText, 0.0f, 1.0f);
        spdlog::info("SearchEngine weakQueryMinTopTextScore overridden to {:.3f} via env",
                     config.weakQueryMinTopTextScore);
    }

    // Multi-vector sub-phrase search overrides
    if (auto multiVec = getEnvBool("YAMS_SEARCH_MULTI_VECTOR_QUERY")) {
        config.enableMultiVectorQuery = *multiVec;
        spdlog::info("SearchEngine enableMultiVectorQuery overridden to {} via env",
                     config.enableMultiVectorQuery);
    }
    if (auto multiVecPhrases = getEnvInt("YAMS_SEARCH_MULTI_VECTOR_MAX_PHRASES")) {
        config.multiVectorMaxPhrases = static_cast<size_t>(std::clamp(*multiVecPhrases, 1, 8));
        spdlog::info("SearchEngine multiVectorMaxPhrases overridden to {} via env",
                     config.multiVectorMaxPhrases);
    }
    if (auto multiVecDecay = getEnvFloat("YAMS_SEARCH_MULTI_VECTOR_SCORE_DECAY")) {
        config.multiVectorScoreDecay = std::clamp(*multiVecDecay, 0.1f, 1.0f);
        spdlog::info("SearchEngine multiVectorScoreDecay overridden to {:.3f} via env",
                     config.multiVectorScoreDecay);
    }

    // Sub-phrase FTS expansion overrides
    if (auto subPhrase = getEnvBool("YAMS_SEARCH_SUB_PHRASE_EXPANSION")) {
        config.enableSubPhraseExpansion = *subPhrase;
        spdlog::info("SearchEngine enableSubPhraseExpansion overridden to {} via env",
                     config.enableSubPhraseExpansion);
    }
    if (auto subPhraseMinHits = getEnvInt("YAMS_SEARCH_SUB_PHRASE_MIN_HITS")) {
        config.subPhraseExpansionMinHits = static_cast<size_t>(std::max(0, *subPhraseMinHits));
        spdlog::info("SearchEngine subPhraseExpansionMinHits overridden to {} via env",
                     config.subPhraseExpansionMinHits);
    }
    if (auto subPhrasePenalty = getEnvFloat("YAMS_SEARCH_SUB_PHRASE_PENALTY")) {
        config.subPhraseExpansionPenalty = std::clamp(*subPhrasePenalty, 0.1f, 1.0f);
        spdlog::info("SearchEngine subPhraseExpansionPenalty overridden to {:.3f} via env",
                     config.subPhraseExpansionPenalty);
    }

    pins.text = envTextPinned;
    pins.simeonText = envSimeonTextPinned;
    pins.vector = envVectorPinned;
    pins.kg = envKgPinned;
    pins.similarityThreshold = envSimilarityThresholdPinned;
    return pins;
}

} // namespace yams::search
