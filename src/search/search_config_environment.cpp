#include "search_config_environment_internal.h"

#include <yams/config/config_helpers.h>
#include <yams/search/search_environment.hpp>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <utility>

namespace yams::search {

LegacySearchConfigEnvironment::LegacySearchConfigEnvironment(SearchEnvironmentLookup lookup)
    : lookup_(std::move(lookup)) {}

LegacySearchConfigEnvironment LegacySearchConfigEnvironment::fromProcess() {
    return LegacySearchConfigEnvironment{
        [](std::string_view name) { return yams::config::getenv_optional(name); }};
}

SearchEnvironmentSnapshot snapshotLegacySearchEnvironment() {
    SearchEnvironmentSnapshot snapshot;
    LegacySearchConfigEnvironment recordingEnvironment{
        [&snapshot](std::string_view name) -> std::optional<std::string> {
            auto value = yams::config::getenv_optional(name);
            if (value.has_value()) {
                snapshot.emplace(std::string{name}, *value);
            }
            return value;
        }};
    (void)recordingEnvironment.tuningStateOverride();
    SearchEngineConfig probe;
    (void)recordingEnvironment.applyTo(probe);
    return snapshot;
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
            } catch (const std::exception& error) {
                spdlog::warn("Invalid legacy search environment {}='{}': {}", name, *value,
                             error.what());
            }
        }
        return std::nullopt;
    };
    const auto getEnvInt = [&getEnvString](std::string_view name) -> std::optional<int> {
        if (const auto value = getEnvString(name)) {
            try {
                return std::stoi(*value);
            } catch (const std::exception& error) {
                spdlog::warn("Invalid legacy search environment {}='{}': {}", name, *value,
                             error.what());
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
    if (auto strongVectorOnlyPenalty = getEnvFloat("YAMS_SEARCH_STRONG_VECTOR_ONLY_PENALTY")) {
        config.strongVectorOnlyPenalty = std::clamp(*strongVectorOnlyPenalty, 0.0f, 1.0f);
        spdlog::info("SearchEngine strongVectorOnlyPenalty overridden to {:.3f} via env",
                     config.strongVectorOnlyPenalty);
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

    // Allow chunk aggregation overrides
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

    if (auto graphBudgetMs = getEnvInt("YAMS_SEARCH_GRAPH_BUDGET_MS")) {
        config.graphScoringBudgetMs = std::max(0, *graphBudgetMs);
        spdlog::info("SearchEngine graphScoringBudgetMs overridden to {} via env",
                     config.graphScoringBudgetMs);
    }

    if (auto weakVectorFanout = getEnvFloat("YAMS_SEARCH_WEAK_QUERY_VECTOR_FANOUT_MULTIPLIER")) {
        config.weakQueryVectorFanoutMultiplier = std::max(1.0f, *weakVectorFanout);
        spdlog::info("SearchEngine weakQueryVectorFanoutMultiplier overridden to {:.2f} via env",
                     config.weakQueryVectorFanoutMultiplier);
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
