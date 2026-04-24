#include <yams/search/search_engine_builder.h>

#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine.h>
#include <yams/search/search_tuner.h>
#include <yams/topology/topology_artifacts.h>
#include <yams/vector/vector_database.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstdlib>

namespace yams::search {

namespace {

bool envEnabled(const char* name) {
    if (const char* env = std::getenv(name)) {
        return std::string(env) == "1";
    }
    return false;
}

std::optional<std::string> getEnvString(const char* name) {
    if (const char* env = std::getenv(name)) {
        return std::string(env);
    }
    return std::nullopt;
}

std::optional<float> getEnvFloat(const char* name) {
    if (auto val = getEnvString(name)) {
        try {
            return std::stof(*val);
        } catch (...) {
            return std::nullopt;
        }
    }
    return std::nullopt;
}

std::optional<int> getEnvInt(const char* name) {
    if (auto val = getEnvString(name)) {
        try {
            return std::stoi(*val);
        } catch (...) {
            return std::nullopt;
        }
    }
    return std::nullopt;
}

std::optional<bool> getEnvBool(const char* name) {
    if (auto val = getEnvString(name)) {
        if (*val == "1" || *val == "true" || *val == "TRUE" || *val == "on" || *val == "ON") {
            return true;
        }
        if (*val == "0" || *val == "false" || *val == "FALSE" || *val == "off" || *val == "OFF") {
            return false;
        }
    }
    return std::nullopt;
}

} // namespace

// ------------------------------
// SearchEngineBuilder
// ------------------------------

Result<std::shared_ptr<SearchEngine>>
SearchEngineBuilder::buildEmbedded(const BuildOptions& options) {
    spdlog::info("Building embedded SearchEngine (autoTune={})", options.autoTune);

    // Validate required dependencies
    const bool vectorRequested = options.config.vectorWeight > 0.0f;
    if (vectorRequested && !vectorDatabase_) {
        return Error{ErrorCode::InvalidArgument,
                     "SearchEngineBuilder: VectorDatabase not provided"};
    }
    if (!metadataRepo_) {
        return Error{ErrorCode::InvalidArgument,
                     "SearchEngineBuilder: MetadataRepository not provided"};
    }

    // Determine config: auto-tune or use provided config
    SearchEngineConfig cfg = options.config;

    const bool allowEnvOverrides = envEnabled("YAMS_ENABLE_ENV_OVERRIDES");

    // Check for environment variable override first (useful for benchmarks)
    // This allows setting YAMS_TUNING_OVERRIDE=SCIENTIFIC without code changes
    std::optional<TuningState> envOverride;
    if (allowEnvOverrides) {
        if (auto val = getEnvString("YAMS_TUNING_OVERRIDE")) {
            if (*val == "SCIENTIFIC") {
                envOverride = TuningState::SCIENTIFIC;
            } else if (*val == "SMALL_CODE") {
                envOverride = TuningState::SMALL_CODE;
            } else if (*val == "LARGE_CODE") {
                envOverride = TuningState::LARGE_CODE;
            } else if (*val == "SMALL_PROSE") {
                envOverride = TuningState::SMALL_PROSE;
            } else if (*val == "LARGE_PROSE") {
                envOverride = TuningState::LARGE_PROSE;
            } else if (*val == "MIXED") {
                envOverride = TuningState::MIXED;
            } else if (*val == "MIXED_PRECISION") {
                envOverride = TuningState::MIXED_PRECISION;
            } else if (*val == "MINIMAL") {
                envOverride = TuningState::MINIMAL;
            } else {
                spdlog::warn("Unknown YAMS_TUNING_OVERRIDE value '{}', ignoring", *val);
            }
        }
    }

    // Priority: env override > options override > autoTune
    std::optional<TuningState> effectiveOverride =
        envOverride.has_value() ? envOverride : options.tuningStateOverride;

    std::shared_ptr<SearchTuner> runtimeTuner;

    // Check for tuning state override (useful for benchmarks with known corpus types)
    if (effectiveOverride.has_value()) {
        TuningState overrideState = effectiveOverride.value();
        TunedParams params = getTunedParams(overrideState);
        params.applyTo(cfg);

        // Preserve user-specified options that shouldn't be overridden by tuner
        cfg.maxResults = options.config.maxResults;
        cfg.enableParallelExecution = options.config.enableParallelExecution;
        cfg.includeDebugInfo = options.config.includeDebugInfo;

        spdlog::info(
            "SearchEngine using override state={} (zoom={}, k={}, text={:.2f}, vector={:.2f}, "
            "fusion={}, vector_gate={:.2f}/{:.2f}, lexical_floor={}@{:.3f}, "
            "path_dedup={}, lexical_tiebreak={}, semantic_rescue={}@{:.4f})",
            tuningStateToString(overrideState),
            SearchEngineConfig::navigationZoomLevelToString(cfg.zoomLevel), params.rrfK,
            cfg.textWeight, cfg.vectorWeight,
            SearchEngineConfig::fusionStrategyToString(cfg.fusionStrategy), cfg.vectorOnlyThreshold,
            cfg.vectorOnlyPenalty, cfg.lexicalFloorTopN, cfg.lexicalFloorBoost,
            cfg.enablePathDedupInFusion, cfg.enableLexicalTieBreak, cfg.semanticRescueSlots,
            cfg.semanticRescueMinVectorScore);
    } else if (options.autoTune && metadataRepo_) {
        // Get corpus statistics from metadata repository
        auto statsResult = metadataRepo_->getCorpusStats();
        if (statsResult.has_value()) {
            // Create tuner and get optimized config
            runtimeTuner = std::make_shared<SearchTuner>(statsResult.value());
            cfg = runtimeTuner->getConfig();

            // Preserve user-specified options that shouldn't be overridden by tuner
            cfg.maxResults = options.config.maxResults;
            cfg.enableParallelExecution = options.config.enableParallelExecution;
            cfg.includeDebugInfo = options.config.includeDebugInfo;
            {
                const auto& tp = runtimeTuner->getParams();
                spdlog::info("SearchEngine auto-tuned to state={} overlay={} reconciled_at={} "
                             "(zoom={}, k={}, "
                             "text={:.2f}[{}], vector={:.2f}[{}], kg={:.2f}[{}], "
                             "fusion={}, semantic_rescue={}[{}]@{:.4f})",
                             tuningStateToString(runtimeTuner->currentState()),
                             statsResult.value().usedOnlineOverlay,
                             statsResult.value().reconciledComputedAtMs,
                             SearchEngineConfig::navigationZoomLevelToString(cfg.zoomLevel),
                             runtimeTuner->getRrfK(), tp.weights.text.value,
                             tuningLayerToString(tp.weights.text.source), tp.weights.vector.value,
                             tuningLayerToString(tp.weights.vector.source), tp.weights.kg.value,
                             tuningLayerToString(tp.weights.kg.source),
                             SearchEngineConfig::fusionStrategyToString(cfg.fusionStrategy),
                             tp.semanticRescueSlots.value,
                             tuningLayerToString(tp.semanticRescueSlots.source),
                             cfg.semanticRescueMinVectorScore);
            }
        } else {
            spdlog::warn("SearchTuner: failed to get corpus stats ({}), using default config",
                         statsResult.error().message);
        }
    }

    // Allow environment variable overrides for individual weights (for benchmarking)
    // These take precedence over tuning state weights and are pinned so that
    // downstream layers (zoom, intent, community) cannot override them.
    bool envTextPinned = false;
    bool envVectorPinned = false;
    bool envKgPinned = false;
    bool envSimilarityThresholdPinned = false;
    if (allowEnvOverrides) {
        if (auto textWeight = getEnvFloat("YAMS_SEARCH_TEXT_WEIGHT")) {
            cfg.textWeight = *textWeight;
            envTextPinned = true;
            spdlog::info("SearchEngine textWeight overridden to {:.2f} via env (pinned)",
                         cfg.textWeight);
        }
        if (auto graphTextWeight = getEnvFloat("YAMS_SEARCH_GRAPH_TEXT_WEIGHT")) {
            cfg.graphTextWeight = *graphTextWeight;
            spdlog::info("SearchEngine graphTextWeight overridden to {:.2f} via env",
                         cfg.graphTextWeight);
        }
        if (auto vectorWeight = getEnvFloat("YAMS_SEARCH_VECTOR_WEIGHT")) {
            cfg.vectorWeight = *vectorWeight;
            envVectorPinned = true;
            spdlog::info("SearchEngine vectorWeight overridden to {:.2f} via env (pinned)",
                         cfg.vectorWeight);
        }
        if (auto similarityThreshold = getEnvFloat("YAMS_SEARCH_SIMILARITY_THRESHOLD")) {
            cfg.similarityThreshold = std::clamp(*similarityThreshold, 0.0f, 1.0f);
            envSimilarityThresholdPinned = true;
            spdlog::info("SearchEngine similarityThreshold overridden to {:.3f} via env (pinned)",
                         cfg.similarityThreshold);
        }
        if (auto kgWeight = getEnvFloat("YAMS_SEARCH_KG_WEIGHT")) {
            cfg.kgWeight = *kgWeight;
            envKgPinned = true;
            spdlog::info("SearchEngine kgWeight overridden to {:.2f} via env (pinned)",
                         cfg.kgWeight);
        }
        if (auto graphVectorWeight = getEnvFloat("YAMS_SEARCH_GRAPH_VECTOR_WEIGHT")) {
            cfg.graphVectorWeight = *graphVectorWeight;
            spdlog::info("SearchEngine graphVectorWeight overridden to {:.2f} via env",
                         cfg.graphVectorWeight);
        }
        if (auto rrfK = getEnvFloat("YAMS_SEARCH_RRF_K")) {
            cfg.rrfK = std::clamp(*rrfK, 1.0f, 200.0f);
            spdlog::info("SearchEngine rrfK overridden to {:.2f} via env", cfg.rrfK);
        }
        if (auto vectorOnlyThreshold = getEnvFloat("YAMS_SEARCH_VECTOR_ONLY_THRESHOLD")) {
            cfg.vectorOnlyThreshold = std::clamp(*vectorOnlyThreshold, 0.0f, 1.0f);
            spdlog::info("SearchEngine vectorOnlyThreshold overridden to {:.3f} via env",
                         cfg.vectorOnlyThreshold);
        }
        if (auto vectorOnlyPenalty = getEnvFloat("YAMS_SEARCH_VECTOR_ONLY_PENALTY")) {
            cfg.vectorOnlyPenalty = std::clamp(*vectorOnlyPenalty, 0.0f, 1.0f);
            spdlog::info("SearchEngine vectorOnlyPenalty overridden to {:.3f} via env",
                         cfg.vectorOnlyPenalty);
        }
        if (auto strongVectorOnlyRelief =
                getEnvBool("YAMS_SEARCH_ENABLE_STRONG_VECTOR_ONLY_RELIEF")) {
            cfg.enableStrongVectorOnlyRelief = *strongVectorOnlyRelief;
            spdlog::info("SearchEngine enableStrongVectorOnlyRelief overridden to {} via env",
                         cfg.enableStrongVectorOnlyRelief);
        }
        if (auto strongVectorOnlyMinScore =
                getEnvFloat("YAMS_SEARCH_STRONG_VECTOR_ONLY_MIN_SCORE")) {
            cfg.strongVectorOnlyMinScore = std::clamp(*strongVectorOnlyMinScore, 0.0f, 1.0f);
            spdlog::info("SearchEngine strongVectorOnlyMinScore overridden to {:.3f} via env",
                         cfg.strongVectorOnlyMinScore);
        }
        if (auto strongVectorOnlyTopRank = getEnvInt("YAMS_SEARCH_STRONG_VECTOR_ONLY_TOP_RANK")) {
            cfg.strongVectorOnlyTopRank =
                static_cast<size_t>(std::max(0, *strongVectorOnlyTopRank));
            spdlog::info("SearchEngine strongVectorOnlyTopRank overridden to {} via env",
                         cfg.strongVectorOnlyTopRank);
        }
        if (auto strongVectorOnlyPenalty = getEnvFloat("YAMS_SEARCH_STRONG_VECTOR_ONLY_PENALTY")) {
            cfg.strongVectorOnlyPenalty = std::clamp(*strongVectorOnlyPenalty, 0.0f, 1.0f);
            spdlog::info("SearchEngine strongVectorOnlyPenalty overridden to {:.3f} via env",
                         cfg.strongVectorOnlyPenalty);
        }
        if (auto nearMissReserve = getEnvInt("YAMS_SEARCH_VECTOR_ONLY_NEAR_MISS_RESERVE")) {
            cfg.vectorOnlyNearMissReserve = static_cast<size_t>(std::max(0, *nearMissReserve));
            spdlog::info("SearchEngine vectorOnlyNearMissReserve overridden to {} via env",
                         cfg.vectorOnlyNearMissReserve);
        }
        if (auto nearMissSlack = getEnvFloat("YAMS_SEARCH_VECTOR_ONLY_NEAR_MISS_SLACK")) {
            cfg.vectorOnlyNearMissSlack = std::clamp(*nearMissSlack, 0.0f, 1.0f);
            spdlog::info("SearchEngine vectorOnlyNearMissSlack overridden to {:.3f} via env",
                         cfg.vectorOnlyNearMissSlack);
        }
        if (auto nearMissPenalty = getEnvFloat("YAMS_SEARCH_VECTOR_ONLY_NEAR_MISS_PENALTY")) {
            cfg.vectorOnlyNearMissPenalty = std::clamp(*nearMissPenalty, 0.0f, 1.0f);
            spdlog::info("SearchEngine vectorOnlyNearMissPenalty overridden to {:.3f} via env",
                         cfg.vectorOnlyNearMissPenalty);
        }
        if (auto conceptBoostWeight = getEnvFloat("YAMS_SEARCH_CONCEPT_BOOST_WEIGHT")) {
            cfg.conceptBoostWeight = std::clamp(*conceptBoostWeight, 0.0f, 1.0f);
            spdlog::info("SearchEngine conceptBoostWeight overridden to {:.3f} via env",
                         cfg.conceptBoostWeight);
        }
        if (auto waitForConcepts = getEnvBool("YAMS_SEARCH_WAIT_FOR_CONCEPTS")) {
            cfg.waitForConceptExtraction = *waitForConcepts;
            spdlog::info("SearchEngine waitForConceptExtraction overridden to {} via env",
                         cfg.waitForConceptExtraction);
        }

        if (auto zoomLevel = getEnvString("YAMS_SEARCH_ZOOM_LEVEL")) {
            if (*zoomLevel == "AUTO") {
                cfg.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Auto;
            } else if (*zoomLevel == "MAP") {
                cfg.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Map;
            } else if (*zoomLevel == "NEIGHBORHOOD") {
                cfg.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Neighborhood;
            } else if (*zoomLevel == "STREET") {
                cfg.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Street;
            } else {
                spdlog::warn("Unknown YAMS_SEARCH_ZOOM_LEVEL value '{}', ignoring", *zoomLevel);
            }
            spdlog::info("SearchEngine zoomLevel overridden to {} via env",
                         SearchEngineConfig::navigationZoomLevelToString(cfg.zoomLevel));
        }
    }

    // Allow fusion strategy override for benchmarking
    if (allowEnvOverrides) {
        if (auto fusionEnv = getEnvString("YAMS_FUSION_STRATEGY")) {
            if (*fusionEnv == "WEIGHTED_SUM") {
                cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_SUM;
            } else if (*fusionEnv == "RECIPROCAL_RANK") {
                cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::RECIPROCAL_RANK;
            } else if (*fusionEnv == "WEIGHTED_RECIPROCAL") {
                cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_RECIPROCAL;
            } else if (*fusionEnv == "COMB_MNZ") {
                cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::COMB_MNZ;
            } else {
                spdlog::warn("Unknown YAMS_FUSION_STRATEGY value '{}', ignoring", *fusionEnv);
            }
            spdlog::info("SearchEngine fusionStrategy overridden to {} via env",
                         SearchEngineConfig::fusionStrategyToString(cfg.fusionStrategy));
        }
    }

    // Allow semantic rescue, rerank, and chunk aggregation overrides
    if (allowEnvOverrides) {
        if (auto slots = getEnvInt("YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS")) {
            cfg.semanticRescueSlots = static_cast<size_t>(std::max(0, *slots));
            spdlog::info("SearchEngine semanticRescueSlots overridden to {} via env",
                         cfg.semanticRescueSlots);
        }
        if (auto minScore = getEnvFloat("YAMS_SEARCH_SEMANTIC_RESCUE_MIN_SCORE")) {
            cfg.semanticRescueMinVectorScore = std::clamp(*minScore, 0.0f, 1.0f);
            spdlog::info("SearchEngine semanticRescueMinVectorScore overridden to {:.3f} via env",
                         cfg.semanticRescueMinVectorScore);
        }
        if (auto topK = getEnvInt("YAMS_SEARCH_RERANK_TOP_K")) {
            cfg.rerankTopK = static_cast<size_t>(std::max(1, *topK));
            spdlog::info("SearchEngine rerankTopK overridden to {} via env", cfg.rerankTopK);
        }
        if (auto aggEnv = getEnvString("YAMS_SEARCH_CHUNK_AGGREGATION")) {
            if (*aggEnv == "MAX" || *aggEnv == "max") {
                cfg.chunkAggregation = SearchEngineConfig::ChunkAggregation::MAX;
            } else if (*aggEnv == "SUM" || *aggEnv == "sum") {
                cfg.chunkAggregation = SearchEngineConfig::ChunkAggregation::SUM;
            } else if (*aggEnv == "TOP_K_AVG" || *aggEnv == "top_k_avg") {
                cfg.chunkAggregation = SearchEngineConfig::ChunkAggregation::TOP_K_AVG;
            } else if (*aggEnv == "WEIGHTED_TOP_K_AVG" || *aggEnv == "weighted_top_k_avg" ||
                       *aggEnv == "WEIGHTED" || *aggEnv == "weighted") {
                cfg.chunkAggregation = SearchEngineConfig::ChunkAggregation::WEIGHTED_TOP_K_AVG;
            } else {
                spdlog::warn("Unknown YAMS_SEARCH_CHUNK_AGGREGATION value '{}', ignoring", *aggEnv);
            }
            spdlog::info("SearchEngine chunkAggregation overridden via env");
        }
    }

    // Allow candidate limit overrides for recall benchmarking
    // YAMS_CANDIDATE_MULTIPLIER scales all maxResults values (e.g., 2.0 = 2x candidates)
    if (allowEnvOverrides) {
        if (auto multiplier = getEnvFloat("YAMS_CANDIDATE_MULTIPLIER")) {
            cfg.textMaxResults = static_cast<size_t>(cfg.textMaxResults * *multiplier);
            cfg.vectorMaxResults = static_cast<size_t>(cfg.vectorMaxResults * *multiplier);
            cfg.entityVectorMaxResults =
                static_cast<size_t>(cfg.entityVectorMaxResults * *multiplier);
            cfg.pathTreeMaxResults = static_cast<size_t>(cfg.pathTreeMaxResults * *multiplier);
            cfg.kgMaxResults = static_cast<size_t>(cfg.kgMaxResults * *multiplier);
            cfg.tagMaxResults = static_cast<size_t>(cfg.tagMaxResults * *multiplier);
            cfg.metadataMaxResults = static_cast<size_t>(cfg.metadataMaxResults * *multiplier);
            spdlog::info(
                "SearchEngine candidate limits scaled by {:.2f}x via env (text={}, vec={})",
                *multiplier, cfg.textMaxResults, cfg.vectorMaxResults);
        }
    }

    // Individual maxResults overrides
    if (allowEnvOverrides) {
        if (auto textMax = getEnvInt("YAMS_TEXT_MAX_RESULTS")) {
            cfg.textMaxResults = static_cast<size_t>(*textMax);
            spdlog::info("SearchEngine textMaxResults overridden to {} via env",
                         cfg.textMaxResults);
        }
        if (auto vectorMax = getEnvInt("YAMS_VECTOR_MAX_RESULTS")) {
            cfg.vectorMaxResults = static_cast<size_t>(*vectorMax);
            spdlog::info("SearchEngine vectorMaxResults overridden to {} via env",
                         cfg.vectorMaxResults);
        }
        if (auto kgMax = getEnvInt("YAMS_KG_MAX_RESULTS")) {
            cfg.kgMaxResults = static_cast<size_t>(*kgMax);
            spdlog::info("SearchEngine kgMaxResults overridden to {} via env", cfg.kgMaxResults);
        }

        if (auto intentAdaptive = getEnvBool("YAMS_SEARCH_ENABLE_INTENT_ADAPTIVE")) {
            cfg.enableIntentAdaptiveWeighting = *intentAdaptive;
            spdlog::info("SearchEngine enableIntentAdaptiveWeighting overridden to {} via env",
                         cfg.enableIntentAdaptiveWeighting);
        }

        if (auto fieldAware = getEnvBool("YAMS_SEARCH_FIELD_AWARE_WEIGHTING")) {
            cfg.enableFieldAwareWeightedRrf = *fieldAware;
            spdlog::info("SearchEngine enableFieldAwareWeightedRrf overridden to {} via env",
                         cfg.enableFieldAwareWeightedRrf);
        }

        if (auto lexicalExpansion = getEnvBool("YAMS_SEARCH_ENABLE_LEXICAL_EXPANSION")) {
            cfg.enableLexicalExpansion = *lexicalExpansion;
            spdlog::info("SearchEngine enableLexicalExpansion overridden to {} via env",
                         cfg.enableLexicalExpansion);
        }

        if (auto lexicalMinHits = getEnvInt("YAMS_SEARCH_LEXICAL_EXPANSION_MIN_HITS")) {
            cfg.lexicalExpansionMinHits = static_cast<size_t>(std::max(0, *lexicalMinHits));
            spdlog::info("SearchEngine lexicalExpansionMinHits overridden to {} via env",
                         cfg.lexicalExpansionMinHits);
        }

        if (auto lexicalPenalty = getEnvFloat("YAMS_SEARCH_LEXICAL_EXPANSION_PENALTY")) {
            cfg.lexicalExpansionScorePenalty = std::clamp(*lexicalPenalty, 0.1f, 1.0f);
            spdlog::info("SearchEngine lexicalExpansionScorePenalty overridden to {:.2f} via env",
                         cfg.lexicalExpansionScorePenalty);
        }

        if (auto dedupByPath = getEnvBool("YAMS_SEARCH_ENABLE_PATH_DEDUP")) {
            cfg.enablePathDedupInFusion = *dedupByPath;
            spdlog::info("SearchEngine enablePathDedupInFusion overridden to {} via env",
                         cfg.enablePathDedupInFusion);
        }

        if (auto lexicalFloorTopN = getEnvInt("YAMS_SEARCH_LEXICAL_FLOOR_TOPN")) {
            cfg.lexicalFloorTopN = static_cast<size_t>(std::max(0, *lexicalFloorTopN));
            spdlog::info("SearchEngine lexicalFloorTopN overridden to {} via env",
                         cfg.lexicalFloorTopN);
        }

        if (auto lexicalFloorBoost = getEnvFloat("YAMS_SEARCH_LEXICAL_FLOOR_BOOST")) {
            cfg.lexicalFloorBoost = std::clamp(*lexicalFloorBoost, 0.0f, 1.0f);
            spdlog::info("SearchEngine lexicalFloorBoost overridden to {:.3f} via env",
                         cfg.lexicalFloorBoost);
        }

        if (auto lexicalTieBreak = getEnvBool("YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK")) {
            cfg.enableLexicalTieBreak = *lexicalTieBreak;
            spdlog::info("SearchEngine enableLexicalTieBreak overridden to {} via env",
                         cfg.enableLexicalTieBreak);
        }

        if (auto lexicalTieBreakEps = getEnvFloat("YAMS_SEARCH_LEXICAL_TIEBREAK_EPS")) {
            cfg.lexicalTieBreakEpsilon = std::max(0.0f, *lexicalTieBreakEps);
            spdlog::info("SearchEngine lexicalTieBreakEpsilon overridden to {:.4f} via env",
                         cfg.lexicalTieBreakEpsilon);
        }

        if (auto semanticRescueSlots = getEnvInt("YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS")) {
            cfg.semanticRescueSlots = static_cast<size_t>(std::max(0, *semanticRescueSlots));
            spdlog::info("SearchEngine semanticRescueSlots overridden to {} via env",
                         cfg.semanticRescueSlots);
        }

        if (auto semanticRescueMinVector =
                getEnvFloat("YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE")) {
            cfg.semanticRescueMinVectorScore = std::max(0.0f, *semanticRescueMinVector);
            spdlog::info("SearchEngine semanticRescueMinVectorScore overridden to {:.4f} via env",
                         cfg.semanticRescueMinVectorScore);
        }

        if (auto rerankingEnabled = getEnvBool("YAMS_SEARCH_ENABLE_RERANKING")) {
            cfg.enableReranking = *rerankingEnabled;
            spdlog::info("SearchEngine enableReranking overridden to {} via env",
                         cfg.enableReranking);
        }

        if (auto rerankTopK = getEnvInt("YAMS_SEARCH_RERANK_TOPK")) {
            cfg.rerankTopK = static_cast<size_t>(std::max(0, *rerankTopK));
            spdlog::info("SearchEngine rerankTopK overridden to {} via env", cfg.rerankTopK);
        }

        if (auto rerankReplace = getEnvBool("YAMS_SEARCH_RERANK_REPLACE_SCORES")) {
            cfg.rerankReplaceScores = *rerankReplace;
            spdlog::info("SearchEngine rerankReplaceScores overridden to {} via env",
                         cfg.rerankReplaceScores);
        }

        if (auto graphRerankEnabled = getEnvBool("YAMS_SEARCH_ENABLE_GRAPH_RERANK")) {
            cfg.enableGraphRerank = *graphRerankEnabled;
            spdlog::info("SearchEngine enableGraphRerank overridden to {} via env",
                         cfg.enableGraphRerank);
        }

        if (auto graphTopN = getEnvInt("YAMS_SEARCH_GRAPH_RERANK_TOPN")) {
            cfg.graphRerankTopN = static_cast<size_t>(std::max(0, *graphTopN));
            spdlog::info("SearchEngine graphRerankTopN overridden to {} via env",
                         cfg.graphRerankTopN);
        }

        if (auto includeTiming = getEnvBool("YAMS_SEARCH_INCLUDE_COMPONENT_TIMING")) {
            cfg.includeComponentTiming = *includeTiming;
            spdlog::info("SearchEngine includeComponentTiming overridden to {} via env",
                         cfg.includeComponentTiming);
        }

        if (auto graphWeight = getEnvFloat("YAMS_SEARCH_GRAPH_RERANK_WEIGHT")) {
            cfg.graphRerankWeight = std::max(0.0f, *graphWeight);
            spdlog::info("SearchEngine graphRerankWeight overridden to {:.3f} via env",
                         cfg.graphRerankWeight);
        }

        if (auto graphMaxBoost = getEnvFloat("YAMS_SEARCH_GRAPH_RERANK_MAX_BOOST")) {
            cfg.graphRerankMaxBoost = std::max(0.0f, *graphMaxBoost);
            spdlog::info("SearchEngine graphRerankMaxBoost overridden to {:.3f} via env",
                         cfg.graphRerankMaxBoost);
        }

        if (auto graphMinSignal = getEnvFloat("YAMS_SEARCH_GRAPH_RERANK_MIN_SIGNAL")) {
            cfg.graphRerankMinSignal = std::max(0.0f, *graphMinSignal);
            spdlog::info("SearchEngine graphRerankMinSignal overridden to {:.3f} via env",
                         cfg.graphRerankMinSignal);
        }
        if (auto graphCommunityWeight = getEnvFloat("YAMS_SEARCH_GRAPH_COMMUNITY_WEIGHT")) {
            cfg.graphCommunityWeight = std::clamp(*graphCommunityWeight, 0.0f, 1.0f);
            spdlog::info("SearchEngine graphCommunityWeight overridden to {:.3f} via env",
                         cfg.graphCommunityWeight);
        }
        if (auto graphUseQueryConcepts = getEnvBool("YAMS_SEARCH_GRAPH_USE_QUERY_CONCEPTS")) {
            cfg.graphUseQueryConcepts = *graphUseQueryConcepts;
            spdlog::info("SearchEngine graphUseQueryConcepts overridden to {} via env",
                         cfg.graphUseQueryConcepts);
        }
        if (auto graphFallbackTopSignal = getEnvBool("YAMS_SEARCH_GRAPH_FALLBACK_TOP_SIGNAL")) {
            cfg.graphFallbackToTopSignal = *graphFallbackTopSignal;
            spdlog::info("SearchEngine graphFallbackToTopSignal overridden to {} via env",
                         cfg.graphFallbackToTopSignal);
        }

        if (auto graphNeighbors = getEnvInt("YAMS_SEARCH_GRAPH_MAX_NEIGHBORS")) {
            cfg.graphMaxNeighbors = static_cast<size_t>(std::max(1, *graphNeighbors));
            spdlog::info("SearchEngine graphMaxNeighbors overridden to {} via env",
                         cfg.graphMaxNeighbors);
        }

        if (auto graphHops = getEnvInt("YAMS_SEARCH_GRAPH_MAX_HOPS")) {
            cfg.graphMaxHops = static_cast<size_t>(std::clamp(*graphHops, 1, 5));
            spdlog::info("SearchEngine graphMaxHops overridden to {} via env", cfg.graphMaxHops);
        }

        if (auto graphBudgetMs = getEnvInt("YAMS_SEARCH_GRAPH_BUDGET_MS")) {
            cfg.graphScoringBudgetMs = std::max(0, *graphBudgetMs);
            spdlog::info("SearchEngine graphScoringBudgetMs overridden to {} via env",
                         cfg.graphScoringBudgetMs);
        }

        if (auto graphPaths = getEnvBool("YAMS_SEARCH_GRAPH_ENABLE_PATHS")) {
            cfg.graphEnablePathEnumeration = *graphPaths;
            spdlog::info("SearchEngine graphEnablePathEnumeration overridden to {} via env",
                         cfg.graphEnablePathEnumeration);
        }

        if (auto graphMaxPaths = getEnvInt("YAMS_SEARCH_GRAPH_MAX_PATHS")) {
            cfg.graphMaxPaths = static_cast<size_t>(std::max(1, *graphMaxPaths));
            spdlog::info("SearchEngine graphMaxPaths overridden to {} via env", cfg.graphMaxPaths);
        }

        if (auto graphHopDecay = getEnvFloat("YAMS_SEARCH_GRAPH_HOP_DECAY")) {
            cfg.graphHopDecay = std::clamp(*graphHopDecay, 0.0f, 1.0f);
            spdlog::info("SearchEngine graphHopDecay overridden to {:.3f} via env",
                         cfg.graphHopDecay);
        }

        if (auto tieredExecution = getEnvBool("YAMS_SEARCH_ENABLE_TIERED_EXECUTION")) {
            cfg.enableTieredExecution = *tieredExecution;
            spdlog::info("SearchEngine enableTieredExecution overridden to {} via env",
                         cfg.enableTieredExecution);
        }

        if (auto tieredNarrow = getEnvBool("YAMS_SEARCH_TIERED_NARROW_VECTOR_SEARCH")) {
            cfg.tieredNarrowVectorSearch = *tieredNarrow;
            spdlog::info("SearchEngine tieredNarrowVectorSearch overridden to {} via env",
                         cfg.tieredNarrowVectorSearch);
        }

        if (auto tieredMinCandidates = getEnvInt("YAMS_SEARCH_TIERED_MIN_CANDIDATES")) {
            cfg.tieredMinCandidates = static_cast<size_t>(std::max(0, *tieredMinCandidates));
            spdlog::info("SearchEngine tieredMinCandidates overridden to {} via env",
                         cfg.tieredMinCandidates);
        }

        if (auto weakFanout = getEnvBool("YAMS_SEARCH_ENABLE_WEAK_QUERY_FANOUT_BOOST")) {
            cfg.enableWeakQueryFanoutBoost = *weakFanout;
            spdlog::info("SearchEngine enableWeakQueryFanoutBoost overridden to {} via env",
                         cfg.enableWeakQueryFanoutBoost);
        }
        if (auto weakVectorFanout =
                getEnvFloat("YAMS_SEARCH_WEAK_QUERY_VECTOR_FANOUT_MULTIPLIER")) {
            cfg.weakQueryVectorFanoutMultiplier = std::max(1.0f, *weakVectorFanout);
            spdlog::info(
                "SearchEngine weakQueryVectorFanoutMultiplier overridden to {:.2f} via env",
                cfg.weakQueryVectorFanoutMultiplier);
        }
        if (auto weakEntityFanout =
                getEnvFloat("YAMS_SEARCH_WEAK_QUERY_ENTITY_VECTOR_FANOUT_MULTIPLIER")) {
            cfg.weakQueryEntityVectorFanoutMultiplier = std::max(1.0f, *weakEntityFanout);
            spdlog::info(
                "SearchEngine weakQueryEntityVectorFanoutMultiplier overridden to {:.2f} via env",
                cfg.weakQueryEntityVectorFanoutMultiplier);
        }
        if (auto topologyWeakRouting = getEnvBool("YAMS_SEARCH_ENABLE_TOPOLOGY_WEAK_ROUTING")) {
            cfg.enableTopologyWeakQueryRouting = *topologyWeakRouting;
            spdlog::info("SearchEngine topology weak-query routing {} via env",
                         cfg.enableTopologyWeakQueryRouting ? "enabled" : "disabled");
        }
        if (auto topologyMaxClusters = getEnvInt("YAMS_SEARCH_TOPOLOGY_MAX_CLUSTERS")) {
            cfg.topologyWeakQueryMaxClusters =
                static_cast<size_t>(std::max(0, *topologyMaxClusters));
            spdlog::info("SearchEngine topologyWeakQueryMaxClusters overridden to {} via env",
                         cfg.topologyWeakQueryMaxClusters);
        }
        if (auto topologyMaxDocs = getEnvInt("YAMS_SEARCH_TOPOLOGY_MAX_DOCS")) {
            cfg.topologyWeakQueryMaxDocs = static_cast<size_t>(std::max(0, *topologyMaxDocs));
            spdlog::info("SearchEngine topologyWeakQueryMaxDocs overridden to {} via env",
                         cfg.topologyWeakQueryMaxDocs);
        }
        if (auto medoidBoost = getEnvFloat("YAMS_SEARCH_TOPOLOGY_MEDOID_BOOST")) {
            cfg.topologyMedoidBoost = std::max(0.0f, *medoidBoost);
            spdlog::info("SearchEngine topologyMedoidBoost overridden to {:.3f} via env",
                         cfg.topologyMedoidBoost);
        }
        if (auto bridgeBoost = getEnvFloat("YAMS_SEARCH_TOPOLOGY_BRIDGE_BOOST")) {
            cfg.topologyBridgeBoost = std::max(0.0f, *bridgeBoost);
            spdlog::info("SearchEngine topologyBridgeBoost overridden to {:.3f} via env",
                         cfg.topologyBridgeBoost);
        }
        if (auto anchorEnabled = getEnvBool("YAMS_SEARCH_ENABLE_ANCHOR_FUSION")) {
            cfg.enableAnchorFusion = *anchorEnabled;
            spdlog::info("SearchEngine enableAnchorFusion {} via env",
                         cfg.enableAnchorFusion ? "enabled" : "disabled");
        }
        if (auto anchorWeight = getEnvFloat("YAMS_SEARCH_ANCHOR_WEIGHT")) {
            cfg.anchorWeight = std::max(0.0f, *anchorWeight);
            spdlog::info("SearchEngine anchorWeight overridden to {:.3f} via env",
                         cfg.anchorWeight);
        }
        if (auto anchorPhssGate = getEnvBool("YAMS_SEARCH_ANCHOR_PHSS_GATE")) {
            cfg.anchorPhssGateEnabled = *anchorPhssGate;
            spdlog::info("SearchEngine anchorPhssGateEnabled {} via env",
                         cfg.anchorPhssGateEnabled ? "enabled" : "disabled");
        }
        if (auto anchorPhssMin = getEnvFloat("YAMS_SEARCH_ANCHOR_PHSS_MIN_CONFIDENCE")) {
            cfg.anchorPhssMinConfidence = std::clamp(*anchorPhssMin, 0.0f, 1.0f);
            spdlog::info("SearchEngine anchorPhssMinConfidence overridden to {:.3f} via env",
                         cfg.anchorPhssMinConfidence);
        }
        if (auto anchorTopK = getEnvInt("YAMS_SEARCH_ANCHOR_TOP_K")) {
            cfg.anchorTopK = static_cast<size_t>(std::max(1, *anchorTopK));
            spdlog::info("SearchEngine anchorTopK overridden to {} via env", cfg.anchorTopK);
        }
        if (auto routedBase = getEnvFloat("YAMS_SEARCH_TOPOLOGY_ROUTED_BASE_MULTIPLIER")) {
            cfg.topologyRoutedBaseMultiplier = std::max(0.0f, *routedBase);
            spdlog::info("SearchEngine topologyRoutedBaseMultiplier overridden to {:.3f} via env",
                         cfg.topologyRoutedBaseMultiplier);
        }
        if (auto bypassWarming = getEnvBool("YAMS_SEARCH_BYPASS_CORPUS_WARMING_GATE")) {
            cfg.bypassCorpusWarmingGate = *bypassWarming;
            spdlog::info("SearchEngine bypassCorpusWarmingGate overridden to {} via env",
                         cfg.bypassCorpusWarmingGate);
        }
        if (auto routingVariant = getEnvString("YAMS_SEARCH_TOPOLOGY_ROUTING_VARIANT")) {
            using V = SearchEngineConfig::TopologyRoutingVariant;
            const auto& raw = *routingVariant;
            if (raw == "vector_seed")
                cfg.topologyRoutingVariant = V::VectorSeed;
            else if (raw == "kg_walk")
                cfg.topologyRoutingVariant = V::KgWalk;
            else if (raw == "score_replace")
                cfg.topologyRoutingVariant = V::ScoreReplace;
            else if (raw == "medoid_promote")
                cfg.topologyRoutingVariant = V::MedoidPromote;
            else
                cfg.topologyRoutingVariant = V::Baseline;
            spdlog::info("SearchEngine topologyRoutingVariant set to '{}' via env", raw);
        }
        if (auto evidenceRescueSlots = getEnvInt("YAMS_SEARCH_FUSION_EVIDENCE_RESCUE_SLOTS")) {
            cfg.fusionEvidenceRescueSlots = static_cast<size_t>(std::max(0, *evidenceRescueSlots));
            spdlog::info("SearchEngine fusionEvidenceRescueSlots overridden to {} via env",
                         cfg.fusionEvidenceRescueSlots);
        }
        if (auto evidenceRescueMinScore =
                getEnvFloat("YAMS_SEARCH_FUSION_EVIDENCE_RESCUE_MIN_SCORE")) {
            cfg.fusionEvidenceRescueMinScore = std::max(0.0f, *evidenceRescueMinScore);
            spdlog::info("SearchEngine fusionEvidenceRescueMinScore overridden to {:.3f} via env",
                         cfg.fusionEvidenceRescueMinScore);
        }
        if (auto graphQueryExpansion = getEnvBool("YAMS_SEARCH_ENABLE_GRAPH_QUERY_EXPANSION")) {
            cfg.enableGraphQueryExpansion = *graphQueryExpansion;
            spdlog::info("SearchEngine enableGraphQueryExpansion overridden to {} via env",
                         cfg.enableGraphQueryExpansion);
        }
        if (auto graphExpansionMinHits = getEnvInt("YAMS_SEARCH_GRAPH_EXPANSION_MIN_HITS")) {
            cfg.graphExpansionMinHits = static_cast<size_t>(std::max(0, *graphExpansionMinHits));
            spdlog::info("SearchEngine graphExpansionMinHits overridden to {} via env",
                         cfg.graphExpansionMinHits);
        }
        if (auto graphExpansionMaxTerms = getEnvInt("YAMS_SEARCH_GRAPH_EXPANSION_MAX_TERMS")) {
            cfg.graphExpansionMaxTerms = static_cast<size_t>(std::max(0, *graphExpansionMaxTerms));
            spdlog::info("SearchEngine graphExpansionMaxTerms overridden to {} via env",
                         cfg.graphExpansionMaxTerms);
        }
        if (auto graphExpansionMaxSeeds = getEnvInt("YAMS_SEARCH_GRAPH_EXPANSION_MAX_SEEDS")) {
            cfg.graphExpansionMaxSeeds = static_cast<size_t>(std::max(0, *graphExpansionMaxSeeds));
            spdlog::info("SearchEngine graphExpansionMaxSeeds overridden to {} via env",
                         cfg.graphExpansionMaxSeeds);
        }
        if (auto graphExpansionQueryNeighborK =
                getEnvInt("YAMS_SEARCH_GRAPH_EXPANSION_QUERY_NEIGHBOR_K")) {
            cfg.graphExpansionQueryNeighborK =
                static_cast<size_t>(std::max(0, *graphExpansionQueryNeighborK));
            spdlog::info("SearchEngine graphExpansionQueryNeighborK overridden to {} via env",
                         cfg.graphExpansionQueryNeighborK);
        }
        if (auto graphExpansionQueryNeighborMinScore =
                getEnvFloat("YAMS_SEARCH_GRAPH_EXPANSION_QUERY_NEIGHBOR_MIN_SCORE")) {
            cfg.graphExpansionQueryNeighborMinScore =
                std::clamp(*graphExpansionQueryNeighborMinScore, 0.0f, 1.0f);
            spdlog::info(
                "SearchEngine graphExpansionQueryNeighborMinScore overridden to {:.3f} via env",
                cfg.graphExpansionQueryNeighborMinScore);
        }
        if (auto graphVectorRequireCorroboration =
                getEnvBool("YAMS_SEARCH_GRAPH_VECTOR_REQUIRE_CORROBORATION")) {
            cfg.graphVectorRequireCorroboration = *graphVectorRequireCorroboration;
            spdlog::info("SearchEngine graphVectorRequireCorroboration overridden to {} via env",
                         cfg.graphVectorRequireCorroboration);
        }
        if (auto graphVectorRequireTextAnchoring =
                getEnvBool("YAMS_SEARCH_GRAPH_VECTOR_REQUIRE_TEXT_ANCHORING")) {
            cfg.graphVectorRequireTextAnchoring = *graphVectorRequireTextAnchoring;
            spdlog::info("SearchEngine graphVectorRequireTextAnchoring overridden to {} via env",
                         cfg.graphVectorRequireTextAnchoring);
        }
        if (auto graphVectorRequireBaselineTextAnchoring =
                getEnvBool("YAMS_SEARCH_GRAPH_VECTOR_REQUIRE_BASELINE_TEXT_ANCHORING")) {
            cfg.graphVectorRequireBaselineTextAnchoring = *graphVectorRequireBaselineTextAnchoring;
            spdlog::info(
                "SearchEngine graphVectorRequireBaselineTextAnchoring overridden to {} via env",
                cfg.graphVectorRequireBaselineTextAnchoring);
        }
        if (auto graphFusionWindowGuard =
                getEnvBool("YAMS_SEARCH_ENABLE_GRAPH_FUSION_WINDOW_GUARD")) {
            cfg.enableGraphFusionWindowGuard = *graphFusionWindowGuard;
            spdlog::info("SearchEngine enableGraphFusionWindowGuard overridden to {} via env",
                         cfg.enableGraphFusionWindowGuard);
        }
        if (auto graphFusionGuardDepthMultiplier =
                getEnvInt("YAMS_SEARCH_GRAPH_FUSION_GUARD_DEPTH_MULTIPLIER")) {
            cfg.graphFusionGuardDepthMultiplier =
                static_cast<size_t>(std::max(1, *graphFusionGuardDepthMultiplier));
            spdlog::info("SearchEngine graphFusionGuardDepthMultiplier overridden to {} via env",
                         cfg.graphFusionGuardDepthMultiplier);
        }
        if (auto graphMaxAddedInFusionWindow =
                getEnvInt("YAMS_SEARCH_GRAPH_MAX_ADDED_IN_FUSION_WINDOW")) {
            cfg.graphMaxAddedInFusionWindow =
                static_cast<size_t>(std::max(0, *graphMaxAddedInFusionWindow));
            spdlog::info("SearchEngine graphMaxAddedInFusionWindow overridden to {} via env",
                         cfg.graphMaxAddedInFusionWindow);
        }
        if (auto graphTextMinAdmissionScore =
                getEnvFloat("YAMS_SEARCH_GRAPH_TEXT_MIN_ADMISSION_SCORE")) {
            cfg.graphTextMinAdmissionScore = std::max(0.0f, *graphTextMinAdmissionScore);
            spdlog::info("SearchEngine graphTextMinAdmissionScore overridden to {:.4f} via env",
                         cfg.graphTextMinAdmissionScore);
        }
        if (auto graphExpansionFtsPenalty =
                getEnvFloat("YAMS_SEARCH_GRAPH_EXPANSION_FTS_PENALTY")) {
            cfg.graphExpansionFtsPenalty = std::clamp(*graphExpansionFtsPenalty, 0.1f, 1.0f);
            spdlog::info("SearchEngine graphExpansionFtsPenalty overridden to {:.3f} via env",
                         cfg.graphExpansionFtsPenalty);
        }
        if (auto graphExpansionVectorPenalty =
                getEnvFloat("YAMS_SEARCH_GRAPH_EXPANSION_VECTOR_PENALTY")) {
            cfg.graphExpansionVectorPenalty = std::clamp(*graphExpansionVectorPenalty, 0.1f, 1.0f);
            spdlog::info("SearchEngine graphExpansionVectorPenalty overridden to {:.3f} via env",
                         cfg.graphExpansionVectorPenalty);
        }

        if (auto weakMinTextHits = getEnvInt("YAMS_SEARCH_WEAK_QUERY_MIN_TEXT_HITS")) {
            cfg.weakQueryMinTextHits = static_cast<size_t>(std::max(0, *weakMinTextHits));
            spdlog::info("SearchEngine weakQueryMinTextHits overridden to {} via env",
                         cfg.weakQueryMinTextHits);
        }

        if (auto weakMinTopText = getEnvFloat("YAMS_SEARCH_WEAK_QUERY_MIN_TOP_TEXT_SCORE")) {
            cfg.weakQueryMinTopTextScore = std::clamp(*weakMinTopText, 0.0f, 1.0f);
            spdlog::info("SearchEngine weakQueryMinTopTextScore overridden to {:.3f} via env",
                         cfg.weakQueryMinTopTextScore);
        }

        // Multi-vector sub-phrase search overrides
        if (auto multiVec = getEnvBool("YAMS_SEARCH_MULTI_VECTOR_QUERY")) {
            cfg.enableMultiVectorQuery = *multiVec;
            spdlog::info("SearchEngine enableMultiVectorQuery overridden to {} via env",
                         cfg.enableMultiVectorQuery);
        }
        if (auto multiVecPhrases = getEnvInt("YAMS_SEARCH_MULTI_VECTOR_MAX_PHRASES")) {
            cfg.multiVectorMaxPhrases = static_cast<size_t>(std::clamp(*multiVecPhrases, 1, 8));
            spdlog::info("SearchEngine multiVectorMaxPhrases overridden to {} via env",
                         cfg.multiVectorMaxPhrases);
        }
        if (auto multiVecDecay = getEnvFloat("YAMS_SEARCH_MULTI_VECTOR_SCORE_DECAY")) {
            cfg.multiVectorScoreDecay = std::clamp(*multiVecDecay, 0.1f, 1.0f);
            spdlog::info("SearchEngine multiVectorScoreDecay overridden to {:.3f} via env",
                         cfg.multiVectorScoreDecay);
        }

        // Sub-phrase FTS expansion overrides
        if (auto subPhrase = getEnvBool("YAMS_SEARCH_SUB_PHRASE_EXPANSION")) {
            cfg.enableSubPhraseExpansion = *subPhrase;
            spdlog::info("SearchEngine enableSubPhraseExpansion overridden to {} via env",
                         cfg.enableSubPhraseExpansion);
        }
        if (auto subPhraseMinHits = getEnvInt("YAMS_SEARCH_SUB_PHRASE_MIN_HITS")) {
            cfg.subPhraseExpansionMinHits = static_cast<size_t>(std::max(0, *subPhraseMinHits));
            spdlog::info("SearchEngine subPhraseExpansionMinHits overridden to {} via env",
                         cfg.subPhraseExpansionMinHits);
        }
        if (auto subPhrasePenalty = getEnvFloat("YAMS_SEARCH_SUB_PHRASE_PENALTY")) {
            cfg.subPhraseExpansionPenalty = std::clamp(*subPhrasePenalty, 0.1f, 1.0f);
            spdlog::info("SearchEngine subPhraseExpansionPenalty overridden to {:.3f} via env",
                         cfg.subPhraseExpansionPenalty);
        }
    }

    if (runtimeTuner) {
        runtimeTuner->seedRuntimeConfig(cfg);
        if (envTextPinned || envVectorPinned || envKgPinned || envSimilarityThresholdPinned) {
            runtimeTuner->pinEnvOverrides(envTextPinned, envVectorPinned, envKgPinned,
                                          envSimilarityThresholdPinned);
        }
        if (!options.tunerStatePath.empty()) {
            auto loaded = runtimeTuner->loadAdaptiveState(options.tunerStatePath);
            if (!loaded) {
                spdlog::warn("SearchTuner: failed to load state from {}: {}",
                             options.tunerStatePath.string(), loaded.error().message);
            } else {
                spdlog::info("SearchTuner: adaptive state loaded from {}",
                             options.tunerStatePath.string());
            }
            runtimeTuner->setAdaptivePersistPath(options.tunerStatePath);
        }
    }

    // Create the SearchEngine using the factory function
    // Factory returns unique_ptr, convert to shared_ptr for builder interface
    auto engine =
        createSearchEngine(metadataRepo_, vectorDatabase_, embeddingGenerator_, kgStore_, cfg);
    if (!engine) {
        return Error{ErrorCode::InvalidState, "Failed to create SearchEngine"};
    }
    if (options.simeonLexicalConfig) {
        engine->setSimeonLexicalBackend(
            std::make_unique<SimeonLexicalBackend>(*options.simeonLexicalConfig));
    }
    if (runtimeTuner) {
        engine->setSearchTuner(runtimeTuner);
    }
    return std::shared_ptr<SearchEngine>(std::move(engine));
}

} // namespace yams::search
