#include <yams/search/search_engine_builder.h>

#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine.h>
#include <yams/search/search_tuner.h>
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
            "SearchEngine using override state={} (k={}, text={:.2f}, vector={:.2f}, fusion={})",
            tuningStateToString(overrideState), params.rrfK, cfg.textWeight, cfg.vectorWeight,
            SearchEngineConfig::fusionStrategyToString(cfg.fusionStrategy));
    } else if (options.autoTune && metadataRepo_) {
        // Get corpus statistics from metadata repository
        auto statsResult = metadataRepo_->getCorpusStats();
        if (statsResult.has_value()) {
            // Create tuner and get optimized config
            SearchTuner tuner(statsResult.value());
            cfg = tuner.getConfig();

            // Preserve user-specified options that shouldn't be overridden by tuner
            cfg.maxResults = options.config.maxResults;
            cfg.enableParallelExecution = options.config.enableParallelExecution;
            cfg.includeDebugInfo = options.config.includeDebugInfo;

            spdlog::info(
                "SearchEngine auto-tuned to state={} (k={}, text={:.2f}, vector={:.2f}, fusion={})",
                tuningStateToString(tuner.currentState()), tuner.getRrfK(), cfg.textWeight,
                cfg.vectorWeight, SearchEngineConfig::fusionStrategyToString(cfg.fusionStrategy));
        } else {
            spdlog::warn("SearchTuner: failed to get corpus stats ({}), using default config",
                         statsResult.error().message);
        }
    }

    // Allow environment variable overrides for individual weights (for benchmarking)
    // These take precedence over tuning state weights
    if (allowEnvOverrides) {
        if (auto textWeight = getEnvFloat("YAMS_SEARCH_TEXT_WEIGHT")) {
            cfg.textWeight = *textWeight;
            spdlog::info("SearchEngine textWeight overridden to {:.2f} via env", cfg.textWeight);
        }
        if (auto vectorWeight = getEnvFloat("YAMS_SEARCH_VECTOR_WEIGHT")) {
            cfg.vectorWeight = *vectorWeight;
            spdlog::info("SearchEngine vectorWeight overridden to {:.2f} via env",
                         cfg.vectorWeight);
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

        if (auto rerankingEnabled = getEnvBool("YAMS_SEARCH_ENABLE_RERANKING")) {
            cfg.enableReranking = *rerankingEnabled;
            spdlog::info("SearchEngine enableReranking overridden to {} via env",
                         cfg.enableReranking);
        }

        if (auto rerankTopK = getEnvInt("YAMS_SEARCH_RERANK_TOPK")) {
            cfg.rerankTopK = static_cast<size_t>(std::max(0, *rerankTopK));
            spdlog::info("SearchEngine rerankTopK overridden to {} via env", cfg.rerankTopK);
        }
    }

    // Create the SearchEngine using the factory function
    // Factory returns unique_ptr, convert to shared_ptr for builder interface
    auto engine =
        createSearchEngine(metadataRepo_, vectorDatabase_, embeddingGenerator_, kgStore_, cfg);
    if (!engine) {
        return Error{ErrorCode::InvalidState, "Failed to create SearchEngine"};
    }
    return std::shared_ptr<SearchEngine>(std::move(engine));
}

} // namespace yams::search
