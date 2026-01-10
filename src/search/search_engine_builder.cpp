#include <yams/search/search_engine_builder.h>

#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine.h>
#include <yams/search/search_tuner.h>
#include <yams/vector/vector_database.h>

#include <spdlog/spdlog.h>

#include <cstdlib>

namespace yams::search {

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

    // Check for environment variable override first (useful for benchmarks)
    // This allows setting YAMS_TUNING_OVERRIDE=SCIENTIFIC without code changes
    std::optional<TuningState> envOverride;
    if (const char* envVal = std::getenv("YAMS_TUNING_OVERRIDE")) {
        std::string val(envVal);
        if (val == "SCIENTIFIC") {
            envOverride = TuningState::SCIENTIFIC;
        } else if (val == "SMALL_CODE") {
            envOverride = TuningState::SMALL_CODE;
        } else if (val == "LARGE_CODE") {
            envOverride = TuningState::LARGE_CODE;
        } else if (val == "SMALL_PROSE") {
            envOverride = TuningState::SMALL_PROSE;
        } else if (val == "LARGE_PROSE") {
            envOverride = TuningState::LARGE_PROSE;
        } else if (val == "MIXED") {
            envOverride = TuningState::MIXED;
        } else if (val == "MINIMAL") {
            envOverride = TuningState::MINIMAL;
        } else {
            spdlog::warn("Unknown YAMS_TUNING_OVERRIDE value '{}', ignoring", val);
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
    if (const char* textWeightEnv = std::getenv("YAMS_SEARCH_TEXT_WEIGHT")) {
        try {
            cfg.textWeight = std::stof(textWeightEnv);
            spdlog::info("SearchEngine textWeight overridden to {:.2f} via env", cfg.textWeight);
        } catch (...) {
            spdlog::warn("Invalid YAMS_SEARCH_TEXT_WEIGHT value '{}', ignoring", textWeightEnv);
        }
    }
    if (const char* vectorWeightEnv = std::getenv("YAMS_SEARCH_VECTOR_WEIGHT")) {
        try {
            cfg.vectorWeight = std::stof(vectorWeightEnv);
            spdlog::info("SearchEngine vectorWeight overridden to {:.2f} via env",
                         cfg.vectorWeight);
        } catch (...) {
            spdlog::warn("Invalid YAMS_SEARCH_VECTOR_WEIGHT value '{}', ignoring", vectorWeightEnv);
        }
    }

    // Allow fusion strategy override for benchmarking
    if (const char* fusionEnv = std::getenv("YAMS_FUSION_STRATEGY")) {
        std::string val(fusionEnv);
        if (val == "WEIGHTED_SUM") {
            cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_SUM;
        } else if (val == "RECIPROCAL_RANK") {
            cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::RECIPROCAL_RANK;
        } else if (val == "BORDA_COUNT") {
            cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::BORDA_COUNT;
        } else if (val == "WEIGHTED_RECIPROCAL") {
            cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_RECIPROCAL;
        } else if (val == "COMB_MNZ") {
            cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::COMB_MNZ;
        } else if (val == "TEXT_ANCHOR") {
            cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::TEXT_ANCHOR;
        } else {
            spdlog::warn("Unknown YAMS_FUSION_STRATEGY value '{}', ignoring", val);
        }
        spdlog::info("SearchEngine fusionStrategy overridden to {} via env",
                     SearchEngineConfig::fusionStrategyToString(cfg.fusionStrategy));
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
