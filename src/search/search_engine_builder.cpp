#include <yams/search/search_engine_builder.h>

#include "search_config_environment_internal.h"

#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine.h>
#include <yams/search/search_tuner.h>
#include <yams/topology/topology_artifacts.h>
#include <yams/vector/vector_database.h>

#include <spdlog/spdlog.h>

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

    const auto environment = LegacySearchConfigEnvironment::fromProcess();
    const auto envOverride = environment.tuningStateOverride();

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
            "SearchEngine using override state={} (zoom={}, k={}, text={:.2f}, "
            "simeon_text={:.2f}, vector={:.2f}, "
            "vector_gate={:.2f}/{:.2f}, lexical_floor={}@{:.3f}, "
            "lexical_tiebreak={}, semantic_rescue={}@{:.4f}, "
            "concept_backend={})",
            tuningStateToString(overrideState),
            SearchEngineConfig::navigationZoomLevelToString(cfg.zoomLevel), params.rrfK,
            cfg.textWeight, cfg.simeonTextWeight, cfg.vectorWeight, cfg.vectorOnlyThreshold,
            cfg.vectorOnlyPenalty, cfg.lexicalFloorTopN, cfg.lexicalFloorBoost,
            cfg.enableLexicalTieBreak, cfg.semanticRescueSlots, cfg.semanticRescueMinVectorScore,
            SearchEngineConfig::conceptExtractionBackendToString(cfg.conceptExtractionBackend));
    } else if (options.autoTune && metadataRepo_) {
        // Always materialize a SearchTuner from corpus stats. Skipping when
        // docs>0 left product builds on ungated MIXED_PRECISION defaults
        // (path/entity/tag still weighted despite zero stage contribution).
        auto statsResult = metadataRepo_->getCorpusStats();
        if (statsResult.has_value()) {
            runtimeTuner = std::make_shared<SearchTuner>(statsResult.value());
            // Prefer tuner config (dead-source gates already applied) over the
            // raw BuildOptions defaults sitting in options.config.
            cfg = runtimeTuner->getConfig();

            // Preserve user-specified options that shouldn't be overridden by tuner
            cfg.maxResults = options.config.maxResults;
            cfg.enableParallelExecution = options.config.enableParallelExecution;
            cfg.includeDebugInfo = options.config.includeDebugInfo;
            {
                const auto& tp = runtimeTuner->getParams();
                spdlog::info(
                    "SearchEngine auto-tuned to state={} overlay={} reconciled_at={} "
                    "(zoom={}, k={}, "
                    "text={:.2f}[{}], simeon_text={:.2f}[{}], vector={:.2f}[{}], "
                    "kg={:.2f}[{}], "
                    "semantic_rescue={}[{}]@{:.4f})",
                    tuningStateToString(runtimeTuner->currentState()),
                    statsResult.value().usedOnlineOverlay,
                    statsResult.value().reconciledComputedAtMs,
                    SearchEngineConfig::navigationZoomLevelToString(cfg.zoomLevel), tp.rrfK,
                    tp.weights.text.value, tuningLayerToString(tp.weights.text.source),
                    tp.weights.simeonText.value, tuningLayerToString(tp.weights.simeonText.source),
                    tp.weights.vector.value, tuningLayerToString(tp.weights.vector.source),
                    tp.weights.kg.value, tuningLayerToString(tp.weights.kg.source),
                    tp.semanticRescueSlots.value,
                    tuningLayerToString(tp.semanticRescueSlots.source),
                    cfg.semanticRescueMinVectorScore);
            }
        } else {
            spdlog::warn("SearchTuner: failed to get corpus stats ({}), using default config",
                         statsResult.error().message);
        }
    }

    // Tuning owns relevance weights and result budgets. The topology policy is
    // explicitly selected through typed config and must survive replacement of
    // cfg by SearchTuner::getConfig().
    cfg.applyExecutionPolicyFrom(options.config);

    const auto environmentPins = environment.applyTo(cfg);

    if (runtimeTuner) {
        runtimeTuner->seedRuntimeConfig(cfg);
        if (environmentPins.text || environmentPins.simeonText || environmentPins.vector ||
            environmentPins.kg || environmentPins.similarityThreshold) {
            runtimeTuner->pinEnvOverrides(environmentPins.text, environmentPins.simeonText,
                                          environmentPins.vector, environmentPins.kg,
                                          environmentPins.similarityThreshold);
        }
        // seedRuntimeConfig re-applies dead-source gates; push the gated view
        // back into cfg so the engine and tuner share one weight package.
        {
            const auto maxResults = cfg.maxResults;
            const auto enableParallelExecution = cfg.enableParallelExecution;
            const auto includeDebugInfo = cfg.includeDebugInfo;
            cfg = runtimeTuner->getConfig();
            cfg.maxResults = maxResults;
            cfg.enableParallelExecution = enableParallelExecution;
            cfg.includeDebugInfo = includeDebugInfo;
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
