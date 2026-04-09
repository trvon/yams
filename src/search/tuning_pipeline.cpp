// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

#include <yams/search/tuning_pipeline.h>

#include <algorithm>

namespace yams::search {

// ---------------------------------------------------------------------------
// Layer 4: Zoom
// ---------------------------------------------------------------------------

void applyZoomLayer(SearchEngineConfig::NavigationZoomLevel zoom,
                    TunedParams& params) { // NOLINT(readability-function-cognitive-complexity)
    switch (zoom) {
        case SearchEngineConfig::NavigationZoomLevel::Auto:
            return;

        case SearchEngineConfig::NavigationZoomLevel::Map:
            params.weights.kg.scaleBy(1.25f, TuningLayer::Zoom);
            params.weights.pathTree.scaleBy(1.10f, TuningLayer::Zoom);
            params.weights.vector.scaleBy(0.90f, TuningLayer::Zoom);
            params.weights.entityVector.scaleBy(0.85f, TuningLayer::Zoom);
            params.graphRerankTopN = std::max(params.graphRerankTopN, size_t{32});
            params.graphScoringBudgetMs = std::max(params.graphScoringBudgetMs, 12);
            params.rerankTopK = std::min(params.rerankTopK, size_t{3});
            params.semanticRescueSlots.set(std::min(params.semanticRescueSlots.value, size_t{1}),
                                           TuningLayer::Zoom);
            break;

        case SearchEngineConfig::NavigationZoomLevel::Neighborhood:
            params.weights.kg.scaleBy(1.10f, TuningLayer::Zoom);
            params.weights.pathTree.scaleBy(1.05f, TuningLayer::Zoom);
            params.graphRerankTopN = std::max(params.graphRerankTopN, size_t{24});
            break;

        case SearchEngineConfig::NavigationZoomLevel::Street:
            params.weights.text.scaleBy(1.10f, TuningLayer::Zoom);
            params.weights.pathTree.scaleBy(1.15f, TuningLayer::Zoom);
            params.weights.entityVector.scaleBy(1.10f, TuningLayer::Zoom);
            params.weights.vector.scaleBy(0.85f, TuningLayer::Zoom);
            params.weights.kg.scaleBy(0.90f, TuningLayer::Zoom);
            setClamp(params.similarityThreshold, params.similarityThreshold.value + 0.03f,
                     TuningLayer::Zoom, 0.0f, 1.0f);
            params.vectorOnlyThreshold = std::clamp(params.vectorOnlyThreshold + 0.02f, 0.0f, 1.0f);
            params.vectorOnlyPenalty = std::clamp(params.vectorOnlyPenalty * 0.90f, 0.0f, 1.0f);
            params.semanticRescueSlots.set(0, TuningLayer::Zoom);
            params.lexicalFloorTopN = std::max(params.lexicalFloorTopN, size_t{10});
            params.lexicalFloorBoost = std::max(params.lexicalFloorBoost, 0.16f);
            params.enableLexicalTieBreak = true;
            params.lexicalTieBreakEpsilon = std::max(params.lexicalTieBreakEpsilon, 0.010f);
            params.rerankTopK = std::max(params.rerankTopK, size_t{8});
            break;
    }
}

void applyZoomConfigExtras(SearchEngineConfig::NavigationZoomLevel zoom,
                           SearchEngineConfig& config) {
    switch (zoom) {
        case SearchEngineConfig::NavigationZoomLevel::Auto:
            return;
        case SearchEngineConfig::NavigationZoomLevel::Map:
            config.graphTextWeight = std::clamp(config.graphTextWeight * 1.15f, 0.0f, 1.0f);
            config.graphVectorWeight = std::clamp(config.graphVectorWeight * 1.15f, 0.0f, 1.0f);
            config.graphExpansionMaxTerms = std::max(config.graphExpansionMaxTerms, size_t{10});
            config.graphExpansionMaxSeeds = std::max(config.graphExpansionMaxSeeds, size_t{8});
            config.rerankSnippetMaxChars = std::min(config.rerankSnippetMaxChars, size_t{192});
            break;
        case SearchEngineConfig::NavigationZoomLevel::Neighborhood:
            config.graphTextWeight = std::clamp(config.graphTextWeight * 1.05f, 0.0f, 1.0f);
            config.graphExpansionMaxTerms = std::max(config.graphExpansionMaxTerms, size_t{8});
            config.rerankSnippetMaxChars = std::max(config.rerankSnippetMaxChars, size_t{256});
            break;
        case SearchEngineConfig::NavigationZoomLevel::Street:
            config.graphVectorWeight = std::clamp(config.graphVectorWeight * 0.85f, 0.0f, 1.0f);
            config.graphTextWeight = std::clamp(config.graphTextWeight * 0.95f, 0.0f, 1.0f);
            config.rerankSnippetMaxChars = std::max(config.rerankSnippetMaxChars, size_t{384});
            if (config.graphExpansionMaxTerms > 0) {
                config.graphExpansionMaxTerms = std::min(config.graphExpansionMaxTerms, size_t{6});
            }
            if (config.graphRerankTopN > 0) {
                config.graphRerankTopN = std::min(config.graphRerankTopN, size_t{18});
            }
            break;
    }
}

// ---------------------------------------------------------------------------
// Layer 5: Intent
// ---------------------------------------------------------------------------

void applyIntentLayer(QueryIntent intent, TunedParams& params) {
    switch (intent) {
        case QueryIntent::Path:
            params.weights.pathTree.scaleBy(1.8f, TuningLayer::Intent);
            params.weights.text.scaleBy(0.8f, TuningLayer::Intent);
            params.weights.vector.scaleBy(0.7f, TuningLayer::Intent);
            params.weights.entityVector.scaleBy(0.8f, TuningLayer::Intent);
            params.weights.kg.scaleBy(0.8f, TuningLayer::Intent);
            params.weights.tag.scaleBy(0.9f, TuningLayer::Intent);
            break;
        case QueryIntent::Code:
            params.weights.pathTree.scaleBy(1.5f, TuningLayer::Intent);
            params.weights.entityVector.scaleBy(1.5f, TuningLayer::Intent);
            params.weights.text.scaleBy(0.8f, TuningLayer::Intent);
            params.weights.vector.scaleBy(0.7f, TuningLayer::Intent);
            params.weights.kg.scaleBy(0.9f, TuningLayer::Intent);
            break;
        case QueryIntent::Prose:
            params.weights.text.scaleBy(1.25f, TuningLayer::Intent);
            params.weights.vector.scaleBy(0.9f, TuningLayer::Intent);
            params.weights.pathTree.scaleBy(0.6f, TuningLayer::Intent);
            params.weights.entityVector.scaleBy(0.6f, TuningLayer::Intent);
            params.weights.kg.scaleBy(0.8f, TuningLayer::Intent);
            break;
        case QueryIntent::Mixed:
            break;
    }
}

// ---------------------------------------------------------------------------
// Layer 6: Community
// ---------------------------------------------------------------------------

void applyCommunityLayer(std::optional<TuningState> communityState, TuningState currentState,
                         TunedParams& params) {
    if (!communityState.has_value() || *communityState == currentState) {
        return;
    }

    const TunedParams target = getTunedParams(*communityState);
    constexpr float kBlend = 0.60f;

    // Slotted fields: 60% toward target, pinned slots unmodified (set() is a no-op when pinned)
    blendSlot(params.weights.text, target.weights.text.value, kBlend, TuningLayer::Community);
    blendSlot(params.weights.vector, target.weights.vector.value, kBlend, TuningLayer::Community);
    blendSlot(params.weights.entityVector, target.weights.entityVector.value, kBlend,
              TuningLayer::Community);
    blendSlot(params.weights.pathTree, target.weights.pathTree.value, kBlend,
              TuningLayer::Community);
    blendSlot(params.weights.kg, target.weights.kg.value, kBlend, TuningLayer::Community);
    blendSlot(params.weights.tag, target.weights.tag.value, kBlend, TuningLayer::Community);
    blendSlot(params.weights.metadata, target.weights.metadata.value, kBlend,
              TuningLayer::Community);
    blendSlot(params.similarityThreshold, target.similarityThreshold.value, kBlend,
              TuningLayer::Community);
    blendSlot(params.semanticRescueSlots, target.semanticRescueSlots.value, kBlend,
              TuningLayer::Community);

    // Plain numeric fields: lerp toward target
    params.semanticRescueMinVectorScore =
        lerpValue(params.semanticRescueMinVectorScore, target.semanticRescueMinVectorScore, kBlend);
    params.rerankTopK = lerpValue(params.rerankTopK, target.rerankTopK, kBlend);
    params.rerankAnchoredMinRelativeScore = lerpValue(
        params.rerankAnchoredMinRelativeScore, target.rerankAnchoredMinRelativeScore, kBlend);
    params.lexicalFloorTopN = lerpValue(params.lexicalFloorTopN, target.lexicalFloorTopN, kBlend);
    params.lexicalFloorBoost =
        lerpValue(params.lexicalFloorBoost, target.lexicalFloorBoost, kBlend);
    params.lexicalTieBreakEpsilon =
        lerpValue(params.lexicalTieBreakEpsilon, target.lexicalTieBreakEpsilon, kBlend);
    params.fusionEvidenceRescueSlots =
        lerpValue(params.fusionEvidenceRescueSlots, target.fusionEvidenceRescueSlots, kBlend);
    params.fusionEvidenceRescueMinScore =
        lerpValue(params.fusionEvidenceRescueMinScore, target.fusionEvidenceRescueMinScore, kBlend);
    params.subPhraseScoringPenalty =
        lerpValue(params.subPhraseScoringPenalty, target.subPhraseScoringPenalty, kBlend);

    // Boolean gates: OR semantics (community query gets union of capabilities)
    params.enableSubPhraseRescoring =
        params.enableSubPhraseRescoring || target.enableSubPhraseRescoring;
    params.enableLexicalTieBreak = params.enableLexicalTieBreak || target.enableLexicalTieBreak;
    params.enableAdaptiveVectorFallback =
        params.enableAdaptiveVectorFallback || target.enableAdaptiveVectorFallback;

    // Enums/strategy: adopt target profile's strategy
    params.rrfK = target.rrfK;
    params.fusionStrategy = target.fusionStrategy;
    params.chunkAggregation = target.chunkAggregation;
    params.vectorBoostFactor = target.vectorBoostFactor;
}

// ---------------------------------------------------------------------------
// Layer 7: Semantic-only mode
// ---------------------------------------------------------------------------

void applySemanticOnlyLayer(TunedParams& params) {
    params.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_SUM;
    params.similarityThreshold.forceSet(std::min(params.similarityThreshold.value, 0.30f),
                                        TuningLayer::Mode);
    params.weights.text.forceSet(std::min(params.weights.text.value, 0.20f), TuningLayer::Mode);
    params.weights.kg.forceSet(0.0f, TuningLayer::Mode);
    params.weights.vector.forceSet(std::max(params.weights.vector.value, 0.45f), TuningLayer::Mode);
    params.weights.entityVector.forceSet(std::max(params.weights.entityVector.value, 0.15f),
                                         TuningLayer::Mode);
    params.enableGraphRerank = false;
    params.vectorOnlyThreshold =
        std::min(params.vectorOnlyThreshold, params.similarityThreshold.value);
    params.vectorOnlyPenalty = 1.0f;
    params.vectorOnlyNearMissPenalty = 1.0f;
}

void applySemanticOnlyConfigExtras(SearchEngineConfig& config) {
    config.graphTextWeight = 0.0f;
    config.graphVectorWeight = 0.0f;
}

} // namespace yams::search
