// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

#pragma once

#include <yams/search/query_router.h>
#include <yams/search/search_engine.h>
#include <yams/search/search_tuner.h>

#include <optional>
#include <string_view>

namespace yams::search {

struct QueryPolicyResolution {
    QueryRouteDecision routeDecision;
    SearchEngineConfig config;
    SearchEngineConfig::NavigationZoomLevel effectiveZoomLevel =
        SearchEngineConfig::NavigationZoomLevel::Auto;
    bool zoomLevelInferredFromIntent = false;
    std::optional<TuningState> communityOverride;
};

/// Seed TunedParams from an existing SearchEngineConfig so the layered tuning pipeline can
/// safely operate even when no SearchTuner is installed.
[[nodiscard]] TunedParams seedTunedParamsFromConfig(const SearchEngineConfig& config);

/// Resolve per-query routing and layered tuning into a final SearchEngineConfig.
[[nodiscard]] QueryPolicyResolution resolveQueryPolicy(std::string_view query,
                                                       const SearchEngineConfig& baseConfig,
                                                       const TunedParams& baseParams,
                                                       std::optional<TuningState> baselineState,
                                                       bool semanticOnly);

/// Layer 4: Zoom policy — scales component weights based on zoom level.
/// Operates on TunedParams slots via scaleBy() (respects pinned values).
void applyZoomLayer(SearchEngineConfig::NavigationZoomLevel zoom, TunedParams& params);

/// Zoom-specific adjustments for SearchEngineConfig-only fields
/// (graphExpansionMaxTerms, rerankSnippetMaxChars, etc.).
/// Call AFTER applyTo() since these fields don't exist in TunedParams.
void applyZoomConfigExtras(SearchEngineConfig::NavigationZoomLevel zoom,
                           SearchEngineConfig& config);

/// Layer 5: Intent policy — scales component weights based on query intent.
/// Operates on TunedParams slots via scaleBy() (respects pinned values).
void applyIntentLayer(QueryIntent intent, TunedParams& params);

/// Layer 6: Community override — blends toward a community-specific profile
/// instead of fully swapping (preserving env-pinned values).
/// Uses 60% blend toward target, 40% of current.
void applyCommunityLayer(std::optional<TuningState> communityState, TuningState currentState,
                         TunedParams& params);

/// Layer 7: Semantic-only mode — forces vector-dominant weights for
/// explicit user requests. Uses forceSet() to override even non-pinned values.
void applySemanticOnlyLayer(TunedParams& params);

/// Semantic-only adjustments for SearchEngineConfig-only fields.
/// Call AFTER applyTo().
void applySemanticOnlyConfigExtras(SearchEngineConfig& config);

} // namespace yams::search
