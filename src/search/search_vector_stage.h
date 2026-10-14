// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

// Vector candidate stage of the tiered search fanout: global ANN plus the topology Shadow /
// Augment / Narrow / RouteOnly policies, routed candidate queries, rescue merging and route-work
// accounting.
// The stage is a pure function of its request; SearchEngine owns scheduling, the relaxed-retry
// query wrapper, the certificate it applies route work to, and every debug-stat write.

#pragma once

#include <yams/core/types.h>
#include <yams/search/search_engine_config.h>
#include <yams/search/search_models.h>
#include <yams/search/topology_routing_session.h>
#include <yams/vector/vector_types.h>

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

namespace yams::search::detail {

/// One query against the vector index. A non-null `candidates` set narrows the search; a non-null
/// `diagnostics` receives the merged backend sample. SearchEngine injects its relaxed-threshold
/// retry wrapper so the stage never reads engine state.
using VectorStageQueryFn = std::function<Result<std::vector<ComponentResult>>(
    const std::vector<float>& embedding, std::size_t limit,
    const std::unordered_set<std::string>* candidates, vector::VectorSearchDiagnostics* diagnostics,
    vector::CandidateFilterMode candidateFilterMode)>;

struct VectorStageRequest {
    std::vector<float> queryEmbedding;
    VectorStageQueryFn queryVector;
    /// Tier-1 candidate hashes before topology expansion: the narrowing set under
    /// `narrowToBaseline`, and the "already known" set when rescues are classified. Non-null.
    std::shared_ptr<const std::unordered_set<std::string>> baselineCandidates;

    // Policy (mutually exclusive topology policies; all false = plain global ANN).
    bool shadow{false};  ///< TopologyVectorPolicy::Shadow
    bool augment{false}; ///< TopologyVectorPolicy::Augment
    /// TopologyVectorPolicy::RouteOnly with the evaluation gate open: only the routed candidate
    /// queries run (DocumentTopK on allowed-set ANN backends, ExactDocumentComplete otherwise),
    /// with no global fill and no admission check. The caller downgrades a gated-off RouteOnly to
    /// `shadow` before building the request.
    bool routeOnly{false};
    bool filterEligible{false};        ///< Narrow with a route eligible for trial narrowing
    bool allowedSetAnnEligible{false}; ///< filterEligible and the backend ranks the allowed set
    bool narrowToBaseline{false};      ///< tieredNarrowVectorSearch with enough Tier-1 candidates
    bool stageTraceEnabled{false};     ///< collect doc-id lists and the exact Shadow control

    // Backend capabilities.
    bool backendSupportsRoutedCandidates{false};
    bool backendSupportsAllowedSetAnn{false};

    // Limits.
    std::size_t maxResults{0};        ///< effective global vector result limit
    std::size_t routedResultLimit{0}; ///< cap on the union of routed candidates
    /// Per-group result quotas aligned with `allowedDocumentGroups`
    /// (allocateBalancedRouteQuotas).
    std::vector<std::size_t> routeQuotas;
    SearchEngineConfig::TopologyRouteWorkBudget routeWorkBudget;

    // Route. The certificate is read for route usefulness and, with the observed work applied to a
    // local copy, for the admission action. The allowed sets are the backend filter inputs and
    // normally alias the certificate's members. All three are non-null.
    std::shared_ptr<const TopologyRouteCertificate> certificate;
    std::shared_ptr<const std::unordered_set<std::string>> allowedDocuments;
    std::shared_ptr<const std::vector<std::unordered_set<std::string>>> allowedDocumentGroups;

    // Road beam (TopologyRoutingSessionResult::roadExpandedDocumentHashGroups). RouteOnly and
    // Augment query these groups after the certified groups as extra routes with their own quotas
    // (`roadQuotas`, aligned) and their own cap on appended results (`roadResultLimit`). Null or
    // empty when road_hops is 0. Never a Narrow filter and never part of the Shadow control.
    std::shared_ptr<const std::vector<std::unordered_set<std::string>>> roadDocumentGroups;
    std::vector<std::size_t> roadQuotas;
    std::size_t roadResultLimit{0};

    // Road walk (TopologyRoutingSessionResult::roadWalkIndex). After the certified and road
    // groups, RouteOnly and Augment expand every routed hit by `roadWalkWindow` positions along
    // its cluster's walk order and score the walked documents as one more routed query, so their
    // rows and distance evaluations land in the routed work like road candidates. Off when the
    // window is 0 or the index is null. Never a Narrow filter and never part of the Shadow control.
    std::size_t roadWalkWindow{0};
    std::shared_ptr<const TopologyRoadWalkIndex> roadWalkIndex;
};

/// Where the stage's result set came from (vector_candidate_source_effective).
enum class VectorCandidateSource {
    Ann,         ///< global ANN, possibly narrowed to the Tier-1 baseline
    RouteOnly,   ///< routed candidate queries only, no global fill
    Augment,     ///< global ANN unioned with the routed expansion
    Narrow,      ///< committed Narrow: allowed-set ANN or the admitted route filter
    Unavailable, ///< RouteOnly requested but the backend has no routed candidates (Vec0): the
                 ///< global leg is returned unchanged
};

[[nodiscard]] constexpr const char* vectorCandidateSourceToString(VectorCandidateSource source) {
    switch (source) {
        case VectorCandidateSource::Ann:
            return "ann";
        case VectorCandidateSource::RouteOnly:
            return "route_only";
        case VectorCandidateSource::Augment:
            return "augment";
        case VectorCandidateSource::Narrow:
            return "narrow";
        case VectorCandidateSource::Unavailable:
            return "unavailable";
    }
    return "ann";
}

/// Routed-vector work the stage observed. The caller applies it to the certificate.
struct VectorStageRouteWork {
    TopologyRouteWorkObservation work;
    TopologyProofObligationStatus admissionWork{TopologyProofObligationStatus::Unavailable};
};

/// Everything the stage produces. The posted work owns its instance (shared_ptr captured by value)
/// and the search frame adopts it once the component has been collected, so a completion after the
/// component deadline never writes into the frame.
struct VectorStageOutcome {
    Result<std::vector<ComponentResult>> results{std::vector<ComponentResult>{}};

    /// Diagnostics of every global query (including a narrowed-to-baseline query).
    vector::VectorSearchDiagnostics globalDiag;
    /// Diagnostics of every routed query: the Augment expansion, the RouteOnly result set, the
    /// Narrow allowed-set query and the Shadow counterfactual. Shadow's routed work feeds
    /// `routeWork` only and is not reported as vector search cost; the caller decides what to
    /// merge.
    vector::VectorSearchDiagnostics routedDiag;
    /// Set at most once per run: Shadow (counterfactual), RouteOnly (routed query), Narrow
    /// allowed-set ANN (routed query) or Narrow filter (global query).
    std::optional<VectorStageRouteWork> routeWork;
    VectorCandidateSource candidateSource{VectorCandidateSource::Ann};
    /// Road-expanded documents appended to the routed result set (RouteOnly, Augment).
    std::size_t roadResultsAdded{0};
    /// Road walk: the window walked with (0 when nothing was walkable), the routed hits that
    /// anchored a window and the walked documents appended to the routed result set.
    std::size_t roadWalkWindowApplied{0};
    std::size_t roadWalkAnchors{0};
    std::size_t roadWalkResultsAdded{0};

    // Narrow.
    bool filterApplied{false};
    bool filterFallback{false};
    std::size_t filterMatched{0};
    std::size_t filterRemoved{0};
    bool allowedSetAnnApplied{false};
    bool allowedSetAnnFallback{false};

    // Shadow projection of the global results onto the route.
    std::size_t shadowRetainedCandidates{0};
    std::size_t shadowRemovedCandidates{0};
    std::vector<std::string> shadowRetainedDocIds; // trace only
    std::vector<std::string> shadowRemovedDocIds;  // trace only

    // Rescue merge (Augment commits it; Shadow measures it).
    bool rescueAttempted{false};
    bool rescueApplied{false}; // Augment only
    std::size_t rescueAdded{0};
    std::size_t rescueNovel{0};
    std::size_t evidenceRescues{0};
    std::size_t rescueDuplicates{0};
    std::vector<std::string> rescueAddedHashes;
    std::vector<std::string> rescueAddedDocIds;
    std::vector<std::string> rescueNovelHashes;
    std::vector<std::string> rescueNovelDocIds;
    std::vector<std::string> evidenceRescueHashes;
    std::vector<std::string> evidenceRescueDocIds;
    std::vector<std::string> rescueScoredDocIds;

    // Shadow exact-control query (trace only).
    std::vector<std::string> rescueExactScoredDocIds;
    std::string rescueExactScoredRows;
    std::size_t rescueExactCandidateHashesRequested{0};
    bool rescueExactRowsVisitedObserved{false};
    std::size_t rescueExactRowsVisited{0};
    bool rescueExactDistanceEvaluationsObserved{false};
    std::size_t rescueExactDistanceEvaluations{0};
    std::size_t rescueExactReturnedRows{0};
    std::vector<std::string> rescueExactMatchedDocumentHashes;
};

/// Run the vector candidate stage. Query order is part of the contract: the injected query
/// function may record last-writer statistics (relaxed_vector_primary_hit_count).
[[nodiscard]] VectorStageOutcome runVectorCandidateStage(const VectorStageRequest& request);

} // namespace yams::search::detail
