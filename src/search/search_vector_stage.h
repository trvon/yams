// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

// Vector candidate stage of the tiered search fanout: global ANN plus the topology Shadow /
// Augment / Narrow policies, routed candidate queries, rescue merging and route-work accounting.
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
    bool shadow{false};                ///< TopologyVectorPolicy::Shadow
    bool augment{false};               ///< TopologyVectorPolicy::Augment
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
};

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
    /// Diagnostics of every routed query: the Augment expansion, the Narrow allowed-set query and
    /// the Shadow counterfactual. Shadow's routed work feeds `routeWork` only and is not reported
    /// as vector search cost; the caller decides what to merge.
    vector::VectorSearchDiagnostics routedDiag;
    /// Set at most once per run: Shadow (counterfactual), Narrow allowed-set ANN (routed query) or
    /// Narrow filter (global query).
    std::optional<VectorStageRouteWork> routeWork;

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
