// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include "search_vector_stage.h"

#include "search_vector_pipeline_internal.h"
#include <yams/search/search_tracing.h>

#include <algorithm>
#include <utility>

namespace yams::search::detail {

namespace {

void assignRescueMerge(VectorStageOutcome& outcome, CandidateRescueMergeResult& merged) {
    outcome.rescueAdded = merged.added;
    outcome.rescueNovel = merged.novelDocuments;
    outcome.evidenceRescues = merged.evidenceRescues;
    outcome.rescueDuplicates = merged.duplicates;
    outcome.rescueAddedHashes = std::move(merged.addedDocumentHashes);
    outcome.rescueAddedDocIds = std::move(merged.addedDocumentIds);
    outcome.rescueNovelHashes = std::move(merged.novelDocumentHashes);
    outcome.rescueNovelDocIds = std::move(merged.novelDocumentIds);
    outcome.evidenceRescueHashes = std::move(merged.evidenceRescueDocumentHashes);
    outcome.evidenceRescueDocIds = std::move(merged.evidenceRescueDocumentIds);
}

} // namespace

VectorStageOutcome runVectorCandidateStage(const VectorStageRequest& request) {
    VectorStageOutcome outcome;
    const auto& embedding = request.queryEmbedding;
    const auto& baselineCandidates = *request.baselineCandidates;
    const auto& certificate = *request.certificate;
    const auto& allowedDocuments = *request.allowedDocuments;
    const auto& allowedDocumentGroups = *request.allowedDocumentGroups;

    const auto* roadDocumentGroups = request.roadDocumentGroups.get();
    const bool roadGroupsAvailable = roadDocumentGroups != nullptr &&
                                     !roadDocumentGroups->empty() &&
                                     request.roadQuotas.size() == roadDocumentGroups->size();

    // `includeRoads` appends the road-expanded groups after the certified groups under their own
    // cap; the certified part of the result is identical with roads on or off.
    const auto queryRoutedVectors = [&](vector::VectorSearchDiagnostics& diagnostics,
                                        vector::CandidateFilterMode candidateFilterMode =
                                            vector::CandidateFilterMode::BackendDefault,
                                        bool includeRoads = false) {
        std::vector<ComponentResult> routedResults;
        routedResults.reserve(request.routedResultLimit);
        std::unordered_set<std::string> seenDocuments;
        seenDocuments.reserve(request.routedResultLimit * 2);
        const auto appendUnique = [&](std::vector<ComponentResult> incoming, std::size_t cap) {
            std::size_t appended = 0;
            for (auto& candidate : incoming) {
                if (routedResults.size() >= cap) {
                    break;
                }
                if (candidate.documentHash.empty() ||
                    !seenDocuments.insert(candidate.documentHash).second) {
                    continue;
                }
                routedResults.push_back(std::move(candidate));
                ++appended;
            }
            return appended;
        };

        if (!allowedDocumentGroups.empty()) {
            for (std::size_t route = 0; route < allowedDocumentGroups.size(); ++route) {
                const auto quota = request.routeQuotas[route];
                if (quota == 0) {
                    continue;
                }
                auto routed = request.queryVector(embedding, quota, &allowedDocumentGroups[route],
                                                  &diagnostics, candidateFilterMode);
                if (routed) {
                    appendUnique(std::move(routed.value()), request.routedResultLimit);
                }
            }
        } else {
            auto routed = request.queryVector(embedding, request.routedResultLimit,
                                              &allowedDocuments, &diagnostics, candidateFilterMode);
            if (routed) {
                appendUnique(std::move(routed.value()), request.routedResultLimit);
            }
        }
        if (includeRoads && roadGroupsAvailable) {
            const std::size_t roadCap = routedResults.size() + request.roadResultLimit;
            for (std::size_t road = 0; road < roadDocumentGroups->size(); ++road) {
                const auto quota = request.roadQuotas[road];
                if (quota == 0 || (*roadDocumentGroups)[road].empty()) {
                    continue;
                }
                auto routed = request.queryVector(embedding, quota, &(*roadDocumentGroups)[road],
                                                  &diagnostics, candidateFilterMode);
                if (routed) {
                    outcome.roadResultsAdded += appendUnique(std::move(routed.value()), roadCap);
                }
            }
        }
        // Road walk: every routed hit so far (certified and road) anchors +-window positions along
        // its cluster's order. The walked set is one extra routed query whose quota is its own
        // size: the walk rescues ordinal neighbours, so ranking them by similarity and truncating
        // would reintroduce the semantic filter it exists to bypass. Work is counted through the
        // same diagnostics as the road candidates.
        if (includeRoads && request.roadWalkWindow > 0 && request.roadWalkIndex) {
            std::vector<std::string> anchors;
            anchors.reserve(routedResults.size());
            for (const auto& hit : routedResults) {
                anchors.push_back(hit.documentHash);
            }
            outcome.roadWalkWindowApplied = request.roadWalkWindow;
            const auto walked =
                walkTopologyRoadOrders(*request.roadWalkIndex, anchors, request.roadWalkWindow,
                                       seenDocuments, &outcome.roadWalkAnchors);
            if (!walked.empty()) {
                auto routed = request.queryVector(embedding, walked.size(), &walked, &diagnostics,
                                                  candidateFilterMode);
                if (routed) {
                    outcome.roadWalkResultsAdded += appendUnique(
                        std::move(routed.value()), routedResults.size() + walked.size());
                }
            }
        }
        return routedResults;
    };

    const auto queryGlobal = [&](const std::unordered_set<std::string>* candidates) {
        return request.queryVector(embedding, request.maxResults, candidates, &outcome.globalDiag,
                                   vector::CandidateFilterMode::BackendDefault);
    };

    // Record the observed routed work and return the admission action it implies. The session
    // certificate is never written here; the caller applies `outcome.routeWork` after collection.
    const auto recordRouteWork = [&](const vector::VectorSearchDiagnostics& diagnostics) {
        outcome.routeWork = VectorStageRouteWork{
            .work = observeTopologyRouteWork(diagnostics),
            .admissionWork = evaluateTopologyRouteWork(diagnostics, request.routeWorkBudget),
        };
        TopologyRouteCertificate evaluated = certificate;
        evaluated.work = outcome.routeWork->work;
        evaluated.admission.work = outcome.routeWork->admissionWork;
        return evaluated.action();
    };

    const bool routedQueriesAvailable = certificate.hasUsefulRoute() &&
                                        request.backendSupportsRoutedCandidates &&
                                        !allowedDocuments.empty();

    // Routed results are served without a global fill, so order them like the global leg would.
    const auto rankByScore = [](std::vector<ComponentResult>& routedResults) {
        std::ranges::sort(routedResults, [](const auto& lhs, const auto& rhs) {
            if (lhs.score != rhs.score) {
                return lhs.score > rhs.score;
            }
            return lhs.documentHash < rhs.documentHash;
        });
        for (std::size_t rank = 0; rank < routedResults.size(); ++rank) {
            routedResults[rank].rank = rank;
        }
    };

    std::vector<ComponentResult> shadowExpansion;
    Result<std::vector<ComponentResult>> results = [&]() {
        if (request.routeOnly) {
            if (!routedQueriesAvailable) {
                // Nothing to route to. A backend without routed candidates (Vec0) can never serve
                // this policy; a supporting backend without a useful route is plain ANN.
                outcome.candidateSource = request.backendSupportsRoutedCandidates
                                              ? VectorCandidateSource::Ann
                                              : VectorCandidateSource::Unavailable;
                return queryGlobal(nullptr);
            }
            // Complete document-level scoring of the routed set: one best row per document on
            // allowed-set ANN backends, every row for exact backends. No admission gate and no
            // global fill: the route is the candidate set, certified or not.
            const auto candidateFilterMode =
                request.backendSupportsAllowedSetAnn
                    ? vector::CandidateFilterMode::DocumentTopK
                    : vector::CandidateFilterMode::ExactDocumentComplete;
            auto routedResults =
                queryRoutedVectors(outcome.routedDiag, candidateFilterMode, /*includeRoads=*/true);
            recordRouteWork(outcome.routedDiag);
            rankByScore(routedResults);
            outcome.candidateSource = VectorCandidateSource::RouteOnly;
            return Result<std::vector<ComponentResult>>{std::move(routedResults)};
        }
        if (request.augment && routedQueriesAvailable) {
            outcome.candidateSource = VectorCandidateSource::Augment;
            outcome.rescueAttempted = true;
            auto global = queryGlobal(nullptr);
            if (!global) {
                return global;
            }
            const auto candidateFilterMode = request.backendSupportsAllowedSetAnn
                                                 ? vector::CandidateFilterMode::DocumentTopK
                                                 : vector::CandidateFilterMode::BackendDefault;
            auto expansion =
                queryRoutedVectors(outcome.routedDiag, candidateFilterMode, /*includeRoads=*/true);
            outcome.rescueScoredDocIds = collectUniqueComponentDocIds(expansion);
            auto merged = mergeVectorCandidateRescues(std::move(global.value()),
                                                      std::move(expansion), baselineCandidates);
            assignRescueMerge(outcome, merged);
            outcome.rescueApplied = merged.added > 0;
            return Result<std::vector<ComponentResult>>{std::move(merged.results)};
        }
        if (request.shadow && routedQueriesAvailable) {
            outcome.rescueAttempted = true;
            shadowExpansion = queryRoutedVectors(outcome.routedDiag);
            outcome.rescueScoredDocIds = collectUniqueComponentDocIds(shadowExpansion);
            if (request.stageTraceEnabled) {
                vector::VectorSearchDiagnostics exactDiagnostics;
                exactDiagnostics.collectVisitedDocumentHashes = true;
                auto exactExpansion = queryRoutedVectors(
                    exactDiagnostics, vector::CandidateFilterMode::ExactDocumentComplete);
                outcome.rescueExactCandidateHashesRequested = allowedDocuments.size();
                outcome.rescueExactRowsVisitedObserved = exactDiagnostics.rowsVisitedObserved;
                outcome.rescueExactRowsVisited = exactDiagnostics.rowsVisited;
                outcome.rescueExactDistanceEvaluationsObserved =
                    exactDiagnostics.exactDistanceEvaluationsObserved;
                outcome.rescueExactDistanceEvaluations = exactDiagnostics.exactDistanceEvaluations;
                outcome.rescueExactReturnedRows = exactDiagnostics.returnedRows;
                outcome.rescueExactMatchedDocumentHashes.assign(
                    exactDiagnostics.visitedDocumentHashes.begin(),
                    exactDiagnostics.visitedDocumentHashes.end());
                std::ranges::sort(outcome.rescueExactMatchedDocumentHashes);
                outcome.rescueExactScoredDocIds = collectUniqueComponentDocIds(exactExpansion);
                outcome.rescueExactScoredRows =
                    buildComponentHitSummaryJson(exactExpansion, request.routedResultLimit).dump();
            }
            if (!shadowExpansion.empty()) {
                recordRouteWork(outcome.routedDiag);
            }
        }
        if (request.allowedSetAnnEligible) {
            auto routedResults = queryRoutedVectors(outcome.routedDiag);

            if (!routedResults.empty()) {
                if (recordRouteWork(outcome.routedDiag) != TopologyRouteAction::Narrow) {
                    outcome.allowedSetAnnFallback = true;
                    outcome.filterFallback = true;
                    return queryGlobal(nullptr);
                }
                outcome.allowedSetAnnApplied = true;
                outcome.filterApplied = true;
                outcome.filterMatched = routedResults.size();
                // No global fill: an admitted hard route must remain a subset of its completely
                // materialized cover.
                rankByScore(routedResults);
                return Result<std::vector<ComponentResult>>{std::move(routedResults)};
            }
            outcome.allowedSetAnnFallback = true;
            outcome.filterFallback = true;
            return queryGlobal(nullptr);
        }
        if (request.narrowToBaseline) {
            return queryGlobal(&baselineCandidates);
        }
        return queryGlobal(nullptr);
    }();

    if (results && request.shadow && !allowedDocuments.empty()) {
        auto shadowRescue = mergeVectorCandidateRescues(results.value(), std::move(shadowExpansion),
                                                        baselineCandidates);
        assignRescueMerge(outcome, shadowRescue);
        const auto globalDocIds = collectUniqueComponentDocIds(results.value());
        auto projected = filterVectorResultsByAllowedDocuments(results.value(), allowedDocuments);
        const auto retainedDocIds = collectUniqueComponentDocIds(projected.results);
        outcome.shadowRetainedCandidates = retainedDocIds.size();
        outcome.shadowRemovedCandidates = globalDocIds.size() - retainedDocIds.size();
        if (request.stageTraceEnabled) {
            outcome.shadowRetainedDocIds = retainedDocIds;
            outcome.shadowRemovedDocIds = setDifferenceIds(globalDocIds, retainedDocIds);
        }
    }
    if (results && request.filterEligible && !request.allowedSetAnnEligible) {
        if (recordRouteWork(outcome.globalDiag) != TopologyRouteAction::Narrow) {
            outcome.filterFallback = true;
        } else {
            auto filtered =
                filterVectorResultsByAllowedDocuments(std::move(results.value()), allowedDocuments);
            outcome.filterApplied = filtered.applied;
            outcome.filterFallback = filtered.fellBackToGlobal;
            outcome.filterMatched = filtered.matched;
            outcome.filterRemoved = filtered.removed;
            results = Result<std::vector<ComponentResult>>{std::move(filtered.results)};
        }
    }
    if (outcome.filterApplied) {
        outcome.candidateSource = VectorCandidateSource::Narrow;
    }
    outcome.results = std::move(results);
    return outcome;
}

} // namespace yams::search::detail
