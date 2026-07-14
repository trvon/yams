// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <string_view>

namespace yams::search::metrics {

constexpr std::string_view kSearchPipelineName = "search_pipeline_name";
constexpr std::string_view kCandidatePipelineInputComponents =
    "candidate_pipeline_input_components";
constexpr std::string_view kCandidatePipelineAggregatedCandidates =
    "candidate_pipeline_aggregated_candidates";
constexpr std::string_view kCandidatePipelineFusedCandidates =
    "candidate_pipeline_fused_candidates";
constexpr std::string_view kCandidatePipelineFinalCandidates =
    "candidate_pipeline_final_candidates";
constexpr std::string_view kCandidatePipelineTopologyAnnotatedCandidates =
    "candidate_pipeline_topology_annotated_candidates";

constexpr std::string_view kSearchEngineReady = "search_engine_ready";
constexpr std::string_view kVectorReady = "vector_ready";
constexpr std::string_view kKgReady = "kg_ready";
constexpr std::string_view kTopologyReady = "topology_ready";
constexpr std::string_view kTopologyArtifactsFresh = "topology_artifacts_fresh";
constexpr std::string_view kTopologyEpoch = "topology_epoch";
constexpr std::string_view kTopologyConstructionFingerprint = "topology_construction_fingerprint";

constexpr std::string_view kTopologyRoutingMode = "topology_routing_mode";
constexpr std::string_view kTopologyVectorPolicy = "topology_vector_policy";
constexpr std::string_view kTopologyRouteScoringMode = "topology_route_scoring_mode";
constexpr std::string_view kTopologySparseDenseAlpha = "topology_sparse_dense_alpha";
constexpr std::string_view kTopologyMinRouteScore = "topology_min_route_score";
constexpr std::string_view kTopologyArtifactAdmitted = "topology_artifact_admitted";
constexpr std::string_view kTopologyWeakQueryEnabled = "topology_weak_query_enabled";
constexpr std::string_view kTopologyWeakQueryLoadAttempted = "topology_weak_query_load_attempted";
constexpr std::string_view kTopologyWeakQueryLoadSucceeded = "topology_weak_query_load_succeeded";
constexpr std::string_view kTopologyWeakQueryApplied = "topology_weak_query_applied";
constexpr std::string_view kTopologyWeakQueryNarrowApplied = "topology_weak_query_narrow_applied";
constexpr std::string_view kTopologyWeakQuerySkipReason = "topology_weak_query_skip_reason";
constexpr std::string_view kTopologyWeakQueryRoutesRejected = "topology_weak_query_routes_rejected";
constexpr std::string_view kTopologyWeakQueryRoutedClusters = "topology_weak_query_routed_clusters";
constexpr std::string_view kTopologyRouteAvailableCount = "topology_route_available_count";
constexpr std::string_view kTopologyRouteBoundaryScoreMargin =
    "topology_route_boundary_score_margin";
constexpr std::string_view kTopologyRouteConfidenceAbstained =
    "topology_route_confidence_abstained";
constexpr std::string_view kTopologyRouteRepresentativeDistanceEvaluations =
    "topology_route_representative_distance_evaluations";
constexpr std::string_view kTopologyRouteRepresentativeCountMax =
    "topology_route_representative_count_max";
constexpr std::string_view kTopologyWeakQueryRoutedDocs = "topology_weak_query_routed_docs";
constexpr std::string_view kTopologyWeakQueryAddedCandidates =
    "topology_weak_query_added_candidates";
constexpr std::string_view kTopologyWeakQueryDuplicateCandidates =
    "topology_weak_query_duplicate_candidates";
constexpr std::string_view kTopologyWeakQueryStaleCandidates =
    "topology_weak_query_stale_candidates";
constexpr std::string_view kTopologyWeakQueryAddedCandidateHashes =
    "topology_weak_query_added_candidate_hashes";
constexpr std::string_view kTopologyWeakQueryTotalCandidates =
    "topology_weak_query_total_candidates";
constexpr std::string_view kTopologyWeakQueryAllowedCandidates =
    "topology_weak_query_allowed_candidates";
constexpr std::string_view kTopologyWeakQueryAllowedCandidateHashes =
    "topology_weak_query_allowed_candidate_hashes";
constexpr std::string_view kTopologySnapshotCacheHit = "topology_snapshot_cache_hit";
constexpr std::string_view kTopologyStructureCandidateCount = "topology_structure_candidate_count";
constexpr std::string_view kTopologyStructureScaleAgreementMean =
    "topology_structure_scale_agreement_mean";
constexpr std::string_view kTopologyStructureOverlapSupportMean =
    "topology_structure_overlap_support_mean";
constexpr std::string_view kTopologyStructurePersistenceSupportMean =
    "topology_structure_persistence_support_mean";
constexpr std::string_view kTopologyStructureCohesionSupportMean =
    "topology_structure_cohesion_support_mean";
constexpr std::string_view kTopologyStructureBridgeSupportMean =
    "topology_structure_bridge_support_mean";
constexpr std::string_view kTopologyStructureDensitySupportMean =
    "topology_structure_density_support_mean";
constexpr std::string_view kVectorSearchCandidateBudget = "vector_search_candidate_budget";
constexpr std::string_view kVectorSearchResultBudget = "vector_search_result_budget";
constexpr std::string_view kVectorSearchDistanceEvaluationBudget =
    "vector_search_distance_evaluation_budget";
constexpr std::string_view kVectorSearchRowsVisitedActual = "vector_search_rows_visited_actual";
constexpr std::string_view kVectorSearchExactDistanceEvaluationsActual =
    "vector_search_exact_distance_evaluations_actual";
constexpr std::string_view kVectorSearchAnnCandidateBudgetActual =
    "vector_search_ann_candidate_budget_actual";
constexpr std::string_view kTopologyVectorFilterApplied = "topology_vector_filter_applied";
constexpr std::string_view kTopologyVectorFilterFallback = "topology_vector_filter_fallback";
constexpr std::string_view kTopologyVectorFilterMatched = "topology_vector_filter_matched";
constexpr std::string_view kTopologyVectorFilterRemoved = "topology_vector_filter_removed";
constexpr std::string_view kTopologyVectorAllowedSetAnnApplied =
    "topology_vector_allowed_set_ann_applied";
constexpr std::string_view kTopologyVectorAllowedSetAnnFallback =
    "topology_vector_allowed_set_ann_fallback";
constexpr std::string_view kTopologyShadowEvaluated = "topology_shadow_evaluated";
constexpr std::string_view kTopologyShadowProposedAction = "topology_shadow_proposed_action";
constexpr std::string_view kTopologyShadowRetainedCandidates =
    "topology_shadow_retained_candidates";
constexpr std::string_view kTopologyShadowRemovedCandidates = "topology_shadow_removed_candidates";
constexpr std::string_view kTopologyShadowRetainedCandidateDocIds =
    "topology_shadow_retained_candidate_doc_ids";
constexpr std::string_view kTopologyShadowRemovedCandidateDocIds =
    "topology_shadow_removed_candidate_doc_ids";

constexpr std::string_view kTimingTopologyWeakQuery = "topology_weak_query";
constexpr std::string_view kTimingTopologyLoad = "topology_load";
constexpr std::string_view kTimingTopologyValidate = "topology_validate";
constexpr std::string_view kTimingTopologyRequestPrep = "topology_request_prep";
constexpr std::string_view kTimingTopologyRoute = "topology_route";
constexpr std::string_view kTimingTopologyClusterLookup = "topology_cluster_lookup";
constexpr std::string_view kTimingTopologyDocLookup = "topology_doc_lookup";
constexpr std::string_view kTimingTopologyCandidateInsert = "topology_candidate_insert";

constexpr std::string_view kTopologyRouteBestScore = "topology_route_best_score";
constexpr std::string_view kTopologyRouteMeanAcceptedScore = "topology_route_mean_accepted_score";
constexpr std::string_view kTopologyRouteAcceptedCount = "topology_route_accepted_count";
constexpr std::string_view kTopologySeedCount = "topology_seed_count";
constexpr std::string_view kTopologySeedCoverageCount = "topology_seed_coverage_count";

} // namespace yams::search::metrics
