// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <string_view>

namespace yams::search::metrics {

constexpr std::string_view kSearchPipelineInterface = "search_pipeline_interface";
constexpr std::string_view kSearchPipelineName = "search_pipeline_name";
constexpr std::string_view kSearchEngineVariant = "search_engine_variant";
constexpr std::string_view kSearchPipelineVariant = "search_pipeline_variant";

constexpr std::string_view kSearchEngineReady = "search_engine_ready";
constexpr std::string_view kVectorReady = "vector_ready";
constexpr std::string_view kKgReady = "kg_ready";
constexpr std::string_view kTopologyReady = "topology_ready";
constexpr std::string_view kTopologyArtifactsFresh = "topology_artifacts_fresh";
constexpr std::string_view kTopologyEpoch = "topology_epoch";

constexpr std::string_view kTopologyRoutingMode = "topology_routing_mode";
constexpr std::string_view kTopologyVectorPolicy = "topology_vector_policy";
constexpr std::string_view kTopologyVectorAugmentationCandidates =
    "topology_vector_augmentation_candidates";
constexpr std::string_view kTopologyVectorAugmentationDocIds =
    "topology_vector_augmentation_doc_ids";
constexpr std::string_view kTopologyRouteScoringMode = "topology_route_scoring_mode";
constexpr std::string_view kTopologySparseDenseAlpha = "topology_sparse_dense_alpha";
constexpr std::string_view kTopologyMinRouteScore = "topology_min_route_score";
constexpr std::string_view kTopologyMedoidOnlyExpansion = "topology_medoid_only_expansion";
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
constexpr std::string_view kTopologyMemberRerankCandidates = "topology_member_rerank_candidates";
constexpr std::string_view kTopologyMemberRerankSelected = "topology_member_rerank_selected";
constexpr std::string_view kVectorSearchCandidateBudget = "vector_search_candidate_budget";
constexpr std::string_view kVectorSearchResultBudget = "vector_search_result_budget";
constexpr std::string_view kVectorSearchDistanceEvaluationBudget =
    "vector_search_distance_evaluation_budget";
constexpr std::string_view kVectorSearchRowsVisitedActual = "vector_search_rows_visited_actual";
constexpr std::string_view kVectorSearchExactDistanceEvaluationsActual =
    "vector_search_exact_distance_evaluations_actual";
constexpr std::string_view kVectorSearchAnnCandidateBudgetActual =
    "vector_search_ann_candidate_budget_actual";
constexpr std::string_view kTopologyMemberRerankRowsVisitedActual =
    "topology_member_rerank_rows_visited_actual";
constexpr std::string_view kTopologyMemberRerankDistanceEvaluationsActual =
    "topology_member_rerank_distance_evaluations_actual";
constexpr std::string_view kTopologyVectorScoresReused = "topology_vector_scores_reused";
constexpr std::string_view kTopologyVectorScoresReusedCount = "topology_vector_scores_reused_count";
constexpr std::string_view kTopologyVectorFilterApplied = "topology_vector_filter_applied";
constexpr std::string_view kTopologyVectorFilterFallback = "topology_vector_filter_fallback";
constexpr std::string_view kTopologyVectorFilterMatched = "topology_vector_filter_matched";
constexpr std::string_view kTopologyVectorFilterRemoved = "topology_vector_filter_removed";
constexpr std::string_view kTopologyVectorPartitionAnnApplied =
    "topology_vector_partition_ann_applied";
constexpr std::string_view kTopologyVectorPartitionAnnFallback =
    "topology_vector_partition_ann_fallback";

constexpr std::string_view kTimingTopologyWeakQuery = "topology_weak_query";
constexpr std::string_view kTimingTopologyLoad = "topology_load";
constexpr std::string_view kTimingTopologyValidate = "topology_validate";
constexpr std::string_view kTimingTopologyRequestPrep = "topology_request_prep";
constexpr std::string_view kTimingTopologyRoute = "topology_route";
constexpr std::string_view kTimingTopologyClusterLookup = "topology_cluster_lookup";
constexpr std::string_view kTimingTopologyDocLookup = "topology_doc_lookup";
constexpr std::string_view kTimingTopologyCandidateInsert = "topology_candidate_insert";
constexpr std::string_view kTopologySidecarVectorCandidates = "topology_sidecar_vector_candidates";
constexpr std::string_view kTopologySidecarVectorDocIds = "topology_sidecar_vector_doc_ids";
constexpr std::string_view kTopologySidecarPostFusionCount = "topology_sidecar_post_fusion_count";
constexpr std::string_view kTopologySidecarFinalCount = "topology_sidecar_final_count";
constexpr std::string_view kTopologyNewPostFusionCount = "topology_new_post_fusion_count";
constexpr std::string_view kTopologyDuplicatePostFusionCount =
    "topology_duplicate_post_fusion_count";
constexpr std::string_view kTopologySidecarPostFusionDocIds =
    "topology_sidecar_post_fusion_doc_ids";
constexpr std::string_view kTopologySidecarFinalDocIds = "topology_sidecar_final_doc_ids";
constexpr std::string_view kTopologyNewPostFusionDocIds = "topology_new_post_fusion_doc_ids";
constexpr std::string_view kTopologyDuplicatePostFusionDocIds =
    "topology_duplicate_post_fusion_doc_ids";
constexpr std::string_view kTopologyAddedCandidatePostFusionCount =
    "topology_added_candidate_post_fusion_count";
constexpr std::string_view kTopologyAddedCandidateFusionDroppedCount =
    "topology_added_candidate_fusion_dropped_count";
constexpr std::string_view kTopologyFusionDroppedDocIds = "topology_fusion_dropped_doc_ids";

constexpr std::string_view kTopologyRouteBestScore = "topology_route_best_score";
constexpr std::string_view kTopologyRouteMeanAcceptedScore = "topology_route_mean_accepted_score";
constexpr std::string_view kTopologyRouteAcceptedCount = "topology_route_accepted_count";
constexpr std::string_view kTopologySeedCount = "topology_seed_count";
constexpr std::string_view kTopologySeedCoverageCount = "topology_seed_coverage_count";

} // namespace yams::search::metrics
