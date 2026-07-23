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
constexpr std::string_view kTopologyRouteAnnUsed = "topology_route_ann_used";
constexpr std::string_view kTopologyRouteAnnCandidates = "topology_route_ann_candidates";
constexpr std::string_view kTopologyRouteAnnDistanceEvaluations =
    "topology_route_ann_distance_evaluations";
constexpr std::string_view kTopologyRouteExactRepresentativeDistanceEvaluations =
    "topology_route_exact_representative_distance_evaluations";
constexpr std::string_view kTopologyRouteEvidenceCount = "topology_route_evidence_count";
constexpr std::string_view kTopologyRouteEvidenceRows = "topology_route_evidence_rows";
constexpr std::string_view kTopologyRouteNarrowProposed = "topology_route_narrow_proposed";
constexpr std::string_view kTopologyRouteAction = "topology_route_action";
constexpr std::string_view kTopologyRouteSelectedCoverCount = "topology_route_selected_cover_count";
constexpr std::string_view kTopologyRouteSelectedProtectedRelationFiberCount =
    "topology_route_selected_protected_relation_fiber_count";
constexpr std::string_view kTopologyRouteAdmissionEligible = "topology_route_admission_eligible";
constexpr std::string_view kTopologyRouteAdmissionDenialReason =
    "topology_route_admission_denial_reason";
constexpr std::string_view kTopologyRouteCoordinateSpaceIdentity =
    "topology_route_coordinate_space_identity";
constexpr std::string_view kTopologyRouteCoordinateSpaceAlignmentStatus =
    "topology_route_coordinate_space_alignment_status";
constexpr std::string_view kTopologyRouteProtectedRelationCoverageStatus =
    "topology_route_protected_relation_coverage_status";
constexpr std::string_view kTopologyRouteProtectedFibersRepresentedStatus =
    "topology_route_protected_fibers_represented_status";
constexpr std::string_view kTopologyRouteCertificateSaturatesProtectedFibersStatus =
    "topology_route_certificate_saturates_protected_fibers_status";
constexpr std::string_view kTopologyRouteRiskStatus = "topology_route_risk_status";
constexpr std::string_view kTopologyRouteWorkStatus = "topology_route_work_status";
constexpr std::string_view kTopologyRouteWorkObserved = "topology_route_work_observed";
constexpr std::string_view kTopologyRouteWorkRowsVisitedActual =
    "topology_route_work_rows_visited_actual";
constexpr std::string_view kTopologyRouteWorkExactDistanceEvaluationsActual =
    "topology_route_work_exact_distance_evaluations_actual";
constexpr std::string_view kTopologyRouteWorkAnnCandidateBudgetActual =
    "topology_route_work_ann_candidate_budget_actual";
constexpr std::string_view kTopologyRouteCoverMaterializationStatus =
    "topology_route_cover_materialization_status";
constexpr std::string_view kTopologyRouteSelectedCoverDocuments =
    "topology_route_selected_cover_documents";
constexpr std::string_view kTopologyRouteMaterializedCoverDocuments =
    "topology_route_materialized_cover_documents";
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
constexpr std::string_view kTopologyWeakQueryAllowedCandidateDocIds =
    "topology_weak_query_allowed_candidate_doc_ids";
constexpr std::string_view kTopologyGraphTraceCollected = "topology_graph_trace_collected";
constexpr std::string_view kTopologyGraphEdgeFetchLimit = "topology_graph_edge_fetch_limit";
constexpr std::string_view kTopologyGraphFetchTruncatedSeedCount =
    "topology_graph_fetch_truncated_seed_count";
constexpr std::string_view kTopologyGraphSeedDocIds = "topology_graph_seed_doc_ids";
constexpr std::string_view kTopologyGraphSeedUnresolvedCount =
    "topology_graph_seed_unresolved_count";
constexpr std::string_view kTopologyGraphRelationCandidateCount =
    "topology_graph_relation_candidate_count";
constexpr std::string_view kTopologyGraphRelationCandidateDocIds =
    "topology_graph_relation_candidate_doc_ids";
constexpr std::string_view kTopologyGraphRelationUnresolvedCount =
    "topology_graph_relation_unresolved_count";
constexpr std::string_view kTopologyGraphFetchedCandidateCount =
    "topology_graph_fetched_candidate_count";
constexpr std::string_view kTopologyGraphFetchedCandidateDocIds =
    "topology_graph_fetched_candidate_doc_ids";
constexpr std::string_view kTopologyGraphFetchedUnresolvedCount =
    "topology_graph_fetched_unresolved_count";
constexpr std::string_view kTopologyGraphEligibleCandidateCount =
    "topology_graph_eligible_candidate_count";
constexpr std::string_view kTopologyGraphEligibleCandidateDocIds =
    "topology_graph_eligible_candidate_doc_ids";
constexpr std::string_view kTopologyGraphEligibleUnresolvedCount =
    "topology_graph_eligible_unresolved_count";
constexpr std::string_view kTopologyCandidateRescueAttempted =
    "topology_candidate_rescue_attempted";
constexpr std::string_view kTopologyCandidateRescueApplied = "topology_candidate_rescue_applied";
constexpr std::string_view kTopologyCandidateRescueAddedCandidates =
    "topology_candidate_rescue_added_candidates";
constexpr std::string_view kTopologyCandidateRescueNovelCandidates =
    "topology_candidate_rescue_novel_candidates";
constexpr std::string_view kTopologyCandidateRescueEvidenceRescues =
    "topology_candidate_rescue_evidence_rescues";
constexpr std::string_view kTopologyCandidateRescueDuplicateCandidates =
    "topology_candidate_rescue_duplicate_candidates";
constexpr std::string_view kTopologyCandidateRescueAddedCandidateHashes =
    "topology_candidate_rescue_added_candidate_hashes";
constexpr std::string_view kTopologyCandidateRescueAddedCandidateDocIds =
    "topology_candidate_rescue_added_candidate_doc_ids";
constexpr std::string_view kTopologyCandidateRescueNovelCandidateHashes =
    "topology_candidate_rescue_novel_candidate_hashes";
constexpr std::string_view kTopologyCandidateRescueNovelCandidateDocIds =
    "topology_candidate_rescue_novel_candidate_doc_ids";
constexpr std::string_view kTopologyCandidateRescueEvidenceRescueHashes =
    "topology_candidate_rescue_evidence_rescue_hashes";
constexpr std::string_view kTopologyCandidateRescueEvidenceRescueDocIds =
    "topology_candidate_rescue_evidence_rescue_doc_ids";
constexpr std::string_view kTopologyCandidateRescueScoredCandidateDocIds =
    "topology_candidate_rescue_scored_candidate_doc_ids";
constexpr std::string_view kTopologyCandidateRescueExactScoredCandidateDocIds =
    "topology_candidate_rescue_exact_scored_candidate_doc_ids";
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
constexpr std::string_view kVectorSearchRowsVisitedStatus = "vector_search_rows_visited_status";
constexpr std::string_view kVectorSearchExactDistanceEvaluationsActual =
    "vector_search_exact_distance_evaluations_actual";
constexpr std::string_view kVectorSearchExactDistanceEvaluationsStatus =
    "vector_search_exact_distance_evaluations_status";
constexpr std::string_view kVectorSearchAnnCandidateBudgetActual =
    "vector_search_ann_candidate_budget_actual";
constexpr std::string_view kVectorSearchAnnCandidateBudgetStatus =
    "vector_search_ann_candidate_budget_status";
constexpr std::string_view kVectorSearchCandidateLookupCount =
    "vector_search_candidate_lookup_count";
constexpr std::string_view kVectorSearchCandidateIndexCacheUsed =
    "vector_search_candidate_index_cache_used";
constexpr std::string_view kVectorSearchCandidateIndexPayloadBytes =
    "vector_search_candidate_index_payload_bytes";
constexpr std::string_view kVectorSearchMaterializedRows = "vector_search_materialized_rows";
constexpr std::string_view kVectorSearchCandidateLookupNs = "vector_search_candidate_lookup_ns";
constexpr std::string_view kVectorSearchCandidateProjectionNs =
    "vector_search_candidate_projection_ns";
constexpr std::string_view kVectorSearchPqLutNs = "vector_search_pq_lut_ns";
constexpr std::string_view kVectorSearchAdcScoringNs = "vector_search_adc_scoring_ns";
constexpr std::string_view kVectorSearchTopKSelectionNs = "vector_search_topk_selection_ns";
constexpr std::string_view kVectorSearchResultMaterializationNs =
    "vector_search_result_materialization_ns";
constexpr std::string_view kVectorSearchExactRerankNs = "vector_search_exact_rerank_ns";
constexpr std::string_view kTopologyVectorFilterApplied = "topology_vector_filter_applied";
constexpr std::string_view kTopologyVectorFilterFallback = "topology_vector_filter_fallback";
constexpr std::string_view kTopologyVectorFilterMatched = "topology_vector_filter_matched";
constexpr std::string_view kTopologyVectorFilterRemoved = "topology_vector_filter_removed";
constexpr std::string_view kTopologyVectorAllowedSetAnnApplied =
    "topology_vector_allowed_set_ann_applied";
constexpr std::string_view kTopologyVectorAllowedSetAnnFallback =
    "topology_vector_allowed_set_ann_fallback";
constexpr std::string_view kTopologyVectorGlobalFillCount = "topology_vector_global_fill_count";
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
