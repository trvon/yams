#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/search/search_engine.h>
#include <yams/search/search_metric_keys.h>
#include <yams/search/search_tracing.h>

using yams::search::ComponentResult;
using yams::search::SearchEngineConfig;
using yams::search::SearchTraceCollector;

namespace {

ComponentResult makeComponent(std::string path, float score, ComponentResult::Source source,
                              size_t rank) {
    ComponentResult c;
    c.filePath = std::move(path);
    c.documentHash = c.filePath;
    c.score = score;
    c.source = source;
    c.rank = rank;
    return c;
}

} // namespace

TEST_CASE("buildComponentHitSummaryJson preserves ranked top hit details",
          "[search][tracing][catch2]") {
    std::vector<ComponentResult> componentResults;
    componentResults.push_back(
        makeComponent("/tmp/31715818.txt", 0.91f, ComponentResult::Source::Vector, 150));
    componentResults.push_back(
        makeComponent("/tmp/31715818.txt", 0.89f, ComponentResult::Source::Vector, 151));
    componentResults.push_back(
        makeComponent("/tmp/11111111.txt", 0.95f, ComponentResult::Source::Vector, 5));
    componentResults.push_back(
        makeComponent("/tmp/22222222.txt", 0.72f, ComponentResult::Source::Text, 12));

    const auto summary = yams::search::buildComponentHitSummaryJson(componentResults, 3);

    REQUIRE(summary.contains("vector"));
    const auto& vectorSummary = summary.at("vector");
    REQUIRE(vectorSummary.contains("unique_top_doc_ids"));
    REQUIRE(vectorSummary.contains("top_hits"));

    const auto& vectorIds = vectorSummary.at("unique_top_doc_ids");
    REQUIRE(vectorIds.is_array());
    REQUIRE(vectorIds.size() == 2);
    CHECK(vectorIds.at(0).get<std::string>() == "11111111");
    CHECK(vectorIds.at(1).get<std::string>() == "31715818");

    const auto& topHits = vectorSummary.at("top_hits");
    REQUIRE(topHits.is_array());
    REQUIRE(topHits.size() == 2);
    CHECK(topHits.at(0).at("doc_id").get<std::string>() == "11111111");
    CHECK(topHits.at(0).at("rank").get<size_t>() == 6);
    CHECK(topHits.at(1).at("doc_id").get<std::string>() == "31715818");
    CHECK(topHits.at(1).at("rank").get<size_t>() == 151);
    CHECK(topHits.at(1).at("score").get<double>() == Catch::Approx(0.91));

    REQUIRE(summary.contains("text"));
    const auto& textHits = summary.at("text").at("top_hits");
    REQUIRE(textHits.is_array());
    REQUIRE(textHits.size() == 1);
    CHECK(textHits.at(0).at("doc_id").get<std::string>() == "22222222");
    CHECK(textHits.at(0).at("rank").get<size_t>() == 13);
}

TEST_CASE("SearchTraceCollector records per-surface counters under stage[\"counters\"]",
          "[search][tracing][catch2]") {
    SearchEngineConfig config;
    SearchTraceCollector collector(config);

    collector.markStageConfigured("vector", true);
    collector.markStageAttempted("vector");
    collector.recordStageCounter("vector", "backend_raw_return", 300);
    collector.recordStageCounter("vector", "post_threshold", 86);
    collector.recordStageCounter("vector", "should_narrow_applied", 1);
    collector.recordStageCounter("vector", "post_threshold", 92);

    const auto summary = collector.buildStageSummaryJson();
    REQUIRE(summary.contains("vector"));
    const auto& vectorStage = summary.at("vector");
    REQUIRE(vectorStage.contains("counters"));
    const auto& counters = vectorStage.at("counters");
    REQUIRE(counters.is_object());
    CHECK(counters.at("backend_raw_return").get<std::int64_t>() == 300);
    CHECK(counters.at("post_threshold").get<std::int64_t>() == 92);
    CHECK(counters.at("should_narrow_applied").get<std::int64_t>() == 1);

    REQUIRE(summary.contains("vector"));
    CHECK(summary.at("vector").at("attempted").get<bool>() == true);
}

TEST_CASE("SearchTraceCollector counters are per-stage isolated", "[search][tracing][catch2]") {
    SearchEngineConfig config;
    SearchTraceCollector collector(config);

    collector.recordStageCounter("vector", "shouldSkipSemantic", 0);
    collector.recordStageCounter("text", "shouldSkipSemantic", 1);

    const auto summary = collector.buildStageSummaryJson();
    REQUIRE(summary.at("vector").at("counters").at("shouldSkipSemantic").get<std::int64_t>() == 0);
    REQUIRE(summary.at("text").at("counters").at("shouldSkipSemantic").get<std::int64_t>() == 1);
}

TEST_CASE("SearchTraceCollector emits empty counters object when none recorded",
          "[search][tracing][catch2]") {
    SearchEngineConfig config;
    SearchTraceCollector collector(config);

    collector.markStageConfigured("embedding", true);
    collector.markStageAttempted("embedding");

    const auto summary = collector.buildStageSummaryJson();
    REQUIRE(summary.at("embedding").contains("counters"));
    CHECK(summary.at("embedding").at("counters").is_object());
    CHECK(summary.at("embedding").at("counters").empty());
}

TEST_CASE("recordTopologyRoutingDebug emits the legacy topology key set",
          "[search][tracing][topology][catch2]") {
    SearchEngineConfig config;
    config.topologyRouteScoringMode = SearchEngineConfig::TopologyRouteScoringMode::SizeWeighted;
    config.topologySparseDenseAlpha = 0.25f;
    config.topologyMinRouteScore = 0.5f;
    config.topologyMedoidOnlyExpansion = true;

    yams::search::TopologyRoutingSessionResult session;
    session.loadAttempted = true;
    session.loadSucceeded = true;
    session.artifactAdmitted = true;
    session.applied = true;
    session.narrowApplied = false;
    session.artifactsFresh = true;
    session.topologyEpoch = 7;
    session.routedClusters = 2;
    session.availableRoutes = 3;
    session.routedDocs = 5;
    session.routesRejected = 1;
    session.addedCandidates = 4;
    session.duplicateCandidates = 3;
    session.staleCandidates = 1;
    session.routeBoundaryScoreMargin = 0.17F;
    session.confidenceAbstained = true;
    session.addedCandidateHashes = {"hash-a", "hash-b"};
    session.routedCandidateHashes = {"hash-c", "hash-a"};
    session.timings.totalMicros = 100;
    session.timings.loadMicros = 10;
    session.timings.validateMicros = 20;
    session.timings.requestPrepMicros = 30;
    session.timings.routeMicros = 40;
    session.timings.clusterLookupMicros = 50;
    session.timings.docLookupMicros = 60;
    session.timings.candidateInsertMicros = 70;

    yams::search::SearchResponse response;
    yams::search::recordTopologyRoutingDebug(response, config,
                                             SearchEngineConfig::TopologyRoutingMode::HybridAssist,
                                             session, "skip-reason", 42);

    const auto& debug = response.debugStats;
    CHECK(debug.at("topology_routing_mode") == "hybrid_assist");
    CHECK(debug.at("topology_route_scoring_mode") == "size_weighted");
    CHECK(debug.at("topology_sparse_dense_alpha") == std::to_string(0.25f));
    CHECK(debug.at("topology_min_route_score") == std::to_string(0.5f));
    CHECK(debug.at("topology_medoid_only_expansion") == "1");
    CHECK(debug.at("topology_weak_query_enabled") == "1");
    CHECK(debug.at("topology_weak_query_load_attempted") == "1");
    CHECK(debug.at("topology_weak_query_load_succeeded") == "1");
    CHECK(debug.at("topology_artifact_admitted") == "1");
    CHECK(debug.at("topology_weak_query_applied") == "1");
    CHECK(debug.at("topology_weak_query_narrow_applied") == "0");
    CHECK(debug.at("topology_weak_query_skip_reason") == "skip-reason");
    CHECK(debug.at("topology_weak_query_routes_rejected") == "1");
    CHECK(debug.at("topology_weak_query_routed_clusters") == "2");
    CHECK(debug.at("topology_route_available_count") == "3");
    CHECK(debug.at("topology_route_boundary_score_margin") == std::to_string(0.17F));
    CHECK(debug.at("topology_route_confidence_abstained") == "1");
    CHECK(debug.at("topology_weak_query_routed_docs") == "5");
    CHECK(debug.at("topology_weak_query_added_candidates") == "4");
    CHECK(debug.at("topology_weak_query_duplicate_candidates") == "3");
    CHECK(debug.at("topology_weak_query_stale_candidates") == "1");
    CHECK(debug.at("topology_weak_query_added_candidate_hashes") == "hash-a\thash-b");
    CHECK(debug.at("topology_weak_query_allowed_candidate_hashes") == "hash-a\thash-c");
    CHECK(debug.at("topology_weak_query_total_candidates") == "42");
    CHECK(debug.at("topology_ready") == "1");
    CHECK(debug.at("topology_artifacts_fresh") == "1");
    CHECK(debug.at("topology_epoch") == "7");

    const auto& timing = response.componentTimingMicros;
    CHECK(timing.at("topology_weak_query") == 100);
    CHECK(timing.at("topology_load") == 10);
    CHECK(timing.at("topology_validate") == 20);
    CHECK(timing.at("topology_request_prep") == 30);
    CHECK(timing.at("topology_route") == 40);
    CHECK(timing.at("topology_cluster_lookup") == 50);
    CHECK(timing.at("topology_doc_lookup") == 60);
    CHECK(timing.at("topology_candidate_insert") == 70);
}

TEST_CASE("recordTopologyRoutingDebug omits readiness and timing when load not attempted",
          "[search][tracing][topology][catch2]") {
    SearchEngineConfig config;
    yams::search::TopologyRoutingSessionResult session;

    yams::search::SearchResponse response;
    yams::search::recordTopologyRoutingDebug(response, config,
                                             SearchEngineConfig::TopologyRoutingMode::Disabled,
                                             session, "disabled", 0);

    const auto& debug = response.debugStats;
    CHECK(debug.at("topology_weak_query_enabled") == "0");
    CHECK(debug.at("topology_weak_query_load_attempted") == "0");
    CHECK(debug.at("topology_artifact_admitted") == "0");
    CHECK(debug.at("topology_weak_query_skip_reason") == "disabled");
    CHECK_FALSE(debug.contains("topology_ready"));
    CHECK_FALSE(debug.contains("topology_epoch"));
    CHECK(response.componentTimingMicros.empty());
}

TEST_CASE("recordTopologySidecarSurvivalDebug matches legacy sidecar keys",
          "[search][tracing][topology][catch2]") {
    yams::search::TopologySidecarSurvival survival;
    survival.postFusionDocIds = {"a", "b"};
    survival.finalDocIds = {"a"};
    survival.newPostFusionDocIds = {"b"};
    survival.duplicatePostFusionDocIds = {"a"};

    std::unordered_map<std::string, std::string> debug;
    yams::search::recordTopologySidecarSurvivalDebug(debug, survival);

    CHECK(debug.at("topology_sidecar_post_fusion_count") == "2");
    CHECK(debug.at("topology_sidecar_final_count") == "1");
    CHECK(debug.at("topology_new_post_fusion_count") == "1");
    CHECK(debug.at("topology_duplicate_post_fusion_count") == "1");
    CHECK(debug.at("topology_sidecar_post_fusion_doc_ids") == "a\tb");
    CHECK(debug.at("topology_sidecar_final_doc_ids") == "a");
    CHECK(debug.at("topology_new_post_fusion_doc_ids") == "b");
    CHECK(debug.at("topology_duplicate_post_fusion_doc_ids") == "a");
    CHECK(debug.size() == 8);
}

TEST_CASE("recordIndexReadinessDebug emits readiness flags for both pipelines",
          "[search][tracing][catch2]") {
    yams::search::IndexFreshnessSnapshot freshness;
    freshness.lexicalReady = true;
    freshness.vectorReady = false;
    freshness.kgReady = true;
    freshness.topologyReady = false;
    freshness.topologyArtifactsFresh = true;
    freshness.topologyEpoch = 12;

    std::unordered_map<std::string, std::string> debug;
    yams::search::recordIndexReadinessDebug(debug, freshness);

    CHECK(debug.at("search_engine_ready") == "1");
    CHECK(debug.at("vector_ready") == "0");
    CHECK(debug.at("kg_ready") == "1");
    CHECK(debug.at("topology_ready") == "0");
    CHECK(debug.at("topology_artifacts_fresh") == "1");
    CHECK(debug.at("topology_epoch") == "12");
    CHECK(debug.size() == 6);
}

TEST_CASE("metric key constants are stable strings", "[search][tracing][catch2]") {
    namespace metrics = yams::search::metrics;
    CHECK(metrics::kSearchEngineVariant == "search_engine_variant");
    CHECK(metrics::kSearchPipelineVariant == "search_pipeline_variant");
    CHECK(metrics::kSearchPipelineInterface == "search_pipeline_interface");
    CHECK(metrics::kSearchPipelineName == "search_pipeline_name");
    CHECK(metrics::kTopologyArtifactAdmitted == "topology_artifact_admitted");
    CHECK(metrics::kTopologyWeakQueryApplied == "topology_weak_query_applied");
    CHECK(metrics::kTopologyWeakQueryAddedCandidates == "topology_weak_query_added_candidates");
    CHECK(metrics::kTopologyWeakQueryDuplicateCandidates ==
          "topology_weak_query_duplicate_candidates");
    CHECK(metrics::kTopologySidecarVectorCandidates == "topology_sidecar_vector_candidates");
    CHECK(metrics::kTopologySidecarFinalCount == "topology_sidecar_final_count");
    CHECK(metrics::kTopologyNewPostFusionCount == "topology_new_post_fusion_count");
    CHECK(metrics::kTopologyDuplicatePostFusionCount == "topology_duplicate_post_fusion_count");
    CHECK(metrics::kTopologyRouteBestScore == "topology_route_best_score");
    CHECK(metrics::kTopologyRouteMeanAcceptedScore == "topology_route_mean_accepted_score");
    CHECK(metrics::kTopologyRouteAcceptedCount == "topology_route_accepted_count");
    CHECK(metrics::kTopologySeedCount == "topology_seed_count");
    CHECK(metrics::kTopologySeedCoverageCount == "topology_seed_coverage_count");
}
