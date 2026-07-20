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

TEST_CASE("SearchTraceCollector only retains document identifiers for explicit traces",
          "[search][tracing][catch2]") {
    SearchEngineConfig config;
    const std::vector<ComponentResult> results{
        makeComponent("/tmp/b.txt", 0.8F, ComponentResult::Source::Vector, 0),
        makeComponent("/tmp/a.txt", 0.7F, ComponentResult::Source::Vector, 1),
    };

    SearchTraceCollector compactCollector(config, false);
    compactCollector.markStageResult("vector", results, 10, true);
    const auto compact = compactCollector.buildStageSummaryJson().at("vector");
    CHECK(compact.at("unique_doc_count").get<std::size_t>() == 2);
    CHECK(compact.at("unique_doc_ids").empty());

    SearchTraceCollector tracedCollector(config, true);
    tracedCollector.markStageResult("vector", results, 10, true);
    const auto traced = tracedCollector.buildStageSummaryJson().at("vector");
    CHECK(traced.at("unique_doc_count").get<std::size_t>() == 2);
    CHECK(traced.at("unique_doc_ids") == nlohmann::json::array({"a", "b"}));
}

TEST_CASE("SearchTraceCollector records non-document stage outcomes without fake hits",
          "[search][tracing][catch2]") {
    SearchEngineConfig config;
    SearchTraceCollector collector(config);

    collector.markValueStageResult("embedding", true, 2500);
    collector.markValueStageResult("concepts", false, 1000);

    const auto summary = collector.buildStageSummaryJson();
    const auto& embedding = summary.at("embedding");
    CHECK(embedding.at("attempted").get<bool>());
    CHECK(embedding.at("contributed").get<bool>());
    CHECK(embedding.at("raw_hit_count").get<std::size_t>() == 1);
    CHECK(embedding.at("unique_doc_count").get<std::size_t>() == 0);
    CHECK(embedding.at("score_stats_valid").get<bool>());
    CHECK(embedding.at("min_score").get<double>() == Catch::Approx(1.0));
    CHECK(embedding.at("max_score").get<double>() == Catch::Approx(1.0));
    CHECK(embedding.at("duration_ms").get<double>() == Catch::Approx(2.5));

    const auto& concepts = summary.at("concepts");
    CHECK(concepts.at("attempted").get<bool>());
    CHECK_FALSE(concepts.at("contributed").get<bool>());
    CHECK(concepts.at("raw_hit_count").get<std::size_t>() == 0);
    CHECK_FALSE(concepts.at("score_stats_valid").get<bool>());
}

TEST_CASE("recordTopologyRoutingDebug emits the legacy topology key set",
          "[search][tracing][topology][catch2]") {
    SearchEngineConfig config;
    config.topologyRouteScoringMode = SearchEngineConfig::TopologyRouteScoringMode::SizeWeighted;
    config.topologySparseDenseAlpha = 0.25f;
    config.topologyMinRouteScore = 0.5f;

    yams::search::TopologyRoutingSessionResult session;
    session.loadAttempted = true;
    session.loadSucceeded = true;
    session.artifactAdmitted = true;
    session.applied = true;
    session.narrowApplied = false;
    session.artifactsFresh = true;
    session.topologyEpoch = 7;
    session.constructionFingerprint = "0123456789abcdef";
    session.routedClusters = 2;
    session.availableRoutes = 3;
    session.routedDocs = 5;
    session.routesRejected = 1;
    session.addedCandidates = 4;
    session.duplicateCandidates = 3;
    session.staleCandidates = 1;
    session.routeBoundaryScoreMargin = 0.17F;
    session.confidenceAbstained = true;
    session.routeRepresentativeDistanceEvaluations = 7;
    session.routeRepresentativeCountMax = 4;
    session.routeAnnUsed = true;
    session.routeAnnCandidates = 6;
    session.routeAnnDistanceEvaluations = 4;
    session.routeExactRepresentativeDistanceEvaluations = 3;
    session.routeCoordinateEvidence = {
        yams::search::TopologyRouteCoordinateEvidence{
            .clusterId = "cluster-a",
            .semanticCost = 0.1F,
            .sparseCost = 0.2F,
            .distortionPenalty = 0.15F,
            .localIntrinsicDimension = 2.5F,
            .uncertaintyPenalty = 0.25F,
            .persistencePenalty = 0.3F,
            .cohesionPenalty = 0.4F,
            .sizePenalty = 0.5F,
            .routeScore = 0.9F,
            .scoreEligible = true,
            .inSelectedPrefix = true,
        },
        yams::search::TopologyRouteCoordinateEvidence{
            .clusterId = "cluster-b",
            .sparseCost = 0.8F,
            .persistencePenalty = 0.7F,
            .cohesionPenalty = 0.6F,
            .sizePenalty = 0.5F,
            .routeScore = 0.3F,
            .scoreEligible = false,
            .inSelectedPrefix = false,
        },
    };
    session.addedCandidateHashes = {"hash-a", "hash-b"};
    session.routedCandidateHashes = {"hash-c", "hash-a"};
    session.candidateStructureEvidence.emplace("hash-a",
                                               yams::search::TopologyCandidateStructureEvidence{
                                                   .scaleAgreement = 0.8F,
                                                   .overlapSupport = 0.4F,
                                                   .persistenceSupport = 0.6F,
                                                   .cohesionSupport = 0.9F,
                                                   .bridgeSupport = 0.3F,
                                                   .densitySupport = 0.7F,
                                               });
    session.candidateStructureEvidence.emplace("hash-c",
                                               yams::search::TopologyCandidateStructureEvidence{
                                                   .scaleAgreement = 0.2F,
                                                   .overlapSupport = 0.2F,
                                                   .persistenceSupport = 0.8F,
                                                   .cohesionSupport = 0.3F,
                                                   .bridgeSupport = 0.2F,
                                                   .densitySupport = 0.6F,
                                               });
    session.timings.totalMicros = 100;
    session.timings.loadMicros = 10;
    session.timings.validateMicros = 20;
    session.timings.requestPrepMicros = 30;
    session.timings.routeMicros = 40;
    session.timings.clusterLookupMicros = 50;
    session.timings.docLookupMicros = 60;
    session.timings.candidateInsertMicros = 70;

    yams::search::SearchResponse response;
    yams::search::recordTopologyRoutingDebug(
        response, config, SearchEngineConfig::TopologyRoutingMode::HybridAssist, session,
        "skip-reason", 42, yams::search::TopologyRoutingDebugOptions{.includeDetailedTrace = true});

    const auto& debug = response.debugStats;
    CHECK(debug.at("topology_routing_mode") == "hybrid_assist");
    CHECK(debug.at("topology_route_scoring_mode") == "size_weighted");
    CHECK(debug.at("topology_sparse_dense_alpha") == std::to_string(0.25f));
    CHECK(debug.at("topology_min_route_score") == std::to_string(0.5f));
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
    CHECK(debug.at("topology_route_representative_distance_evaluations") == "7");
    CHECK(debug.at("topology_route_representative_count_max") == "4");
    CHECK(debug.at("topology_route_ann_used") == "1");
    CHECK(debug.at("topology_route_ann_candidates") == "6");
    CHECK(debug.at("topology_route_ann_distance_evaluations") == "4");
    CHECK(debug.at("topology_route_exact_representative_distance_evaluations") == "3");
    CHECK(debug.at("topology_route_coordinate_count") == "2");
    const auto coordinateRows = nlohmann::json::parse(debug.at("topology_route_coordinate_rows"));
    REQUIRE(coordinateRows.size() == 2);
    CHECK(coordinateRows.at(0).at("cluster_id") == "cluster-a");
    CHECK(coordinateRows.at(0).at("semantic_cost").get<float>() == Catch::Approx(0.1F));
    CHECK(coordinateRows.at(0).at("in_selected_prefix") == true);
    CHECK(coordinateRows.at(0).at("graph_cost").is_null());
    CHECK(coordinateRows.at(0).at("distortion_penalty").get<float>() == Catch::Approx(0.15F));
    CHECK(coordinateRows.at(0).at("local_intrinsic_dimension").get<float>() == Catch::Approx(2.5F));
    CHECK(coordinateRows.at(0).at("uncertainty_penalty").get<float>() == Catch::Approx(0.25F));
    CHECK(coordinateRows.at(1).at("semantic_cost").is_null());
    CHECK(coordinateRows.at(1).at("distortion_penalty").is_null());
    CHECK(coordinateRows.at(1).at("local_intrinsic_dimension").is_null());
    CHECK(coordinateRows.at(1).at("uncertainty_penalty").is_null());
    CHECK(coordinateRows.at(1).at("score_eligible") == false);
    CHECK(coordinateRows.at(1).at("in_selected_prefix") == false);
    CHECK(debug.at("topology_weak_query_routed_docs") == "5");
    CHECK(debug.at("topology_weak_query_added_candidates") == "4");
    CHECK(debug.at("topology_weak_query_duplicate_candidates") == "3");
    CHECK(debug.at("topology_weak_query_stale_candidates") == "1");
    CHECK(debug.at("topology_weak_query_added_candidate_hashes") == "hash-a\thash-b");
    CHECK(debug.at("topology_weak_query_allowed_candidate_hashes") == "hash-a\thash-c");
    CHECK(debug.at("topology_weak_query_total_candidates") == "42");
    CHECK(debug.at("topology_structure_candidate_count") == "2");
    CHECK(std::stof(debug.at("topology_structure_scale_agreement_mean")) == Catch::Approx(0.5F));
    CHECK(std::stof(debug.at("topology_structure_overlap_support_mean")) == Catch::Approx(0.3F));
    CHECK(std::stof(debug.at("topology_structure_persistence_support_mean")) ==
          Catch::Approx(0.7F));
    CHECK(std::stof(debug.at("topology_structure_cohesion_support_mean")) == Catch::Approx(0.6F));
    CHECK(std::stof(debug.at("topology_structure_bridge_support_mean")) == Catch::Approx(0.25F));
    CHECK(std::stof(debug.at("topology_structure_density_support_mean")) == Catch::Approx(0.65F));
    CHECK(debug.at("topology_ready") == "1");
    CHECK(debug.at("topology_artifacts_fresh") == "1");
    CHECK(debug.at("topology_epoch") == "7");
    CHECK(debug.at("topology_construction_fingerprint") == "0123456789abcdef");

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
    CHECK_FALSE(debug.contains("topology_weak_query_added_candidate_hashes"));
    CHECK_FALSE(debug.contains("topology_weak_query_allowed_candidate_hashes"));
    CHECK(response.componentTimingMicros.empty());
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
    CHECK(metrics::kSearchPipelineName == "search_pipeline_name");
    CHECK(metrics::kTopologyArtifactAdmitted == "topology_artifact_admitted");
    CHECK(metrics::kTopologyWeakQueryApplied == "topology_weak_query_applied");
    CHECK(metrics::kTopologyWeakQueryAddedCandidates == "topology_weak_query_added_candidates");
    CHECK(metrics::kTopologyWeakQueryDuplicateCandidates ==
          "topology_weak_query_duplicate_candidates");
    CHECK(metrics::kTopologyRouteBestScore == "topology_route_best_score");
    CHECK(metrics::kTopologyRouteMeanAcceptedScore == "topology_route_mean_accepted_score");
    CHECK(metrics::kTopologyRouteAcceptedCount == "topology_route_accepted_count");
    CHECK(metrics::kTopologyRouteCoordinateCount == "topology_route_coordinate_count");
    CHECK(metrics::kTopologyRouteCoordinateRows == "topology_route_coordinate_rows");
    CHECK(metrics::kTopologySeedCount == "topology_seed_count");
    CHECK(metrics::kTopologySeedCoverageCount == "topology_seed_coverage_count");
}
