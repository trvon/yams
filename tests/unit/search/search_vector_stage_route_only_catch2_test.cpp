// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

// RouteOnly cells of the vector-stage matrix: TopologyVectorPolicy::RouteOnly x backend
// (SimeonPqAdc, ExactScan, Vec0L2) x evaluation gate (allowUncertifiedRouteOnly on/off), plus a
// certified route and an executor-backed run. Kept apart from
// search_vector_stage_characterization_catch2_test.cpp so the Shadow / Augment / Narrow goldens
// there stay byte-identical.
//
// Contract pinned here:
//   * Gate open: the stage runs only the routed candidate queries (DocumentTopK on Simeon PQ,
//     ExactDocumentComplete on ExactScan) with no global fill. Results come solely from the routed
//     set, routed diagnostics are reported as vector search cost and the routed work is applied to
//     the certificate. vector_candidate_source_effective=route_only.
//   * Gate open on Vec0: the backend supports no routed candidates, so the global leg is returned
//     unchanged and vector_candidate_source_effective=unavailable.
//   * Gate closed: the stage behaves exactly like Shadow and records
//     topology_vector_policy_downgraded=uncertified; topology_vector_policy still reports the
//     configured route_only.
//   * topology_route_certified reports whether the certificate admits a hard route after the
//     observed work is applied. An uncertified route still runs under RouteOnly (that is the point
//     of the gate); a calibrated route reports 1.
//
// The hidden "[.dump]" case prints the gate-open cells in golden form.

#include <nlohmann/json.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

#include <yams/search/search_engine.h>
#include <yams/topology/topology_metadata_store.h>
#include <yams/vector/vector_database.h>

#include "tests/common/test_helpers_catch2.h"
#include "tests/unit/search/topology_search_test_support.h"

using namespace yams;
using namespace yams::search;
using namespace yams::search::topology_test_support;

// NOLINTBEGIN(cppcoreguidelines-avoid-do-while, readability-function-cognitive-complexity)

namespace {

using Policy = SearchEngineConfig::TopologyVectorPolicy;
using Backend = vector::VectorSearchEngine;

constexpr std::array kBackends{Backend::SimeonPqAdc, Backend::ExactScan, Backend::Vec0L2};

std::string_view backendName(Backend backend) {
    switch (backend) {
        case Backend::SimeonPqAdc:
            return "simeon_pq_adc";
        case Backend::ExactScan:
            return "exact_scan";
        case Backend::Vec0L2:
            return "vec0_l2";
    }
    return "unknown";
}

struct StageSnapshot {
    std::vector<std::string> fusedHashes;
    std::map<std::string, std::string> debug; // stage keys, timings excluded
    std::vector<std::string> rawVectorDocIds; // trace only
};

bool isStageDebugKey(std::string_view key) {
    if (key.ends_with("_ns") || key.ends_with("_payload_bytes")) {
        return false;
    }
    constexpr std::array prefixes{
        std::string_view{"topology_vector_"},
        std::string_view{"topology_shadow_"},
        std::string_view{"topology_candidate_rescue_"},
        std::string_view{"topology_route_work_"},
        std::string_view{"topology_route_admission_"},
        std::string_view{"relaxed_vector_"},
        std::string_view{"vector_search_"},
    };
    constexpr std::array exact{
        std::string_view{"topology_route_action"},
        std::string_view{"topology_route_certified"},
        std::string_view{"topology_route_narrow_proposed"},
        std::string_view{"topology_weak_query_narrow_applied"},
        std::string_view{"topology_expansion_output_budget"},
        std::string_view{"vector_candidate_source_effective"},
        std::string_view{"vector_document_refill_attempts"},
    };
    return std::ranges::any_of(prefixes, [&](auto p) { return key.starts_with(p); }) ||
           std::ranges::any_of(exact, [&](auto k) { return key == k; });
}

StageSnapshot capture(const SearchResponse& response) {
    StageSnapshot snapshot;
    snapshot.fusedHashes = resultHashes(response);
    for (const auto& [key, value] : response.debugStats) {
        if (isStageDebugKey(key)) {
            snapshot.debug[key] = value;
        }
    }
    if (const auto hits = response.debugStats.find("trace_component_hits_json");
        hits != response.debugStats.end()) {
        const auto summary = nlohmann::json::parse(hits->second);
        if (summary.contains("vector")) {
            for (const auto& hit : summary.at("vector").at("top_hits")) {
                snapshot.rawVectorDocIds.push_back(hit.at("doc_id").get<std::string>());
            }
        }
    }
    return snapshot;
}

std::string renderDebug(const std::map<std::string, std::string>& debug) {
    std::ostringstream out;
    for (const auto& [key, value] : debug) {
        out << key << '=' << value << '\n';
    }
    return out.str();
}

struct CellOptions {
    Policy policy{Policy::RouteOnly};
    bool gate{false};
    bool certified{false};
    bool stageTrace{true};
};

// Dense-only routing (alpha 0) from the fixed query embedding {0, 1}: the route is always the
// {y1, y2} cluster. Uncertified cells search "unmatched query" (no lexical hits); the certified
// cell searches "omega" so the routed cluster carries the seed hits route admission needs.
std::string cellQuery(const CellOptions& options) {
    return options.certified ? "omega" : "unmatched query";
}

SearchEngineConfig cellConfig(const CellOptions& options, Backend backend,
                              const yams::topology::TopologyArtifactBatch& batch) {
    auto config = topologyRoutingTestConfig(true);
    config.topologyRoutingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    config.topologySparseDenseAlpha = 0.0F;
    config.topologyNarrowMinBoundaryMargin = 0.0F;
    config.similarityThreshold = -1.0F;
    config.enableWeakQueryFanoutBoost = false;
    config.topologyExpansionOutputLimit = 2;
    config.topologyVectorPolicy = options.policy;
    config.allowUncertifiedRouteOnly = options.gate;
    if (options.certified) {
        const bool simeon = backend == Backend::SimeonPqAdc;
        // Certification needs observed route work. The vector pipeline rewrites DocumentTopK to
        // ExactDocumentComplete for every aggregation but MAX (the PQ ANN budget is then never
        // observed, exactly as in the Augment cells), so the certified cell pins MAX to reach the
        // allowed-set ANN path. One vector per document makes the aggregation choice moot.
        config.chunkAggregation = SearchEngineConfig::ChunkAggregation::MAX;
        configureCertifiedTopologyRoute(
            config, batch,
            CertifiedTopologyRouteOptions{.maxRowsVisited = 2,
                                          .maxExactDistanceEvaluations = 2,
                                          .maxAnnCandidates = 2,
                                          .deferDocumentLimitToVectorRanker = simeon});
    } else {
        // Same work budget as the Shadow characterization cell so the gate-closed cells can be
        // compared against Shadow key for key.
        config.topologyRouteWorkBudget.maxRowsVisited = 4;
        config.topologyRouteWorkBudget.maxExactDistanceEvaluations = 4;
        config.topologyRouteWorkBudget.maxAnnCandidates = 4;
    }
    return config;
}

StageSnapshot runCell(const CellOptions& options, Backend backend,
                      std::optional<boost::asio::any_io_executor> executor = std::nullopt) {
    yams::test::ScopedEnvVar trace("YAMS_SEARCH_STAGE_TRACE", options.stageTrace
                                                                  ? std::optional<std::string>("1")
                                                                  : std::nullopt);
    TopologySearchFixture fix{backend};
    seedTopologyDocuments(fix);
    const auto batch = buildTwoClusterTopologyBatch();
    yams::topology::MetadataKgTopologyArtifactStore topologyStore(fix.repo, fix.kgStore);
    REQUIRE(topologyStore.storeBatch(batch).has_value());
    if (backend != Backend::Vec0L2) {
        REQUIRE(fix.vectorDb->buildIndex());
    }
    auto generator = makeFixedGenerator({0.0F, 1.0F});
    const auto config = cellConfig(options, backend, batch);
    auto response =
        runTopologySearch(fix, generator, config, 4, cellQuery(options), std::move(executor));
    REQUIRE(response.has_value());
    return capture(response.value());
}

std::string debugAt(const StageSnapshot& snapshot, std::string_view key) {
    const auto it = snapshot.debug.find(std::string{key});
    return it == snapshot.debug.end() ? std::string{"<missing>"} : it->second;
}

const std::vector<std::string> kRoutedOnly{"y1", "y2"};
const std::vector<std::string> kGlobal{"y1", "y2", "x2", "x1"};

std::string goldenDebug(Backend backend, bool certified);

void checkRouteOnlyCommitted(const StageSnapshot& actual, Backend backend, bool certified) {
    CHECK(actual.fusedHashes == kRoutedOnly);
    CHECK(actual.rawVectorDocIds == kRoutedOnly);
    CHECK(debugAt(actual, "topology_vector_policy") == "route_only");
    CHECK_FALSE(actual.debug.contains("topology_vector_policy_downgraded"));
    CHECK(debugAt(actual, "vector_candidate_source_effective") == "route_only");
    CHECK(debugAt(actual, "topology_route_certified") == (certified ? "1" : "0"));
    CHECK(debugAt(actual, "topology_candidate_rescue_attempted") == "0");
    CHECK(debugAt(actual, "topology_vector_global_fill_count") == "0");
    // Routed work is observed and reported both as route work and as vector search cost.
    CHECK(debugAt(actual, "topology_route_work_rows_visited_actual") == "2");
    CHECK(debugAt(actual, "vector_search_rows_visited_actual") == "2");
    CHECK(renderDebug(actual.debug) == goldenDebug(backend, certified));
}

} // namespace

TEST_CASE("RouteOnly with the gate open returns only the routed set",
          "[search][vector-stage][route-only][catch2]") {
    const TopologyVectorEnvPin vectorEnv;
    for (const auto backend : {Backend::SimeonPqAdc, Backend::ExactScan}) {
        for (const bool stageTrace : {false, true}) {
            DYNAMIC_SECTION(backendName(backend) << (stageTrace ? " / trace" : " / no-trace")) {
                const auto actual = runCell(CellOptions{.policy = Policy::RouteOnly,
                                                        .gate = true,
                                                        .stageTrace = stageTrace},
                                            backend);
                CHECK(actual.fusedHashes == kRoutedOnly);
                CHECK(debugAt(actual, "topology_vector_policy") == "route_only");
                CHECK_FALSE(actual.debug.contains("topology_vector_policy_downgraded"));
                CHECK(debugAt(actual, "vector_candidate_source_effective") == "route_only");
                CHECK(debugAt(actual, "topology_route_certified") == "0");
                if (stageTrace) {
                    checkRouteOnlyCommitted(actual, backend, false);
                } else {
                    CHECK(actual.rawVectorDocIds.empty());
                }
            }
        }
    }
}

TEST_CASE("RouteOnly on Vec0 reports an unavailable candidate source and keeps the global leg",
          "[search][vector-stage][route-only][catch2]") {
    const TopologyVectorEnvPin vectorEnv;
    const auto actual =
        runCell(CellOptions{.policy = Policy::RouteOnly, .gate = true}, Backend::Vec0L2);
    CHECK(actual.fusedHashes == kGlobal);
    CHECK(actual.rawVectorDocIds == kGlobal);
    CHECK(debugAt(actual, "topology_vector_policy") == "route_only");
    CHECK_FALSE(actual.debug.contains("topology_vector_policy_downgraded"));
    CHECK(debugAt(actual, "vector_candidate_source_effective") == "unavailable");
    CHECK(debugAt(actual, "topology_route_certified") == "0");
    CHECK(debugAt(actual, "topology_candidate_rescue_attempted") == "0");
    CHECK(renderDebug(actual.debug) == goldenDebug(Backend::Vec0L2, false));
}

TEST_CASE("RouteOnly with a certified route reports topology_route_certified=1",
          "[search][vector-stage][route-only][catch2]") {
    const TopologyVectorEnvPin vectorEnv;
    const auto actual =
        runCell(CellOptions{.policy = Policy::RouteOnly, .gate = true, .certified = true},
                Backend::SimeonPqAdc);
    CHECK(debugAt(actual, "topology_route_action") == "narrow");
    checkRouteOnlyCommitted(actual, Backend::SimeonPqAdc, true);
}

TEST_CASE("RouteOnly without the gate downgrades to Shadow and says so",
          "[search][vector-stage][route-only][catch2]") {
    const TopologyVectorEnvPin vectorEnv;
    for (const auto backend : kBackends) {
        DYNAMIC_SECTION(backendName(backend)) {
            auto routeOnly =
                runCell(CellOptions{.policy = Policy::RouteOnly, .gate = false}, backend);
            auto shadow = runCell(CellOptions{.policy = Policy::Shadow, .gate = false}, backend);

            CHECK(routeOnly.fusedHashes == kGlobal);
            CHECK(routeOnly.fusedHashes == shadow.fusedHashes);
            CHECK(routeOnly.rawVectorDocIds == shadow.rawVectorDocIds);
            CHECK(debugAt(routeOnly, "topology_vector_policy") == "route_only");
            CHECK(debugAt(routeOnly, "topology_vector_policy_downgraded") == "uncertified");
            CHECK(debugAt(routeOnly, "vector_candidate_source_effective") == "ann");
            CHECK(debugAt(routeOnly, "topology_route_certified") == "0");

            CHECK(debugAt(shadow, "topology_vector_policy") == "shadow");
            CHECK_FALSE(shadow.debug.contains("topology_vector_policy_downgraded"));
            CHECK(debugAt(shadow, "vector_candidate_source_effective") == "ann");

            // Every other stage key matches Shadow byte for byte.
            routeOnly.debug.erase("topology_vector_policy");
            routeOnly.debug.erase("topology_vector_policy_downgraded");
            shadow.debug.erase("topology_vector_policy");
            CHECK(renderDebug(routeOnly.debug) == renderDebug(shadow.debug));
        }
    }
}

TEST_CASE("RouteOnly executor-backed fanout matches the inline cell",
          "[search][vector-stage][route-only][executor][catch2]") {
    const TopologyVectorEnvPin vectorEnv;
    boost::asio::io_context io;
    auto guard = boost::asio::make_work_guard(io);
    std::vector<std::thread> runners;
    for (int i = 0; i < 2; ++i) {
        runners.emplace_back([&io]() { io.run(); });
    }
    {
        const auto actual =
            runCell(CellOptions{.policy = Policy::RouteOnly, .gate = true}, Backend::SimeonPqAdc,
                    std::optional<boost::asio::any_io_executor>{io.get_executor()});
        checkRouteOnlyCommitted(actual, Backend::SimeonPqAdc, false);
    }
    guard.reset();
    io.stop();
    for (auto& runner : runners) {
        runner.join();
    }
}

// Hidden: prints the gate-open cells as golden literals. Run with
//   build/debug/tests/catch2_search_vector_stage_route_only "[dump]"
TEST_CASE("RouteOnly cell dump", "[.][search][vector-stage][route-only][dump]") {
    const TopologyVectorEnvPin vectorEnv;
    for (const auto backend : kBackends) {
        const auto snapshot =
            runCell(CellOptions{.policy = Policy::RouteOnly, .gate = true}, backend);
        std::cout << "// ---- route_only / " << backendName(backend) << " / uncertified ----\n"
                  << "R\"golden(" << renderDebug(snapshot.debug) << ")golden\"\n\n";
    }
    const auto certified =
        runCell(CellOptions{.policy = Policy::RouteOnly, .gate = true, .certified = true},
                Backend::SimeonPqAdc);
    std::cout << "// ---- route_only / simeon_pq_adc / certified ----\n"
              << "R\"golden(" << renderDebug(certified.debug) << ")golden\"\n\n";
}

namespace {

std::string goldenDebug(Backend backend, bool certified) {
    if (certified) {
        return R"golden(relaxed_vector_primary_hit_count=2
relaxed_vector_retry_applied=0
relaxed_vector_retry_attempted=0
relaxed_vector_retry_enabled=0
relaxed_vector_retry_threshold=0.000
topology_candidate_rescue_added_candidate_doc_ids=
topology_candidate_rescue_added_candidate_hashes=
topology_candidate_rescue_added_candidates=0
topology_candidate_rescue_applied=0
topology_candidate_rescue_attempted=0
topology_candidate_rescue_duplicate_candidates=0
topology_candidate_rescue_evidence_rescue_doc_ids=
topology_candidate_rescue_evidence_rescue_hashes=
topology_candidate_rescue_evidence_rescues=0
topology_candidate_rescue_exact_candidate_hashes_requested=0
topology_candidate_rescue_exact_distance_evaluations=0
topology_candidate_rescue_exact_distance_evaluations_status=unavailable
topology_candidate_rescue_exact_matched_document_doc_ids=
topology_candidate_rescue_exact_matched_document_hashes=
topology_candidate_rescue_exact_matched_documents=0
topology_candidate_rescue_exact_returned_rows=0
topology_candidate_rescue_exact_rows_visited=0
topology_candidate_rescue_exact_rows_visited_status=unavailable
topology_candidate_rescue_exact_scored_candidate_doc_ids=
topology_candidate_rescue_exact_scored_candidate_rows=
topology_candidate_rescue_novel_candidate_doc_ids=
topology_candidate_rescue_novel_candidate_hashes=
topology_candidate_rescue_novel_candidates=0
topology_candidate_rescue_scored_candidate_doc_ids=
topology_expansion_output_budget=2
topology_route_action=narrow
topology_route_admission_denial_reason=
topology_route_admission_eligible=1
topology_route_certified=1
topology_route_narrow_proposed=1
topology_route_work_ann_candidate_budget_actual=2
topology_route_work_exact_distance_evaluations_actual=2
topology_route_work_observed=1
topology_route_work_rows_visited_actual=2
topology_route_work_status=satisfied
topology_shadow_evaluated=0
topology_shadow_proposed_action=global
topology_shadow_removed_candidate_doc_ids=
topology_shadow_removed_candidates=0
topology_shadow_retained_candidate_doc_ids=
topology_shadow_retained_candidates=0
topology_vector_allowed_set_ann_applied=0
topology_vector_allowed_set_ann_fallback=0
topology_vector_filter_applied=0
topology_vector_filter_fallback=0
topology_vector_filter_matched=0
topology_vector_filter_removed=0
topology_vector_global_fill_count=0
topology_vector_policy=route_only
topology_vector_seed_probe=0
topology_weak_query_narrow_applied=0
vector_candidate_source_effective=route_only
vector_document_refill_attempts=0
vector_search_ann_candidate_budget_actual=2
vector_search_ann_candidate_budget_status=observed
vector_search_candidate_budget=4
vector_search_candidate_index_cache_used=1
vector_search_candidate_lookup_count=1
vector_search_distance_evaluation_budget=8
vector_search_exact_distance_evaluations_actual=2
vector_search_exact_distance_evaluations_status=observed
vector_search_materialized_rows=2
vector_search_result_budget=4
vector_search_rows_visited_actual=2
vector_search_rows_visited_status=observed
)golden";
    }
    switch (backend) {
        case Backend::SimeonPqAdc:
            return R"golden(relaxed_vector_primary_hit_count=2
relaxed_vector_retry_applied=0
relaxed_vector_retry_attempted=0
relaxed_vector_retry_enabled=0
relaxed_vector_retry_threshold=0.000
topology_candidate_rescue_added_candidate_doc_ids=
topology_candidate_rescue_added_candidate_hashes=
topology_candidate_rescue_added_candidates=0
topology_candidate_rescue_applied=0
topology_candidate_rescue_attempted=0
topology_candidate_rescue_duplicate_candidates=0
topology_candidate_rescue_evidence_rescue_doc_ids=
topology_candidate_rescue_evidence_rescue_hashes=
topology_candidate_rescue_evidence_rescues=0
topology_candidate_rescue_exact_candidate_hashes_requested=0
topology_candidate_rescue_exact_distance_evaluations=0
topology_candidate_rescue_exact_distance_evaluations_status=unavailable
topology_candidate_rescue_exact_matched_document_doc_ids=
topology_candidate_rescue_exact_matched_document_hashes=
topology_candidate_rescue_exact_matched_documents=0
topology_candidate_rescue_exact_returned_rows=0
topology_candidate_rescue_exact_rows_visited=0
topology_candidate_rescue_exact_rows_visited_status=unavailable
topology_candidate_rescue_exact_scored_candidate_doc_ids=
topology_candidate_rescue_exact_scored_candidate_rows=
topology_candidate_rescue_novel_candidate_doc_ids=
topology_candidate_rescue_novel_candidate_hashes=
topology_candidate_rescue_novel_candidates=0
topology_candidate_rescue_scored_candidate_doc_ids=
topology_expansion_output_budget=2
topology_route_action=augment
topology_route_admission_denial_reason=protected_fibers_representation_unavailable
topology_route_admission_eligible=0
topology_route_certified=0
topology_route_narrow_proposed=1
topology_route_work_ann_candidate_budget_actual=0
topology_route_work_exact_distance_evaluations_actual=2
topology_route_work_observed=0
topology_route_work_rows_visited_actual=2
topology_route_work_status=unavailable
topology_shadow_evaluated=0
topology_shadow_proposed_action=global
topology_shadow_removed_candidate_doc_ids=
topology_shadow_removed_candidates=0
topology_shadow_retained_candidate_doc_ids=
topology_shadow_retained_candidates=0
topology_vector_allowed_set_ann_applied=0
topology_vector_allowed_set_ann_fallback=0
topology_vector_filter_applied=0
topology_vector_filter_fallback=0
topology_vector_filter_matched=0
topology_vector_filter_removed=0
topology_vector_global_fill_count=0
topology_vector_policy=route_only
topology_vector_seed_probe=0
topology_weak_query_narrow_applied=0
vector_candidate_source_effective=route_only
vector_document_refill_attempts=0
vector_search_ann_candidate_budget_status=unavailable
vector_search_candidate_budget=4
vector_search_candidate_index_cache_used=0
vector_search_candidate_lookup_count=0
vector_search_distance_evaluation_budget=8
vector_search_exact_distance_evaluations_actual=2
vector_search_exact_distance_evaluations_status=observed
vector_search_materialized_rows=0
vector_search_result_budget=4
vector_search_rows_visited_actual=2
vector_search_rows_visited_status=observed
)golden";
        case Backend::ExactScan:
            return R"golden(relaxed_vector_primary_hit_count=2
relaxed_vector_retry_applied=0
relaxed_vector_retry_attempted=0
relaxed_vector_retry_enabled=0
relaxed_vector_retry_threshold=0.000
topology_candidate_rescue_added_candidate_doc_ids=
topology_candidate_rescue_added_candidate_hashes=
topology_candidate_rescue_added_candidates=0
topology_candidate_rescue_applied=0
topology_candidate_rescue_attempted=0
topology_candidate_rescue_duplicate_candidates=0
topology_candidate_rescue_evidence_rescue_doc_ids=
topology_candidate_rescue_evidence_rescue_hashes=
topology_candidate_rescue_evidence_rescues=0
topology_candidate_rescue_exact_candidate_hashes_requested=0
topology_candidate_rescue_exact_distance_evaluations=0
topology_candidate_rescue_exact_distance_evaluations_status=unavailable
topology_candidate_rescue_exact_matched_document_doc_ids=
topology_candidate_rescue_exact_matched_document_hashes=
topology_candidate_rescue_exact_matched_documents=0
topology_candidate_rescue_exact_returned_rows=0
topology_candidate_rescue_exact_rows_visited=0
topology_candidate_rescue_exact_rows_visited_status=unavailable
topology_candidate_rescue_exact_scored_candidate_doc_ids=
topology_candidate_rescue_exact_scored_candidate_rows=
topology_candidate_rescue_novel_candidate_doc_ids=
topology_candidate_rescue_novel_candidate_hashes=
topology_candidate_rescue_novel_candidates=0
topology_candidate_rescue_scored_candidate_doc_ids=
topology_expansion_output_budget=4
topology_route_action=augment
topology_route_admission_denial_reason=protected_fibers_representation_unavailable
topology_route_admission_eligible=0
topology_route_certified=0
topology_route_narrow_proposed=1
topology_route_work_ann_candidate_budget_actual=0
topology_route_work_exact_distance_evaluations_actual=2
topology_route_work_observed=0
topology_route_work_rows_visited_actual=2
topology_route_work_status=unavailable
topology_shadow_evaluated=0
topology_shadow_proposed_action=global
topology_shadow_removed_candidate_doc_ids=
topology_shadow_removed_candidates=0
topology_shadow_retained_candidate_doc_ids=
topology_shadow_retained_candidates=0
topology_vector_allowed_set_ann_applied=0
topology_vector_allowed_set_ann_fallback=0
topology_vector_filter_applied=0
topology_vector_filter_fallback=0
topology_vector_filter_matched=0
topology_vector_filter_removed=0
topology_vector_global_fill_count=0
topology_vector_policy=route_only
topology_vector_seed_probe=0
topology_weak_query_narrow_applied=0
vector_candidate_source_effective=route_only
vector_document_refill_attempts=0
vector_search_ann_candidate_budget_status=unavailable
vector_search_candidate_budget=4
vector_search_candidate_index_cache_used=0
vector_search_candidate_lookup_count=0
vector_search_distance_evaluation_budget=4
vector_search_exact_distance_evaluations_actual=2
vector_search_exact_distance_evaluations_status=observed
vector_search_materialized_rows=0
vector_search_result_budget=4
vector_search_rows_visited_actual=2
vector_search_rows_visited_status=observed
)golden";
        case Backend::Vec0L2:
            return R"golden(relaxed_vector_primary_hit_count=4
relaxed_vector_retry_applied=0
relaxed_vector_retry_attempted=0
relaxed_vector_retry_enabled=0
relaxed_vector_retry_threshold=0.000
topology_candidate_rescue_added_candidate_doc_ids=
topology_candidate_rescue_added_candidate_hashes=
topology_candidate_rescue_added_candidates=0
topology_candidate_rescue_applied=0
topology_candidate_rescue_attempted=0
topology_candidate_rescue_duplicate_candidates=0
topology_candidate_rescue_evidence_rescue_doc_ids=
topology_candidate_rescue_evidence_rescue_hashes=
topology_candidate_rescue_evidence_rescues=0
topology_candidate_rescue_exact_candidate_hashes_requested=0
topology_candidate_rescue_exact_distance_evaluations=0
topology_candidate_rescue_exact_distance_evaluations_status=unavailable
topology_candidate_rescue_exact_matched_document_doc_ids=
topology_candidate_rescue_exact_matched_document_hashes=
topology_candidate_rescue_exact_matched_documents=0
topology_candidate_rescue_exact_returned_rows=0
topology_candidate_rescue_exact_rows_visited=0
topology_candidate_rescue_exact_rows_visited_status=unavailable
topology_candidate_rescue_exact_scored_candidate_doc_ids=
topology_candidate_rescue_exact_scored_candidate_rows=
topology_candidate_rescue_novel_candidate_doc_ids=
topology_candidate_rescue_novel_candidate_hashes=
topology_candidate_rescue_novel_candidates=0
topology_candidate_rescue_scored_candidate_doc_ids=
topology_expansion_output_budget=4
topology_route_action=augment
topology_route_admission_denial_reason=protected_fibers_representation_unavailable
topology_route_admission_eligible=0
topology_route_certified=0
topology_route_narrow_proposed=1
topology_route_work_ann_candidate_budget_actual=0
topology_route_work_exact_distance_evaluations_actual=0
topology_route_work_observed=0
topology_route_work_rows_visited_actual=0
topology_route_work_status=unavailable
topology_shadow_evaluated=0
topology_shadow_proposed_action=global
topology_shadow_removed_candidate_doc_ids=
topology_shadow_removed_candidates=0
topology_shadow_retained_candidate_doc_ids=
topology_shadow_retained_candidates=0
topology_vector_allowed_set_ann_applied=0
topology_vector_allowed_set_ann_fallback=0
topology_vector_filter_applied=0
topology_vector_filter_fallback=0
topology_vector_filter_matched=0
topology_vector_filter_removed=0
topology_vector_global_fill_count=0
topology_vector_policy=route_only
topology_vector_seed_probe=0
topology_weak_query_narrow_applied=0
vector_candidate_source_effective=unavailable
vector_document_refill_attempts=0
vector_search_ann_candidate_budget_status=unavailable
vector_search_candidate_budget=4
vector_search_candidate_index_cache_used=0
vector_search_candidate_lookup_count=0
vector_search_distance_evaluation_budget=8
vector_search_exact_distance_evaluations_status=unavailable
vector_search_materialized_rows=0
vector_search_result_budget=4
vector_search_rows_visited_status=unavailable
)golden";
    }
    return {};
}

} // namespace

// NOLINTEND(cppcoreguidelines-avoid-do-while, readability-function-cognitive-complexity)
