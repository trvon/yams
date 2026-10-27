// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

// Characterization goldens for the SearchEngine vector candidate stage (the fanout.vector lambda
// in src/search/search_engine.cpp): global ANN, the topology Shadow / Augment / Narrow policies,
// routed candidate queries and rescue merging, route-work accounting and the stage's debug
// surface. The goldens pin current behaviour, quirks included, so the stage can be extracted
// without changing a byte of observable output:
//
//   * relaxed_vector_primary_hit_count is last-writer: it reports the size of whichever vector
//     query ran last inside the stage (Shadow and Narrow fallback: the global query, 4; Augment
//     and Narrow commit: the routed query, 2), not the global query.
//   * Vec0 supports neither routed candidates nor allowed-set ANN, so Shadow and Augment report
//     the policy, keep the global results unchanged, and attempt no rescue.
//   * Narrow commits only on Simeon PQ. ExactScan and Vec0 never observe an ANN candidate
//     budget, so certificate.work stays unobserved, admission reports
//     work_observation_unavailable and both Narrow cells fall back to the global results
//     whatever the budget. Vec0 still leaks a partial ann_candidate_budget_actual=12 into the
//     unobserved work record.
//   * Shadow fills certificate.work / admission.work from the counterfactual routed query, so
//     topology_route_work_* reports routed work (2 rows) while vector_search_* reports the
//     global query (4 rows).
//   * Routed query failures are dropped silently inside queryRoutedVectors (not pinned here:
//     the real backends in this fixture cannot be made to fail a routed query).
//
// The hidden "[.dump]" case prints every cell in golden form; re-run it to regenerate after an
// intentional behaviour change. Tab separators in id lists render as "<tab>".

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

// Catch2 assertion macros expand through do/while and large generated test bodies.
// NOLINTBEGIN(cppcoreguidelines-avoid-do-while, readability-function-cognitive-complexity)

namespace {

enum class StagePolicy { Shadow, Augment, NarrowCommit, NarrowFallback };

constexpr std::array kPolicies{StagePolicy::Shadow, StagePolicy::Augment, StagePolicy::NarrowCommit,
                               StagePolicy::NarrowFallback};
constexpr std::array kBackends{vector::VectorSearchEngine::SimeonPqAdc,
                               vector::VectorSearchEngine::ExactScan,
                               vector::VectorSearchEngine::Vec0L2};

std::string_view policyName(StagePolicy policy) {
    switch (policy) {
        case StagePolicy::Shadow:
            return "shadow";
        case StagePolicy::Augment:
            return "augment";
        case StagePolicy::NarrowCommit:
            return "narrow_commit";
        case StagePolicy::NarrowFallback:
            return "narrow_fallback";
    }
    return "unknown";
}

std::string_view backendName(vector::VectorSearchEngine backend) {
    switch (backend) {
        case vector::VectorSearchEngine::SimeonPqAdc:
            return "simeon_pq_adc";
        case vector::VectorSearchEngine::ExactScan:
            return "exact_scan";
        case vector::VectorSearchEngine::Vec0L2:
            return "vec0_l2";
    }
    return "unknown";
}

// Observable output of one vector-stage run, rendered as stable text. Timings are excluded.
struct VectorStageSnapshot {
    std::string fused;      // final results: hash, rank, fused score, vector contribution
    std::string debug;      // stage debug keys present with and without stage tracing
    std::string traceDebug; // stage debug keys emitted only under YAMS_SEARCH_STAGE_TRACE
    std::string rawVector;  // raw vector ComponentResults (trace_component_hits_json, trace only)
};

std::string formatScore(double value) {
    std::ostringstream out;
    out << std::fixed << std::setprecision(4) << value;
    return out.str();
}

bool isStageDebugKey(std::string_view key) {
    if (key.ends_with("_ns")) {
        return false; // timings
    }
    if (key.ends_with("_payload_bytes")) {
        return false; // hash-table bucket counts differ between libstdc++ and libc++
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
        std::string_view{"topology_route_narrow_proposed"},
        std::string_view{"topology_weak_query_narrow_applied"},
        std::string_view{"topology_expansion_output_budget"},
        std::string_view{"vector_document_refill_attempts"},
    };
    return std::ranges::any_of(prefixes, [&](auto p) { return key.starts_with(p); }) ||
           std::ranges::any_of(exact, [&](auto k) { return key == k; });
}

// Keys SearchEngine writes only when stage tracing is on (doc-id / hash lists and the exact
// shadow controls).
bool isTraceOnlyDebugKey(std::string_view key) {
    return key.ends_with("_doc_ids") || key.ends_with("_hashes") ||
           key.starts_with("topology_candidate_rescue_exact_") ||
           key.starts_with("topology_candidate_rescue_scored_");
}

VectorStageSnapshot captureVectorStage(const SearchResponse& response) {
    VectorStageSnapshot snapshot;

    std::ostringstream fused;
    for (std::size_t rank = 0; rank < response.results.size(); ++rank) {
        const auto& result = response.results[rank];
        fused << result.document.sha256Hash << " rank=" << rank
              << " score=" << formatScore(result.score) << " vector="
              << (result.vectorScore.has_value() ? formatScore(*result.vectorScore) : "-") << '\n';
    }
    snapshot.fused = fused.str();

    std::map<std::string, std::string> stable;
    std::map<std::string, std::string> traceOnly;
    for (const auto& [key, value] : response.debugStats) {
        if (!isStageDebugKey(key)) {
            continue;
        }
        (isTraceOnlyDebugKey(key) ? traceOnly : stable)[key] = value;
    }
    const auto render = [](const std::map<std::string, std::string>& entries) {
        std::ostringstream out;
        for (const auto& [key, value] : entries) {
            // Tab-joined id lists are rendered with a visible separator so the goldens stay
            // readable and survive editors that normalize whitespace.
            std::string visible = value;
            for (std::size_t pos = visible.find('\t'); pos != std::string::npos;
                 pos = visible.find('\t', pos + 5)) {
                visible.replace(pos, 1, "<tab>");
            }
            out << key << '=' << visible << '\n';
        }
        return out.str();
    };
    snapshot.debug = render(stable);
    snapshot.traceDebug = render(traceOnly);

    if (const auto hits = response.debugStats.find("trace_component_hits_json");
        hits != response.debugStats.end()) {
        const auto summary = nlohmann::json::parse(hits->second);
        std::ostringstream raw;
        if (summary.contains("vector")) {
            const auto& vectorSummary = summary.at("vector");
            raw << "raw_hits=" << vectorSummary.at("raw_hits").get<std::size_t>() << '\n';
            for (const auto& hit : vectorSummary.at("top_hits")) {
                raw << hit.at("doc_id").get<std::string>()
                    << " rank=" << hit.at("rank").get<std::size_t>()
                    << " score=" << formatScore(hit.at("score").get<double>()) << '\n';
            }
        } else {
            raw << "raw_hits=0\n";
        }
        snapshot.rawVector = raw.str();
    }
    return snapshot;
}

// One configuration per policy, applied to every backend. Routing is dense-only (alpha 0) from
// the fixed query embedding {0, 1}, so the route is always the {y1, y2} cluster. Shadow and
// Augment search "unmatched query" (no lexical hits: the vector stage alone produces candidates).
// Narrow searches "omega": route admission needs lexical seed hits inside the routed cluster
// (minSeedHits), which y1/y2 provide.
std::string cellQuery(StagePolicy policy) {
    switch (policy) {
        case StagePolicy::Shadow:
        case StagePolicy::Augment:
            return "unmatched query";
        case StagePolicy::NarrowCommit:
        case StagePolicy::NarrowFallback:
            return "omega";
    }
    return "unmatched query";
}

SearchEngineConfig cellConfig(StagePolicy policy, vector::VectorSearchEngine backend,
                              const yams::topology::TopologyArtifactBatch& batch) {
    auto config = topologyRoutingTestConfig(true);
    config.topologyRoutingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    config.topologySparseDenseAlpha = 0.0F;
    config.topologyNarrowMinBoundaryMargin = 0.0F;
    config.similarityThreshold = -1.0F;
    config.enableWeakQueryFanoutBoost = false;
    config.topologyExpansionOutputLimit = 2;

    const bool simeon = backend == vector::VectorSearchEngine::SimeonPqAdc;
    // Simeon's allowed-set ANN visits only the two routed rows; the other backends scan the whole
    // four-row corpus before any route filter applies. (Neither ExactScan nor Vec0 can commit at
    // any budget: see the header. The budget is still sized so the work check is not the blocker.)
    const std::size_t commitBudget = simeon ? 2 : 4;
    switch (policy) {
        case StagePolicy::Shadow:
            config.topologyVectorPolicy = SearchEngineConfig::TopologyVectorPolicy::Shadow;
            config.topologyRouteWorkBudget.maxRowsVisited = 4;
            config.topologyRouteWorkBudget.maxExactDistanceEvaluations = 4;
            config.topologyRouteWorkBudget.maxAnnCandidates = 4;
            break;
        case StagePolicy::Augment:
            config.topologyVectorPolicy = SearchEngineConfig::TopologyVectorPolicy::Augment;
            config.vectorMaxResults = 1; // global top-1 is y1; the route adds y2
            break;
        case StagePolicy::NarrowCommit:
            config.topologyVectorPolicy = SearchEngineConfig::TopologyVectorPolicy::Narrow;
            configureCertifiedTopologyRoute(
                config, batch,
                CertifiedTopologyRouteOptions{.maxRowsVisited = commitBudget,
                                              .maxExactDistanceEvaluations = commitBudget,
                                              .maxAnnCandidates = commitBudget,
                                              .deferDocumentLimitToVectorRanker = simeon});
            break;
        case StagePolicy::NarrowFallback:
            config.topologyVectorPolicy = SearchEngineConfig::TopologyVectorPolicy::Narrow;
            configureCertifiedTopologyRoute(
                config, batch,
                CertifiedTopologyRouteOptions{.maxRowsVisited = 1,
                                              .maxExactDistanceEvaluations = 1,
                                              .maxAnnCandidates = 1,
                                              .deferDocumentLimitToVectorRanker = simeon});
            break;
    }
    return config;
}

VectorStageSnapshot runCell(StagePolicy policy, vector::VectorSearchEngine backend, bool stageTrace,
                            std::optional<boost::asio::any_io_executor> executor = std::nullopt) {
    yams::test::ScopedEnvVar trace("YAMS_SEARCH_STAGE_TRACE",
                                   stageTrace ? std::optional<std::string>("1") : std::nullopt);
    TopologySearchFixture fix{backend};
    seedTopologyDocuments(fix);
    const auto batch = buildTwoClusterTopologyBatch();
    yams::topology::MetadataKgTopologyArtifactStore topologyStore(fix.repo, fix.kgStore);
    REQUIRE(topologyStore.storeBatch(batch).has_value());
    if (backend != vector::VectorSearchEngine::Vec0L2) {
        REQUIRE(fix.vectorDb->buildIndex());
    }
    auto generator = makeFixedGenerator({0.0F, 1.0F});
    const auto config = cellConfig(policy, backend, batch);
    auto response =
        runTopologySearch(fix, generator, config, 4, cellQuery(policy), std::move(executor));
    REQUIRE(response.has_value());
    return captureVectorStage(response.value());
}

struct VectorStageGolden {
    std::string fused;
    std::string debug;
    std::string traceDebug;
    std::string rawVector;
};

VectorStageGolden golden(StagePolicy policy, vector::VectorSearchEngine backend);

void checkCell(StagePolicy policy, vector::VectorSearchEngine backend, bool stageTrace,
               const VectorStageSnapshot& actual) {
    CAPTURE(policyName(policy), backendName(backend), stageTrace);
    const auto expected = golden(policy, backend);
    CHECK(actual.fused == expected.fused);
    CHECK(actual.debug == expected.debug);
    if (stageTrace) {
        CHECK(actual.traceDebug == expected.traceDebug);
        CHECK(actual.rawVector == expected.rawVector);
    } else {
        CHECK(actual.traceDebug.empty());
        CHECK(actual.rawVector.empty());
    }
}

} // namespace

TEST_CASE("Vector stage characterization: policy x backend, stage trace off and on",
          "[search][vector-stage][characterization][catch2]") {
    const TopologyVectorEnvPin vectorEnv;
    for (const auto policy : kPolicies) {
        for (const auto backend : kBackends) {
            for (const bool stageTrace : {false, true}) {
                DYNAMIC_SECTION(policyName(policy) << " / " << backendName(backend)
                                                   << (stageTrace ? " / trace" : " / no-trace")) {
                    checkCell(policy, backend, stageTrace, runCell(policy, backend, stageTrace));
                }
            }
        }
    }
}

TEST_CASE("Vector stage characterization: executor-backed fanout matches the inline golden",
          "[search][vector-stage][characterization][executor][catch2]") {
    const TopologyVectorEnvPin vectorEnv;
    boost::asio::io_context io;
    auto guard = boost::asio::make_work_guard(io);
    std::vector<std::thread> runners;
    for (int i = 0; i < 2; ++i) {
        runners.emplace_back([&io]() { io.run(); });
    }
    {
        const auto snapshot =
            runCell(StagePolicy::NarrowCommit, vector::VectorSearchEngine::SimeonPqAdc, true,
                    std::optional<boost::asio::any_io_executor>{io.get_executor()});
        checkCell(StagePolicy::NarrowCommit, vector::VectorSearchEngine::SimeonPqAdc, true,
                  snapshot);
    }
    guard.reset();
    io.stop();
    for (auto& runner : runners) {
        runner.join();
    }
}

// Hidden: prints every cell as golden literals. Run with
//   build/debug/tests/catch2_search_vector_stage "[dump]"
TEST_CASE("Vector stage characterization dump", "[.][search][vector-stage][dump]") {
    const TopologyVectorEnvPin vectorEnv;
    for (const auto policy : kPolicies) {
        for (const auto backend : kBackends) {
            const auto snapshot = runCell(policy, backend, true);
            std::cout << "// ---- " << policyName(policy) << " / " << backendName(backend)
                      << " ----\n"
                      << ".fused = R\"(" << snapshot.fused << ")\",\n"
                      << ".debug = R\"(" << snapshot.debug << ")\",\n"
                      << ".traceDebug = R\"(" << snapshot.traceDebug << ")\",\n"
                      << ".rawVector = R\"(" << snapshot.rawVector << ")\",\n\n";
        }
    }
}

namespace {

VectorStageGolden golden(StagePolicy policy, vector::VectorSearchEngine backend) {
    using Backend = vector::VectorSearchEngine;
    switch (policy) {
        case StagePolicy::Shadow:
            switch (backend) {
                case Backend::SimeonPqAdc:
                    return {
                        .fused = R"golden(y1 rank=0 score=0.0577 vector=0.0577
y2 rank=1 score=0.0535 vector=0.0535
x2 rank=2 score=0.0352 vector=0.0352
x1 rank=3 score=0.0312 vector=0.0312
)golden",
                        .debug = R"golden(relaxed_vector_primary_hit_count=4
relaxed_vector_retry_applied=0
relaxed_vector_retry_attempted=0
relaxed_vector_retry_enabled=0
relaxed_vector_retry_threshold=0.000
topology_candidate_rescue_added_candidates=0
topology_candidate_rescue_applied=0
topology_candidate_rescue_attempted=1
topology_candidate_rescue_duplicate_candidates=2
topology_candidate_rescue_evidence_rescues=0
topology_candidate_rescue_novel_candidates=0
topology_expansion_output_budget=2
topology_route_action=augment
topology_route_admission_denial_reason=protected_fibers_representation_unavailable
topology_route_admission_eligible=0
topology_route_narrow_proposed=1
topology_route_work_ann_candidate_budget_actual=2
topology_route_work_exact_distance_evaluations_actual=2
topology_route_work_observed=1
topology_route_work_rows_visited_actual=2
topology_route_work_status=satisfied
topology_shadow_evaluated=1
topology_shadow_proposed_action=narrow
topology_shadow_removed_candidates=2
topology_shadow_retained_candidates=2
topology_vector_allowed_set_ann_applied=0
topology_vector_allowed_set_ann_fallback=0
topology_vector_filter_applied=0
topology_vector_filter_fallback=0
topology_vector_filter_matched=0
topology_vector_filter_removed=0
topology_vector_global_fill_count=0
topology_vector_policy=shadow
topology_vector_seed_probe=0
topology_weak_query_narrow_applied=0
vector_document_refill_attempts=0
vector_search_ann_candidate_budget_actual=4
vector_search_ann_candidate_budget_status=observed
vector_search_candidate_budget=4
vector_search_candidate_index_cache_used=0
vector_search_candidate_lookup_count=0
vector_search_distance_evaluation_budget=8
vector_search_exact_distance_evaluations_actual=4
vector_search_exact_distance_evaluations_status=observed
vector_search_materialized_rows=4
vector_search_result_budget=4
vector_search_rows_visited_actual=4
vector_search_rows_visited_status=observed
)golden",
                        .traceDebug = R"golden(topology_candidate_rescue_added_candidate_doc_ids=
topology_candidate_rescue_added_candidate_hashes=
topology_candidate_rescue_evidence_rescue_doc_ids=
topology_candidate_rescue_evidence_rescue_hashes=
topology_candidate_rescue_exact_candidate_hashes_requested=2
topology_candidate_rescue_exact_distance_evaluations=2
topology_candidate_rescue_exact_distance_evaluations_status=observed
topology_candidate_rescue_exact_matched_document_doc_ids=y1<tab>y2
topology_candidate_rescue_exact_matched_document_hashes=y1<tab>y2
topology_candidate_rescue_exact_matched_documents=2
topology_candidate_rescue_exact_returned_rows=2
topology_candidate_rescue_exact_rows_visited=2
topology_candidate_rescue_exact_rows_visited_status=observed
topology_candidate_rescue_exact_scored_candidate_doc_ids=y1<tab>y2
topology_candidate_rescue_exact_scored_candidate_rows={"vector":{"raw_hits":2,"top_hits":[{"doc_id":"y1","rank":1,"score":1.0},{"doc_id":"y2","rank":2,"score":0.9938837289810181}],"unique_top_doc_ids":["y1","y2"]}}
topology_candidate_rescue_novel_candidate_doc_ids=
topology_candidate_rescue_novel_candidate_hashes=
topology_candidate_rescue_scored_candidate_doc_ids=y1<tab>y2
topology_shadow_removed_candidate_doc_ids=x1<tab>x2
topology_shadow_retained_candidate_doc_ids=y1<tab>y2
)golden",
                        .rawVector = R"golden(raw_hits=4
y1 rank=1 score=1.0000
y2 rank=2 score=0.9939
x2 rank=3 score=0.1104
x1 rank=4 score=0.0000
)golden",
                    };
                case Backend::ExactScan:
                    return {
                        .fused = R"golden(y1 rank=0 score=0.0577 vector=0.0577
y2 rank=1 score=0.0535 vector=0.0535
x2 rank=2 score=0.0352 vector=0.0352
x1 rank=3 score=0.0312 vector=0.0312
)golden",
                        .debug = R"golden(relaxed_vector_primary_hit_count=4
relaxed_vector_retry_applied=0
relaxed_vector_retry_attempted=0
relaxed_vector_retry_enabled=0
relaxed_vector_retry_threshold=0.000
topology_candidate_rescue_added_candidates=0
topology_candidate_rescue_applied=0
topology_candidate_rescue_attempted=1
topology_candidate_rescue_duplicate_candidates=2
topology_candidate_rescue_evidence_rescues=0
topology_candidate_rescue_novel_candidates=0
topology_expansion_output_budget=2
topology_route_action=augment
topology_route_admission_denial_reason=protected_fibers_representation_unavailable
topology_route_admission_eligible=0
topology_route_narrow_proposed=1
topology_route_work_ann_candidate_budget_actual=0
topology_route_work_exact_distance_evaluations_actual=2
topology_route_work_observed=0
topology_route_work_rows_visited_actual=2
topology_route_work_status=unavailable
topology_shadow_evaluated=1
topology_shadow_proposed_action=narrow
topology_shadow_removed_candidates=2
topology_shadow_retained_candidates=2
topology_vector_allowed_set_ann_applied=0
topology_vector_allowed_set_ann_fallback=0
topology_vector_filter_applied=0
topology_vector_filter_fallback=0
topology_vector_filter_matched=0
topology_vector_filter_removed=0
topology_vector_global_fill_count=0
topology_vector_policy=shadow
topology_vector_seed_probe=0
topology_weak_query_narrow_applied=0
vector_document_refill_attempts=0
vector_search_ann_candidate_budget_status=unavailable
vector_search_candidate_budget=4
vector_search_candidate_index_cache_used=0
vector_search_candidate_lookup_count=0
vector_search_distance_evaluation_budget=4
vector_search_exact_distance_evaluations_actual=4
vector_search_exact_distance_evaluations_status=observed
vector_search_materialized_rows=0
vector_search_result_budget=4
vector_search_rows_visited_actual=4
vector_search_rows_visited_status=observed
)golden",
                        .traceDebug = R"golden(topology_candidate_rescue_added_candidate_doc_ids=
topology_candidate_rescue_added_candidate_hashes=
topology_candidate_rescue_evidence_rescue_doc_ids=
topology_candidate_rescue_evidence_rescue_hashes=
topology_candidate_rescue_exact_candidate_hashes_requested=2
topology_candidate_rescue_exact_distance_evaluations=2
topology_candidate_rescue_exact_distance_evaluations_status=observed
topology_candidate_rescue_exact_matched_document_doc_ids=y1<tab>y2
topology_candidate_rescue_exact_matched_document_hashes=y1<tab>y2
topology_candidate_rescue_exact_matched_documents=2
topology_candidate_rescue_exact_returned_rows=2
topology_candidate_rescue_exact_rows_visited=2
topology_candidate_rescue_exact_rows_visited_status=observed
topology_candidate_rescue_exact_scored_candidate_doc_ids=y1<tab>y2
topology_candidate_rescue_exact_scored_candidate_rows={"vector":{"raw_hits":2,"top_hits":[{"doc_id":"y1","rank":1,"score":1.0},{"doc_id":"y2","rank":2,"score":0.9938837289810181}],"unique_top_doc_ids":["y1","y2"]}}
topology_candidate_rescue_novel_candidate_doc_ids=
topology_candidate_rescue_novel_candidate_hashes=
topology_candidate_rescue_scored_candidate_doc_ids=y1<tab>y2
topology_shadow_removed_candidate_doc_ids=x1<tab>x2
topology_shadow_retained_candidate_doc_ids=y1<tab>y2
)golden",
                        .rawVector = R"golden(raw_hits=4
y1 rank=1 score=1.0000
y2 rank=2 score=0.9939
x2 rank=3 score=0.1104
x1 rank=4 score=0.0000
)golden",
                    };
                case Backend::Vec0L2:
                    return {
                        .fused = R"golden(y1 rank=0 score=0.0577 vector=0.0577
y2 rank=1 score=0.0535 vector=0.0535
x2 rank=2 score=0.0352 vector=0.0352
x1 rank=3 score=0.0312 vector=0.0312
)golden",
                        .debug = R"golden(relaxed_vector_primary_hit_count=4
relaxed_vector_retry_applied=0
relaxed_vector_retry_attempted=0
relaxed_vector_retry_enabled=0
relaxed_vector_retry_threshold=0.000
topology_candidate_rescue_added_candidates=0
topology_candidate_rescue_applied=0
topology_candidate_rescue_attempted=0
topology_candidate_rescue_duplicate_candidates=0
topology_candidate_rescue_evidence_rescues=0
topology_candidate_rescue_novel_candidates=0
topology_expansion_output_budget=2
topology_route_action=augment
topology_route_admission_denial_reason=protected_fibers_representation_unavailable
topology_route_admission_eligible=0
topology_route_narrow_proposed=1
topology_route_work_ann_candidate_budget_actual=0
topology_route_work_exact_distance_evaluations_actual=0
topology_route_work_observed=0
topology_route_work_rows_visited_actual=0
topology_route_work_status=unavailable
topology_shadow_evaluated=1
topology_shadow_proposed_action=narrow
topology_shadow_removed_candidates=2
topology_shadow_retained_candidates=2
topology_vector_allowed_set_ann_applied=0
topology_vector_allowed_set_ann_fallback=0
topology_vector_filter_applied=0
topology_vector_filter_fallback=0
topology_vector_filter_matched=0
topology_vector_filter_removed=0
topology_vector_global_fill_count=0
topology_vector_policy=shadow
topology_vector_seed_probe=0
topology_weak_query_narrow_applied=0
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
)golden",
                        .traceDebug = R"golden(topology_candidate_rescue_added_candidate_doc_ids=
topology_candidate_rescue_added_candidate_hashes=
topology_candidate_rescue_evidence_rescue_doc_ids=
topology_candidate_rescue_evidence_rescue_hashes=
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
topology_candidate_rescue_scored_candidate_doc_ids=
topology_shadow_removed_candidate_doc_ids=x1<tab>x2
topology_shadow_retained_candidate_doc_ids=y1<tab>y2
)golden",
                        .rawVector = R"golden(raw_hits=4
y1 rank=1 score=1.0000
y2 rank=2 score=0.9939
x2 rank=3 score=0.1104
x1 rank=4 score=0.0000
)golden",
                    };
            }
            break;
        case StagePolicy::Augment:
            switch (backend) {
                case Backend::SimeonPqAdc:
                    return {
                        .fused = R"golden(y1 rank=0 score=0.0729 vector=0.0577
y2 rank=1 score=0.0617 vector=0.0535
)golden",
                        .debug = R"golden(relaxed_vector_primary_hit_count=2
relaxed_vector_retry_applied=0
relaxed_vector_retry_attempted=0
relaxed_vector_retry_enabled=0
relaxed_vector_retry_threshold=0.000
topology_candidate_rescue_added_candidates=1
topology_candidate_rescue_applied=1
topology_candidate_rescue_attempted=1
topology_candidate_rescue_duplicate_candidates=1
topology_candidate_rescue_evidence_rescues=0
topology_candidate_rescue_novel_candidates=1
topology_expansion_output_budget=2
topology_route_action=augment
topology_route_admission_denial_reason=protected_fibers_representation_unavailable
topology_route_admission_eligible=0
topology_route_narrow_proposed=1
topology_route_work_ann_candidate_budget_actual=0
topology_route_work_exact_distance_evaluations_actual=0
topology_route_work_observed=0
topology_route_work_rows_visited_actual=0
topology_route_work_status=unavailable
topology_shadow_evaluated=0
topology_shadow_proposed_action=global
topology_shadow_removed_candidates=0
topology_shadow_retained_candidates=0
topology_vector_allowed_set_ann_applied=0
topology_vector_allowed_set_ann_fallback=0
topology_vector_filter_applied=0
topology_vector_filter_fallback=0
topology_vector_filter_matched=0
topology_vector_filter_removed=0
topology_vector_global_fill_count=0
topology_vector_policy=augment
topology_vector_seed_probe=0
topology_weak_query_narrow_applied=0
vector_document_refill_attempts=0
vector_search_ann_candidate_budget_status=unavailable
vector_search_candidate_budget=4
vector_search_candidate_index_cache_used=0
vector_search_candidate_lookup_count=0
vector_search_distance_evaluation_budget=6
vector_search_exact_distance_evaluations_actual=6
vector_search_exact_distance_evaluations_status=observed
vector_search_materialized_rows=4
vector_search_result_budget=1
vector_search_rows_visited_actual=6
vector_search_rows_visited_status=observed
)golden",
                        .traceDebug = R"golden(topology_candidate_rescue_added_candidate_doc_ids=y2
topology_candidate_rescue_added_candidate_hashes=y2
topology_candidate_rescue_evidence_rescue_doc_ids=
topology_candidate_rescue_evidence_rescue_hashes=
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
topology_candidate_rescue_novel_candidate_doc_ids=y2
topology_candidate_rescue_novel_candidate_hashes=y2
topology_candidate_rescue_scored_candidate_doc_ids=y1<tab>y2
topology_shadow_removed_candidate_doc_ids=
topology_shadow_retained_candidate_doc_ids=
)golden",
                        .rawVector = R"golden(raw_hits=2
y1 rank=1 score=1.0000
y2 rank=2 score=0.9939
)golden",
                    };
                case Backend::ExactScan:
                    return {
                        .fused = R"golden(y1 rank=0 score=0.0729 vector=0.0577
y2 rank=1 score=0.0617 vector=0.0535
)golden",
                        .debug = R"golden(relaxed_vector_primary_hit_count=2
relaxed_vector_retry_applied=0
relaxed_vector_retry_attempted=0
relaxed_vector_retry_enabled=0
relaxed_vector_retry_threshold=0.000
topology_candidate_rescue_added_candidates=1
topology_candidate_rescue_applied=1
topology_candidate_rescue_attempted=1
topology_candidate_rescue_duplicate_candidates=1
topology_candidate_rescue_evidence_rescues=0
topology_candidate_rescue_novel_candidates=1
topology_expansion_output_budget=2
topology_route_action=augment
topology_route_admission_denial_reason=protected_fibers_representation_unavailable
topology_route_admission_eligible=0
topology_route_narrow_proposed=1
topology_route_work_ann_candidate_budget_actual=0
topology_route_work_exact_distance_evaluations_actual=0
topology_route_work_observed=0
topology_route_work_rows_visited_actual=0
topology_route_work_status=unavailable
topology_shadow_evaluated=0
topology_shadow_proposed_action=global
topology_shadow_removed_candidates=0
topology_shadow_retained_candidates=0
topology_vector_allowed_set_ann_applied=0
topology_vector_allowed_set_ann_fallback=0
topology_vector_filter_applied=0
topology_vector_filter_fallback=0
topology_vector_filter_matched=0
topology_vector_filter_removed=0
topology_vector_global_fill_count=0
topology_vector_policy=augment
topology_vector_seed_probe=0
topology_weak_query_narrow_applied=0
vector_document_refill_attempts=0
vector_search_ann_candidate_budget_status=unavailable
vector_search_candidate_budget=4
vector_search_candidate_index_cache_used=0
vector_search_candidate_lookup_count=0
vector_search_distance_evaluation_budget=4
vector_search_exact_distance_evaluations_actual=6
vector_search_exact_distance_evaluations_status=observed
vector_search_materialized_rows=0
vector_search_result_budget=1
vector_search_rows_visited_actual=6
vector_search_rows_visited_status=observed
)golden",
                        .traceDebug = R"golden(topology_candidate_rescue_added_candidate_doc_ids=y2
topology_candidate_rescue_added_candidate_hashes=y2
topology_candidate_rescue_evidence_rescue_doc_ids=
topology_candidate_rescue_evidence_rescue_hashes=
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
topology_candidate_rescue_novel_candidate_doc_ids=y2
topology_candidate_rescue_novel_candidate_hashes=y2
topology_candidate_rescue_scored_candidate_doc_ids=y1<tab>y2
topology_shadow_removed_candidate_doc_ids=
topology_shadow_retained_candidate_doc_ids=
)golden",
                        .rawVector = R"golden(raw_hits=2
y1 rank=1 score=1.0000
y2 rank=2 score=0.9939
)golden",
                    };
                case Backend::Vec0L2:
                    return {
                        .fused = R"golden(y1 rank=0 score=0.0729 vector=0.0577
)golden",
                        .debug = R"golden(relaxed_vector_primary_hit_count=1
relaxed_vector_retry_applied=0
relaxed_vector_retry_attempted=0
relaxed_vector_retry_enabled=0
relaxed_vector_retry_threshold=0.000
topology_candidate_rescue_added_candidates=0
topology_candidate_rescue_applied=0
topology_candidate_rescue_attempted=0
topology_candidate_rescue_duplicate_candidates=0
topology_candidate_rescue_evidence_rescues=0
topology_candidate_rescue_novel_candidates=0
topology_expansion_output_budget=2
topology_route_action=augment
topology_route_admission_denial_reason=protected_fibers_representation_unavailable
topology_route_admission_eligible=0
topology_route_narrow_proposed=1
topology_route_work_ann_candidate_budget_actual=0
topology_route_work_exact_distance_evaluations_actual=0
topology_route_work_observed=0
topology_route_work_rows_visited_actual=0
topology_route_work_status=unavailable
topology_shadow_evaluated=0
topology_shadow_proposed_action=global
topology_shadow_removed_candidates=0
topology_shadow_retained_candidates=0
topology_vector_allowed_set_ann_applied=0
topology_vector_allowed_set_ann_fallback=0
topology_vector_filter_applied=0
topology_vector_filter_fallback=0
topology_vector_filter_matched=0
topology_vector_filter_removed=0
topology_vector_global_fill_count=0
topology_vector_policy=augment
topology_vector_seed_probe=0
topology_weak_query_narrow_applied=0
vector_document_refill_attempts=0
vector_search_ann_candidate_budget_status=unavailable
vector_search_candidate_budget=4
vector_search_candidate_index_cache_used=0
vector_search_candidate_lookup_count=0
vector_search_distance_evaluation_budget=5
vector_search_exact_distance_evaluations_status=unavailable
vector_search_materialized_rows=0
vector_search_result_budget=1
vector_search_rows_visited_status=unavailable
)golden",
                        .traceDebug = R"golden(topology_candidate_rescue_added_candidate_doc_ids=
topology_candidate_rescue_added_candidate_hashes=
topology_candidate_rescue_evidence_rescue_doc_ids=
topology_candidate_rescue_evidence_rescue_hashes=
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
topology_candidate_rescue_scored_candidate_doc_ids=
topology_shadow_removed_candidate_doc_ids=
topology_shadow_retained_candidate_doc_ids=
)golden",
                        .rawVector = R"golden(raw_hits=1
y1 rank=1 score=1.0000
)golden",
                    };
            }
            break;
        case StagePolicy::NarrowCommit:
            switch (backend) {
                case Backend::SimeonPqAdc:
                    return {
                        .fused = R"golden(y1 rank=0 score=0.1161 vector=0.0577
y2 rank=1 score=0.0994 vector=0.0535
)golden",
                        .debug = R"golden(relaxed_vector_primary_hit_count=2
relaxed_vector_retry_applied=0
relaxed_vector_retry_attempted=0
relaxed_vector_retry_enabled=0
relaxed_vector_retry_threshold=0.000
topology_candidate_rescue_added_candidates=0
topology_candidate_rescue_applied=0
topology_candidate_rescue_attempted=0
topology_candidate_rescue_duplicate_candidates=0
topology_candidate_rescue_evidence_rescues=0
topology_candidate_rescue_novel_candidates=0
topology_expansion_output_budget=2
topology_route_action=narrow
topology_route_admission_denial_reason=
topology_route_admission_eligible=1
topology_route_narrow_proposed=1
topology_route_work_ann_candidate_budget_actual=2
topology_route_work_exact_distance_evaluations_actual=2
topology_route_work_observed=1
topology_route_work_rows_visited_actual=2
topology_route_work_status=satisfied
topology_shadow_evaluated=0
topology_shadow_proposed_action=global
topology_shadow_removed_candidates=0
topology_shadow_retained_candidates=0
topology_vector_allowed_set_ann_applied=1
topology_vector_allowed_set_ann_fallback=0
topology_vector_filter_applied=1
topology_vector_filter_fallback=0
topology_vector_filter_matched=2
topology_vector_filter_removed=0
topology_vector_global_fill_count=0
topology_vector_policy=narrow
topology_vector_seed_probe=0
topology_weak_query_narrow_applied=1
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
)golden",
                        .traceDebug = R"golden(topology_candidate_rescue_added_candidate_doc_ids=
topology_candidate_rescue_added_candidate_hashes=
topology_candidate_rescue_evidence_rescue_doc_ids=
topology_candidate_rescue_evidence_rescue_hashes=
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
topology_candidate_rescue_scored_candidate_doc_ids=
topology_shadow_removed_candidate_doc_ids=
topology_shadow_retained_candidate_doc_ids=
)golden",
                        .rawVector = R"golden(raw_hits=2
y1 rank=1 score=1.0000
y2 rank=2 score=0.9939
)golden",
                    };
                case Backend::ExactScan:
                    return {
                        .fused = R"golden(y1 rank=0 score=0.1161 vector=0.0577
y2 rank=1 score=0.0994 vector=0.0535
x2 rank=2 score=0.0352 vector=0.0352
x1 rank=3 score=0.0312 vector=0.0312
)golden",
                        .debug = R"golden(relaxed_vector_primary_hit_count=4
relaxed_vector_retry_applied=0
relaxed_vector_retry_attempted=0
relaxed_vector_retry_enabled=0
relaxed_vector_retry_threshold=0.000
topology_candidate_rescue_added_candidates=0
topology_candidate_rescue_applied=0
topology_candidate_rescue_attempted=0
topology_candidate_rescue_duplicate_candidates=0
topology_candidate_rescue_evidence_rescues=0
topology_candidate_rescue_novel_candidates=0
topology_expansion_output_budget=4
topology_route_action=augment
topology_route_admission_denial_reason=work_observation_unavailable
topology_route_admission_eligible=0
topology_route_narrow_proposed=1
topology_route_work_ann_candidate_budget_actual=0
topology_route_work_exact_distance_evaluations_actual=4
topology_route_work_observed=0
topology_route_work_rows_visited_actual=4
topology_route_work_status=unavailable
topology_shadow_evaluated=0
topology_shadow_proposed_action=global
topology_shadow_removed_candidates=0
topology_shadow_retained_candidates=0
topology_vector_allowed_set_ann_applied=0
topology_vector_allowed_set_ann_fallback=0
topology_vector_filter_applied=0
topology_vector_filter_fallback=1
topology_vector_filter_matched=0
topology_vector_filter_removed=0
topology_vector_global_fill_count=0
topology_vector_policy=narrow
topology_vector_seed_probe=0
topology_weak_query_narrow_applied=0
vector_document_refill_attempts=0
vector_search_ann_candidate_budget_status=unavailable
vector_search_candidate_budget=4
vector_search_candidate_index_cache_used=0
vector_search_candidate_lookup_count=0
vector_search_distance_evaluation_budget=4
vector_search_exact_distance_evaluations_actual=4
vector_search_exact_distance_evaluations_status=observed
vector_search_materialized_rows=0
vector_search_result_budget=4
vector_search_rows_visited_actual=4
vector_search_rows_visited_status=observed
)golden",
                        .traceDebug = R"golden(topology_candidate_rescue_added_candidate_doc_ids=
topology_candidate_rescue_added_candidate_hashes=
topology_candidate_rescue_evidence_rescue_doc_ids=
topology_candidate_rescue_evidence_rescue_hashes=
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
topology_candidate_rescue_scored_candidate_doc_ids=
topology_shadow_removed_candidate_doc_ids=
topology_shadow_retained_candidate_doc_ids=
)golden",
                        .rawVector = R"golden(raw_hits=4
y1 rank=1 score=1.0000
y2 rank=2 score=0.9939
x2 rank=3 score=0.1104
x1 rank=4 score=0.0000
)golden",
                    };
                case Backend::Vec0L2:
                    return {
                        .fused = R"golden(y1 rank=0 score=0.1161 vector=0.0577
y2 rank=1 score=0.0994 vector=0.0535
x2 rank=2 score=0.0352 vector=0.0352
x1 rank=3 score=0.0312 vector=0.0312
)golden",
                        .debug = R"golden(relaxed_vector_primary_hit_count=4
relaxed_vector_retry_applied=0
relaxed_vector_retry_attempted=0
relaxed_vector_retry_enabled=0
relaxed_vector_retry_threshold=0.000
topology_candidate_rescue_added_candidates=0
topology_candidate_rescue_applied=0
topology_candidate_rescue_attempted=0
topology_candidate_rescue_duplicate_candidates=0
topology_candidate_rescue_evidence_rescues=0
topology_candidate_rescue_novel_candidates=0
topology_expansion_output_budget=4
topology_route_action=augment
topology_route_admission_denial_reason=work_observation_unavailable
topology_route_admission_eligible=0
topology_route_narrow_proposed=1
topology_route_work_ann_candidate_budget_actual=12
topology_route_work_exact_distance_evaluations_actual=0
topology_route_work_observed=0
topology_route_work_rows_visited_actual=0
topology_route_work_status=unavailable
topology_shadow_evaluated=0
topology_shadow_proposed_action=global
topology_shadow_removed_candidates=0
topology_shadow_retained_candidates=0
topology_vector_allowed_set_ann_applied=0
topology_vector_allowed_set_ann_fallback=0
topology_vector_filter_applied=0
topology_vector_filter_fallback=1
topology_vector_filter_matched=0
topology_vector_filter_removed=0
topology_vector_global_fill_count=0
topology_vector_policy=narrow
topology_vector_seed_probe=0
topology_weak_query_narrow_applied=0
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
)golden",
                        .traceDebug = R"golden(topology_candidate_rescue_added_candidate_doc_ids=
topology_candidate_rescue_added_candidate_hashes=
topology_candidate_rescue_evidence_rescue_doc_ids=
topology_candidate_rescue_evidence_rescue_hashes=
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
topology_candidate_rescue_scored_candidate_doc_ids=
topology_shadow_removed_candidate_doc_ids=
topology_shadow_retained_candidate_doc_ids=
)golden",
                        .rawVector = R"golden(raw_hits=4
y1 rank=1 score=1.0000
y2 rank=2 score=0.9939
x2 rank=3 score=0.1104
x1 rank=4 score=0.0000
)golden",
                    };
            }
            break;
        case StagePolicy::NarrowFallback:
            switch (backend) {
                case Backend::SimeonPqAdc:
                    return {
                        .fused = R"golden(y1 rank=0 score=0.1161 vector=0.0577
y2 rank=1 score=0.0994 vector=0.0535
x2 rank=2 score=0.0352 vector=0.0352
x1 rank=3 score=0.0312 vector=0.0312
)golden",
                        .debug = R"golden(relaxed_vector_primary_hit_count=4
relaxed_vector_retry_applied=0
relaxed_vector_retry_attempted=0
relaxed_vector_retry_enabled=0
relaxed_vector_retry_threshold=0.000
topology_candidate_rescue_added_candidates=0
topology_candidate_rescue_applied=0
topology_candidate_rescue_attempted=0
topology_candidate_rescue_duplicate_candidates=0
topology_candidate_rescue_evidence_rescues=0
topology_candidate_rescue_novel_candidates=0
topology_expansion_output_budget=2
topology_route_action=augment
topology_route_admission_denial_reason=work_budget_violated
topology_route_admission_eligible=0
topology_route_narrow_proposed=1
topology_route_work_ann_candidate_budget_actual=2
topology_route_work_exact_distance_evaluations_actual=2
topology_route_work_observed=1
topology_route_work_rows_visited_actual=2
topology_route_work_status=violated
topology_shadow_evaluated=0
topology_shadow_proposed_action=global
topology_shadow_removed_candidates=0
topology_shadow_retained_candidates=0
topology_vector_allowed_set_ann_applied=0
topology_vector_allowed_set_ann_fallback=1
topology_vector_filter_applied=0
topology_vector_filter_fallback=1
topology_vector_filter_matched=0
topology_vector_filter_removed=0
topology_vector_global_fill_count=0
topology_vector_policy=narrow
topology_vector_seed_probe=0
topology_weak_query_narrow_applied=0
vector_document_refill_attempts=0
vector_search_ann_candidate_budget_actual=6
vector_search_ann_candidate_budget_status=observed
vector_search_candidate_budget=4
vector_search_candidate_index_cache_used=1
vector_search_candidate_lookup_count=1
vector_search_distance_evaluation_budget=8
vector_search_exact_distance_evaluations_actual=6
vector_search_exact_distance_evaluations_status=observed
vector_search_materialized_rows=6
vector_search_result_budget=4
vector_search_rows_visited_actual=6
vector_search_rows_visited_status=observed
)golden",
                        .traceDebug = R"golden(topology_candidate_rescue_added_candidate_doc_ids=
topology_candidate_rescue_added_candidate_hashes=
topology_candidate_rescue_evidence_rescue_doc_ids=
topology_candidate_rescue_evidence_rescue_hashes=
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
topology_candidate_rescue_scored_candidate_doc_ids=
topology_shadow_removed_candidate_doc_ids=
topology_shadow_retained_candidate_doc_ids=
)golden",
                        .rawVector = R"golden(raw_hits=4
y1 rank=1 score=1.0000
y2 rank=2 score=0.9939
x2 rank=3 score=0.1104
x1 rank=4 score=0.0000
)golden",
                    };
                case Backend::ExactScan:
                    return {
                        .fused = R"golden(y1 rank=0 score=0.1161 vector=0.0577
y2 rank=1 score=0.0994 vector=0.0535
x2 rank=2 score=0.0352 vector=0.0352
x1 rank=3 score=0.0312 vector=0.0312
)golden",
                        .debug = R"golden(relaxed_vector_primary_hit_count=4
relaxed_vector_retry_applied=0
relaxed_vector_retry_attempted=0
relaxed_vector_retry_enabled=0
relaxed_vector_retry_threshold=0.000
topology_candidate_rescue_added_candidates=0
topology_candidate_rescue_applied=0
topology_candidate_rescue_attempted=0
topology_candidate_rescue_duplicate_candidates=0
topology_candidate_rescue_evidence_rescues=0
topology_candidate_rescue_novel_candidates=0
topology_expansion_output_budget=4
topology_route_action=augment
topology_route_admission_denial_reason=work_observation_unavailable
topology_route_admission_eligible=0
topology_route_narrow_proposed=1
topology_route_work_ann_candidate_budget_actual=0
topology_route_work_exact_distance_evaluations_actual=4
topology_route_work_observed=0
topology_route_work_rows_visited_actual=4
topology_route_work_status=unavailable
topology_shadow_evaluated=0
topology_shadow_proposed_action=global
topology_shadow_removed_candidates=0
topology_shadow_retained_candidates=0
topology_vector_allowed_set_ann_applied=0
topology_vector_allowed_set_ann_fallback=0
topology_vector_filter_applied=0
topology_vector_filter_fallback=1
topology_vector_filter_matched=0
topology_vector_filter_removed=0
topology_vector_global_fill_count=0
topology_vector_policy=narrow
topology_vector_seed_probe=0
topology_weak_query_narrow_applied=0
vector_document_refill_attempts=0
vector_search_ann_candidate_budget_status=unavailable
vector_search_candidate_budget=4
vector_search_candidate_index_cache_used=0
vector_search_candidate_lookup_count=0
vector_search_distance_evaluation_budget=4
vector_search_exact_distance_evaluations_actual=4
vector_search_exact_distance_evaluations_status=observed
vector_search_materialized_rows=0
vector_search_result_budget=4
vector_search_rows_visited_actual=4
vector_search_rows_visited_status=observed
)golden",
                        .traceDebug = R"golden(topology_candidate_rescue_added_candidate_doc_ids=
topology_candidate_rescue_added_candidate_hashes=
topology_candidate_rescue_evidence_rescue_doc_ids=
topology_candidate_rescue_evidence_rescue_hashes=
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
topology_candidate_rescue_scored_candidate_doc_ids=
topology_shadow_removed_candidate_doc_ids=
topology_shadow_retained_candidate_doc_ids=
)golden",
                        .rawVector = R"golden(raw_hits=4
y1 rank=1 score=1.0000
y2 rank=2 score=0.9939
x2 rank=3 score=0.1104
x1 rank=4 score=0.0000
)golden",
                    };
                case Backend::Vec0L2:
                    return {
                        .fused = R"golden(y1 rank=0 score=0.1161 vector=0.0577
y2 rank=1 score=0.0994 vector=0.0535
x2 rank=2 score=0.0352 vector=0.0352
x1 rank=3 score=0.0312 vector=0.0312
)golden",
                        .debug = R"golden(relaxed_vector_primary_hit_count=4
relaxed_vector_retry_applied=0
relaxed_vector_retry_attempted=0
relaxed_vector_retry_enabled=0
relaxed_vector_retry_threshold=0.000
topology_candidate_rescue_added_candidates=0
topology_candidate_rescue_applied=0
topology_candidate_rescue_attempted=0
topology_candidate_rescue_duplicate_candidates=0
topology_candidate_rescue_evidence_rescues=0
topology_candidate_rescue_novel_candidates=0
topology_expansion_output_budget=4
topology_route_action=augment
topology_route_admission_denial_reason=work_observation_unavailable
topology_route_admission_eligible=0
topology_route_narrow_proposed=1
topology_route_work_ann_candidate_budget_actual=12
topology_route_work_exact_distance_evaluations_actual=0
topology_route_work_observed=0
topology_route_work_rows_visited_actual=0
topology_route_work_status=unavailable
topology_shadow_evaluated=0
topology_shadow_proposed_action=global
topology_shadow_removed_candidates=0
topology_shadow_retained_candidates=0
topology_vector_allowed_set_ann_applied=0
topology_vector_allowed_set_ann_fallback=0
topology_vector_filter_applied=0
topology_vector_filter_fallback=1
topology_vector_filter_matched=0
topology_vector_filter_removed=0
topology_vector_global_fill_count=0
topology_vector_policy=narrow
topology_vector_seed_probe=0
topology_weak_query_narrow_applied=0
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
)golden",
                        .traceDebug = R"golden(topology_candidate_rescue_added_candidate_doc_ids=
topology_candidate_rescue_added_candidate_hashes=
topology_candidate_rescue_evidence_rescue_doc_ids=
topology_candidate_rescue_evidence_rescue_hashes=
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
topology_candidate_rescue_scored_candidate_doc_ids=
topology_shadow_removed_candidate_doc_ids=
topology_shadow_retained_candidate_doc_ids=
)golden",
                        .rawVector = R"golden(raw_hits=4
y1 rank=1 score=1.0000
y2 rank=2 score=0.9939
x2 rank=3 score=0.1104
x1 rank=4 score=0.0000
)golden",
                    };
            }
            break;
    }
    return {};
}

} // namespace

// NOLINTEND(cppcoreguidelines-avoid-do-while, readability-function-cognitive-complexity)
