// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

// Road beam in the vector stage: `search.topology.road_hops` expands the selected route clusters
// along cluster roads for RouteOnly and Augment. Kept apart from the RouteOnly cells so those
// goldens stay byte-identical.
//
// Contract pinned here:
//   * road_hops = 0 (default) is the RouteOnly cell: only the routed cluster's documents, and no
//     topology_road_* debug key.
//   * Starting from the selected routes, each hop scores the unvisited road neighbours by
//     road weight x centroid similarity to the query, keeps `road_beam_width` of them, and every
//     centroid evaluation is counted in router work.
//   * The road-expanded documents are extra routed queries with their own quota share; the
//     certificate's allowed set, the admission and the routing policy fingerprint are unchanged.
//   * Roads absent from the snapshot: no expansion and topology_road_skip_reason.
//
// Fixture: four reciprocal pairs {a1,a2} {b1,b2} {c1,c2} {d1,d2}; the query (1,0) routes to A.
// Roads A-B (2.0), A-D (1.0), B-C (1.5); C has no road to A. From A the beam picks B over D
// (heavier road, similar centroid), then C from B.

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include <yams/search/search_engine.h>
#include <yams/search/topology_routing_session.h>
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

constexpr std::string_view kClusterA = "topology.cluster.a1";
constexpr std::string_view kClusterB = "topology.cluster.b1";
constexpr std::string_view kClusterC = "topology.cluster.c1";
constexpr std::string_view kClusterD = "topology.cluster.d1";

struct RoadDoc {
    const char* hash;
    const char* content;
    const char* neighbor;
    std::vector<float> embedding;
};

// Angles from the query (1,0): A ~6 deg, B ~48 deg, D ~-56 deg, C ~93 deg. Within-cluster
// neighbours are reciprocal and above the edge filter; no cross-cluster edge exists, so the
// components are exactly the four pairs and the roads below are hand-built.
const std::vector<RoadDoc>& roadDocs() {
    static const std::vector<RoadDoc> docs{
        {"a1", "alpha one", "a2", {1.0F, 0.0F}},  {"a2", "alpha two", "a1", {0.98F, 0.2F}},
        {"b1", "beta one", "b2", {0.7F, 0.7F}},   {"b2", "beta two", "b1", {0.6F, 0.8F}},
        {"c1", "gamma one", "c2", {0.0F, 1.0F}},  {"c2", "gamma two", "c1", {-0.1F, 0.95F}},
        {"d1", "delta one", "d2", {0.6F, -0.8F}}, {"d2", "delta two", "d1", {0.5F, -0.85F}},
    };
    return docs;
}

std::vector<yams::topology::ClusterRoad> fourClusterRoads() {
    // Sorted by (from asc, weight desc, to asc), both orientations, two per source at most.
    return {
        {std::string{kClusterA}, std::string{kClusterB}, 2.0},
        {std::string{kClusterA}, std::string{kClusterD}, 1.0},
        {std::string{kClusterB}, std::string{kClusterA}, 2.0},
        {std::string{kClusterB}, std::string{kClusterC}, 1.5},
        {std::string{kClusterC}, std::string{kClusterB}, 1.5},
        {std::string{kClusterD}, std::string{kClusterA}, 1.0},
    };
}

yams::topology::TopologyArtifactBatch buildFourClusterBatch(bool withRoads) {
    yams::topology::ConnectedComponentTopologyEngine engine;
    yams::topology::TopologyBuildConfig config;
    config.reciprocalOnly = true;
    config.minEdgeScore = 0.5;
    config.embeddingSpaceIdentity = "test-space-v1";
    std::vector<yams::topology::TopologyDocumentInput> inputs;
    for (const auto& doc : roadDocs()) {
        inputs.push_back(
            {.documentHash = doc.hash,
             .filePath = std::string{"/tmp/"} + doc.hash + ".txt",
             .embedding = doc.embedding,
             .neighbors = {{.documentHash = doc.neighbor, .score = 0.9F, .reciprocal = true}}});
    }
    auto built = engine.buildArtifacts(inputs, config);
    REQUIRE(built.has_value());
    auto batch = std::move(built.value());
    REQUIRE(batch.clusters.size() == 4);
    if (withRoads) {
        batch.roads = fourClusterRoads();
        batch.roadMaxPerCluster = 2;
    }
    return batch;
}

void seedFourClusterDocuments(TopologySearchFixture& fix) {
    for (const auto& doc : roadDocs()) {
        fix.addDocument(doc.hash, doc.content, doc.embedding);
    }
}

struct RoadCell {
    Policy policy{Policy::RouteOnly};
    std::size_t hops{0};
    std::size_t beamWidth{1};
    bool roads{true};
    std::size_t vectorMaxResults{8};
    std::size_t expansionOutputLimit{0};
};

SearchEngineConfig roadCellConfig(const RoadCell& cell) {
    auto config = topologyRoutingTestConfig(true);
    config.topologyRoutingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    config.topologySparseDenseAlpha = 0.0F;
    config.topologyNarrowMinBoundaryMargin = 0.0F;
    config.similarityThreshold = -1.0F;
    config.enableWeakQueryFanoutBoost = false;
    config.vectorMaxResults = cell.vectorMaxResults;
    config.topologyMaxDocs = 0;
    config.topologyExpansionOutputLimit = cell.expansionOutputLimit;
    config.topologyVectorPolicy = cell.policy;
    config.allowUncertifiedRouteOnly = true;
    config.topologyRoadHops = cell.hops;
    config.topologyRoadBeamWidth = cell.beamWidth;
    return config;
}

struct RoadSnapshot {
    std::vector<std::string> sortedHashes;
    std::map<std::string, std::string> debug;
};

std::vector<std::string> sorted(std::vector<std::string> hashes) {
    std::ranges::sort(hashes);
    return hashes;
}

std::vector<std::string> splitTabs(const std::string& joined) {
    std::vector<std::string> parts;
    std::size_t start = 0;
    while (start <= joined.size()) {
        const auto end = joined.find('\t', start);
        const auto part =
            joined.substr(start, end == std::string::npos ? std::string::npos : end - start);
        if (!part.empty()) {
            parts.push_back(part);
        }
        if (end == std::string::npos) {
            break;
        }
        start = end + 1;
    }
    return parts;
}

RoadSnapshot runRoadCell(const RoadCell& cell, bool stageTrace = true) {
    yams::test::ScopedEnvVar trace("YAMS_SEARCH_STAGE_TRACE",
                                   stageTrace ? std::optional<std::string>("1") : std::nullopt);
    TopologySearchFixture fix{Backend::ExactScan};
    seedFourClusterDocuments(fix);
    yams::topology::MetadataKgTopologyArtifactStore store(fix.repo, fix.kgStore);
    REQUIRE(store.storeBatch(buildFourClusterBatch(cell.roads)).has_value());
    auto generator = makeFixedGenerator({1.0F, 0.0F});
    auto response = runTopologySearch(fix, generator, roadCellConfig(cell), 8, "unmatched query");
    REQUIRE(response.has_value());
    RoadSnapshot snapshot;
    snapshot.sortedHashes = sorted(resultHashes(response.value()));
    for (const auto& [key, value] : response.value().debugStats) {
        if (key.ends_with("_ns")) {
            continue;
        }
        snapshot.debug[key] = value;
    }
    return snapshot;
}

std::string debugAt(const RoadSnapshot& snapshot, std::string_view key) {
    const auto it = snapshot.debug.find(std::string{key});
    return it == snapshot.debug.end() ? std::string{"<missing>"} : it->second;
}

std::size_t debugCount(const RoadSnapshot& snapshot, std::string_view key) {
    const auto value = debugAt(snapshot, key);
    REQUIRE(value != "<missing>");
    return static_cast<std::size_t>(std::stoull(value));
}

bool hasRoadDebugKeys(const RoadSnapshot& snapshot) {
    return std::ranges::any_of(snapshot.debug, [](const auto& entry) {
        return entry.first.starts_with("topology_road_");
    });
}

const std::vector<std::string> kA{"a1", "a2"};
const std::vector<std::string> kAB{"a1", "a2", "b1", "b2"};
const std::vector<std::string> kABC{"a1", "a2", "b1", "b2", "c1", "c2"};
const std::vector<std::string> kABD{"a1", "a2", "b1", "b2", "d1", "d2"};

// Session-level run against the stored batch: the seam that produces the road groups.
TopologyRoutingSessionResult runRoadSession(TopologySearchFixture& fix,
                                            const yams::topology::TopologyArtifactBatch& batch,
                                            std::size_t hops, std::size_t beamWidth) {
    TopologyRoutingSessionRequest request;
    request.query = "unmatched query";
    request.queryEmbedding = std::vector<float>{1.0F, 0.0F};
    request.queryEmbeddingSpaceIdentity = "test-space-v1";
    request.options.routingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    request.options.weakTier1Query = true;
    request.options.sparseDenseAlpha = 0.0F;
    request.options.narrowMinBoundaryMargin = 0.0F;
    request.options.maxClusters = 1;
    request.options.maxDocs = 0;
    request.options.collectRouteMembership = true;
    request.options.roadHops = hops;
    request.options.roadBeamWidth = beamWidth;
    request.snapshotCache = std::make_shared<TopologyRoutingSnapshotCache>([batch]() mutable {
        return Result<std::optional<yams::topology::TopologyArtifactBatch>>{
            std::optional<yams::topology::TopologyArtifactBatch>{std::move(batch)}};
    });
    return runTopologyRoutingSession(request, fix.repo, fix.kgStore);
}

std::vector<std::string> sortedUnion(const std::vector<std::unordered_set<std::string>>& groups) {
    std::vector<std::string> hashes;
    for (const auto& group : groups) {
        hashes.insert(hashes.end(), group.begin(), group.end());
    }
    return sorted(std::move(hashes));
}

} // namespace

TEST_CASE("Road adjacency is built from the loaded roads in both orientations",
          "[search][topology][road-beam][catch2]") {
    const auto batch = buildFourClusterBatch(true);
    auto adjacency = buildTopologyRoadAdjacency(batch);
    REQUIRE(adjacency.size() == batch.clusters.size());
    std::map<std::string, std::vector<std::pair<std::string, double>>> byId;
    for (std::size_t index = 0; index < adjacency.size(); ++index) {
        for (const auto& neighbor : adjacency[index]) {
            byId[batch.clusters[index].clusterId].emplace_back(
                batch.clusters[neighbor.clusterIndex].clusterId, neighbor.weight);
        }
    }
    // Per source: weight desc, then target id asc. The C-B road is reachable from C although
    // only one orientation would have been enough to list it.
    CHECK(byId[std::string{kClusterA}] ==
          std::vector<std::pair<std::string, double>>{{std::string{kClusterB}, 2.0},
                                                      {std::string{kClusterD}, 1.0}});
    CHECK(byId[std::string{kClusterB}] ==
          std::vector<std::pair<std::string, double>>{{std::string{kClusterA}, 2.0},
                                                      {std::string{kClusterC}, 1.5}});
    CHECK(byId[std::string{kClusterC}] ==
          std::vector<std::pair<std::string, double>>{{std::string{kClusterB}, 1.5}});
    CHECK(byId[std::string{kClusterD}] ==
          std::vector<std::pair<std::string, double>>{{std::string{kClusterA}, 1.0}});

    SECTION("a single orientation is enough and duplicates collapse") {
        auto oneWay = batch;
        oneWay.roads = {{std::string{kClusterA}, std::string{kClusterB}, 2.0},
                        {std::string{kClusterB}, std::string{kClusterA}, 2.0},
                        {std::string{kClusterB}, std::string{kClusterC}, 1.5}};
        const auto adj = buildTopologyRoadAdjacency(oneWay);
        std::size_t edges = 0;
        for (const auto& list : adj) {
            edges += list.size();
        }
        CHECK(edges == 4); // A-B both ways once, B-C both ways
    }

    SECTION("no roads gives an empty adjacency") {
        CHECK(buildTopologyRoadAdjacency(buildFourClusterBatch(false)).empty());
    }

    SECTION("the snapshot cache carries the adjacency and the road fingerprint") {
        TopologyRoutingSnapshotCache cache([batch]() mutable {
            return Result<std::optional<yams::topology::TopologyArtifactBatch>>{
                std::optional<yams::topology::TopologyArtifactBatch>{std::move(batch)}};
        });
        auto lookup = cache.get(0, false);
        REQUIRE(lookup.has_value());
        REQUIRE(lookup.value().snapshot);
        CHECK(lookup.value().snapshot->roadAdjacency.size() == 4);
        CHECK(lookup.value().snapshot->roadFingerprint == topologyRoadFingerprint(batch));
    }
}

TEST_CASE("Road beam options never change the routing policy fingerprint",
          "[search][topology][road-beam][fingerprint][catch2]") {
    const auto batch = buildFourClusterBatch(true);
    const auto representation = topologyRoutingRepresentationFingerprint(batch);
    TopologyRoutingOptions options;
    options.collectRouteMembership = true;
    const auto baseline = topologyRoutingPolicyFingerprint(representation, options);
    options.roadHops = 2;
    CHECK(topologyRoutingPolicyFingerprint(representation, options) == baseline);
    options.roadBeamWidth = 7;
    CHECK(topologyRoutingPolicyFingerprint(representation, options) == baseline);
    // A real policy lever still moves it, so the equality above is not vacuous.
    options.maxClusters += 1;
    CHECK(topologyRoutingPolicyFingerprint(representation, options) != baseline);

    SECTION("config defaults and option snapshot") {
        SearchEngineConfig config;
        CHECK(config.topologyRoadHops == 0U);
        CHECK(config.topologyRoadBeamWidth == 4U);
        config.topologyRoadHops = 3;
        config.topologyRoadBeamWidth = 2;
        // Shadow never consumes road groups, so the session is not asked to build them.
        config.topologyVectorPolicy = Policy::Shadow;
        CHECK(makeTopologyRoutingOptions(config, config.topologyRoutingMode, true, true).roadHops ==
              0U);
        config.topologyVectorPolicy = Policy::Augment;
        auto augment = makeTopologyRoutingOptions(config, config.topologyRoutingMode, true, true);
        CHECK(augment.roadHops == 3U);
        CHECK(augment.roadBeamWidth == 2U);
        // RouteOnly without the gate downgrades to Shadow and takes no roads with it.
        config.topologyVectorPolicy = Policy::RouteOnly;
        CHECK(makeTopologyRoutingOptions(config, config.topologyRoutingMode, true, true).roadHops ==
              0U);
        config.allowUncertifiedRouteOnly = true;
        CHECK(makeTopologyRoutingOptions(config, config.topologyRoutingMode, true, true).roadHops ==
              3U);
        // Corpus tuning preserves the operator's road knobs.
        SearchEngineConfig tuned;
        tuned.applyTopologyPolicyFrom(config);
        CHECK(tuned.topologyRoadHops == 3U);
        CHECK(tuned.topologyRoadBeamWidth == 2U);
    }
}

TEST_CASE("Road beam session keeps the certificate and expands along roads",
          "[search][topology][road-beam][session][catch2]") {
    const TopologyVectorEnvPin vectorEnv;
    TopologySearchFixture fix{Backend::ExactScan};
    seedFourClusterDocuments(fix);
    const auto batch = buildFourClusterBatch(true);

    const auto hops0 = runRoadSession(fix, batch, 0, 1);
    REQUIRE(hops0.certificate.selectedCoverIds == std::vector<std::string>{std::string{kClusterA}});
    const auto allowed0 =
        sorted(std::vector<std::string>(hops0.certificate.allowedDocumentHashes.begin(),
                                        hops0.certificate.allowedDocumentHashes.end()));
    CHECK(allowed0 == kA);
    CHECK(hops0.roadHopsApplied == 0);
    CHECK(hops0.roadExpandedClusterIds.empty());
    CHECK(hops0.roadExpandedDocumentHashGroups.empty());
    CHECK(hops0.roadWorkCentroidEvaluations == 0);
    CHECK(hops0.roadSkipReason.empty());

    const auto hops1 = runRoadSession(fix, batch, 1, 1);
    CHECK(hops1.certificate.selectedCoverIds == hops0.certificate.selectedCoverIds);
    CHECK(hops1.certificate.allowedDocumentHashes == hops0.certificate.allowedDocumentHashes);
    CHECK(hops1.certificate.allowedDocumentHashGroups ==
          hops0.certificate.allowedDocumentHashGroups);
    CHECK(hops1.certificate.admission.denialReason() == hops0.certificate.admission.denialReason());
    CHECK(hops1.certificate.routingPolicyFingerprint == hops0.certificate.routingPolicyFingerprint);
    CHECK(hops1.roadHopsApplied == 1);
    CHECK(hops1.roadExpandedClusterIds == std::vector<std::string>{std::string{kClusterB}});
    CHECK(sortedUnion(hops1.roadExpandedDocumentHashGroups) ==
          std::vector<std::string>{"b1", "b2"});
    // B and D were scored at hop 1; A is the start and is never re-scored.
    CHECK(hops1.roadWorkCentroidEvaluations == 2);
    CHECK(hops1.routeRepresentativeDistanceEvaluations ==
          hops0.routeRepresentativeDistanceEvaluations + 2);
    CHECK(hops1.routeExactRepresentativeDistanceEvaluations ==
          hops0.routeExactRepresentativeDistanceEvaluations + 2);

    const auto hops2 = runRoadSession(fix, batch, 2, 1);
    CHECK(hops2.certificate.allowedDocumentHashes == hops0.certificate.allowedDocumentHashes);
    CHECK(hops2.roadHopsApplied == 2);
    CHECK(hops2.roadExpandedClusterIds ==
          std::vector<std::string>{std::string{kClusterB}, std::string{kClusterC}});
    CHECK(sortedUnion(hops2.roadExpandedDocumentHashGroups) ==
          std::vector<std::string>{"b1", "b2", "c1", "c2"});
    CHECK(hops2.roadWorkCentroidEvaluations == 3);
    CHECK(hops2.routeRepresentativeDistanceEvaluations >
          hops1.routeRepresentativeDistanceEvaluations);

    SECTION("beam width 2 keeps both road neighbours of A") {
        const auto wide = runRoadSession(fix, batch, 1, 2);
        CHECK(wide.roadExpandedClusterIds ==
              std::vector<std::string>{std::string{kClusterB}, std::string{kClusterD}});
        CHECK(sortedUnion(wide.roadExpandedDocumentHashGroups) ==
              std::vector<std::string>{"b1", "b2", "d1", "d2"});
    }

    SECTION("hops beyond the reachable roads stop early") {
        const auto deep = runRoadSession(fix, batch, 5, 1);
        CHECK(deep.roadHopsApplied == 2);
        CHECK(deep.roadExpandedClusterIds ==
              std::vector<std::string>{std::string{kClusterB}, std::string{kClusterC}});
    }

    SECTION("roads absent: no expansion and a reason") {
        const auto none = runRoadSession(fix, buildFourClusterBatch(false), 2, 1);
        CHECK(none.certificate.allowedDocumentHashes == hops0.certificate.allowedDocumentHashes);
        CHECK(none.roadHopsApplied == 0);
        CHECK(none.roadExpandedClusterIds.empty());
        CHECK(none.roadSkipReason == "roads_absent");
    }
}

TEST_CASE("RouteOnly road beam hops 0 is the route-only cell",
          "[search][vector-stage][road-beam][catch2]") {
    const TopologyVectorEnvPin vectorEnv;
    for (const bool stageTrace : {false, true}) {
        DYNAMIC_SECTION((stageTrace ? "trace" : "no-trace")) {
            const auto actual =
                runRoadCell(RoadCell{.policy = Policy::RouteOnly, .hops = 0}, stageTrace);
            CHECK(actual.sortedHashes == kA);
            CHECK(debugAt(actual, "vector_candidate_source_effective") == "route_only");
            CHECK(debugAt(actual, "topology_route_selected_cover_count") == "1");
            CHECK_FALSE(hasRoadDebugKeys(actual));
        }
    }
}

TEST_CASE("RouteOnly road beam expands along roads hop by hop",
          "[search][vector-stage][road-beam][catch2]") {
    const TopologyVectorEnvPin vectorEnv;
    const auto hops0 = runRoadCell(RoadCell{.policy = Policy::RouteOnly, .hops = 0});
    const auto hops1 = runRoadCell(RoadCell{.policy = Policy::RouteOnly, .hops = 1});
    const auto hops2 = runRoadCell(RoadCell{.policy = Policy::RouteOnly, .hops = 2});

    CHECK(hops1.sortedHashes == kAB);
    CHECK(debugAt(hops1, "vector_candidate_source_effective") == "route_only");
    CHECK(debugAt(hops1, "topology_road_hops_applied") == "1");
    CHECK(debugAt(hops1, "topology_road_clusters_added") == "1");
    CHECK(debugAt(hops1, "topology_road_candidates_added") == "2");
    CHECK(debugAt(hops1, "topology_road_work_centroid_evaluations") == "2");
    CHECK(splitTabs(debugAt(hops1, "topology_road_expanded_cluster_ids")) ==
          std::vector<std::string>{std::string{kClusterB}});

    CHECK(hops2.sortedHashes == kABC);
    CHECK(debugAt(hops2, "topology_road_hops_applied") == "2");
    CHECK(debugAt(hops2, "topology_road_clusters_added") == "2");
    CHECK(debugAt(hops2, "topology_road_candidates_added") == "4");
    CHECK(debugAt(hops2, "topology_road_work_centroid_evaluations") == "3");

    // Router work grows with every hop; the routed vector work grows with the extra queries.
    const auto representative = "topology_route_representative_distance_evaluations";
    CHECK(debugCount(hops1, representative) == debugCount(hops0, representative) + 2);
    CHECK(debugCount(hops2, representative) == debugCount(hops0, representative) + 3);
    CHECK(debugCount(hops1, "vector_search_rows_visited_actual") >
          debugCount(hops0, "vector_search_rows_visited_actual"));
    CHECK(debugCount(hops2, "vector_search_rows_visited_actual") >
          debugCount(hops1, "vector_search_rows_visited_actual"));

    // Certified narrowing is never widened by roads.
    for (const auto* key :
         {"topology_route_selected_cover_count", "topology_route_selected_cover_documents",
          "topology_route_materialized_cover_documents", "topology_route_action",
          "topology_route_admission_eligible", "topology_route_admission_denial_reason",
          "topology_route_certified"}) {
        CHECK(debugAt(hops1, key) == debugAt(hops0, key));
        CHECK(debugAt(hops2, key) == debugAt(hops0, key));
    }

    SECTION("beam width 2 at one hop takes both road neighbours") {
        const auto wide =
            runRoadCell(RoadCell{.policy = Policy::RouteOnly, .hops = 1, .beamWidth = 2});
        CHECK(wide.sortedHashes == kABD);
        CHECK(debugAt(wide, "topology_road_clusters_added") == "2");
    }

    SECTION("roads absent from the snapshot") {
        const auto none =
            runRoadCell(RoadCell{.policy = Policy::RouteOnly, .hops = 2, .roads = false});
        CHECK(none.sortedHashes == kA);
        CHECK(debugAt(none, "topology_road_hops_applied") == "0");
        CHECK(debugAt(none, "topology_road_clusters_added") == "0");
        CHECK(debugAt(none, "topology_road_candidates_added") == "0");
        CHECK(debugAt(none, "topology_road_skip_reason") == "roads_absent");
    }
}

TEST_CASE("Augment road beam adds the road cluster's documents as rescues",
          "[search][vector-stage][road-beam][augment][catch2]") {
    const TopologyVectorEnvPin vectorEnv;
    // Global ANN returns A only (two results); the routed expansion may return four.
    const RoadCell base{
        .policy = Policy::Augment, .hops = 0, .vectorMaxResults = 2, .expansionOutputLimit = 4};
    auto withRoads = base;
    withRoads.hops = 1;

    const auto hops0 = runRoadCell(base);
    const auto hops1 = runRoadCell(withRoads);

    CHECK(debugAt(hops0, "vector_candidate_source_effective") == "augment");
    CHECK(hops0.sortedHashes == kA);
    CHECK(debugAt(hops0, "topology_candidate_rescue_added_candidates") == "0");
    CHECK_FALSE(hasRoadDebugKeys(hops0));

    CHECK(debugAt(hops1, "vector_candidate_source_effective") == "augment");
    CHECK(hops1.sortedHashes == kAB);
    CHECK(debugAt(hops1, "topology_candidate_rescue_applied") == "1");
    CHECK(debugAt(hops1, "topology_candidate_rescue_added_candidates") == "2");
    CHECK(sorted(splitTabs(debugAt(hops1, "topology_candidate_rescue_added_candidate_hashes"))) ==
          std::vector<std::string>{"b1", "b2"});
    CHECK(debugAt(hops1, "topology_road_hops_applied") == "1");
    CHECK(debugAt(hops1, "topology_road_candidates_added") == "2");
    CHECK(debugAt(hops1, "topology_route_selected_cover_count") ==
          debugAt(hops0, "topology_route_selected_cover_count"));
    CHECK(debugAt(hops1, "topology_route_action") == debugAt(hops0, "topology_route_action"));
}

TEST_CASE("Shadow and Narrow take no road groups", "[search][vector-stage][road-beam][catch2]") {
    const TopologyVectorEnvPin vectorEnv;
    for (const auto policy : {Policy::Shadow, Policy::Narrow}) {
        DYNAMIC_SECTION(SearchEngineConfig::topologyVectorPolicyToString(policy)) {
            const auto actual = runRoadCell(RoadCell{.policy = policy, .hops = 2});
            CHECK_FALSE(hasRoadDebugKeys(actual));
            CHECK(debugAt(actual, "topology_candidate_rescue_added_candidates") == "0");
        }
    }
}

// NOLINTEND(cppcoreguidelines-avoid-do-while, readability-function-cognitive-complexity)
