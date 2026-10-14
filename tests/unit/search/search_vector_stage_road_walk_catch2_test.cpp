// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

// Road walk in the vector stage: `search.topology.road_walk_window` expands every routed hit
// (certified route hits and road-beam hits) by +-window positions along its cluster's walk order
// (`ClusterArtifact::orderedMembers`) for RouteOnly and Augment. Kept apart from the RouteOnly and
// road beam cells so those goldens stay byte-identical.
//
// Contract pinned here:
//   * road_walk_window = 0 (default) is the road beam cell: same documents, same debug surface,
//     and no topology_road_walk_* key.
//   * The snapshot loader resolves each cluster's order to member hashes; clusters without an
//     order resolve to nothing and are counted, never walked.
//   * Every routed hit anchors a window bounded to its cluster's order; the walked documents are
//     one extra routed query (road group accounting) and never enter the certificate's allowed
//     set, the admission or the routing policy fingerprint.
//   * Shadow and Narrow never walk.
//
// Fixture: A = {a1..a6} and B = {b1..b4}, each a reciprocal chain, road A-B (2.0); the query (1,0)
// routes to A and the beam reaches B. Time order (eventTime) deliberately differs from similarity
// order: A walks a1,a3,a4,a5,a6,a2 and B walks b3,b1,b4,b2. With a vector limit of 2 the hits are
// a1,a2 (A) and b1,b2 (B); window 1 adds a3,a6,b3,b4 and window 2 adds a4,a5 as well.

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include <yams/search/search_engine.h>
#include <yams/search/search_tracing.h>
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
using yams::topology::RoadOrderKind;

constexpr std::string_view kClusterA = "topology.cluster.a1";
constexpr std::string_view kClusterB = "topology.cluster.b1";

struct WalkDoc {
    const char* hash;
    const char* content;
    std::vector<const char*> neighbors;
    std::vector<float> embedding;
    std::int64_t eventTime;
};

// Similarity to the query (1,0) decreases a1 > a2 > ... > a6 > b1 > ... > b4. Event times put a1
// first and a2 last in A, and b3, b1, b4, b2 in B, so the walk and the ranking disagree.
const std::vector<WalkDoc>& walkDocs() {
    static const std::vector<WalkDoc> docs{
        {"a1", "alpha one", {"a2"}, {1.0F, 0.0F}, 10},
        {"a2", "alpha two", {"a1", "a3"}, {0.98F, 0.2F}, 60},
        {"a3", "alpha three", {"a2", "a4"}, {0.94F, 0.34F}, 20},
        {"a4", "alpha four", {"a3", "a5"}, {0.9F, 0.44F}, 30},
        {"a5", "alpha five", {"a4", "a6"}, {0.85F, 0.53F}, 40},
        {"a6", "alpha six", {"a5"}, {0.8F, 0.6F}, 50},
        {"b1", "beta one", {"b2"}, {0.7F, 0.7F}, 20},
        {"b2", "beta two", {"b1", "b3"}, {0.6F, 0.8F}, 40},
        {"b3", "beta three", {"b2", "b4"}, {0.5F, 0.87F}, 10},
        {"b4", "beta four", {"b3"}, {0.42F, 0.9F}, 30},
    };
    return docs;
}

const std::vector<std::string> kOrderA{"a1", "a3", "a4", "a5", "a6", "a2"};
const std::vector<std::string> kOrderB{"b3", "b1", "b4", "b2"};

struct WalkBatchOptions {
    bool roads{true};
    bool orderA{true};
    bool orderB{true};
};

yams::topology::TopologyArtifactBatch buildWalkBatch(const WalkBatchOptions& options = {}) {
    yams::topology::ConnectedComponentTopologyEngine engine;
    yams::topology::TopologyBuildConfig config;
    config.reciprocalOnly = true;
    config.minEdgeScore = 0.5;
    config.embeddingSpaceIdentity = "test-space-v1";
    config.roadOrder = RoadOrderKind::Time;
    std::vector<yams::topology::TopologyDocumentInput> inputs;
    for (const auto& doc : walkDocs()) {
        yams::topology::TopologyDocumentInput input{
            .documentHash = doc.hash,
            .filePath = std::string{"/tmp/"} + doc.hash + ".txt",
            .embedding = doc.embedding,
        };
        for (const auto* neighbor : doc.neighbors) {
            input.neighbors.push_back(
                {.documentHash = neighbor, .score = 0.9F, .reciprocal = true});
        }
        input.eventTime = doc.eventTime;
        inputs.push_back(std::move(input));
    }
    auto built = engine.buildArtifacts(inputs, config);
    REQUIRE(built.has_value());
    auto batch = std::move(built.value());
    REQUIRE(batch.clusters.size() == 2);
    for (auto& cluster : batch.clusters) {
        const bool keep = cluster.clusterId == kClusterA ? options.orderA : options.orderB;
        if (!keep) {
            cluster.orderedMembers.clear();
            cluster.orderKind = RoadOrderKind::None;
        }
    }
    if (options.roads) {
        batch.roads = {{std::string{kClusterA}, std::string{kClusterB}, 2.0},
                       {std::string{kClusterB}, std::string{kClusterA}, 2.0}};
        batch.roadMaxPerCluster = 1;
    }
    return batch;
}

std::vector<std::string> orderOf(const yams::topology::ClusterArtifact& cluster) {
    std::vector<std::string> order;
    for (const auto index : cluster.orderedMembers) {
        order.push_back(cluster.memberDocumentHashes.at(index));
    }
    return order;
}

void seedWalkDocuments(TopologySearchFixture& fix) {
    for (const auto& doc : walkDocs()) {
        fix.addDocument(doc.hash, doc.content, doc.embedding);
    }
}

struct WalkCell {
    Policy policy{Policy::RouteOnly};
    std::size_t hops{1};
    std::size_t window{0};
    WalkBatchOptions batch{};
    std::size_t vectorMaxResults{2};
};

SearchEngineConfig walkCellConfig(const WalkCell& cell) {
    auto config = topologyRoutingTestConfig(true);
    config.topologyRoutingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    config.topologySparseDenseAlpha = 0.0F;
    config.topologyNarrowMinBoundaryMargin = 0.0F;
    config.similarityThreshold = -1.0F;
    config.enableWeakQueryFanoutBoost = false;
    config.vectorMaxResults = cell.vectorMaxResults;
    config.topologyMaxDocs = 0;
    config.topologyExpansionOutputLimit = 0;
    config.topologyVectorPolicy = cell.policy;
    config.allowUncertifiedRouteOnly = true;
    config.topologyRoadHops = cell.hops;
    config.topologyRoadBeamWidth = 1;
    config.topologyRoadWalkWindow = cell.window;
    return config;
}

struct WalkSnapshot {
    std::vector<std::string> sortedHashes;
    std::map<std::string, std::string> debug;
};

std::vector<std::string> sorted(std::vector<std::string> hashes) {
    std::ranges::sort(hashes);
    return hashes;
}

std::vector<std::string> sortedSet(const std::unordered_set<std::string>& hashes) {
    return sorted(std::vector<std::string>(hashes.begin(), hashes.end()));
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

WalkSnapshot runWalkCell(const WalkCell& cell, bool stageTrace = true) {
    yams::test::ScopedEnvVar trace("YAMS_SEARCH_STAGE_TRACE",
                                   stageTrace ? std::optional<std::string>("1") : std::nullopt);
    TopologySearchFixture fix{Backend::ExactScan};
    seedWalkDocuments(fix);
    yams::topology::MetadataKgTopologyArtifactStore store(fix.repo, fix.kgStore);
    REQUIRE(store.storeBatch(buildWalkBatch(cell.batch)).has_value());
    auto generator = makeFixedGenerator({1.0F, 0.0F});
    auto response = runTopologySearch(fix, generator, walkCellConfig(cell), 16, "unmatched query");
    REQUIRE(response.has_value());
    WalkSnapshot snapshot;
    snapshot.sortedHashes = sorted(resultHashes(response.value()));
    for (const auto& [key, value] : response.value().debugStats) {
        if (key.ends_with("_ns")) {
            continue;
        }
        snapshot.debug[key] = value;
    }
    return snapshot;
}

std::string debugAt(const WalkSnapshot& snapshot, std::string_view key) {
    const auto it = snapshot.debug.find(std::string{key});
    return it == snapshot.debug.end() ? std::string{"<missing>"} : it->second;
}

std::size_t debugCount(const WalkSnapshot& snapshot, std::string_view key) {
    const auto value = debugAt(snapshot, key);
    REQUIRE(value != "<missing>");
    return static_cast<std::size_t>(std::stoull(value));
}

bool hasWalkDebugKeys(const WalkSnapshot& snapshot) {
    return std::ranges::any_of(snapshot.debug, [](const auto& entry) {
        return entry.first.starts_with("topology_road_walk_") ||
               entry.first == "topology_order_unavailable_reason";
    });
}

const std::vector<std::string> kHits{"a1", "a2", "b1", "b2"};
const std::vector<std::string> kWalk1{"a1", "a2", "a3", "a6", "b1", "b2", "b3", "b4"};
const std::vector<std::string> kWalk1OrderedAOnly{"a1", "a2", "a3", "a6", "b1", "b2"};
const std::vector<std::string> kAll{"a1", "a2", "a3", "a4", "a5", "a6", "b1", "b2", "b3", "b4"};

TopologyRoutingSessionResult runWalkSession(TopologySearchFixture& fix,
                                            const yams::topology::TopologyArtifactBatch& batch,
                                            std::size_t hops, std::size_t window) {
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
    request.options.roadBeamWidth = 1;
    request.options.roadWalkWindow = window;
    request.snapshotCache = std::make_shared<TopologyRoutingSnapshotCache>([batch]() mutable {
        return Result<std::optional<yams::topology::TopologyArtifactBatch>>{
            std::optional<yams::topology::TopologyArtifactBatch>{std::move(batch)}};
    });
    return runTopologyRoutingSession(request, fix.repo, fix.kgStore);
}

std::vector<std::string> walk(const TopologyRoadWalkIndex& index,
                              const std::vector<std::string>& anchors, std::size_t window,
                              std::size_t* anchorsExpanded = nullptr) {
    std::unordered_set<std::string> exclude(anchors.begin(), anchors.end());
    return sortedSet(walkTopologyRoadOrders(index, anchors, window, exclude, anchorsExpanded));
}

} // namespace

TEST_CASE("The build stores a time order that differs from the similarity ranking",
          "[search][topology][road-walk][fixture][catch2]") {
    const auto batch = buildWalkBatch();
    for (const auto& cluster : batch.clusters) {
        CHECK(cluster.orderKind == RoadOrderKind::Time);
        CHECK(orderOf(cluster) == (cluster.clusterId == kClusterA ? kOrderA : kOrderB));
    }
    const auto unorderedB = buildWalkBatch({.orderB = false});
    for (const auto& cluster : unorderedB.clusters) {
        if (cluster.clusterId == kClusterB) {
            CHECK(cluster.orderKind == RoadOrderKind::None);
            CHECK(cluster.orderedMembers.empty());
        }
    }
}

TEST_CASE("Snapshot loader resolves each cluster's order to member hashes",
          "[search][topology][road-walk][snapshot][catch2]") {
    auto batch = buildWalkBatch({.orderB = false});
    const auto orders = buildTopologyOrderedMemberHashes(batch);
    REQUIRE(orders.size() == batch.clusters.size());
    for (std::size_t index = 0; index < batch.clusters.size(); ++index) {
        std::vector<std::string> resolved(orders[index].begin(), orders[index].end());
        if (batch.clusters[index].clusterId == kClusterA) {
            CHECK(resolved == kOrderA);
        } else {
            CHECK(resolved.empty());
        }
    }

    SECTION("an invalid permutation resolves to nothing") {
        auto broken = buildWalkBatch();
        for (auto& cluster : broken.clusters) {
            if (cluster.clusterId == kClusterA) {
                cluster.orderedMembers.back() = cluster.orderedMembers.front();
            }
        }
        const auto resolved = buildTopologyOrderedMemberHashes(broken);
        for (std::size_t index = 0; index < broken.clusters.size(); ++index) {
            if (broken.clusters[index].clusterId == kClusterA) {
                CHECK(resolved[index].empty());
            } else {
                CHECK(resolved[index].size() == kOrderB.size());
            }
        }
    }

    SECTION("the cache carries the orders and the order diagnostic") {
        batch.orderUnavailableReason = "order_index_repeated:" + std::string{kClusterB};
        TopologyRoutingSnapshotCache cache([batch]() mutable {
            return Result<std::optional<yams::topology::TopologyArtifactBatch>>{
                std::optional<yams::topology::TopologyArtifactBatch>{std::move(batch)}};
        });
        auto lookup = cache.get(0, false);
        REQUIRE(lookup.has_value());
        REQUIRE(lookup.value().snapshot);
        const auto& snapshot = *lookup.value().snapshot;
        REQUIRE(snapshot.orderedMemberHashes.size() == 2);
        const auto a = snapshot.clustersById.at(kClusterA);
        const auto b = snapshot.clustersById.at(kClusterB);
        CHECK(std::vector<std::string>(snapshot.orderedMemberHashes[a].begin(),
                                       snapshot.orderedMemberHashes[a].end()) == kOrderA);
        CHECK(snapshot.orderedMemberHashes[b].empty());
        CHECK(snapshot.artifacts->orderUnavailableReason ==
              "order_index_repeated:" + std::string{kClusterB});
    }
}

TEST_CASE("Road walk window never changes the routing policy fingerprint",
          "[search][topology][road-walk][fingerprint][catch2]") {
    const auto batch = buildWalkBatch();
    const auto representation = topologyRoutingRepresentationFingerprint(batch);
    TopologyRoutingOptions options;
    options.collectRouteMembership = true;
    const auto baseline = topologyRoutingPolicyFingerprint(representation, options);
    options.roadWalkWindow = 3;
    CHECK(topologyRoutingPolicyFingerprint(representation, options) == baseline);
    options.roadHops = 2;
    CHECK(topologyRoutingPolicyFingerprint(representation, options) == baseline);
    options.maxClusters += 1;
    CHECK(topologyRoutingPolicyFingerprint(representation, options) != baseline);

    SECTION("config default and option snapshot") {
        SearchEngineConfig config;
        CHECK(config.topologyRoadWalkWindow == 0U);
        config.topologyRoadWalkWindow = 2;
        config.topologyVectorPolicy = Policy::Shadow;
        CHECK(makeTopologyRoutingOptions(config, config.topologyRoutingMode, true, true)
                  .roadWalkWindow == 0U);
        config.topologyVectorPolicy = Policy::Narrow;
        CHECK(makeTopologyRoutingOptions(config, config.topologyRoutingMode, true, true)
                  .roadWalkWindow == 0U);
        config.topologyVectorPolicy = Policy::Augment;
        CHECK(makeTopologyRoutingOptions(config, config.topologyRoutingMode, true, true)
                  .roadWalkWindow == 2U);
        config.topologyVectorPolicy = Policy::RouteOnly;
        CHECK(makeTopologyRoutingOptions(config, config.topologyRoutingMode, true, true)
                  .roadWalkWindow == 0U);
        config.allowUncertifiedRouteOnly = true;
        CHECK(makeTopologyRoutingOptions(config, config.topologyRoutingMode, true, true)
                  .roadWalkWindow == 2U);
        SearchEngineConfig tuned;
        tuned.applyTopologyPolicyFrom(config);
        CHECK(tuned.topologyRoadWalkWindow == 2U);
    }
}

TEST_CASE("Road walk session keeps the certificate and indexes the walkable orders",
          "[search][topology][road-walk][session][catch2]") {
    const TopologyVectorEnvPin vectorEnv;
    TopologySearchFixture fix{Backend::ExactScan};
    seedWalkDocuments(fix);
    const auto batch = buildWalkBatch();

    const auto window0 = runWalkSession(fix, batch, 1, 0);
    REQUIRE(window0.certificate.selectedCoverIds ==
            std::vector<std::string>{std::string{kClusterA}});
    CHECK(window0.roadWalkWindowRequested == 0);
    CHECK(window0.roadWalkClustersUnordered == 0);
    CHECK_FALSE(window0.roadWalkIndex);
    CHECK(window0.orderUnavailableReason.empty());

    const auto window1 = runWalkSession(fix, batch, 1, 1);
    CHECK(window1.certificate.selectedCoverIds == window0.certificate.selectedCoverIds);
    CHECK(window1.certificate.allowedDocumentHashes == window0.certificate.allowedDocumentHashes);
    CHECK(window1.certificate.allowedDocumentHashGroups ==
          window0.certificate.allowedDocumentHashGroups);
    CHECK(window1.certificate.admission.denialReason() ==
          window0.certificate.admission.denialReason());
    CHECK(window1.certificate.routingPolicyFingerprint ==
          window0.certificate.routingPolicyFingerprint);
    CHECK(window1.roadExpandedClusterIds == window0.roadExpandedClusterIds);
    CHECK(window1.roadExpandedDocumentHashGroups == window0.roadExpandedDocumentHashGroups);
    CHECK(window1.roadWorkCentroidEvaluations == window0.roadWorkCentroidEvaluations);
    CHECK(window1.roadWalkWindowRequested == 1);
    CHECK(window1.roadWalkClustersUnordered == 0);
    REQUIRE(window1.roadWalkIndex);
    const auto& index = *window1.roadWalkIndex;
    // The selected route first, then the road-expanded cluster.
    REQUIRE(index.clusterIndices.size() == 2);
    CHECK(index.snapshot->artifacts->clusters[index.clusterIndices[0]].clusterId == kClusterA);
    CHECK(index.snapshot->artifacts->clusters[index.clusterIndices[1]].clusterId == kClusterB);
    CHECK(index.positions.size() == kAll.size());

    std::size_t anchors = 0;
    CHECK(walk(index, kHits, 1, &anchors) == std::vector<std::string>{"a3", "a6", "b3", "b4"});
    CHECK(anchors == 4);
    CHECK(walk(index, kHits, 2, &anchors) ==
          std::vector<std::string>{"a3", "a4", "a5", "a6", "b3", "b4"});
    CHECK(anchors == 4);
    // Bounded to the cluster: a wide window never leaves A or B, and the exclusion holds.
    CHECK(walk(index, kHits, 100) == std::vector<std::string>{"a3", "a4", "a5", "a6", "b3", "b4"});
    CHECK(walk(index, kHits, 0).empty());
    // A hit outside every walkable order anchors nothing.
    CHECK(walk(index, {"zz"}, 2, &anchors).empty());
    CHECK(anchors == 0);

    SECTION("a cluster without an order is counted and never walked") {
        const auto partial = runWalkSession(fix, buildWalkBatch({.orderB = false}), 1, 1);
        CHECK(partial.roadWalkClustersUnordered == 1);
        REQUIRE(partial.roadWalkIndex);
        REQUIRE(partial.roadWalkIndex->clusterIndices.size() == 1);
        CHECK(partial.roadWalkIndex->snapshot->artifacts
                  ->clusters[partial.roadWalkIndex->clusterIndices[0]]
                  .clusterId == kClusterA);
        CHECK(walk(*partial.roadWalkIndex, kHits, 1) == std::vector<std::string>{"a3", "a6"});
    }

    SECTION("no order anywhere: every walkable cluster is counted and there is no index") {
        const auto none =
            runWalkSession(fix, buildWalkBatch({.orderA = false, .orderB = false}), 1, 1);
        CHECK(none.roadWalkClustersUnordered == 2);
        CHECK_FALSE(none.roadWalkIndex);
        CHECK(none.certificate.allowedDocumentHashes == window0.certificate.allowedDocumentHashes);
    }

    SECTION("without the beam the selected route is still walkable") {
        const auto noBeam = runWalkSession(fix, batch, 0, 1);
        REQUIRE(noBeam.roadWalkIndex);
        REQUIRE(noBeam.roadWalkIndex->clusterIndices.size() == 1);
        CHECK(walk(*noBeam.roadWalkIndex, {"a1", "a2"}, 1) == std::vector<std::string>{"a3", "a6"});
    }

    SECTION("the order diagnostic reaches the session and the debug surface only when set") {
        auto flagged = batch;
        flagged.orderUnavailableReason = "order_length_mismatch:" + std::string{kClusterB};
        const auto session = runWalkSession(fix, flagged, 1, 1);
        CHECK(session.orderUnavailableReason == flagged.orderUnavailableReason);

        SearchEngineConfig config;
        SearchResponse response;
        recordTopologyRoutingDebug(response, config, config.topologyRoutingMode, session, "", 0);
        CHECK(response.debugStats.at("topology_order_unavailable_reason") ==
              flagged.orderUnavailableReason);
        CHECK(response.debugStats.at("topology_road_walk_clusters_unordered") == "0");

        SearchResponse clean;
        recordTopologyRoutingDebug(clean, config, config.topologyRoutingMode, window0, "", 0);
        CHECK_FALSE(clean.debugStats.contains("topology_order_unavailable_reason"));
        CHECK_FALSE(clean.debugStats.contains("topology_road_walk_clusters_unordered"));
    }
}

TEST_CASE("RouteOnly road walk window 0 is the road beam cell",
          "[search][vector-stage][road-walk][catch2]") {
    const TopologyVectorEnvPin vectorEnv;
    for (const bool stageTrace : {false, true}) {
        DYNAMIC_SECTION((stageTrace ? "trace" : "no-trace")) {
            const auto actual = runWalkCell(WalkCell{.policy = Policy::RouteOnly}, stageTrace);
            CHECK(actual.sortedHashes == kHits);
            CHECK(debugAt(actual, "vector_candidate_source_effective") == "route_only");
            CHECK(debugAt(actual, "topology_road_hops_applied") == "1");
            CHECK(debugAt(actual, "topology_road_candidates_added") == "2");
            CHECK_FALSE(hasWalkDebugKeys(actual));
        }
    }
}

TEST_CASE("RouteOnly road walk expands each hit along its cluster's order",
          "[search][vector-stage][road-walk][catch2]") {
    const TopologyVectorEnvPin vectorEnv;
    const auto window0 = runWalkCell(WalkCell{.policy = Policy::RouteOnly, .window = 0});
    const auto window1 = runWalkCell(WalkCell{.policy = Policy::RouteOnly, .window = 1});
    const auto window2 = runWalkCell(WalkCell{.policy = Policy::RouteOnly, .window = 2});

    CHECK(window1.sortedHashes == kWalk1);
    CHECK(debugAt(window1, "vector_candidate_source_effective") == "route_only");
    CHECK(debugAt(window1, "topology_road_walk_window_applied") == "1");
    CHECK(debugAt(window1, "topology_road_walk_anchors") == "4");
    CHECK(debugAt(window1, "topology_road_walk_candidates_added") == "4");
    CHECK(debugAt(window1, "topology_road_walk_clusters_unordered") == "0");
    // The beam itself is unchanged by the walk.
    CHECK(debugAt(window1, "topology_road_hops_applied") == "1");
    CHECK(debugAt(window1, "topology_road_clusters_added") == "1");
    CHECK(debugAt(window1, "topology_road_candidates_added") == "2");

    CHECK(window2.sortedHashes == kAll);
    CHECK(debugAt(window2, "topology_road_walk_window_applied") == "2");
    CHECK(debugAt(window2, "topology_road_walk_anchors") == "4");
    CHECK(debugAt(window2, "topology_road_walk_candidates_added") == "6");

    // The walked documents are scored like road candidates: routed vector work grows.
    CHECK(debugCount(window1, "vector_search_rows_visited_actual") >
          debugCount(window0, "vector_search_rows_visited_actual"));
    CHECK(debugCount(window2, "vector_search_rows_visited_actual") >
          debugCount(window1, "vector_search_rows_visited_actual"));
    // Router work is untouched: the walk evaluates no centroid.
    for (const auto* key : {"topology_route_representative_distance_evaluations",
                            "topology_road_work_centroid_evaluations"}) {
        CHECK(debugAt(window1, key) == debugAt(window0, key));
        CHECK(debugAt(window2, key) == debugAt(window0, key));
    }

    // Certified narrowing is never widened by the walk.
    for (const auto* key :
         {"topology_route_selected_cover_count", "topology_route_selected_cover_documents",
          "topology_route_materialized_cover_documents", "topology_route_action",
          "topology_route_admission_eligible", "topology_route_admission_denial_reason",
          "topology_route_certified"}) {
        CHECK(debugAt(window1, key) == debugAt(window0, key));
        CHECK(debugAt(window2, key) == debugAt(window0, key));
    }

    SECTION("a cluster without an order contributes nothing and is counted") {
        const auto partial = runWalkCell(
            WalkCell{.policy = Policy::RouteOnly, .window = 1, .batch = {.orderB = false}});
        CHECK(partial.sortedHashes == kWalk1OrderedAOnly);
        CHECK(debugAt(partial, "topology_road_walk_anchors") == "2");
        CHECK(debugAt(partial, "topology_road_walk_candidates_added") == "2");
        CHECK(debugAt(partial, "topology_road_walk_clusters_unordered") == "1");
    }

    SECTION("no order anywhere is the road beam cell plus the counters") {
        const auto none = runWalkCell(WalkCell{
            .policy = Policy::RouteOnly, .window = 2, .batch = {.orderA = false, .orderB = false}});
        CHECK(none.sortedHashes == kHits);
        CHECK(debugAt(none, "topology_road_walk_window_applied") == "0");
        CHECK(debugAt(none, "topology_road_walk_anchors") == "0");
        CHECK(debugAt(none, "topology_road_walk_candidates_added") == "0");
        CHECK(debugAt(none, "topology_road_walk_clusters_unordered") == "2");
    }

    SECTION("without the beam the walk still follows the selected route's order") {
        const auto noBeam =
            runWalkCell(WalkCell{.policy = Policy::RouteOnly, .hops = 0, .window = 1});
        CHECK(noBeam.sortedHashes == std::vector<std::string>{"a1", "a2", "a3", "a6"});
        CHECK(debugAt(noBeam, "topology_road_walk_anchors") == "2");
        CHECK(debugAt(noBeam, "topology_road_walk_candidates_added") == "2");
        CHECK(debugAt(noBeam, "topology_road_walk_clusters_unordered") == "0");
        CHECK(debugAt(noBeam, "topology_road_hops_applied") == "<missing>");
    }
}

TEST_CASE("Augment road walk adds the walked documents as rescues",
          "[search][vector-stage][road-walk][augment][catch2]") {
    const TopologyVectorEnvPin vectorEnv;
    const auto window0 = runWalkCell(WalkCell{.policy = Policy::Augment, .window = 0});
    const auto window1 = runWalkCell(WalkCell{.policy = Policy::Augment, .window = 1});

    CHECK(debugAt(window0, "vector_candidate_source_effective") == "augment");
    CHECK(window0.sortedHashes == kHits);
    CHECK(debugAt(window0, "topology_candidate_rescue_added_candidates") == "2");
    CHECK_FALSE(hasWalkDebugKeys(window0));

    CHECK(debugAt(window1, "vector_candidate_source_effective") == "augment");
    CHECK(window1.sortedHashes == kWalk1);
    CHECK(debugAt(window1, "topology_candidate_rescue_applied") == "1");
    CHECK(debugAt(window1, "topology_candidate_rescue_added_candidates") == "6");
    CHECK(sorted(splitTabs(debugAt(window1, "topology_candidate_rescue_added_candidate_hashes"))) ==
          std::vector<std::string>{"a3", "a6", "b1", "b2", "b3", "b4"});
    CHECK(debugAt(window1, "topology_road_walk_window_applied") == "1");
    CHECK(debugAt(window1, "topology_road_walk_anchors") == "4");
    CHECK(debugAt(window1, "topology_road_walk_candidates_added") == "4");
    CHECK(debugAt(window1, "topology_route_selected_cover_count") ==
          debugAt(window0, "topology_route_selected_cover_count"));
    CHECK(debugAt(window1, "topology_route_action") == debugAt(window0, "topology_route_action"));
}

TEST_CASE("Shadow and Narrow never walk", "[search][vector-stage][road-walk][catch2]") {
    const TopologyVectorEnvPin vectorEnv;
    for (const auto policy : {Policy::Shadow, Policy::Narrow}) {
        DYNAMIC_SECTION(SearchEngineConfig::topologyVectorPolicyToString(policy)) {
            const auto actual = runWalkCell(WalkCell{.policy = policy, .hops = 1, .window = 2});
            CHECK_FALSE(hasWalkDebugKeys(actual));
            CHECK(debugAt(actual, "topology_road_hops_applied") == "<missing>");
            CHECK(debugAt(actual, "topology_candidate_rescue_added_candidates") == "0");
        }
    }
}

// NOLINTEND(cppcoreguidelines-avoid-do-while, readability-function-cognitive-complexity)
