// Cluster roads: undirected cluster-to-cluster links accumulated from the raw neighbor lists at
// build time. Roads are an in-memory batch field here; persistence and routing consumers land in
// later PRs. These tests drive the public engines through buildArtifacts only.

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/search/topology_routing_session.h>
#include <yams/topology/topology_alternate_engines.h>
#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_baseline.h>
#include <yams/topology/topology_codec.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <string>
#include <vector>

using yams::topology::ClusterRoad;
using yams::topology::ConnectedComponentTopologyEngine;
using yams::topology::KMeansTopologyEngine;
using yams::topology::LouvainTopologyEngine;
using yams::topology::TopologyArtifactBatch;
using yams::topology::TopologyBuildConfig;
using yams::topology::TopologyDocumentInput;
using yams::topology::TopologyNeighbor;

namespace {

std::vector<float> unit2(float x, float y) {
    const float n = std::sqrt(x * x + y * y);
    return {x / n, y / n};
}

TopologyDocumentInput mk(std::string hash, std::vector<float> emb) {
    TopologyDocumentInput doc;
    doc.documentHash = std::move(hash);
    doc.filePath = "/tmp/" + doc.documentHash + ".txt";
    doc.embedding = std::move(emb);
    return doc;
}

void link(TopologyDocumentInput& doc, std::string target, float score, bool reciprocal) {
    doc.neighbors.push_back(TopologyNeighbor{
        .documentHash = std::move(target), .score = score, .reciprocal = reciprocal});
}

TopologyDocumentInput* find(std::vector<TopologyDocumentInput>& docs, const std::string& hash) {
    for (auto& doc : docs) {
        if (doc.documentHash == hash) {
            return &doc;
        }
    }
    return nullptr;
}

// Three graph clusters on the connected engine: A = {a0, a1}, B = {b0, b1}, C = {c0, c1}.
// Intra-cluster edges are reciprocal and above minEdgeScore. Cross-cluster edges are raw
// neighbor observations that the construction filter drops (non-reciprocal or below the score
// floor), so the components stay apart while the roads still see them:
//   A-B: a1->b0 0.30 (non-reciprocal)                      weight 0.30
//   A-C: a0->c0 0.20 (below floor) + a1->c1 0.25 (below)   weight 0.45
//   B-C: none
std::vector<TopologyDocumentInput> threeGraphClusters() {
    std::vector<TopologyDocumentInput> docs;
    docs.push_back(mk("a0", unit2(1.0F, 0.0F)));
    docs.push_back(mk("a1", unit2(0.99F, 0.05F)));
    docs.push_back(mk("b0", unit2(0.0F, 1.0F)));
    docs.push_back(mk("b1", unit2(0.05F, 0.99F)));
    docs.push_back(mk("c0", unit2(-1.0F, 0.0F)));
    docs.push_back(mk("c1", unit2(-0.99F, 0.05F)));
    link(*find(docs, "a0"), "a1", 0.9F, true);
    link(*find(docs, "a1"), "a0", 0.9F, true);
    link(*find(docs, "b0"), "b1", 0.9F, true);
    link(*find(docs, "b1"), "b0", 0.9F, true);
    link(*find(docs, "c0"), "c1", 0.9F, true);
    link(*find(docs, "c1"), "c0", 0.9F, true);
    link(*find(docs, "a1"), "b0", 0.30F, false);
    link(*find(docs, "a0"), "c0", 0.20F, true);
    link(*find(docs, "a1"), "c1", 0.25F, true);
    return docs;
}

TopologyBuildConfig graphConfig(std::size_t roadMaxPerCluster) {
    TopologyBuildConfig cfg;
    cfg.reciprocalOnly = true;
    cfg.minEdgeScore = 0.5;
    cfg.roadMaxPerCluster = roadMaxPerCluster;
    return cfg;
}

const std::string kA = "topology.cluster.a0";
const std::string kB = "topology.cluster.b0";
const std::string kC = "topology.cluster.c0";

std::vector<ClusterRoad> roadsFrom(const TopologyArtifactBatch& batch, const std::string& from) {
    std::vector<ClusterRoad> out;
    for (const auto& road : batch.roads) {
        if (road.fromClusterId == from) {
            out.push_back(road);
        }
    }
    return out;
}

std::string clusterOf(const TopologyArtifactBatch& batch, const std::string& hash) {
    for (const auto& membership : batch.memberships) {
        if (membership.documentHash == hash) {
            return membership.clusterId;
        }
    }
    return {};
}

} // namespace

TEST_CASE("Roads are off by default and leave the batch untouched",
          "[unit][topology][roads][catch2]") {
    CHECK(TopologyBuildConfig{}.roadMaxPerCluster == 0);

    ConnectedComponentTopologyEngine engine;
    auto off = engine.buildArtifacts(threeGraphClusters(), graphConfig(0));
    REQUIRE(off.has_value());
    CHECK(off.value().roads.empty());
    REQUIRE(off.value().clusters.size() == 3);

    auto on = engine.buildArtifacts(threeGraphClusters(), graphConfig(4));
    REQUIRE(on.has_value());
    CHECK_FALSE(on.value().roads.empty());

    // Publication stamps differ between two builds; everything else must be identical, and the
    // persisted encoding must not know about roads in this PR.
    auto normalized = on.value();
    normalized.snapshotId = off.value().snapshotId;
    normalized.generatedAtUnixSeconds = off.value().generatedAtUnixSeconds;
    auto offBytes = yams::topology::serializeTopologyBatchBinary(off.value());
    auto onBytes = yams::topology::serializeTopologyBatchBinary(normalized);
    REQUIRE(offBytes.has_value());
    REQUIRE(onBytes.has_value());
    CHECK(offBytes.value() == onBytes.value());
}

TEST_CASE("Connected engine accumulates roads from raw neighbors the construction filter dropped",
          "[unit][topology][roads][connected][catch2]") {
    ConnectedComponentTopologyEngine engine;
    auto result = engine.buildArtifacts(threeGraphClusters(), graphConfig(8));
    REQUIRE(result.has_value());
    const auto& batch = result.value();
    REQUIRE(batch.clusters.size() == 3);
    CHECK(clusterOf(batch, "a1") == kA);
    CHECK(clusterOf(batch, "b1") == kB);
    CHECK(clusterOf(batch, "c1") == kC);

    // Every cross-cluster observation was filtered out of the construction graph, so the
    // construction relation digest has no cross-cluster pairs, yet the roads do.
    for (const auto& cluster : batch.clusters) {
        CHECK(cluster.protectedPairCount == cluster.preservedProtectedPairCount);
    }

    const auto fromA = roadsFrom(batch, kA);
    REQUIRE(fromA.size() == 2);
    CHECK(fromA[0].toClusterId == kC);
    CHECK(fromA[0].weight == Catch::Approx(0.45).margin(1e-6));
    CHECK(fromA[1].toClusterId == kB);
    CHECK(fromA[1].weight == Catch::Approx(0.30).margin(1e-6));

    const auto fromB = roadsFrom(batch, kB);
    REQUIRE(fromB.size() == 1);
    CHECK(fromB[0].toClusterId == kA);
    CHECK(fromB[0].weight == Catch::Approx(0.30).margin(1e-6));

    const auto fromC = roadsFrom(batch, kC);
    REQUIRE(fromC.size() == 1);
    CHECK(fromC[0].toClusterId == kA);
    CHECK(fromC[0].weight == Catch::Approx(0.45).margin(1e-6));

    // Roads are listed by source cluster, heaviest first; no self roads.
    CHECK(std::ranges::is_sorted(batch.roads, [](const ClusterRoad& lhs, const ClusterRoad& rhs) {
        if (lhs.fromClusterId != rhs.fromClusterId) {
            return lhs.fromClusterId < rhs.fromClusterId;
        }
        if (lhs.weight != rhs.weight) {
            return lhs.weight > rhs.weight;
        }
        return lhs.toClusterId < rhs.toClusterId;
    }));
    for (const auto& road : batch.roads) {
        CHECK(road.fromClusterId != road.toClusterId);
    }
}

TEST_CASE("Roads keep the top-K per cluster with a deterministic tie-break",
          "[unit][topology][roads][catch2]") {
    ConnectedComponentTopologyEngine engine;

    SECTION("K=1 keeps the heaviest road per cluster") {
        auto result = engine.buildArtifacts(threeGraphClusters(), graphConfig(1));
        REQUIRE(result.has_value());
        const auto& batch = result.value();
        const auto fromA = roadsFrom(batch, kA);
        REQUIRE(fromA.size() == 1);
        CHECK(fromA[0].toClusterId == kC);
        // B's only road is to A; truncation on A's side does not remove B's view of it.
        const auto fromB = roadsFrom(batch, kB);
        REQUIRE(fromB.size() == 1);
        CHECK(fromB[0].toClusterId == kA);
        CHECK(batch.roads.size() == 3);
    }

    SECTION("equal weights break ties on the target cluster id") {
        auto docs = threeGraphClusters();
        // Exactly representable scores so the tie is exact: A-C = 0.25 + 0.25, A-B = 0.5.
        for (auto& neighbor : find(docs, "a0")->neighbors) {
            if (neighbor.documentHash == "c0") {
                neighbor.score = 0.25F;
            }
        }
        for (auto& neighbor : find(docs, "a1")->neighbors) {
            if (neighbor.documentHash == "b0") {
                neighbor.score = 0.5F;
            }
        }
        auto docsB = docs;
        auto result = engine.buildArtifacts(docs, graphConfig(1));
        REQUIRE(result.has_value());
        const auto fromA = roadsFrom(result.value(), kA);
        REQUIRE(fromA.size() == 1);
        CHECK(fromA[0].toClusterId == kB);
        CHECK(fromA[0].weight == 0.5);
        CHECK(roadsFrom(result.value(), kC).front().weight == 0.5);

        // Reordering the input documents does not change the kept road.
        std::ranges::reverse(docsB);
        auto reordered = engine.buildArtifacts(docsB, graphConfig(1));
        REQUIRE(reordered.has_value());
        CHECK(reordered.value().roads == result.value().roads);
    }
}

TEST_CASE("Road weights sum every raw observation between two clusters, in both directions",
          "[unit][topology][roads][catch2]") {
    auto docs = threeGraphClusters();
    // A reciprocal cross pair below the score floor is observed from both endpoints.
    link(*find(docs, "b1"), "a0", 0.10F, true);
    link(*find(docs, "a0"), "b1", 0.10F, true);
    ConnectedComponentTopologyEngine engine;
    auto result = engine.buildArtifacts(docs, graphConfig(8));
    REQUIRE(result.has_value());
    const auto fromA = roadsFrom(result.value(), kA);
    REQUIRE(fromA.size() == 2);
    // A-B = 0.30 + 0.10 + 0.10 = 0.50 now outranks A-C = 0.45.
    CHECK(fromA[0].toClusterId == kB);
    CHECK(fromA[0].weight == Catch::Approx(0.50).margin(1e-6));
    CHECK(fromA[1].toClusterId == kC);
    const auto fromB = roadsFrom(result.value(), kB);
    REQUIRE(fromB.size() == 1);
    CHECK(fromB[0].weight == Catch::Approx(0.50).margin(1e-6));
}

TEST_CASE("Roads ignore neighbors outside the batch and non-finite scores",
          "[unit][topology][roads][catch2]") {
    auto docs = threeGraphClusters();
    link(*find(docs, "a0"), "zz-not-in-batch", 0.9F, true);
    link(*find(docs, "b0"), "c0", std::numeric_limits<float>::quiet_NaN(), true);
    link(*find(docs, "b1"), "c1", -0.5F, true);
    ConnectedComponentTopologyEngine engine;
    auto result = engine.buildArtifacts(docs, graphConfig(8));
    REQUIRE(result.has_value());
    CHECK(roadsFrom(result.value(), kB).size() == 1);
    CHECK(roadsFrom(result.value(), kC).size() == 1);
    CHECK(result.value().roads.size() == 4);
}

TEST_CASE("KMeans engine accumulates roads between embedding clusters",
          "[unit][topology][roads][kmeans][catch2]") {
    // Two blobs; k-means ignores the neighbor graph for assignment, so a raw edge between the
    // blobs is purely road evidence.
    std::vector<TopologyDocumentInput> docs;
    docs.push_back(mk("a0", unit2(1.0F, 0.02F)));
    docs.push_back(mk("a1", unit2(0.98F, 0.05F)));
    docs.push_back(mk("b0", unit2(0.02F, 1.0F)));
    docs.push_back(mk("b1", unit2(0.05F, 0.98F)));
    link(*find(docs, "a1"), "b0", 0.40F, false);
    link(*find(docs, "b1"), "a0", 0.20F, false);

    KMeansTopologyEngine engine;
    TopologyBuildConfig cfg;
    cfg.kmeansK = 2;

    auto off = engine.buildArtifacts(docs, cfg);
    REQUIRE(off.has_value());
    REQUIRE(off.value().clusters.size() == 2);
    CHECK(off.value().roads.empty());

    cfg.roadMaxPerCluster = 2;
    auto on = engine.buildArtifacts(docs, cfg);
    REQUIRE(on.has_value());
    const auto& batch = on.value();
    const auto a = clusterOf(batch, "a0");
    const auto b = clusterOf(batch, "b0");
    REQUIRE(a != b);
    REQUIRE(batch.roads.size() == 2);
    const auto fromA = roadsFrom(batch, a);
    REQUIRE(fromA.size() == 1);
    CHECK(fromA[0].toClusterId == b);
    CHECK(fromA[0].weight == Catch::Approx(0.60).margin(1e-6));
    const auto fromB = roadsFrom(batch, b);
    REQUIRE(fromB.size() == 1);
    CHECK(fromB[0].toClusterId == a);
    CHECK(fromB[0].weight == Catch::Approx(0.60).margin(1e-6));
}

TEST_CASE("Louvain engine accumulates roads from filtered cross-community edges",
          "[unit][topology][roads][louvain][catch2]") {
    LouvainTopologyEngine engine;
    auto result = engine.buildArtifacts(threeGraphClusters(), graphConfig(8));
    REQUIRE(result.has_value());
    const auto& batch = result.value();
    const auto a = clusterOf(batch, "a0");
    const auto b = clusterOf(batch, "b0");
    const auto c = clusterOf(batch, "c0");
    REQUIRE(a != b);
    REQUIRE(a != c);
    REQUIRE(b != c);
    CHECK(clusterOf(batch, "a1") == a);
    const auto fromA = roadsFrom(batch, a);
    REQUIRE(fromA.size() == 2);
    CHECK(fromA[0].toClusterId == c);
    CHECK(fromA[0].weight == Catch::Approx(0.45).margin(1e-6));
    CHECK(fromA[1].toClusterId == b);
    CHECK(fromA[1].weight == Catch::Approx(0.30).margin(1e-6));
}

TEST_CASE("Routing construction fingerprint is road-free",
          "[unit][topology][roads][fingerprint][catch2]") {
    ConnectedComponentTopologyEngine engine;
    auto off = engine.buildArtifacts(threeGraphClusters(), graphConfig(0));
    auto on = engine.buildArtifacts(threeGraphClusters(), graphConfig(8));
    REQUIRE(off.has_value());
    REQUIRE(on.has_value());
    REQUIRE(off.value().roads.empty());
    REQUIRE_FALSE(on.value().roads.empty());

    const auto offIdentity = yams::search::topologyRoutingConstructionFingerprint(off.value());
    CHECK(yams::search::topologyRoutingConstructionFingerprint(on.value()) == offIdentity);

    // Editing roads in place must not move the fingerprint either.
    auto edited = on.value();
    edited.roads.front().weight += 10.0;
    edited.roads.push_back(ClusterRoad{.fromClusterId = kB, .toClusterId = kC, .weight = 1.0});
    CHECK(yams::search::topologyRoutingConstructionFingerprint(edited) == offIdentity);

    // The road fingerprint is the identity that does follow roads.
    const auto roadsOff = yams::search::topologyRoadFingerprint(off.value());
    const auto roadsOn = yams::search::topologyRoadFingerprint(on.value());
    CHECK(roadsOff != roadsOn);
    CHECK(yams::search::topologyRoadFingerprint(edited) != roadsOn);

    SECTION("the persisted path preserves both identities") {
        // Main payload (road-free v1 codec) plus the road sidecar, as the store writes them.
        auto mainPayload = yams::topology::serializeTopologyBatchCompressed(on.value());
        REQUIRE(mainPayload.has_value());
        auto reloaded = yams::topology::deserializeTopologyBatchCompressed(mainPayload.value());
        REQUIRE(reloaded.has_value());
        REQUIRE(reloaded.value().roads.empty());
        CHECK(yams::search::topologyRoutingConstructionFingerprint(reloaded.value()) ==
              offIdentity);
        CHECK(yams::search::topologyRoadFingerprint(reloaded.value()) == roadsOff);

        yams::topology::TopologyRoadSidecar sidecar;
        sidecar.snapshotId = on.value().snapshotId;
        sidecar.maxPerCluster = on.value().roadMaxPerCluster;
        sidecar.roads = on.value().roads;
        CHECK(sidecar.maxPerCluster == 8);
        auto sidecarPayload = yams::topology::serializeTopologyRoadSidecarCompressed(sidecar);
        REQUIRE(sidecarPayload.has_value());
        auto decodedSidecar =
            yams::topology::deserializeTopologyRoadSidecarCompressed(sidecarPayload.value());
        REQUIRE(decodedSidecar.has_value());
        CHECK_FALSE(
            yams::topology::validateTopologyRoadSidecar(reloaded.value(), decodedSidecar.value())
                .has_value());
        reloaded.value().roads = std::move(decodedSidecar.value().roads);
        reloaded.value().roadMaxPerCluster = decodedSidecar.value().maxPerCluster;
        CHECK(reloaded.value().roads == on.value().roads);
        CHECK(yams::search::topologyRoutingConstructionFingerprint(reloaded.value()) ==
              offIdentity);
        CHECK(yams::search::topologyRoadFingerprint(reloaded.value()) == roadsOn);
    }
}
