// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Incremental road maintenance. An incremental update sees only the dirty region's documents, so
// it keeps the roads between untouched clusters, drops every road with an endpoint in a removed
// or rebuilt cluster, recomputes the rebuilt clusters' roads from the region's raw neighbor lists
// against the merged memberships, and re-ranks each source's top-K over the union. A neighbor
// outside the region contributes its mirror observation, which is exact on a symmetric neighbor
// graph; the property tests below are therefore stated on symmetric graphs.

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/topology/topology_alternate_engines.h>
#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_baseline.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <map>
#include <random>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

using yams::topology::ClusterRoad;
using yams::topology::ConnectedComponentTopologyEngine;
using yams::topology::KMeansTopologyEngine;
using yams::topology::LouvainTopologyEngine;
using yams::topology::TopologyArtifactBatch;
using yams::topology::TopologyBuildConfig;
using yams::topology::TopologyDocumentInput;
using yams::topology::TopologyNeighbor;
using yams::topology::TopologyUpdateStats;

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

TopologyDocumentInput* find(std::vector<TopologyDocumentInput>& docs, const std::string& hash) {
    for (auto& doc : docs) {
        if (doc.documentHash == hash) {
            return &doc;
        }
    }
    return nullptr;
}

void link(TopologyDocumentInput& doc, std::string target, float score, bool reciprocal) {
    doc.neighbors.push_back(TopologyNeighbor{
        .documentHash = std::move(target), .score = score, .reciprocal = reciprocal});
}

// Symmetric neighbor graph: every edge is observed from both endpoints with the same score.
void linkSym(std::vector<TopologyDocumentInput>& docs, const std::string& a, const std::string& b,
             float score, bool reciprocal) {
    link(*find(docs, a), b, score, reciprocal);
    link(*find(docs, b), a, score, reciprocal);
}

void unlinkSym(std::vector<TopologyDocumentInput>& docs, const std::string& a,
               const std::string& b) {
    std::erase_if(find(docs, a)->neighbors,
                  [&](const TopologyNeighbor& n) { return n.documentHash == b; });
    std::erase_if(find(docs, b)->neighbors,
                  [&](const TopologyNeighbor& n) { return n.documentHash == a; });
}

std::vector<TopologyDocumentInput> select(const std::vector<TopologyDocumentInput>& docs,
                                          const std::unordered_set<std::string>& hashes) {
    std::vector<TopologyDocumentInput> out;
    for (const auto& doc : docs) {
        if (hashes.contains(doc.documentHash)) {
            out.push_back(doc);
        }
    }
    return out;
}

TopologyBuildConfig graphConfig(std::size_t roadMaxPerCluster) {
    TopologyBuildConfig cfg;
    cfg.reciprocalOnly = true;
    cfg.minEdgeScore = 0.5;
    cfg.roadMaxPerCluster = roadMaxPerCluster;
    return cfg;
}

// Cluster ids are anchored to a member hash and an incremental update may reuse a prior id, so
// roads are compared through the clusters' member sets: a cluster is named by its smallest member.
std::unordered_map<std::string, std::string> anchorByClusterId(const TopologyArtifactBatch& batch) {
    std::unordered_map<std::string, std::string> anchors;
    for (const auto& cluster : batch.clusters) {
        REQUIRE_FALSE(cluster.memberDocumentHashes.empty());
        anchors[cluster.clusterId] = *std::ranges::min_element(cluster.memberDocumentHashes);
    }
    return anchors;
}

using CanonicalRoad = std::tuple<std::string, std::string, double>;

std::vector<CanonicalRoad> canonicalRoads(const TopologyArtifactBatch& batch) {
    const auto anchors = anchorByClusterId(batch);
    std::vector<CanonicalRoad> out;
    for (const auto& road : batch.roads) {
        REQUIRE(anchors.contains(road.fromClusterId));
        REQUIRE(anchors.contains(road.toClusterId));
        out.emplace_back(anchors.at(road.fromClusterId), anchors.at(road.toClusterId), road.weight);
    }
    std::ranges::sort(out);
    return out;
}

std::set<std::vector<std::string>> canonicalClusters(const TopologyArtifactBatch& batch) {
    std::set<std::vector<std::string>> out;
    for (const auto& cluster : batch.clusters) {
        auto members = cluster.memberDocumentHashes;
        std::ranges::sort(members);
        out.insert(std::move(members));
    }
    return out;
}

bool roadsRanked(const TopologyArtifactBatch& batch) {
    return std::ranges::is_sorted(batch.roads, [](const ClusterRoad& lhs, const ClusterRoad& rhs) {
        if (lhs.fromClusterId != rhs.fromClusterId) {
            return lhs.fromClusterId < rhs.fromClusterId;
        }
        if (lhs.weight != rhs.weight) {
            return lhs.weight > rhs.weight;
        }
        return lhs.toClusterId < rhs.toClusterId;
    });
}

std::vector<ClusterRoad> roadsFrom(const TopologyArtifactBatch& batch, const std::string& from) {
    std::vector<ClusterRoad> out;
    for (const auto& road : batch.roads) {
        if (road.fromClusterId == from) {
            out.push_back(road);
        }
    }
    return out;
}

std::size_t maxRoadsPerSource(const TopologyArtifactBatch& batch) {
    std::unordered_map<std::string, std::size_t> counts;
    std::size_t best = 0;
    for (const auto& road : batch.roads) {
        best = std::max(best, ++counts[road.fromClusterId]);
    }
    return best;
}

// Four graph clusters A = {a0, a1}, B = {b0, b1}, C = {c0, c1}, D = {d0, d1}. Intra-cluster
// edges are reciprocal and above the 0.5 floor; cross-cluster edges are symmetric, below the
// floor, and dyadic so every road weight is an exact double:
//   a1-b0 0.25   a0-c0 0.125   b1-c1 0.5   c0-d1 0.25   b0-d0 0.125
// Full roads (each edge is observed from both ends): A-B 0.5, A-C 0.25, B-C 1.0, C-D 0.5,
// B-D 0.25.
std::vector<TopologyDocumentInput> fourClusters() {
    std::vector<TopologyDocumentInput> docs;
    docs.push_back(mk("a0", unit2(1.0F, 0.0F)));
    docs.push_back(mk("a1", unit2(0.99F, 0.05F)));
    docs.push_back(mk("b0", unit2(0.0F, 1.0F)));
    docs.push_back(mk("b1", unit2(0.05F, 0.99F)));
    docs.push_back(mk("c0", unit2(-1.0F, 0.0F)));
    docs.push_back(mk("c1", unit2(-0.99F, 0.05F)));
    docs.push_back(mk("d0", unit2(0.0F, -1.0F)));
    docs.push_back(mk("d1", unit2(0.05F, -0.99F)));
    linkSym(docs, "a0", "a1", 0.9F, true);
    linkSym(docs, "b0", "b1", 0.9F, true);
    linkSym(docs, "c0", "c1", 0.9F, true);
    linkSym(docs, "d0", "d1", 0.9F, true);
    linkSym(docs, "a1", "b0", 0.25F, true);
    linkSym(docs, "a0", "c0", 0.125F, true);
    linkSym(docs, "b1", "c1", 0.5F, false);
    linkSym(docs, "c0", "d1", 0.25F, true);
    linkSym(docs, "b0", "d0", 0.125F, true);
    return docs;
}

const std::string kA = "topology.cluster.a0";
const std::string kB = "topology.cluster.b0";
const std::string kC = "topology.cluster.c0";
const std::string kD = "topology.cluster.d0";
const std::unordered_set<std::string> kRegionAB{"a0", "a1", "b0", "b1"};
const std::unordered_set<std::string> kRegionCD{"c0", "c1", "d0", "d1"};

// Planted clusters for the property tests: dense reciprocal intra edges above the floor, sparse
// symmetric cross edges below it with dyadic scores. `label` tracks each document's planted
// cluster through mutations.
struct Planted {
    std::vector<TopologyDocumentInput> docs;
    std::vector<std::vector<std::string>> clusters;
    std::unordered_map<std::string, std::size_t> label;

    std::unordered_set<std::string> region(const std::vector<std::size_t>& clusterIndices) const {
        std::unordered_set<std::string> hashes;
        for (const std::size_t index : clusterIndices) {
            hashes.insert(clusters[index].begin(), clusters[index].end());
        }
        return hashes;
    }
};

float dyadicCrossScore(std::mt19937& rng) {
    // k/64 for k in [1, 31]: exactly representable and below the 0.5 construction floor.
    return static_cast<float>(std::uniform_int_distribution<int>(1, 31)(rng)) / 64.0F;
}

std::vector<float> plantedEmbedding(std::size_t cluster, std::size_t clusterCount, float jitter) {
    const auto angle = static_cast<float>(cluster) / static_cast<float>(clusterCount) * 6.2831853F;
    return unit2(std::cos(angle) + jitter, std::sin(angle) + jitter * 0.5F);
}

Planted plant(std::mt19937& rng) {
    Planted planted;
    const std::size_t clusterCount = std::uniform_int_distribution<std::size_t>(3, 6)(rng);
    planted.clusters.resize(clusterCount);
    for (std::size_t c = 0; c < clusterCount; ++c) {
        const std::size_t size = std::uniform_int_distribution<std::size_t>(3, 4)(rng);
        for (std::size_t m = 0; m < size; ++m) {
            const std::string hash = "c" + std::to_string(c) + "m" + std::to_string(m);
            planted.docs.push_back(
                mk(hash, plantedEmbedding(c, clusterCount, static_cast<float>(m) * 0.02F)));
            planted.clusters[c].push_back(hash);
            planted.label[hash] = c;
        }
        for (std::size_t i = 0; i < size; ++i) {
            for (std::size_t j = i + 1; j < size; ++j) {
                linkSym(planted.docs, planted.clusters[c][i], planted.clusters[c][j], 0.9F, true);
            }
        }
    }
    std::bernoulli_distribution crossEdge(0.3);
    for (std::size_t i = 0; i < planted.docs.size(); ++i) {
        for (std::size_t j = i + 1; j < planted.docs.size(); ++j) {
            const auto& a = planted.docs[i].documentHash;
            const auto& b = planted.docs[j].documentHash;
            if (planted.label[a] != planted.label[b] && crossEdge(rng)) {
                linkSym(planted.docs, a, b, dyadicCrossScore(rng), true);
            }
        }
    }
    return planted;
}

bool hasEdge(const TopologyDocumentInput& doc, const std::string& target) {
    return std::ranges::any_of(doc.neighbors,
                               [&](const TopologyNeighbor& n) { return n.documentHash == target; });
}

// Mutate only documents inside the planted clusters `scope`, so every changed neighbor list
// belongs to the dirty region. Returns the region's document hashes.
std::unordered_set<std::string> mutateWithin(Planted& planted, std::mt19937& rng,
                                             const std::vector<std::size_t>& scope) {
    std::bernoulli_distribution coin(0.5);
    // (a) rewire cross edges whose endpoints are both in scope.
    std::vector<std::string> scoped;
    for (const std::size_t c : scope) {
        scoped.insert(scoped.end(), planted.clusters[c].begin(), planted.clusters[c].end());
    }
    for (std::size_t i = 0; i < scoped.size(); ++i) {
        for (std::size_t j = i + 1; j < scoped.size(); ++j) {
            if (planted.label[scoped[i]] == planted.label[scoped[j]] || !coin(rng)) {
                continue;
            }
            if (hasEdge(*find(planted.docs, scoped[i]), scoped[j])) {
                unlinkSym(planted.docs, scoped[i], scoped[j]);
            }
            if (coin(rng)) {
                linkSym(planted.docs, scoped[i], scoped[j], dyadicCrossScore(rng), true);
            }
        }
    }
    // (b) move one document between two scoped clusters; both clusters' intra lists change.
    if (scope.size() >= 2 && coin(rng)) {
        const std::size_t from = scope[0];
        const std::size_t to = scope[1];
        if (planted.clusters[from].size() >= 3) {
            const std::string mover = planted.clusters[from].back();
            planted.clusters[from].pop_back();
            for (const auto& member : planted.clusters[from]) {
                unlinkSym(planted.docs, mover, member);
            }
            for (const auto& member : planted.clusters[to]) {
                if (hasEdge(*find(planted.docs, mover), member)) {
                    unlinkSym(planted.docs, mover, member);
                }
                linkSym(planted.docs, mover, member, 0.9F, true);
            }
            planted.clusters[to].push_back(mover);
            planted.label[mover] = to;
            find(planted.docs, mover)->embedding =
                plantedEmbedding(to, planted.clusters.size(), 0.03F);
        }
    }
    return planted.region(scope);
}

std::vector<std::size_t> pickScope(std::mt19937& rng, std::size_t clusterCount) {
    std::vector<std::size_t> all(clusterCount);
    for (std::size_t i = 0; i < clusterCount; ++i) {
        all[i] = i;
    }
    std::shuffle(all.begin(), all.end(), rng);
    const std::size_t count = std::uniform_int_distribution<std::size_t>(1, 2)(rng);
    all.resize(count);
    return all;
}

} // namespace

TEST_CASE("Roads stay off through an incremental update",
          "[unit][topology][roads][incremental][catch2]") {
    ConnectedComponentTopologyEngine engine;
    auto docs = fourClusters();

    SECTION("existing without roads, config off") {
        auto existing = engine.buildArtifacts(docs, graphConfig(0));
        REQUIRE(existing.has_value());
        auto merged =
            engine.updateArtifacts(existing.value(), select(docs, kRegionAB), graphConfig(0));
        REQUIRE(merged.has_value());
        CHECK(merged.value().roads.empty());
        CHECK(merged.value().roadMaxPerCluster == 0);
        CHECK(merged.value().roadsFullBuildEpoch == 0);
        CHECK(merged.value().roadsIncrementalPasses == 0);
        CHECK(merged.value().roadUnavailableReason.empty());
    }

    SECTION("existing with roads, config switched off: the update sheds them") {
        auto existing = engine.buildArtifacts(docs, graphConfig(8));
        REQUIRE(existing.has_value());
        REQUIRE_FALSE(existing.value().roads.empty());
        auto merged =
            engine.updateArtifacts(existing.value(), select(docs, kRegionAB), graphConfig(0));
        REQUIRE(merged.has_value());
        CHECK(merged.value().roads.empty());
        CHECK(merged.value().roadMaxPerCluster == 0);
        CHECK(merged.value().roadsFullBuildEpoch == 0);
        CHECK(merged.value().roadsIncrementalPasses == 0);
    }
}

TEST_CASE("Full builds produce full roads: no incremental passes, no base epoch",
          "[unit][topology][roads][incremental][catch2]") {
    ConnectedComponentTopologyEngine engine;
    auto full = engine.buildArtifacts(fourClusters(), graphConfig(8));
    REQUIRE(full.has_value());
    REQUIRE_FALSE(full.value().roads.empty());
    CHECK(full.value().roadsFullBuildEpoch == 0);
    CHECK(full.value().roadsIncrementalPasses == 0);
    CHECK(full.value().roadMaxPerCluster == 8);
}

TEST_CASE("Incremental update keeps untouched roads, retires removed ones and recomputes the "
          "rebuilt clusters from raw neighbors",
          "[unit][topology][roads][incremental][connected][catch2]") {
    ConnectedComponentTopologyEngine engine;
    auto docs = fourClusters();
    auto existing = engine.buildArtifacts(docs, graphConfig(8));
    REQUIRE(existing.has_value());
    REQUIRE(existing.value().clusters.size() == 4);
    // The publisher stamps the epoch after a full build; mimic it so provenance is observable.
    existing.value().topologyEpoch = 5;
    {
        const auto fromA = roadsFrom(existing.value(), kA);
        REQUIRE(fromA.size() == 2);
        CHECK(fromA[0].toClusterId == kB);
        CHECK(fromA[0].weight == 0.5);
    }

    // Region A u B: the a1-b0 edge goes away and a weaker a0-b1 edge appears. Both endpoints of
    // every changed edge are in the region; C and D are untouched.
    unlinkSym(docs, "a1", "b0");
    linkSym(docs, "a0", "b1", 0.0625F, true);
    TopologyUpdateStats stats;
    auto merged =
        engine.updateArtifacts(existing.value(), select(docs, kRegionAB), graphConfig(8), &stats);
    REQUIRE(merged.has_value());
    const auto& batch = merged.value();
    REQUIRE(batch.clusters.size() == 4);
    CHECK(roadsRanked(batch));
    CHECK(batch.roadMaxPerCluster == 8);
    CHECK(batch.roadUnavailableReason.empty());

    // A-B is recomputed from the region (both sides observed): 0.0625 + 0.0625.
    const auto fromA = roadsFrom(batch, kA);
    REQUIRE(fromA.size() == 2);
    CHECK(fromA[0].toClusterId == kC);
    CHECK(fromA[0].weight == 0.25); // a0-c0 seen from a0 only, mirrored for c0's side
    CHECK(fromA[1].toClusterId == kB);
    CHECK(fromA[1].weight == 0.125);
    // The retired A-B 0.5 road is gone from both ends.
    for (const auto& road : batch.roads) {
        CHECK_FALSE((road.fromClusterId == kA && road.weight == 0.5));
        CHECK_FALSE((road.toClusterId == kA && road.weight == 0.5));
    }
    // Cross-region roads are rebuilt in both orientations with the full weight.
    const auto fromC = roadsFrom(batch, kC);
    REQUIRE(fromC.size() == 3);
    CHECK(fromC[0].toClusterId == kB);
    CHECK(fromC[0].weight == 1.0);
    CHECK(fromC[1].toClusterId == kD);
    CHECK(fromC[1].weight == 0.5); // untouched C-D road kept from `existing`
    CHECK(fromC[2].toClusterId == kA);
    CHECK(fromC[2].weight == 0.25);
    const auto fromD = roadsFrom(batch, kD);
    REQUIRE(fromD.size() == 2);
    CHECK(fromD[0].toClusterId == kC);
    CHECK(fromD[0].weight == 0.5);
    CHECK(fromD[1].toClusterId == kB);
    CHECK(fromD[1].weight == 0.25);

    // Equal to a from-scratch build on the same final input.
    auto full = engine.buildArtifacts(docs, graphConfig(8));
    REQUIRE(full.has_value());
    CHECK(canonicalRoads(batch) == canonicalRoads(full.value()));

    // Provenance: one derived pass on top of epoch 5's full accumulation.
    CHECK(batch.topologyEpoch == 6);
    CHECK(batch.roadsIncrementalPasses == 1);
    CHECK(batch.roadsFullBuildEpoch == 5);

    SECTION("a second pass counts up and keeps the base epoch") {
        unlinkSym(docs, "c0", "d1");
        linkSym(docs, "c1", "d0", 0.375F, true);
        auto second = engine.updateArtifacts(batch, select(docs, kRegionCD), graphConfig(8));
        REQUIRE(second.has_value());
        CHECK(second.value().topologyEpoch == 7);
        CHECK(second.value().roadsIncrementalPasses == 2);
        CHECK(second.value().roadsFullBuildEpoch == 5);
        auto fullAgain = engine.buildArtifacts(docs, graphConfig(8));
        REQUIRE(fullAgain.has_value());
        CHECK(canonicalRoads(second.value()) == canonicalRoads(fullAgain.value()));
        CHECK(roadsRanked(second.value()));

        SECTION("a full build resets the provenance") {
            CHECK(fullAgain.value().roadsIncrementalPasses == 0);
            CHECK(fullAgain.value().roadsFullBuildEpoch == 0);
        }
    }

    SECTION("an empty region returns the existing batch untouched") {
        auto same = engine.updateArtifacts(batch, {}, graphConfig(8));
        REQUIRE(same.has_value());
        CHECK(same.value().roads == batch.roads);
        CHECK(same.value().roadsIncrementalPasses == batch.roadsIncrementalPasses);
    }
}

TEST_CASE("Incremental update re-ranks every source's top-K over kept and recomputed roads",
          "[unit][topology][roads][incremental][connected][catch2]") {
    ConnectedComponentTopologyEngine engine;
    auto docs = fourClusters();
    // K = 1: C keeps only its heaviest road. In the full build that is C-B (1.0).
    auto existing = engine.buildArtifacts(docs, graphConfig(1));
    REQUIRE(existing.has_value());
    REQUIRE(roadsFrom(existing.value(), kC).size() == 1);
    CHECK(roadsFrom(existing.value(), kC)[0].toClusterId == kB);

    // Weaken B-C inside region B u C so C-D (0.5, untouched, kept) becomes C's heaviest.
    unlinkSym(docs, "b1", "c1");
    linkSym(docs, "b1", "c1", 0.0625F, true);
    auto merged = engine.updateArtifacts(existing.value(), select(docs, {"b0", "b1", "c0", "c1"}),
                                         graphConfig(1));
    REQUIRE(merged.has_value());
    CHECK(maxRoadsPerSource(merged.value()) == 1);
    CHECK(roadsRanked(merged.value()));
    const auto fromC = roadsFrom(merged.value(), kC);
    REQUIRE(fromC.size() == 1);
    CHECK(fromC[0].toClusterId == kD);
    CHECK(fromC[0].weight == 0.5);
    // Untouched D only ever had C-D 0.5 and B-D 0.25; it keeps C.
    const auto fromD = roadsFrom(merged.value(), kD);
    REQUIRE(fromD.size() == 1);
    CHECK(fromD[0].toClusterId == kC);
    auto full = engine.buildArtifacts(docs, graphConfig(1));
    REQUIRE(full.has_value());
    CHECK(canonicalRoads(merged.value()) == canonicalRoads(full.value()));
}

TEST_CASE("Incremental update without prior roads derives roads for the rebuilt clusters only",
          "[unit][topology][roads][incremental][connected][catch2]") {
    ConnectedComponentTopologyEngine engine;
    auto docs = fourClusters();
    auto existing = engine.buildArtifacts(docs, graphConfig(0));
    REQUIRE(existing.has_value());
    REQUIRE(existing.value().roads.empty());
    existing.value().topologyEpoch = 3;

    auto merged = engine.updateArtifacts(existing.value(), select(docs, kRegionAB), graphConfig(8));
    REQUIRE(merged.has_value());
    const auto& batch = merged.value();
    CHECK(roadsRanked(batch));
    CHECK(batch.roadMaxPerCluster == 8);
    // Every road touches a rebuilt cluster; the untouched C-D pair is unknown to this pass.
    REQUIRE_FALSE(batch.roads.empty());
    for (const auto& road : batch.roads) {
        const bool touchesRebuilt = road.fromClusterId == kA || road.fromClusterId == kB ||
                                    road.toClusterId == kA || road.toClusterId == kB;
        CHECK(touchesRebuilt);
    }
    CHECK(roadsFrom(batch, kC).size() == 2); // C-A, C-B but not C-D
    CHECK(roadsFrom(batch, kD).size() == 1); // D-B only
    CHECK(batch.roadsIncrementalPasses == 1);
    CHECK(batch.roadsFullBuildEpoch == 3);
}

TEST_CASE("Property: incremental roads equal full roads on a symmetric graph (connected engine)",
          "[unit][topology][roads][incremental][connected][property][catch2]") {
    ConnectedComponentTopologyEngine engine;
    // No source has more than 5 roads, so K = 64 never truncates; equality must be exact.
    const auto cfg = graphConfig(64);
    for (std::uint32_t seed = 1; seed <= 24; ++seed) {
        std::mt19937 rng(seed);
        auto planted = plant(rng);
        auto existing = engine.buildArtifacts(planted.docs, cfg);
        REQUIRE(existing.has_value());
        existing.value().topologyEpoch = 10;
        const auto baseEpoch = existing.value().topologyEpoch;

        for (std::uint32_t pass = 1; pass <= 4; ++pass) {
            const auto scope = pickScope(rng, planted.clusters.size());
            const auto region = mutateWithin(planted, rng, scope);
            INFO("seed " << seed << " pass " << pass << " region size " << region.size());

            auto merged =
                engine.updateArtifacts(existing.value(), select(planted.docs, region), cfg);
            REQUIRE(merged.has_value());
            auto full = engine.buildArtifacts(planted.docs, cfg);
            REQUIRE(full.has_value());

            REQUIRE(canonicalClusters(merged.value()) == canonicalClusters(full.value()));
            CHECK(canonicalRoads(merged.value()) == canonicalRoads(full.value()));
            CHECK(roadsRanked(merged.value()));
            CHECK(merged.value().roadsIncrementalPasses == pass);
            CHECK(merged.value().roadsFullBuildEpoch == baseEpoch);
            CHECK(merged.value().topologyEpoch == baseEpoch + pass);
            existing = std::move(merged);
        }
    }
}

TEST_CASE("Property: with a tight top-K, rebuilt clusters match the full ranking and every "
          "derived road carries the full weight (connected engine)",
          "[unit][topology][roads][incremental][connected][property][catch2]") {
    ConnectedComponentTopologyEngine engine;
    const auto cfg = graphConfig(2);
    const auto untruncated = graphConfig(64);
    for (std::uint32_t seed = 101; seed <= 116; ++seed) {
        std::mt19937 rng(seed);
        auto planted = plant(rng);
        auto existing = engine.buildArtifacts(planted.docs, cfg);
        REQUIRE(existing.has_value());

        for (std::uint32_t pass = 1; pass <= 3; ++pass) {
            const auto scope = pickScope(rng, planted.clusters.size());
            const auto region = mutateWithin(planted, rng, scope);
            INFO("seed " << seed << " pass " << pass);

            auto merged =
                engine.updateArtifacts(existing.value(), select(planted.docs, region), cfg);
            REQUIRE(merged.has_value());
            auto full = engine.buildArtifacts(planted.docs, cfg);
            REQUIRE(full.has_value());
            auto allRoads = engine.buildArtifacts(planted.docs, untruncated);
            REQUIRE(allRoads.has_value());
            REQUIRE(canonicalClusters(merged.value()) == canonicalClusters(full.value()));

            CHECK(roadsRanked(merged.value()));
            CHECK(maxRoadsPerSource(merged.value()) <= 2);

            // Rebuilt sources (any member in the region) are recomputed in full, so their top-K
            // is the full build's top-K.
            std::unordered_set<std::string> rebuiltAnchors;
            for (const auto& cluster : merged.value().clusters) {
                if (std::ranges::any_of(cluster.memberDocumentHashes,
                                        [&](const auto& h) { return region.contains(h); })) {
                    rebuiltAnchors.insert(*std::ranges::min_element(cluster.memberDocumentHashes));
                }
            }
            auto rebuiltOnly = [&](std::vector<CanonicalRoad> roads) {
                std::erase_if(roads, [&](const CanonicalRoad& road) {
                    return !rebuiltAnchors.contains(std::get<0>(road));
                });
                return roads;
            };
            CHECK(rebuiltOnly(canonicalRoads(merged.value())) ==
                  rebuiltOnly(canonicalRoads(full.value())));

            // Untouched sources re-rank what they kept plus the recomputed roads; each road they
            // keep is a real road with the from-scratch weight.
            std::map<std::pair<std::string, std::string>, double> truth;
            for (const auto& [from, to, weight] : canonicalRoads(allRoads.value())) {
                truth[{from, to}] = weight;
            }
            for (const auto& [from, to, weight] : canonicalRoads(merged.value())) {
                const auto it = truth.find({from, to});
                REQUIRE(it != truth.end());
                CHECK(it->second == weight);
            }
            existing = std::move(merged);
        }
    }
}

TEST_CASE("Louvain and k-means updates rebuild the supplied input, so their roads are full roads "
          "for that batch",
          "[unit][topology][roads][incremental][louvain][kmeans][catch2]") {
    // These engines' updateArtifacts is buildArtifacts over the changed documents: the result has
    // no untouched clusters to keep roads for, and its roads are accumulated from scratch over
    // every document it contains. The property that holds is equality with a from-scratch build
    // on the same input, with zero incremental passes.
    auto docs = fourClusters();
    auto mutated = docs;
    unlinkSym(mutated, "a1", "b0");
    linkSym(mutated, "a0", "b1", 0.0625F, true);

    auto check = [&](auto& engine, TopologyBuildConfig cfg) {
        auto existing = engine.buildArtifacts(docs, cfg);
        REQUIRE(existing.has_value());
        REQUIRE_FALSE(existing.value().roads.empty());
        existing.value().topologyEpoch = 5;

        SECTION("whole corpus as the changed set") {
            TopologyUpdateStats stats;
            auto merged = engine.updateArtifacts(existing.value(), mutated, cfg, &stats);
            REQUIRE(merged.has_value());
            auto full = engine.buildArtifacts(mutated, cfg);
            REQUIRE(full.has_value());
            CHECK(canonicalRoads(merged.value()) == canonicalRoads(full.value()));
            CHECK(roadsRanked(merged.value()));
            CHECK(merged.value().roadsIncrementalPasses == 0);
            CHECK(merged.value().roadsFullBuildEpoch == 0);
            CHECK(stats.fallbackFullRebuilds == 1);
        }
        SECTION("a region as the changed set rebuilds only that region") {
            auto region = select(mutated, kRegionAB);
            auto merged = engine.updateArtifacts(existing.value(), region, cfg);
            REQUIRE(merged.has_value());
            auto full = engine.buildArtifacts(region, cfg);
            REQUIRE(full.has_value());
            CHECK(canonicalClusters(merged.value()) == canonicalClusters(full.value()));
            CHECK(canonicalRoads(merged.value()) == canonicalRoads(full.value()));
            CHECK(merged.value().roadsIncrementalPasses == 0);
        }
    };

    SECTION("louvain") {
        LouvainTopologyEngine engine;
        check(engine, graphConfig(8));
    }
    SECTION("kmeans") {
        KMeansTopologyEngine engine;
        auto cfg = graphConfig(8);
        cfg.kmeansK = 4;
        check(engine, cfg);
    }
}
