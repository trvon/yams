// SPDX-License-Identifier: GPL-3.0-or-later
//
// Checks the hypotheses the Lean topology contracts (formal/topology) take about the C++ engines
// and router. A Lean theorem says something about YAMS only when the C++ code satisfies the
// definitions it is stated over, so each case below names the Lean definition it stands in for.
//
// Property cases run the engines over small seeded random inputs. Cases tagged [!shouldfail]
// pin a place where the C++ behaviour does not satisfy the Lean definition as written; each one
// says why. When the code or the model changes so that the definition holds, Catch2 reports the
// case as failed and the tag has to be removed.

#include <catch2/catch_test_macros.hpp>

#include <yams/topology/topology_alternate_engines.h>
#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_baseline.h>
#include <yams/topology/topology_representatives.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <queue>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

using yams::topology::applyOrthogonalBoundarySpill;
using yams::topology::ClusterArtifact;
using yams::topology::ClusterRoute;
using yams::topology::ConnectedComponentTopologyEngine;
using yams::topology::DocumentClusterMembership;
using yams::topology::ITopologyEngine;
using yams::topology::KMeansTopologyEngine;
using yams::topology::LouvainTopologyEngine;
using yams::topology::SparseGuidedClusterRouter;
using yams::topology::TopologyArtifactBatch;
using yams::topology::TopologyBuildConfig;
using yams::topology::TopologyDocumentInput;
using yams::topology::TopologyNeighbor;
using yams::topology::TopologyRouteRequest;

namespace {

constexpr std::uint32_t kSeed = 0x1ea4u;
constexpr int kIterations = 150;

std::string docHash(std::size_t i) {
    return "d" + std::to_string(100 + i);
}

/// Unique, non-empty hashes; neighbor lists also point at the document itself, at an unknown
/// hash and at the empty hash, which the Lean edge filter must drop.
std::vector<TopologyDocumentInput> randomDocuments(std::mt19937& rng, std::size_t n) {
    std::uniform_real_distribution<float> unit(0.0F, 1.0F);
    std::uniform_real_distribution<float> coord(-1.0F, 1.0F);
    std::uniform_int_distribution<std::size_t> target(0, n + 2);
    std::uniform_int_distribution<std::size_t> fanout(0, 4);

    std::vector<TopologyDocumentInput> docs(n);
    for (std::size_t i = 0; i < n; ++i) {
        docs[i].documentHash = docHash(i);
        docs[i].filePath = "/corpus/" + docs[i].documentHash;
        docs[i].embedding = {coord(rng), coord(rng), coord(rng)};
        const std::size_t edges = fanout(rng);
        for (std::size_t e = 0; e < edges; ++e) {
            const std::size_t t = target(rng);
            std::string hash;
            if (t < n) {
                hash = docHash(t);
            } else if (t == n) {
                hash = "missing";
            } else if (t == n + 1) {
                hash = docs[i].documentHash;
            }
            docs[i].neighbors.push_back(TopologyNeighbor{
                .documentHash = hash, .score = unit(rng), .reciprocal = unit(rng) < 0.7F});
        }
    }
    return docs;
}

/// Lean Contracts.NeighborEdgeContract: the neighbor is a corpus document, not the source, is
/// reciprocal when reciprocal-only mode is on, and meets the minimum edge score. In
/// reciprocal-only mode an edge also needs ConstructionGates.ReciprocalNeighborWitness: the
/// reverse observation passes the same contract.
std::vector<std::pair<std::string, std::string>>
leanRetainedEdges(const std::vector<TopologyDocumentInput>& docs,
                  const TopologyBuildConfig& config) {
    std::unordered_set<std::string> corpus;
    for (const auto& doc : docs) {
        corpus.insert(doc.documentHash);
    }
    std::vector<std::pair<std::string, std::string>> observations;
    for (const auto& doc : docs) {
        for (const auto& neighbor : doc.neighbors) {
            if (!corpus.contains(neighbor.documentHash) ||
                neighbor.documentHash == doc.documentHash) {
                continue;
            }
            if (config.reciprocalOnly && !neighbor.reciprocal) {
                continue;
            }
            if (neighbor.score < static_cast<float>(config.minEdgeScore)) {
                continue;
            }
            observations.emplace_back(doc.documentHash, neighbor.documentHash);
        }
    }
    if (!config.reciprocalOnly) {
        return observations;
    }
    const std::set<std::pair<std::string, std::string>> admitted(observations.begin(),
                                                                 observations.end());
    std::vector<std::pair<std::string, std::string>> edges;
    for (const auto& [src, dst] : observations) {
        if (admitted.contains({dst, src})) {
            edges.emplace_back(src, dst);
        }
    }
    return edges;
}

/// Lean Artifacts.WellFormedBatch as written: member counts match, every membership names a
/// produced cluster, and every cluster member has a membership row for that exact cluster.
/// (tests/unit/topology/topology_baseline_catch2_test.cpp checks a relaxed form that also
/// accepts a row whose overlapClusterIds names the cluster.)
bool leanWellFormedBatch(const TopologyArtifactBatch& batch) {
    std::unordered_set<std::string> clusterIds;
    for (const auto& cluster : batch.clusters) {
        if (cluster.memberCount != cluster.memberDocumentHashes.size()) {
            return false;
        }
        clusterIds.insert(cluster.clusterId);
    }
    std::set<std::pair<std::string, std::string>> rows;
    for (const auto& membership : batch.memberships) {
        if (!clusterIds.contains(membership.clusterId)) {
            return false;
        }
        rows.emplace(membership.documentHash, membership.clusterId);
    }
    for (const auto& cluster : batch.clusters) {
        for (const auto& hash : cluster.memberDocumentHashes) {
            if (!rows.contains({hash, cluster.clusterId})) {
                return false;
            }
        }
    }
    return true;
}

std::unordered_map<std::string, std::string> primaryClusterOf(const TopologyArtifactBatch& batch) {
    std::unordered_map<std::string, std::string> clusterOf;
    for (const auto& membership : batch.memberships) {
        clusterOf.emplace(membership.documentHash, membership.clusterId);
    }
    return clusterOf;
}

/// Each corpus document is a member of exactly one cluster and has exactly one membership row
/// (no overlap configured).
void checkPartition(const TopologyArtifactBatch& batch,
                    const std::vector<TopologyDocumentInput>& docs) {
    std::unordered_map<std::string, std::size_t> memberOccurrences;
    for (const auto& cluster : batch.clusters) {
        for (const auto& hash : cluster.memberDocumentHashes) {
            ++memberOccurrences[hash];
        }
    }
    std::unordered_map<std::string, std::size_t> rowOccurrences;
    for (const auto& membership : batch.memberships) {
        ++rowOccurrences[membership.documentHash];
    }
    CHECK(memberOccurrences.size() == docs.size());
    CHECK(rowOccurrences.size() == docs.size());
    for (const auto& doc : docs) {
        CAPTURE(doc.documentHash);
        CHECK(memberOccurrences[doc.documentHash] == 1U);
        CHECK(rowOccurrences[doc.documentHash] == 1U);
    }
}

/// Lean Connected.ConnectedFrom: every member is reachable from the root over retained edges
/// that stay inside the cluster.
bool clusterConnected(const ClusterArtifact& cluster,
                      const std::vector<std::pair<std::string, std::string>>& edges) {
    if (cluster.memberDocumentHashes.empty()) {
        return true;
    }
    const std::unordered_set<std::string> members(cluster.memberDocumentHashes.begin(),
                                                  cluster.memberDocumentHashes.end());
    std::unordered_map<std::string, std::vector<std::string>> adjacency;
    for (const auto& [a, b] : edges) {
        if (members.contains(a) && members.contains(b)) {
            adjacency[a].push_back(b);
            adjacency[b].push_back(a);
        }
    }
    std::unordered_set<std::string> seen{cluster.memberDocumentHashes.front()};
    std::queue<std::string> frontier;
    frontier.push(cluster.memberDocumentHashes.front());
    while (!frontier.empty()) {
        const auto current = frontier.front();
        frontier.pop();
        for (const auto& next : adjacency[current]) {
            if (seen.insert(next).second) {
                frontier.push(next);
            }
        }
    }
    return seen.size() == members.size();
}

TopologyBuildConfig randomConfig(std::mt19937& rng) {
    static constexpr double kMinScores[] = {0.0, 0.25, 0.5, 0.75};
    std::uniform_int_distribution<std::size_t> pick(0, 3);
    TopologyBuildConfig config;
    config.minEdgeScore = kMinScores[pick(rng)];
    config.reciprocalOnly = pick(rng) != 0;
    config.allowOverlap = false;
    config.maxComponentDocs = 0;
    return config;
}

} // namespace

TEST_CASE("Lean contracts: connected engine output is a well-formed partition",
          "[unit][topology][lean][contract][property]") {
    // Lean: Artifacts.WellFormedBatch, Connected.ConnectedFrom, Connected.IsMedoidByScore
    // (m ∈ members), and Contracts.component_noCrossEdges_uncapped (maxComponentDocs = 0, so
    // NoCrossClusterEdges holds).
    std::mt19937 rng(kSeed);
    std::uniform_int_distribution<std::size_t> size(1, 12);
    ConnectedComponentTopologyEngine engine;
    for (int iteration = 0; iteration < kIterations; ++iteration) {
        const auto docs = randomDocuments(rng, size(rng));
        const auto config = randomConfig(rng);
        CAPTURE(iteration, docs.size(), config.minEdgeScore, config.reciprocalOnly);

        const auto built = engine.buildArtifacts(docs, config);
        REQUIRE(built.has_value());
        const auto& batch = built.value();
        CHECK(leanWellFormedBatch(batch));
        checkPartition(batch, docs);

        const auto edges = leanRetainedEdges(docs, config);
        const auto clusterOf = primaryClusterOf(batch);
        for (const auto& [a, b] : edges) {
            CAPTURE(a, b);
            CHECK(clusterOf.at(a) == clusterOf.at(b));
        }
        for (const auto& cluster : batch.clusters) {
            CAPTURE(cluster.clusterId);
            CHECK(clusterConnected(cluster, edges));
            REQUIRE(cluster.medoid.has_value());
            CHECK(std::ranges::find(cluster.memberDocumentHashes, cluster.medoid->documentHash) !=
                  cluster.memberDocumentHashes.end());
        }
    }
}

TEST_CASE("Lean contracts: capped connected components stay connected and within the cap",
          "[unit][topology][lean][contract][property]") {
    // Lean: ConstructionGates.ComponentsConnectedAndCapped (ConnectedFrom ∧ ComponentRespectsCap,
    // cap > 0 here; cap 0 means unlimited).
    std::mt19937 rng(kSeed + 1);
    std::uniform_int_distribution<std::size_t> size(1, 12);
    std::uniform_int_distribution<std::size_t> cap(1, 4);
    ConnectedComponentTopologyEngine engine;
    for (int iteration = 0; iteration < kIterations; ++iteration) {
        const auto docs = randomDocuments(rng, size(rng));
        auto config = randomConfig(rng);
        config.maxComponentDocs = cap(rng);
        CAPTURE(iteration, docs.size(), config.maxComponentDocs, config.minEdgeScore);

        const auto built = engine.buildArtifacts(docs, config);
        REQUIRE(built.has_value());
        const auto& batch = built.value();
        CHECK(leanWellFormedBatch(batch));
        checkPartition(batch, docs);
        const auto edges = leanRetainedEdges(docs, config);
        for (const auto& cluster : batch.clusters) {
            CAPTURE(cluster.clusterId);
            CHECK(cluster.memberCount <= config.maxComponentDocs);
            CHECK(clusterConnected(cluster, edges));
        }
    }
}

namespace {

/// The original connected components over the retained edges, before the anti-giant split:
/// Lean `componentOf` (hash to component index) and `componentSize` (index to size).
struct OriginalComponents {
    std::unordered_map<std::string, std::size_t> componentOf;
    std::vector<std::size_t> componentSize;
};

OriginalComponents
originalComponents(const std::vector<TopologyDocumentInput>& docs,
                   const std::vector<std::pair<std::string, std::string>>& edges) {
    std::unordered_map<std::string, std::vector<std::string>> adjacency;
    for (const auto& [a, b] : edges) {
        adjacency[a].push_back(b);
        adjacency[b].push_back(a);
    }
    OriginalComponents out;
    for (const auto& doc : docs) {
        if (out.componentOf.contains(doc.documentHash)) {
            continue;
        }
        const std::size_t index = out.componentSize.size();
        out.componentSize.push_back(0);
        std::queue<std::string> frontier;
        frontier.push(doc.documentHash);
        out.componentOf.emplace(doc.documentHash, index);
        while (!frontier.empty()) {
            const auto current = frontier.front();
            frontier.pop();
            ++out.componentSize[index];
            for (const auto& next : adjacency[current]) {
                if (out.componentOf.emplace(next, index).second) {
                    frontier.push(next);
                }
            }
        }
    }
    return out;
}

/// Lean Contracts.CrossEdgesOnlyFromSplit: an edge whose endpoints sit in different clusters
/// joins two pieces of one original component that exceeded the cap (cap 0 = unlimited).
/// Returns the number of crossing edges seen.
std::size_t checkCrossEdgesOnlyFromSplit(
    const TopologyArtifactBatch& batch, const std::vector<TopologyDocumentInput>& docs,
    const std::vector<std::pair<std::string, std::string>>& edges, std::size_t cap) {
    const auto clusterOf = primaryClusterOf(batch);
    const auto original = originalComponents(docs, edges);
    std::size_t crossing = 0;
    for (const auto& [a, b] : edges) {
        if (clusterOf.at(a) == clusterOf.at(b)) {
            continue;
        }
        ++crossing;
        CAPTURE(a, b, cap);
        const auto component = original.componentOf.at(a);
        CHECK(original.componentOf.at(b) == component);
        CHECK(cap != 0);
        CHECK(original.componentSize[component] > cap);
    }
    return crossing;
}

} // namespace

TEST_CASE("Lean contracts: retained edges cross clusters only inside a split component",
          "[unit][topology][lean][contract][property]") {
    // Lean Contracts.ConnectedComponentContract with the anti-giant cap: every cluster is
    // connected and has at most maxComponentDocs members (component_respectsCap), and a retained
    // edge crosses clusters only between pieces of one original component larger than the cap
    // (component_crossEdgesOnlyFromSplit). splitOversizedComponent cuts edges inside such a
    // component. With cap 0 or no oversized component nothing crosses (component_noCrossEdges,
    // component_noCrossEdges_uncapped).
    ConnectedComponentTopologyEngine engine;

    SECTION("a five-document chain split under cap 2, beside a pair under the cap") {
        TopologyBuildConfig config;
        config.minEdgeScore = 0.5;
        config.reciprocalOnly = true;
        config.maxComponentDocs = 2;
        std::vector<TopologyDocumentInput> docs;
        for (std::size_t i = 0; i < 5; ++i) {
            TopologyDocumentInput doc{.documentHash = docHash(i)};
            if (i > 0) {
                doc.neighbors.push_back(
                    {.documentHash = docHash(i - 1), .score = 0.9F, .reciprocal = true});
            }
            if (i + 1 < 5) {
                doc.neighbors.push_back(
                    {.documentHash = docHash(i + 1), .score = 0.9F, .reciprocal = true});
            }
            docs.push_back(std::move(doc));
        }
        docs.push_back(TopologyDocumentInput{
            .documentHash = "p0",
            .neighbors = {{.documentHash = "p1", .score = 0.9F, .reciprocal = true}}});
        docs.push_back(TopologyDocumentInput{
            .documentHash = "p1",
            .neighbors = {{.documentHash = "p0", .score = 0.9F, .reciprocal = true}}});

        const auto built = engine.buildArtifacts(docs, config);
        REQUIRE(built.has_value());
        const auto edges = leanRetainedEdges(docs, config);
        for (const auto& cluster : built.value().clusters) {
            CAPTURE(cluster.clusterId);
            CHECK(cluster.memberCount <= config.maxComponentDocs);
            CHECK(clusterConnected(cluster, edges));
        }
        CHECK(checkCrossEdgesOnlyFromSplit(built.value(), docs, edges, config.maxComponentDocs) >
              0U);
        const auto clusterOf = primaryClusterOf(built.value());
        CHECK(clusterOf.at("p0") == clusterOf.at("p1"));

        config.maxComponentDocs = 0;
        const auto uncapped = engine.buildArtifacts(docs, config);
        REQUIRE(uncapped.has_value());
        CHECK(checkCrossEdgesOnlyFromSplit(uncapped.value(), docs, edges, 0) == 0U);
    }

    SECTION("random corpora and caps") {
        std::mt19937 rng(kSeed + 7);
        std::uniform_int_distribution<std::size_t> size(1, 12);
        std::uniform_int_distribution<std::size_t> cap(0, 4);
        for (int iteration = 0; iteration < kIterations; ++iteration) {
            const auto docs = randomDocuments(rng, size(rng));
            auto config = randomConfig(rng);
            config.maxComponentDocs = cap(rng);
            CAPTURE(iteration, docs.size(), config.maxComponentDocs, config.minEdgeScore,
                    config.reciprocalOnly);

            const auto built = engine.buildArtifacts(docs, config);
            REQUIRE(built.has_value());
            const auto edges = leanRetainedEdges(docs, config);
            for (const auto& cluster : built.value().clusters) {
                CAPTURE(cluster.clusterId);
                if (config.maxComponentDocs > 0) {
                    CHECK(cluster.memberCount <= config.maxComponentDocs);
                }
                CHECK(clusterConnected(cluster, edges));
            }
            checkCrossEdgesOnlyFromSplit(built.value(), docs, edges, config.maxComponentDocs);
        }
    }
}

TEST_CASE("Lean contracts: an edge needs both directed observations above the score floor",
          "[unit][topology][lean][contract]") {
    // Lean ConstructionGates.ReciprocalNeighborWitness admits an edge only when the forward and
    // the reverse observation both pass minNeighborScore. With reciprocalOnly the engine does not
    // take the per-neighbor `reciprocal` flag's word: both endpoints must list each other above
    // the floor, and the edge carries the weaker score.
    ConnectedComponentTopologyEngine engine;
    TopologyBuildConfig config;
    config.minEdgeScore = 0.5;
    config.reciprocalOnly = true;
    const std::vector<TopologyDocumentInput> docs{
        TopologyDocumentInput{
            .documentHash = "a",
            .neighbors = {{.documentHash = "b", .score = 0.9F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "b",
            .neighbors = {{.documentHash = "a", .score = 0.1F, .reciprocal = true}}},
    };

    const auto built = engine.buildArtifacts(docs, config);
    REQUIRE(built.has_value());
    const auto clusterOf = primaryClusterOf(built.value());
    CHECK(clusterOf.at("a") != clusterOf.at("b"));
}

TEST_CASE("Lean contracts: Louvain and k-means outputs are well-formed partitions",
          "[unit][topology][lean][contract][property]") {
    // Lean: Artifacts.WellFormedBatch is stated independently of the algorithm.
    std::mt19937 rng(kSeed + 2);
    std::uniform_int_distribution<std::size_t> size(1, 12);
    LouvainTopologyEngine louvain;
    KMeansTopologyEngine kmeans;
    for (int iteration = 0; iteration < kIterations; ++iteration) {
        const auto docs = randomDocuments(rng, size(rng));
        const auto config = randomConfig(rng);
        CAPTURE(iteration, docs.size());
        for (ITopologyEngine* engine :
             {static_cast<ITopologyEngine*>(&louvain), static_cast<ITopologyEngine*>(&kmeans)}) {
            const auto built = engine->buildArtifacts(docs, config);
            REQUIRE(built.has_value());
            CHECK(leanWellFormedBatch(built.value()));
            checkPartition(built.value(), docs);
        }
    }
}

namespace {

/// Drives updateArtifacts the way TopologyManager does: define the dirty region for the changed
/// documents, then rebuild from the current inputs of every document in the expanded region.
/// Returns false when the region asks for a wider rebuild (the manager then rebuilds fully).
bool regionDrivenUpdate(std::mt19937& rng, yams::topology::DirtyRegionExpansionMode mode,
                        std::vector<TopologyDocumentInput>& current,
                        TopologyArtifactBatch& updated) {
    std::uniform_int_distribution<std::size_t> size(2, 12);
    std::uniform_real_distribution<float> unit(0.0F, 1.0F);
    const auto base = randomDocuments(rng, size(rng));
    auto config = randomConfig(rng);
    config.dirtyRegionExpansion = mode;
    ConnectedComponentTopologyEngine engine;
    const auto existing = engine.buildArtifacts(base, config);
    REQUIRE(existing.has_value());

    // Rewire a random subset of documents against fresh random neighbors.
    const auto rewired = randomDocuments(rng, base.size());
    current = base;
    std::vector<TopologyDocumentInput> changed;
    for (std::size_t i = 0; i < base.size(); ++i) {
        if (unit(rng) < 0.4F) {
            current[i] = rewired[i];
            changed.push_back(rewired[i]);
        }
    }
    const auto region = engine.defineDirtyRegion(existing.value(), changed, config);
    REQUIRE(region.has_value());
    if (region.value().requiresWiderRebuild) {
        return false;
    }
    const std::unordered_set<std::string> expanded(region.value().expandedDocumentHashes.begin(),
                                                   region.value().expandedDocumentHashes.end());
    std::vector<TopologyDocumentInput> regionInputs;
    for (const auto& doc : current) {
        if (expanded.contains(doc.documentHash)) {
            regionInputs.push_back(doc);
        }
    }
    const auto result = engine.updateArtifacts(existing.value(), regionInputs, config);
    REQUIRE(result.has_value());
    updated = result.value();
    return true;
}

} // namespace

TEST_CASE("Lean contracts: region-driven incremental updates stay well-formed partitions",
          "[unit][topology][lean][contract][property]") {
    // Lean: Artifacts.WellFormedBatch after ITopologyEngine::updateArtifacts. The model has no
    // update theorem; corpus coverage after an update is the unstated precondition checked here.
    // updateArtifacts drops every member of a touched cluster that is not in its input, so the
    // caller must pass the whole prior cluster; every dirty region does.
    using yams::topology::DirtyRegionExpansionMode;
    std::mt19937 rng(kSeed + 3);
    for (const auto mode :
         {DirtyRegionExpansionMode::PriorClusterAndNeighbors, DirtyRegionExpansionMode::Adaptive}) {
        for (int iteration = 0; iteration < kIterations; ++iteration) {
            std::vector<TopologyDocumentInput> current;
            TopologyArtifactBatch updated;
            if (!regionDrivenUpdate(rng, mode, current, updated)) {
                continue;
            }
            CAPTURE(static_cast<int>(mode), iteration, current.size());
            CHECK(leanWellFormedBatch(updated));
            checkPartition(updated, current);
        }
    }
}

TEST_CASE("Lean contracts: a neighbors-only region update keeps every document",
          "[unit][topology][lean][contract][property]") {
    // Same composition as above with DirtyRegionExpansionMode::NeighborsOnly. updateArtifacts
    // removes every touched prior cluster whole, so defineDirtyRegion closes the region over
    // prior clusters in every mode; otherwise the untouched members of a changed document's
    // prior cluster would vanish from the snapshot until the next full rebuild.
    using yams::topology::DirtyRegionExpansionMode;
    std::mt19937 rng(kSeed + 6);
    for (int iteration = 0; iteration < kIterations; ++iteration) {
        std::vector<TopologyDocumentInput> current;
        TopologyArtifactBatch updated;
        if (!regionDrivenUpdate(rng, DirtyRegionExpansionMode::NeighborsOnly, current, updated)) {
            continue;
        }
        CAPTURE(iteration, current.size());
        CHECK(leanWellFormedBatch(updated));
        checkPartition(updated, current);
    }
}

TEST_CASE("Lean contracts: dirty regions contain their seeds and respect the region budget",
          "[unit][topology][lean][contract][property]") {
    // Lean: Contracts.DirtyRegionContract (seeds ⊆ expanded; budget unless requiresWiderRebuild).
    std::mt19937 rng(kSeed + 4);
    std::uniform_int_distribution<std::size_t> size(2, 12);
    std::uniform_int_distribution<std::size_t> budget(1, 8);
    std::uniform_real_distribution<float> unit(0.0F, 1.0F);
    ConnectedComponentTopologyEngine engine;
    for (int iteration = 0; iteration < kIterations; ++iteration) {
        const auto docs = randomDocuments(rng, size(rng));
        auto config = randomConfig(rng);
        const auto existing = engine.buildArtifacts(docs, config);
        REQUIRE(existing.has_value());

        const auto rewired = randomDocuments(rng, docs.size() + 2);
        std::vector<TopologyDocumentInput> changed;
        for (const auto& doc : rewired) {
            if (unit(rng) < 0.4F) {
                changed.push_back(doc);
            }
        }
        config.maxDirtyRegionDocs = budget(rng);
        CAPTURE(iteration, docs.size(), changed.size(), config.maxDirtyRegionDocs);

        const auto region = engine.defineDirtyRegion(existing.value(), changed, config);
        REQUIRE(region.has_value());
        const auto& r = region.value();
        const std::unordered_set<std::string> expanded(r.expandedDocumentHashes.begin(),
                                                       r.expandedDocumentHashes.end());
        for (const auto& seed : r.seedDocumentHashes) {
            CAPTURE(seed);
            CHECK(expanded.contains(seed));
        }
        // The Lean seed list is whatever the engine names as seeds. A changed document that an
        // earlier one already pulled in is not listed as a seed, so check the changed documents
        // against the expanded region: a bounded region must still cover every one of them.
        if (!r.requiresWiderRebuild) {
            for (const auto& doc : changed) {
                CAPTURE(doc.documentHash);
                CHECK(expanded.contains(doc.documentHash));
            }
        }
        CHECK((r.requiresWiderRebuild ||
               r.expandedDocumentHashes.size() <= config.maxDirtyRegionDocs));
    }
}

TEST_CASE("Lean contracts: routes name known clusters, obey the limit and are ordered",
          "[unit][topology][lean][contract][router][property]") {
    // Lean: Contracts.RouteResultContract (RoutesReferenceClusters ∧ length ≤ limit) for
    // limit ≥ 1, plus the score-descending, cluster-id tie-break order the router documents.
    std::mt19937 rng(kSeed + 5);
    std::uniform_int_distribution<std::size_t> size(1, 12);
    std::uniform_int_distribution<std::size_t> limit(1, 5);
    std::uniform_real_distribution<float> unit(0.0F, 1.0F);
    std::uniform_real_distribution<float> coord(-1.0F, 1.0F);
    ConnectedComponentTopologyEngine engine;
    SparseGuidedClusterRouter router;
    for (int iteration = 0; iteration < kIterations; ++iteration) {
        const auto docs = randomDocuments(rng, size(rng));
        const auto built = engine.buildArtifacts(docs, randomConfig(rng));
        REQUIRE(built.has_value());
        const auto& batch = built.value();
        std::unordered_set<std::string> clusterIds;
        for (const auto& cluster : batch.clusters) {
            clusterIds.insert(cluster.clusterId);
        }

        TopologyRouteRequest request;
        request.limit = limit(rng);
        request.sparseDenseAlpha = unit(rng);
        if (unit(rng) < 0.5F) {
            request.queryEmbedding = {coord(rng), coord(rng), coord(rng)};
        }
        for (std::size_t i = 0; i < docs.size() + 1; ++i) {
            if (unit(rng) < 0.3F) {
                request.seedDocumentHashes.push_back(i < docs.size() ? docHash(i) : "missing");
            }
        }
        CAPTURE(iteration, docs.size(), request.limit, request.seedDocumentHashes.size());

        const auto routes = router.route(request, batch);
        REQUIRE(routes.has_value());
        const auto& r = routes.value();
        CHECK(r.size() <= request.limit);
        std::unordered_set<std::string> seen;
        for (std::size_t i = 0; i < r.size(); ++i) {
            CAPTURE(r[i].clusterId);
            CHECK(clusterIds.contains(r[i].clusterId));
            CHECK(seen.insert(r[i].clusterId).second);
            if (i > 0) {
                const bool ordered =
                    r[i - 1].routeScore > r[i].routeScore ||
                    (r[i - 1].routeScore == r[i].routeScore && r[i - 1].clusterId < r[i].clusterId);
                CHECK(ordered);
            }
        }
    }
}

TEST_CASE("Lean contracts: a zero route limit returns no routes",
          "[unit][topology][lean][contract][router][!shouldfail]") {
    // Lean Contracts.RouteResultContract requires routes.length ≤ request.limit for every limit.
    // SparseGuidedClusterRouter treats limit == 0 as "no limit" and returns every cluster, so the
    // contract holds only for limit ≥ 1. Callers that pass 0 get an unbounded route list.
    ConnectedComponentTopologyEngine engine;
    const std::vector<TopologyDocumentInput> docs{
        TopologyDocumentInput{.documentHash = "a"},
        TopologyDocumentInput{.documentHash = "b"},
    };
    const auto built = engine.buildArtifacts(docs, TopologyBuildConfig{});
    REQUIRE(built.has_value());
    REQUIRE(built.value().clusters.size() == 2U);

    TopologyRouteRequest request;
    request.limit = 0;
    request.seedDocumentHashes = {"a", "b"};
    const auto routes = SparseGuidedClusterRouter{}.route(request, built.value());
    REQUIRE(routes.has_value());
    CHECK(routes.value().size() <= request.limit);
}

TEST_CASE("Lean contracts: boundary spill keeps member counts in step",
          "[unit][topology][lean][contract][overlap]") {
    // Lean: ClusterArtifact.memberCountMatches and membershipsReferenceClusters after SOAR spill.
    // Same geometry as "SOAR boundary spill chooses a complementary secondary residual".
    const std::vector<TopologyDocumentInput> documents{
        TopologyDocumentInput{.documentHash = "x", .embedding = {2.0F, 2.0F}},
        TopologyDocumentInput{.documentHash = "near", .embedding = {0.9F, 2.0F}},
        TopologyDocumentInput{.documentHash = "orthogonal", .embedding = {2.0F, 0.8F}},
    };
    TopologyArtifactBatch batch;
    batch.clusters = {
        ClusterArtifact{.clusterId = "primary",
                        .memberCount = 1,
                        .memberDocumentHashes = {"x"},
                        .centroidEmbedding = {1.0F, 2.0F}},
        ClusterArtifact{.clusterId = "near",
                        .memberCount = 1,
                        .memberDocumentHashes = {"near"},
                        .centroidEmbedding = {0.9F, 2.0F}},
        ClusterArtifact{.clusterId = "orthogonal",
                        .memberCount = 1,
                        .memberDocumentHashes = {"orthogonal"},
                        .centroidEmbedding = {2.0F, 0.8F}},
    };
    batch.memberships = {
        DocumentClusterMembership{.documentHash = "x", .clusterId = "primary"},
        DocumentClusterMembership{.documentHash = "near", .clusterId = "near"},
        DocumentClusterMembership{.documentHash = "orthogonal", .clusterId = "orthogonal"},
    };
    TopologyBuildConfig config;
    config.allowOverlap = true;
    config.overlapLimit = 1;
    config.overlapBoundaryDistanceRatio = 1.25;
    config.overlapResidualPenalty = 1.0;

    REQUIRE(applyOrthogonalBoundarySpill(documents, config, batch) == 1U);
    std::unordered_set<std::string> clusterIds;
    for (const auto& cluster : batch.clusters) {
        CAPTURE(cluster.clusterId);
        CHECK(cluster.memberCount == cluster.memberDocumentHashes.size());
        clusterIds.insert(cluster.clusterId);
    }
    for (const auto& membership : batch.memberships) {
        CHECK(clusterIds.contains(membership.clusterId));
        for (const auto& overlap : membership.overlapClusterIds) {
            CHECK(clusterIds.contains(overlap));
        }
    }
}

TEST_CASE("Lean contracts: a spilled member has a membership row for its secondary cluster",
          "[unit][topology][lean][contract][overlap][!shouldfail]") {
    // Lean Artifacts.clusterMembersHaveMemberships requires a membership row whose clusterId is
    // the cluster for every member hash. After boundary spill a replica is listed in the
    // secondary cluster's members, but its only row names the primary cluster and lists the
    // secondary in overlapClusterIds. With allowOverlap the batch does not satisfy
    // WellFormedBatch as written; the model needs an overlap-aware membership predicate.
    const std::vector<TopologyDocumentInput> documents{
        TopologyDocumentInput{.documentHash = "x", .embedding = {2.0F, 2.0F}},
        TopologyDocumentInput{.documentHash = "near", .embedding = {0.9F, 2.0F}},
        TopologyDocumentInput{.documentHash = "orthogonal", .embedding = {2.0F, 0.8F}},
    };
    TopologyArtifactBatch batch;
    batch.clusters = {
        ClusterArtifact{.clusterId = "primary",
                        .memberCount = 1,
                        .memberDocumentHashes = {"x"},
                        .centroidEmbedding = {1.0F, 2.0F}},
        ClusterArtifact{.clusterId = "near",
                        .memberCount = 1,
                        .memberDocumentHashes = {"near"},
                        .centroidEmbedding = {0.9F, 2.0F}},
        ClusterArtifact{.clusterId = "orthogonal",
                        .memberCount = 1,
                        .memberDocumentHashes = {"orthogonal"},
                        .centroidEmbedding = {2.0F, 0.8F}},
    };
    batch.memberships = {
        DocumentClusterMembership{.documentHash = "x", .clusterId = "primary"},
        DocumentClusterMembership{.documentHash = "near", .clusterId = "near"},
        DocumentClusterMembership{.documentHash = "orthogonal", .clusterId = "orthogonal"},
    };
    TopologyBuildConfig config;
    config.allowOverlap = true;
    config.overlapLimit = 1;
    config.overlapBoundaryDistanceRatio = 1.25;
    config.overlapResidualPenalty = 1.0;

    REQUIRE(applyOrthogonalBoundarySpill(documents, config, batch) == 1U);
    CHECK(leanWellFormedBatch(batch));
}
