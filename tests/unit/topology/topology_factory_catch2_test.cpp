#include <catch2/catch_test_macros.hpp>

#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_factory.h>

#include <algorithm>
#include <string>
#include <vector>

using yams::topology::ITopologyEngine;
using yams::topology::listAlgorithms;
using yams::topology::makeEngine;
using yams::topology::resolveFactoryKey;

TEST_CASE("topology::makeEngine returns a usable engine for the default key",
          "[topology][factory][p3_1][catch2]") {
    auto engine = makeEngine("connected");
    REQUIRE(engine != nullptr);

    yams::topology::TopologyBuildConfig cfg;
    cfg.reciprocalOnly = true;
    cfg.inputKind = yams::topology::TopologyInputKind::Hybrid;

    std::vector<yams::topology::TopologyDocumentInput> empty;
    auto result = engine->buildArtifacts(empty, cfg);
    REQUIRE(result);
    // The connected-components engine stamps its own label on the batch; the
    // factory key ("connected") is distinct from the engine label.
    CHECK(result.value().algorithm == "connected_components_v1");
}

TEST_CASE("topology::makeEngine resolves empty / unknown keys to connected",
          "[topology][factory][p3_1][catch2]") {
    auto e1 = makeEngine("");
    auto e2 = makeEngine("nonexistent_algorithm");
    REQUIRE(e1 != nullptr);
    REQUIRE(e2 != nullptr);
    // Both fallback paths should yield the baseline engine, which stamps
    // "connected_components_v1" on empty inputs.
    yams::topology::TopologyBuildConfig cfg;
    std::vector<yams::topology::TopologyDocumentInput> empty;
    auto r1 = e1->buildArtifacts(empty, cfg);
    auto r2 = e2->buildArtifacts(empty, cfg);
    REQUIRE(r1);
    REQUIRE(r2);
    CHECK(r1.value().algorithm == "connected_components_v1");
    CHECK(r2.value().algorithm == "connected_components_v1");
}

TEST_CASE("topology::resolveFactoryKey normalizes unknown inputs",
          "[topology][factory][p3_1][catch2]") {
    CHECK(resolveFactoryKey("connected") == std::string_view{"connected"});
    CHECK(resolveFactoryKey("") == std::string_view{"connected"});
    CHECK(resolveFactoryKey("not_registered") == std::string_view{"connected"});
}

TEST_CASE("topology::listAlgorithms includes the default key",
          "[topology][factory][p3_1][catch2]") {
    const auto algos = listAlgorithms();
    REQUIRE(!algos.empty());
    const bool hasConnected =
        std::find(algos.begin(), algos.end(), std::string{"connected"}) != algos.end();
    CHECK(hasConnected);
}

namespace {

std::vector<yams::topology::TopologyDocumentInput> buildTwoClusterFixture() {
    // Two disjoint triangles: {a,b,c} and {d,e,f}. Reciprocal edges, weight 0.9.
    const auto edge = [](std::string hash, float s) {
        yams::topology::TopologyNeighbor n;
        n.documentHash = std::move(hash);
        n.score = s;
        n.reciprocal = true;
        return n;
    };
    auto mk = [&](std::string hash, std::vector<std::string> neighbors) {
        yams::topology::TopologyDocumentInput doc;
        doc.documentHash = std::move(hash);
        for (auto& nb : neighbors) {
            doc.neighbors.push_back(edge(std::move(nb), 0.9f));
        }
        doc.embedding = {0.0f, 0.0f, 0.0f};
        return doc;
    };
    std::vector<yams::topology::TopologyDocumentInput> docs;
    docs.push_back(mk("a", {"b", "c"}));
    docs.push_back(mk("b", {"a", "c"}));
    docs.push_back(mk("c", {"a", "b"}));
    docs.push_back(mk("d", {"e", "f"}));
    docs.push_back(mk("e", {"d", "f"}));
    docs.push_back(mk("f", {"d", "e"}));
    // Give embeddings separation so kmeans can split them too.
    docs[0].embedding = {1.0f, 0.0f, 0.0f};
    docs[1].embedding = {0.95f, 0.05f, 0.0f};
    docs[2].embedding = {0.9f, 0.1f, 0.0f};
    docs[3].embedding = {0.0f, 1.0f, 0.0f};
    docs[4].embedding = {0.05f, 0.95f, 0.0f};
    docs[5].embedding = {0.1f, 0.9f, 0.0f};
    return docs;
}

} // namespace

TEST_CASE("topology::makeEngine builds artifacts for the Axis-8 engines",
          "[topology][factory][axis8][catch2]") {
    const auto docs = buildTwoClusterFixture();
    yams::topology::TopologyBuildConfig cfg;
    cfg.reciprocalOnly = true;
    cfg.inputKind = yams::topology::TopologyInputKind::Hybrid;
    cfg.kmeansK = 2;

    for (const char* key : {"connected", "louvain", "label_propagation", "kmeans_embedding"}) {
        auto engine = makeEngine(key);
        REQUIRE(engine != nullptr);
        auto result = engine->buildArtifacts(docs, cfg);
        REQUIRE(result);
        const auto& batch = result.value();
        CAPTURE(key, batch.algorithm);
        CHECK(batch.clusters.size() == 2);
        CHECK(batch.memberships.size() == docs.size());
    }
}

namespace {

// Two K_4 cliques {a,b,c,d} and {e,f,g,h} joined by a single weak bridge d↔e.
// Louvain should split the graph into two communities of size 4 because the
// modularity gain from keeping the bridge edge inside a merged community is
// dominated by the loss of per-community density. This is the classic
// "bridged cliques" community-detection benchmark — any modularity-optimizing
// algorithm must recover it.
std::vector<yams::topology::TopologyDocumentInput> buildBridgedCliquesFixture() {
    const auto edge = [](std::string hash, float s) {
        yams::topology::TopologyNeighbor n;
        n.documentHash = std::move(hash);
        n.score = s;
        n.reciprocal = true;
        return n;
    };
    auto mk = [&](std::string hash, std::vector<std::pair<std::string, float>> nbrs) {
        yams::topology::TopologyDocumentInput doc;
        doc.documentHash = std::move(hash);
        for (auto& [nb, w] : nbrs) {
            doc.neighbors.push_back(edge(std::move(nb), w));
        }
        return doc;
    };
    constexpr float W = 1.0f;
    constexpr float B = 0.05f; // bridge weight << intra-clique weight
    std::vector<yams::topology::TopologyDocumentInput> docs;
    docs.push_back(mk("a", {{"b", W}, {"c", W}, {"d", W}}));
    docs.push_back(mk("b", {{"a", W}, {"c", W}, {"d", W}}));
    docs.push_back(mk("c", {{"a", W}, {"b", W}, {"d", W}}));
    docs.push_back(mk("d", {{"a", W}, {"b", W}, {"c", W}, {"e", B}})); // bridge
    docs.push_back(mk("e", {{"d", B}, {"f", W}, {"g", W}, {"h", W}})); // bridge
    docs.push_back(mk("f", {{"e", W}, {"g", W}, {"h", W}}));
    docs.push_back(mk("g", {{"e", W}, {"f", W}, {"h", W}}));
    docs.push_back(mk("h", {{"e", W}, {"f", W}, {"g", W}}));
    return docs;
}

} // namespace

TEST_CASE("topology::louvain splits bridged cliques by modularity",
          "[topology][factory][axis8][louvain][catch2]") {
    // Correctness gate: Louvain must recover the known community structure
    // on the bridged-K_4 benchmark before we trust any L+/XXL bench results.
    // Expected outcome: two communities, each containing exactly one clique
    // ({a,b,c,d} and {e,f,g,h}).
    const auto docs = buildBridgedCliquesFixture();
    yams::topology::TopologyBuildConfig cfg;
    cfg.reciprocalOnly = true;
    cfg.inputKind = yams::topology::TopologyInputKind::Hybrid;

    auto engine = makeEngine("louvain");
    REQUIRE(engine != nullptr);
    auto result = engine->buildArtifacts(docs, cfg);
    REQUIRE(result);
    const auto& batch = result.value();
    CAPTURE(batch.algorithm);

    REQUIRE(batch.clusters.size() == 2);
    REQUIRE(batch.memberships.size() == docs.size());

    std::unordered_map<std::string, std::string> clusterOf;
    for (const auto& m : batch.memberships) {
        clusterOf.emplace(m.documentHash, m.clusterId);
    }
    REQUIRE(clusterOf.size() == docs.size());

    const std::string clusterA = clusterOf.at("a");
    const std::string clusterE = clusterOf.at("e");
    CHECK(clusterA != clusterE);
    for (const char* h : {"b", "c", "d"}) {
        CAPTURE(h);
        CHECK(clusterOf.at(h) == clusterA);
    }
    for (const char* h : {"f", "g", "h"}) {
        CAPTURE(h);
        CHECK(clusterOf.at(h) == clusterE);
    }
}
