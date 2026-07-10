#include <catch2/catch_test_macros.hpp>

#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_factory.h>
#include <yams/topology/topology_sgc.h>

#include <cmath>

#include <algorithm>
#include <string>
#include <unordered_map>
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
    CHECK(resolveFactoryKey("hdbscan") == std::string_view{"connected"});
}

TEST_CASE("topology::makeEngine resolves the kmeans key to the k-means engine",
          "[topology][factory][kmeans][catch2]") {
    CHECK(resolveFactoryKey("kmeans") == std::string_view{"kmeans"});
    auto engine = makeEngine("kmeans");
    REQUIRE(engine != nullptr);
    yams::topology::TopologyBuildConfig cfg;
    std::vector<yams::topology::TopologyDocumentInput> empty;
    auto result = engine->buildArtifacts(empty, cfg);
    REQUIRE(result);
    CHECK(result.value().algorithm == "kmeans_v1");
}

TEST_CASE("topology::listAlgorithms includes the default key",
          "[topology][factory][p3_1][catch2]") {
    const auto algos = listAlgorithms();
    REQUIRE(!algos.empty());
    const bool hasConnected =
        std::find(algos.begin(), algos.end(), std::string{"connected"}) != algos.end();
    CHECK(hasConnected);
    CHECK(std::find(algos.begin(), algos.end(), std::string{"hdbscan"}) == algos.end());
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

    for (const char* key : {"connected", "kmeans"}) {
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

TEST_CASE("topology::applySGCSmoothing hops=0 is a no-op", "[topology][sgc][catch2]") {
    auto docs = buildTwoClusterFixture();
    const auto before = docs;
    yams::topology::TopologyBuildConfig cfg;
    cfg.reciprocalOnly = true;
    yams::topology::applySGCSmoothing(docs, cfg, 0);
    REQUIRE(docs.size() == before.size());
    for (std::size_t i = 0; i < docs.size(); ++i) {
        CAPTURE(i);
        REQUIRE(docs[i].embedding.size() == before[i].embedding.size());
        for (std::size_t d = 0; d < docs[i].embedding.size(); ++d) {
            CHECK(docs[i].embedding[d] == before[i].embedding[d]);
        }
    }
}

TEST_CASE("topology::applySGCSmoothing preserves topology document identity",
          "[topology][sgc][catch2]") {
    auto docs = buildTwoClusterFixture();
    for (std::size_t i = 0; i < docs.size(); ++i) {
        docs[i].filePath = "fixture/" + docs[i].documentHash + ".txt";
        docs[i].metadata.emplace("ordinal", std::to_string(i));
    }
    const auto before = docs;
    yams::topology::TopologyBuildConfig cfg;
    cfg.reciprocalOnly = true;

    yams::topology::applySGCSmoothing(docs, cfg, 1);

    REQUIRE(docs.size() == before.size());
    for (std::size_t i = 0; i < docs.size(); ++i) {
        CAPTURE(i);
        CHECK(docs[i].documentHash == before[i].documentHash);
        CHECK(docs[i].filePath == before[i].filePath);
        CHECK(docs[i].metadata == before[i].metadata);
        REQUIRE(docs[i].neighbors.size() == before[i].neighbors.size());
        for (std::size_t j = 0; j < docs[i].neighbors.size(); ++j) {
            CHECK(docs[i].neighbors[j].documentHash == before[i].neighbors[j].documentHash);
            CHECK(docs[i].neighbors[j].score == before[i].neighbors[j].score);
            CHECK(docs[i].neighbors[j].reciprocal == before[i].neighbors[j].reciprocal);
        }
        CHECK(docs[i].embedding.size() == before[i].embedding.size());
    }
}

TEST_CASE("topology::applySGCSmoothing preserves identity for ragged inputs",
          "[topology][sgc][catch2]") {
    auto docs = buildTwoClusterFixture();
    docs[0].metadata.emplace("kind", "reference");
    docs[1].embedding.clear();
    docs[2].embedding = {0.9F, 0.1F};
    docs[3].neighbors.push_back({.documentHash = "missing", .score = 0.95F, .reciprocal = true});
    docs.push_back(yams::topology::TopologyDocumentInput{.documentHash = "",
                                                         .filePath = "/repo/unhashed.txt",
                                                         .embedding = {42.0F},
                                                         .neighbors = {},
                                                         .metadata = {{"kind", "unhashed"}}});

    const auto before = docs;
    yams::topology::TopologyBuildConfig cfg;
    cfg.reciprocalOnly = true;

    yams::topology::applySGCSmoothing(docs, cfg, 3);

    REQUIRE((docs.size() == before.size()));
    for (std::size_t i = 0; i < docs.size(); ++i) {
        CAPTURE(i);
        CHECK((docs[i].documentHash == before[i].documentHash));
        CHECK((docs[i].filePath == before[i].filePath));
        CHECK((docs[i].metadata == before[i].metadata));
        REQUIRE((docs[i].neighbors.size() == before[i].neighbors.size()));
        for (std::size_t j = 0; j < docs[i].neighbors.size(); ++j) {
            CHECK((docs[i].neighbors[j].documentHash == before[i].neighbors[j].documentHash));
            CHECK((docs[i].neighbors[j].score == before[i].neighbors[j].score));
            CHECK((docs[i].neighbors[j].reciprocal == before[i].neighbors[j].reciprocal));
        }
        CHECK((docs[i].embedding.size() == before[i].embedding.size()));
    }
}

TEST_CASE("topology::applySGCSmoothing shrinks intra-cluster variance", "[topology][sgc][catch2]") {
    auto docs = buildTwoClusterFixture();
    yams::topology::TopologyBuildConfig cfg;
    cfg.reciprocalOnly = true;

    const auto variance = [](const std::vector<yams::topology::TopologyDocumentInput>& v,
                             std::size_t lo, std::size_t hi) {
        std::vector<double> mean(v[lo].embedding.size(), 0.0);
        const double n = static_cast<double>(hi - lo);
        for (std::size_t i = lo; i < hi; ++i) {
            for (std::size_t d = 0; d < mean.size(); ++d) {
                mean[d] += static_cast<double>(v[i].embedding[d]);
            }
        }
        for (auto& m : mean) {
            m /= n;
        }
        double acc = 0.0;
        for (std::size_t i = lo; i < hi; ++i) {
            for (std::size_t d = 0; d < mean.size(); ++d) {
                const double diff = static_cast<double>(v[i].embedding[d]) - mean[d];
                acc += diff * diff;
            }
        }
        return acc / n;
    };

    const double beforeClusterA = variance(docs, 0, 3);
    const double beforeClusterB = variance(docs, 3, 6);

    auto smoothed = docs;
    yams::topology::applySGCSmoothing(smoothed, cfg, 2);

    REQUIRE(smoothed.size() == docs.size());
    const double afterClusterA = variance(smoothed, 0, 3);
    const double afterClusterB = variance(smoothed, 3, 6);

    CHECK(afterClusterA < beforeClusterA);
    CHECK(afterClusterB < beforeClusterB);
}
