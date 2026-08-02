#include <catch2/catch_test_macros.hpp>

#include <yams/topology/protected_relation_cover.h>
#include <yams/topology/topology_baseline.h>

#include <algorithm>
#include <limits>
#include <string_view>
#include <vector>

using namespace yams::topology;

TEST_CASE("Protected relation construction identity is stable across input order",
          "[unit][topology][protected-relation][identity]") {
    std::vector<TopologyDocumentInput> documents = {
        {.documentHash = "a",
         .neighbors = {{.documentHash = "b", .score = 0.75F, .reciprocal = true},
                       {.documentHash = "c", .score = 0.10F, .reciprocal = true}}},
        {.documentHash = "b",
         .neighbors = {{.documentHash = "a", .score = 0.80F, .reciprocal = true},
                       {.documentHash = "c", .score = 0.50F, .reciprocal = true}}},
        {.documentHash = "c",
         .neighbors = {{.documentHash = "b", .score = 0.40F, .reciprocal = true}}},
    };
    TopologyBuildConfig config;
    config.minEdgeScore = 0.25;
    config.reciprocalOnly = true;

    constexpr std::string_view expected =
        "semantic_neighbor:v1:construction:fnv1a64:c3d98ce69d6e540e";
    CHECK((protectedRelationConstructionIdentity(documents, config) == expected));
    ConnectedComponentTopologyEngine engine;
    const auto artifacts = engine.buildArtifacts(documents, config);
    REQUIRE(artifacts);
    CHECK((artifacts.value().protectedRelationIdentity == expected));

    std::ranges::reverse(documents);
    for (auto& document : documents) {
        std::ranges::reverse(document.neighbors);
    }
    CHECK((protectedRelationConstructionIdentity(documents, config) == expected));
}

TEST_CASE("Protected relation construction identity preserves float encoding",
          "[unit][topology][protected-relation][identity]") {
    std::vector<TopologyDocumentInput> documents{
        {.documentHash = "a",
         .neighbors =
             {{.documentHash = "b", .score = 0.1F, .reciprocal = true},
              {.documentHash = "c", .score = 1.0F / 3.0F, .reciprocal = true},
              {.documentHash = "d", .score = std::numeric_limits<float>::max(), .reciprocal = true},
              {.documentHash = "e", .score = std::numeric_limits<float>::min(), .reciprocal = true},
              {.documentHash = "f", .score = -0.0F, .reciprocal = true}}},
        {.documentHash = "b"},
        {.documentHash = "c"},
        {.documentHash = "d"},
        {.documentHash = "e"},
        {.documentHash = "f"},
    };
    TopologyBuildConfig config;
    config.minEdgeScore = -std::numeric_limits<double>::infinity();
    config.reciprocalOnly = true;

    CHECK((protectedRelationConstructionIdentity(documents, config) ==
           "semantic_neighbor:v1:construction:fnv1a64:eed83faf2209a96a"));
}

TEST_CASE("Protected relation construction ignores non-finite scores",
          "[unit][topology][protected-relation][identity]") {
    std::vector<TopologyDocumentInput> cleanDocuments{
        {.documentHash = "a",
         .neighbors = {{.documentHash = "b", .score = 0.75F, .reciprocal = true}}},
        {.documentHash = "b"},
        {.documentHash = "c"},
        {.documentHash = "d"},
    };
    auto nonFiniteDocuments = cleanDocuments;
    nonFiniteDocuments[0].neighbors.push_back({.documentHash = "b",
                                               .score = std::numeric_limits<float>::signaling_NaN(),
                                               .reciprocal = true});
    nonFiniteDocuments[0].neighbors.push_back(
        {.documentHash = "c", .score = std::numeric_limits<float>::infinity(), .reciprocal = true});
    nonFiniteDocuments[0].neighbors.push_back({.documentHash = "d",
                                               .score = -std::numeric_limits<float>::infinity(),
                                               .reciprocal = true});

    TopologyBuildConfig config;
    config.minEdgeScore = -std::numeric_limits<double>::infinity();
    config.reciprocalOnly = true;
    const auto expected = protectedRelationConstructionIdentity(cleanDocuments, config);
    CHECK((protectedRelationConstructionIdentity(nonFiniteDocuments, config) == expected));

    ConnectedComponentTopologyEngine engine;
    const auto cleanArtifacts = engine.buildArtifacts(cleanDocuments, config);
    const auto nonFiniteArtifacts = engine.buildArtifacts(nonFiniteDocuments, config);
    REQUIRE(cleanArtifacts);
    REQUIRE(nonFiniteArtifacts);
    CHECK((nonFiniteArtifacts.value().protectedRelationIdentity == expected));
    CHECK((nonFiniteArtifacts.value().clusters.size() == cleanArtifacts.value().clusters.size()));
}

TEST_CASE("Topology build preserves singleton and multi-member centroids",
          "[unit][topology][baseline][centroid]") {
    ConnectedComponentTopologyEngine engine;
    TopologyBuildConfig config;
    config.reciprocalOnly = true;
    config.minEdgeScore = 0.5;

    std::vector<TopologyDocumentInput> documents{
        {.documentHash = "a",
         .embedding = {2.0F, 4.0F},
         .neighbors = {{.documentHash = "b", .score = 0.9F, .reciprocal = true}}},
        {.documentHash = "b",
         .embedding = {4.0F, 8.0F},
         .neighbors = {{.documentHash = "a", .score = 0.9F, .reciprocal = true}}},
        {.documentHash = "singleton", .embedding = {3.0F, 6.0F}, .neighbors = {}},
    };

    const auto result = engine.buildArtifacts(documents, config);
    REQUIRE(result);
    const auto findCluster = [&](std::string_view documentHash) {
        return std::ranges::find_if(result.value().clusters, [&](const auto& cluster) {
            return std::ranges::find(cluster.memberDocumentHashes, documentHash) !=
                   cluster.memberDocumentHashes.end();
        });
    };
    const auto pairCluster = findCluster("a");
    const auto singletonCluster = findCluster("singleton");
    REQUIRE((pairCluster != result.value().clusters.end()));
    REQUIRE((singletonCluster != result.value().clusters.end()));
    CHECK((pairCluster->centroidEmbedding == std::vector<float>{3.0F, 6.0F}));
    CHECK((singletonCluster->centroidEmbedding == std::vector<float>{3.0F, 6.0F}));
}
