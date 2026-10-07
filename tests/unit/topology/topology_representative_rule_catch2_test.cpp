// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/topology/topology_baseline.h>
#include <yams/topology/topology_factory.h>
#include <yams/topology/topology_representatives.h>

#include <algorithm>
#include <cstdio>
#include <memory>
#include <string>
#include <utility>
#include <vector>

using namespace yams::topology;

namespace {

constexpr std::size_t kDim = 8;

std::vector<float> axis(std::size_t index, float scale = 1.0F) {
    std::vector<float> v(kDim, 0.0F);
    v[index] = scale;
    return v;
}

void addEdge(std::vector<TopologyDocumentInput>& docs, std::size_t a, std::size_t b, float score) {
    docs[a].neighbors.push_back(
        TopologyNeighbor{.documentHash = docs[b].documentHash, .score = score, .reciprocal = true});
    docs[b].neighbors.push_back(
        TopologyNeighbor{.documentHash = docs[a].documentHash, .score = score, .reciprocal = true});
}

// One connected component that is geometrically skewed:
// - c0..c3: a dense clique (edge 0.9) whose embeddings sit near axis 0;
// - h: a hub orthogonal to everything (axis 1) with many weak (0.3) edges;
// - l0..l9: leaves near axis 0, each tilted into its own direction, linked only to the hub.
// The hub carries the largest weighted degree (14 x 0.3 = 4.2 vs 3 x 0.9 + 0.3 = 3.0) but is the
// worst geometric center of the cluster.
std::vector<TopologyDocumentInput> skewedCluster() {
    std::vector<TopologyDocumentInput> docs;
    auto make = [&](std::string hash, std::vector<float> embedding) {
        TopologyDocumentInput doc;
        doc.documentHash = std::move(hash);
        doc.filePath = "/corpus/" + doc.documentHash + ".md";
        doc.embedding = std::move(embedding);
        docs.push_back(std::move(doc));
    };
    for (std::size_t i = 0; i < 4; ++i) {
        auto e = axis(0);
        e[2 + i] = 0.05F;
        make("c" + std::to_string(i), std::move(e));
    }
    make("h", axis(1));
    for (std::size_t i = 0; i < 10; ++i) {
        auto e = axis(0);
        e[2 + (i % 6)] = (i % 2 == 0 ? 0.5F : -0.5F);
        make("l" + std::to_string(i), std::move(e));
    }
    for (std::size_t a = 0; a < 4; ++a) {
        for (std::size_t b = a + 1; b < 4; ++b) {
            addEdge(docs, a, b, 0.9F);
        }
    }
    for (std::size_t i = 0; i < 4; ++i) {
        addEdge(docs, 4, i, 0.3F);
    }
    for (std::size_t i = 5; i < docs.size(); ++i) {
        addEdge(docs, 4, i, 0.3F);
    }
    return docs;
}

const char* roleName(DocumentTopologyRole role) {
    switch (role) {
        case DocumentTopologyRole::Core:
            return "core";
        case DocumentTopologyRole::Bridge:
            return "bridge";
        case DocumentTopologyRole::Medoid:
            return "medoid";
        case DocumentTopologyRole::Outlier:
            return "outlier";
    }
    return "?";
}

// Stable textual digest of everything representative selection influences.
std::string describe(const TopologyArtifactBatch& batch) {
    std::vector<std::string> clusters;
    for (const auto& cluster : batch.clusters) {
        std::string row = cluster.medoid ? cluster.medoid->documentHash : std::string{"-"};
        char score[32];
        std::snprintf(score, sizeof(score), "%.4f",
                      cluster.medoid ? cluster.medoid->representativeScore : -1.0);
        row += "@";
        row += score;
        row += "[";
        for (const auto& representative : cluster.routingRepresentatives) {
            row += representative.documentHash + ",";
        }
        row += "]n=" + std::to_string(cluster.memberCount);
        clusters.push_back(std::move(row));
    }
    std::ranges::sort(clusters);
    std::string out;
    for (const auto& row : clusters) {
        out += row + ";";
    }
    out += "|";
    std::vector<std::string> roles;
    for (const auto& membership : batch.memberships) {
        if (membership.role != DocumentTopologyRole::Core) {
            roles.push_back(membership.documentHash + ":" + roleName(membership.role));
        }
    }
    std::ranges::sort(roles);
    for (const auto& role : roles) {
        out += role + ",";
    }
    return out;
}

TopologyBuildConfig characterizationConfig() {
    TopologyBuildConfig config;
    config.reciprocalOnly = true;
    config.routingRepresentativeCount = 3;
    config.kmeansK = 2;
    return config;
}

std::string buildAndDescribe(std::string_view engineKey, const TopologyBuildConfig& config,
                             const std::vector<TopologyDocumentInput>& docs) {
    auto engine = makeEngine(engineKey);
    REQUIRE(engine);
    auto built = engine->buildArtifacts(docs, config);
    REQUIRE(built.has_value());
    return describe(built.value());
}

} // namespace

TEST_CASE("Degree representative selection is characterized for every engine",
          "[unit][topology][representative][characterization]") {
    const auto docs = skewedCluster();
    const auto config = characterizationConfig();

    CHECK(buildAndDescribe("connected", config, docs) ==
          "h@4.2000[h,l4,]n=15;|c0:bridge,c1:bridge,c2:bridge,c3:bridge,h:medoid,");
    CHECK(buildAndDescribe("louvain", config, docs) ==
          "h@4.2000[h,l4,]n=15;|c0:bridge,c1:bridge,c2:bridge,c3:bridge,h:medoid,");
    CHECK(buildAndDescribe("kmeans", config, docs) ==
          "c0@2.7000[l4,l5,]n=14;h@0.0000[h,]n=1;|c0:medoid,c1:bridge,c2:bridge,c3:bridge,h:"
          "outlier,");
}

TEST_CASE("Degree representative ties break toward the smaller document hash",
          "[unit][topology][representative][characterization]") {
    // A symmetric pair: equal weighted degree, so the smaller hash must win in every engine.
    std::vector<TopologyDocumentInput> docs(2);
    docs[0].documentHash = "zz-pair";
    docs[0].embedding = axis(0);
    docs[1].documentHash = "aa-pair";
    docs[1].embedding = axis(1);
    addEdge(docs, 0, 1, 0.8F);

    auto config = characterizationConfig();
    config.kmeansK = 1;
    for (const std::string_view engineKey : {"connected", "louvain", "kmeans"}) {
        auto engine = makeEngine(engineKey);
        auto built = engine->buildArtifacts(docs, config);
        REQUIRE(built.has_value());
        for (const auto& cluster : built.value().clusters) {
            if (cluster.memberCount == 2) {
                REQUIRE(cluster.medoid.has_value());
                CHECK(cluster.medoid->documentHash == "aa-pair");
            }
        }
    }
}
