// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/topology/topology_baseline.h>
#include <yams/topology/topology_codec.h>
#include <yams/topology/topology_factory.h>
#include <yams/topology/topology_representatives.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <limits>
#include <memory>
#include <numbers>
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

namespace {

double chordal(const std::vector<float>& a, const std::vector<float>& b) {
    double dot = 0.0;
    double na = 0.0;
    double nb = 0.0;
    for (std::size_t i = 0; i < a.size(); ++i) {
        dot += static_cast<double>(a[i]) * b[i];
        na += static_cast<double>(a[i]) * a[i];
        nb += static_cast<double>(b[i]) * b[i];
    }
    const double cosine = std::clamp(dot / (std::sqrt(na) * std::sqrt(nb)), -1.0, 1.0);
    return std::sqrt(std::max(0.0, 2.0 * (1.0 - cosine)));
}

// Brute-force argmin of the medoid objective over the named members; ties -> smaller hash.
std::string bruteForceMedoid(const std::vector<TopologyDocumentInput>& docs,
                             const std::vector<std::string>& memberHashes) {
    std::vector<const TopologyDocumentInput*> members;
    for (const auto& hash : memberHashes) {
        for (const auto& doc : docs) {
            if (doc.documentHash == hash && !doc.embedding.empty()) {
                members.push_back(&doc);
            }
        }
    }
    std::ranges::sort(members, {}, &TopologyDocumentInput::documentHash);
    std::string best;
    double bestObjective = std::numeric_limits<double>::max();
    for (const auto* candidate : members) {
        double objective = 0.0;
        for (const auto* other : members) {
            if (other != candidate) {
                objective += chordal(candidate->embedding, other->embedding);
            }
        }
        if (objective < bestObjective - 1e-9) {
            bestObjective = objective;
            best = candidate->documentHash;
        }
    }
    return best;
}

std::vector<TopologyDocumentInput> scatteredClique() {
    // Eight docs, fully connected with distinct edge weights, embeddings spread unevenly.
    const std::vector<std::vector<float>> embeddings = {
        {0.9F, 0.1F, 0.0F, 0.2F, 0.1F},  {0.7F, 0.5F, 0.1F, 0.0F, 0.2F},
        {0.2F, 0.9F, 0.3F, 0.1F, 0.0F},  {0.8F, 0.2F, 0.4F, 0.1F, 0.1F},
        {0.1F, 0.1F, 0.9F, 0.4F, 0.2F},  {0.6F, 0.3F, 0.3F, 0.3F, 0.3F},
        {-0.2F, 0.4F, 0.1F, 0.9F, 0.1F}, {0.5F, -0.1F, 0.2F, 0.2F, 0.8F},
    };
    std::vector<TopologyDocumentInput> docs;
    for (std::size_t i = 0; i < embeddings.size(); ++i) {
        TopologyDocumentInput doc;
        doc.documentHash = "doc" + std::to_string(7 - i); // hash order != insertion order
        doc.embedding = embeddings[i];
        docs.push_back(std::move(doc));
    }
    for (std::size_t a = 0; a < docs.size(); ++a) {
        for (std::size_t b = a + 1; b < docs.size(); ++b) {
            addEdge(docs, a, b, 0.4F + 0.01F * static_cast<float>(a + 3 * b));
        }
    }
    return docs;
}

TopologyArtifactBatch build(std::string_view engineKey, const TopologyBuildConfig& config,
                            const std::vector<TopologyDocumentInput>& docs) {
    auto engine = makeEngine(engineKey);
    REQUIRE(engine);
    auto built = engine->buildArtifacts(docs, config);
    REQUIRE(built.has_value());
    return std::move(built).value();
}

const ClusterArtifact& largestCluster(const TopologyArtifactBatch& batch) {
    REQUIRE_FALSE(batch.clusters.empty());
    return *std::ranges::max_element(batch.clusters, {}, &ClusterArtifact::memberCount);
}

} // namespace

TEST_CASE("Representative rule names round-trip through their config spelling",
          "[unit][topology][representative]") {
    for (const auto rule : {RepresentativeRule::Degree, RepresentativeRule::Medoid}) {
        CHECK(parseRepresentativeRule(representativeRuleName(rule)) == rule);
    }
    CHECK(parseRepresentativeRule("MEDOID") == RepresentativeRule::Medoid);
    CHECK_FALSE(parseRepresentativeRule("centroid").has_value());
    CHECK(TopologyBuildConfig{}.representativeRule == RepresentativeRule::Degree);
}

TEST_CASE("Medoid representative minimises the summed chordal distance",
          "[unit][topology][representative][medoid]") {
    const auto docs = scatteredClique();
    std::vector<std::string> allHashes;
    std::vector<std::size_t> allIndices;
    for (std::size_t i = 0; i < docs.size(); ++i) {
        allHashes.push_back(docs[i].documentHash);
        allIndices.push_back(i);
    }
    const auto expected = bruteForceMedoid(docs, allHashes);

    const auto selection = selectMedoidRepresentative(docs, allIndices);
    REQUIRE(selection.has_value());
    CHECK(docs[selection->document].documentHash == expected);
    CHECK(selection->evaluatedMembers == docs.size());

    auto config = characterizationConfig();
    config.representativeRule = RepresentativeRule::Medoid;
    const auto batch = build("connected", config, docs);
    REQUIRE(batch.clusters.size() == 1U);
    REQUIRE(batch.clusters.front().medoid.has_value());
    CHECK(batch.clusters.front().medoid->documentHash == expected);
    CHECK(batch.representativeRule == RepresentativeRule::Medoid);
    for (const auto& membership : batch.memberships) {
        CHECK((membership.role == DocumentTopologyRole::Medoid) ==
              (membership.documentHash == expected));
    }
}

TEST_CASE("Degree and medoid disagree on a skewed cluster", "[unit][topology][representative]") {
    const auto docs = skewedCluster();
    auto degreeConfig = characterizationConfig();
    auto medoidConfig = degreeConfig;
    medoidConfig.representativeRule = RepresentativeRule::Medoid;

    const auto degreeBatch = build("connected", degreeConfig, docs);
    const auto medoidBatch = build("connected", medoidConfig, docs);
    const auto& degreeCluster = largestCluster(degreeBatch);
    const auto& medoidCluster = largestCluster(medoidBatch);
    REQUIRE(degreeCluster.medoid.has_value());
    REQUIRE(medoidCluster.medoid.has_value());
    CHECK(degreeCluster.medoid->documentHash == "h");
    CHECK(medoidCluster.medoid->documentHash != "h");
    CHECK(medoidCluster.medoid->documentHash ==
          bruteForceMedoid(docs, medoidCluster.memberDocumentHashes));
    // Cluster identity and membership are unaffected by the rule.
    CHECK(degreeCluster.clusterId == medoidCluster.clusterId);
    CHECK(degreeCluster.memberDocumentHashes == medoidCluster.memberDocumentHashes);
}

TEST_CASE("Medoid rule applies in every engine", "[unit][topology][representative][medoid]") {
    const auto docs = skewedCluster();
    auto config = characterizationConfig();
    config.representativeRule = RepresentativeRule::Medoid;
    for (const std::string_view engineKey : {"connected", "louvain", "kmeans"}) {
        const auto batch = build(engineKey, config, docs);
        CHECK(batch.representativeRule == RepresentativeRule::Medoid);
        for (const auto& cluster : batch.clusters) {
            if (cluster.memberCount < 2) {
                continue;
            }
            REQUIRE(cluster.medoid.has_value());
            CHECK(cluster.medoid->documentHash ==
                  bruteForceMedoid(docs, cluster.memberDocumentHashes));
        }
    }
}

TEST_CASE("Explicit Degree rule reproduces the default build exactly",
          "[unit][topology][representative][characterization]") {
    const auto docs = skewedCluster();
    const auto defaults = characterizationConfig();
    auto explicitDegree = defaults;
    explicitDegree.representativeRule = RepresentativeRule::Degree;
    for (const std::string_view engineKey : {"connected", "louvain", "kmeans"}) {
        auto lhs = build(engineKey, defaults, docs);
        auto rhs = build(engineKey, explicitDegree, docs);
        lhs.snapshotId = rhs.snapshotId = "fixed";
        lhs.generatedAtUnixSeconds = rhs.generatedAtUnixSeconds = 0;
        const auto lhsBytes = serializeTopologyBatchBinary(lhs);
        const auto rhsBytes = serializeTopologyBatchBinary(rhs);
        REQUIRE(lhsBytes.has_value());
        REQUIRE(rhsBytes.has_value());
        CHECK(lhsBytes.value() == rhsBytes.value());
        CHECK(lhs.representativeRule == RepresentativeRule::Degree);
    }
}

TEST_CASE("Representative rule survives the binary snapshot codec",
          "[unit][topology][representative][codec]") {
    auto config = characterizationConfig();
    config.representativeRule = RepresentativeRule::Medoid;
    const auto batch = build("connected", config, skewedCluster());
    const auto bytes = serializeTopologyBatchBinary(batch);
    REQUIRE(bytes.has_value());
    const auto decoded = deserializeTopologyBatchBinary(bytes.value());
    REQUIRE(decoded.has_value());
    CHECK(decoded.value().representativeRule == RepresentativeRule::Medoid);
    CHECK(largestCluster(decoded.value()).medoid->documentHash ==
          largestCluster(batch).medoid->documentHash);
}

TEST_CASE("Medoid ties break toward the smaller document hash",
          "[unit][topology][representative][medoid]") {
    // Equilateral triangle on the unit circle: every member has the same objective.
    std::vector<TopologyDocumentInput> docs(3);
    const std::vector<std::string> hashes = {"m-bravo", "m-alpha", "m-charlie"};
    for (std::size_t i = 0; i < 3; ++i) {
        const double angle = 2.0 * std::numbers::pi * static_cast<double>(i) / 3.0;
        docs[i].documentHash = hashes[i];
        docs[i].embedding = {static_cast<float>(std::cos(angle)),
                             static_cast<float>(std::sin(angle))};
    }
    addEdge(docs, 0, 1, 0.5F);
    addEdge(docs, 1, 2, 0.5F);
    addEdge(docs, 0, 2, 0.5F);
    const std::vector<std::size_t> members = {0, 1, 2};
    for (int repeat = 0; repeat < 3; ++repeat) {
        const auto selection = selectMedoidRepresentative(docs, members);
        REQUIRE(selection.has_value());
        CHECK(docs[selection->document].documentHash == "m-alpha");
    }
    auto config = characterizationConfig();
    config.representativeRule = RepresentativeRule::Medoid;
    const auto batch = build("connected", config, docs);
    CHECK(largestCluster(batch).medoid->documentHash == "m-alpha");
}

TEST_CASE("Medoid falls back to the degree choice without usable geometry",
          "[unit][topology][representative][medoid]") {
    std::vector<TopologyDocumentInput> docs(3);
    docs[0].documentHash = "g-a";
    docs[1].documentHash = "g-b";
    docs[2].documentHash = "g-c";
    docs[1].embedding = {1.0F, 0.0F}; // only one usable embedding
    const std::vector<std::size_t> members = {0, 1, 2};
    CHECK_FALSE(selectMedoidRepresentative(docs, members).has_value());
    CHECK(applyRepresentativeRule(RepresentativeRule::Medoid, docs, members, 2U) == 2U);
    CHECK(applyRepresentativeRule(RepresentativeRule::Degree, docs, members, 2U) == 2U);
}

TEST_CASE("Medoid search is capped and deterministic on large clusters",
          "[unit][topology][representative][medoid]") {
    std::vector<TopologyDocumentInput> docs;
    std::vector<std::size_t> members;
    for (std::size_t i = 0; i < kMedoidRepresentativeMaxMembers + 44; ++i) {
        TopologyDocumentInput doc;
        doc.documentHash = "big" + std::to_string(1000 + i);
        const double angle = 0.001 * static_cast<double>((i * 37) % 997);
        doc.embedding = {static_cast<float>(std::cos(angle)), static_cast<float>(std::sin(angle)),
                         0.1F};
        docs.push_back(std::move(doc));
        members.push_back(i);
    }
    const auto first = selectMedoidRepresentative(docs, members);
    REQUIRE(first.has_value());
    CHECK(first->evaluatedMembers == kMedoidRepresentativeMaxMembers);
    std::vector<std::size_t> reversed(members.rbegin(), members.rend());
    const auto second = selectMedoidRepresentative(docs, reversed);
    REQUIRE(second.has_value());
    CHECK(second->document == first->document);
}

TEST_CASE("Medoid rule seeds farthest-first routing representatives from the medoid",
          "[unit][topology][representative][medoid]") {
    const auto docs = scatteredClique();
    auto config = characterizationConfig();
    config.representativeRule = RepresentativeRule::Medoid;
    config.routingRepresentativeCount = 2;
    const auto batch = build("connected", config, docs);
    const auto& cluster = largestCluster(batch);
    REQUIRE(cluster.medoid.has_value());
    REQUIRE(cluster.routingRepresentatives.size() == 1U);

    const auto& medoidDoc = *std::ranges::find(docs, cluster.medoid->documentHash,
                                               &TopologyDocumentInput::documentHash);
    std::vector<const TopologyDocumentInput*> sorted;
    for (const auto& doc : docs) {
        sorted.push_back(&doc);
    }
    std::ranges::sort(sorted, {}, &TopologyDocumentInput::documentHash);
    std::string farthest;
    double farthestDistance = -1.0;
    for (const auto* doc : sorted) {
        const double chord = chordal(doc->embedding, medoidDoc.embedding);
        const double distance = 0.5 * chord * chord; // 1 - cos
        if (distance > farthestDistance) {
            farthestDistance = distance;
            farthest = doc->documentHash;
        }
    }
    CHECK(cluster.routingRepresentatives.front().documentHash == farthest);
}
