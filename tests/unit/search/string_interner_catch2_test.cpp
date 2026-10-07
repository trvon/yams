// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/search/string_interner.h>
#include <yams/search/topology_routing_session.h>
#include <yams/topology/topology_artifacts.h>
#include <yams/vector/binary_quantization.h>

#include <string>
#include <string_view>
#include <vector>

using namespace yams;
using namespace yams::search;
using namespace yams::topology;

TEST_CASE("StringInterner basic interning and deduplication", "[unit][search][interner]") {
    StringInterner interner;
    CHECK(interner.empty());
    CHECK(interner.size() == 0);
    CHECK(interner.payloadBytes() == 0);

    // Empty string
    auto emptyView = interner.intern("");
    CHECK(emptyView.empty());
    CHECK(interner.empty());

    // First insertion
    std::string str1 = "cluster-01934abc";
    auto view1 = interner.intern(str1);
    CHECK(view1 == "cluster-01934abc");
    CHECK(interner.size() == 1);
    CHECK(interner.payloadBytes() == str1.size());

    // Re-interning identical string must return pointer to identical storage
    std::string str2 = "cluster-01934abc"; // distinct heap string
    auto view2 = interner.intern(str2);
    CHECK(view2 == "cluster-01934abc");
    CHECK(view1.data() == view2.data()); // Zero-allocation hit
    CHECK(interner.size() == 1);

    // Second distinct string
    auto view3 =
        interner.intern("doc_hash_64_characters_long_0123456789abcdef0123456789abcdef012345");
    CHECK(interner.size() == 2);
    CHECK(view3 != view1);

    // Move construction preserves interned strings
    StringInterner moved(std::move(interner));
    CHECK(moved.size() == 2);
    auto view1AfterMove = moved.intern("cluster-01934abc");
    CHECK(view1AfterMove.data() == view1.data());

    moved.clear();
    CHECK(moved.empty());
    CHECK(moved.size() == 0);
}

TEST_CASE("TopologyRoutingSnapshotCache zero-copy flyweight index maps",
          "[unit][search][topology][cache][flyweight]") {
    ConnectedComponentTopologyEngine engine;
    TopologyBuildConfig config;
    config.reciprocalOnly = true;
    config.embeddingSpaceIdentity = "test-space";
    std::vector<TopologyDocumentInput> docs{
        {.documentHash = "hash_doc_1",
         .filePath = "/path/1.txt",
         .embedding = {1.0F, 0.0F},
         .neighbors = {{.documentHash = "hash_doc_2", .score = 0.9F, .reciprocal = true}}},
        {.documentHash = "hash_doc_2",
         .filePath = "/path/2.txt",
         .embedding = {0.9F, 0.1F},
         .neighbors = {{.documentHash = "hash_doc_1", .score = 0.9F, .reciprocal = true}}},
        {.documentHash = "hash_doc_3",
         .filePath = "/path/3.txt",
         .embedding = {0.0F, 1.0F},
         .neighbors = {}},
    };
    auto batchRes = engine.buildArtifacts(docs, config);
    REQUIRE(batchRes.has_value());
    auto batch = std::move(batchRes.value());
    batch.topologyEpoch = 42;

    TopologyRoutingSnapshotCache cache([batch]() mutable {
        return Result<std::optional<TopologyArtifactBatch>>{
            std::optional<TopologyArtifactBatch>{std::move(batch)}};
    });

    auto lookup = cache.get(42, false);
    REQUIRE(lookup.has_value());
    auto snapshot = lookup.value().snapshot;
    REQUIRE(snapshot);

    // 1. Verify clustersById size and zero-copy string_view pointing into artifacts
    REQUIRE(snapshot->clustersById.size() == snapshot->artifacts->clusters.size());
    for (std::size_t i = 0; i < snapshot->artifacts->clusters.size(); ++i) {
        const auto& cId = snapshot->artifacts->clusters[i].clusterId;
        auto it = snapshot->clustersById.find(cId);
        REQUIRE(it != snapshot->clustersById.end());
        CHECK(it->second == i);
        // Key in map points directly to the string storage in snapshot->artifacts
        CHECK(it->first.data() == cId.data());
    }

    // 2. Verify membershipsByDocumentHash size and zero-copy string_view pointing into artifacts
    REQUIRE(snapshot->membershipsByDocumentHash.size() == snapshot->artifacts->memberships.size());
    for (std::size_t i = 0; i < snapshot->artifacts->memberships.size(); ++i) {
        const auto& dHash = snapshot->artifacts->memberships[i].documentHash;
        auto it = snapshot->membershipsByDocumentHash.find(dHash);
        REQUIRE(it != snapshot->membershipsByDocumentHash.end());
        CHECK(it->second == i);
        CHECK(it->first.data() == dHash.data());
    }

    // 3. Upgrade snapshot (triggering copy-construction for ANN index upgrade)
    auto upgradeLookup = cache.get(42, true);
    REQUIRE(upgradeLookup.has_value());
    auto upgraded = upgradeLookup.value().snapshot;
    REQUIRE(upgraded);
    CHECK(upgraded->denseAnnBuildAttempted);

    // Pointers must still point to identical underlying artifact storage
    for (std::size_t i = 0; i < snapshot->artifacts->clusters.size(); ++i) {
        const auto& cId = snapshot->artifacts->clusters[i].clusterId;
        auto it = upgraded->clustersById.find(cId);
        REQUIRE(it != upgraded->clustersById.end());
        CHECK(it->first.data() == cId.data());
    }

    for (std::size_t i = 0; i < snapshot->artifacts->memberships.size(); ++i) {
        const auto& dHash = snapshot->artifacts->memberships[i].documentHash;
        auto it = upgraded->membershipsByDocumentHash.find(dHash);
        REQUIRE(it != upgraded->membershipsByDocumentHash.end());
        CHECK(it->first.data() == dHash.data());
    }

    // 4. A different BQ prefix rebuilds only the route index over the shared artifacts.
    REQUIRE(upgraded->sparseRouteIndex.centroidBqIndex);
    CHECK(upgraded->sparseRouteIndex.centroidBqIndex->dimension() == 2U);
    auto prefixLookup = cache.get(42, true, 1);
    REQUIRE(prefixLookup.has_value());
    auto prefixed = prefixLookup.value().snapshot;
    REQUIRE(prefixed);
    CHECK(prefixed->bqPrefixDimension == 1U);
    CHECK(prefixed->denseAnnBuildAttempted);
    REQUIRE(prefixed->sparseRouteIndex.centroidBqIndex);
    CHECK(prefixed->sparseRouteIndex.centroidBqIndex->dimension() == 1U);
    CHECK(prefixed->artifacts.get() == upgraded->artifacts.get());
}

TEST_CASE("topologyCommunitySupport scores candidates that share a snapshot cluster",
          "[unit][search][topology][community]") {
    auto batch = std::make_shared<TopologyArtifactBatch>();
    const auto member = [](std::string hash, std::string cluster) {
        yams::topology::DocumentClusterMembership m;
        m.documentHash = std::move(hash);
        m.clusterId = std::move(cluster);
        return m;
    };
    batch->memberships = {member("a", "c1"), member("b", "c1"), member("c", "c1"),
                          member("d", "c2"), member("e", "c3")};
    TopologyRoutingSnapshot snapshot;
    snapshot.artifacts = batch;
    for (std::size_t i = 0; i < batch->memberships.size(); ++i) {
        snapshot.membershipsByDocumentHash.emplace(batch->memberships[i].documentHash, i);
    }

    // a, b, c share c1; d and e are alone in their clusters; x is not in the snapshot.
    const std::vector<std::string> candidates{"a", "d", "b", "x", "c", "e"};
    yams::search::TopologyCommunityStats stats;
    const auto support = yams::search::topologyCommunitySupport(snapshot, candidates, 8.0F, &stats);
    REQUIRE(support.size() == candidates.size());
    CHECK(support[0] == Catch::Approx(2.0F / 7.0F));
    CHECK(support[2] == Catch::Approx(2.0F / 7.0F));
    CHECK(support[4] == Catch::Approx(2.0F / 7.0F));
    CHECK(support[1] == 0.0F);
    CHECK(support[3] == 0.0F);
    CHECK(support[5] == 0.0F);
    CHECK(stats.communities == 1U);
    CHECK(stats.supportedDocs == 3U);
    CHECK(stats.largestCommunity == 3U);

    SECTION("Without a reference size the candidate count normalizes") {
        const auto relative = yams::search::topologyCommunitySupport(snapshot, candidates, 0.0F);
        CHECK(relative[0] == Catch::Approx(2.0F / 5.0F));
    }
}

TEST_CASE("TopologyRoutingSnapshotCache keys the BQ index on its rotation",
          "[unit][search][topology][cache][bq]") {
    TopologyArtifactBatch batch;
    batch.topologyEpoch = 7;
    for (std::size_t i = 0; i < 4; ++i) {
        ClusterArtifact cluster;
        cluster.clusterId = "cluster-" + std::to_string(i);
        cluster.memberCount = 1;
        cluster.memberDocumentHashes = {"doc-" + std::to_string(i)};
        cluster.centroidEmbedding = {1.0F, static_cast<float>(i), 0.5F};
        batch.clusters.push_back(std::move(cluster));
        DocumentClusterMembership membership;
        membership.documentHash = "doc-" + std::to_string(i);
        membership.clusterId = "cluster-" + std::to_string(i);
        batch.memberships.push_back(std::move(membership));
    }
    TopologyRoutingSnapshotCache cache(
        [batch]() { return Result<std::optional<TopologyArtifactBatch>>{std::optional{batch}}; });

    auto plain = cache.get(7, false);
    REQUIRE(plain.has_value());
    REQUIRE(plain.value().snapshot->sparseRouteIndex.centroidBqIndex);
    CHECK(plain.value().snapshot->bqRotation == yams::vector::BinaryRotation::None);
    CHECK(plain.value().snapshot->sparseRouteIndex.centroidBqIndex->rotation() ==
          yams::vector::BinaryRotation::None);

    auto rotated = cache.get(7, false, 0, yams::vector::BinaryRotation::Fwht);
    REQUIRE(rotated.has_value());
    const auto& snapshot = *rotated.value().snapshot;
    CHECK(snapshot.bqRotation == yams::vector::BinaryRotation::Fwht);
    CHECK(snapshot.bqRotationSeed == yams::vector::kDefaultBinaryRotationSeed);
    REQUIRE(snapshot.sparseRouteIndex.centroidBqIndex);
    CHECK(snapshot.sparseRouteIndex.centroidBqIndex->rotation() ==
          yams::vector::BinaryRotation::Fwht);
    CHECK(snapshot.sparseRouteIndex.centroidBqIndex->rotationSeed() == snapshot.bqRotationSeed);
    CHECK(snapshot.artifacts.get() == plain.value().snapshot->artifacts.get());

    auto repeated = cache.get(7, false, 0, yams::vector::BinaryRotation::Fwht);
    REQUIRE(repeated.has_value());
    CHECK(repeated.value().snapshot.get() == rotated.value().snapshot.get());

    auto back = cache.get(7, false);
    REQUIRE(back.has_value());
    CHECK(back.value().snapshot->sparseRouteIndex.centroidBqIndex->rotation() ==
          yams::vector::BinaryRotation::None);
}

TEST_CASE("Topology routing options carry the BQ rotation and fingerprint it only when set",
          "[unit][search][topology][bq]") {
    SearchEngineConfig config;
    CHECK(config.topologyRoutingBqRotation == SearchEngineConfig::TopologyBqRotation::None);
    const auto plainOptions = makeTopologyRoutingOptions(
        config, SearchEngineConfig::TopologyRoutingMode::HybridAssist, false);
    CHECK(plainOptions.bqRotation == SearchEngineConfig::TopologyBqRotation::None);

    auto rotatedConfig = config;
    rotatedConfig.topologyRoutingBqRotation = SearchEngineConfig::TopologyBqRotation::Fwht;
    const auto rotatedOptions = makeTopologyRoutingOptions(
        rotatedConfig, SearchEngineConfig::TopologyRoutingMode::HybridAssist, false);
    CHECK(rotatedOptions.bqRotation == SearchEngineConfig::TopologyBqRotation::Fwht);
    CHECK(topologyRoutingPolicyFingerprint("repr", plainOptions) !=
          topologyRoutingPolicyFingerprint("repr", rotatedOptions));

    SearchEngineConfig copy;
    copy.applyTopologyPolicyFrom(rotatedConfig);
    CHECK(copy.topologyRoutingBqRotation == SearchEngineConfig::TopologyBqRotation::Fwht);
    CHECK(std::string_view{SearchEngineConfig::topologyBqRotationToString(
              SearchEngineConfig::TopologyBqRotation::Fwht)} == "fwht");
}
