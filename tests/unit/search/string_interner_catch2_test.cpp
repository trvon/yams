// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/search/string_interner.h>
#include <yams/search/topology_routing_session.h>
#include <yams/topology/topology_artifacts.h>

#include <string>
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
