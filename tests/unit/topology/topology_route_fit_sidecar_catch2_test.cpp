// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Route-fit sidecar persistence. Fitted route vectors live in their own KG node
// (`topology:routefit:<snapshotId>`), so the main snapshot payload is byte-identical with
// fitting on or off. The sidecar is optional on load: a missing one leaves every cluster on its
// centroid, a bad one does the same and records a reason without disabling routing.

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_codec.h>
#include <yams/topology/topology_metadata_store.h>

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <vector>

using namespace yams;
using namespace yams::metadata;
using namespace yams::topology;

namespace {

std::string routeFitNodeKey(const std::string& snapshotId) {
    return "topology:routefit:" + snapshotId;
}

std::string snapshotNodeKey(const std::string& snapshotId) {
    return "topology:snapshot:" + snapshotId;
}

struct StoreFixture {
    StoreFixture() {
        dbPath =
            std::filesystem::temp_directory_path() /
            ("topo_route_fit_sidecar_test_" +
             std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".db");
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
        ConnectionPoolConfig poolConfig;
        poolConfig.minConnections = 1;
        poolConfig.maxConnections = 2;
        pool = std::make_unique<ConnectionPool>(dbPath.string(), poolConfig);
        REQUIRE(pool->initialize().has_value());
        repository = std::make_shared<MetadataRepository>(*pool);
        auto kgResult = makeSqliteKnowledgeGraphStore(*pool, KnowledgeGraphStoreConfig{});
        REQUIRE(kgResult.has_value());
        kgStore = std::shared_ptr<KnowledgeGraphStore>(kgResult.value().release());
        for (const auto* hash : {"hash_a", "hash_b", "hash_c"}) {
            DocumentInfo doc;
            doc.sha256Hash = hash;
            doc.filePath = std::string("/repo/") + hash + ".md";
            doc.fileName = std::string(hash) + ".md";
            doc.fileSize = 100;
            doc.mimeType = "text/plain";
            REQUIRE(repository->insertDocument(doc).has_value());
        }
    }

    ~StoreFixture() {
        kgStore.reset();
        repository.reset();
        pool->shutdown();
        pool.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
    }

    void overwriteNodeProperties(const std::string& nodeKey, const std::string& properties) {
        auto res = pool->withConnection([&](Database& db) -> Result<void> {
            auto stmtRes = db.prepare("UPDATE kg_nodes SET properties = ? WHERE node_key = ?");
            if (!stmtRes) {
                return stmtRes.error();
            }
            auto stmt = std::move(stmtRes).value();
            if (auto bound = stmt.bind(1, properties); !bound) {
                return bound.error();
            }
            if (auto bound = stmt.bind(2, nodeKey); !bound) {
                return bound.error();
            }
            auto stepRes = stmt.step();
            if (!stepRes) {
                return stepRes.error();
            }
            return {};
        });
        REQUIRE(res.has_value());
    }

    bool nodeExists(const std::string& nodeKey) {
        auto node = kgStore->getNodeByKey(nodeKey);
        REQUIRE(node.has_value());
        return node.value().has_value();
    }

    std::string nodeProperties(const std::string& nodeKey) {
        auto node = kgStore->getNodeByKey(nodeKey);
        REQUIRE(node.has_value());
        REQUIRE(node.value().has_value());
        REQUIRE(node.value()->properties.has_value());
        return *node.value()->properties;
    }

    std::filesystem::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> repository;
    std::shared_ptr<KnowledgeGraphStore> kgStore;
};

// Three clusters over hash_a / hash_b / hash_c with two-dimensional centroids.
TopologyArtifactBatch makeBatch(const std::string& snapshotId, uint64_t epoch) {
    TopologyArtifactBatch batch;
    batch.snapshotId = snapshotId;
    batch.algorithm = "connected_components_v1";
    batch.embeddingSpaceIdentity = "test-space-v1";
    batch.topologyEpoch = epoch;
    batch.generatedAtUnixSeconds = 1700000000 + epoch;
    for (const auto* hash : {"hash_a", "hash_b", "hash_c"}) {
        ClusterArtifact cluster;
        cluster.clusterId = std::string("cluster-") + hash;
        cluster.memberCount = 1;
        cluster.memberDocumentHashes = {hash};
        cluster.centroidEmbedding = {1.0F, 0.0F};
        batch.clusters.push_back(cluster);
        DocumentClusterMembership membership;
        membership.documentHash = hash;
        membership.clusterId = cluster.clusterId;
        batch.memberships.push_back(membership);
    }
    return batch;
}

RouteFitProvenance sampleProvenance() {
    return RouteFitProvenance{.chosenStrength = 0.3,
                              .centroidHoldoutRecall = 0.41,
                              .fittedHoldoutRecall = 0.58,
                              .fitQueries = 2400,
                              .holdoutQueries = 600,
                              .loggedQueries = 1000,
                              .generatedQueries = 2000,
                              .fittedAtEpoch = 7};
}

// Clusters a and c fitted, b left on its centroid.
TopologyArtifactBatch makeFittedBatch(const std::string& snapshotId, uint64_t epoch) {
    auto batch = makeBatch(snapshotId, epoch);
    batch.clusters[0].fittedRouteVector = {0.75F, -0.25F};
    batch.clusters[2].fittedRouteVector = {1.5F, 0.125F};
    batch.routeFit = sampleProvenance();
    return batch;
}

} // namespace

TEST_CASE("Route-fit sidecar binary codec round trips vectors, identity and provenance",
          "[unit][topology][route-fit][sidecar][codec][catch2]") {
    const auto sidecar = collectTopologyRouteFit(makeFittedBatch("snap-1", 1));
    CHECK(sidecar.snapshotId == "snap-1");
    CHECK(sidecar.embeddingSpaceIdentity == "test-space-v1");
    CHECK(sidecar.dim == 2u);
    REQUIRE(sidecar.vectors.size() == 2u);
    CHECK(sidecar.vectors[0].clusterId == "cluster-hash_a");
    CHECK(sidecar.vectors[1].clusterId == "cluster-hash_c");

    auto binary = serializeTopologyRouteFitSidecarBinary(sidecar);
    REQUIRE(binary.has_value());
    auto decoded = deserializeTopologyRouteFitSidecarBinary(binary.value());
    REQUIRE(decoded.has_value());
    CHECK(decoded.value().snapshotId == sidecar.snapshotId);
    CHECK(decoded.value().embeddingSpaceIdentity == sidecar.embeddingSpaceIdentity);
    CHECK(decoded.value().dim == sidecar.dim);
    CHECK(decoded.value().provenance == sampleProvenance());
    CHECK(decoded.value().vectors == sidecar.vectors);

    auto compressed = serializeTopologyRouteFitSidecarCompressed(sidecar);
    REQUIRE(compressed.has_value());
    auto reopened = deserializeTopologyRouteFitSidecarCompressed(compressed.value());
    REQUIRE(reopened.has_value());
    CHECK(reopened.value().vectors == sidecar.vectors);
    CHECK(reopened.value().provenance == sampleProvenance());

    SECTION("a vector of the wrong length is refused at encode time") {
        auto broken = sidecar;
        broken.vectors[0].vector.push_back(0.0F);
        CHECK_FALSE(serializeTopologyRouteFitSidecarBinary(broken).has_value());
    }
}

TEST_CASE("Route-fit sidecar decoders reject foreign and truncated payloads without throwing",
          "[unit][topology][route-fit][sidecar][codec][catch2]") {
    const auto sidecar = collectTopologyRouteFit(makeFittedBatch("snap-1", 1));
    auto binary = serializeTopologyRouteFitSidecarBinary(sidecar);
    REQUIRE(binary.has_value());

    SECTION("every truncation fails cleanly") {
        for (std::size_t length = 0; length < binary.value().size(); ++length) {
            const std::span<const std::byte> prefix(binary.value().data(), length);
            CHECK_FALSE(deserializeTopologyRouteFitSidecarBinary(prefix).has_value());
        }
    }
    SECTION("trailing bytes are rejected") {
        auto padded = binary.value();
        padded.push_back(std::byte{0});
        CHECK_FALSE(deserializeTopologyRouteFitSidecarBinary(padded).has_value());
    }
    SECTION("a wrong magic is rejected") {
        auto foreign = binary.value();
        foreign[0] = std::byte{0x00};
        CHECK_FALSE(deserializeTopologyRouteFitSidecarBinary(foreign).has_value());
    }
    SECTION("an entry count larger than the payload is rejected") {
        // The count sits right before the first cluster id; claim far more entries than fit.
        TopologyRouteFitSidecar empty = sidecar;
        empty.vectors.clear();
        auto header = serializeTopologyRouteFitSidecarBinary(empty);
        REQUIRE(header.has_value());
        auto inflated = binary.value();
        const std::size_t countOffset = header.value().size() - sizeof(std::uint32_t);
        for (std::size_t i = 0; i < sizeof(std::uint32_t); ++i) {
            inflated[countOffset + i] = std::byte{0xFF};
        }
        CHECK_FALSE(deserializeTopologyRouteFitSidecarBinary(inflated).has_value());
    }
    SECTION("the compressed form rejects another format and non-JSON") {
        CHECK_FALSE(deserializeTopologyRouteFitSidecarCompressed("").has_value());
        CHECK_FALSE(deserializeTopologyRouteFitSidecarCompressed("not json").has_value());
        CHECK_FALSE(
            deserializeTopologyRouteFitSidecarCompressed(R"({"format":"topology_roads_zstd_v1"})")
                .has_value());
    }
}

TEST_CASE("attachTopologyRouteFit validates the sidecar against the snapshot",
          "[unit][topology][route-fit][sidecar][codec][catch2]") {
    const auto sidecar = collectTopologyRouteFit(makeFittedBatch("snap-1", 1));

    SECTION("a matching sidecar attaches every vector and the provenance") {
        auto batch = makeBatch("snap-1", 1);
        CHECK(attachTopologyRouteFit(batch, sidecar) == 2u);
        CHECK(batch.clusters[0].fittedRouteVector == std::vector<float>{0.75F, -0.25F});
        CHECK(batch.clusters[1].fittedRouteVector.empty());
        CHECK(batch.clusters[2].fittedRouteVector == std::vector<float>{1.5F, 0.125F});
        CHECK(batch.routeFit == sampleProvenance());
        CHECK(batch.routeFitUnavailableReason.empty());
    }
    SECTION("another snapshot's sidecar is rejected whole") {
        auto batch = makeBatch("snap-2", 2);
        batch.clusters[1].fittedRouteVector = {9.0F, 9.0F}; // stale, must be cleared
        CHECK(attachTopologyRouteFit(batch, sidecar) == 0u);
        CHECK(batch.routeFitUnavailableReason == "route_fit_snapshot_mismatch");
        for (const auto& cluster : batch.clusters) {
            CHECK(cluster.fittedRouteVector.empty());
        }
        CHECK(batch.routeFit == RouteFitProvenance{});
    }
    SECTION("another embedding space's sidecar is rejected whole") {
        auto batch = makeBatch("snap-1", 1);
        batch.embeddingSpaceIdentity = "other-space";
        CHECK(attachTopologyRouteFit(batch, sidecar) == 0u);
        CHECK(batch.routeFitUnavailableReason == "route_fit_space_mismatch");
    }
    SECTION("bad entries are skipped one by one") {
        auto mixed = sidecar;
        mixed.vectors.push_back({"cluster-unknown", {1.0F, 1.0F}});
        mixed.vectors.push_back({"cluster-hash_a", {2.0F, 2.0F}}); // duplicate
        mixed.vectors.push_back(
            {"cluster-hash_b", {std::numeric_limits<float>::quiet_NaN(), 0.0F}});
        auto batch = makeBatch("snap-1", 1);
        CHECK(attachTopologyRouteFit(batch, mixed) == 2u);
        CHECK(batch.clusters[0].fittedRouteVector == std::vector<float>{0.75F, -0.25F});
        CHECK(batch.clusters[1].fittedRouteVector.empty());
        CHECK(batch.routeFitUnavailableReason == "route_fit_entries_skipped:3");
        CHECK(batch.routeFit == sampleProvenance());
    }
    SECTION("a centroid of another dimension refuses its vector") {
        auto batch = makeBatch("snap-1", 1);
        batch.clusters[0].centroidEmbedding = {1.0F, 0.0F, 0.0F};
        CHECK(attachTopologyRouteFit(batch, sidecar) == 1u);
        CHECK(batch.clusters[0].fittedRouteVector.empty());
        CHECK(batch.routeFitUnavailableReason == "route_fit_entries_skipped:1");
    }
}

TEST_CASE("MetadataKgTopologyArtifactStore persists fitted route vectors and reloads them",
          "[unit][topology][route-fit][sidecar][store][catch2]") {
    StoreFixture fixture;
    const auto batch = makeFittedBatch("snap-1", 1);
    {
        MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
        REQUIRE(store.storeBatch(batch).has_value());
    }
    REQUIRE(fixture.nodeExists(routeFitNodeKey("snap-1")));

    // The main snapshot node carries no fitted vectors: an old decoder sees none.
    auto main =
        deserializeTopologyBatchCompressed(fixture.nodeProperties(snapshotNodeKey("snap-1")));
    REQUIRE(main.has_value());
    for (const auto& cluster : main.value().clusters) {
        CHECK(cluster.fittedRouteVector.empty());
    }

    MetadataKgTopologyArtifactStore reopened(fixture.repository, fixture.kgStore);
    auto loaded = reopened.loadLatestShared();
    REQUIRE(loaded.has_value());
    REQUIRE(loaded.value());
    REQUIRE(loaded.value()->clusters.size() == 3u);
    std::size_t fitted = 0;
    for (const auto& cluster : loaded.value()->clusters) {
        if (cluster.clusterId == "cluster-hash_a") {
            CHECK(cluster.fittedRouteVector == std::vector<float>{0.75F, -0.25F});
            ++fitted;
        } else if (cluster.clusterId == "cluster-hash_c") {
            CHECK(cluster.fittedRouteVector == std::vector<float>{1.5F, 0.125F});
            ++fitted;
        } else {
            CHECK(cluster.fittedRouteVector.empty());
        }
    }
    CHECK(fitted == 2u);
    CHECK(loaded.value()->routeFit == sampleProvenance());
    CHECK(loaded.value()->routeFitUnavailableReason.empty());

    SECTION("re-storing the snapshot id without fitted vectors sheds the sidecar") {
        REQUIRE(reopened.storeBatch(makeBatch("snap-1", 1)).has_value());
        CHECK_FALSE(fixture.nodeExists(routeFitNodeKey("snap-1")));
    }
    SECTION("a newer snapshot without a fit loads with no fitted vectors") {
        REQUIRE(reopened.storeBatch(makeBatch("snap-2", 2)).has_value());
        auto current = reopened.loadLatest();
        REQUIRE(current.has_value());
        REQUIRE(current.value().has_value());
        for (const auto& cluster : current.value()->clusters) {
            CHECK(cluster.fittedRouteVector.empty());
        }
        auto historical = reopened.loadLatest("snap-1");
        REQUIRE(historical.has_value());
        REQUIRE(historical.value().has_value());
        CHECK(historical.value()->routeFit == sampleProvenance());
    }
}

TEST_CASE("Main snapshot payload is byte-identical with and without fitted route vectors",
          "[unit][topology][route-fit][sidecar][store][catch2]") {
    StoreFixture plain;
    StoreFixture fitted;
    {
        MetadataKgTopologyArtifactStore store(plain.repository, plain.kgStore);
        REQUIRE(store.storeBatch(makeBatch("snap-1", 1)).has_value());
    }
    {
        MetadataKgTopologyArtifactStore store(fitted.repository, fitted.kgStore);
        REQUIRE(store.storeBatch(makeFittedBatch("snap-1", 1)).has_value());
    }
    CHECK(plain.nodeProperties(snapshotNodeKey("snap-1")) ==
          fitted.nodeProperties(snapshotNodeKey("snap-1")));
    CHECK_FALSE(plain.nodeExists(routeFitNodeKey("snap-1")));
}

TEST_CASE("MetadataKgTopologyArtifactStore drops a bad route-fit sidecar and keeps routing",
          "[unit][topology][route-fit][sidecar][store][catch2]") {
    StoreFixture fixture;
    {
        MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
        REQUIRE(store.storeBatch(makeFittedBatch("snap-1", 1)).has_value());
    }

    SECTION("undecodable payload") {
        fixture.overwriteNodeProperties(routeFitNodeKey("snap-1"), "{not json");
        MetadataKgTopologyArtifactStore reopened(fixture.repository, fixture.kgStore);
        auto loaded = reopened.loadLatest();
        REQUIRE(loaded.has_value());
        REQUIRE(loaded.value().has_value());
        CHECK(loaded.value()->clusters.size() == 3u);
        for (const auto& cluster : loaded.value()->clusters) {
            CHECK(cluster.fittedRouteVector.empty());
        }
        CHECK(loaded.value()->routeFitUnavailableReason.starts_with(
            "route_fit_sidecar_undecodable:"));
    }
    SECTION("a sidecar copied from another snapshot") {
        auto foreign = collectTopologyRouteFit(makeFittedBatch("snap-other", 9));
        auto payload = serializeTopologyRouteFitSidecarCompressed(foreign);
        REQUIRE(payload.has_value());
        fixture.overwriteNodeProperties(routeFitNodeKey("snap-1"), payload.value());
        MetadataKgTopologyArtifactStore reopened(fixture.repository, fixture.kgStore);
        auto loaded = reopened.loadLatest();
        REQUIRE(loaded.has_value());
        REQUIRE(loaded.value().has_value());
        for (const auto& cluster : loaded.value()->clusters) {
            CHECK(cluster.fittedRouteVector.empty());
        }
        CHECK(loaded.value()->routeFitUnavailableReason == "route_fit_snapshot_mismatch");
    }
}

TEST_CASE("MetadataKgTopologyArtifactStore retention removes route-fit sidecars with snapshots",
          "[unit][topology][route-fit][sidecar][store][catch2]") {
    StoreFixture fixture;
    MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
    const std::size_t total = MetadataKgTopologyArtifactStore::kRetainedSnapshots + 2;
    for (std::size_t i = 1; i <= total; ++i) {
        REQUIRE(store.storeBatch(makeFittedBatch("snap-" + std::to_string(i), i)).has_value());
    }
    CHECK_FALSE(fixture.nodeExists(routeFitNodeKey("snap-1")));
    CHECK(fixture.nodeExists(routeFitNodeKey("snap-" + std::to_string(total))));
}
