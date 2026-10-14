// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Road sidecar persistence. Roads live in their own KG node (`topology:roads:<snapshotId>`,
// type `topology_road_snapshot`) with their own magic and version, so the main snapshot payload
// is byte-identical with roads on or off and an older decoder never sees road bytes. The sidecar
// is optional on load: a missing one yields empty roads, a bad one drops roads and records a
// reason without disabling routing.

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/topology_routing_session.h>
#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_codec.h>
#include <yams/topology/topology_metadata_store.h>

#include <nlohmann/json.hpp>

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

constexpr std::string_view kRoadNodeType = "topology_road_snapshot";

std::string roadNodeKey(const std::string& snapshotId) {
    return "topology:roads:" + snapshotId;
}

std::string snapshotNodeKey(const std::string& snapshotId) {
    return "topology:snapshot:" + snapshotId;
}

struct StoreFixture {
    StoreFixture() {
        dbPath =
            std::filesystem::temp_directory_path() /
            ("topo_road_sidecar_test_" +
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

// Three singleton-ish clusters over hash_a / hash_b / hash_c, valid for routing.
TopologyArtifactBatch makeBatch(const std::string& snapshotId, uint64_t epoch) {
    TopologyArtifactBatch batch;
    batch.snapshotId = snapshotId;
    batch.algorithm = "connected_components_v1";
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

std::vector<ClusterRoad> sampleRoads() {
    return {
        ClusterRoad{"cluster-hash_a", "cluster-hash_b", 0.75},
        ClusterRoad{"cluster-hash_a", "cluster-hash_c", 0.25},
        ClusterRoad{"cluster-hash_b", "cluster-hash_a", 0.75},
        ClusterRoad{"cluster-hash_c", "cluster-hash_a", 0.25},
    };
}

TopologyArtifactBatch makeBatchWithRoads(const std::string& snapshotId, uint64_t epoch) {
    auto batch = makeBatch(snapshotId, epoch);
    batch.roads = sampleRoads();
    batch.roadMaxPerCluster = 2;
    return batch;
}

TopologyRoadSidecar sidecarFor(const TopologyArtifactBatch& batch) {
    TopologyRoadSidecar sidecar;
    sidecar.snapshotId = batch.snapshotId;
    sidecar.maxPerCluster = batch.roadMaxPerCluster;
    sidecar.roads = batch.roads;
    return sidecar;
}

std::string encodedSidecar(const TopologyRoadSidecar& sidecar) {
    auto encoded = serializeTopologyRoadSidecarCompressed(sidecar);
    REQUIRE(encoded.has_value());
    return encoded.value();
}

} // namespace

// ---------------------------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------------------------

TEST_CASE("Road sidecar binary codec round trips roads, snapshot id and top-K",
          "[unit][topology][roads][sidecar][codec][catch2]") {
    auto batch = makeBatchWithRoads("snap-rt", 1);
    const auto sidecar = sidecarFor(batch);

    auto bytes = serializeTopologyRoadSidecarBinary(sidecar);
    REQUIRE(bytes.has_value());
    // Own magic: the first four bytes are not the main snapshot magic ('Y','A','M','T').
    REQUIRE(bytes.value().size() >= 8);
    CHECK(static_cast<char>(bytes.value()[0]) == 'Y');
    CHECK(static_cast<char>(bytes.value()[1]) == 'A');
    CHECK(static_cast<char>(bytes.value()[2]) == 'M');
    CHECK(static_cast<char>(bytes.value()[3]) == 'R');

    auto decoded = deserializeTopologyRoadSidecarBinary(bytes.value());
    REQUIRE(decoded.has_value());
    CHECK(decoded.value().snapshotId == "snap-rt");
    CHECK(decoded.value().maxPerCluster == 2);
    CHECK(decoded.value().roads == sidecar.roads);

    SECTION("compressed envelope round trips too") {
        const auto payload = encodedSidecar(sidecar);
        auto fromEnvelope = deserializeTopologyRoadSidecarCompressed(payload);
        REQUIRE(fromEnvelope.has_value());
        CHECK(fromEnvelope.value().snapshotId == "snap-rt");
        CHECK(fromEnvelope.value().maxPerCluster == 2);
        CHECK(fromEnvelope.value().roads == sidecar.roads);
    }

    SECTION("an empty sidecar round trips") {
        TopologyRoadSidecar empty;
        empty.snapshotId = "snap-empty";
        auto emptyBytes = serializeTopologyRoadSidecarBinary(empty);
        REQUIRE(emptyBytes.has_value());
        auto back = deserializeTopologyRoadSidecarBinary(emptyBytes.value());
        REQUIRE(back.has_value());
        CHECK(back.value().roads.empty());
        CHECK(back.value().snapshotId == "snap-empty");
    }
}

TEST_CASE("Road sidecar decoders reject foreign and truncated payloads without throwing",
          "[unit][topology][roads][sidecar][codec][catch2]") {
    auto batch = makeBatchWithRoads("snap-bad", 1);
    const auto sidecar = sidecarFor(batch);
    auto bytes = serializeTopologyRoadSidecarBinary(sidecar);
    REQUIRE(bytes.has_value());

    SECTION("bad magic") {
        auto corrupt = bytes.value();
        corrupt[3] = std::byte{'X'};
        auto decoded = deserializeTopologyRoadSidecarBinary(corrupt);
        REQUIRE_FALSE(decoded.has_value());
        CHECK(decoded.error().code == ErrorCode::InvalidData);
    }

    SECTION("truncated") {
        auto truncated = bytes.value();
        truncated.resize(truncated.size() / 2);
        auto decoded = deserializeTopologyRoadSidecarBinary(truncated);
        REQUIRE_FALSE(decoded.has_value());
    }

    SECTION("trailing bytes") {
        auto padded = bytes.value();
        padded.push_back(std::byte{0});
        auto decoded = deserializeTopologyRoadSidecarBinary(padded);
        REQUIRE_FALSE(decoded.has_value());
    }

    SECTION("the main snapshot decoder does not accept a sidecar payload") {
        // An old peer that somehow reads the sidecar bytes as a snapshot gets an error, not a
        // batch built from road bytes.
        auto asBatch = deserializeTopologyBatchBinary(bytes.value());
        REQUIRE_FALSE(asBatch.has_value());
        auto asBatchEnvelope = deserializeTopologyBatchCompressed(encodedSidecar(sidecar));
        REQUIRE_FALSE(asBatchEnvelope.has_value());
    }

    SECTION("the sidecar decoder does not accept a main snapshot payload") {
        auto mainBytes = serializeTopologyBatchBinary(batch);
        REQUIRE(mainBytes.has_value());
        auto decoded = deserializeTopologyRoadSidecarBinary(mainBytes.value());
        REQUIRE_FALSE(decoded.has_value());
        auto mainEnvelope = serializeTopologyBatchCompressed(batch);
        REQUIRE(mainEnvelope.has_value());
        auto decodedEnvelope = deserializeTopologyRoadSidecarCompressed(mainEnvelope.value());
        REQUIRE_FALSE(decodedEnvelope.has_value());
    }

    SECTION("garbage text") {
        auto decoded = deserializeTopologyRoadSidecarCompressed("not a sidecar");
        REQUIRE_FALSE(decoded.has_value());
    }
}

TEST_CASE("Main snapshot payload is byte-identical with roads on and off",
          "[unit][topology][roads][sidecar][codec][catch2]") {
    auto off = makeBatch("snap-same", 3);
    auto on = makeBatchWithRoads("snap-same", 3);
    REQUIRE(off.roads.empty());
    REQUIRE_FALSE(on.roads.empty());

    auto offBinary = serializeTopologyBatchBinary(off);
    auto onBinary = serializeTopologyBatchBinary(on);
    REQUIRE(offBinary.has_value());
    REQUIRE(onBinary.has_value());
    CHECK(offBinary.value() == onBinary.value());

    auto offCompressed = serializeTopologyBatchCompressed(off);
    auto onCompressed = serializeTopologyBatchCompressed(on);
    REQUIRE(offCompressed.has_value());
    REQUIRE(onCompressed.has_value());
    CHECK(offCompressed.value() == onCompressed.value());

    // Decoding the main payload alone never yields roads.
    auto decoded = deserializeTopologyBatchCompressed(onCompressed.value());
    REQUIRE(decoded.has_value());
    CHECK(decoded.value().roads.empty());
    CHECK(decoded.value().roadUnavailableReason.empty());
    CHECK(decoded.value().clusters.size() == 3);
}

TEST_CASE("Road sidecar validation is soft and names the first violation",
          "[unit][topology][roads][sidecar][validate][catch2]") {
    auto batch = makeBatch("snap-v", 1);
    auto sidecar = sidecarFor(makeBatchWithRoads("snap-v", 1));
    CHECK_FALSE(validateTopologyRoadSidecar(batch, sidecar).has_value());

    SECTION("snapshot id mismatch") {
        sidecar.snapshotId = "someone-else";
        auto reason = validateTopologyRoadSidecar(batch, sidecar);
        REQUIRE(reason.has_value());
        CHECK(*reason == "road_snapshot_mismatch");
    }
    SECTION("self loop") {
        sidecar.roads[0].toClusterId = sidecar.roads[0].fromClusterId;
        auto reason = validateTopologyRoadSidecar(batch, sidecar);
        REQUIRE(reason.has_value());
        CHECK(*reason == "road_self_loop");
    }
    SECTION("unknown source cluster") {
        sidecar.roads[0].fromClusterId = "cluster-nope";
        auto reason = validateTopologyRoadSidecar(batch, sidecar);
        REQUIRE(reason.has_value());
        CHECK(*reason == "road_endpoint_not_cluster");
    }
    SECTION("unknown target cluster") {
        sidecar.roads[1].toClusterId = "cluster-nope";
        auto reason = validateTopologyRoadSidecar(batch, sidecar);
        REQUIRE(reason.has_value());
        CHECK(*reason == "road_endpoint_not_cluster");
    }
    SECTION("negative weight") {
        sidecar.roads[2].weight = -0.1;
        auto reason = validateTopologyRoadSidecar(batch, sidecar);
        REQUIRE(reason.has_value());
        CHECK(*reason == "road_weight_invalid");
    }
    SECTION("non-finite weight") {
        sidecar.roads[2].weight = std::numeric_limits<double>::quiet_NaN();
        REQUIRE(validateTopologyRoadSidecar(batch, sidecar) == std::string{"road_weight_invalid"});
        sidecar.roads[2].weight = std::numeric_limits<double>::infinity();
        REQUIRE(validateTopologyRoadSidecar(batch, sidecar) == std::string{"road_weight_invalid"});
    }
    SECTION("more roads per source than the declared top-K") {
        sidecar.maxPerCluster = 1; // cluster-hash_a has two roads
        auto reason = validateTopologyRoadSidecar(batch, sidecar);
        REQUIRE(reason.has_value());
        CHECK(*reason == "road_count_exceeds_max_per_cluster");
    }
    SECTION("a zero top-K with roads present is a bound violation") {
        sidecar.maxPerCluster = 0;
        auto reason = validateTopologyRoadSidecar(batch, sidecar);
        REQUIRE(reason.has_value());
        CHECK(*reason == "road_count_exceeds_max_per_cluster");
    }
    SECTION("an empty sidecar is always valid") {
        sidecar.roads.clear();
        sidecar.maxPerCluster = 0;
        CHECK_FALSE(validateTopologyRoadSidecar(batch, sidecar).has_value());
    }
}

// ---------------------------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------------------------

TEST_CASE("MetadataKgTopologyArtifactStore persists roads in a sidecar node and reloads them",
          "[unit][topology][roads][sidecar][store][catch2]") {
    StoreFixture fixture;
    auto batch = makeBatchWithRoads("snap-1", 1);
    {
        MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
        REQUIRE(store.storeBatch(batch).has_value());
        // The resident snapshot carries the roads it was stored with.
        auto resident = store.loadLatestShared();
        REQUIRE(resident.has_value());
        REQUIRE(resident.value());
        CHECK(resident.value()->roads == batch.roads);
    }

    auto node = fixture.kgStore->getNodeByKey(roadNodeKey("snap-1"));
    REQUIRE(node.has_value());
    REQUIRE(node.value().has_value());
    CHECK(node.value()->type == std::string(kRoadNodeType));
    REQUIRE(node.value()->properties.has_value());
    auto sidecar = deserializeTopologyRoadSidecarCompressed(*node.value()->properties);
    REQUIRE(sidecar.has_value());
    CHECK(sidecar.value().snapshotId == "snap-1");
    CHECK(sidecar.value().maxPerCluster == 2);
    CHECK(sidecar.value().roads == batch.roads);

    // The main snapshot node is a plain v1 payload: an old decoder reading it alone sees no roads.
    auto main =
        deserializeTopologyBatchCompressed(fixture.nodeProperties(snapshotNodeKey("snap-1")));
    REQUIRE(main.has_value());
    CHECK(main.value().roads.empty());
    CHECK(main.value().clusters.size() == 3);

    MetadataKgTopologyArtifactStore reopened(fixture.repository, fixture.kgStore);
    auto loaded = reopened.loadLatestShared();
    REQUIRE(loaded.has_value());
    REQUIRE(loaded.value());
    CHECK(loaded.value()->snapshotId == "snap-1");
    CHECK(loaded.value()->roads == batch.roads);
    CHECK(loaded.value()->roadMaxPerCluster == 2);
    CHECK(loaded.value()->roadUnavailableReason.empty());

    SECTION("loading a historical snapshot by id attaches its own sidecar") {
        REQUIRE(reopened.storeBatch(makeBatch("snap-2", 2)).has_value());
        auto historical = reopened.loadLatest("snap-1");
        REQUIRE(historical.has_value());
        REQUIRE(historical.value().has_value());
        CHECK(historical.value()->roads == batch.roads);
        auto current = reopened.loadLatest();
        REQUIRE(current.has_value());
        REQUIRE(current.value().has_value());
        CHECK(current.value()->roads.empty());
    }
}

TEST_CASE("MetadataKgTopologyArtifactStore writes no sidecar when roads are off",
          "[unit][topology][roads][sidecar][store][catch2]") {
    StoreFixture fixture;
    MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
    REQUIRE(store.storeBatch(makeBatch("snap-off", 1)).has_value());
    CHECK_FALSE(fixture.nodeExists(roadNodeKey("snap-off")));
    auto count = fixture.kgStore->countNodesByType(kRoadNodeType);
    REQUIRE(count.has_value());
    CHECK(count.value() == 0);

    SECTION("re-storing a snapshot id without roads removes its stale sidecar") {
        REQUIRE(store.storeBatch(makeBatchWithRoads("snap-off", 2)).has_value());
        REQUIRE(fixture.nodeExists(roadNodeKey("snap-off")));
        REQUIRE(store.storeBatch(makeBatch("snap-off", 3)).has_value());
        CHECK_FALSE(fixture.nodeExists(roadNodeKey("snap-off")));
        MetadataKgTopologyArtifactStore reopened(fixture.repository, fixture.kgStore);
        auto loaded = reopened.loadLatestShared();
        REQUIRE(loaded.has_value());
        REQUIRE(loaded.value());
        CHECK(loaded.value()->roads.empty());
    }
}

TEST_CASE("MetadataKgTopologyArtifactStore treats a missing sidecar as empty roads",
          "[unit][topology][roads][sidecar][store][catch2]") {
    StoreFixture fixture;
    {
        MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
        REQUIRE(store.storeBatch(makeBatchWithRoads("snap-1", 1)).has_value());
    }
    auto node = fixture.kgStore->getNodeByKey(roadNodeKey("snap-1"));
    REQUIRE(node.has_value());
    REQUIRE(node.value().has_value());
    REQUIRE(fixture.kgStore->deleteNodeById(node.value()->id).has_value());

    MetadataKgTopologyArtifactStore reopened(fixture.repository, fixture.kgStore);
    auto loaded = reopened.loadLatestShared();
    REQUIRE(loaded.has_value());
    REQUIRE(loaded.value());
    CHECK(loaded.value()->snapshotId == "snap-1");
    CHECK(loaded.value()->roads.empty());
    CHECK(loaded.value()->roadUnavailableReason.empty());
    CHECK(loaded.value()->clusters.size() == 3);
}

TEST_CASE("MetadataKgTopologyArtifactStore drops a bad sidecar, records a reason, and keeps "
          "routing",
          "[unit][topology][roads][sidecar][store][catch2]") {
    StoreFixture fixture;
    const auto stored = makeBatchWithRoads("snap-1", 1);
    {
        MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
        REQUIRE(store.storeBatch(stored).has_value());
    }
    const auto key = roadNodeKey("snap-1");
    std::string expectedReasonToken;

    SECTION("undecodable payload") {
        fixture.overwriteNodeProperties(key, "not a road sidecar");
        expectedReasonToken = "road_sidecar_undecodable";
    }
    SECTION("bad magic inside a well-formed envelope") {
        auto sidecar = sidecarFor(stored);
        auto bytes = serializeTopologyRoadSidecarBinary(sidecar);
        REQUIRE(bytes.has_value());
        bytes.value()[0] = std::byte{'Z'};
        // Re-wrap the corrupted bytes in a valid envelope by hand.
        nlohmann::json envelope;
        envelope["format"] = "topology_roads_zstd_v1";
        envelope["compression"] = "none";
        envelope["snapshot_id"] = "snap-1";
        envelope["uncompressed_bytes"] = bytes.value().size();
        envelope["data_b64"] = encodeBase64(bytes.value());
        fixture.overwriteNodeProperties(key, envelope.dump());
        expectedReasonToken = "road_sidecar_undecodable";
    }
    SECTION("self loop") {
        auto sidecar = sidecarFor(stored);
        sidecar.roads[0].toClusterId = sidecar.roads[0].fromClusterId;
        fixture.overwriteNodeProperties(key, encodedSidecar(sidecar));
        expectedReasonToken = "road_self_loop";
    }
    SECTION("unknown cluster") {
        auto sidecar = sidecarFor(stored);
        sidecar.roads[1].toClusterId = "cluster-ghost";
        fixture.overwriteNodeProperties(key, encodedSidecar(sidecar));
        expectedReasonToken = "road_endpoint_not_cluster";
    }
    SECTION("over the declared top-K") {
        auto sidecar = sidecarFor(stored);
        sidecar.maxPerCluster = 1;
        fixture.overwriteNodeProperties(key, encodedSidecar(sidecar));
        expectedReasonToken = "road_count_exceeds_max_per_cluster";
    }
    SECTION("sidecar for another snapshot") {
        auto sidecar = sidecarFor(stored);
        sidecar.snapshotId = "snap-0";
        fixture.overwriteNodeProperties(key, encodedSidecar(sidecar));
        expectedReasonToken = "road_snapshot_mismatch";
    }
    SECTION("negative weight") {
        auto sidecar = sidecarFor(stored);
        sidecar.roads[3].weight = -1.0;
        fixture.overwriteNodeProperties(key, encodedSidecar(sidecar));
        expectedReasonToken = "road_weight_invalid";
    }

    MetadataKgTopologyArtifactStore reopened(fixture.repository, fixture.kgStore);
    auto loaded = reopened.loadLatestShared();
    REQUIRE(loaded.has_value());
    REQUIRE(loaded.value());
    CHECK(loaded.value()->snapshotId == "snap-1");
    CHECK(loaded.value()->clusters.size() == 3);
    CHECK(loaded.value()->memberships.size() == 3);
    CHECK(loaded.value()->roads.empty());
    REQUIRE_FALSE(loaded.value()->roadUnavailableReason.empty());
    CHECK(loaded.value()->roadUnavailableReason.find(expectedReasonToken) != std::string::npos);

    // Routing still admits the snapshot: a bad sidecar never disables routing.
    yams::search::TopologyRoutingSnapshotCache cache(
        yams::search::TopologyRoutingSharedSnapshotLoader{
            [&reopened]() { return reopened.loadLatestShared(); }});
    auto lookup = cache.get(0, /*requireDenseAnnIndex=*/false);
    REQUIRE(lookup.has_value());
    REQUIRE(lookup.value().snapshot);
    CHECK(lookup.value().snapshot->artifacts->roads.empty());
    CHECK(lookup.value().snapshot->artifacts->roadUnavailableReason ==
          loaded.value()->roadUnavailableReason);
    CHECK_FALSE(lookup.value().snapshot->constructionFingerprint.empty());
    // The construction fingerprint is the same as the one the stored batch would carry.
    CHECK(lookup.value().snapshot->constructionFingerprint ==
          yams::search::topologyRoutingConstructionFingerprint(stored));
}

TEST_CASE("MetadataKgTopologyArtifactStore writes the sidecar before the latest pointer",
          "[unit][topology][roads][sidecar][store][catch2]") {
    StoreFixture fixture;
    {
        MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
        REQUIRE(store.storeBatch(makeBatchWithRoads("snap-good", 1)).has_value());
    }
    auto injected = fixture.pool->withConnection([](Database& db) -> Result<void> {
        return db.execute("CREATE TRIGGER inject_road_failure BEFORE INSERT ON kg_nodes "
                          "WHEN NEW.node_key = 'topology:roads:snap-bad' "
                          "BEGIN SELECT RAISE(ABORT, 'injected road sidecar failure'); END;");
    });
    REQUIRE(injected.has_value());
    {
        MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
        CHECK_FALSE(store.storeBatch(makeBatchWithRoads("snap-bad", 2)).has_value());
    }
    MetadataKgTopologyArtifactStore reopened(fixture.repository, fixture.kgStore);
    auto latest = reopened.loadLatestShared();
    REQUIRE(latest.has_value());
    REQUIRE(latest.value());
    CHECK(latest.value()->snapshotId == "snap-good");
    CHECK(latest.value()->roads.size() == 4);
}

TEST_CASE("MetadataKgTopologyArtifactStore retention removes sidecars with their snapshots",
          "[unit][topology][roads][sidecar][store][catch2]") {
    StoreFixture fixture;
    MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
    for (int i = 1; i <= 6; ++i) {
        REQUIRE(store.storeBatch(makeBatchWithRoads("snap-" + std::to_string(i), i)).has_value());
    }
    auto snapshots = fixture.kgStore->countNodesByType("topology_snapshot");
    REQUIRE(snapshots.has_value());
    CHECK(snapshots.value() == MetadataKgTopologyArtifactStore::kRetainedSnapshots);
    auto sidecars = fixture.kgStore->countNodesByType(kRoadNodeType);
    REQUIRE(sidecars.has_value());
    CHECK(sidecars.value() == MetadataKgTopologyArtifactStore::kRetainedSnapshots);
    CHECK_FALSE(fixture.nodeExists(roadNodeKey("snap-1")));
    CHECK_FALSE(fixture.nodeExists(roadNodeKey("snap-3")));
    CHECK(fixture.nodeExists(roadNodeKey("snap-4")));
    CHECK(fixture.nodeExists(roadNodeKey("snap-6")));

    MetadataKgTopologyArtifactStore reopened(fixture.repository, fixture.kgStore);
    auto latest = reopened.loadLatestShared();
    REQUIRE(latest.has_value());
    REQUIRE(latest.value());
    CHECK(latest.value()->snapshotId == "snap-6");
    CHECK(latest.value()->roads.size() == 4);
}

// ---------------------------------------------------------------------------------------------
// Fingerprints
// ---------------------------------------------------------------------------------------------

TEST_CASE("Road fingerprint tracks roads while the construction fingerprint ignores them",
          "[unit][topology][roads][sidecar][fingerprint][catch2]") {
    const auto off = makeBatch("snap-fp", 1);
    const auto on = makeBatchWithRoads("snap-fp", 1);

    const auto construction = yams::search::topologyRoutingConstructionFingerprint(off);
    CHECK(yams::search::topologyRoutingConstructionFingerprint(on) == construction);

    const auto roadsOff = yams::search::topologyRoadFingerprint(off);
    const auto roadsOn = yams::search::topologyRoadFingerprint(on);
    CHECK_FALSE(roadsOff.empty());
    CHECK_FALSE(roadsOn.empty());
    CHECK(roadsOff != roadsOn);
    CHECK(roadsOn != construction);

    SECTION("a weight change moves the road fingerprint only") {
        auto edited = on;
        edited.roads[0].weight += 0.5;
        CHECK(yams::search::topologyRoadFingerprint(edited) != roadsOn);
        CHECK(yams::search::topologyRoutingConstructionFingerprint(edited) == construction);
    }
    SECTION("an extra road moves the road fingerprint only") {
        auto edited = on;
        edited.roads.push_back(ClusterRoad{"cluster-hash_b", "cluster-hash_c", 0.1});
        CHECK(yams::search::topologyRoadFingerprint(edited) != roadsOn);
        CHECK(yams::search::topologyRoutingConstructionFingerprint(edited) == construction);
    }
    SECTION("road order does not matter") {
        auto shuffled = on;
        std::swap(shuffled.roads[0], shuffled.roads[3]);
        CHECK(yams::search::topologyRoadFingerprint(shuffled) == roadsOn);
    }
    SECTION("the load-time reason is not part of either fingerprint") {
        auto flagged = on;
        flagged.roadUnavailableReason = "road_self_loop";
        CHECK(yams::search::topologyRoadFingerprint(flagged) == roadsOn);
        CHECK(yams::search::topologyRoutingConstructionFingerprint(flagged) == construction);
    }
}

TEST_CASE("Fingerprints survive the persisted path with and without the sidecar",
          "[unit][topology][roads][sidecar][fingerprint][store][catch2]") {
    StoreFixture fixture;
    const auto on = makeBatchWithRoads("snap-fp", 1);
    const auto construction = yams::search::topologyRoutingConstructionFingerprint(on);
    const auto roadsOn = yams::search::topologyRoadFingerprint(on);
    {
        MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
        REQUIRE(store.storeBatch(on).has_value());
    }

    MetadataKgTopologyArtifactStore withSidecar(fixture.repository, fixture.kgStore);
    auto loaded = withSidecar.loadLatestShared();
    REQUIRE(loaded.has_value());
    REQUIRE(loaded.value());
    CHECK(yams::search::topologyRoutingConstructionFingerprint(*loaded.value()) == construction);
    CHECK(yams::search::topologyRoadFingerprint(*loaded.value()) == roadsOn);

    yams::search::TopologyRoutingSnapshotCache cache(
        yams::search::TopologyRoutingSharedSnapshotLoader{
            [&withSidecar]() { return withSidecar.loadLatestShared(); }});
    auto lookup = cache.get(0, /*requireDenseAnnIndex=*/false);
    REQUIRE(lookup.has_value());
    REQUIRE(lookup.value().snapshot);
    CHECK(lookup.value().snapshot->constructionFingerprint == construction);
    CHECK(lookup.value().snapshot->roadFingerprint == roadsOn);

    // Remove the sidecar: construction identity is unchanged, road identity is the empty one.
    auto node = fixture.kgStore->getNodeByKey(roadNodeKey("snap-fp"));
    REQUIRE(node.has_value());
    REQUIRE(node.value().has_value());
    REQUIRE(fixture.kgStore->deleteNodeById(node.value()->id).has_value());
    MetadataKgTopologyArtifactStore withoutSidecar(fixture.repository, fixture.kgStore);
    auto bare = withoutSidecar.loadLatestShared();
    REQUIRE(bare.has_value());
    REQUIRE(bare.value());
    CHECK(yams::search::topologyRoutingConstructionFingerprint(*bare.value()) == construction);
    CHECK(yams::search::topologyRoadFingerprint(*bare.value()) ==
          yams::search::topologyRoadFingerprint(makeBatch("snap-fp", 1)));
    CHECK(yams::search::topologyRoadFingerprint(*bare.value()) != roadsOn);
}
