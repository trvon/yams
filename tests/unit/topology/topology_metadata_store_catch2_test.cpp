// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_codec.h>
#include <yams/topology/topology_metadata_store.h>

#include <nlohmann/json.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace yams;
using namespace yams::metadata;
using namespace yams::topology;

namespace {

std::filesystem::path tempDbPath(const char* prefix) {
    auto p = std::filesystem::temp_directory_path() /
             (std::string(prefix) +
              std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".db");
    std::error_code ec;
    std::filesystem::remove(p, ec);
    return p;
}

struct TestFixture {
    TestFixture() {
        dbPath = tempDbPath("topo_meta_store_test_");

        ConnectionPoolConfig poolConfig;
        poolConfig.minConnections = 1;
        poolConfig.maxConnections = 2;

        pool = std::make_unique<ConnectionPool>(dbPath.string(), poolConfig);
        REQUIRE(pool->initialize().has_value());

        repository = std::make_shared<MetadataRepository>(*pool);
        auto kgResult = makeSqliteKnowledgeGraphStore(*pool, KnowledgeGraphStoreConfig{});
        REQUIRE(kgResult.has_value());
        kgStore = std::shared_ptr<KnowledgeGraphStore>(kgResult.value().release());
    }

    ~TestFixture() {
        kgStore.reset();
        repository.reset();
        pool->shutdown();
        pool.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
    }

    int64_t addDocument(const std::string& hash, const std::string& path) {
        DocumentInfo doc;
        doc.sha256Hash = hash;
        doc.filePath = path;
        doc.fileName = std::filesystem::path(path).filename().string();
        doc.fileSize = 100;
        doc.mimeType = "text/plain";
        auto res = repository->insertDocument(doc);
        REQUIRE(res.has_value());
        return res.value();
    }

    int64_t countMetadataValueCounts(const std::string& key) {
        auto res = pool->withConnection([&](Database& db) -> Result<int64_t> {
            auto stmtRes = db.prepare(
                "SELECT COALESCE(SUM(count), 0) FROM metadata_value_counts WHERE key = ?");
            if (!stmtRes)
                return stmtRes.error();
            auto stmt = std::move(stmtRes).value();
            auto bindRes = stmt.bind(1, key);
            if (!bindRes)
                return bindRes.error();
            auto stepRes = stmt.step();
            if (!stepRes)
                return stepRes.error();
            return stmt.getInt64(0);
        });
        REQUIRE(res.has_value());
        return res.value();
    }

    int64_t countMetadataValueCountsLike(const std::string& pattern) {
        auto res = pool->withConnection([&](Database& db) -> Result<int64_t> {
            auto stmtRes = db.prepare(
                "SELECT COALESCE(SUM(count), 0) FROM metadata_value_counts WHERE key LIKE ?");
            if (!stmtRes)
                return stmtRes.error();
            auto stmt = std::move(stmtRes).value();
            auto bindRes = stmt.bind(1, pattern);
            if (!bindRes)
                return bindRes.error();
            auto stepRes = stmt.step();
            if (!stepRes)
                return stepRes.error();
            return stmt.getInt64(0);
        });
        REQUIRE(res.has_value());
        return res.value();
    }

    std::filesystem::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> repository;
    std::shared_ptr<KnowledgeGraphStore> kgStore;
};

} // namespace

TEST_CASE("Metadata value counts triggers bypass topology metadata keys",
          "[unit][metadata][topology][triggers]") {
    TestFixture fixture;

    const auto docId = fixture.addDocument("doc_hash_1", "/path/to/doc1.txt");

    // 1. Insert normal metadata: should be tracked in metadata_value_counts
    REQUIRE(fixture.repository->setMetadata(docId, "tag", MetadataValue("c++")).has_value());

    // 2. Insert topology metadata: MUST be ignored by triggers
    REQUIRE(
        fixture.repository->setMetadata(docId, "topology.snapshot_id", MetadataValue("snap-123"))
            .has_value());
    REQUIRE(fixture.repository->setMetadata(docId, "topology.cluster_id", MetadataValue("c_001"))
                .has_value());

    // "tag" -> "c++" must be present with count 1 in metadata_value_counts table
    CHECK(fixture.countMetadataValueCounts("tag") == 1);

    // "topology.snapshot_id" and "topology.cluster_id" MUST NOT be tracked in metadata_value_counts
    CHECK(fixture.countMetadataValueCounts("topology.snapshot_id") == 0);
    CHECK(fixture.countMetadataValueCounts("topology.cluster_id") == 0);
    CHECK(fixture.countMetadataValueCountsLike("topology.%") == 0);
}

TEST_CASE("MetadataKgTopologyArtifactStore storeBatch and loadMemberships fast path",
          "[unit][topology][store][perf]") {
    TestFixture fixture;

    const auto id1 = fixture.addDocument("hash_a", "/repo/a.md");
    const auto id2 = fixture.addDocument("hash_b", "/repo/b.md");
    const auto id3 = fixture.addDocument("hash_c", "/repo/c.md");

    MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);

    TopologyArtifactBatch batch;
    batch.snapshotId = "topology-snap-001";
    batch.algorithm = "connected_components_v1";
    batch.topologyEpoch = 1;
    batch.generatedAtUnixSeconds = 1700000000;

    ClusterArtifact cluster1;
    cluster1.clusterId = "cluster-1";
    cluster1.level = 0;
    cluster1.memberCount = 2;
    cluster1.memberDocumentHashes = {"hash_a", "hash_b"};
    batch.clusters.push_back(cluster1);

    ClusterArtifact cluster2;
    cluster2.clusterId = "cluster-2";
    cluster2.level = 0;
    cluster2.memberCount = 1;
    cluster2.memberDocumentHashes = {"hash_c"};
    batch.clusters.push_back(cluster2);

    DocumentClusterMembership m1;
    m1.documentHash = "hash_a";
    m1.clusterId = "cluster-1";
    m1.clusterLevel = 0;
    m1.role = DocumentTopologyRole::Medoid;
    m1.persistenceScore = 0.85;
    m1.cohesionScore = 0.90;
    m1.bridgeScore = 0.10;
    batch.memberships.push_back(m1);

    DocumentClusterMembership m2;
    m2.documentHash = "hash_b";
    m2.clusterId = "cluster-1";
    m2.clusterLevel = 0;
    m2.role = DocumentTopologyRole::Core;
    m2.persistenceScore = 0.80;
    m2.cohesionScore = 0.75;
    m2.bridgeScore = 0.15;
    batch.memberships.push_back(m2);

    DocumentClusterMembership m3;
    m3.documentHash = "hash_c";
    m3.clusterId = "cluster-2";
    m3.clusterLevel = 0;
    m3.role = DocumentTopologyRole::Outlier;
    m3.persistenceScore = 0.50;
    m3.cohesionScore = 0.40;
    m3.bridgeScore = 0.60;
    m3.overlapClusterIds = {"cluster-1"};
    batch.memberships.push_back(m3);

    // 1. Store batch
    auto storeRes = store.storeBatch(batch);
    REQUIRE(storeRes.has_value());

    // 2. Verify metadata table only has essential snapshot and cluster ID keys
    auto allMeta1 = fixture.repository->getAllMetadata(id1);
    REQUIRE(allMeta1.has_value());
    CHECK(allMeta1.value().contains("topology.snapshot_id"));
    CHECK(allMeta1.value().contains("topology.cluster_id"));
    CHECK_FALSE(allMeta1.value().contains("topology.persistence_score"));
    CHECK_FALSE(allMeta1.value().contains("topology.cohesion_score"));
    CHECK_FALSE(allMeta1.value().contains("topology.role"));

    auto allMeta2 = fixture.repository->getAllMetadata(id2);
    REQUIRE(allMeta2.has_value());
    CHECK(allMeta2.value().contains("topology.snapshot_id"));
    CHECK(allMeta2.value().contains("topology.cluster_id"));

    auto allMeta3 = fixture.repository->getAllMetadata(id3);
    REQUIRE(allMeta3.has_value());
    CHECK(allMeta3.value().contains("topology.snapshot_id"));
    CHECK(allMeta3.value().contains("topology.cluster_id"));

    // 3. Verify metadata_value_counts has 0 entries for topology
    CHECK(fixture.countMetadataValueCounts("topology.snapshot_id") == 0);
    CHECK(fixture.countMetadataValueCounts("topology.cluster_id") == 0);
    CHECK(fixture.countMetadataValueCountsLike("topology.%") == 0);

    // 4. Load memberships via fast-path
    std::vector<std::string> queryHashes = {"hash_a", "hash_c", "non_existent"};
    auto loadedRes = store.loadMemberships(queryHashes);
    REQUIRE(loadedRes.has_value());
    REQUIRE(loadedRes.value().size() == 2);

    const auto& loaded = loadedRes.value();
    CHECK(loaded[0].documentHash == "hash_a");
    CHECK(loaded[0].clusterId == "cluster-1");
    CHECK(loaded[0].role == DocumentTopologyRole::Medoid);
    CHECK(loaded[0].persistenceScore == Catch::Approx(0.85));

    CHECK(loaded[1].documentHash == "hash_c");
    CHECK(loaded[1].clusterId == "cluster-2");
    CHECK(loaded[1].role == DocumentTopologyRole::Outlier);
    CHECK(loaded[1].overlapClusterIds == std::vector<std::string>{"cluster-1"});
}

namespace {

TopologyArtifactBatch makeTwoDocBatch(const std::string& snapshotId, uint64_t epoch) {
    TopologyArtifactBatch batch;
    batch.snapshotId = snapshotId;
    batch.algorithm = "connected_components_v1";
    batch.topologyEpoch = epoch;
    ClusterArtifact cluster;
    cluster.clusterId = "cluster-" + snapshotId;
    cluster.memberCount = 2;
    cluster.memberDocumentHashes = {"hash_a", "hash_b"};
    batch.clusters.push_back(cluster);
    for (const auto* hash : {"hash_a", "hash_b"}) {
        DocumentClusterMembership membership;
        membership.documentHash = hash;
        membership.clusterId = cluster.clusterId;
        batch.memberships.push_back(membership);
    }
    return batch;
}

} // namespace

TEST_CASE("MetadataKgTopologyArtifactStore shares one resident snapshot",
          "[unit][topology][store]") {
    TestFixture fixture;
    fixture.addDocument("hash_a", "/repo/a.md");
    fixture.addDocument("hash_b", "/repo/b.md");

    MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
    REQUIRE(store.storeBatch(makeTwoDocBatch("snap-1", 1)).has_value());

    auto first = store.loadLatestShared();
    auto second = store.loadLatestShared();
    REQUIRE(first.has_value());
    REQUIRE(second.has_value());
    REQUIRE(first.value());
    CHECK(first.value().get() == second.value().get());
    CHECK(first.value()->snapshotId == "snap-1");

    REQUIRE(store.storeBatch(makeTwoDocBatch("snap-2", 2)).has_value());
    auto third = store.loadLatestShared();
    REQUIRE(third.has_value());
    REQUIRE(third.value());
    CHECK(third.value()->snapshotId == "snap-2");
    // A reader holding the previous snapshot keeps a valid, unchanged view.
    CHECK(first.value()->snapshotId == "snap-1");

    SECTION("A fresh store reloads the persisted snapshot") {
        MetadataKgTopologyArtifactStore reopened(fixture.repository, fixture.kgStore);
        auto loaded = reopened.loadLatestShared();
        REQUIRE(loaded.has_value());
        REQUIRE(loaded.value());
        CHECK(loaded.value()->snapshotId == "snap-2");
    }
}

TEST_CASE("MetadataKgTopologyArtifactStore tolerates concurrent readers and a writer",
          "[unit][topology][store][concurrency]") {
    TestFixture fixture;
    fixture.addDocument("hash_a", "/repo/a.md");
    fixture.addDocument("hash_b", "/repo/b.md");

    MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
    REQUIRE(store.storeBatch(makeTwoDocBatch("snap-0", 1)).has_value());

    constexpr int kWrites = 8;
    constexpr int kReadsPerThread = 64;
    std::atomic<bool> readFailure{false};
    std::vector<std::thread> readers;
    for (int t = 0; t < 3; ++t) {
        readers.emplace_back([&] {
            const std::vector<std::string> hashes = {"hash_a", "hash_b"};
            for (int i = 0; i < kReadsPerThread; ++i) {
                auto latest = store.loadLatest();
                auto memberships = store.loadMemberships(hashes);
                if (!latest || !latest.value().has_value() || !memberships ||
                    memberships.value().size() != 2) {
                    readFailure = true;
                }
            }
        });
    }
    for (int i = 1; i <= kWrites; ++i) {
        REQUIRE(store.storeBatch(makeTwoDocBatch("snap-" + std::to_string(i), i + 1)).has_value());
    }
    for (auto& reader : readers) {
        reader.join();
    }
    CHECK_FALSE(readFailure.load());

    auto latest = store.loadLatestShared();
    REQUIRE(latest.has_value());
    REQUIRE(latest.value());
    CHECK(latest.value()->snapshotId == "snap-" + std::to_string(kWrites));
}

TEST_CASE("MetadataKgTopologyArtifactStore failed store leaves the prior snapshot authoritative",
          "[unit][topology][store]") {
    TestFixture fixture;
    const auto idA = fixture.addDocument("hash_a", "/repo/a.md");
    fixture.addDocument("hash_b", "/repo/b.md");

    {
        MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
        REQUIRE(store.storeBatch(makeTwoDocBatch("snap-good", 1)).has_value());
    }

    // Inject a failure when the next snapshot node is written.
    auto injected = fixture.pool->withConnection([](Database& db) -> Result<void> {
        return db.execute("CREATE TRIGGER inject_snapshot_failure BEFORE INSERT ON kg_nodes "
                          "WHEN NEW.node_key = 'topology:snapshot:snap-bad' "
                          "BEGIN SELECT RAISE(ABORT, 'injected snapshot failure'); END;");
    });
    REQUIRE(injected.has_value());

    {
        MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
        CHECK_FALSE(store.storeBatch(makeTwoDocBatch("snap-bad", 2)).has_value());
    }

    MetadataKgTopologyArtifactStore reopened(fixture.repository, fixture.kgStore);
    auto latest = reopened.loadLatestShared();
    REQUIRE(latest.has_value());
    REQUIRE(latest.value());
    CHECK(latest.value()->snapshotId == "snap-good");

    // Per-document metadata must not point at a snapshot that was never persisted.
    auto snapshotId = fixture.repository->getMetadata(idA, "topology.snapshot_id");
    REQUIRE(snapshotId.has_value());
    REQUIRE(snapshotId.value().has_value());
    CHECK(snapshotId.value()->asString() == "snap-good");
}

TEST_CASE("MetadataKgTopologyArtifactStore recovers from an undecodable latest snapshot",
          "[unit][topology][store]") {
    TestFixture fixture;
    fixture.addDocument("hash_a", "/repo/a.md");
    fixture.addDocument("hash_b", "/repo/b.md");
    {
        MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
        REQUIRE(store.storeBatch(makeTwoDocBatch("snap-1", 1)).has_value());
    }

    // A corrupt payload must not block every future rebuild: the previous snapshot is only
    // needed to find documents that left the topology.
    auto corrupted = fixture.pool->withConnection([](Database& db) -> Result<void> {
        return db.execute("UPDATE kg_nodes SET properties = 'not a topology payload' "
                          "WHERE node_key = 'topology:snapshot:snap-1'");
    });
    REQUIRE(corrupted.has_value());

    MetadataKgTopologyArtifactStore reopened(fixture.repository, fixture.kgStore);
    auto stored = reopened.storeBatch(makeTwoDocBatch("snap-2", 2));
    INFO((stored ? std::string{} : stored.error().message));
    REQUIRE(stored.has_value());
    auto latest = reopened.loadLatestShared();
    REQUIRE(latest.has_value());
    REQUIRE(latest.value());
    CHECK(latest.value()->snapshotId == "snap-2");
}

TEST_CASE("MetadataKgTopologyArtifactStore clears topology keys for dropped documents",
          "[unit][topology][store]") {
    TestFixture fixture;
    fixture.addDocument("hash_a", "/repo/a.md");
    const auto idB = fixture.addDocument("hash_b", "/repo/b.md");

    MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
    REQUIRE(store.storeBatch(makeTwoDocBatch("snap-1", 1)).has_value());

    auto next = makeTwoDocBatch("snap-2", 2);
    next.memberships.pop_back(); // hash_b leaves the topology
    next.clusters[0].memberDocumentHashes = {"hash_a"};
    next.clusters[0].memberCount = 1;
    REQUIRE(store.storeBatch(next).has_value());

    auto metaB = fixture.repository->getAllMetadata(idB);
    REQUIRE(metaB.has_value());
    CHECK_FALSE(metaB.value().contains("topology.snapshot_id"));
    CHECK_FALSE(metaB.value().contains("topology.cluster_id"));
}

namespace {

void addDocGraphNode(TestFixture& fixture, const std::string& hash) {
    KGNode node;
    node.nodeKey = "doc:" + hash;
    node.label = hash;
    node.type = std::string{"document"};
    REQUIRE(fixture.kgStore->upsertNode(node).has_value());
}

std::int64_t nodeIdForKey(TestFixture& fixture, const std::string& key) {
    auto node = fixture.kgStore->getNodeByKey(key);
    REQUIRE(node.has_value());
    REQUIRE(node.value().has_value());
    return node.value()->id;
}

} // namespace

TEST_CASE("MetadataKgTopologyArtifactStore materializes clusters in the knowledge graph",
          "[unit][topology][store][graph]") {
    TestFixture fixture;
    fixture.addDocument("hash_a", "/repo/a.md");
    fixture.addDocument("hash_b", "/repo/b.md");
    addDocGraphNode(fixture, "hash_a");
    addDocGraphNode(fixture, "hash_b");

    auto batch = makeTwoDocBatch("snap-1", 1);
    batch.clusters[0].medoid = ClusterRepresentative{.clusterId = batch.clusters[0].clusterId,
                                                     .documentHash = "hash_a",
                                                     .filePath = "/repo/a.md",
                                                     .representativeScore = 0.9};
    ClusterArtifact child;
    child.clusterId = "child-cluster";
    child.parentClusterId = batch.clusters[0].clusterId;
    child.level = 1;
    child.memberCount = 1;
    child.memberDocumentHashes = {"hash_b"};
    batch.clusters.push_back(child);
    batch.memberships[1].overlapClusterIds = {"child-cluster"};

    MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
    REQUIRE(store.storeBatch(batch).has_value());

    const auto clusterId = nodeIdForKey(fixture, "topology:cluster:cluster-snap-1");
    const auto childId = nodeIdForKey(fixture, "topology:cluster:child-cluster");
    const auto docA = nodeIdForKey(fixture, "doc:hash_a");
    const auto docB = nodeIdForKey(fixture, "doc:hash_b");

    auto members = fixture.kgStore->getEdgesTo(clusterId, std::string_view{"member_of"}, 10, 0);
    REQUIRE(members.has_value());
    CHECK(members.value().size() == 2U);

    auto medoid = fixture.kgStore->getEdgesFrom(docA, std::string_view{"medoid_of"}, 10, 0);
    REQUIRE(medoid.has_value());
    REQUIRE(medoid.value().size() == 1U);
    CHECK(medoid.value().front().dstNodeId == clusterId);

    auto overlaps = fixture.kgStore->getEdgesFrom(docB, std::string_view{"overlaps"}, 10, 0);
    REQUIRE(overlaps.has_value());
    REQUIRE(overlaps.value().size() == 1U);
    CHECK(overlaps.value().front().dstNodeId == childId);

    auto parent = fixture.kgStore->getEdgesFrom(childId, std::string_view{"subcluster_of"}, 10, 0);
    REQUIRE(parent.has_value());
    REQUIRE(parent.value().size() == 1U);
    CHECK(parent.value().front().dstNodeId == clusterId);

    SECTION("A newer snapshot replaces the previous cluster graph") {
        REQUIRE(store.storeBatch(makeTwoDocBatch("snap-2", 2)).has_value());
        auto stale = fixture.kgStore->getNodeByKey("topology:cluster:cluster-snap-1");
        REQUIRE(stale.has_value());
        CHECK_FALSE(stale.value().has_value());
        auto staleChild = fixture.kgStore->getNodeByKey("topology:cluster:child-cluster");
        REQUIRE(staleChild.has_value());
        CHECK_FALSE(staleChild.value().has_value());
        const auto fresh = nodeIdForKey(fixture, "topology:cluster:cluster-snap-2");
        auto freshMembers =
            fixture.kgStore->getEdgesTo(fresh, std::string_view{"member_of"}, 10, 0);
        REQUIRE(freshMembers.has_value());
        CHECK(freshMembers.value().size() == 2U);
    }
}

TEST_CASE("MetadataKgTopologyArtifactStore retains a bounded number of snapshots",
          "[unit][topology][store][graph]") {
    TestFixture fixture;
    fixture.addDocument("hash_a", "/repo/a.md");
    fixture.addDocument("hash_b", "/repo/b.md");

    MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
    for (int i = 1; i <= 6; ++i) {
        REQUIRE(store.storeBatch(makeTwoDocBatch("snap-" + std::to_string(i), i)).has_value());
    }
    auto snapshots = fixture.kgStore->countNodesByType("topology_snapshot");
    REQUIRE(snapshots.has_value());
    CHECK(snapshots.value() == MetadataKgTopologyArtifactStore::kRetainedSnapshots);
    // The newest snapshots survive, including the one the latest pointer names.
    auto latest = store.loadLatestShared();
    REQUIRE(latest.has_value());
    REQUIRE(latest.value());
    CHECK(latest.value()->snapshotId == "snap-6");
    auto oldest = fixture.kgStore->getNodeByKey("topology:snapshot:snap-1");
    REQUIRE(oldest.has_value());
    CHECK_FALSE(oldest.value().has_value());
}

namespace {

// A batch over `docCount` documents whose compressed payload spans many small chunks: the
// document hashes are distinct 64-character strings, which compress poorly.
TopologyArtifactBatch makeWideBatch(TestFixture& fixture, const std::string& snapshotId,
                                    std::size_t docCount, bool addDocs = true) {
    TopologyArtifactBatch batch;
    batch.snapshotId = snapshotId;
    batch.algorithm = "connected_components_v1";
    batch.topologyEpoch = 7;
    batch.generatedAtUnixSeconds = 1700000000;
    constexpr std::size_t kClusters = 8;
    for (std::size_t c = 0; c < kClusters; ++c) {
        ClusterArtifact cluster;
        cluster.clusterId = "cluster-" + snapshotId + "-" + std::to_string(c);
        batch.clusters.push_back(cluster);
    }
    for (std::size_t i = 0; i < docCount; ++i) {
        std::string hash = std::to_string(i) + "x" + std::to_string(i * 2654435761ULL);
        while (hash.size() < 64) {
            hash += std::to_string((hash.size() * 40503ULL + i * 977ULL) % 1000003ULL);
        }
        hash.resize(64);
        if (addDocs) {
            fixture.addDocument(hash, "/repo/wide/" + std::to_string(i) + ".md");
        }
        auto& cluster = batch.clusters[i % kClusters];
        cluster.memberDocumentHashes.push_back(hash);
        ++cluster.memberCount;
        DocumentClusterMembership membership;
        membership.documentHash = hash;
        membership.clusterId = cluster.clusterId;
        membership.persistenceScore = static_cast<double>(i % 17) / 17.0;
        batch.memberships.push_back(membership);
    }
    return batch;
}

std::int64_t scalarQuery(TestFixture& fixture, const std::string& sql,
                         const std::string& param = {}) {
    auto res = fixture.pool->withConnection([&](Database& db) -> Result<std::int64_t> {
        auto stmtRes = db.prepare(sql);
        if (!stmtRes) {
            return stmtRes.error();
        }
        auto stmt = std::move(stmtRes).value();
        if (!param.empty()) {
            if (auto bound = stmt.bind(1, param); !bound) {
                return bound.error();
            }
        }
        auto stepRes = stmt.step();
        if (!stepRes) {
            return stepRes.error();
        }
        return stmt.getInt64(0);
    });
    REQUIRE(res.has_value());
    return res.value();
}

std::int64_t chunkCountFor(TestFixture& fixture, const std::string& ownerKey) {
    return scalarQuery(fixture,
                       "SELECT COUNT(*) FROM kg_nodes WHERE type = 'topology_payload_chunk' "
                       "AND label = ?",
                       ownerKey);
}

std::int64_t largestStoredValue(TestFixture& fixture) {
    return scalarQuery(fixture,
                       "SELECT COALESCE(MAX(LENGTH(CAST(properties AS BLOB))), 0) FROM kg_nodes");
}

nlohmann::json manifestOf(TestFixture& fixture, const std::string& nodeKey) {
    auto node = fixture.kgStore->getNodeByKey(nodeKey);
    REQUIRE(node.has_value());
    REQUIRE(node.value().has_value());
    REQUIRE(node.value()->properties.has_value());
    return nlohmann::json::parse(*node.value()->properties);
}

constexpr std::size_t kSmallChunk = 512;

} // namespace

TEST_CASE("MetadataKgTopologyArtifactStore round-trips a snapshot spanning many chunks",
          "[unit][topology][store][chunks]") {
    TestFixture fixture;
    const auto batch = makeWideBatch(fixture, "snap-wide", 200);

    MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
    store.setPayloadChunkBytesForTesting(kSmallChunk);
    REQUIRE(store.storeBatch(batch).has_value());

    const auto chunks = chunkCountFor(fixture, "topology:snapshot:snap-wide");
    CHECK(chunks >= 4);
    // No single stored value, manifest included, exceeds the chunk size.
    CHECK(largestStoredValue(fixture) <= static_cast<std::int64_t>(kSmallChunk));

    // The snapshot node holds only a manifest.
    const auto manifest = manifestOf(fixture, "topology:snapshot:snap-wide");
    CHECK(manifest.at("format") == "topology_chunked_v1");
    CHECK(manifest.at("chunk_count").get<std::int64_t>() == chunks);
    CHECK(manifest.at("membership_count") == 200);
    CHECK(manifest.at("snapshot_id") == "snap-wide");

    MetadataKgTopologyArtifactStore reopened(fixture.repository, fixture.kgStore);
    auto loaded = reopened.loadLatestShared();
    REQUIRE(loaded.has_value());
    REQUIRE(loaded.value());
    CHECK(loaded.value()->snapshotId == "snap-wide");
    REQUIRE(loaded.value()->memberships.size() == batch.memberships.size());
    CHECK(loaded.value()->memberships.back().documentHash == batch.memberships.back().documentHash);
    REQUIRE(loaded.value()->clusters.size() == batch.clusters.size());
    CHECK(loaded.value()->clusters[3].memberDocumentHashes ==
          batch.clusters[3].memberDocumentHashes);

    SECTION("a missing chunk makes the snapshot undecodable, not partially loaded") {
        auto removed = fixture.pool->withConnection([](Database& db) -> Result<void> {
            return db.execute("DELETE FROM kg_nodes WHERE node_key = "
                              "'topology:chunk:topology:snapshot:snap-wide:1'");
        });
        REQUIRE(removed.has_value());
        MetadataKgTopologyArtifactStore broken(fixture.repository, fixture.kgStore);
        auto failed = broken.loadLatestShared();
        REQUIRE_FALSE(failed.has_value());
        CHECK(failed.error().code == ErrorCode::InvalidData);
        // A rebuild still replaces it.
        broken.setPayloadChunkBytesForTesting(kSmallChunk);
        REQUIRE(broken.storeBatch(makeWideBatch(fixture, "snap-next", 50, false)).has_value());
        auto next = broken.loadLatestShared();
        REQUIRE(next.has_value());
        REQUIRE(next.value());
        CHECK(next.value()->snapshotId == "snap-next");
    }
}

TEST_CASE("MetadataKgTopologyArtifactStore reads a legacy single-value snapshot",
          "[unit][topology][store][chunks]") {
    TestFixture fixture;
    fixture.addDocument("hash_a", "/repo/a.md");
    fixture.addDocument("hash_b", "/repo/b.md");
    const auto batch = makeTwoDocBatch("snap-legacy", 3);
    {
        MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
        REQUIRE(store.storeBatch(batch).has_value());
    }
    // Rewrite the snapshot as the pre-chunking writer stored it: the whole v1 payload in
    // properties, no chunk nodes.
    auto legacy = serializeTopologyBatchCompressed(batch);
    REQUIRE(legacy.has_value());
    auto rewritten = fixture.pool->withConnection([&](Database& db) -> Result<void> {
        if (auto cleared = db.execute("DELETE FROM kg_nodes WHERE type = 'topology_payload_chunk'");
            !cleared) {
            return cleared.error();
        }
        auto stmtRes = db.prepare("UPDATE kg_nodes SET properties = ? WHERE node_key = ?");
        if (!stmtRes) {
            return stmtRes.error();
        }
        auto stmt = std::move(stmtRes).value();
        if (auto bound = stmt.bind(1, legacy.value()); !bound) {
            return bound.error();
        }
        if (auto bound = stmt.bind(2, std::string("topology:snapshot:snap-legacy")); !bound) {
            return bound.error();
        }
        return stmt.execute();
    });
    REQUIRE(rewritten.has_value());

    MetadataKgTopologyArtifactStore reopened(fixture.repository, fixture.kgStore);
    auto loaded = reopened.loadLatestShared();
    REQUIRE(loaded.has_value());
    REQUIRE(loaded.value());
    CHECK(loaded.value()->snapshotId == "snap-legacy");
    CHECK(loaded.value()->memberships.size() == 2U);
    CHECK(loaded.value()->topologyEpoch == 3U);

    // The next write uses the chunked format.
    REQUIRE(reopened.storeBatch(makeTwoDocBatch("snap-new", 4)).has_value());
    CHECK(chunkCountFor(fixture, "topology:snapshot:snap-new") >= 1);
    CHECK(manifestOf(fixture, "topology:snapshot:snap-new").at("format") == "topology_chunked_v1");
}

TEST_CASE("MetadataKgTopologyArtifactStore replacing or pruning a snapshot removes its chunks",
          "[unit][topology][store][chunks]") {
    TestFixture fixture;
    const auto wide = makeWideBatch(fixture, "snap-1", 200);
    MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
    store.setPayloadChunkBytesForTesting(kSmallChunk);
    REQUIRE(store.storeBatch(wide).has_value());
    const auto wideChunks = chunkCountFor(fixture, "topology:snapshot:snap-1");
    REQUIRE(wideChunks >= 4);

    SECTION("the same snapshot id stored again with a smaller payload") {
        REQUIRE(store.storeBatch(makeWideBatch(fixture, "snap-1", 10, false)).has_value());
        const auto narrowChunks = chunkCountFor(fixture, "topology:snapshot:snap-1");
        CHECK(narrowChunks < wideChunks);
        CHECK(
            manifestOf(fixture, "topology:snapshot:snap-1").at("chunk_count").get<std::int64_t>() ==
            narrowChunks);
        MetadataKgTopologyArtifactStore reopened(fixture.repository, fixture.kgStore);
        auto loaded = reopened.loadLatestShared();
        REQUIRE(loaded.has_value());
        REQUIRE(loaded.value());
        CHECK(loaded.value()->memberships.size() == 10U);
    }

    SECTION("snapshots beyond the retention window lose their chunks") {
        const auto total = MetadataKgTopologyArtifactStore::kRetainedSnapshots + 2;
        for (std::size_t i = 2; i <= total; ++i) {
            REQUIRE(
                store.storeBatch(makeWideBatch(fixture, "snap-" + std::to_string(i), 200, false))
                    .has_value());
        }
        CHECK(chunkCountFor(fixture, "topology:snapshot:snap-1") == 0);
        CHECK(chunkCountFor(fixture, "topology:snapshot:snap-2") == 0);
        CHECK(chunkCountFor(fixture, "topology:snapshot:snap-" + std::to_string(total)) > 0);
        const auto owners = scalarQuery(fixture, "SELECT COUNT(DISTINCT label) FROM kg_nodes "
                                                 "WHERE type = 'topology_payload_chunk'");
        CHECK(owners ==
              static_cast<std::int64_t>(MetadataKgTopologyArtifactStore::kRetainedSnapshots));
    }
}

TEST_CASE("MetadataKgTopologyArtifactStore failed chunk write leaves the previous pointer intact",
          "[unit][topology][store][chunks]") {
    TestFixture fixture;
    const auto good = makeWideBatch(fixture, "snap-good", 200);
    MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
    store.setPayloadChunkBytesForTesting(kSmallChunk);
    REQUIRE(store.storeBatch(good).has_value());

    // Fail the write partway through the next snapshot's chunks.
    auto injected = fixture.pool->withConnection([](Database& db) -> Result<void> {
        return db.execute("CREATE TRIGGER inject_chunk_failure BEFORE INSERT ON kg_nodes "
                          "WHEN NEW.node_key = 'topology:chunk:topology:snapshot:snap-bad:2' "
                          "BEGIN SELECT RAISE(ABORT, 'injected chunk failure'); END;");
    });
    REQUIRE(injected.has_value());
    CHECK_FALSE(store.storeBatch(makeWideBatch(fixture, "snap-bad", 200, false)).has_value());

    // The chunk transaction rolled back: no orphan chunks and no manifest for the bad snapshot.
    CHECK(chunkCountFor(fixture, "topology:snapshot:snap-bad") == 0);
    auto badNode = fixture.kgStore->getNodeByKey("topology:snapshot:snap-bad");
    REQUIRE(badNode.has_value());
    CHECK_FALSE(badNode.value().has_value());

    MetadataKgTopologyArtifactStore reopened(fixture.repository, fixture.kgStore);
    auto latest = reopened.loadLatestShared();
    REQUIRE(latest.has_value());
    REQUIRE(latest.value());
    CHECK(latest.value()->snapshotId == "snap-good");
    CHECK(latest.value()->memberships.size() == good.memberships.size());
}

TEST_CASE("MetadataKgTopologyArtifactStore clamps the chunk size to the value ceiling",
          "[unit][topology][store][chunks]") {
    TestFixture fixture;
    fixture.addDocument("hash_a", "/repo/a.md");
    fixture.addDocument("hash_b", "/repo/b.md");
    MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
    store.setPayloadChunkBytesForTesting(std::size_t{4} << 30U);
    REQUIRE(store.storeBatch(makeTwoDocBatch("snap-1", 1)).has_value());
    CHECK(manifestOf(fixture, "topology:snapshot:snap-1").at("chunk_bytes").get<std::size_t>() ==
          MetadataKgTopologyArtifactStore::kMaxStoredValueBytes);
}
