// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_metadata_store.h>

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
