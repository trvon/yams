#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>
#include <yams/daemon/components/TopologyManager.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/topology/topology_metadata_store.h>
#include <yams/vector/vector_database.h>

#include "tests/common/test_helpers_catch2.h"

using namespace yams::daemon;

static TopologyManager::Dependencies nullDeps() {
    return {
        [] { return std::shared_ptr<yams::metadata::MetadataRepository>{}; },
        [] { return std::shared_ptr<yams::metadata::KnowledgeGraphStore>{}; },
        [] { return std::shared_ptr<yams::vector::VectorDatabase>{}; },
    };
}

TEST_CASE("TopologyManager markDirty and hasDirtyHashes", "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    CHECK_FALSE(mgr.hasDirtyHashes());
    mgr.markDirty("hash1");
    CHECK(mgr.hasDirtyHashes());
}

TEST_CASE("TopologyManager markDirty ignores empty strings", "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    mgr.markDirty("");
    CHECK_FALSE(mgr.hasDirtyHashes());
}

TEST_CASE("TopologyManager markDirtyBatch inserts multiple hashes",
          "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    mgr.markDirtyBatch({"a", "b", "", "c"});
    auto overlay = mgr.getOverlayHashes(100);
    CHECK(overlay.size() == 3);
}

TEST_CASE("TopologyManager getOverlayHashes respects limit", "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    mgr.markDirtyBatch({"a", "b", "c", "d", "e"});
    auto limited = mgr.getOverlayHashes(2);
    CHECK(limited.size() == 2);
}

TEST_CASE("TopologyManager drainDirtyHashes clears state", "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    mgr.markDirtyBatch({"x", "y", "z"});
    auto drained = mgr.drainDirtyHashes();
    CHECK(drained.size() == 3);
    CHECK_FALSE(mgr.hasDirtyHashes());
}

TEST_CASE("TopologyManager restoreDirtyHashes re-inserts", "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    mgr.markDirty("a");
    auto drained = mgr.drainDirtyHashes();
    CHECK_FALSE(mgr.hasDirtyHashes());

    mgr.restoreDirtyHashes(drained);
    CHECK(mgr.hasDirtyHashes());
    CHECK(mgr.getOverlayHashes().size() == 1);
}

TEST_CASE("TopologyManager tryScheduleRebuild is one-shot", "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    CHECK(mgr.tryScheduleRebuild());
    CHECK_FALSE(mgr.tryScheduleRebuild());

    mgr.clearScheduled();
    CHECK(mgr.tryScheduleRebuild());
}

TEST_CASE("TopologyManager initial state", "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    CHECK_FALSE(mgr.isRebuildInProgress());
    CHECK(mgr.publishedEpoch() == 0);

    auto telemetry = mgr.getTelemetrySnapshot();
    CHECK_FALSE(telemetry.rebuildRunning);
    CHECK(telemetry.rebuildsTotal == 0);
    CHECK(telemetry.dirtyDocumentCount == 0);
}

TEST_CASE("TopologyManager telemetry tracks dirty count", "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    mgr.markDirtyBatch({"a", "b"});
    auto telemetry = mgr.getTelemetrySnapshot();
    CHECK(telemetry.dirtyDocumentCount == 2);
    CHECK(telemetry.dirtySinceUnixMillis > 0);
}

TEST_CASE("TopologyManager rebuildArtifacts fails without deps",
          "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    auto result = mgr.rebuildArtifacts("test", false, {"hash1"}, "connected");
    CHECK_FALSE(result.has_value());

    auto telemetry = mgr.getTelemetrySnapshot();
    CHECK(telemetry.rebuildsTotal == 1);
    CHECK(telemetry.rebuildFailuresTotal == 1);
    CHECK_FALSE(telemetry.rebuildRunning);
}

TEST_CASE("TopologyManager rebuildArtifacts concurrency guard",
          "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    std::atomic<bool> firstStarted{false};
    std::atomic<bool> secondDone{false};
    TopologyManager::Dependencies slowDeps;
    slowDeps.getMetadataRepo = [&]() -> std::shared_ptr<yams::metadata::MetadataRepository> {
        firstStarted.store(true);
        while (!secondDone.load(std::memory_order_relaxed))
            std::this_thread::yield();
        return nullptr;
    };
    slowDeps.getKgStore = nullDeps().getKgStore;
    slowDeps.getVectorDatabase = nullDeps().getVectorDatabase;

    TopologyManager slowMgr(std::move(slowDeps));
    std::thread t1([&] { slowMgr.rebuildArtifacts("first", false, {"h"}, "connected"); });

    while (!firstStarted.load(std::memory_order_relaxed))
        std::this_thread::yield();

    auto result = slowMgr.rebuildArtifacts("second", false, {"h"}, "connected");
    CHECK(result.has_value());
    CHECK(result.value().skipped);
    CHECK(result.value().issues.size() == 1);

    secondDone.store(true);
    t1.join();
}

namespace {

struct RebuildFixture {
    yams::test::ScopedEnvVar enableVectors{"YAMS_DISABLE_VECTORS", std::string{"0"}};
    yams::test::ScopedEnvVar initializeVectors{"YAMS_SQLITE_VEC_SKIP_INIT", std::string{"0"}};
    yams::test::ScopedEnvVar vectorMemoryMode{"YAMS_VDB_IN_MEMORY", std::nullopt};
    yams::test::TempDirGuard directory{"topology_rebuild_"};
    std::unique_ptr<yams::metadata::ConnectionPool> pool;
    std::shared_ptr<yams::metadata::MetadataRepository> repository;
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kg;
    std::shared_ptr<yams::vector::VectorDatabase> vectors;

    RebuildFixture() {
        yams::metadata::ConnectionPoolConfig poolConfig;
        poolConfig.minConnections = 1;
        poolConfig.maxConnections = 2;
        pool = std::make_unique<yams::metadata::ConnectionPool>(
            (directory.path() / "metadata.db").string(), poolConfig);
        REQUIRE(pool->initialize().has_value());
        repository = std::make_shared<yams::metadata::MetadataRepository>(*pool);
        auto kgResult = yams::metadata::makeSqliteKnowledgeGraphStore(
            *pool, yams::metadata::KnowledgeGraphStoreConfig{});
        REQUIRE(kgResult.has_value());
        kg = std::shared_ptr<yams::metadata::KnowledgeGraphStore>(std::move(kgResult.value()));

        yams::vector::VectorDatabaseConfig vectorConfig;
        vectorConfig.database_path = ":memory:";
        vectorConfig.embedding_dim = 4;
        vectorConfig.create_if_missing = true;
        vectorConfig.use_in_memory = true;
        vectors = std::make_shared<yams::vector::VectorDatabase>(vectorConfig);
        REQUIRE(vectors->initialize());
    }

    std::int64_t addDocument(const std::string& hash) {
        yams::metadata::DocumentInfo document;
        document.fileName = hash + ".md";
        document.filePath = (directory.path() / document.fileName).string();
        document.sha256Hash = hash;
        document.mimeType = "text/plain";
        document.contentExtracted = true;
        document.extractionStatus = yams::metadata::ExtractionStatus::Success;
        auto inserted = repository->insertDocument(document);
        REQUIRE(inserted.has_value());

        yams::metadata::KGNode node;
        node.nodeKey = "doc:" + hash;
        node.label = hash;
        node.type = "document";
        REQUIRE(kg->upsertNode(node).has_value());

        yams::vector::VectorRecord vector;
        vector.chunk_id = "doc-" + hash;
        vector.document_hash = hash;
        vector.embedding = {1.0F, 0.0F, 0.0F, 0.0F};
        vector.model_id = "test-space-v1";
        vector.level = yams::vector::EmbeddingLevel::DOCUMENT;
        REQUIRE(vectors->insertVector(vector));
        return inserted.value();
    }

    TopologyManager::Dependencies dependencies() {
        return {[this] { return repository; }, [this] { return kg; }, [this] { return vectors; }};
    }

    yams::topology::TopologyArtifactBatch latest() {
        // A fresh store must observe the persisted pointer, not an earlier local cache.
        yams::topology::MetadataKgTopologyArtifactStore store(repository, kg);
        auto loaded = store.loadLatest();
        REQUIRE(loaded.has_value());
        REQUIRE(loaded.value().has_value());
        return std::move(*loaded.value());
    }
};

} // namespace

TEST_CASE("TopologyManager does not reuse deleted prior members", "[daemon][topology-recovery]") {
    RebuildFixture fixture;
    const auto removedId = fixture.addDocument("aaa");
    fixture.addDocument("bbb");
    fixture.addDocument("ccc");
    // The removed document shares a cluster with a live member. Recovery must rebuild
    // representatives and retain that live member, not merely discard the whole cluster.
    auto aNode = fixture.kg->getNodeByKey("doc:aaa");
    auto bNode = fixture.kg->getNodeByKey("doc:bbb");
    REQUIRE(aNode.has_value());
    REQUIRE(aNode.value().has_value());
    REQUIRE(bNode.has_value());
    REQUIRE(bNode.value().has_value());
    yams::metadata::KGEdge forward;
    forward.srcNodeId = aNode.value()->id;
    forward.dstNodeId = bNode.value()->id;
    forward.relation = "semantic_neighbor";
    auto reverse = forward;
    std::swap(reverse.srcNodeId, reverse.dstNodeId);
    REQUIRE(fixture.kg->addEdgesUnique(std::vector{forward, reverse}).has_value());
    TopologyManager manager(fixture.dependencies());
    auto initial = manager.rebuildArtifacts("initial", false, {}, "connected");
    REQUIRE(initial.has_value());
    REQUIRE(initial.value().stored);
    const auto previous = fixture.latest();
    REQUIRE(previous.memberships.size() == 3);
    REQUIRE(previous.clusters.size() == 2);

    bool deleted = true;
    bool dryRun = false;
    std::vector<std::string> seeds{"ccc"};
    SECTION("healthy incremental rebuild retains untouched live members") {
        deleted = false;
    }
    SECTION("deleted member is outside the requested dirty region") {}
    SECTION("the only dirty seed was deleted") {
        seeds = {"aaa"};
    }
    SECTION("dry run detects stale reuse without publishing") {
        dryRun = true;
    }

    if (deleted) {
        REQUIRE(fixture.repository->deleteDocument(removedId).has_value());
        // Stale vector/KG state must not resurrect a document absent from metadata.
        REQUIRE_FALSE(fixture.vectors->getVectorsByDocument("aaa").empty());
    }
    // Work not included in this request must survive even if recovery needs a full rebuild.
    manager.markDirty("new-pending");
    auto result = manager.rebuildArtifacts("post_ingest_drain", dryRun, seeds, "connected");
    INFO((result ? "rebuild succeeded" : result.error().message));
    REQUIRE(result.has_value());
    CHECK_FALSE(result.value().skipped);
    CHECK(result.value().stored == !dryRun);
    CHECK(result.value().fullRebuild == deleted);
    CHECK(result.value().fallbackFullRebuilds == (deleted ? 1 : 0));
    CHECK(result.value().membershipsBuilt == (deleted ? 2 : 3));
    CHECK(manager.getOverlayHashes() == std::vector<std::string>{"new-pending"});

    const auto current = fixture.latest();
    CHECK(current.topologyEpoch == previous.topologyEpoch + (dryRun ? 0 : 1));
    CHECK(manager.publishedEpoch() == current.topologyEpoch);
    if (dryRun) {
        CHECK(current.snapshotId == previous.snapshotId);
        CHECK(current.memberships.size() == previous.memberships.size());
    } else {
        const std::unordered_set<std::string> expected =
            deleted ? std::unordered_set<std::string>{"bbb", "ccc"}
                    : std::unordered_set<std::string>{"aaa", "bbb", "ccc"};
        std::unordered_set<std::string> members;
        for (const auto& membership : current.memberships) {
            members.insert(membership.documentHash);
        }
        CHECK(members == expected);
        CHECK(current.clusters.size() == 2);
        for (const auto& cluster : current.clusters) {
            REQUIRE(cluster.medoid.has_value());
            CHECK(expected.contains(cluster.medoid->documentHash));
            for (const auto& representative : cluster.routingRepresentatives) {
                CHECK(expected.contains(representative.documentHash));
            }
            CHECK(cluster.memberCount == cluster.memberDocumentHashes.size());
            for (const auto& hash : cluster.memberDocumentHashes) {
                CHECK(expected.contains(hash));
            }
        }
    }
}

TEST_CASE("TopologyManager distinguishes an empty corpus from unready documents",
          "[daemon][topology-recovery]") {
    RebuildFixture fixture;
    const auto removedId = fixture.addDocument("aaa");
    const auto otherId = fixture.addDocument("bbb");
    TopologyManager manager(fixture.dependencies());
    auto initial = manager.rebuildArtifacts("initial", false, {}, "connected");
    REQUIRE(initial.has_value());
    REQUIRE(initial.value().stored);
    const auto previous = fixture.latest();
    REQUIRE(fixture.repository->deleteDocument(removedId).has_value());

    bool emptyCorpus = false;
    SECTION("all documents were deleted") {
        REQUIRE(fixture.repository->deleteDocument(otherId).has_value());
        emptyCorpus = true;
    }
    SECTION("a remaining document is temporarily missing embeddings") {
        REQUIRE(fixture.vectors->deleteVectorsByDocument("bbb"));
    }

    manager.markDirty("new-pending");
    auto result = manager.rebuildArtifacts("post_ingest_drain", false, {"aaa"}, "connected");
    INFO((result ? "rebuild succeeded" : result.error().message));
    REQUIRE(result.has_value());
    CHECK(result.value().skipped == !emptyCorpus);
    CHECK(result.value().stored == emptyCorpus);
    CHECK(result.value().fullRebuild);
    CHECK(result.value().fallbackFullRebuilds == 1);
    CHECK(manager.getOverlayHashes() == std::vector<std::string>{"new-pending"});
    const auto current = fixture.latest();
    CHECK(manager.publishedEpoch() == current.topologyEpoch);
    if (emptyCorpus) {
        CHECK(current.topologyEpoch == previous.topologyEpoch + 1);
        CHECK(current.memberships.empty());
        CHECK(current.clusters.empty());
    } else {
        CHECK(result.value().documentsMissingEmbeddings == 1);
        CHECK(current.snapshotId == previous.snapshotId);
        CHECK(current.topologyEpoch == previous.topologyEpoch);
        CHECK(current.memberships.size() == previous.memberships.size());
    }
}

TEST_CASE("Topology publication rejects deletion after a successful membership check",
          "[daemon][topology-recovery]") {
    RebuildFixture fixture;
    const auto liveId = fixture.addDocument("aaa");
    const auto removedId = fixture.addDocument("bbb");
    TopologyManager manager(fixture.dependencies());
    auto initial = manager.rebuildArtifacts("initial", false, {}, "connected");
    REQUIRE(initial.has_value());
    REQUIRE(initial.value().stored);
    const auto previous = fixture.latest();

    auto existing = fixture.repository->getExistingDocumentHashes({"aaa", "bbb"});
    REQUIRE(existing.has_value());
    REQUIRE(existing.value().size() == 2);
    REQUIRE(fixture.repository->deleteDocument(removedId).has_value());

    auto candidate = previous;
    candidate.snapshotId = "candidate-after-deletion";
    ++candidate.topologyEpoch;
    std::sort(
        candidate.memberships.begin(), candidate.memberships.end(),
        [](const auto& left, const auto& right) { return left.documentHash < right.documentHash; });
    yams::topology::MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kg);
    const auto result = store.storeBatch(candidate);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == yams::ErrorCode::NotFound);
    const auto current = fixture.latest();
    CHECK(current.snapshotId == previous.snapshotId);
    CHECK(current.topologyEpoch == previous.topologyEpoch);
    CHECK(current.memberships.size() == previous.memberships.size());
    const auto liveSnapshot = fixture.repository->getMetadata(liveId, "topology.snapshot_id");
    REQUIRE(liveSnapshot.has_value());
    REQUIRE(liveSnapshot.value().has_value());
    CHECK(liveSnapshot.value()->asString() == previous.snapshotId);
}
