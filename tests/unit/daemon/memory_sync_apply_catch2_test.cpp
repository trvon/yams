// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// Apply-cycle behavior of the daemon memory-sync path across several in-process nodes.
//
// Each node is a real ServiceManager with its own content store, knowledge graph, and memory-sync
// writer identity, all replicating through one shared-store backend. Cycles are driven explicitly
// (sync + apply + rate-limited outbound backfill), so convergence is asserted in a bounded number
// of rounds rather than with wall-clock polling.

// pi-lens-ignore: fatal error
#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

#include "../../common/test_helpers_catch2.h"

#include <yams/api/content_store_builder.h>
#include <yams/crypto/hasher.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/VectorIndexCoordinator.h>
#include <yams/daemon/daemon.h>
#include <yams/memory_sync/memory_sync_service.h>
#include <yams/memory_sync/records.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/topology_sync_adapter.h>
#include <yams/storage/storage_backend.h>
#include <yams/vector/vector_database.h>

namespace fs = std::filesystem;
using namespace yams;
using namespace yams::daemon;

namespace {

constexpr std::string_view kCorpus = "apply-deadlock-corpus";
constexpr std::string_view kRelation = "links_to";

std::vector<std::byte> bytes(std::string_view text) {
    std::vector<std::byte> out(text.size());
    std::memcpy(out.data(), text.data(), text.size());
    return out;
}

std::string digest(std::span<const std::byte> data) {
    crypto::SHA256Hasher hasher;
    hasher.init();
    hasher.update(data);
    return hasher.finalize();
}

std::unique_ptr<storage::FilesystemBackend> makeBackend(const fs::path& path) {
    storage::BackendConfig config;
    config.type = "filesystem";
    config.localPath = path;
    auto backend = std::make_unique<storage::FilesystemBackend>();
    REQUIRE(backend->initialize(config).has_value());
    return backend;
}

struct TempRoot {
    TempRoot() {
        path = fs::temp_directory_path() /
               ("yams-memory-sync-apply-" +
                std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())) + "-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(path);
    }
    TempRoot(const TempRoot&) = delete;
    TempRoot& operator=(const TempRoot&) = delete;
    TempRoot(TempRoot&&) = delete;
    TempRoot& operator=(TempRoot&&) = delete;
    ~TempRoot() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    fs::path path;
};

/// One daemon-shaped replica: real ServiceManager apply/backfill code over local stores.
struct MeshNode {
    MeshNode(const fs::path& root, const std::string& nodeId, const fs::path& sharedStore)
        : name(nodeId) {
        const auto home = root / nodeId;
        fs::create_directories(home / "data");
        config.dataDir = home / "data";
        config.socketPath = home / "daemon.sock";
        config.pidFile = home / "daemon.pid";
        config.logFile = home / "daemon.log";
        config.memorySync.enabled = true;
        config.memorySync.nodeId = nodeId;
        config.memorySync.corpusId = std::string(kCorpus);
        config.memorySync.transport = "shared-store";
        config.memorySync.backend = "filesystem";
        manager = std::make_unique<ServiceManager>(config, state, lifecycleFsm);

        auto content = api::ContentStoreBuilder::createDefault(home / "storage");
        REQUIRE(content.has_value());
        manager->__test_setContentStore(
            std::shared_ptr<api::IContentStore>(std::move(content.value())));

        auto graph = metadata::makeSqliteKnowledgeGraphStore((home / "kg.db").string());
        REQUIRE(graph.has_value());
        kgStore = std::shared_ptr<metadata::KnowledgeGraphStore>(std::move(graph.value()));
        manager->__test_setKgStore(kgStore);

        auto sync = std::make_unique<memory_sync::MemorySyncService>(
            makeBackend(sharedStore),
            memory_sync::MemorySyncConfig{nodeId, 60'000, std::string(kCorpus), 1});
        sync_ = sync.get();
        manager->testingSetMemorySyncService(std::move(sync));
    }
    MeshNode(const MeshNode&) = delete;
    MeshNode& operator=(const MeshNode&) = delete;
    MeshNode(MeshNode&&) = delete;
    MeshNode& operator=(MeshNode&&) = delete;
    ~MeshNode() {
        if (sync_ != nullptr) {
            sync_->stop();
        }
        manager.reset();
    }

    std::string sourceKey() const { return "entity:" + name + ":source"; }
    std::string targetKey() const { return "entity:" + name + ":target"; }

    /// Seed a local edge whose target node never reached the shared store: the edge and its
    /// source were published, then publication stopped before the target (the backfill sweeps
    /// node types in order, so an edge can precede its target node of a later type).
    void seedEdgePublishedAheadOfTarget() {
        const metadata::KGNode source{
            .nodeKey = sourceKey(), .label = name + "-source", .type = "alpha"};
        const metadata::KGNode target{
            .nodeKey = targetKey(), .label = name + "-target", .type = "omega"};
        const auto sourceId = kgStore->upsertNode(source);
        const auto targetId = kgStore->upsertNode(target);
        REQUIRE(sourceId.has_value());
        REQUIRE(targetId.has_value());
        const metadata::KGEdge edge{.srcNodeId = sourceId.value(),
                                    .dstNodeId = targetId.value(),
                                    .relation = std::string(kRelation),
                                    .weight = 1.0F};
        REQUIRE(kgStore->addEdge(edge).has_value());

        metadata::TopologySyncAdapter publisher{*kgStore, *sync_};
        REQUIRE(publisher.publishNode(source).has_value());
        REQUIRE(publisher.publishEdge(source.nodeKey, edge, target.nodeKey).has_value());
    }

    /// One production-shaped cycle: inbound apply followed by the outbound backfill.
    void runCycle() {
        manager->testingExpireMemorySyncBackfillSchedule();
        manager->testingApplyMemorySyncWinners();
    }

    bool hasEdge(const std::string& sourceNodeKey, const std::string& targetNodeKey) const {
        const auto source = kgStore->getNodeByKey(sourceNodeKey);
        const auto target = kgStore->getNodeByKey(targetNodeKey);
        REQUIRE(source.has_value());
        REQUIRE(target.has_value());
        if (!source.value() || !target.value()) {
            return false;
        }
        const auto edges = kgStore->getEdgesFrom(source.value()->id, std::string(kRelation));
        REQUIRE(edges.has_value());
        for (const auto& edge : edges.value()) {
            if (edge.dstNodeId == target.value()->id) {
                return true;
            }
        }
        return false;
    }

    bool hasEdgeOf(const MeshNode& origin) const {
        return hasEdge(origin.sourceKey(), origin.targetKey());
    }

    metadata::KGNode addNode(const std::string& key, const std::string& type) const {
        metadata::KGNode node{.nodeKey = key, .label = key, .type = type};
        const auto id = kgStore->upsertNode(node);
        REQUIRE(id.has_value());
        node.id = id.value();
        return node;
    }

    void addEdge(const metadata::KGNode& source, const metadata::KGNode& target) const {
        const metadata::KGEdge edge{.srcNodeId = source.id,
                                    .dstNodeId = target.id,
                                    .relation = std::string(kRelation),
                                    .weight = 1.0F};
        REQUIRE(kgStore->addEdge(edge).has_value());
    }

    /// What a topology rebuild does to a replaced cluster node: delete it, cascading its edges.
    void deleteNode(const metadata::KGNode& node) const {
        REQUIRE(kgStore->deleteNodeById(node.id).has_value());
    }

    bool hasNode(const std::string& key) const {
        const auto node = kgStore->getNodeByKey(key);
        REQUIRE(node.has_value());
        return node.value().has_value();
    }

    /// Drive the outbound backfill alone, one bounded publish cycle per call.
    void backfill(std::size_t cycles) {
        for (std::size_t i = 0; i < cycles; ++i) {
            manager->testingPublishMemorySyncBackfill();
        }
    }

    std::uint64_t deferredTopology() const {
        const auto status = manager->getMemorySyncStatus();
        REQUIRE(status.has_value());
        return status.value().apply.deferredIn(memory_sync::ApplyStage::Topology);
    }

    std::string name;
    yams::test::ScopedEnvVar disableWatcher{"YAMS_DISABLE_SESSION_WATCHER", std::string("1")};
    DaemonConfig config;
    StateComponent state;
    DaemonLifecycleFsm lifecycleFsm;
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore;
    memory_sync::MemorySyncService* sync_{nullptr};
    std::unique_ptr<ServiceManager> manager;
};

} // namespace

TEST_CASE("Memory sync nodes holding each other's missing prerequisites still converge",
          "[daemon][memory-sync][apply][deadlock]") {
    TempRoot root;
    const auto sharedStore = root.path / "shared-store";
    fs::create_directories(sharedStore);

    MeshNode nodeA{root.path, "node-a", sharedStore};
    MeshNode nodeB{root.path, "node-b", sharedStore};
    // Each node holds an inbound edge whose target only the other node can publish.
    nodeA.seedEdgePublishedAheadOfTarget();
    nodeB.seedEdgePublishedAheadOfTarget();

    // Two rounds suffice when publication is independent of inbound apply: round one publishes
    // each missing target, round two applies it. Allow slack, but stay bounded.
    constexpr std::size_t kMaxRounds = 4;
    std::size_t rounds = 0;
    bool converged = false;
    while (!converged && rounds < kMaxRounds) {
        nodeA.runCycle();
        if (rounds == 0) {
            // A ran first, before B published its target: B's edge is deferred, not an error.
            const auto status = nodeA.manager->getMemorySyncStatus();
            REQUIRE(status.has_value());
            CHECK(status.value().apply.deferredIn(memory_sync::ApplyStage::Topology) == 1);
            CHECK(status.value().apply.oldestDeferralAgeMs < 60'000);
            CHECK(status.value().apply.applyFailedCycles == 0);
        }
        nodeB.runCycle();
        ++rounds;
        converged = nodeA.hasEdgeOf(nodeB) && nodeB.hasEdgeOf(nodeA);
    }

    INFO("rounds=" << rounds);
    CHECK(nodeA.hasEdgeOf(nodeB));
    CHECK(nodeB.hasEdgeOf(nodeA));
    CHECK(converged);

    for (const auto* node : {&nodeA, &nodeB}) {
        const auto status = node->manager->getMemorySyncStatus();
        REQUIRE(status.has_value());
        const auto& apply = status.value().apply;
        INFO("node=" << node->name);
        CHECK(apply.applyCycles == rounds);
        CHECK(apply.applyFailedCycles == 0);
        CHECK(apply.deferredTotal() == 0);
        CHECK(apply.oldestDeferralAgeMs == 0);
        CHECK(apply.publishSkippedCycles == 0);
        CHECK(apply.publishFailedCycles == 0);
    }
}

TEST_CASE("Memory sync content apply stores good blobs past a corrupt one",
          "[daemon][memory-sync][apply][content]") {
    TempRoot root;
    const auto sharedStore = root.path / "shared-store";
    fs::create_directories(sharedStore);
    MeshNode writer{root.path, "writer", sharedStore};
    MeshNode reader{root.path, "reader", sharedStore};

    // Blobs apply in key order; put the corrupt one first so it cannot hide the good one.
    const auto first = bytes("content-apply-first");
    const auto second = bytes("content-apply-second");
    const auto firstHash = digest(first);
    const auto secondHash = digest(second);
    const bool firstSortsFirst = firstHash < secondHash;
    const auto& corruptHash = firstSortsFirst ? firstHash : secondHash;
    const auto& goodHash = firstSortsFirst ? secondHash : firstHash;
    const auto& goodBytes = firstSortsFirst ? second : first;
    REQUIRE(
        writer.sync_->publish("content-blob/" + corruptHash, bytes("not-the-bytes")).has_value());
    REQUIRE(writer.sync_->publish("content-blob/" + goodHash, goodBytes).has_value());

    reader.runCycle();

    auto contentStore = reader.manager->getContentStore();
    REQUIRE(contentStore != nullptr);
    const auto good = contentStore->exists(goodHash);
    REQUIRE(good.has_value());
    CHECK(good.value());
    const auto corrupt = contentStore->exists(corruptHash);
    REQUIRE(corrupt.has_value());
    CHECK_FALSE(corrupt.value());

    // The corrupt blob is a genuine failure: it fails the stage and the cycle, visibly.
    const auto status = reader.manager->getMemorySyncStatus();
    REQUIRE(status.has_value());
    const auto& apply = status.value().apply;
    CHECK(apply.failuresIn(memory_sync::ApplyStage::Content) == 1);
    CHECK(apply.applyCycles == 1);
    CHECK(apply.applyFailedCycles == 1);
    CHECK(apply.lastFailureStage == "content");
    CHECK(apply.lastFailure.find("do not match the claimed hash") != std::string::npos);
}

namespace {

std::string topologyNodeKey(std::string_view nodeKey) {
    return "topology-node/" + memory_sync::escapeRecordKeySegment(nodeKey);
}

std::string topologyEdgeKey(std::string_view sourceKey, std::string_view targetKey) {
    memory_sync::TopologyEdgeRecord record;
    record.sourceNodeKey = sourceKey;
    record.relation = kRelation;
    record.targetNodeKey = targetKey;
    return "topology-edge/" + record.id();
}

/// A read-only view of what has been published to the shared store.
struct SharedStoreView {
    explicit SharedStoreView(const fs::path& sharedStore)
        : sync(makeBackend(sharedStore),
               memory_sync::MemorySyncConfig{"observer", 60'000, std::string(kCorpus), 1}) {}

    void refresh() { REQUIRE(sync.syncOnce().has_value()); }
    bool published(const std::string& key) const { return sync.readCached(key).has_value(); }
    bool tombstoned(const std::string& key) const { return sync.hasCommittedTombstone(key); }

    memory_sync::MemorySyncService sync;
};

} // namespace

TEST_CASE("Memory sync topology backfill publishes every node despite deletes between items",
          "[daemon][memory-sync][backfill][topology]") {
    TempRoot root;
    const auto sharedStore = root.path / "shared-store";
    fs::create_directories(sharedStore);
    MeshNode origin{root.path, "origin", sharedStore};
    SharedStoreView view{sharedStore};
    origin.manager->testingSetMemorySyncBackfillItemBudget(1);

    std::vector<metadata::KGNode> previous;
    for (int i = 0; i < 3; ++i) {
        previous.push_back(origin.addNode("topology:cluster:old-" + std::to_string(i), "cluster"));
    }
    origin.backfill(2);

    // A topology rebuild lands between two backfill items: it deletes every cluster node,
    // including the ones the sweep already passed, and inserts the replacements.
    for (const auto& node : previous) {
        origin.deleteNode(node);
    }
    std::vector<std::string> current;
    for (int i = 0; i < 3; ++i) {
        current.push_back(
            origin.addNode("topology:cluster:new-" + std::to_string(i), "cluster").nodeKey);
    }
    origin.backfill(16);

    view.refresh();
    for (const auto& key : current) {
        INFO("node=" << key);
        CHECK(view.published(topologyNodeKey(key)));
    }
}

TEST_CASE("Memory sync topology backfill publishes an edge added after its node was swept",
          "[daemon][memory-sync][backfill][topology]") {
    TempRoot root;
    const auto sharedStore = root.path / "shared-store";
    fs::create_directories(sharedStore);
    MeshNode origin{root.path, "origin", sharedStore};
    MeshNode peer{root.path, "peer", sharedStore};
    SharedStoreView view{sharedStore};
    origin.manager->testingSetMemorySyncBackfillItemBudget(1);

    const auto document = origin.addNode("doc:late-edge-source", "document");
    const auto neighbor = origin.addNode("doc:late-edge-target", "document");
    // Let the sweep pass both nodes (and finish the pass) before the edge exists.
    origin.backfill(8);
    view.refresh();
    REQUIRE(view.published(topologyNodeKey(document.nodeKey)));
    REQUIRE(view.published(topologyNodeKey(neighbor.nodeKey)));

    origin.addEdge(document, neighbor);
    origin.backfill(8);

    view.refresh();
    CHECK(view.published(topologyEdgeKey(document.nodeKey, neighbor.nodeKey)));
    peer.runCycle();
    CHECK(peer.hasEdge(document.nodeKey, neighbor.nodeKey));
    CHECK(peer.deferredTopology() == 0);
}

TEST_CASE("Memory sync retracts a cluster node and its edges when a rebuild deletes them",
          "[daemon][memory-sync][backfill][topology][retraction]") {
    TempRoot root;
    const auto sharedStore = root.path / "shared-store";
    fs::create_directories(sharedStore);
    MeshNode origin{root.path, "origin", sharedStore};
    MeshNode peer{root.path, "peer", sharedStore};
    SharedStoreView view{sharedStore};

    const auto document = origin.addNode("doc:rebuild-member", "document");
    const auto stale = origin.addNode("topology:cluster:stale", "topology_cluster");
    origin.addEdge(document, stale);
    origin.runCycle();
    peer.runCycle();
    REQUIRE(peer.hasEdge(document.nodeKey, stale.nodeKey));

    // The rebuild replaces the cluster: the old node goes (cascading its member edge) and the
    // document joins a new one.
    origin.deleteNode(stale);
    const auto fresh = origin.addNode("topology:cluster:fresh", "topology_cluster");
    origin.addEdge(document, fresh);

    for (int round = 0; round < 3; ++round) {
        origin.runCycle();
        peer.runCycle();
    }

    // The origin's own published records must not resurrect what its rebuild deleted.
    CHECK_FALSE(origin.hasNode(stale.nodeKey));
    view.refresh();
    CHECK(view.tombstoned(topologyNodeKey(stale.nodeKey)));
    CHECK(view.tombstoned(topologyEdgeKey(document.nodeKey, stale.nodeKey)));
    CHECK_FALSE(peer.hasNode(stale.nodeKey));
    CHECK_FALSE(peer.hasEdge(document.nodeKey, stale.nodeKey));
    CHECK(peer.hasEdge(document.nodeKey, fresh.nodeKey));
    CHECK(origin.deferredTopology() == 0);
    CHECK(peer.deferredTopology() == 0);
}

TEST_CASE("Memory sync clears a deferral on an edge whose cluster node was deleted unpublished",
          "[daemon][memory-sync][backfill][topology][retraction]") {
    TempRoot root;
    const auto sharedStore = root.path / "shared-store";
    fs::create_directories(sharedStore);
    MeshNode origin{root.path, "origin", sharedStore};
    MeshNode peer{root.path, "peer", sharedStore};

    // The state the production mesh was left in: the member edge and its document reached the
    // shared store, but the rebuild deleted the cluster node before it was ever published.
    const auto document = origin.addNode("doc:orphaned-member", "document");
    const auto cluster = origin.addNode("topology:cluster:never-published", "topology_cluster");
    origin.addEdge(document, cluster);
    {
        metadata::TopologySyncAdapter publisher{*origin.kgStore, *origin.sync_};
        const auto edges = origin.kgStore->getEdgesFrom(document.id, std::string(kRelation));
        REQUIRE(edges.has_value());
        REQUIRE(edges.value().size() == 1);
        REQUIRE(publisher.publishNode(document).has_value());
        REQUIRE(publisher.publishEdge(document.nodeKey, edges.value().front(), cluster.nodeKey)
                    .has_value());
    }
    origin.deleteNode(cluster);
    peer.runCycle();
    REQUIRE(peer.deferredTopology() == 1);

    for (int round = 0; round < 3; ++round) {
        origin.runCycle();
        peer.runCycle();
    }

    CHECK_FALSE(peer.hasEdge(document.nodeKey, cluster.nodeKey));
    CHECK(origin.deferredTopology() == 0);
    CHECK(peer.deferredTopology() == 0);
}

namespace {

// Pins the vector-store environment for one case. CI runs the unit lanes with vectors
// disabled (YAMS_DISABLE_VECTORS, YAMS_SQLITE_VEC_SKIP_INIT) and the vector DB forced in
// memory; VectorDatabase then skips creating its tables, so these cases must not inherit that.
struct VectorStoreEnv {
    yams::test::ScopedEnvVar disable{"YAMS_DISABLE_VECTORS", std::nullopt};
    yams::test::ScopedEnvVar disableSingular{"YAMS_DISABLE_VECTOR", std::nullopt};
    yams::test::ScopedEnvVar disableDb{"YAMS_DISABLE_VECTOR_DB", std::nullopt};
    yams::test::ScopedEnvVar skipVecInit{"YAMS_SQLITE_VEC_SKIP_INIT", std::nullopt};
    yams::test::ScopedEnvVar inMemory{"YAMS_VDB_IN_MEMORY", std::nullopt};
};

std::shared_ptr<vector::VectorDatabase> attachVectorDb(MeshNode& node) {
    vector::VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 4;
    config.create_if_missing = true;
    config.use_in_memory = true;
    config.search_engine = vector::VectorSearchEngine::ExactScan;
    auto database = std::make_shared<vector::VectorDatabase>(config);
    REQUIRE(database->initializeChecked().has_value());
    // Wire it as vector initialization does: the store, then the index coordinator.
    node.manager->testingSetVectorDatabase(database);
    if (auto coordinator = node.manager->getVectorIndexCoordinator()) {
        coordinator->setVectorDatabase(database);
    }
    REQUIRE(node.manager->getVectorDatabase() == database);
    return database;
}

vector::VectorRecord makeVector(std::string chunkId, char documentDigit, std::string model) {
    vector::VectorRecord record;
    record.chunk_id = std::move(chunkId);
    record.document_hash = std::string(64, documentDigit);
    record.model_id = std::move(model);
    record.embedding = {1.0F, 0.0F, 0.0F, 0.0F};
    record.embedding_dim = 4;
    record.content = "vector backfill content";
    return record;
}

} // namespace

TEST_CASE("Memory sync vector backfill publishes good vectors past an unpublishable one",
          "[daemon][memory-sync][backfill][vector]") {
    const VectorStoreEnv vectorEnv;
    TempRoot root;
    const auto sharedStore = root.path / "shared-store";
    fs::create_directories(sharedStore);
    MeshNode origin{root.path, "origin", sharedStore};
    SharedStoreView view{sharedStore};
    auto vectors = attachVectorDb(origin);

    // The sweep walks (document_hash, chunk_id) order. The first row can never publish: it has
    // no model identity, as rows written before model ids were recorded do not. The rows behind
    // it are good and must still replicate.
    REQUIRE(vectors->insertVectorChecked(makeVector("legacy-chunk", '1', "")).has_value());
    REQUIRE(vectors->insertVectorChecked(makeVector("good-chunk-a", '2', "model")).has_value());
    REQUIRE(vectors->insertVectorChecked(makeVector("good-chunk-b", '3', "model")).has_value());

    origin.backfill(4);

    view.refresh();
    CHECK(view.published("embedding/model/good-chunk-a"));
    CHECK(view.published("embedding/model/good-chunk-b"));

    // A skipped record is not an outbound failure; it is logged once and passed.
    const auto status = origin.manager->getMemorySyncStatus();
    REQUIRE(status.has_value());
    CHECK(status.value().apply.publishFailedCycles == 0);
}

TEST_CASE("Memory sync vector apply stores good embeddings past a bad one",
          "[daemon][memory-sync][apply][vector]") {
    const VectorStoreEnv vectorEnv;
    TempRoot root;
    const auto sharedStore = root.path / "shared-store";
    fs::create_directories(sharedStore);
    MeshNode writer{root.path, "writer", sharedStore};
    MeshNode reader{root.path, "reader", sharedStore};
    auto vectors = attachVectorDb(reader);

    // Both embeddings belong to a document whose bytes replicate in the same cycle.
    const auto document = bytes("vector-apply-document");
    const auto documentHash = digest(document);
    REQUIRE(writer.sync_->publish("content-blob/" + documentHash, document).has_value());

    // Winners apply in key order; the bad record sorts first so it cannot hide the good one.
    memory_sync::EmbeddingRecord bad;
    bad.model = "model";
    bad.chunkId = "a-bad-dimension";
    bad.documentId = documentHash;
    bad.dimensions = 4;
    bad.values = {1.0F, 0.0F, 0.0F};
    memory_sync::EmbeddingRecord good = bad;
    good.chunkId = "b-good";
    good.values = {0.0F, 1.0F, 0.0F, 0.0F};
    REQUIRE(
        writer.sync_->publish("embedding/model/a-bad-dimension", bytes(nlohmann::json(bad).dump()))
            .has_value());
    REQUIRE(writer.sync_->publish("embedding/model/b-good", bytes(nlohmann::json(good).dump()))
                .has_value());

    reader.runCycle();

    CHECK(vectors->getVector("b-good").has_value());
    CHECK_FALSE(vectors->getVector("a-bad-dimension").has_value());

    // The bad record is a genuine failure: it fails the stage and the cycle, visibly, and the
    // stage still scanned every record.
    const auto status = reader.manager->getMemorySyncStatus();
    REQUIRE(status.has_value());
    const auto& apply = status.value().apply;
    CHECK(apply.failuresIn(memory_sync::ApplyStage::Vector) == 1);
    CHECK(apply.deferredIn(memory_sync::ApplyStage::Vector) == 0);
    CHECK(apply.applyCycles == 1);
    CHECK(apply.applyFailedCycles == 1);
    CHECK(apply.lastFailureStage == "vector");
    CHECK(apply.lastFailure.find("embedding/model/a-bad-dimension") != std::string::npos);
}

namespace {

std::string embeddingKey(std::string_view chunkId) {
    return "embedding/model/" + std::string(chunkId);
}

std::vector<float> publishedValues(const SharedStoreView& view, std::string_view chunkId) {
    const auto cached = view.sync.readCached(embeddingKey(chunkId));
    REQUIRE(cached.has_value());
    const std::string text(reinterpret_cast<const char*>(cached.value().data()),
                           cached.value().size());
    return nlohmann::json::parse(text).get<memory_sync::EmbeddingRecord>().values;
}

/// Re-embed a one-chunk document: its vector is replaced by one with new values, as an
/// embedding job does when the document's content or model changes.
void reembed(vector::VectorDatabase& vectors, const std::string& chunkId, char documentDigit) {
    REQUIRE(vectors.deleteVectorsByDocumentChecked(std::string(64, documentDigit)).has_value());
    auto record = makeVector(chunkId, documentDigit, "model");
    record.embedding = {0.0F, 0.0F, 1.0F, 0.0F};
    REQUIRE(vectors.insertVectorChecked(record).has_value());
}

} // namespace

TEST_CASE("Memory sync vector backfill wraps to publish vectors behind its cursor",
          "[daemon][memory-sync][backfill][vector]") {
    const VectorStoreEnv vectorEnv;
    TempRoot root;
    const auto sharedStore = root.path / "shared-store";
    fs::create_directories(sharedStore);
    MeshNode origin{root.path, "origin", sharedStore};
    SharedStoreView view{sharedStore};
    auto vectors = attachVectorDb(origin);
    origin.manager->testingSetMemorySyncBackfillItemBudget(1);

    REQUIRE(vectors->insertVectorChecked(makeVector("swept-early", '2', "model")).has_value());
    REQUIRE(vectors->insertVectorChecked(makeVector("swept-late", '6', "model")).has_value());
    // Let the sweep publish both rows and reach the end of the table.
    origin.backfill(4);
    view.refresh();
    REQUIRE(view.published(embeddingKey("swept-early")));
    REQUIRE(view.published(embeddingKey("swept-late")));

    // Both changes sort below the sweep's last position, (document '6...', "swept-late"), and
    // nothing announces them: only a sweep that starts over can reach them.
    REQUIRE(vectors->insertVectorChecked(makeVector("added-behind", '1', "model")).has_value());
    reembed(*vectors, "swept-early", '2');
    origin.backfill(8);

    view.refresh();
    CHECK(view.published(embeddingKey("added-behind")));
    CHECK(publishedValues(view, "swept-early") == std::vector<float>{0.0F, 0.0F, 1.0F, 0.0F});
}

TEST_CASE("Memory sync publishes committed embeddings without waiting for the vector sweep",
          "[daemon][memory-sync][backfill][vector]") {
    const VectorStoreEnv vectorEnv;
    TempRoot root;
    const auto sharedStore = root.path / "shared-store";
    fs::create_directories(sharedStore);
    MeshNode origin{root.path, "origin", sharedStore};
    SharedStoreView view{sharedStore};
    auto vectors = attachVectorDb(origin);
    origin.manager->testingSetMemorySyncBackfillItemBudget(1);

    // A bulk document sorts first, so a sweep starting over spends many items before it could
    // reach anything after it.
    constexpr int kBulkChunks = 6;
    for (int i = 0; i < kBulkChunks; ++i) {
        REQUIRE(vectors->insertVectorChecked(makeVector("bulk-" + std::to_string(i), '1', "model"))
                    .has_value());
    }
    REQUIRE(vectors->insertVectorChecked(makeVector("reembedded", '5', "model")).has_value());
    origin.backfill(kBulkChunks + 3);
    view.refresh();
    REQUIRE(view.published(embeddingKey("reembedded")));

    // An embedding job commits a new document's vectors and re-embeds an existing one.
    REQUIRE(vectors->insertVectorChecked(makeVector("committed-new", '3', "model")).has_value());
    reembed(*vectors, "reembedded", '5');
    origin.manager->notifyMemorySyncEmbeddingsCommitted(
        {std::string(64, '3'), std::string(64, '5')});

    // One item per committed vector: fewer than the sweep needs to get past the bulk document.
    origin.backfill(2);

    view.refresh();
    CHECK(view.published(embeddingKey("committed-new")));
    CHECK(publishedValues(view, "reembedded") == std::vector<float>{0.0F, 0.0F, 1.0F, 0.0F});
}

TEST_CASE("Memory sync topology backfill keeps progressing while the vector sweep wraps",
          "[daemon][memory-sync][backfill][vector][topology]") {
    const VectorStoreEnv vectorEnv;
    TempRoot root;
    const auto sharedStore = root.path / "shared-store";
    fs::create_directories(sharedStore);
    MeshNode origin{root.path, "origin", sharedStore};
    SharedStoreView view{sharedStore};
    auto vectors = attachVectorDb(origin);
    origin.manager->testingSetMemorySyncBackfillItemBudget(1);

    constexpr int kVectors = 8;
    constexpr int kNodes = 4;
    for (int i = 0; i < kVectors; ++i) {
        REQUIRE(vectors->insertVectorChecked(makeVector("sweep-" + std::to_string(i), '1', "model"))
                    .has_value());
    }
    std::vector<std::string> nodes;
    for (int i = 0; i < kNodes; ++i) {
        nodes.push_back(origin.addNode("doc:fair-" + std::to_string(i), "document").nodeKey);
    }

    // A wrapping vector sweep always has work. These cycles are fewer than the two sweeps need
    // back to back, but enough for each to publish its share when neither can take every item.
    origin.backfill(2 * kNodes + 2);

    view.refresh();
    for (const auto& key : nodes) {
        INFO("node=" << key);
        CHECK(view.published(topologyNodeKey(key)));
    }
    int vectorsPublished = 0;
    for (int i = 0; i < kVectors; ++i) {
        vectorsPublished += view.published(embeddingKey("sweep-" + std::to_string(i))) ? 1 : 0;
    }
    CHECK(vectorsPublished >= kNodes);
}
