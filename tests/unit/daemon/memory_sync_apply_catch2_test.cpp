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
#include <yams/daemon/daemon.h>
#include <yams/memory_sync/memory_sync_service.h>
#include <yams/memory_sync/records.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/topology_sync_adapter.h>
#include <yams/storage/storage_backend.h>

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
