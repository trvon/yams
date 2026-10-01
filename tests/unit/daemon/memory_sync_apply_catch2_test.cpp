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
