// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>

#include <yams/memory_sync/memory_sync_service.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/topology_sync_adapter.h>
#include <yams/storage/storage_backend.h>

using namespace yams::metadata;
using namespace yams::memory_sync;

namespace {

std::vector<std::byte> bytes(std::string_view text) {
    std::vector<std::byte> result(text.size());
    std::memcpy(result.data(), text.data(), text.size());
    return result;
}

std::filesystem::path tempDbPath(const char* prefix) {
    auto path =
        std::filesystem::temp_directory_path() /
        (std::string(prefix) +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".db");
    std::error_code ec;
    std::filesystem::remove(path, ec);
    return path;
}

struct KGFixture {
    explicit KGFixture(const char* prefix) : dbPath(tempDbPath(prefix)) {
        auto result = makeSqliteKnowledgeGraphStore(dbPath.string());
        REQUIRE(result.has_value());
        store = std::move(result.value());
    }
    ~KGFixture() {
        store.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
        std::filesystem::remove(dbPath.string() + "-wal", ec);
        std::filesystem::remove(dbPath.string() + "-shm", ec);
    }

    std::filesystem::path dbPath;
    std::unique_ptr<KnowledgeGraphStore> store;
};

std::unique_ptr<yams::storage::FilesystemBackend> makeBackend(const std::filesystem::path& path) {
    yams::storage::BackendConfig config;
    config.type = "filesystem";
    config.localPath = path;
    auto backend = std::make_unique<yams::storage::FilesystemBackend>();
    REQUIRE(backend->initialize(config).has_value());
    return backend;
}

struct TempDirGuard {
    TempDirGuard() {
        path = std::filesystem::temp_directory_path() /
               ("yams-topology-sync-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(path);
    }
    ~TempDirGuard() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
    std::filesystem::path path;
};

} // namespace

TEST_CASE("topology sync adapter rejects payload identity mismatches before mutation",
          "[metadata][topology][memory-sync][identity]") {
    TempDirGuard temp;
    KGFixture graph("topology_sync_mismatch_");
    MemorySyncService writer{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MemorySyncService reader{makeBackend(temp.path / "sync"), MemorySyncConfig{"B", 50}};
    TopologySyncAdapter adapter{*graph.store, reader};

    SECTION("node value") {
        TopologyNodeRecord record;
        record.nodeKey = "node:B";
        REQUIRE(writer.publish("topology-node/node%3AA", bytes(nlohmann::json(record).dump()))
                    .has_value());
        const auto applied = adapter.apply();
        REQUIRE_FALSE(applied.has_value());
        CHECK(applied.error().code == yams::ErrorCode::InvalidData);
        const auto unexpected = graph.store->getNodeByKey(record.nodeKey);
        REQUIRE(unexpected.has_value());
        CHECK_FALSE(unexpected.value().has_value());
    }

    SECTION("node tombstone") {
        const KGNode retained{.nodeKey = "node:B", .label = "B", .type = "test"};
        REQUIRE(graph.store->upsertNode(retained).has_value());
        REQUIRE(writer.erase("topology-node/node%3AA", retained.nodeKey).has_value());
        const auto applied = adapter.apply();
        REQUIRE_FALSE(applied.has_value());
        CHECK(applied.error().code == yams::ErrorCode::InvalidData);
        REQUIRE(graph.store->getNodeByKey(retained.nodeKey).value().has_value());
    }

    SECTION("edge value") {
        TopologyEdgeRecord record;
        record.sourceNodeKey = "node:B";
        record.relation = "CALLS";
        record.targetNodeKey = "node:C";
        REQUIRE(writer
                    .publish("topology-edge/node%3AA/CALLS/node%3AC",
                             bytes(nlohmann::json(record).dump()))
                    .has_value());
        const auto applied = adapter.apply();
        REQUIRE_FALSE(applied.has_value());
        CHECK(applied.error().code == yams::ErrorCode::InvalidData);
    }

    SECTION("edge tombstone") {
        TopologyEdgeRecord record;
        record.sourceNodeKey = "node:B";
        record.relation = "CALLS";
        record.targetNodeKey = "node:C";
        REQUIRE(writer.erase("topology-edge/node%3AA/CALLS/node%3AC", nlohmann::json(record).dump())
                    .has_value());
        const auto applied = adapter.apply();
        REQUIRE_FALSE(applied.has_value());
        CHECK(applied.error().code == yams::ErrorCode::InvalidData);
    }
}

TEST_CASE("topology sync adapter makes CALLS and DEFINED_IN edges traversable on B",
          "[metadata][topology][memory-sync]") {
    TempDirGuard temp;
    KGFixture graphA("topology_sync_a_");
    KGFixture graphB("topology_sync_b_");
    MemorySyncService syncA{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MemorySyncService syncB{makeBackend(temp.path / "sync"), MemorySyncConfig{"B", 50}};
    TopologySyncAdapter adapterA{*graphA.store, syncA};
    TopologySyncAdapter adapterB{*graphB.store, syncB};

    const KGNode caller{.nodeKey = "function:caller@src/main.cpp",
                        .label = "caller",
                        .type = "function",
                        .properties = R"({"start_line":10})"};
    const KGNode callee{
        .nodeKey = "function:callee@src/lib.cpp", .label = "callee", .type = "function"};
    const KGNode file{.nodeKey = "file:src/main.cpp", .label = "src/main.cpp", .type = "file"};

    const auto callerId = graphA.store->upsertNode(caller);
    const auto calleeId = graphA.store->upsertNode(callee);
    const auto fileId = graphA.store->upsertNode(file);
    REQUIRE(callerId.has_value());
    REQUIRE(calleeId.has_value());
    REQUIRE(fileId.has_value());

    const KGEdge calls{.srcNodeId = callerId.value(),
                       .dstNodeId = calleeId.value(),
                       .relation = "CALLS",
                       .weight = 0.9F,
                       .properties = R"({"source_file":"src/main.cpp"})"};
    const KGEdge definedIn{.srcNodeId = callerId.value(),
                           .dstNodeId = fileId.value(),
                           .relation = "DEFINED_IN",
                           .weight = 1.0F};
    REQUIRE(graphA.store->addEdgesUnique({calls, definedIn}).has_value());

    REQUIRE(adapterA.publishNode(caller).has_value());
    REQUIRE(adapterA.publishNode(callee).has_value());
    REQUIRE(adapterA.publishNode(file).has_value());
    REQUIRE(adapterA.publishEdge(caller.nodeKey, calls, callee.nodeKey).has_value());
    REQUIRE(adapterA.publishEdge(caller.nodeKey, definedIn, file.nodeKey).has_value());

    auto applied = adapterB.apply();
    REQUIRE(applied.has_value());
    CHECK(applied.value().nodesApplied == 3);
    CHECK(applied.value().edgesApplied == 2);

    const auto callerB = graphB.store->getNodeByKey(caller.nodeKey);
    const auto calleeB = graphB.store->getNodeByKey(callee.nodeKey);
    const auto fileB = graphB.store->getNodeByKey(file.nodeKey);
    REQUIRE(callerB.has_value());
    REQUIRE(callerB.value().has_value());
    REQUIRE(calleeB.has_value());
    REQUIRE(calleeB.value().has_value());
    REQUIRE(fileB.has_value());
    REQUIRE(fileB.value().has_value());
    CHECK(callerB.value()->properties == caller.properties);

    // A selected node winner clears optional properties rather than retaining stale local state.
    auto clearedCaller = caller;
    clearedCaller.properties.reset();
    REQUIRE(adapterA.publishNode(clearedCaller).has_value());
    const auto nodeReplaced = adapterB.apply();
    REQUIRE(nodeReplaced.has_value());
    CHECK(nodeReplaced.value().nodesApplied == 1);
    const auto callerWithoutProperties = graphB.store->getNodeByKey(caller.nodeKey);
    REQUIRE(callerWithoutProperties.has_value());
    REQUIRE(callerWithoutProperties.value().has_value());
    CHECK_FALSE(callerWithoutProperties.value()->properties.has_value());
    const auto nodeUnchanged = adapterB.apply();
    REQUIRE(nodeUnchanged.has_value());
    CHECK(nodeUnchanged.value().nodesApplied == 0);

    const auto callsB = graphB.store->getEdgesFrom(callerB.value()->id, "CALLS");
    REQUIRE(callsB.has_value());
    REQUIRE(callsB.value().size() == 1);
    CHECK(callsB.value().front().dstNodeId == calleeB.value()->id);
    CHECK(callsB.value().front().properties == calls.properties);

    const auto definedInB = graphB.store->getEdgesFrom(callerB.value()->id, "DEFINED_IN");
    REQUIRE(definedInB.has_value());
    REQUIRE(definedInB.value().size() == 1);
    CHECK(definedInB.value().front().dstNodeId == fileB.value()->id);

    // A later winner replaces the exact edge, including lower weights and removed properties.
    auto loweredCalls = calls;
    loweredCalls.weight = 0.2F;
    loweredCalls.properties.reset();
    REQUIRE(adapterA.publishEdge(caller.nodeKey, loweredCalls, callee.nodeKey).has_value());
    const auto replaced = adapterB.apply();
    REQUIRE(replaced.has_value());
    CHECK(replaced.value().edgesApplied == 1);
    const auto replacedCalls = graphB.store->getEdgesFrom(callerB.value()->id, "CALLS");
    REQUIRE(replacedCalls.has_value());
    REQUIRE(replacedCalls.value().size() == 1);
    CHECK(replacedCalls.value().front().weight == 0.2F);
    CHECK_FALSE(replacedCalls.value().front().properties.has_value());

    const auto unchanged = adapterB.apply();
    REQUIRE(unchanged.has_value());
    CHECK(unchanged.value().edgesApplied == 0);
    const auto repeatedCalls = graphB.store->getEdgesFrom(callerB.value()->id, "CALLS");
    REQUIRE(repeatedCalls.has_value());
    CHECK(repeatedCalls.value().size() == 1);

    REQUIRE(adapterA.publishDeleteEdge(caller.nodeKey, "CALLS", callee.nodeKey).has_value());
    REQUIRE(adapterA.publishDeleteNode(callee.nodeKey).has_value());
    const auto deleted = adapterB.apply();
    REQUIRE(deleted.has_value());
    CHECK(deleted.value().edgesDeleted == 1);
    CHECK(deleted.value().nodesDeleted == 1);
    const auto deletedNode = graphB.store->getNodeByKey(callee.nodeKey);
    REQUIRE(deletedNode.has_value());
    CHECK_FALSE(deletedNode.value().has_value());
    const auto callsAfterDelete = graphB.store->getEdgesFrom(callerB.value()->id, "CALLS");
    REQUIRE(callsAfterDelete.has_value());
    CHECK(callsAfterDelete.value().empty());
}

TEST_CASE("topology sync adapter rejects incomplete stable identities",
          "[metadata][topology][memory-sync]") {
    TempDirGuard temp;
    KGFixture graph("topology_sync_identity_");
    MemorySyncService sync{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    TopologySyncAdapter adapter{*graph.store, sync};

    CHECK_FALSE(adapter.publishNode(KGNode{}).has_value());
    const KGEdge edge{.relation = "CALLS"};
    CHECK_FALSE(adapter.publishEdge("", edge, "target").has_value());
}

TEST_CASE("topology sync adapter defers an edge whose endpoint has not replicated yet",
          "[metadata][topology][memory-sync][prerequisite][defer]") {
    TempDirGuard temp;
    KGFixture graphA("topology_sync_defer_a_");
    KGFixture graphB("topology_sync_defer_b_");
    MemorySyncService syncA{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MemorySyncService syncB{makeBackend(temp.path / "sync"), MemorySyncConfig{"B", 50}};
    TopologySyncAdapter adapterA{*graphA.store, syncA};

    const KGNode source{.nodeKey = "entity:source", .label = "source", .type = "alpha"};
    const KGNode present{.nodeKey = "entity:present", .label = "present", .type = "alpha"};
    const KGNode late{.nodeKey = "entity:late", .label = "late", .type = "omega"};
    const auto sourceId = graphA.store->upsertNode(source);
    const auto presentId = graphA.store->upsertNode(present);
    const auto lateId = graphA.store->upsertNode(late);
    REQUIRE(sourceId.has_value());
    REQUIRE(presentId.has_value());
    REQUIRE(lateId.has_value());
    const KGEdge toPresent{
        .srcNodeId = sourceId.value(), .dstNodeId = presentId.value(), .relation = "LINKS"};
    const KGEdge toLate{
        .srcNodeId = sourceId.value(), .dstNodeId = lateId.value(), .relation = "LINKS"};

    // The edge to `late` is replicated before its target node: a publication-order race.
    REQUIRE(adapterA.publishNode(source).has_value());
    REQUIRE(adapterA.publishNode(present).has_value());
    REQUIRE(adapterA.publishEdge(source.nodeKey, toPresent, present.nodeKey).has_value());
    REQUIRE(adapterA.publishEdge(source.nodeKey, toLate, late.nodeKey).has_value());

    const auto edgeTargets = [&](std::string_view targetKey) {
        const auto sourceB = graphB.store->getNodeByKey(source.nodeKey);
        const auto targetB = graphB.store->getNodeByKey(targetKey);
        REQUIRE(sourceB.has_value());
        REQUIRE(targetB.has_value());
        if (!sourceB.value() || !targetB.value()) {
            return false;
        }
        const auto edges = graphB.store->getEdgesFrom(sourceB.value()->id, "LINKS");
        REQUIRE(edges.has_value());
        return std::any_of(edges.value().begin(), edges.value().end(), [&](const KGEdge& edge) {
            return edge.dstNodeId == targetB.value()->id;
        });
    };

    TopologySyncAdapter consumer{*graphB.store, syncB};
    const auto first = consumer.apply();
    REQUIRE(first.has_value());
    CHECK(first.value().nodesApplied == 2);
    CHECK(first.value().edgesApplied == 1);
    CHECK(edgeTargets(present.nodeKey));
    CHECK_FALSE(edgeTargets(late.nodeKey));
    REQUIRE(consumer.deferredKeys().size() == 1);
    CHECK(consumer.deferredKeys().front() == "topology-edge/entity%3Asource/LINKS/entity%3Alate");

    // The deferred edge stays pending and lands once its target node replicates.
    REQUIRE(adapterA.publishNode(late).has_value());
    TopologySyncAdapter retry{*graphB.store, syncB};
    const auto second = retry.apply();
    REQUIRE(second.has_value());
    CHECK(second.value().nodesApplied == 1);
    CHECK(second.value().edgesApplied == 1);
    CHECK(edgeTargets(late.nodeKey));
    CHECK(retry.deferredKeys().empty());
}
