// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <yams/memory_sync/memory_sync_service.h>
#include <yams/metadata/connection_pool.h>
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

TEST_CASE("topology retraction checks local presence without hydrating graph payloads",
          "[metadata][topology][memory-sync][local-presence-bounded]") {
    bool edge = false;
    SECTION("node presence does not select its properties") {
        edge = false;
    }
    SECTION("edge presence does not select endpoint or edge properties") {
        edge = true;
    }

    TempDirGuard temp;
    KGFixture graph("topology_presence_bounded_");
    MemorySyncConfig config{"A", 60'000};
    config.exclusiveBackend = true;
    MemorySyncService sync{makeBackend(temp.path / "sync"), config};
    TopologySyncAdapter adapter{*graph.store, sync};
    const auto properties = nlohmann::json{{"data", std::string(8192, 'p')}}.dump();
    if (edge) {
        REQUIRE(adapter.publishEdge("source", KGEdge{.relation = "RELATED"}, "target").has_value());
        auto source =
            graph.store->upsertNode(KGNode{.nodeKey = "source", .properties = properties});
        auto target =
            graph.store->upsertNode(KGNode{.nodeKey = "target", .properties = properties});
        REQUIRE(source.has_value());
        REQUIRE(target.has_value());
        REQUIRE(graph.store
                    ->addEdges({KGEdge{.srcNodeId = source.value(),
                                       .dstNodeId = target.value(),
                                       .relation = "RELATED",
                                       .properties = properties}})
                    .has_value());
    } else {
        REQUIRE(adapter.publishNode(KGNode{.nodeKey = "present", .type = "test"}).has_value());
        REQUIRE(
            graph.store
                ->upsertNode(KGNode{.nodeKey = "present", .type = "test", .properties = properties})
                .has_value());
    }
    const auto own = sync.committedWinnersAfter("topology-", "", 1);
    REQUIRE(own.size() == 1);
    REQUIRE(own.front().second.origin == "A");
    REQUIRE_FALSE(own.front().second.isTombstone());
    const auto presentNode = graph.store->hasNodeByKey(edge ? "source" : "present");
    const auto absentNode = graph.store->hasNodeByKey("absent");
    const auto absentRelation = graph.store->hasEdgeByKeys("source", "target", "OTHER");
    const auto reverseEdge = graph.store->hasEdgeByKeys("target", "source", "RELATED");
    REQUIRE(presentNode.has_value());
    REQUIRE(absentNode.has_value());
    REQUIRE(absentRelation.has_value());
    REQUIRE(reverseEdge.has_value());
    CHECK(presentNode.value());
    CHECK_FALSE(absentNode.value());
    CHECK_FALSE(absentRelation.value());
    CHECK_FALSE(reverseEdge.value());

    struct TraceState {
        std::atomic<std::size_t> rows{0};
        std::atomic<std::size_t> textBytes{0};
        std::atomic<std::size_t> deniedReads{0};
    } traceState;
    ConnectionPoolConfig readConfig;
    readConfig.minConnections = 1;
    readConfig.maxConnections = 1;
    ConnectionPool readPool(graph.dbPath.string(), readConfig);
    REQUIRE(readPool.initialize().has_value());
    graph.store->setReadPool(&readPool);
    struct ReadPoolReset {
        KnowledgeGraphStore& store;
        ~ReadPoolReset() { store.setReadPool(nullptr); }
    } reset{*graph.store};
    const auto trace = +[](unsigned event, void* context, void* statement, void*) -> int {
        if (event != SQLITE_TRACE_ROW) {
            return 0;
        }
        auto* stmt = static_cast<sqlite3_stmt*>(statement);
        const auto* sql = sqlite3_sql(stmt);
        if (sql == nullptr || std::string_view(sql).find("FROM kg_") == std::string_view::npos) {
            return 0;
        }
        auto& state = *static_cast<TraceState*>(context);
        state.rows.fetch_add(1, std::memory_order_relaxed);
        for (int column = 0; column < sqlite3_column_count(stmt); ++column) {
            const auto type = sqlite3_column_type(stmt, column);
            if (type == SQLITE_TEXT || type == SQLITE_BLOB) {
                state.textBytes.fetch_add(sqlite3_column_bytes(stmt, column),
                                          std::memory_order_relaxed);
            }
        }
        return 0;
    };
    auto installed = readPool.withConnection([&](Database& db) -> yams::Result<void> {
        REQUIRE(sqlite3_trace_v2(db.rawHandle(), SQLITE_TRACE_ROW, trace, &traceState) ==
                SQLITE_OK);
        return {};
    });
    REQUIRE(installed.has_value());
    const auto retracted = adapter.retractIfLocallyAbsent(own.front().first, own.front().second);
    REQUIRE(retracted.has_value());
    CHECK_FALSE(retracted.value());
    REQUIRE(traceState.rows.load(std::memory_order_relaxed) > 0);
    CHECK(traceState.textBytes.load(std::memory_order_relaxed) == 0);
    const auto denyGraphRead = +[](void* context, int action, const char* table, const char*,
                                   const char*, const char*) {
        if (action == SQLITE_READ && table != nullptr &&
            std::string_view(table).starts_with("kg_")) {
            static_cast<TraceState*>(context)->deniedReads.fetch_add(1, std::memory_order_relaxed);
            return SQLITE_DENY;
        }
        return SQLITE_OK;
    };
    auto denied = readPool.withConnection([&](Database& db) -> yams::Result<void> {
        REQUIRE(sqlite3_set_authorizer(db.rawHandle(), denyGraphRead, &traceState) == SQLITE_OK);
        return {};
    });
    REQUIRE(denied.has_value());
    const auto expectedFailure = graph.store->hasNodeByKey(edge ? "source" : "present");
    REQUIRE_FALSE(expectedFailure.has_value());
    const auto beforeFailure = sync.currentVersion();
    const auto failedRetract =
        adapter.retractIfLocallyAbsent(own.front().first, own.front().second);
    REQUIRE_FALSE(failedRetract.has_value());
    CHECK(failedRetract.error().code == expectedFailure.error().code);
    CHECK(failedRetract.error().code != yams::ErrorCode::ResourceBusy);
    CHECK(failedRetract.error().code != yams::ErrorCode::NotFound);
    CHECK(sync.currentVersion() == beforeFailure);
    CHECK(traceState.deniedReads.load(std::memory_order_relaxed) > 0);
    const auto after = sync.committedWinnersAfter("topology-", "", 1);
    REQUIRE(after.size() == 1);
    CHECK_FALSE(after.front().second.isTombstone());
}

TEST_CASE("topology write batch isolates pending nodes and edges and rolls back as one unit",
          "[metadata][topology][memory-sync][batch-visibility]") {
    bool commit = false;
    SECTION("commit publishes all replacements and deletions") {
        commit = true;
    }
    SECTION("scope exit without commit restores the original graph") {
        commit = false;
    }
    KGFixture graph("topology_batch_visibility_");
    const KGNode original{.nodeKey = "kept", .label = "original", .type = "test"};
    auto originalId = graph.store->upsertNode(original);
    REQUIRE(originalId.has_value());
    std::int64_t sourceId = 0;
    std::int64_t targetId = 0;
    {
        auto opened = graph.store->beginWriteBatch();
        REQUIRE(opened.has_value());
        auto batch = std::move(opened).value();
        auto source = batch->replaceNodeExact(
            KGNode{.nodeKey = "pending:source", .label = "source", .type = "test"});
        auto target = batch->replaceNodeExact(
            KGNode{.nodeKey = "pending:target", .label = "target", .type = "test"});
        REQUIRE(source.has_value());
        REQUIRE(target.has_value());
        sourceId = source.value();
        targetId = target.value();
        REQUIRE(sourceId > 0);
        REQUIRE(targetId > 0);
        auto edge = batch->addEdge(KGEdge{
            .srcNodeId = sourceId, .dstNodeId = targetId, .relation = "LINKS", .weight = 0.5F});
        REQUIRE(edge.has_value());
        REQUIRE(batch->deleteNodeById(originalId.value()).has_value());
        // Ordinary reads use a different pooled connection. Numeric IDs returned by the
        // batch are usable for its writes, but do not make pending nodes visible here.
        auto pendingSource = graph.store->getNodeByKey("pending:source");
        auto pendingTarget = graph.store->getNodeByKey("pending:target");
        auto pendingEdges = graph.store->getEdgesBetween(sourceId, targetId, "LINKS");
        auto retained = graph.store->getNodeByKey("kept");
        REQUIRE(pendingSource.has_value());
        REQUIRE(pendingTarget.has_value());
        REQUIRE(pendingEdges.has_value());
        REQUIRE(retained.has_value());
        CHECK_FALSE(pendingSource.value().has_value());
        CHECK_FALSE(pendingTarget.value().has_value());
        CHECK(pendingEdges.value().empty());
        const auto pendingNodePresent = graph.store->hasNodeByKey("pending:source");
        const auto pendingEdgePresent =
            graph.store->hasEdgeByKeys("pending:source", "pending:target", "LINKS");
        const auto retainedPresent = graph.store->hasNodeByKey("kept");
        REQUIRE(pendingNodePresent.has_value());
        REQUIRE(pendingEdgePresent.has_value());
        REQUIRE(retainedPresent.has_value());
        CHECK_FALSE(pendingNodePresent.value());
        CHECK_FALSE(pendingEdgePresent.value());
        CHECK(retainedPresent.value());
        REQUIRE(retained.value().has_value());
        CHECK(retained.value()->label == original.label);
        if (commit)
            REQUIRE(batch->commit().has_value());
    }
    auto source = graph.store->getNodeByKey("pending:source");
    auto target = graph.store->getNodeByKey("pending:target");
    auto edges = graph.store->getEdgesBetween(sourceId, targetId, "LINKS");
    auto retained = graph.store->getNodeByKey("kept");
    const auto nodePresent = graph.store->hasNodeByKey("pending:source");
    const auto edgePresent =
        graph.store->hasEdgeByKeys("pending:source", "pending:target", "LINKS");
    const auto keptPresent = graph.store->hasNodeByKey("kept");
    REQUIRE(nodePresent.has_value());
    REQUIRE(edgePresent.has_value());
    REQUIRE(keptPresent.has_value());
    CHECK(nodePresent.value() == commit);
    CHECK(edgePresent.value() == commit);
    CHECK(keptPresent.value() == !commit);
    REQUIRE(source.has_value());
    REQUIRE(target.has_value());
    REQUIRE(edges.has_value());
    REQUIRE(retained.has_value());
    if (commit) {
        REQUIRE(source.value().has_value());
        REQUIRE(target.value().has_value());
        CHECK(source.value()->id == sourceId);
        CHECK(target.value()->id == targetId);
        REQUIRE(edges.value().size() == 1);
        CHECK(edges.value().front().weight == 0.5F);
        CHECK_FALSE(retained.value().has_value());
    } else {
        CHECK_FALSE(source.value().has_value());
        CHECK_FALSE(target.value().has_value());
        CHECK(edges.value().empty());
        REQUIRE(retained.value().has_value());
        CHECK(retained.value()->id == originalId.value());
        CHECK(retained.value()->label == original.label);
    }
}

TEST_CASE("topology batch node reads observe pending replacements and deletions",
          "[metadata][topology][memory-sync][batch-read]") {
    bool commit = false;
    SECTION("commit publishes the read state") {
        commit = true;
    }
    SECTION("rollback restores the original state") {
        commit = false;
    }
    KGFixture graph("topology_batch_node_read_");
    const KGNode original{.nodeKey = "kept",
                          .label = "original",
                          .type = "test",
                          .createdTime = 17,
                          .updatedTime = 19,
                          .properties = "{\"old\":true}"};
    auto originalId = graph.store->upsertNode(original);
    REQUIRE(originalId.has_value());
    {
        auto opened = graph.store->beginWriteBatch();
        REQUIRE(opened.has_value());
        auto batch = std::move(opened).value();
        auto initial = batch->getNodeByKey("kept");
        REQUIRE(initial.has_value());
        REQUIRE(initial.value().has_value());
        CHECK(initial.value()->id == originalId.value());
        CHECK(initial.value()->label == original.label);
        CHECK(initial.value()->type == original.type);
        CHECK(initial.value()->createdTime == original.createdTime);
        CHECK(initial.value()->updatedTime == original.updatedTime);
        CHECK(initial.value()->properties == original.properties);

        auto replaced = batch->replaceNodeExact(KGNode{.nodeKey = "kept"});
        auto inserted = batch->replaceNodeExact(KGNode{.nodeKey = "pending", .label = "new"});
        REQUIRE(replaced.has_value());
        REQUIRE(inserted.has_value());
        auto current = batch->getNodeByKey("kept");
        auto pending = batch->getNodeByKey("pending");
        auto missing = batch->getNodeByKey("absent");
        REQUIRE(current.has_value());
        REQUIRE(pending.has_value());
        REQUIRE(missing.has_value());
        REQUIRE(current.value().has_value());
        REQUIRE(pending.value().has_value());
        CHECK(current.value()->id == originalId.value());
        CHECK_FALSE(current.value()->label.has_value());
        CHECK_FALSE(current.value()->type.has_value());
        CHECK_FALSE(current.value()->createdTime.has_value());
        CHECK_FALSE(current.value()->updatedTime.has_value());
        CHECK_FALSE(current.value()->properties.has_value());
        CHECK(pending.value()->id == inserted.value());
        CHECK(pending.value()->label == "new");
        CHECK_FALSE(missing.value().has_value());
        REQUIRE(batch->deleteNodeById(inserted.value()).has_value());
        auto deleted = batch->getNodeByKey("pending");
        REQUIRE(deleted.has_value());
        CHECK_FALSE(deleted.value().has_value());

        auto outside = graph.store->getNodeByKey("kept");
        REQUIRE(outside.has_value());
        REQUIRE(outside.value().has_value());
        CHECK(outside.value()->label == original.label);
        if (commit) {
            REQUIRE(batch->commit().has_value());
            auto ended = batch->getNodeByKey("kept");
            REQUIRE_FALSE(ended.has_value());
            CHECK(ended.error().code == yams::ErrorCode::InvalidState);
        }
    }
    auto retained = graph.store->getNodeByKey("kept");
    auto removed = graph.store->getNodeByKey("pending");
    REQUIRE(retained.has_value());
    REQUIRE(retained.value().has_value());
    REQUIRE(removed.has_value());
    CHECK_FALSE(removed.value().has_value());
    CHECK(retained.value()->id == originalId.value());
    CHECK(retained.value()->label == (commit ? std::optional<std::string>{} : original.label));
    CHECK(retained.value()->properties ==
          (commit ? std::optional<std::string>{} : original.properties));
}

TEST_CASE("topology batch edge reads are bounded ordered and transaction visible",
          "[metadata][topology][memory-sync][batch-read]") {
    bool commit = false;
    SECTION("commit publishes the paged state") {
        commit = true;
    }
    SECTION("rollback removes every pending page") {
        commit = false;
    }
    KGFixture graph("topology_batch_edge_read_");
    std::int64_t sourceId = 0;
    std::int64_t targetId = 0;
    {
        auto opened = graph.store->beginWriteBatch();
        REQUIRE(opened.has_value());
        auto batch = std::move(opened).value();
        auto source = batch->replaceNodeExact(KGNode{.nodeKey = "source"});
        auto target = batch->replaceNodeExact(KGNode{.nodeKey = "target"});
        REQUIRE(source.has_value());
        REQUIRE(target.has_value());
        sourceId = source.value();
        targetId = target.value();
        constexpr auto pageSize = KnowledgeGraphStore::WriteBatch::kEdgePageSize;
        for (std::size_t i = 0; i < pageSize + 3; ++i) {
            auto edge = batch->addEdge(KGEdge{.srcNodeId = sourceId,
                                              .dstNodeId = targetId,
                                              .relation = "LINK_" + std::to_string(i),
                                              .weight = 0.5F,
                                              .createdTime = 23,
                                              .properties = "{\"pending\":true}"});
            // Opposite-direction edges leave ID gaps in the requested traversal.
            auto other = batch->addEdge(KGEdge{.srcNodeId = targetId,
                                               .dstNodeId = sourceId,
                                               .relation = "OTHER_" + std::to_string(i)});
            REQUIRE(edge.has_value());
            REQUIRE(other.has_value());
        }
        auto first = batch->getEdgesBetweenPage(sourceId, targetId);
        REQUIRE(first.has_value());
        REQUIRE(first.value().size() == pageSize);
        std::int64_t cursor = 0;
        for (const auto& edge : first.value()) {
            CHECK(edge.id > cursor);
            CHECK(edge.srcNodeId == sourceId);
            CHECK(edge.dstNodeId == targetId);
            CHECK(edge.weight == 0.5F);
            CHECK(edge.createdTime == 23);
            CHECK(edge.properties == "{\"pending\":true}");
            cursor = edge.id;
            REQUIRE(batch->removeEdgeById(edge.id).has_value());
        }
        auto rest = batch->getEdgesBetweenPage(sourceId, targetId, std::nullopt, cursor);
        REQUIRE(rest.has_value());
        REQUIRE(rest.value().size() == 3);
        for (const auto& edge : rest.value()) {
            CHECK(edge.id > cursor);
            cursor = edge.id;
        }
        auto restarted = batch->getEdgesBetweenPage(sourceId, targetId);
        REQUIRE(restarted.has_value());
        CHECK(restarted.value().size() == 3);
        auto filtered = batch->getEdgesBetweenPage(sourceId, targetId, "LINK_66");
        REQUIRE(filtered.has_value());
        REQUIRE(filtered.value().size() == 1);
        CHECK(filtered.value().front().id == rest.value().back().id);
        auto noMatch = batch->getEdgesBetweenPage(sourceId, targetId, "missing");
        REQUIRE(noMatch.has_value());
        CHECK(noMatch.value().empty());
        auto late = batch->addEdge(KGEdge{
            .srcNodeId = sourceId, .dstNodeId = targetId, .relation = "LINK_0", .weight = 0.75F});
        REQUIRE(late.has_value());
        auto after = batch->getEdgesBetweenPage(sourceId, targetId, std::nullopt, cursor);
        REQUIRE(after.has_value());
        REQUIRE(after.value().size() == 1);
        CHECK(after.value().front().id == late.value());
        CHECK(after.value().front().weight == 0.75F);
        auto end = batch->getEdgesBetweenPage(sourceId, targetId, std::nullopt, late.value());
        REQUIRE(end.has_value());
        CHECK(end.value().empty());
        auto outside = graph.store->getEdgesBetween(sourceId, targetId, "LINK_66");
        REQUIRE(outside.has_value());
        CHECK(outside.value().empty());
        if (commit) {
            REQUIRE(batch->commit().has_value());
            auto ended = batch->getEdgesBetweenPage(sourceId, targetId);
            REQUIRE_FALSE(ended.has_value());
            CHECK(ended.error().code == yams::ErrorCode::InvalidState);
        }
    }
    std::size_t edgeCount = 0;
    for (const auto relation : {"LINK_0", "LINK_64", "LINK_65", "LINK_66"}) {
        auto edges = graph.store->getEdgesBetween(sourceId, targetId, relation);
        REQUIRE(edges.has_value());
        edgeCount += edges.value().size();
    }
    auto source = graph.store->getNodeByKey("source");
    REQUIRE(source.has_value());
    CHECK(edgeCount == (commit ? 4 : 0));
    CHECK(source.value().has_value() == commit);
}

TEST_CASE("topology deferrals report an exact total with bounded diagnostic keys",
          "[metadata][topology][memory-sync][adapter-report-budget]") {
    TempDirGuard tmp;
    MemorySyncConfig writerConfig{"writer"};
    writerConfig.exclusiveBackend = true;
    MemorySyncService writer{makeBackend(tmp.path), writerConfig};
    constexpr std::size_t kEdges = 70;
    for (std::size_t i = 0; i < kEdges; ++i) {
        TopologyEdgeRecord record;
        record.sourceNodeKey = "missing-source";
        record.targetNodeKey = "missing-target";
        record.relation = "relation-" + std::to_string(i);
        const auto key =
            std::string(memoryStoreName(MemoryStore::TopologyEdge)) + "/" + record.id();
        REQUIRE(key.size() <= kMaxLogicalKeyBytes);
        const auto published = writer.publish(key, bytes(nlohmann::json(record).dump()));
        INFO((published ? std::string{} : published.error().message));
        REQUIRE(published.has_value());
    }
    MemorySyncConfig readerConfig{"reader"};
    readerConfig.exclusiveBackend = true;
    MemorySyncService reader{makeBackend(tmp.path), readerConfig};
    REQUIRE(reader.syncFully().has_value());
    REQUIRE(reader.mergedRecordCount() == kEdges);
    KGFixture graph("topology_deferred_report_");
    TopologySyncAdapter adapter{*graph.store, reader};
    const auto generation = graph.store->changeGeneration();
    for (int repeat = 0; repeat < 2; ++repeat) {
        auto result = adapter.apply();
        REQUIRE(result.has_value());
        CHECK(result.value().edgesDeferred == kEdges);
        CHECK(result.value().edgesApplied == 0);
        CHECK(result.value().edgesDropped == 0);
        CHECK(adapter.deferredKeys().size() <= 64);
        std::size_t retainedBytes = 0;
        for (const auto& key : adapter.deferredKeys()) {
            retainedBytes += key.size();
        }
        CHECK(retainedBytes <= 64 * 1024);
        CHECK(graph.store->changeGeneration() == generation);
    }
}

TEST_CASE("topology pressure or cancellation after a real target write rolls back the whole pass",
          "[metadata][topology][memory-sync][adapter-transaction-pressure]") {
    bool cancel = false;
    SECTION("local pressure") {}
    SECTION("cancellation") {
        cancel = true;
    }
    KGFixture graph("topology_pressure_rollback_");
    struct HookState {
        std::atomic<bool> pressure{false};
        std::atomic<bool> armed{true};
        std::atomic<std::size_t> writes{0};
        MemorySyncService* service{nullptr};
        bool cancel{false};
    } state;
    state.cancel = cancel;
    ConnectionPoolConfig poolConfig;
    poolConfig.minConnections = 2;
    poolConfig.maxConnections = 2;
    poolConfig.reservedConnectionsPct = 0;
    ConnectionPool pool{graph.dbPath.string(), poolConfig};
    REQUIRE(pool.initialize().has_value());
    auto target = makeSqliteKnowledgeGraphStore(pool);
    REQUIRE(target.has_value());
    TempDirGuard tmp;
    MemorySyncConfig writerConfig{"writer"};
    writerConfig.exclusiveBackend = true;
    MemorySyncService writer{makeBackend(tmp.path), writerConfig};
    constexpr std::size_t kNodes = 6;
    for (std::size_t i = 0; i < kNodes; ++i) {
        TopologyNodeRecord record;
        record.nodeKey = "pressure-node-" + std::to_string(i);
        record.label = "replicated";
        const auto key = std::string(memoryStoreName(MemoryStore::TopologyNode)) + "/" +
                         escapeRecordKeySegment(record.nodeKey);
        REQUIRE(writer.publish(key, bytes(nlohmann::json(record).dump())).has_value());
    }
    MemorySyncConfig readerConfig{"reader"};
    readerConfig.exclusiveBackend = true;
    MemorySyncService reader{makeBackend(tmp.path),
                             readerConfig,
                             {},
                             true,
                             {},
                             {},
                             [&] { return state.pressure.load(); }};
    state.service = &reader;
    REQUIRE(reader.syncFully().has_value());
    REQUIRE(reader.mergedRecordCount() == kNodes);
    // Install on BOTH real pooled connections before releasing either, so the
    // batch cannot accidentally use an unobserved connection. No fake target.
    {
        auto first = pool.acquire(std::chrono::seconds(2), ConnectionPriority::High);
        auto second = pool.acquire(std::chrono::seconds(2), ConnectionPriority::High);
        REQUIRE(first.has_value());
        REQUIRE(second.has_value());
        auto hook = +[](void* context, int operation, const char*, const char* table,
                        sqlite3_int64) noexcept {
            auto& state = *static_cast<HookState*>(context);
            if (operation != SQLITE_INSERT || table == nullptr ||
                std::string_view(table) != "kg_nodes") {
                return;
            }
            state.writes.fetch_add(1);
            if (state.armed.exchange(false)) {
                if (state.cancel) {
                    state.service->stop();
                } else {
                    state.pressure.store(true);
                }
            }
        };
        sqlite3_update_hook((*first.value())->rawHandle(), hook, &state);
        sqlite3_update_hook((*second.value())->rawHandle(), hook, &state);
    }
    TopologySyncAdapter adapter{*target.value(), reader};
    const auto generation = target.value()->changeGeneration();
    auto result = adapter.apply();
    CHECK(state.writes.load() >= 1); // Proves a real uncommitted target write occurred.
    CHECK_FALSE(result.has_value());
    if (!result) {
        CHECK(result.error().code ==
              (cancel ? yams::ErrorCode::OperationCancelled : yams::ErrorCode::ResourceBusy));
    }
    CHECK(target.value()->changeGeneration() == generation);
    for (std::size_t i = 0; i < kNodes; ++i) {
        // A separate actual store/pool verifies physical transaction rollback.
        auto node = graph.store->getNodeByKey("pressure-node-" + std::to_string(i));
        REQUIRE(node.has_value());
        CHECK_FALSE(node.value().has_value());
    }
    if (!cancel) {
        state.pressure.store(false);
        auto retried = adapter.apply();
        REQUIRE(retried.has_value());
        CHECK(retried.value().nodesApplied == kNodes);
    }
}

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

TEST_CASE("topology adapter applies all nodes and edges beyond resident budgets",
          "[metadata][topology][memory-sync][paged-adapter][disk-backed-serving]") {
    TempDirGuard temp;
    KGFixture source("topology_paged_source_");
    KGFixture target("topology_paged_target_");
    REQUIRE(target.store->upsertNode(KGNode{.nodeKey = "target:seed", .type = "test"}).has_value());
    std::vector<KGNode> nodes;
    std::size_t maxPayloadBytes = 0;
    {
        MemorySyncService writer{makeBackend(temp.path / "sync"), MemorySyncConfig{"writer"}};
        TopologySyncAdapter publisher{*source.store, writer};
        std::vector<std::string> payloadKeys;
        for (int i = 0; i < 6; ++i) {
            KGNode node{.nodeKey = "entity:page-" + std::to_string(i),
                        .label = "page-" + std::to_string(i),
                        .type = "entity"};
            const auto inserted = source.store->upsertNode(node);
            REQUIRE(inserted.has_value());
            node.id = inserted.value();
            REQUIRE(publisher.publishNode(node).has_value());
            payloadKeys.push_back("topology-node/" + escapeRecordKeySegment(node.nodeKey));
            nodes.push_back(std::move(node));
        }
        for (std::size_t i = 1; i < nodes.size(); ++i) {
            const KGEdge edge{.srcNodeId = nodes[i - 1].id,
                              .dstNodeId = nodes[i].id,
                              .relation = "LINKS",
                              .weight = 1.0F};
            REQUIRE(source.store->addEdgesUnique({edge}).has_value());
            REQUIRE(
                publisher.publishEdge(nodes[i - 1].nodeKey, edge, nodes[i].nodeKey).has_value());
            payloadKeys.push_back("topology-edge/" + escapeRecordKeySegment(nodes[i - 1].nodeKey) +
                                  "/LINKS/" + escapeRecordKeySegment(nodes[i].nodeKey));
        }
        // Shared-store publication persists operations; explicit committed reads require a
        // completed reconciliation. Survey the exact wire sizes once all inputs are committed.
        REQUIRE(writer.syncFully().has_value());
        REQUIRE(writer.mergedRecordCount() == payloadKeys.size());
        for (const auto& key : payloadKeys) {
            const auto payload = writer.readCommitted(key);
            INFO((payload ? "success" : payload.error().message));
            REQUIRE(payload.has_value());
            maxPayloadBytes = std::max(maxPayloadBytes, payload.value().size());
        }
    }
    MemorySyncLimits limits;
    limits.maxIndexObjectsPerSync = 2;
    SECTION("metadata slots") {
        limits.maxMergedKeys = 2;
    }
    SECTION("cache for two maximum-size payloads") {
        limits.maxCacheBytes = 2 * maxPayloadBytes;
    }
    MemorySyncConfig config{"reader", 60'000, "local-test-corpus", 1, limits};
    config.exclusiveBackend = true;
    MemorySyncService reader{makeBackend(temp.path / "sync"), config};
    REQUIRE(reader.syncFully().has_value());
    REQUIRE(reader.mergedRecordCount() == nodes.size() + nodes.size() - 1);
    TopologySyncAdapter consumer{*target.store, reader};
    const auto applied = consumer.apply();
    INFO((applied ? "success" : applied.error().message));
    REQUIRE(applied.has_value());
    CHECK(applied.value().nodesApplied == nodes.size());
    CHECK(applied.value().edgesApplied == nodes.size() - 1);
    CHECK(consumer.deferredKeys().empty());
    std::vector<std::int64_t> ids;
    for (const auto& node : nodes) {
        const auto actual = target.store->getNodeByKey(node.nodeKey);
        REQUIRE(actual.has_value());
        CHECK(actual.value().has_value());
        if (actual.value()) {
            CHECK(actual.value()->label == node.label);
            CHECK(actual.value()->id != node.id);
            ids.push_back(actual.value()->id);
        }
    }
    if (ids.size() == nodes.size()) {
        for (std::size_t i = 1; i < ids.size(); ++i) {
            const auto edges = target.store->getEdgesBetween(ids[i - 1], ids[i], "LINKS");
            REQUIRE(edges.has_value());
            REQUIRE(edges.value().size() == 1);
            CHECK(edges.value().front().weight == 1.0F);
        }
    }
    const auto repeated = consumer.apply();
    REQUIRE(repeated.has_value());
    CHECK(repeated.value().nodesApplied == 0);
    CHECK(repeated.value().edgesApplied == 0);
    CHECK(consumer.deferredKeys().empty());
}

TEST_CASE("topology late-page identity failure preserves earlier nodes and deletions",
          "[metadata][topology][memory-sync][paged-characterization]") {
    TempDirGuard temp;
    KGFixture graph("topology_preflight_");
    REQUIRE(graph.store->upsertNode(KGNode{.nodeKey = "entity:retained", .type = "entity"})
                .has_value());
    MemorySyncService writer{makeBackend(temp.path / "sync"), MemorySyncConfig{"writer"}};
    TopologyNodeRecord valid;
    valid.nodeKey = "entity:a";
    TopologyNodeRecord invalid;
    invalid.nodeKey = "entity:different";
    REQUIRE(writer.publish("topology-node/entity%3Aa", bytes(nlohmann::json(valid).dump()))
                .has_value());
    REQUIRE(writer.erase("topology-node/entity%3Aretained", "entity:retained").has_value());
    REQUIRE(writer.publish("topology-node/entity%3Az", bytes(nlohmann::json(invalid).dump()))
                .has_value());
    MemorySyncLimits limits;
    limits.maxIndexObjectsPerSync = 1;
    MemorySyncConfig config{"reader", 60'000, "local-test-corpus", 1, limits};
    config.exclusiveBackend = true;
    MemorySyncService reader{makeBackend(temp.path / "sync"), config};
    REQUIRE(reader.syncFully().has_value());
    REQUIRE(reader.mergedRecordCount() == 3);
    TopologySyncAdapter consumer{*graph.store, reader};
    const auto applied = consumer.apply();
    REQUIRE_FALSE(applied.has_value());
    CHECK(applied.error().code == yams::ErrorCode::InvalidData);
    const auto earlier = graph.store->getNodeByKey(valid.nodeKey);
    REQUIRE(earlier.has_value());
    CHECK_FALSE(earlier.value().has_value());
    const auto retained = graph.store->getNodeByKey("entity:retained");
    REQUIRE(retained.has_value());
    CHECK(retained.value().has_value());
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

TEST_CASE("topology sync adapter drops an edge whose endpoint node was deleted",
          "[metadata][topology][memory-sync][prerequisite][tombstone]") {
    TempDirGuard temp;
    KGFixture graphA("topology_sync_tombstoned_a_");
    KGFixture graphB("topology_sync_tombstoned_b_");
    MemorySyncService syncA{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MemorySyncService syncB{makeBackend(temp.path / "sync"), MemorySyncConfig{"B", 50}};
    TopologySyncAdapter adapterA{*graphA.store, syncA};

    const KGNode member{.nodeKey = "doc:member", .label = "member", .type = "document"};
    const KGNode cluster{
        .nodeKey = "topology:cluster:gone", .label = "gone", .type = "topology_cluster"};
    const auto memberId = graphA.store->upsertNode(member);
    const auto clusterId = graphA.store->upsertNode(cluster);
    REQUIRE(memberId.has_value());
    REQUIRE(clusterId.has_value());
    const KGEdge memberOf{
        .srcNodeId = memberId.value(), .dstNodeId = clusterId.value(), .relation = "member_of"};

    // A's edge reached the store, then a rebuild deleted the cluster node and A retracted the
    // node; the edge's own retraction is still in flight.
    REQUIRE(adapterA.publishNode(member).has_value());
    REQUIRE(adapterA.publishEdge(member.nodeKey, memberOf, cluster.nodeKey).has_value());
    REQUIRE(adapterA.publishDeleteNode(cluster.nodeKey).has_value());

    TopologySyncAdapter consumer{*graphB.store, syncB};
    const auto applied = consumer.apply();
    REQUIRE(applied.has_value());
    CHECK(applied.value().nodesApplied == 1);
    CHECK(applied.value().edgesApplied == 0);
    // The endpoint is known to be gone, so the edge cannot become applicable by waiting.
    CHECK(consumer.deferredKeys().empty());
    const auto clusterB = graphB.store->getNodeByKey(cluster.nodeKey);
    REQUIRE(clusterB.has_value());
    CHECK_FALSE(clusterB.value().has_value());
}

TEST_CASE("topology retraction reads cold own winners from the committed generation",
          "[metadata][topology][memory-sync][retraction][retract-cold]") {
    bool edge = false;
    SECTION("node") {}
    SECTION("edge") {
        edge = true;
    }
    TempDirGuard temp;
    KGFixture graph("topology_retract_cold_");
    MemorySyncConfig config{"A", 60'000};
    config.exclusiveBackend = true;
    config.limits.maxCacheBytes = 1024;
    MemorySyncService sync{makeBackend(temp.path / "sync"), config};
    TopologySyncAdapter adapter{*graph.store, sync};
    if (edge) {
        REQUIRE(adapter.publishEdge("source", KGEdge{.relation = "related"}, "target").has_value());
    } else {
        REQUIRE(adapter.publishNode(KGNode{.nodeKey = "cold-node", .label = "own", .type = "test"})
                    .has_value());
    }
    const auto own = sync.committedWinnersAfter("topology-", "", 1);
    REQUIRE(own.size() == 1);
    const auto key = own.front().first;
    const auto winner = own.front().second;
    REQUIRE(winner.origin == "A");
    REQUIRE(sync.publish("user/cache-fill", bytes(std::string(1024, 'x'))).has_value());
    const auto cold = sync.readCached(key);
    REQUIRE_FALSE(cold.has_value());
    REQUIRE(cold.error().code == yams::ErrorCode::NotFound);
    const auto retracted = adapter.retractIfLocallyAbsent(key, winner);
    REQUIRE(retracted.has_value());
    CHECK(retracted.value());
    const auto after = sync.committedWinnersAfter("topology-", "", 1);
    REQUIRE(after.size() == 1);
    CHECK(after.front().second.isTombstone());
}

TEST_CASE("topology retraction preserves a remote winner newer than its own observation",
          "[metadata][topology][memory-sync][retraction][retract-current]") {
    bool edge = false;
    SECTION("node") {}
    SECTION("edge") {
        edge = true;
    }
    TempDirGuard temp;
    KGFixture graph("topology_retract_current_");
    MemorySyncConfig ownConfig{"A", 60'000};
    ownConfig.exclusiveBackend = true;
    MemorySyncConfig peerConfig{"Z", 60'000};
    peerConfig.exclusiveBackend = true;
    MemorySyncService sync{makeBackend(temp.path / "own"), ownConfig};
    MemorySyncService peer{makeBackend(temp.path / "peer"), peerConfig};
    TopologySyncAdapter adapter{*graph.store, sync};
    TopologySyncAdapter remote{*graph.store, peer};
    if (edge) {
        REQUIRE(
            adapter.publishEdge("source", KGEdge{.relation = "related", .weight = 1.0}, "target")
                .has_value());
    } else {
        REQUIRE(adapter.publishNode(KGNode{.nodeKey = "stale-node", .label = "own", .type = "test"})
                    .has_value());
    }
    const auto own = sync.committedWinnersAfter("topology-", "", 1);
    REQUIRE(own.size() == 1);
    const auto key = own.front().first;
    const auto stale = own.front().second;
    REQUIRE(stale.origin == "A");
    const auto preparedRead = sync.readCommittedIfCurrent(key, stale);
    REQUIRE(preparedRead.has_value());
    REQUIRE(preparedRead.value().has_value());
    const auto initial = sync.exportLocalDeltasAfter({});
    REQUIRE(initial.has_value());
    REQUIRE(peer.applyDeltas(initial.value().deltas).has_value());
    if (edge) {
        REQUIRE(remote.publishEdge("source", KGEdge{.relation = "related", .weight = 2.0}, "target")
                    .has_value());
    } else {
        REQUIRE(
            remote.publishNode(KGNode{.nodeKey = "stale-node", .label = "remote", .type = "test"})
                .has_value());
    }
    const auto incoming = peer.exportLocalDeltasAfter({});
    REQUIRE(incoming.has_value());
    REQUIRE(incoming.value().deltas.size() == 1);
    REQUIRE(sync.applyDeltas(incoming.value().deltas).has_value());
    const auto current = sync.committedWinnersAfter("topology-", "", 1);
    REQUIRE(current.size() == 1);
    REQUIRE(current.front().second.origin == "Z");
    REQUIRE_FALSE(current.front().second.isTombstone());
    TopologyEdgeRecord deleted;
    deleted.sourceNodeKey = "source";
    deleted.relation = "related";
    deleted.targetNodeKey = "target";
    const auto before = sync.currentVersion();
    const auto conditional = sync.eraseIfCurrent(
        key, stale, edge ? nlohmann::json(deleted).dump() : std::string("stale-node"));
    REQUIRE(conditional.has_value());
    CHECK_FALSE(conditional.value());
    CHECK(sync.currentVersion() == before);
    const auto obsoleteRead = sync.readCommittedIfCurrent(key, stale);
    REQUIRE(obsoleteRead.has_value());
    CHECK_FALSE(obsoleteRead.value().has_value());
    const auto retracted = adapter.retractIfLocallyAbsent(key, stale);
    REQUIRE(retracted.has_value());
    CHECK_FALSE(retracted.value());
    const auto after = sync.committedWinnersAfter("topology-", "", 1);
    REQUIRE(after.size() == 1);
    CHECK(after.front().second.origin == "Z");
    CHECK_FALSE(after.front().second.isTombstone());
}

TEST_CASE("topology sync adapter leaves the writer's own records to its local graph",
          "[metadata][topology][memory-sync][retraction]") {
    TempDirGuard temp;
    KGFixture graphA("topology_sync_own_a_");
    MemorySyncService syncA{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    TopologySyncAdapter adapterA{*graphA.store, syncA};

    const KGNode member{.nodeKey = "doc:own-member", .label = "member", .type = "document"};
    const KGNode cluster{
        .nodeKey = "topology:cluster:own", .label = "own", .type = "topology_cluster"};
    const auto memberId = graphA.store->upsertNode(member);
    const auto clusterId = graphA.store->upsertNode(cluster);
    REQUIRE(memberId.has_value());
    REQUIRE(clusterId.has_value());
    const KGEdge memberOf{
        .srcNodeId = memberId.value(), .dstNodeId = clusterId.value(), .relation = "member_of"};
    REQUIRE(graphA.store->addEdge(memberOf).has_value());
    REQUIRE(adapterA.publishNode(member).has_value());
    REQUIRE(adapterA.publishNode(cluster).has_value());
    REQUIRE(adapterA.publishEdge(member.nodeKey, memberOf, cluster.nodeKey).has_value());

    // A topology rebuild on A deletes the cluster node; the cascade takes the edge with it.
    REQUIRE(graphA.store->deleteNodeById(clusterId.value()).has_value());

    // Applying A's own winners must not resurrect what A's rebuild deleted.
    const auto applied = adapterA.apply();
    REQUIRE(applied.has_value());
    CHECK(applied.value().nodesApplied == 0);
    CHECK(applied.value().edgesApplied == 0);
    const auto clusterA = graphA.store->getNodeByKey(cluster.nodeKey);
    REQUIRE(clusterA.has_value());
    CHECK_FALSE(clusterA.value().has_value());
    CHECK(adapterA.deferredKeys().empty());
}
