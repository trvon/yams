// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// External callers (graph ingest) mark their rows with properties.ns. These tests pin two
// contracts: document cleanup and orphan repair leave namespaced rows alone, and namespace
// removal deletes only rows of the named namespace.

#include <chrono>
#include <filesystem>
#include <optional>
#include <string>

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>

using namespace yams;
using namespace yams::metadata;

namespace {

std::filesystem::path tempDbPath(const char* prefix) {
    const auto base = std::filesystem::temp_directory_path();
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = base / (std::string(prefix) + std::to_string(ts) + ".db");
    std::filesystem::remove(p, ec);
    return p;
}

struct NamespaceFixture {
    NamespaceFixture() {
        dbPath = tempDbPath("kg_namespace_catch2_test_");
        auto storeRes = makeSqliteKnowledgeGraphStore(dbPath.string(), KnowledgeGraphStoreConfig{});
        REQUIRE(storeRes.has_value());
        store = std::move(storeRes.value());
        // The repository bootstraps the documents table that orphan cleanup reads.
        pool = std::make_unique<ConnectionPool>(dbPath.string(), ConnectionPoolConfig{});
        REQUIRE(pool->initialize().has_value());
        repo = std::make_unique<MetadataRepository>(
            *pool, nullptr, MetadataRepository::SchemaBootstrapMode::EnsureSchema);
    }

    ~NamespaceFixture() {
        repo.reset();
        if (pool) {
            pool->shutdown();
        }
        pool.reset();
        store.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
    }

    std::int64_t node(const std::string& key, const std::string& props) {
        auto id = store->upsertNode(
            KGNode{.nodeKey = key,
                   .label = key,
                   .type = std::string("test"),
                   .properties = props.empty() ? std::nullopt : std::make_optional(props)});
        REQUIRE(id.has_value());
        return id.value();
    }

    void edge(std::int64_t src, std::int64_t dst, const std::string& relation,
              const std::string& props) {
        REQUIRE(store
                    ->addEdgesUnique({KGEdge{
                        .srcNodeId = src,
                        .dstNodeId = dst,
                        .relation = relation,
                        .weight = 1.0F,
                        .properties = props.empty() ? std::nullopt : std::make_optional(props)}})
                    .has_value());
    }

    bool exists(const std::string& key) {
        auto n = store->getNodeByKey(key);
        REQUIRE(n.has_value());
        return n.value().has_value();
    }

    std::size_t edgeCount(std::int64_t src, const std::string& relation) {
        auto edges = store->getEdgesFrom(src, std::string_view(relation), 100, 0);
        REQUIRE(edges.has_value());
        return edges.value().size();
    }

    std::string planFor(const std::string& sql) {
        Database db;
        REQUIRE(db.open(dbPath.string(), ConnectionMode::ReadOnly).has_value());
        auto stmt = db.prepare("EXPLAIN QUERY PLAN " + sql);
        REQUIRE(stmt.has_value());
        std::string plan;
        while (true) {
            auto row = stmt.value().step();
            REQUIRE(row.has_value());
            if (!row.value())
                break;
            plan += stmt.value().getString(3) + "\n";
        }
        return plan;
    }

    std::filesystem::path dbPath;
    std::unique_ptr<KnowledgeGraphStore> store;
    std::unique_ptr<ConnectionPool> pool;
    std::unique_ptr<MetadataRepository> repo;
};

} // namespace

TEST_CASE("KG namespace: document cleanup and orphan repair keep namespaced rows",
          "[unit][metadata][kg][namespace]") {
    NamespaceFixture fix;

    // Same document-bound properties on both sides; only the ns marker differs.
    fix.node("paper:ns-hash", R"({"ns":"paperbridge","document_hash":"h-gone"})");
    fix.node("sym:hash", R"({"document_hash":"h-gone"})");
    auto nsPath = fix.node("paper:ns-path", R"({"ns":"paperbridge","file_path":"/tmp/gone.pdf"})");
    fix.node("sym:path", R"({"file_path":"/tmp/gone.pdf"})");
    fix.node("paper:ns-source", R"({"ns":"paperbridge","source_file":"/tmp/gone.pdf"})");
    auto plain = fix.node("sym:plain", "");

    SECTION("deleteNodesForDocumentHash skips namespaced nodes") {
        auto deleted = fix.store->deleteNodesForDocumentHash("h-gone");
        REQUIRE(deleted.has_value());
        CHECK(deleted.value() == 1);
        CHECK(fix.exists("paper:ns-hash"));
        CHECK_FALSE(fix.exists("sym:hash"));
    }

    SECTION("deleteNodesForSourceFile skips namespaced nodes") {
        auto deleted = fix.store->deleteNodesForSourceFile("/tmp/gone.pdf");
        REQUIRE(deleted.has_value());
        CHECK(deleted.value() == 1);
        CHECK(fix.exists("paper:ns-path"));
        CHECK(fix.exists("paper:ns-source"));
        CHECK_FALSE(fix.exists("sym:path"));
    }

    SECTION("deleteOrphanedNodes skips namespaced nodes but keeps checking plain ones") {
        auto deleted = fix.store->deleteOrphanedNodes();
        REQUIRE(deleted.has_value());
        CHECK(deleted.value() == 2);
        CHECK(fix.exists("paper:ns-hash"));
        CHECK(fix.exists("paper:ns-path"));
        CHECK(fix.exists("paper:ns-source"));
        CHECK_FALSE(fix.exists("sym:hash"));
        CHECK_FALSE(fix.exists("sym:path"));
        CHECK(fix.exists("sym:plain"));
    }

    SECTION("deleteEdgesForSourceFile skips namespaced edges") {
        fix.edge(nsPath, plain, "cites", R"({"ns":"paperbridge","source_file":"/tmp/a.pdf"})");
        fix.edge(nsPath, plain, "mentions", R"({"source_file":"/tmp/a.pdf"})");
        auto deleted = fix.store->deleteEdgesForSourceFile("/tmp/a.pdf");
        REQUIRE(deleted.has_value());
        CHECK(deleted.value() == 1);
        CHECK(fix.edgeCount(nsPath, "cites") == 1);
        CHECK(fix.edgeCount(nsPath, "mentions") == 0);
    }

    SECTION("batch document graph cleanup skips namespaced rows") {
        auto batch = fix.store->beginWriteBatch();
        REQUIRE(batch.has_value());
        auto cleaned = batch.value()->deleteDocumentGraphData(
            {DocumentGraphCleanupTarget{.documentHash = "h-gone", .sourceFile = "/tmp/gone.pdf"}});
        REQUIRE(cleaned.has_value());
        CHECK(cleaned.value().nodesDeleted == 2);
        REQUIRE(batch.value()->commit().has_value());
        CHECK(fix.exists("paper:ns-hash"));
        CHECK(fix.exists("paper:ns-path"));
        CHECK(fix.exists("paper:ns-source"));
        CHECK_FALSE(fix.exists("sym:hash"));
        CHECK_FALSE(fix.exists("sym:path"));
    }
}

TEST_CASE("KG namespace: write batch removes only the named namespace",
          "[unit][metadata][kg][namespace]") {
    NamespaceFixture fix;

    auto p1 = fix.node("paper:1", R"({"ns":"paperbridge"})");
    auto p2 = fix.node("paper:2", R"({"ns":"paperbridge"})");
    auto other = fix.node("other:1", R"({"ns":"other"})");
    auto doc = fix.node("doc:keep", "");
    fix.edge(p1, p2, "cites", R"({"ns":"paperbridge"})");
    fix.edge(p1, p2, "related", R"({"ns":"paperbridge"})");
    fix.edge(other, p1, "cites", R"({"ns":"other"})");  // incident to a paperbridge node
    fix.edge(doc, p1, "mentions", "");                  // un-namespaced, incident
    fix.edge(other, doc, "cites", R"({"ns":"other"})"); // unrelated

    const auto removeWith = [&](std::string_view ns, std::optional<std::string_view> relation,
                                bool dryRun, bool includeForeign = false) {
        auto batch = fix.store->beginWriteBatch();
        REQUIRE(batch.has_value());
        auto removed = batch.value()->deleteByNamespace(ns, relation, dryRun, includeForeign);
        if (removed) {
            REQUIRE(batch.value()->commit().has_value());
        }
        return removed;
    };

    SECTION("an empty namespace or relation is rejected") {
        auto empty = removeWith("", std::nullopt, false);
        REQUIRE_FALSE(empty.has_value());
        CHECK(empty.error().code == ErrorCode::InvalidArgument);
        auto emptyRelation = removeWith("paperbridge", std::string_view{}, false);
        REQUIRE_FALSE(emptyRelation.has_value());
        CHECK(emptyRelation.error().code == ErrorCode::InvalidArgument);
        CHECK(fix.edgeCount(p1, "cites") == 1);
    }

    SECTION("relation scope removes namespaced edges of that relation only") {
        auto dry = removeWith("paperbridge", "cites", true);
        REQUIRE(dry.has_value());
        CHECK(dry.value().edgesDeleted == 1);
        CHECK(dry.value().nodesDeleted == 0);
        CHECK(fix.edgeCount(p1, "cites") == 1);

        auto removed = removeWith("paperbridge", "cites", false);
        REQUIRE(removed.has_value());
        CHECK(removed.value().edgesDeleted == 1);
        CHECK(removed.value().nodesDeleted == 0);
        CHECK(fix.edgeCount(p1, "cites") == 0);
        CHECK(fix.edgeCount(p1, "related") == 1);
        CHECK(fix.edgeCount(other, "cites") == 2);
        CHECK(fix.exists("paper:1"));
    }

    SECTION("namespace scope refuses to drop other owners' incident edges by default") {
        auto dry = removeWith("paperbridge", std::nullopt, true);
        REQUIRE(dry.has_value());
        CHECK(dry.value().nodesDeleted == 2);
        CHECK(dry.value().edgesDeleted == 4);
        CHECK(dry.value().foreignEdges == 2); // other:1 -cites-> and doc:keep -mentions->
        CHECK_FALSE(dry.value().refused);
        CHECK(fix.exists("paper:1"));

        auto refused = removeWith("paperbridge", std::nullopt, false);
        REQUIRE(refused.has_value());
        CHECK(refused.value().refused);
        CHECK(refused.value().foreignEdges == 2);
        CHECK(fix.exists("paper:1"));
        CHECK(fix.edgeCount(p1, "cites") == 1);
        CHECK(fix.edgeCount(doc, "mentions") == 1);
    }

    SECTION("namespace scope with foreign edges allowed removes nodes and incident edges") {
        auto removed = removeWith("paperbridge", std::nullopt, false, true);
        REQUIRE(removed.has_value());
        CHECK_FALSE(removed.value().refused);
        CHECK(removed.value().foreignEdges == 2);
        CHECK(removed.value().nodesDeleted == 2);
        CHECK(removed.value().edgesDeleted == 4);
        CHECK_FALSE(fix.exists("paper:1"));
        CHECK_FALSE(fix.exists("paper:2"));
        CHECK(fix.exists("other:1"));
        CHECK(fix.exists("doc:keep"));
        CHECK(fix.edgeCount(other, "cites") == 1);
        CHECK(fix.edgeCount(doc, "mentions") == 0);
    }

    SECTION("an unknown namespace removes nothing") {
        auto removed = removeWith("nobody", std::nullopt, false);
        REQUIRE(removed.has_value());
        CHECK(removed.value().nodesDeleted == 0);
        CHECK(removed.value().edgesDeleted == 0);
        CHECK(fix.edgeCount(p1, "cites") == 1);
    }
}

TEST_CASE("KG namespace: only a non-empty string namespace is exempt from cleanup",
          "[unit][metadata][kg][namespace]") {
    NamespaceFixture fix;
    // Malformed owners must not pin a row: they are treated as un-namespaced.
    for (const auto* props :
         {R"({"ns":"","document_hash":"h-bad"})", R"({"ns":5,"document_hash":"h-bad"})",
          R"({"ns":true,"document_hash":"h-bad"})", R"({"ns":{},"document_hash":"h-bad"})",
          R"({"ns":null,"document_hash":"h-bad"})"}) {
        fix.node(std::string("sym:bad:") + props, props);
    }
    fix.node("paper:good", R"({"ns":"pb","document_hash":"h-bad"})");

    auto deleted = fix.store->deleteNodesForDocumentHash("h-bad");
    REQUIRE(deleted.has_value());
    CHECK(deleted.value() == 5);
    CHECK(fix.exists("paper:good"));
}

TEST_CASE("KG namespace: relation-wide resets and snapshot pruning keep namespaced edges",
          "[unit][metadata][kg][namespace]") {
    NamespaceFixture fix;
    auto a = fix.node("paper:a", R"({"ns":"pb"})");
    auto b = fix.node("paper:b", R"({"ns":"pb"})");
    auto c = fix.node("sym:c", "");
    fix.edge(a, b, "semantic_neighbor", R"({"ns":"pb"})");
    fix.edge(c, b, "semantic_neighbor", "");

    auto deleted = fix.store->deleteEdgesByRelation("semantic_neighbor");
    REQUIRE(deleted.has_value());
    CHECK(deleted.value() == 1);
    CHECK(fix.edgeCount(a, "semantic_neighbor") == 1);
    CHECK(fix.edgeCount(c, "semantic_neighbor") == 0);

    auto batch = fix.store->beginWriteBatch();
    REQUIRE(batch.has_value());
    auto batchDeleted = batch.value()->deleteEdgesByRelation("semantic_neighbor");
    REQUIRE(batchDeleted.has_value());
    CHECK(batchDeleted.value() == 0);
    REQUIRE(batch.value()->commit().has_value());
}

TEST_CASE("KG namespace: removal lookups use the namespace indexes",
          "[unit][metadata][kg][namespace]") {
    NamespaceFixture fix;
    for (const auto* table : {"kg_nodes", "kg_edges"}) {
        auto plan = fix.planFor(std::string("SELECT id FROM ") + table +
                                " WHERE CASE WHEN json_valid(properties) THEN "
                                "json_extract(properties, '$.ns') END = 'pb'");
        INFO(table << " plan: " << plan);
        CHECK(plan.find(std::string("idx_") + table + "_ns") != std::string::npos);
    }
}

TEST_CASE("KG namespace: write batch savepoints undo writes and keep the transaction",
          "[unit][metadata][kg][namespace]") {
    NamespaceFixture fix;
    auto a = fix.node("n:a", "");
    auto b = fix.node("n:b", "");

    auto batch = fix.store->beginWriteBatch();
    REQUIRE(batch.has_value());
    auto& wb = *batch.value();
    REQUIRE(wb.beginSavepoint().has_value());
    REQUIRE(wb.upsertEdgeExact(
                  KGEdge{.srcNodeId = a, .dstNodeId = b, .relation = "r", .weight = 0.5F}, false)
                .has_value());
    auto seen = wb.getEdgeByEndpoints(a, b, "r");
    REQUIRE(seen.has_value());
    CHECK(seen.value().has_value());
    REQUIRE(wb.rollbackToSavepoint().has_value());
    auto gone = wb.getEdgeByEndpoints(a, b, "r");
    REQUIRE(gone.has_value());
    CHECK_FALSE(gone.value().has_value());

    REQUIRE(wb.beginSavepoint().has_value());
    REQUIRE(wb.upsertEdgeExact(KGEdge{.srcNodeId = a,
                                      .dstNodeId = b,
                                      .relation = "r",
                                      .weight = 0.9F,
                                      .properties = std::string(R"({"v":1})")},
                               false)
                .has_value());
    // Exact upsert replaces a higher weight too (no MAX merge).
    REQUIRE(wb.upsertEdgeExact(KGEdge{.srcNodeId = a,
                                      .dstNodeId = b,
                                      .relation = "r",
                                      .weight = 0.1F,
                                      .properties = std::string(R"({"v":2})")},
                               true)
                .has_value());
    auto first = wb.upsertAliasCounted(KGAlias{.nodeId = a, .alias = "alias-a"});
    auto second = wb.upsertAliasCounted(KGAlias{.nodeId = a, .alias = "alias-a"});
    REQUIRE(first.has_value());
    REQUIRE(second.has_value());
    CHECK(first.value());
    CHECK_FALSE(second.value());
    REQUIRE(wb.releaseSavepoint().has_value());
    REQUIRE(wb.commit().has_value());

    auto edges = fix.store->getEdgesFrom(a, std::string_view("r"), 10, 0);
    REQUIRE(edges.has_value());
    REQUIRE(edges.value().size() == 1);
    CHECK(edges.value()[0].weight == 0.1F);
    CHECK(edges.value()[0].properties.value_or("") == R"({"v":2})");
}
