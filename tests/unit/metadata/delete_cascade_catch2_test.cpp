// Copyright (c) 2026 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Tests for the "remove from corpus" (document delete) cascade. They exercise
// MetadataRepository::deleteDocument + the KG cascade (deleteNodesForDocumentHash /
// deleteNodesForSourceFile / deleteEdgesForSourceFile) against a single shared
// ConnectionPool, mirroring how AppContext wires metadataRepo and kgStore onto the same DB.
//
// Originally authored as characterization tests (documenting orphan leaks); the leak
// assertions now assert the FIXED behavior (full cascade, no orphans).

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

#include "../../common/metadata_test_db.h"

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>

using namespace yams::metadata;

namespace {

struct DeleteCascadeFixture {
    DeleteCascadeFixture() {
        dbPath = yams::test::migrated_metadata_db_template().clone("yams_delete_cascade_db_");
        testDir = dbPath.parent_path() / dbPath.stem();
        std::filesystem::create_directories(testDir);

        ConnectionPoolConfig poolConfig;
        poolConfig.minConnections = 1;
        poolConfig.maxConnections = 2;
        REQUIRE((poolConfig.enableForeignKeys));
        pool = std::make_shared<ConnectionPool>(dbPath.string(), poolConfig);
        REQUIRE((pool->initialize().has_value()));

        metadataRepo = std::make_shared<MetadataRepository>(
            *pool, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);

        KnowledgeGraphStoreConfig kgConfig;
        kgConfig.enable_alias_fts = true;
        kgConfig.enable_wal = false;
        auto kgResult = makeSqliteKnowledgeGraphStore(*pool, kgConfig);
        REQUIRE((kgResult.has_value()));
        kgStore = std::shared_ptr<KnowledgeGraphStore>(std::move(kgResult).value());
        metadataRepo->setKnowledgeGraphStore(kgStore);
    }

    ~DeleteCascadeFixture() {
        kgStore.reset();
        metadataRepo.reset();
        pool.reset();
        std::error_code ec;
        std::filesystem::remove_all(testDir, ec);
        yams::test::remove_sqlite_artifacts(dbPath);
    }

    int64_t insertDoc(const std::string& filePath, const std::string& hash,
                      const std::string& content) {
        DocumentInfo doc;
        doc.filePath = filePath;
        const auto derived = computePathDerivedValues(filePath);
        doc.fileName = std::filesystem::path(derived.normalizedPath).filename().string();
        doc.fileExtension = std::filesystem::path(derived.normalizedPath).extension().string();
        doc.fileSize = static_cast<int64_t>(content.size());
        doc.sha256Hash = hash;
        doc.mimeType = "text/plain";
        doc.pathPrefix = derived.pathPrefix;
        doc.reversePath = derived.reversePath;
        doc.pathHash = derived.pathHash;
        doc.parentHash = derived.parentHash;
        doc.pathDepth = derived.pathDepth;
        const auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        doc.createdTime = now;
        doc.modifiedTime = now;
        doc.indexedTime = now;
        doc.contentExtracted = true;
        auto inserted = metadataRepo->insertDocument(doc);
        REQUIRE((inserted.has_value()));
        const int64_t id = inserted.value();
        REQUIRE((metadataRepo->indexDocumentContent(id, doc.fileName, content, doc.mimeType)
                     .has_value()));
        return id;
    }

    int64_t scalar(const std::string& sql) {
        int64_t out = -1;
        auto r = pool->withConnection([&](Database& db) -> yams::Result<void> {
            auto st = db.prepare(sql);
            if (!st)
                return st.error();
            auto stmt = std::move(st).value();
            auto step = stmt.step();
            if (!step)
                return step.error();
            if (step.value())
                out = stmt.getInt64(0);
            return yams::Result<void>();
        });
        REQUIRE((r.has_value()));
        return out;
    }

    std::filesystem::path testDir;
    std::filesystem::path dbPath;
    std::shared_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> metadataRepo;
    std::shared_ptr<KnowledgeGraphStore> kgStore;
};

} // namespace

TEST_CASE("delete-cascade: foreign_keys pragma is enabled on pooled connections",
          "[metadata][delete][cascade]") {
    DeleteCascadeFixture f;
    auto r = f.pool->withConnection([&](Database& db) -> yams::Result<int64_t> {
        auto st = db.prepare("PRAGMA foreign_keys");
        if (!st)
            return st.error();
        auto stmt = std::move(st).value();
        auto step = stmt.step();
        if (!step)
            return step.error();
        return step.value() ? stmt.getInt64(0) : -1;
    });
    REQUIRE((r.has_value()));
    CHECK((r.value() == 1));
}

// Finding A (fixed): deleteDocument now removes the content FTS row in-transaction.
TEST_CASE("delete-cascade: content FTS row is removed on document delete",
          "[metadata][delete][cascade][fts]") {
    DeleteCascadeFixture f;
    const std::string hash(64, 'a');
    const int64_t id = f.insertDoc("/corpus/leak_fts.txt", hash, "unique_fts_token_zzz");

    REQUIRE(
        (f.scalar("SELECT COUNT(*) FROM documents_fts WHERE rowid = " + std::to_string(id)) == 1));

    REQUIRE((f.metadataRepo->deleteDocument(id).has_value()));

    CHECK((f.scalar("SELECT COUNT(*) FROM documents WHERE id = " + std::to_string(id)) == 0));
    CHECK(
        (f.scalar("SELECT COUNT(*) FROM documents_fts WHERE rowid = " + std::to_string(id)) == 0));
}

TEST_CASE("delete-cascade: document_content and metadata rows cascade via FK",
          "[metadata][delete][cascade]") {
    DeleteCascadeFixture f;
    const std::string hash(64, 'b');
    const int64_t id = f.insertDoc("/corpus/content.txt", hash, "hello world");
    DocumentContent dc;
    dc.documentId = id;
    dc.contentText = "hello world";
    dc.contentLength = 11;
    dc.extractionMethod = "test";
    dc.language = "en";
    REQUIRE((f.metadataRepo->insertContent(dc).has_value()));
    REQUIRE((f.metadataRepo->setMetadata(id, "k", MetadataValue(std::string("v"))).has_value()));

    REQUIRE((f.scalar("SELECT COUNT(*) FROM document_content WHERE document_id = " +
                      std::to_string(id)) == 1));
    REQUIRE(
        (f.scalar("SELECT COUNT(*) FROM metadata WHERE document_id = " + std::to_string(id)) == 1));

    REQUIRE((f.metadataRepo->deleteDocument(id).has_value()));

    CHECK((f.scalar("SELECT COUNT(*) FROM document_content WHERE document_id = " +
                    std::to_string(id)) == 0));
    CHECK(
        (f.scalar("SELECT COUNT(*) FROM metadata WHERE document_id = " + std::to_string(id)) == 0));
}

TEST_CASE("delete-cascade: symbol_metadata cascades on document delete",
          "[metadata][delete][cascade][symbols]") {
    DeleteCascadeFixture f;
    const std::string hash(64, 'c');
    const int64_t id = f.insertDoc("/corpus/sym.cpp", hash, "int f(){}");

    SymbolMetadata sym;
    sym.documentHash = hash;
    sym.filePath = "/corpus/sym.cpp";
    sym.symbolName = "f";
    sym.qualifiedName = "f";
    sym.kind = "function";
    sym.startLine = 1;
    sym.endLine = 1;
    REQUIRE((f.kgStore->upsertSymbolMetadata({sym}).has_value()));

    REQUIRE((f.scalar("SELECT COUNT(*) FROM symbol_metadata WHERE document_hash = '" + hash +
                      "'") == 1));

    REQUIRE((f.metadataRepo->deleteDocument(id).has_value()));

    CHECK((f.scalar("SELECT COUNT(*) FROM symbol_metadata WHERE document_hash = '" + hash + "'") ==
           0));
}

// Finding D (fixed): canonical + reference nodes (and edges between them) are cleaned by the
// path-based cleanup the delete path now performs (deleteNodesForSourceFile +
// deleteEdgesForSourceFile), in addition to deleteNodesForDocumentHash.
TEST_CASE("delete-cascade: KG canonical/reference nodes and edges are cleaned by path cleanup",
          "[metadata][delete][cascade][kg]") {
    DeleteCascadeFixture f;
    const std::string hash(64, 'd');
    const std::string path = "/corpus/graph.cpp";
    f.insertDoc(path, hash, "void g(){}");

    KGNode canonical;
    canonical.nodeKey = "function:g@" + path;
    canonical.label = "g";
    canonical.type = "function";
    canonical.properties = R"({"qualified_name":"g","file_path":"/corpus/graph.cpp"})";
    REQUIRE((f.kgStore->upsertNode(canonical).has_value()));

    KGNode ref;
    ref.nodeKey = "symbol_ref:helper";
    ref.label = "helper";
    ref.type = "symbol_reference";
    ref.properties = R"({"unresolved":true,"source_file":"/corpus/graph.cpp"})";
    REQUIRE((f.kgStore->upsertNode(ref).has_value()));

    KGNode docNode;
    docNode.nodeKey = "doc:" + hash;
    docNode.type = "document";
    REQUIRE((f.kgStore->upsertNode(docNode).has_value()));

    KGNode version;
    version.nodeKey = "function:g@" + path + "@snap:" + hash;
    version.type = "function_version";
    version.properties = std::string(R"({"document_hash":")") + hash + R"("})";
    REQUIRE((f.kgStore->upsertNode(version).has_value()));

    KGEdge edge;
    edge.srcNodeId = f.kgStore->getNodeByKey("function:g@" + path).value()->id;
    edge.dstNodeId = f.kgStore->getNodeByKey("symbol_ref:helper").value()->id;
    edge.relation = "references";
    edge.properties = R"({"source_file":"/corpus/graph.cpp"})";
    REQUIRE((f.kgStore->addEdge(edge).has_value()));

    REQUIRE((f.scalar("SELECT COUNT(*) FROM kg_nodes") == 4));
    REQUIRE((f.scalar("SELECT COUNT(*) FROM kg_edges") == 1));

    // The full cleanup the delete path performs: hash-based + path-based nodes + edges.
    REQUIRE((f.kgStore->deleteNodesForDocumentHash(hash).has_value()));
    REQUIRE((f.kgStore->deleteEdgesForSourceFile(path).has_value()));
    auto pathDeleted = f.kgStore->deleteNodesForSourceFile(path);
    REQUIRE((pathDeleted.has_value()));
    CHECK((pathDeleted.value() == 2)); // canonical + reference

    CHECK((f.scalar("SELECT COUNT(*) FROM kg_nodes WHERE node_key = 'function:g@" + path + "'") ==
           0));
    CHECK((f.scalar("SELECT COUNT(*) FROM kg_nodes WHERE node_key = 'symbol_ref:helper'") == 0));
    CHECK((f.scalar("SELECT COUNT(*) FROM kg_edges") == 0));
}

TEST_CASE("delete-cascade: kg_edges cascade when an endpoint node is deleted",
          "[metadata][delete][cascade][kg]") {
    DeleteCascadeFixture f;
    const std::string hash(64, 'e');

    KGNode docNode;
    docNode.nodeKey = "doc:" + hash;
    docNode.type = "document";
    auto docId = f.kgStore->upsertNode(docNode);
    REQUIRE((docId.has_value()));

    KGNode version;
    version.nodeKey = "function:h@/corpus/e.cpp@snap:" + hash;
    version.type = "function_version";
    version.properties = std::string(R"({"document_hash":")") + hash + R"("})";
    auto versionId = f.kgStore->upsertNode(version);
    REQUIRE((versionId.has_value()));

    KGEdge edge;
    edge.srcNodeId = docId.value();
    edge.dstNodeId = versionId.value();
    edge.relation = "contains";
    REQUIRE((f.kgStore->addEdge(edge).has_value()));
    REQUIRE((f.scalar("SELECT COUNT(*) FROM kg_edges") == 1));

    auto deleted = f.kgStore->deleteNodesForDocumentHash(hash);
    REQUIRE((deleted.has_value()));
    CHECK((deleted.value() == 2));
    CHECK((f.scalar("SELECT COUNT(*) FROM kg_edges") == 0));
}
