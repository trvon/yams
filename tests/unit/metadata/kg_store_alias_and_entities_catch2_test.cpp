// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 YAMS Contributors

#include <chrono>
#include <filesystem>

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>

using namespace yams;
using namespace yams::metadata;

namespace {

std::filesystem::path tempDbPath(const char* prefix) {
    const char* t = std::getenv("YAMS_TEST_TMPDIR");
    auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = base / (std::string(prefix) + std::to_string(ts) + ".db");
    std::filesystem::remove(p, ec);
    return p;
}

struct KGStoreAliasAndEntitiesFixture {
    KGStoreAliasAndEntitiesFixture() {
        dbPath_ = tempDbPath("kg_store_alias_entities_catch2_");

        KnowledgeGraphStoreConfig kgCfg{};
        auto sres = makeSqliteKnowledgeGraphStore(dbPath_.string(), kgCfg);
        REQUIRE(sres.has_value());
        store_ = std::move(sres.value());
        REQUIRE(store_ != nullptr);

        ConnectionPoolConfig pcfg;
        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), pcfg);
        auto init = pool_->initialize();
        REQUIRE(init.has_value());
        repo_ = std::make_unique<MetadataRepository>(*pool_);
    }

    ~KGStoreAliasAndEntitiesFixture() {
        repo_.reset();
        if (pool_) {
            pool_->shutdown();
            pool_.reset();
        }
        store_.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath_, ec);
    }

    std::filesystem::path dbPath_;
    std::unique_ptr<KnowledgeGraphStore> store_;
    std::unique_ptr<ConnectionPool> pool_;
    std::unique_ptr<MetadataRepository> repo_;
};

} // namespace

TEST_CASE("KGStoreAliasAndEntities: alias exact and fuzzy resolution and removal",
          "[unit][metadata][kg]") {
    KGStoreAliasAndEntitiesFixture fix;

    // Create a node and a couple of aliases
    KGNode n;
    n.nodeKey = "ent:alpha";
    n.label = std::string("Alpha");
    n.type = std::string("entity");
    auto nid = fix.store_->upsertNode(n);
    REQUIRE(nid.has_value());

    KGAlias a1;
    a1.nodeId = nid.value();
    a1.alias = "alpha";
    a1.source = std::string("test");
    a1.confidence = 0.9f;
    auto aid1 = fix.store_->addAlias(a1);
    REQUIRE(aid1.has_value());

    SECTION("Exact resolution finds alias") {
        auto exact = fix.store_->resolveAliasExact("alpha", 10);
        REQUIRE(exact.has_value());
        REQUIRE_FALSE(exact.value().empty());
        CHECK(exact.value().front().nodeId == nid.value());
    }

    SECTION("Fuzzy resolution finds alias") {
        auto fuzzy = fix.store_->resolveAliasFuzzy("alpha", 10);
        REQUIRE(fuzzy.has_value());
        REQUIRE_FALSE(fuzzy.value().empty());
        CHECK(fuzzy.value().front().nodeId == nid.value());
    }

    SECTION("Remove aliases clears lookup") {
        auto rm = fix.store_->removeAliasesForNode(nid.value());
        REQUIRE(rm.has_value());
        auto exactAfter = fix.store_->resolveAliasExact("alpha", 10);
        REQUIRE(exactAfter.has_value());
        CHECK(exactAfter.value().empty());
    }
}

TEST_CASE("KGStoreAliasAndEntities: neighbors and doc entities round trip",
          "[unit][metadata][kg]") {
    KGStoreAliasAndEntitiesFixture fix;

    // Two nodes with an edge
    auto ids = fix.store_->upsertNodes(
        {KGNode{.nodeKey = "ent:a", .label = std::string("A"), .type = std::string("entity")},
         KGNode{.nodeKey = "ent:b", .label = std::string("B"), .type = std::string("entity")}});
    REQUIRE(ids.has_value());
    REQUIRE(ids.value().size() == 2);

    KGEdge e;
    e.srcNodeId = ids.value()[0];
    e.dstNodeId = ids.value()[1];
    e.relation = "RELATED_TO";
    auto er = fix.store_->addEdge(e);
    REQUIRE(er.has_value());

    auto nb = fix.store_->neighbors(ids.value()[0], 16);
    REQUIRE(nb.has_value());
    REQUIRE(nb.value().size() == 1);
    CHECK(nb.value().front() == ids.value()[1]);

    // Create a document and attach entities
    DocumentInfo d;
    d.filePath = "/tmp/test-alpha.txt";
    d.fileName = "test-alpha.txt";
    d.fileExtension = ".txt";
    d.fileSize = 123;
    d.sha256Hash = "hash-alpha";
    d.mimeType = "text/plain";
    d.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    d.modifiedTime = d.createdTime;
    d.indexedTime = d.createdTime;
    auto did = fix.repo_->insertDocument(d);
    REQUIRE(did.has_value());

    // Add two entities for the document
    DocEntity de1{.documentId = did.value(),
                  .entityText = std::string("A"),
                  .nodeId = ids.value()[0],
                  .startOffset = 0,
                  .endOffset = 1,
                  .confidence = 0.95f,
                  .extractor = std::string("test")};
    DocEntity de2{.documentId = did.value(),
                  .entityText = std::string("B"),
                  .nodeId = ids.value()[1],
                  .startOffset = 5,
                  .endOffset = 6,
                  .confidence = 0.85f,
                  .extractor = std::string("test")};
    auto ar = fix.store_->addDocEntities({de1, de2});
    REQUIRE(ar.has_value());

    // Retrieve
    auto got = fix.store_->getDocEntitiesForDocument(did.value(), 100, 0);
    REQUIRE(got.has_value());
    CHECK(got.value().size() == 2);

    SECTION("Document ID lookup by hash") {
        auto byHash = fix.store_->getDocumentIdByHash("hash-alpha");
        REQUIRE(byHash.has_value());
        REQUIRE(byHash.value().has_value());
        CHECK(byHash.value().value() == did.value());
    }

    SECTION("Document ID lookup by name") {
        auto byName = fix.store_->getDocumentIdByName("test-alpha.txt");
        REQUIRE(byName.has_value());
        REQUIRE(byName.value().has_value());
        CHECK(byName.value().value() == did.value());
    }

    SECTION("Document ID lookup by path") {
        auto byPath = fix.store_->getDocumentIdByPath("/tmp/test-alpha.txt");
        REQUIRE(byPath.has_value());
        REQUIRE(byPath.value().has_value());
        CHECK(byPath.value().value() == did.value());
    }

    SECTION("Cleanup entities") {
        auto del = fix.store_->deleteDocEntitiesForDocument(did.value());
        REQUIRE(del.has_value());
        auto after = fix.store_->getDocEntitiesForDocument(did.value(), 10, 0);
        REQUIRE(after.has_value());
        CHECK(after.value().empty());
    }
}
