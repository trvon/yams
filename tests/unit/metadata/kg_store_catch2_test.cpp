// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

#include <chrono>
#include <filesystem>
#include <future>
#include <unordered_map>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/kg_topology_analysis.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>

using namespace yams;
using namespace yams::metadata;

using Catch::Approx;

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

struct KGStoreFixture {
    KGStoreFixture() {
        dbPath_ = tempDbPath("kg_store_catch2_test_");
        KnowledgeGraphStoreConfig cfg{};
        auto storeRes = makeSqliteKnowledgeGraphStore(dbPath_.string(), cfg);
        REQUIRE((storeRes.has_value()));
        store_ = std::move(storeRes.value());
        REQUIRE((store_ != nullptr));
    }

    ~KGStoreFixture() {
        store_.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath_, ec);
    }

    std::unique_ptr<KnowledgeGraphStore> store_;
    std::filesystem::path dbPath_;
};

template <typename Seeder>
Result<void> seedWithoutForeignKeys(const std::filesystem::path& dbPath, Seeder&& seeder) {
    Database db;
    auto openResult = db.open(dbPath.string(), ConnectionMode::ReadWrite);
    if (!openResult) {
        return openResult;
    }

    auto timeoutResult = db.setBusyTimeout(std::chrono::milliseconds(2000));
    if (!timeoutResult) {
        return timeoutResult;
    }

    auto disableFk = db.execute("PRAGMA foreign_keys = OFF");
    if (!disableFk) {
        return disableFk;
    }

    auto seedResult = seeder(db);
    if (!seedResult) {
        return seedResult;
    }

    return db.execute("PRAGMA foreign_keys = ON");
}

struct KGStoreRepoFixture {
    KGStoreRepoFixture() {
        dbPath_ = tempDbPath("kg_store_repo_catch2_test_");
        KnowledgeGraphStoreConfig cfg{};
        auto storeRes = makeSqliteKnowledgeGraphStore(dbPath_.string(), cfg);
        REQUIRE((storeRes.has_value()));
        store_ = std::move(storeRes.value());
        REQUIRE((store_ != nullptr));

        ConnectionPoolConfig poolCfg;
        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), poolCfg);
        auto initResult = pool_->initialize();
        REQUIRE((initResult.has_value()));
        repo_ = std::make_unique<MetadataRepository>(*pool_);
    }

    ~KGStoreRepoFixture() {
        repo_.reset();
        if (pool_) {
            pool_->shutdown();
            pool_.reset();
        }
        store_.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath_, ec);
    }

    std::int64_t insertDocument(std::string_view hash, std::string_view path) {
        DocumentInfo doc;
        auto filePath = std::filesystem::path(std::string(path));
        doc.filePath = filePath.string();
        doc.fileName = filePath.filename().string();
        doc.fileExtension = filePath.extension().string();
        doc.fileSize = 128;
        doc.sha256Hash = std::string(hash);
        doc.mimeType = "text/plain";
        auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        doc.createdTime = now;
        doc.modifiedTime = now;
        doc.indexedTime = now;
        auto insertResult = repo_->insertDocument(doc);
        REQUIRE((insertResult.has_value()));
        return insertResult.value();
    }

    Result<void> insertOrphanedEdges(std::size_t count, std::string_view relation) const {
        return seedWithoutForeignKeys(dbPath_, [&](Database& db) -> Result<void> {
            return db.transaction([&]() -> Result<void> {
                auto stmtResult = db.prepare(
                    "INSERT INTO kg_edges (src_node_id, dst_node_id, relation, properties) "
                    "VALUES (?, ?, ?, ?)");
                if (!stmtResult) {
                    return stmtResult.error();
                }
                auto stmt = std::move(stmtResult).value();
                for (std::size_t i = 0; i < count; ++i) {
                    if (i != 0) {
                        auto resetResult = stmt.reset();
                        if (!resetResult) {
                            return resetResult.error();
                        }
                        auto clearResult = stmt.clearBindings();
                        if (!clearResult) {
                            return clearResult.error();
                        }
                    }

                    const auto orphanOffset = static_cast<std::int64_t>(i) * 2;
                    const auto srcNodeId = 900000LL + orphanOffset;
                    const auto dstNodeId = 900001LL + orphanOffset;
                    auto bindResult = stmt.bindAll(srcNodeId, dstNodeId, std::string(relation),
                                                   std::string(R"({"orphan":true})"));
                    if (!bindResult) {
                        return bindResult.error();
                    }
                    auto execResult = stmt.execute();
                    if (!execResult) {
                        return execResult.error();
                    }
                }
                return Result<void>();
            });
        });
    }

    Result<void> insertOrphanedDocEntities(std::size_t count, std::int64_t documentId) const {
        return seedWithoutForeignKeys(dbPath_, [&](Database& db) -> Result<void> {
            return db.transaction([&]() -> Result<void> {
                auto stmtResult =
                    db.prepare("INSERT INTO kg_doc_entities (document_id, entity_text, extractor) "
                               "VALUES (?, ?, ?)");
                if (!stmtResult) {
                    return stmtResult.error();
                }
                auto stmt = std::move(stmtResult).value();
                for (std::size_t i = 0; i < count; ++i) {
                    if (i != 0) {
                        auto resetResult = stmt.reset();
                        if (!resetResult) {
                            return resetResult.error();
                        }
                        auto clearResult = stmt.clearBindings();
                        if (!clearResult) {
                            return clearResult.error();
                        }
                    }

                    auto bindResult =
                        stmt.bindAll(documentId, std::string("orphan-entity-") + std::to_string(i),
                                     std::string("orphan-test"));
                    if (!bindResult) {
                        return bindResult.error();
                    }
                    auto execResult = stmt.execute();
                    if (!execResult) {
                        return execResult.error();
                    }
                }
                return Result<void>();
            });
        });
    }

    std::unique_ptr<KnowledgeGraphStore> store_;
    std::unique_ptr<ConnectionPool> pool_;
    std::unique_ptr<MetadataRepository> repo_;
    std::filesystem::path dbPath_;
};
} // namespace

TEST_CASE("KG Store: batch upsert nodes is idempotent", "[unit][metadata][kg]") {
    KGStoreFixture fix;

    std::vector<KGNode> nodes;
    nodes.push_back(KGNode{.id = 0,
                           .nodeKey = "tag:alpha",
                           .label = std::string("alpha"),
                           .type = std::string("tag")});
    nodes.push_back(KGNode{
        .id = 0, .nodeKey = "tag:beta", .label = std::string("beta"), .type = std::string("tag")});

    SECTION("First upsert creates nodes") {
        auto ids1 = fix.store_->upsertNodes(nodes);
        REQUIRE((ids1.has_value()));
        REQUIRE((ids1.value().size() == 2));
        CHECK((ids1.value()[0] > 0));
        CHECK((ids1.value()[1] > 0));

        SECTION("Second upsert returns same IDs") {
            auto ids2 = fix.store_->upsertNodes(nodes);
            REQUIRE((ids2.has_value()));
            REQUIRE((ids2.value().size() == 2));
            CHECK((ids1.value()[0] == ids2.value()[0]));
            CHECK((ids1.value()[1] == ids2.value()[1]));
        }

        SECTION("getNodeByKey returns correct node") {
            auto n1 = fix.store_->getNodeByKey("tag:alpha");
            REQUIRE((n1.has_value()));
            REQUIRE((n1.value().has_value()));
            CHECK((n1.value()->id == ids1.value()[0]));
        }
    }
}

TEST_CASE("KG Store: getNodesByKeys batches stable node-key lookup", "[unit][metadata][kg]") {
    KGStoreFixture fix;

    std::vector<KGNode> nodes{
        KGNode{.id = 0,
               .nodeKey = "doc:a",
               .label = std::string("a"),
               .type = std::string("document")},
        KGNode{.id = 0,
               .nodeKey = "doc:b",
               .label = std::string("b"),
               .type = std::string("document")},
    };
    auto ids = fix.store_->upsertNodes(nodes);
    REQUIRE((ids.has_value()));

    auto out = fix.store_->getNodesByKeys({"doc:b", "doc:missing", "doc:a"});
    REQUIRE((out.has_value()));
    REQUIRE((out.value().size() == 2));

    std::unordered_map<std::string, std::int64_t> byKey;
    for (const auto& node : out.value()) {
        byKey.emplace(node.nodeKey, node.id);
    }
    CHECK((byKey.at("doc:a") == ids.value()[0]));
    CHECK((byKey.at("doc:b") == ids.value()[1]));
}

TEST_CASE("KG Store: addEdgesUnique deduplicates edges", "[unit][metadata][kg]") {
    KGStoreFixture fix;

    // Prepare two nodes
    std::vector<KGNode> nodes;
    nodes.push_back(KGNode{
        .id = 0, .nodeKey = "doc:a", .label = std::string("a"), .type = std::string("document")});
    nodes.push_back(
        KGNode{.id = 0, .nodeKey = "tag:x", .label = std::string("x"), .type = std::string("tag")});
    auto ids = fix.store_->upsertNodes(nodes);
    REQUIRE((ids.has_value()));
    REQUIRE((ids.value().size() == 2));
    auto src = ids.value()[0];
    auto dst = ids.value()[1];

    // Prepare duplicate edges
    KGEdge e;
    e.srcNodeId = src;
    e.dstNodeId = dst;
    e.relation = "HAS_TAG";
    std::vector<KGEdge> edges{e, e, e};

    SECTION("Insert twice via unique batch") {
        auto r1 = fix.store_->addEdgesUnique(edges);
        REQUIRE((r1.has_value()));
        auto r2 = fix.store_->addEdgesUnique(edges);
        REQUIRE((r2.has_value()));

        // Verify only one edge exists for (src,dst,relation)
        auto out = fix.store_->getEdgesFrom(src, std::string_view("HAS_TAG"), 100, 0);
        REQUIRE((out.has_value()));
        size_t count = 0;
        for (const auto& ed : out.value()) {
            if (ed.dstNodeId == dst && ed.relation == "HAS_TAG")
                ++count;
        }
        CHECK((count == 1));
    }
}

TEST_CASE("KG Store: semantic edge upsert keeps strongest score and reads strongest first",
          "[unit][metadata][kg]") {
    KGStoreFixture fix;

    std::vector<KGNode> nodes{
        KGNode{.id = 0,
               .nodeKey = "doc:src",
               .label = std::string("src"),
               .type = std::string("document")},
        KGNode{.id = 0,
               .nodeKey = "doc:weak",
               .label = std::string("weak"),
               .type = std::string("document")},
        KGNode{.id = 0,
               .nodeKey = "doc:strong",
               .label = std::string("strong"),
               .type = std::string("document")},
    };
    auto ids = fix.store_->upsertNodes(nodes);
    REQUIRE((ids.has_value()));

    KGEdge weak;
    weak.srcNodeId = ids.value()[0];
    weak.dstNodeId = ids.value()[1];
    weak.relation = "semantic_neighbor";
    weak.weight = 0.2F;
    weak.createdTime = 10;
    weak.properties = R"({"rank":2})";

    KGEdge strong;
    strong.srcNodeId = ids.value()[0];
    strong.dstNodeId = ids.value()[2];
    strong.relation = "semantic_neighbor";
    strong.weight = 0.8F;
    strong.createdTime = 11;
    strong.properties = R"({"rank":1})";

    REQUIRE((fix.store_->addEdgesUnique({weak, strong}).has_value()));

    weak.weight = 0.9F;
    weak.createdTime = 12;
    weak.properties = R"({"rank":1,"updated":true})";
    REQUIRE((fix.store_->addEdgesUnique({weak}).has_value()));

    auto out = fix.store_->getEdgesFrom(ids.value()[0], "semantic_neighbor", 10, 0);
    REQUIRE((out.has_value()));
    REQUIRE((out.value().size() == 2));
    CHECK((out.value()[0].dstNodeId == ids.value()[1]));
    CHECK((out.value()[0].weight == Approx(0.9F)));
    REQUIRE((out.value()[0].properties.has_value()));
    CHECK((out.value()[0].properties->find("updated") != std::string::npos));
    CHECK((out.value()[1].dstNodeId == ids.value()[2]));
}

TEST_CASE("KG Store: ensurePathNode links logical path to snapshot path", "[unit][metadata][kg]") {
    KGStoreFixture fix;

    PathNodeDescriptor descriptor;
    descriptor.snapshotId = "snap1";
    descriptor.path = "/repo/file.cpp";
    descriptor.rootTreeHash = "";
    descriptor.isDirectory = false;

    auto snapshotId = fix.store_->ensurePathNode(descriptor);
    REQUIRE((snapshotId.has_value()));

    auto logicalNode = fix.store_->getNodeByKey("path:logical:/repo/file.cpp");
    REQUIRE((logicalNode.has_value()));
    REQUIRE((logicalNode.value().has_value()));

    auto edges = fix.store_->getEdgesFrom(logicalNode.value()->id, "path_version", 10, 0);
    REQUIRE((edges.has_value()));
    REQUIRE_FALSE(edges.value().empty());
    CHECK((edges.value()[0].dstNodeId == snapshotId.value()));
}

TEST_CASE("KG Store: fetchPathHistory follows rename chains and returns blob hashes",
          "[unit][metadata][kg]") {
    KGStoreFixture fix;

    const std::string blobHashOne(64, 'a');
    const std::string blobHashTwo(64, 'b');

    auto blobOneId = fix.store_->ensureBlobNode(blobHashOne);
    auto blobTwoId = fix.store_->ensureBlobNode(blobHashTwo);
    REQUIRE((blobOneId.has_value()));
    REQUIRE((blobTwoId.has_value()));

    auto originalPathId = fix.store_->ensurePathNode(
        PathNodeDescriptor{.snapshotId = "snap1", .path = "/repo/file.cpp", .isDirectory = false});
    auto renamedPathId = fix.store_->ensurePathNode(PathNodeDescriptor{
        .snapshotId = "snap2", .path = "/repo/renamed.cpp", .isDirectory = false});
    REQUIRE((originalPathId.has_value()));
    REQUIRE((renamedPathId.has_value()));

    REQUIRE(
        (fix.store_->linkPathVersion(originalPathId.value(), blobOneId.value(), 7).has_value()));
    REQUIRE((fix.store_->recordRenameEdge(originalPathId.value(), renamedPathId.value(), 8)
                 .has_value()));
    REQUIRE((fix.store_->linkPathVersion(renamedPathId.value(), blobTwoId.value(), 8).has_value()));

    auto history = fix.store_->fetchPathHistory("/repo/file.cpp", 10);
    REQUIRE((history.has_value()));
    REQUIRE((history.value().size() == 2));

    std::unordered_map<std::string, PathHistoryRecord> bySnapshot;
    for (const auto& record : history.value()) {
        bySnapshot.emplace(record.snapshotId, record);
    }

    REQUIRE((bySnapshot.contains("snap1")));
    REQUIRE((bySnapshot.contains("snap2")));
    CHECK((bySnapshot.at("snap1").path == "/repo/file.cpp"));
    CHECK((bySnapshot.at("snap1").blobHash == blobHashOne));
    REQUIRE((bySnapshot.at("snap1").diffId.has_value()));
    CHECK((bySnapshot.at("snap1").diffId.value() == 7));

    CHECK((bySnapshot.at("snap2").path == "/repo/renamed.cpp"));
    CHECK((bySnapshot.at("snap2").blobHash == blobHashTwo));
    REQUIRE((bySnapshot.at("snap2").diffId.has_value()));
    CHECK((bySnapshot.at("snap2").diffId.value() == 8));
}

TEST_CASE("KG Store: ensureDocumentNode is idempotent and refreshes label",
          "[unit][metadata][kg]") {
    KGStoreFixture fix;

    auto first = fix.store_->ensureDocumentNode(
        "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", "/tmp/a.txt");
    REQUIRE((first.has_value()));

    auto second = fix.store_->ensureDocumentNode(
        "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", "/tmp/renamed-a.txt");
    REQUIRE((second.has_value()));
    CHECK((first.value() == second.value()));

    auto node = fix.store_->getNodeByKey(
        "doc:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
    REQUIRE((node.has_value()));
    REQUIRE((node.value().has_value()));
    CHECK((node.value()->type == std::optional<std::string>{"document"}));
    CHECK((node.value()->label == std::optional<std::string>{"/tmp/renamed-a.txt"}));
}

TEST_CASE("KG topology analysis summarizes semantic neighborhoods", "[unit][metadata][kg]") {
    KGStoreFixture fix;

    std::vector<KGNode> nodes = {
        KGNode{.nodeKey = "doc:a", .label = std::string("a"), .type = std::string("document")},
        KGNode{.nodeKey = "doc:b", .label = std::string("b"), .type = std::string("document")},
        KGNode{.nodeKey = "doc:c", .label = std::string("c"), .type = std::string("document")},
    };
    auto ids = fix.store_->upsertNodes(nodes);
    REQUIRE((ids.has_value()));
    REQUIRE((ids.value().size() == 3));

    KGEdge ab{.srcNodeId = ids.value()[0],
              .dstNodeId = ids.value()[1],
              .relation = "semantic_neighbor",
              .weight = 0.92f};
    KGEdge ba{.srcNodeId = ids.value()[1],
              .dstNodeId = ids.value()[0],
              .relation = "semantic_neighbor",
              .weight = 0.92f};
    REQUIRE((fix.store_->addEdgesUnique({ab, ba}).has_value()));

    auto topology = analyzeDocumentTopology(fix.store_.get());
    REQUIRE((topology.has_value()));
    CHECK((topology->documentNodeCount == 3));
    CHECK((topology->semanticEdgeCount == 1));
    CHECK((topology->reciprocalSemanticEdgeCount == 1));
    CHECK((topology->unreciprocatedSemanticEdgeCount == 0));
    CHECK((topology->documentsWithSemanticNeighbors == 2));
    CHECK((topology->documentsWithReciprocalNeighbors == 2));
    CHECK((topology->reciprocalCommunityCount == 1));
    CHECK((topology->reciprocalSingletonDocumentCount == 1));
    CHECK((topology->largestReciprocalCommunitySize == 2));
    CHECK((topology->isolatedDocumentCount == 1));
    CHECK((topology->connectedComponentCount == 2));
    CHECK((topology->largestComponentSize == 2));
    CHECK((topology->semanticReciprocity == Approx(1.0)));
}

TEST_CASE("KG topology analysis distinguishes one-way semantic neighborhoods",
          "[unit][metadata][kg]") {
    KGStoreFixture fix;

    std::vector<KGNode> nodes = {
        KGNode{.nodeKey = "doc:a", .label = std::string("a"), .type = std::string("document")},
        KGNode{.nodeKey = "doc:b", .label = std::string("b"), .type = std::string("document")},
        KGNode{.nodeKey = "doc:c", .label = std::string("c"), .type = std::string("document")},
        KGNode{.nodeKey = "doc:d", .label = std::string("d"), .type = std::string("document")},
    };
    auto ids = fix.store_->upsertNodes(nodes);
    REQUIRE((ids.has_value()));
    REQUIRE((ids.value().size() == 4));

    REQUIRE((fix.store_
                 ->addEdgesUnique({KGEdge{.srcNodeId = ids.value()[0],
                                          .dstNodeId = ids.value()[1],
                                          .relation = "semantic_neighbor",
                                          .weight = 0.91f},
                                   KGEdge{.srcNodeId = ids.value()[1],
                                          .dstNodeId = ids.value()[2],
                                          .relation = "semantic_neighbor",
                                          .weight = 0.90f},
                                   KGEdge{.srcNodeId = ids.value()[2],
                                          .dstNodeId = ids.value()[3],
                                          .relation = "semantic_neighbor",
                                          .weight = 0.89f}})
                 .has_value()));

    auto topology = analyzeDocumentTopology(fix.store_.get());
    REQUIRE((topology.has_value()));
    CHECK((topology->documentNodeCount == 4));
    CHECK((topology->semanticEdgeCount == 3));
    CHECK((topology->reciprocalSemanticEdgeCount == 0));
    CHECK((topology->unreciprocatedSemanticEdgeCount == 3));
    CHECK((topology->documentsWithSemanticNeighbors == 4));
    CHECK((topology->documentsWithReciprocalNeighbors == 0));
    CHECK((topology->reciprocalCommunityCount == 0));
    CHECK((topology->reciprocalSingletonDocumentCount == 4));
    CHECK((topology->largestReciprocalCommunitySize == 0));
    CHECK((topology->connectedComponentCount == 1));
    CHECK((topology->largestComponentSize == 4));
    CHECK((topology->semanticReciprocity == Approx(0.0)));
}

TEST_CASE("KG topology analysis summarizes reciprocal communities", "[unit][metadata][kg]") {
    KGStoreFixture fix;

    std::vector<KGNode> nodes = {
        KGNode{.nodeKey = "doc:a", .label = std::string("a"), .type = std::string("document")},
        KGNode{.nodeKey = "doc:b", .label = std::string("b"), .type = std::string("document")},
        KGNode{.nodeKey = "doc:c", .label = std::string("c"), .type = std::string("document")},
        KGNode{.nodeKey = "doc:d", .label = std::string("d"), .type = std::string("document")},
        KGNode{.nodeKey = "doc:e", .label = std::string("e"), .type = std::string("document")},
    };
    auto ids = fix.store_->upsertNodes(nodes);
    REQUIRE((ids.has_value()));

    REQUIRE((fix.store_
                 ->addEdgesUnique({KGEdge{.srcNodeId = ids.value()[0],
                                          .dstNodeId = ids.value()[1],
                                          .relation = "semantic_neighbor",
                                          .weight = 0.9f},
                                   KGEdge{.srcNodeId = ids.value()[1],
                                          .dstNodeId = ids.value()[0],
                                          .relation = "semantic_neighbor",
                                          .weight = 0.9f},
                                   KGEdge{.srcNodeId = ids.value()[2],
                                          .dstNodeId = ids.value()[3],
                                          .relation = "semantic_neighbor",
                                          .weight = 0.88f},
                                   KGEdge{.srcNodeId = ids.value()[3],
                                          .dstNodeId = ids.value()[2],
                                          .relation = "semantic_neighbor",
                                          .weight = 0.88f}})
                 .has_value()));

    auto topology = analyzeDocumentTopology(fix.store_.get());
    REQUIRE((topology.has_value()));
    CHECK((topology->reciprocalCommunityCount == 2));
    CHECK((topology->largestReciprocalCommunitySize == 2));
    CHECK((topology->reciprocalSingletonDocumentCount == 1));
    REQUIRE((topology->reciprocalCommunitySizes.size() == 2));
    CHECK((topology->reciprocalCommunitySizes[0] == 2));
    CHECK((topology->reciprocalCommunitySizes[1] == 2));
}

TEST_CASE("KG Store: batch rollback — uncommitted node upsert is not persisted",
          "[unit][metadata][kg]") {
    KGStoreFixture fix;

    auto batchResult = fix.store_->beginWriteBatch();
    REQUIRE((batchResult.has_value()));
    auto batch = std::move(batchResult).value();

    std::vector<KGNode> nodes{
        KGNode{.id = 0,
               .nodeKey = "tag:rollback_one",
               .label = std::string("rollback_one"),
               .type = std::string("tag")},
        KGNode{.id = 0,
               .nodeKey = "tag:rollback_two",
               .label = std::string("rollback_two"),
               .type = std::string("tag")},
    };
    auto upsertRes = batch->upsertNodes(nodes);
    REQUIRE((upsertRes.has_value()));
    REQUIRE((upsertRes.value().size() == 2));

    // Verify batch-internal reads see the uncommitted data (within transaction)
    auto internal = batch->upsertNodes(nodes);
    REQUIRE((internal.has_value()));
    CHECK((internal.value()[0] == upsertRes.value()[0]));

    // Destroy batch without commit — triggers rollback via destructor
    batch.reset();

    // Store-level reads (separate connection) must not see rolled-back nodes
    auto n1 = fix.store_->getNodeByKey("tag:rollback_one");
    REQUIRE((n1.has_value()));
    CHECK_FALSE(n1.value().has_value());

    auto n2 = fix.store_->getNodeByKey("tag:rollback_two");
    REQUIRE((n2.has_value()));
    CHECK_FALSE(n2.value().has_value());
}

TEST_CASE("KG Store: batch rollback — uncommitted edges are not persisted",
          "[unit][metadata][kg]") {
    KGStoreFixture fix;

    std::vector<KGNode> nodes{
        KGNode{.id = 0,
               .nodeKey = "doc:src_rb",
               .label = std::string("src"),
               .type = std::string("document")},
        KGNode{.id = 0,
               .nodeKey = "doc:dst_rb",
               .label = std::string("dst"),
               .type = std::string("document")},
    };
    auto ids = fix.store_->upsertNodes(nodes);
    REQUIRE((ids.has_value()));
    REQUIRE((ids.value().size() == 2));
    auto srcId = ids.value()[0];
    auto dstId = ids.value()[1];

    auto batchResult = fix.store_->beginWriteBatch();
    REQUIRE((batchResult.has_value()));
    auto batch = std::move(batchResult).value();

    KGEdge edge{
        .srcNodeId = srcId, .dstNodeId = dstId, .relation = "rollback_relation", .weight = 0.5F};
    {
        std::vector<KGEdge> edges{edge};
        auto addRes = batch->addEdgesUnique(edges);
        REQUIRE((addRes.has_value()));
    }

    // Destroy batch — rollback
    batch.reset();

    // Edges must not be visible after rollback
    auto outEdges = fix.store_->getEdgesFrom(srcId, "rollback_relation", 10, 0);
    REQUIRE((outEdges.has_value()));
    CHECK((outEdges.value().empty()));
}

TEST_CASE("KG Store: batch rollback — prior state is preserved after delete rollback",
          "[unit][metadata][kg]") {
    KGStoreFixture fix;

    std::vector<KGNode> nodes{
        KGNode{.id = 0,
               .nodeKey = "doc:keep_me",
               .label = std::string("keep_me"),
               .type = std::string("document")},
    };
    auto ids = fix.store_->upsertNodes(nodes);
    REQUIRE((ids.has_value()));
    REQUIRE((ids.value().size() == 1));
    auto nodeId = ids.value()[0];

    // Confirm it exists before rollback test
    auto preCheck = fix.store_->getNodeById(nodeId);
    REQUIRE((preCheck.has_value()));
    REQUIRE((preCheck.value().has_value()));

    // Delete in batch, then rollback
    auto batchResult = fix.store_->beginWriteBatch();
    REQUIRE((batchResult.has_value()));
    auto batch = std::move(batchResult).value();
    auto delRes = batch->deleteNodeById(nodeId);
    REQUIRE((delRes.has_value()));
    batch.reset();

    // Node must still exist after rollback
    auto postCheck = fix.store_->getNodeById(nodeId);
    REQUIRE((postCheck.has_value()));
    REQUIRE((postCheck.value().has_value()));
    CHECK((postCheck.value()->nodeKey == "doc:keep_me"));

    // Also confirm no partial mutation — still exactly one node
    auto byKey = fix.store_->getNodeByKey("doc:keep_me");
    REQUIRE((byKey.has_value()));
    REQUIRE((byKey.value().has_value()));
    CHECK((byKey.value()->id == nodeId));
}

TEST_CASE("KG Store: batch rollback — composite batch (nodes + edges) fully rolled back",
          "[unit][metadata][kg]") {
    KGStoreFixture fix;

    // Pre-create one node via store so the edge has a valid dst
    std::vector<KGNode> preNodes{
        KGNode{.id = 0,
               .nodeKey = "doc:tgt",
               .label = std::string("tgt"),
               .type = std::string("document")},
    };
    auto preIds = fix.store_->upsertNodes(preNodes);
    REQUIRE((preIds.has_value()));
    auto tgtId = preIds.value()[0];

    auto batchResult = fix.store_->beginWriteBatch();
    REQUIRE((batchResult.has_value()));
    auto batch = std::move(batchResult).value();

    // Upsert a new source node in the batch
    auto srcIds = batch->upsertNodes({KGNode{.id = 0,
                                             .nodeKey = "doc:src_composite",
                                             .label = std::string("src_composite"),
                                             .type = std::string("document")}});
    REQUIRE((srcIds.has_value()));
    auto srcId = srcIds.value()[0];

    // Add an edge from batch-created node to pre-existing node
    KGEdge edge{
        .srcNodeId = srcId, .dstNodeId = tgtId, .relation = "composite_rel", .weight = 0.75F};
    auto edgeRes = batch->addEdgesUnique({edge});
    REQUIRE((edgeRes.has_value()));

    // Rollback everything
    batch.reset();

    // Source node must not exist
    auto srcCheck = fix.store_->getNodeByKey("doc:src_composite");
    REQUIRE((srcCheck.has_value()));
    CHECK_FALSE(srcCheck.value().has_value());

    // Edge must not exist
    auto outEdges = fix.store_->getEdgesFrom(tgtId, "composite_rel", 10, 0);
    REQUIRE((outEdges.has_value()));
    CHECK((outEdges.value().empty()));

    // Pre-existing target node must still exist
    auto tgtCheck = fix.store_->getNodeById(tgtId);
    REQUIRE((tgtCheck.has_value()));
    REQUIRE((tgtCheck.value().has_value()));
}

TEST_CASE("KG Store: batch rollback — committed batch isolated from uncommitted rollback",
          "[unit][metadata][kg]") {
    KGStoreFixture fix;

    // Batch 1: commit nodes
    {
        auto batchResult = fix.store_->beginWriteBatch();
        REQUIRE((batchResult.has_value()));
        auto batch = std::move(batchResult).value();
        auto r = batch->upsertNodes({KGNode{.id = 0,
                                            .nodeKey = "tag:committed",
                                            .label = std::string("committed"),
                                            .type = std::string("tag")}});
        REQUIRE((r.has_value()));
        auto cr = batch->commit();
        REQUIRE((cr.has_value()));
    }

    // Batch 2: upsert nodes, then rollback
    {
        auto batchResult = fix.store_->beginWriteBatch();
        REQUIRE((batchResult.has_value()));
        auto batch = std::move(batchResult).value();
        auto r = batch->upsertNodes({KGNode{.id = 0,
                                            .nodeKey = "tag:rolled_back",
                                            .label = std::string("rolled_back"),
                                            .type = std::string("tag")}});
        REQUIRE((r.has_value()));
        // batch goes out of scope — implicit rollback
    }

    // Committed node must exist
    auto committed = fix.store_->getNodeByKey("tag:committed");
    REQUIRE((committed.has_value()));
    REQUIRE((committed.value().has_value()));
    CHECK((committed.value()->label == std::optional<std::string>{"committed"}));

    // Rolled-back node must not exist
    auto rolledBack = fix.store_->getNodeByKey("tag:rolled_back");
    REQUIRE((rolledBack.has_value()));
    CHECK_FALSE(rolledBack.value().has_value());
}

TEST_CASE("KG Store: cleanup helpers prune document-bound nodes and scoped edges",
          "[unit][metadata][kg]") {
    KGStoreRepoFixture fix;

    const std::string deletedHash = "hash-delete-me";
    auto docNodeId = fix.store_->ensureDocumentNode(deletedHash, "/tmp/delete-me.cpp");
    REQUIRE((docNodeId.has_value()));

    auto symbolIds = fix.store_->upsertNodes(
        {KGNode{.nodeKey = "sym:delete-me",
                .label = std::string("DeleteMe"),
                .type = std::string("symbol"),
                .properties = std::string(R"({"document_hash":"hash-delete-me"})")},
         KGNode{.nodeKey = "sym:keep-me",
                .label = std::string("KeepMe"),
                .type = std::string("symbol"),
                .properties = std::string(R"({"document_hash":"hash-keep-me"})")}});
    REQUIRE((symbolIds.has_value()));

    auto deletedNodes = fix.store_->deleteNodesForDocumentHash(deletedHash);
    REQUIRE((deletedNodes.has_value()));
    CHECK((deletedNodes.value() == 2));

    auto deletedDocNode = fix.store_->getNodeByKey("doc:hash-delete-me");
    REQUIRE((deletedDocNode.has_value()));
    CHECK_FALSE(deletedDocNode.value().has_value());

    auto deletedSymbolNode = fix.store_->getNodeByKey("sym:delete-me");
    REQUIRE((deletedSymbolNode.has_value()));
    CHECK_FALSE(deletedSymbolNode.value().has_value());

    auto keptSymbolNode = fix.store_->getNodeByKey("sym:keep-me");
    REQUIRE((keptSymbolNode.has_value()));
    REQUIRE((keptSymbolNode.value().has_value()));

    auto edgeNodeIds = fix.store_->upsertNodes({KGNode{.nodeKey = "doc:cleanup-src",
                                                       .label = std::string("cleanup-src"),
                                                       .type = std::string("document")},
                                                KGNode{.nodeKey = "doc:cleanup-mid",
                                                       .label = std::string("cleanup-mid"),
                                                       .type = std::string("document")},
                                                KGNode{.nodeKey = "doc:cleanup-dst",
                                                       .label = std::string("cleanup-dst"),
                                                       .type = std::string("document")}});
    REQUIRE((edgeNodeIds.has_value()));

    REQUIRE((fix.store_
                 ->addEdgesUnique(
                     {KGEdge{.srcNodeId = edgeNodeIds.value()[0],
                             .dstNodeId = edgeNodeIds.value()[1],
                             .relation = "cleanup-relation",
                             .weight = 1.0F,
                             .properties = std::string(R"({"source_file":"/tmp/cleanup.cpp"})")},
                      KGEdge{.srcNodeId = edgeNodeIds.value()[0],
                             .dstNodeId = edgeNodeIds.value()[2],
                             .relation = "cleanup-relation",
                             .weight = 1.0F,
                             .properties = std::string(R"({"source_file":"/tmp/other.cpp"})")},
                      KGEdge{.srcNodeId = edgeNodeIds.value()[1],
                             .dstNodeId = edgeNodeIds.value()[2],
                             .relation = "keep-relation",
                             .weight = 1.0F,
                             .properties = std::string(R"({"source_file":"/tmp/cleanup.cpp"})")}})
                 .has_value()));

    auto deletedBySource = fix.store_->deleteEdgesForSourceFile("/tmp/cleanup.cpp");
    REQUIRE((deletedBySource.has_value()));
    CHECK((deletedBySource.value() == 2));

    auto cleanupEdges = fix.store_->getEdgesFrom(edgeNodeIds.value()[0], "cleanup-relation", 10, 0);
    REQUIRE((cleanupEdges.has_value()));
    REQUIRE((cleanupEdges.value().size() == 1));
    CHECK((cleanupEdges.value()[0].dstNodeId == edgeNodeIds.value()[2]));

    auto keepEdges = fix.store_->getEdgesFrom(edgeNodeIds.value()[1], "keep-relation", 10, 0);
    REQUIRE((keepEdges.has_value()));
    CHECK((keepEdges.value().empty()));

    auto deletedByRelation = fix.store_->deleteEdgesByRelation("cleanup-relation");
    REQUIRE((deletedByRelation.has_value()));
    CHECK((deletedByRelation.value() == 1));

    auto cleanupEdgesAfter =
        fix.store_->getEdgesFrom(edgeNodeIds.value()[0], "cleanup-relation", 10, 0);
    REQUIRE((cleanupEdgesAfter.has_value()));
    CHECK((cleanupEdgesAfter.value().empty()));
}

TEST_CASE("KG Store: orphan cleanup removes invalid rows and preserves valid rows",
          "[unit][metadata][kg]") {
    KGStoreRepoFixture fix;

    auto docId = fix.insertDocument("hash-valid-doc", "/tmp/valid-doc.cpp");
    auto nodeIds = fix.store_->upsertNodes({KGNode{.nodeKey = "doc:valid-src",
                                                   .label = std::string("valid-src"),
                                                   .type = std::string("document")},
                                            KGNode{.nodeKey = "doc:valid-dst",
                                                   .label = std::string("valid-dst"),
                                                   .type = std::string("document")},
                                            KGNode{.nodeKey = "sym:valid-entity",
                                                   .label = std::string("valid-entity"),
                                                   .type = std::string("symbol")}});
    REQUIRE((nodeIds.has_value()));

    REQUIRE((fix.store_
                 ->addEdge(KGEdge{.srcNodeId = nodeIds.value()[0],
                                  .dstNodeId = nodeIds.value()[1],
                                  .relation = "valid-relation",
                                  .weight = 1.0F})
                 .has_value()));

    REQUIRE((fix.store_
                 ->addDocEntities({DocEntity{.documentId = docId,
                                             .entityText = std::string("ValidEntity"),
                                             .nodeId = nodeIds.value()[2],
                                             .startOffset = 0,
                                             .endOffset = 11,
                                             .confidence = 1.0F,
                                             .extractor = std::string("test")}})
                 .has_value()));

    REQUIRE((fix.insertOrphanedEdges(1001, "orphan-relation").has_value()));
    REQUIRE((fix.insertOrphanedDocEntities(1001, 987654321).has_value()));

    auto deletedEdges = fix.store_->deleteOrphanedEdges();
    REQUIRE((deletedEdges.has_value()));
    CHECK((deletedEdges.value() == 1001));

    auto validEdges = fix.store_->getEdgesFrom(nodeIds.value()[0], "valid-relation", 10, 0);
    REQUIRE((validEdges.has_value()));
    REQUIRE((validEdges.value().size() == 1));
    CHECK((validEdges.value()[0].dstNodeId == nodeIds.value()[1]));

    auto deletedEntities = fix.store_->deleteOrphanedDocEntities();
    REQUIRE((deletedEntities.has_value()));
    CHECK((deletedEntities.value() == 1001));

    auto validEntities = fix.store_->getDocEntitiesForDocument(docId, 10, 0);
    REQUIRE((validEntities.has_value()));
    REQUIRE((validEntities.value().size() == 1));
    CHECK((validEntities.value()[0].entityText == "ValidEntity"));
}

TEST_CASE("KG Store: document hash and extraction state helpers handle missing and updated rows",
          "[unit][metadata][kg]") {
    KGStoreRepoFixture fix;

    const auto docId = fix.insertDocument("hash-state-doc", "/tmp/state-doc.cpp");

    auto hashById = fix.store_->getDocumentHashById(docId);
    REQUIRE((hashById.has_value()));
    REQUIRE((hashById.value().has_value()));
    CHECK((hashById.value().value() == "hash-state-doc"));

    auto missingHashById = fix.store_->getDocumentHashById(docId + 1000);
    REQUIRE((missingHashById.has_value()));
    CHECK_FALSE(missingHashById.value().has_value());

    auto missingState = fix.store_->getSymbolExtractionState("hash-state-doc");
    REQUIRE((missingState.has_value()));
    CHECK_FALSE(missingState.value().has_value());

    auto missingDocumentState = fix.store_->getSymbolExtractionState("hash-missing-doc");
    REQUIRE((missingDocumentState.has_value()));
    CHECK_FALSE(missingDocumentState.value().has_value());

    SymbolExtractionState initialState;
    initialState.documentId = 999999;
    initialState.extractorId = "symbol_extractor:v1";
    initialState.extractorConfigHash = "config-a";
    initialState.extractedAt = 111;
    initialState.status = "complete";
    initialState.entityCount = 2;
    REQUIRE((fix.store_->upsertSymbolExtractionState("hash-state-doc", initialState).has_value()));

    auto storedState = fix.store_->getSymbolExtractionState("hash-state-doc");
    REQUIRE((storedState.has_value()));
    REQUIRE((storedState.value().has_value()));
    CHECK((storedState.value()->documentId == docId));
    CHECK((storedState.value()->extractorId == "symbol_extractor:v1"));
    REQUIRE((storedState.value()->extractorConfigHash.has_value()));
    CHECK((storedState.value()->extractorConfigHash.value() == "config-a"));
    CHECK((storedState.value()->extractedAt == 111));
    CHECK((storedState.value()->status == "complete"));
    CHECK((storedState.value()->entityCount == 2));
    CHECK((storedState.value()->errorMessage.value_or("") == ""));

    SymbolExtractionState updatedState;
    updatedState.documentId = -1;
    updatedState.extractorId = "symbol_extractor:v2";
    updatedState.extractorConfigHash = "config-b";
    updatedState.extractedAt = 222;
    updatedState.status = "failed";
    updatedState.entityCount = 5;
    updatedState.errorMessage = "syntax error";
    REQUIRE((fix.store_->upsertSymbolExtractionState("hash-state-doc", updatedState).has_value()));

    auto refreshedState = fix.store_->getSymbolExtractionState("hash-state-doc");
    REQUIRE((refreshedState.has_value()));
    REQUIRE((refreshedState.value().has_value()));
    CHECK((refreshedState.value()->documentId == docId));
    CHECK((refreshedState.value()->extractorId == "symbol_extractor:v2"));
    REQUIRE((refreshedState.value()->extractorConfigHash.has_value()));
    CHECK((refreshedState.value()->extractorConfigHash.value() == "config-b"));
    CHECK((refreshedState.value()->extractedAt == 222));
    CHECK((refreshedState.value()->status == "failed"));
    CHECK((refreshedState.value()->entityCount == 5));
    REQUIRE((refreshedState.value()->errorMessage.has_value()));
    CHECK((refreshedState.value()->errorMessage.value() == "syntax error"));

    auto missingUpsert = fix.store_->upsertSymbolExtractionState("hash-no-document", updatedState);
    REQUIRE_FALSE((missingUpsert.has_value()));
    CHECK((missingUpsert.error().code == ErrorCode::NotFound));
    CHECK((missingUpsert.error().message.find("hash-no-document") != std::string::npos));
}

TEST_CASE("KG Store: symbol metadata queries support update, filter, pagination, and delete",
          "[unit][metadata][kg]") {
    KGStoreRepoFixture fix;

    fix.insertDocument("hash-symbol-a", "/tmp/symbol-a.cpp");
    fix.insertDocument("hash-symbol-b", "/tmp/symbol-b.cpp");

    auto makeSymbol = [](std::string documentHash, std::string filePath, std::string symbolName,
                         std::string qualifiedName, std::string kind,
                         std::optional<std::string> returnType = std::nullopt,
                         std::optional<std::string> documentation = std::nullopt) {
        SymbolMetadata symbol;
        symbol.documentHash = std::move(documentHash);
        symbol.filePath = std::move(filePath);
        symbol.symbolName = std::move(symbolName);
        symbol.qualifiedName = std::move(qualifiedName);
        symbol.kind = std::move(kind);
        symbol.startLine = 10;
        symbol.endLine = 20;
        symbol.startOffset = 100;
        symbol.endOffset = 200;
        symbol.returnType = std::move(returnType);
        symbol.parameters = std::string("[\"int\"]");
        symbol.documentation = std::move(documentation);
        return symbol;
    };

    std::vector<SymbolMetadata> initialSymbols{
        makeSymbol("hash-symbol-a", "/tmp/symbol-a.cpp", "Alpha", "ns::Alpha", "class",
                   std::nullopt, "alpha-doc"),
        makeSymbol("hash-symbol-a", "/tmp/symbol-a.cpp", "buildAlpha", "ns::buildAlpha", "function",
                   "Widget", "builder-doc"),
        makeSymbol("hash-symbol-b", "/tmp/symbol-b.cpp", "Beta", "ns::Beta", "function", "int",
                   "beta-doc"),
    };
    REQUIRE((fix.store_->upsertSymbolMetadata(initialSymbols).has_value()));

    auto allSymbols =
        fix.store_->querySymbolMetadata(std::nullopt, std::nullopt, std::nullopt, 10, 0);
    REQUIRE((allSymbols.has_value()));
    REQUIRE((allSymbols.value().size() == 3));
    CHECK((allSymbols.value()[0].qualifiedName == "ns::Alpha"));
    CHECK((allSymbols.value()[1].qualifiedName == "ns::Beta"));
    CHECK((allSymbols.value()[2].qualifiedName == "ns::buildAlpha"));

    auto fileMatches =
        fix.store_->querySymbolMetadata("symbol-a.cpp", std::nullopt, std::nullopt, 10, 0);
    REQUIRE((fileMatches.has_value()));
    REQUIRE((fileMatches.value().size() == 2));

    auto functionMatches =
        fix.store_->querySymbolMetadata(std::nullopt, "function", std::nullopt, 10, 0);
    REQUIRE((functionMatches.has_value()));
    REQUIRE((functionMatches.value().size() == 2));
    CHECK((functionMatches.value()[0].qualifiedName == "ns::Beta"));
    CHECK((functionMatches.value()[1].qualifiedName == "ns::buildAlpha"));

    auto nameMatches = fix.store_->querySymbolMetadata(std::nullopt, std::nullopt, "Alpha", 10, 0);
    REQUIRE((nameMatches.has_value()));
    REQUIRE((nameMatches.value().size() == 2));
    CHECK((nameMatches.value()[0].qualifiedName == "ns::Alpha"));
    CHECK((nameMatches.value()[1].qualifiedName == "ns::buildAlpha"));

    auto pagedSymbols =
        fix.store_->querySymbolMetadata(std::nullopt, std::nullopt, std::nullopt, 1, 1);
    REQUIRE((pagedSymbols.has_value()));
    REQUIRE((pagedSymbols.value().size() == 1));
    CHECK((pagedSymbols.value()[0].qualifiedName == "ns::Beta"));

    auto updateBatchResult = fix.store_->beginWriteBatch();
    REQUIRE((updateBatchResult.has_value()));
    auto updateBatch = std::move(updateBatchResult).value();
    REQUIRE((updateBatch
                 ->upsertSymbolMetadata(
                     {makeSymbol("hash-symbol-a", "/tmp/renamed-symbol-a.cpp", "buildAlpha",
                                 "ns::buildAlpha", "method", "WidgetRef", "updated-builder-doc")})
                 .has_value()));
    REQUIRE((updateBatch->commit().has_value()));

    auto updatedMatch =
        fix.store_->querySymbolMetadata("renamed-symbol-a.cpp", "method", "buildAlpha", 10, 0);
    REQUIRE((updatedMatch.has_value()));
    REQUIRE((updatedMatch.value().size() == 1));
    CHECK((updatedMatch.value()[0].qualifiedName == "ns::buildAlpha"));
    REQUIRE((updatedMatch.value()[0].returnType.has_value()));
    CHECK((updatedMatch.value()[0].returnType.value() == "WidgetRef"));
    REQUIRE((updatedMatch.value()[0].documentation.has_value()));
    CHECK((updatedMatch.value()[0].documentation.value() == "updated-builder-doc"));

    auto deletedCount = fix.store_->deleteSymbolMetadataForDocument("hash-symbol-a");
    REQUIRE((deletedCount.has_value()));
    CHECK((deletedCount.value() == 2));

    auto remainingSymbols =
        fix.store_->querySymbolMetadata(std::nullopt, std::nullopt, std::nullopt, 10, 0);
    REQUIRE((remainingSymbols.has_value()));
    REQUIRE((remainingSymbols.value().size() == 1));
    CHECK((remainingSymbols.value()[0].documentHash == "hash-symbol-b"));

    auto missingDelete = fix.store_->deleteSymbolMetadataForDocument("hash-missing-symbols");
    REQUIRE((missingDelete.has_value()));
    CHECK((missingDelete.value() == 0));
}

TEST_CASE("KG Store: maintenance helpers summarize graph state and search labels",
          "[unit][metadata][kg]") {
    KGStoreFixture fix;

    auto nodeIds = fix.store_->upsertNodes(
        {KGNode{
             .nodeKey = "ent:alpha", .label = std::string("alpha"), .type = std::string("entity")},
         KGNode{.nodeKey = "ent:alphabet",
                .label = std::string("alphabet"),
                .type = std::string("entity")},
         KGNode{.nodeKey = "tag:beta", .label = std::string("beta"), .type = std::string("tag")},
         KGNode{.nodeKey = "misc:untyped", .label = std::string("omega")},
         KGNode{.nodeKey = "misc:empty-type",
                .label = std::string("epsilon"),
                .type = std::string("")}});
    REQUIRE((nodeIds.has_value()));

    REQUIRE((fix.store_
                 ->addEdgesUnique({KGEdge{.srcNodeId = nodeIds.value()[0],
                                          .dstNodeId = nodeIds.value()[2],
                                          .relation = "REL_A",
                                          .weight = 1.0F},
                                   KGEdge{.srcNodeId = nodeIds.value()[1],
                                          .dstNodeId = nodeIds.value()[2],
                                          .relation = "REL_A",
                                          .weight = 1.0F},
                                   KGEdge{.srcNodeId = nodeIds.value()[2],
                                          .dstNodeId = nodeIds.value()[0],
                                          .relation = "REL_B",
                                          .weight = 1.0F}})
                 .has_value()));

    auto nodeCounts = fix.store_->getNodeTypeCounts();
    REQUIRE((nodeCounts.has_value()));
    REQUIRE((nodeCounts.value().size() == 2));
    CHECK((nodeCounts.value()[0] == std::pair<std::string, std::size_t>{"entity", 2}));
    CHECK((nodeCounts.value()[1] == std::pair<std::string, std::size_t>{"tag", 1}));

    auto relationCounts = fix.store_->getRelationTypeCounts();
    REQUIRE((relationCounts.has_value()));
    REQUIRE((relationCounts.value().size() == 2));
    CHECK((relationCounts.value()[0] == std::pair<std::string, std::size_t>{"REL_A", 2}));
    CHECK((relationCounts.value()[1] == std::pair<std::string, std::size_t>{"REL_B", 1}));

    auto caseInsensitiveMatches = fix.store_->searchNodesByLabel("ALP*", 10, 0);
    REQUIRE((caseInsensitiveMatches.has_value()));
    REQUIRE((caseInsensitiveMatches.value().size() == 2));
    CHECK((caseInsensitiveMatches.value()[0].nodeKey == "ent:alpha"));
    CHECK((caseInsensitiveMatches.value()[1].nodeKey == "ent:alphabet"));

    auto singleWildcardMatch = fix.store_->searchNodesByLabel("bet?", 10, 0);
    REQUIRE((singleWildcardMatch.has_value()));
    REQUIRE((singleWildcardMatch.value().size() == 1));
    CHECK((singleWildcardMatch.value()[0].nodeKey == "tag:beta"));

    auto pagedMatches = fix.store_->searchNodesByLabel("alp*", 1, 1);
    REQUIRE((pagedMatches.has_value()));
    REQUIRE((pagedMatches.value().size() == 1));
    CHECK((pagedMatches.value()[0].nodeKey == "ent:alphabet"));

    REQUIRE((fix.store_->optimize().has_value()));
    REQUIRE((fix.store_->healthCheck().has_value()));
}

TEST_CASE("KG Store: healthCheck uses read pool instead of the write pool",
          "[unit][metadata][kg][contention]") {
    const auto dbPath = tempDbPath("kg_store_health_read_pool_");

    auto bootstrap = makeSqliteKnowledgeGraphStore(dbPath.string(), KnowledgeGraphStoreConfig{});
    REQUIRE((bootstrap.has_value()));
    bootstrap.value().reset();

    ConnectionPoolConfig writeCfg;
    writeCfg.minConnections = 1;
    writeCfg.maxConnections = 1;
    auto writePool = std::make_unique<ConnectionPool>(dbPath.string(), writeCfg);
    REQUIRE((writePool->initialize().has_value()));

    ConnectionPoolConfig readCfg;
    readCfg.minConnections = 1;
    readCfg.maxConnections = 1;
    readCfg.readOnly = true;
    auto readPool = std::make_unique<ConnectionPool>(dbPath.string(), readCfg);
    REQUIRE((readPool->initialize().has_value()));

    auto storeResult = makeSqliteKnowledgeGraphStore(*writePool, KnowledgeGraphStoreConfig{});
    REQUIRE((storeResult.has_value()));
    auto store = std::move(storeResult.value());
    store->setReadPool(readPool.get());

    auto heldWriteConnResult = writePool->acquire();
    REQUIRE((heldWriteConnResult.has_value()));
    auto heldWriteConn = std::move(heldWriteConnResult.value());

    auto future = std::async(std::launch::async, [&]() { return store->healthCheck(); });
    CHECK((future.wait_for(std::chrono::milliseconds(500)) == std::future_status::ready));
    auto healthResult = future.get();
    REQUIRE((healthResult.has_value()));

    heldWriteConn.reset();
    store.reset();
    readPool->shutdown();
    writePool->shutdown();
    readPool.reset();
    writePool.reset();
    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
}

TEST_CASE("KG Store: write batch cleanup helpers commit scoped deletions", "[unit][metadata][kg]") {
    KGStoreRepoFixture fix;

    auto docId = fix.insertDocument("hash-batch-doc", "/tmp/batch-doc.cpp");
    auto docNodeId = fix.store_->ensureDocumentNode("hash-batch-delete", "/tmp/batch-delete.cpp");
    REQUIRE((docNodeId.has_value()));

    auto seededIds = fix.store_->upsertNodes(
        {KGNode{.nodeKey = "sym:batch-delete",
                .label = std::string("BatchDelete"),
                .type = std::string("symbol"),
                .properties = std::string(R"({"document_hash":"hash-batch-delete"})")},
         KGNode{.nodeKey = "sym:batch-keep",
                .label = std::string("BatchKeep"),
                .type = std::string("symbol"),
                .properties = std::string(R"({"document_hash":"hash-batch-keep"})")},
         KGNode{.nodeKey = "doc:batch-src",
                .label = std::string("batch-src"),
                .type = std::string("document")},
         KGNode{.nodeKey = "doc:batch-mid",
                .label = std::string("batch-mid"),
                .type = std::string("document")},
         KGNode{.nodeKey = "doc:batch-dst",
                .label = std::string("batch-dst"),
                .type = std::string("document")}});
    REQUIRE((seededIds.has_value()));

    REQUIRE((fix.store_
                 ->addEdgesUnique(
                     {KGEdge{.srcNodeId = seededIds.value()[2],
                             .dstNodeId = seededIds.value()[3],
                             .relation = "batch-relation",
                             .weight = 1.0F,
                             .properties = std::string(R"({"source_file":"/tmp/batch.cpp"})")},
                      KGEdge{.srcNodeId = seededIds.value()[2],
                             .dstNodeId = seededIds.value()[4],
                             .relation = "batch-relation",
                             .weight = 1.0F,
                             .properties = std::string(R"({"source_file":"/tmp/keep.cpp"})")}})
                 .has_value()));

    REQUIRE((fix.store_
                 ->addDocEntities({DocEntity{.documentId = docId,
                                             .entityText = std::string("BatchEntity"),
                                             .nodeId = seededIds.value()[1],
                                             .startOffset = 0,
                                             .endOffset = 11,
                                             .confidence = 1.0F,
                                             .extractor = std::string("test")}})
                 .has_value()));

    REQUIRE((fix.insertOrphanedEdges(3, "batch-orphan-relation").has_value()));
    REQUIRE((fix.insertOrphanedDocEntities(2, 123456789).has_value()));

    auto batchResult = fix.store_->beginWriteBatch();
    REQUIRE((batchResult.has_value()));
    auto batch = std::move(batchResult).value();

    auto deletedNodes = batch->deleteNodesForDocumentHash("hash-batch-delete");
    REQUIRE((deletedNodes.has_value()));
    CHECK((deletedNodes.value() == 2));

    auto deletedBySource = batch->deleteEdgesForSourceFile("/tmp/batch.cpp");
    REQUIRE((deletedBySource.has_value()));
    CHECK((deletedBySource.value() == 1));

    auto deletedByRelation = batch->deleteEdgesByRelation("batch-relation");
    REQUIRE((deletedByRelation.has_value()));
    CHECK((deletedByRelation.value() == 1));

    auto deletedOrphanEdges = batch->deleteOrphanedEdges();
    REQUIRE((deletedOrphanEdges.has_value()));
    CHECK((deletedOrphanEdges.value() == 3));

    auto deletedOrphanEntities = batch->deleteOrphanedDocEntities();
    REQUIRE((deletedOrphanEntities.has_value()));
    CHECK((deletedOrphanEntities.value() == 2));

    REQUIRE((batch->commit().has_value()));

    auto deletedDocNode = fix.store_->getNodeByKey("doc:hash-batch-delete");
    REQUIRE((deletedDocNode.has_value()));
    CHECK_FALSE(deletedDocNode.value().has_value());

    auto keptSymbolNode = fix.store_->getNodeByKey("sym:batch-keep");
    REQUIRE((keptSymbolNode.has_value()));
    REQUIRE((keptSymbolNode.value().has_value()));

    auto remainingBatchEdges =
        fix.store_->getEdgesFrom(seededIds.value()[2], "batch-relation", 10, 0);
    REQUIRE((remainingBatchEdges.has_value()));
    CHECK((remainingBatchEdges.value().empty()));

    auto validEntities = fix.store_->getDocEntitiesForDocument(docId, 10, 0);
    REQUIRE((validEntities.has_value()));
    REQUIRE((validEntities.value().size() == 1));
    CHECK((validEntities.value()[0].entityText == "BatchEntity"));
}
