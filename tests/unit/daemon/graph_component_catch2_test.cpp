// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: Apache-2.0
// GraphComponent test suite (Catch2)
// Covers: Graph lifecycle, document ingestion, maintenance operations

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/GraphComponent.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>

#include <filesystem>
#include <memory>

using namespace yams::daemon;
using namespace yams::metadata;

namespace {

// Test fixture helper for GraphComponent tests
struct GraphComponentTestFixture {
    GraphComponentTestFixture() {
        testDir = std::filesystem::temp_directory_path() / "yams_graph_component_test";
        std::filesystem::create_directories(testDir);
        dbPath = testDir / "test.db";

        ConnectionPoolConfig poolConfig;
        poolConfig.minConnections = 1;
        poolConfig.maxConnections = 2;
        pool = std::make_shared<ConnectionPool>(dbPath.string(), poolConfig);
        auto poolInitRes = pool->initialize();
        REQUIRE(poolInitRes.has_value());

        metadataRepo = std::make_shared<MetadataRepository>(*pool);

        // Create KG store using factory function and wire it to metadata repo
        KnowledgeGraphStoreConfig kgConfig;
        kgConfig.enable_alias_fts = true;
        kgConfig.enable_wal = false;
        auto kgResult = makeSqliteKnowledgeGraphStore(*pool, kgConfig);
        REQUIRE(kgResult.has_value());
        kgStore = std::shared_ptr<KnowledgeGraphStore>(std::move(kgResult).value());
        metadataRepo->setKnowledgeGraphStore(kgStore);
    }

    ~GraphComponentTestFixture() {
        kgStore.reset();
        metadataRepo.reset();
        pool.reset();
        std::filesystem::remove_all(testDir);
    }

    std::filesystem::path testDir;
    std::filesystem::path dbPath;
    std::shared_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> metadataRepo;
    std::shared_ptr<KnowledgeGraphStore> kgStore;
};

} // namespace

// =============================================================================
// GraphComponent Lifecycle Tests
// =============================================================================

TEST_CASE("GraphComponent: Initialize and shutdown", "[daemon][graph][lifecycle]") {
    GraphComponentTestFixture fixture;
    GraphComponent component(fixture.metadataRepo, fixture.kgStore);

    REQUIRE_FALSE(component.isReady());

    auto initResult = component.initialize();
    REQUIRE(initResult.has_value());
    REQUIRE(component.isReady());

    component.shutdown();
    REQUIRE_FALSE(component.isReady());
}

TEST_CASE("GraphComponent: Initialize without dependencies", "[daemon][graph][lifecycle]") {
    GraphComponentTestFixture fixture;
    GraphComponent component(nullptr, nullptr);

    auto initResult = component.initialize();
    REQUIRE_FALSE(initResult.has_value());
    REQUIRE_FALSE(component.isReady());
}

TEST_CASE("GraphComponent: Get query service after init", "[daemon][graph][lifecycle]") {
    GraphComponentTestFixture fixture;
    GraphComponent component(fixture.metadataRepo, fixture.kgStore);

    REQUIRE(component.getQueryService() == nullptr);

    auto initResult = component.initialize();
    REQUIRE(initResult.has_value());

    auto queryService = component.getQueryService();
    REQUIRE(queryService != nullptr);

    component.shutdown();
    REQUIRE(component.getQueryService() == nullptr);
}

TEST_CASE("GraphComponent: Multiple initializations are idempotent", "[daemon][graph][lifecycle]") {
    GraphComponentTestFixture fixture;
    GraphComponent component(fixture.metadataRepo, fixture.kgStore);

    auto init1 = component.initialize();
    REQUIRE(init1.has_value());
    REQUIRE(component.isReady());

    auto init2 = component.initialize();
    REQUIRE(init2.has_value());
    REQUIRE(component.isReady());
}

TEST_CASE("GraphComponent: Multiple shutdowns are safe", "[daemon][graph][lifecycle]") {
    GraphComponentTestFixture fixture;
    GraphComponent component(fixture.metadataRepo, fixture.kgStore);
    auto initResult = component.initialize();
    REQUIRE(initResult.has_value());

    component.shutdown();
    REQUIRE_FALSE(component.isReady());

    component.shutdown();
    REQUIRE_FALSE(component.isReady());
}

// =============================================================================
// GraphComponent Document Ingestion Tests
// =============================================================================

TEST_CASE("GraphComponent: Document ingestion succeeds (stub)", "[daemon][graph][ingestion]") {
    // NOTE: onDocumentIngested is currently a stub that accepts context but
    // does not create nodes. This test verifies the stub returns success.
    // When implementation is added, update this test to verify node creation.
    GraphComponentTestFixture fixture;
    GraphComponent component(fixture.metadataRepo, fixture.kgStore);
    auto initResult = component.initialize();
    REQUIRE(initResult.has_value());

    GraphComponent::DocumentGraphContext ctx{.documentHash = "abc123",
                                             .filePath = "/test/file.cpp",
                                             .tags = {"code", "cpp"},
                                             .documentDbId = 1};

    auto result = component.onDocumentIngested(ctx);
    REQUIRE(result.has_value());

    // TODO(graph-impl): When onDocumentIngested is fully implemented,
    // uncomment these assertions to verify node creation:
    // auto docNodeResult = fixture.kgStore->getNodeByKey("doc:abc123");
    // REQUIRE(docNodeResult.has_value());
    // REQUIRE(docNodeResult.value().has_value());
    // auto tagNodeResult = fixture.kgStore->getNodeByKey("tag:code");
    // REQUIRE(tagNodeResult.has_value());
    // REQUIRE(tagNodeResult.value().has_value());
}

TEST_CASE("GraphComponent: Dedupe predicate detects existing doc entities",
          "[daemon][graph][dedupe]") {
    GraphComponentTestFixture fixture;

    // Insert a document row.
    DocumentInfo doc;
    doc.fileName = "example.cpp";
    doc.filePath = "/tmp/example.cpp";
    doc.fileExtension = "cpp";
    doc.fileSize = 123;
    doc.sha256Hash = "hash-abc";
    doc.mimeType = "text/x-c++";
    doc.setCreatedTime(1);
    doc.setModifiedTime(1);
    doc.setIndexedTime(1);
    auto docIdRes = fixture.metadataRepo->insertDocument(doc);
    REQUIRE(docIdRes.has_value());

    // Seed a doc entity.
    KGNode sym;
    sym.nodeKey = "sym:hash-abc:F";
    sym.label = std::string("F");
    sym.type = std::string("symbol");
    auto symIdRes = fixture.kgStore->upsertNode(sym);
    REQUIRE(symIdRes.has_value());

    DocEntity de;
    de.documentId = docIdRes.value();
    de.entityText = "F";
    de.nodeId = symIdRes.value();
    de.startOffset = 0;
    de.endOffset = 1;
    de.confidence = 1.0f;
    de.extractor = std::string("test");
    REQUIRE(fixture.kgStore->addDocEntities({de}).has_value());

    CHECK(GraphComponent::shouldSkipEntityExtraction(fixture.kgStore, "hash-abc"));
    CHECK_FALSE(GraphComponent::shouldSkipEntityExtraction(fixture.kgStore, "hash-missing"));
    CHECK_FALSE(GraphComponent::shouldSkipEntityExtraction(nullptr, "hash-abc"));
}

TEST_CASE("GraphComponent: Document ingestion when not initialized", "[daemon][graph][ingestion]") {
    GraphComponentTestFixture fixture;
    GraphComponent component(fixture.metadataRepo, fixture.kgStore);

    GraphComponent::DocumentGraphContext ctx{
        .documentHash = "abc123", .filePath = "/test/file.cpp", .documentDbId = 1};

    auto result = component.onDocumentIngested(ctx);
    REQUIRE_FALSE(result.has_value());
}

// =============================================================================
// GraphComponent Maintenance Operations Tests
// =============================================================================

TEST_CASE("GraphComponent: Repair graph (stub)", "[daemon][graph][maintenance]") {
    // NOTE: repairGraph returns NotImplemented error for now.
    // This test verifies the stub behavior.
    GraphComponentTestFixture fixture;
    GraphComponent component(fixture.metadataRepo, fixture.kgStore);
    auto initResult = component.initialize();
    REQUIRE(initResult.has_value());

    auto result = component.repairGraph(true);
    // Currently returns NotImplemented - update when implemented
    REQUIRE_FALSE(result.has_value());

    // TODO(graph-impl): When repairGraph is fully implemented, verify stats:
    // REQUIRE(result.has_value());
    // const auto& stats = result.value();
    // REQUIRE(stats.nodesCreated == 0);
    // REQUIRE(stats.nodesUpdated == 0);
}

TEST_CASE("GraphComponent: Validate graph (stub)", "[daemon][graph][maintenance]") {
    GraphComponentTestFixture fixture;
    GraphComponent component(fixture.metadataRepo, fixture.kgStore);
    auto initResult = component.initialize();
    REQUIRE(initResult.has_value());

    auto result = component.validateGraph();
    REQUIRE(result.has_value());

    const auto& report = result.value();
    REQUIRE(report.totalNodes == 0);
    REQUIRE(report.totalEdges == 0);
}

TEST_CASE("GraphComponent: Get entity stats when no entity service", "[daemon][graph][stats]") {
    GraphComponentTestFixture fixture;
    GraphComponent component(fixture.metadataRepo, fixture.kgStore);
    auto initResult = component.initialize();
    REQUIRE(initResult.has_value());

    auto stats = component.getEntityStats();
    REQUIRE(stats.jobsAccepted == 0);
    REQUIRE(stats.jobsProcessed == 0);
    REQUIRE(stats.jobsFailed == 0);
}

// =============================================================================
// KnowledgeGraphStore Query Tests (yams-cqp)
// =============================================================================

TEST_CASE("KnowledgeGraphStore: findNodesByType with pagination", "[daemon][graph][query]") {
    GraphComponentTestFixture fixture;

    // Insert some test nodes
    for (int i = 0; i < 25; i++) {
        KGNode node;
        node.nodeKey = "fn:test:" + std::to_string(i);
        node.label = "TestFunction" + std::to_string(i);
        node.type = "function";
        auto r = fixture.kgStore->upsertNode(node);
        REQUIRE(r.has_value());
    }

    SECTION("List all nodes of type") {
        auto result = fixture.kgStore->findNodesByType("function", 100, 0);
        REQUIRE(result.has_value());
        REQUIRE(result.value().size() == 25);
    }

    SECTION("Pagination with limit") {
        auto result = fixture.kgStore->findNodesByType("function", 10, 0);
        REQUIRE(result.has_value());
        REQUIRE(result.value().size() == 10);
    }

    SECTION("Pagination with offset") {
        auto result = fixture.kgStore->findNodesByType("function", 10, 20);
        REQUIRE(result.has_value());
        REQUIRE(result.value().size() == 5); // Only 5 remaining after offset 20
    }

    SECTION("Pagination with offset and limit") {
        auto first = fixture.kgStore->findNodesByType("function", 5, 0);
        auto second = fixture.kgStore->findNodesByType("function", 5, 5);
        REQUIRE(first.has_value());
        REQUIRE(second.has_value());
        REQUIRE(first.value().size() == 5);
        REQUIRE(second.value().size() == 5);
        // Verify no overlap
        for (const auto& n1 : first.value()) {
            for (const auto& n2 : second.value()) {
                REQUIRE(n1.nodeKey != n2.nodeKey);
            }
        }
    }

    SECTION("countNodesByType returns full count") {
        auto count = fixture.kgStore->countNodesByType("function");
        REQUIRE(count.has_value());
        REQUIRE(count.value() == 25);
    }

    SECTION("Empty result for unknown type") {
        auto result = fixture.kgStore->findNodesByType("nonexistent_type", 100, 0);
        REQUIRE(result.has_value());
        REQUIRE(result.value().empty());
    }
}

TEST_CASE("KnowledgeGraphStore: findIsolatedNodes", "[daemon][graph][query]") {
    GraphComponentTestFixture fixture;

    // Create test nodes: 3 functions, 1 gets called, 2 are isolated
    KGNode fn1, fn2, fn3;
    fn1.nodeKey = "fn:called";
    fn1.label = "CalledFunction";
    fn1.type = "function";
    fn2.nodeKey = "fn:isolated1";
    fn2.label = "IsolatedFunction1";
    fn2.type = "function";
    fn3.nodeKey = "fn:isolated2";
    fn3.label = "IsolatedFunction2";
    fn3.type = "function";

    auto r1 = fixture.kgStore->upsertNode(fn1);
    auto r2 = fixture.kgStore->upsertNode(fn2);
    auto r3 = fixture.kgStore->upsertNode(fn3);
    REQUIRE(r1.has_value());
    REQUIRE(r2.has_value());
    REQUIRE(r3.has_value());

    // Create a CALLS edge to fn1 (making it not isolated)
    KGNode caller;
    caller.nodeKey = "fn:caller";
    caller.label = "CallerFunction";
    caller.type = "function";
    auto callerResult = fixture.kgStore->upsertNode(caller);
    REQUIRE(callerResult.has_value());

    KGEdge callEdge;
    callEdge.srcNodeId = callerResult.value();
    callEdge.dstNodeId = r1.value();
    callEdge.relation = "CALLS";
    auto edgeResult = fixture.kgStore->addEdge(callEdge);
    REQUIRE(edgeResult.has_value());

    SECTION("Find isolated functions (no incoming CALLS)") {
        auto result = fixture.kgStore->findIsolatedNodes("function", "CALLS", 100);
        REQUIRE(result.has_value());
        // Should find fn:isolated1, fn:isolated2, and fn:caller (which has no incoming CALLS)
        REQUIRE(result.value().size() == 3);
        // fn:called should NOT be in the list (it has an incoming CALLS edge)
        for (const auto& node : result.value()) {
            REQUIRE(node.nodeKey != "fn:called");
        }
    }

    SECTION("Find isolated with different relation") {
        auto result = fixture.kgStore->findIsolatedNodes("function", "IMPORTS", 100);
        REQUIRE(result.has_value());
        // All 4 functions should be isolated for IMPORTS (no IMPORTS edges exist)
        REQUIRE(result.value().size() == 4);
    }
}

TEST_CASE("KnowledgeGraphStore: getNodeTypeCounts", "[daemon][graph][query]") {
    GraphComponentTestFixture fixture;

    // Insert nodes of various types
    for (int i = 0; i < 10; i++) {
        KGNode node;
        node.nodeKey = "fn:func:" + std::to_string(i);
        node.label = "Function" + std::to_string(i);
        node.type = "function";
        REQUIRE(fixture.kgStore->upsertNode(node).has_value());
    }
    for (int i = 0; i < 5; i++) {
        KGNode node;
        node.nodeKey = "cls:class:" + std::to_string(i);
        node.label = "Class" + std::to_string(i);
        node.type = "class";
        REQUIRE(fixture.kgStore->upsertNode(node).has_value());
    }
    for (int i = 0; i < 3; i++) {
        KGNode node;
        node.nodeKey = "file:file:" + std::to_string(i);
        node.label = "File" + std::to_string(i);
        node.type = "file";
        REQUIRE(fixture.kgStore->upsertNode(node).has_value());
    }

    SECTION("Returns all types with counts") {
        auto result = fixture.kgStore->getNodeTypeCounts();
        REQUIRE(result.has_value());
        REQUIRE(result.value().size() == 3); // function, class, file
    }

    SECTION("Counts are correct and ordered by count desc") {
        auto result = fixture.kgStore->getNodeTypeCounts();
        REQUIRE(result.has_value());
        const auto& counts = result.value();

        // Should be ordered by count descending
        REQUIRE(counts[0].first == "function");
        REQUIRE(counts[0].second == 10);
        REQUIRE(counts[1].first == "class");
        REQUIRE(counts[1].second == 5);
        REQUIRE(counts[2].first == "file");
        REQUIRE(counts[2].second == 3);
    }
}

TEST_CASE("KnowledgeGraphStore: getNodeTypeCounts empty", "[daemon][graph][query]") {
    // Test empty result with fresh fixture (no nodes inserted)
    GraphComponentTestFixture fixture;
    auto result = fixture.kgStore->getNodeTypeCounts();
    REQUIRE(result.has_value());
    REQUIRE(result.value().empty());
}
