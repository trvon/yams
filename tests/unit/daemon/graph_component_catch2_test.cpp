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

TEST_CASE("GraphComponent: Document ingestion creates nodes", "[daemon][graph][ingestion]") {
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

    auto docNodeResult = fixture.kgStore->getNodeByKey("doc:abc123");
    REQUIRE(docNodeResult.has_value());
    REQUIRE(docNodeResult.value().has_value());

    auto tagNodeResult = fixture.kgStore->getNodeByKey("tag:code");
    REQUIRE(tagNodeResult.has_value());
    REQUIRE(tagNodeResult.value().has_value());
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
    GraphComponentTestFixture fixture;
    GraphComponent component(fixture.metadataRepo, fixture.kgStore);
    auto initResult = component.initialize();
    REQUIRE(initResult.has_value());

    auto result = component.repairGraph(true);
    REQUIRE(result.has_value());

    const auto& stats = result.value();
    REQUIRE(stats.nodesCreated == 0);
    REQUIRE(stats.nodesUpdated == 0);
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
