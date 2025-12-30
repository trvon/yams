// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: Apache-2.0

#include <catch2/catch_test_macros.hpp>

#include <yams/app/services/graph_query_service.hpp>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>

#include <filesystem>
#include <memory>

using namespace yams::app::services;
using namespace yams::metadata;

namespace {

struct GraphQueryServiceFixture {
    GraphQueryServiceFixture() {
        testDir = std::filesystem::temp_directory_path() / "yams_graph_query_service_test";
        std::filesystem::create_directories(testDir);
        dbPath = testDir / "test.db";

        ConnectionPoolConfig poolConfig;
        poolConfig.minConnections = 1;
        poolConfig.maxConnections = 2;
        pool = std::make_shared<ConnectionPool>(dbPath.string(), poolConfig);
        auto poolInitRes = pool->initialize();
        REQUIRE(poolInitRes.has_value());

        metadataRepo = std::make_shared<MetadataRepository>(*pool);

        KnowledgeGraphStoreConfig kgConfig;
        kgConfig.enable_alias_fts = true;
        kgConfig.enable_wal = false;
        auto kgResult = makeSqliteKnowledgeGraphStore(*pool, kgConfig);
        REQUIRE(kgResult.has_value());
        kgStore = std::shared_ptr<KnowledgeGraphStore>(std::move(kgResult).value());
        metadataRepo->setKnowledgeGraphStore(kgStore);
    }

    ~GraphQueryServiceFixture() {
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

TEST_CASE("GraphQueryService: relation name filters apply", "[services][graph]") {
    GraphQueryServiceFixture fixture;
    auto service = makeGraphQueryService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE(service != nullptr);

    KGNode caller;
    caller.nodeKey = "symbol:caller";
    caller.label = "caller";
    caller.type = "function";
    auto callerId = fixture.kgStore->upsertNode(caller);
    REQUIRE(callerId.has_value());

    KGNode callee;
    callee.nodeKey = "symbol:callee";
    callee.label = "callee";
    callee.type = "function";
    auto calleeId = fixture.kgStore->upsertNode(callee);
    REQUIRE(calleeId.has_value());

    KGNode includeTarget;
    includeTarget.nodeKey = "file:/include/header.h";
    includeTarget.label = "header.h";
    includeTarget.type = "file";
    auto includeId = fixture.kgStore->upsertNode(includeTarget);
    REQUIRE(includeId.has_value());

    KGEdge callEdge;
    callEdge.srcNodeId = callerId.value();
    callEdge.dstNodeId = calleeId.value();
    callEdge.relation = "calls";
    callEdge.weight = 1.0f;
    REQUIRE(fixture.kgStore->addEdge(callEdge).has_value());

    KGEdge includeEdge;
    includeEdge.srcNodeId = callerId.value();
    includeEdge.dstNodeId = includeId.value();
    includeEdge.relation = "includes";
    includeEdge.weight = 1.0f;
    REQUIRE(fixture.kgStore->addEdge(includeEdge).has_value());

    GraphQueryRequest req;
    req.nodeId = callerId.value();
    req.maxDepth = 1;
    req.maxResults = 10;
    req.relationNames = {"calls"};

    auto result = service->query(req);
    REQUIRE(result.has_value());
    const auto& resp = result.value();

    REQUIRE(resp.allConnectedNodes.size() == 1);
    REQUIRE(resp.allConnectedNodes[0].nodeMetadata.node.nodeKey == "symbol:callee");
}

TEST_CASE("GraphQueryService: PathVersion includes has_version edges", "[services][graph]") {
    GraphQueryServiceFixture fixture;
    auto service = makeGraphQueryService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE(service != nullptr);

    KGNode pathNode;
    pathNode.nodeKey = "path:abc:/repo/file.cpp";
    pathNode.label = "/repo/file.cpp";
    pathNode.type = "path";
    auto pathId = fixture.kgStore->upsertNode(pathNode);
    REQUIRE(pathId.has_value());

    KGNode blobNode;
    blobNode.nodeKey = "blob:deadbeef";
    blobNode.label = "deadbeef";
    blobNode.type = "blob";
    auto blobId = fixture.kgStore->upsertNode(blobNode);
    REQUIRE(blobId.has_value());

    KGEdge versionEdge;
    versionEdge.srcNodeId = pathId.value();
    versionEdge.dstNodeId = blobId.value();
    versionEdge.relation = "has_version";
    versionEdge.weight = 1.0f;
    REQUIRE(fixture.kgStore->addEdge(versionEdge).has_value());

    GraphQueryRequest req;
    req.nodeId = pathId.value();
    req.maxDepth = 1;
    req.maxResults = 10;
    req.relationFilters = {GraphRelationType::PathVersion};

    auto result = service->query(req);
    REQUIRE(result.has_value());
    const auto& resp = result.value();

    REQUIRE(resp.allConnectedNodes.size() == 1);
    REQUIRE(resp.allConnectedNodes[0].nodeMetadata.node.nodeKey == "blob:deadbeef");
}

TEST_CASE("GraphQueryService: RenamedFrom includes renamed_to edges", "[services][graph]") {
    GraphQueryServiceFixture fixture;
    auto service = makeGraphQueryService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE(service != nullptr);

    KGNode oldPath;
    oldPath.nodeKey = "path:snap1:/repo/old.cpp";
    oldPath.label = "/repo/old.cpp";
    oldPath.type = "path";
    auto oldId = fixture.kgStore->upsertNode(oldPath);
    REQUIRE(oldId.has_value());

    KGNode newPath;
    newPath.nodeKey = "path:snap2:/repo/new.cpp";
    newPath.label = "/repo/new.cpp";
    newPath.type = "path";
    auto newId = fixture.kgStore->upsertNode(newPath);
    REQUIRE(newId.has_value());

    KGEdge renameEdge;
    renameEdge.srcNodeId = oldId.value();
    renameEdge.dstNodeId = newId.value();
    renameEdge.relation = "renamed_to";
    renameEdge.weight = 1.0f;
    REQUIRE(fixture.kgStore->addEdge(renameEdge).has_value());

    GraphQueryRequest req;
    req.nodeId = oldId.value();
    req.maxDepth = 1;
    req.maxResults = 10;
    req.relationFilters = {GraphRelationType::RenamedFrom};

    auto result = service->query(req);
    REQUIRE(result.has_value());
    const auto& resp = result.value();

    REQUIRE(resp.allConnectedNodes.size() == 1);
    REQUIRE(resp.allConnectedNodes[0].nodeMetadata.node.nodeKey == "path:snap2:/repo/new.cpp");
}
