// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

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
    includeTarget.nodeKey = "path:file:/include/header.h";
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

TEST_CASE("GraphQueryService: relation names are canonicalized", "[services][graph]") {
    GraphQueryServiceFixture fixture;
    auto service = makeGraphQueryService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE(service != nullptr);

    KGNode caller;
    caller.nodeKey = "symbol:canonical:caller";
    caller.label = "caller";
    caller.type = "function";
    auto callerId = fixture.kgStore->upsertNode(caller);
    REQUIRE(callerId.has_value());

    KGNode callee;
    callee.nodeKey = "symbol:canonical:callee";
    callee.label = "callee";
    callee.type = "function";
    auto calleeId = fixture.kgStore->upsertNode(callee);
    REQUIRE(calleeId.has_value());

    KGEdge callEdge;
    callEdge.srcNodeId = callerId.value();
    callEdge.dstNodeId = calleeId.value();
    callEdge.relation = "calls";
    callEdge.weight = 1.0f;
    REQUIRE(fixture.kgStore->addEdge(callEdge).has_value());

    GraphQueryRequest req;
    req.nodeId = callerId.value();
    req.maxDepth = 1;
    req.maxResults = 10;
    req.relationNames = {"CALL"};

    auto result = service->query(req);
    REQUIRE(result.has_value());
    const auto& resp = result.value();
    REQUIRE(resp.allConnectedNodes.size() == 1);
    REQUIRE(resp.allConnectedNodes[0].nodeMetadata.node.nodeKey == "symbol:canonical:callee");
}

TEST_CASE("GraphQueryService: SymbolReference enum includes calls", "[services][graph]") {
    GraphQueryServiceFixture fixture;
    auto service = makeGraphQueryService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE(service != nullptr);

    KGNode caller;
    caller.nodeKey = "symbol:enum:caller";
    caller.label = "caller";
    caller.type = "function";
    auto callerId = fixture.kgStore->upsertNode(caller);
    REQUIRE(callerId.has_value());

    KGNode callee;
    callee.nodeKey = "symbol:enum:callee";
    callee.label = "callee";
    callee.type = "function";
    auto calleeId = fixture.kgStore->upsertNode(callee);
    REQUIRE(calleeId.has_value());

    KGEdge callEdge;
    callEdge.srcNodeId = callerId.value();
    callEdge.dstNodeId = calleeId.value();
    callEdge.relation = "calls";
    callEdge.weight = 1.0f;
    REQUIRE(fixture.kgStore->addEdge(callEdge).has_value());

    GraphQueryRequest req;
    req.nodeId = callerId.value();
    req.maxDepth = 1;
    req.maxResults = 10;
    req.relationFilters = {GraphRelationType::SymbolReference};

    auto result = service->query(req);
    REQUIRE(result.has_value());
    const auto& resp = result.value();
    REQUIRE(resp.allConnectedNodes.size() == 1);
    REQUIRE(resp.allConnectedNodes[0].nodeMetadata.node.nodeKey == "symbol:enum:callee");
}

TEST_CASE("GraphQueryService: BFS uses discovered parent edge for attribution",
          "[services][graph]") {
    GraphQueryServiceFixture fixture;
    auto service = makeGraphQueryService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE(service != nullptr);

    KGNode origin;
    origin.nodeKey = "symbol:path:origin";
    origin.label = "origin";
    origin.type = "function";
    auto originId = fixture.kgStore->upsertNode(origin);
    REQUIRE(originId.has_value());

    KGNode mid;
    mid.nodeKey = "symbol:path:mid";
    mid.label = "mid";
    mid.type = "function";
    auto midId = fixture.kgStore->upsertNode(mid);
    REQUIRE(midId.has_value());

    KGNode target;
    target.nodeKey = "symbol:path:target";
    target.label = "target";
    target.type = "function";
    auto targetId = fixture.kgStore->upsertNode(target);
    REQUIRE(targetId.has_value());

    KGEdge edge1;
    edge1.srcNodeId = originId.value();
    edge1.dstNodeId = midId.value();
    edge1.relation = "contains";
    edge1.weight = 1.0f;
    REQUIRE(fixture.kgStore->addEdge(edge1).has_value());

    KGEdge edge2;
    edge2.srcNodeId = midId.value();
    edge2.dstNodeId = targetId.value();
    edge2.relation = "calls";
    edge2.weight = 0.9f;
    edge2.properties = R"({"source":"unit","confidence":0.73})";
    REQUIRE(fixture.kgStore->addEdge(edge2).has_value());

    GraphQueryRequest req;
    req.nodeId = originId.value();
    req.maxDepth = 2;
    req.maxResults = 10;
    req.hydrateFully = false;
    req.includeEdgeProperties = true;

    auto result = service->query(req);
    REQUIRE(result.has_value());
    const auto& resp = result.value();

    REQUIRE(resp.allConnectedNodes.size() == 2);

    const auto* targetNode = [&]() -> const GraphConnectedNode* {
        for (const auto& node : resp.allConnectedNodes) {
            if (node.nodeMetadata.node.nodeKey == "symbol:path:target") {
                return &node;
            }
        }
        return nullptr;
    }();

    REQUIRE(targetNode != nullptr);
    CHECK(targetNode->distance == 2);
    REQUIRE(targetNode->connectingEdges.size() == 1);
    CHECK(targetNode->connectingEdges[0].srcNodeId == midId.value());
    CHECK(targetNode->connectingEdges[0].dstNodeId == targetId.value());
    CHECK(targetNode->connectingEdges[0].relation == "calls");
    REQUIRE(targetNode->connectingEdges[0].properties.has_value());
    CHECK(targetNode->connectingEdges[0].properties.value().find("\"confidence\":0.73") !=
          std::string::npos);
}

TEST_CASE("GraphQueryService: multi-path chooses higher-signal parent relation",
          "[services][graph]") {
    GraphQueryServiceFixture fixture;
    auto service = makeGraphQueryService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE(service != nullptr);

    KGNode origin{.nodeKey = "symbol:multi:origin", .label = "origin", .type = "function"};
    KGNode structHop{.nodeKey = "symbol:multi:struct", .label = "structHop", .type = "file"};
    KGNode semanticHop{
        .nodeKey = "symbol:multi:semantic", .label = "semanticHop", .type = "function"};
    KGNode target{.nodeKey = "symbol:multi:target", .label = "target", .type = "function"};

    auto originId = fixture.kgStore->upsertNode(origin);
    auto structHopId = fixture.kgStore->upsertNode(structHop);
    auto semanticHopId = fixture.kgStore->upsertNode(semanticHop);
    auto targetId = fixture.kgStore->upsertNode(target);
    REQUIRE(originId.has_value());
    REQUIRE(structHopId.has_value());
    REQUIRE(semanticHopId.has_value());
    REQUIRE(targetId.has_value());

    KGEdge e1{.srcNodeId = originId.value(),
              .dstNodeId = structHopId.value(),
              .relation = "contains",
              .weight = 1.0f};
    KGEdge e2{.srcNodeId = originId.value(),
              .dstNodeId = semanticHopId.value(),
              .relation = "calls",
              .weight = 1.0f};
    KGEdge e3{.srcNodeId = structHopId.value(),
              .dstNodeId = targetId.value(),
              .relation = "contains",
              .weight = 1.0f};
    KGEdge e4{.srcNodeId = semanticHopId.value(),
              .dstNodeId = targetId.value(),
              .relation = "calls",
              .weight = 1.0f};
    REQUIRE(fixture.kgStore->addEdge(e1).has_value());
    REQUIRE(fixture.kgStore->addEdge(e2).has_value());
    REQUIRE(fixture.kgStore->addEdge(e3).has_value());
    REQUIRE(fixture.kgStore->addEdge(e4).has_value());

    GraphQueryRequest req;
    req.nodeId = originId.value();
    req.maxDepth = 2;
    req.maxResults = 10;
    req.hydrateFully = false;

    auto result = service->query(req);
    REQUIRE(result.has_value());
    const auto& resp = result.value();

    const auto* targetNode = [&]() -> const GraphConnectedNode* {
        for (const auto& node : resp.allConnectedNodes) {
            if (node.nodeMetadata.node.nodeKey == "symbol:multi:target") {
                return &node;
            }
        }
        return nullptr;
    }();

    REQUIRE(targetNode != nullptr);
    REQUIRE(targetNode->connectingEdges.size() == 1);
    CHECK(targetNode->connectingEdges[0].srcNodeId == semanticHopId.value());
    CHECK(targetNode->connectingEdges[0].relation == "calls");
}

TEST_CASE("GraphQueryService: cycles keep path attribution stable", "[services][graph]") {
    GraphQueryServiceFixture fixture;
    auto service = makeGraphQueryService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE(service != nullptr);

    KGNode origin{.nodeKey = "symbol:cycle:origin", .label = "origin", .type = "function"};
    KGNode mid{.nodeKey = "symbol:cycle:mid", .label = "mid", .type = "function"};
    KGNode target{.nodeKey = "symbol:cycle:target", .label = "target", .type = "function"};

    auto originId = fixture.kgStore->upsertNode(origin);
    auto midId = fixture.kgStore->upsertNode(mid);
    auto targetId = fixture.kgStore->upsertNode(target);
    REQUIRE(originId.has_value());
    REQUIRE(midId.has_value());
    REQUIRE(targetId.has_value());

    REQUIRE(fixture.kgStore
                ->addEdge(KGEdge{.srcNodeId = originId.value(),
                                 .dstNodeId = midId.value(),
                                 .relation = "calls",
                                 .weight = 1.0f})
                .has_value());
    REQUIRE(fixture.kgStore
                ->addEdge(KGEdge{.srcNodeId = midId.value(),
                                 .dstNodeId = originId.value(),
                                 .relation = "calls",
                                 .weight = 1.0f})
                .has_value());
    REQUIRE(fixture.kgStore
                ->addEdge(KGEdge{.srcNodeId = midId.value(),
                                 .dstNodeId = targetId.value(),
                                 .relation = "references",
                                 .weight = 1.0f})
                .has_value());

    GraphQueryRequest req;
    req.nodeId = originId.value();
    req.maxDepth = 2;
    req.maxResults = 10;
    req.hydrateFully = false;

    auto result = service->query(req);
    REQUIRE(result.has_value());
    const auto& resp = result.value();

    REQUIRE(resp.allConnectedNodes.size() == 2);
    const auto& depthOne = resp.nodesByDistance.at(1);
    REQUIRE(depthOne.size() == 1);
    CHECK(depthOne.front().nodeMetadata.node.nodeKey == "symbol:cycle:mid");

    const auto& depthTwo = resp.nodesByDistance.at(2);
    REQUIRE(depthTwo.size() == 1);
    CHECK(depthTwo.front().nodeMetadata.node.nodeKey == "symbol:cycle:target");
    REQUIRE(depthTwo.front().connectingEdges.size() == 1);
    CHECK(depthTwo.front().connectingEdges.front().srcNodeId == midId.value());
    CHECK(depthTwo.front().connectingEdges.front().relation == "references");
}

TEST_CASE("GraphQueryService: relation filters apply before traversal expansion",
          "[services][graph]") {
    GraphQueryServiceFixture fixture;
    auto service = makeGraphQueryService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE(service != nullptr);

    KGNode origin{.nodeKey = "symbol:filter:path:origin", .label = "origin", .type = "function"};
    KGNode mid{.nodeKey = "symbol:filter:path:mid", .label = "mid", .type = "file"};
    KGNode target{.nodeKey = "symbol:filter:path:target", .label = "target", .type = "function"};

    auto originId = fixture.kgStore->upsertNode(origin);
    auto midId = fixture.kgStore->upsertNode(mid);
    auto targetId = fixture.kgStore->upsertNode(target);
    REQUIRE(originId.has_value());
    REQUIRE(midId.has_value());
    REQUIRE(targetId.has_value());

    REQUIRE(fixture.kgStore
                ->addEdge(KGEdge{.srcNodeId = originId.value(),
                                 .dstNodeId = midId.value(),
                                 .relation = "contains",
                                 .weight = 1.0f})
                .has_value());
    REQUIRE(fixture.kgStore
                ->addEdge(KGEdge{.srcNodeId = midId.value(),
                                 .dstNodeId = targetId.value(),
                                 .relation = "calls",
                                 .weight = 1.0f})
                .has_value());

    GraphQueryRequest req;
    req.nodeId = originId.value();
    req.maxDepth = 2;
    req.maxResults = 10;
    req.hydrateFully = false;
    req.relationNames = {"contains"};

    auto result = service->query(req);
    REQUIRE(result.has_value());
    const auto& resp = result.value();

    REQUIRE(resp.allConnectedNodes.size() == 1);
    CHECK(resp.allConnectedNodes.front().nodeMetadata.node.nodeKey == "symbol:filter:path:mid");
}

TEST_CASE("GraphQueryService: semantic relations rank above structural at same depth",
          "[services][graph]") {
    GraphQueryServiceFixture fixture;
    auto service = makeGraphQueryService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE(service != nullptr);

    KGNode origin{.nodeKey = "symbol:rank:origin", .label = "origin", .type = "function"};
    KGNode semantic{.nodeKey = "symbol:rank:semantic", .label = "semantic", .type = "function"};
    KGNode structural{.nodeKey = "symbol:rank:structural", .label = "structural", .type = "file"};

    auto originId = fixture.kgStore->upsertNode(origin);
    auto semanticId = fixture.kgStore->upsertNode(semantic);
    auto structuralId = fixture.kgStore->upsertNode(structural);
    REQUIRE(originId.has_value());
    REQUIRE(semanticId.has_value());
    REQUIRE(structuralId.has_value());

    REQUIRE(fixture.kgStore
                ->addEdge(KGEdge{.srcNodeId = originId.value(),
                                 .dstNodeId = structuralId.value(),
                                 .relation = "contains",
                                 .weight = 1.0f})
                .has_value());
    REQUIRE(fixture.kgStore
                ->addEdge(KGEdge{.srcNodeId = originId.value(),
                                 .dstNodeId = semanticId.value(),
                                 .relation = "calls",
                                 .weight = 1.0f})
                .has_value());

    GraphQueryRequest req;
    req.nodeId = originId.value();
    req.maxDepth = 1;
    req.maxResults = 10;
    req.hydrateFully = false;

    auto result = service->query(req);
    REQUIRE(result.has_value());
    const auto& resp = result.value();

    REQUIRE(resp.allConnectedNodes.size() == 2);
    CHECK(resp.allConnectedNodes.front().nodeMetadata.node.nodeKey == "symbol:rank:semantic");
    CHECK(resp.allConnectedNodes.back().nodeMetadata.node.nodeKey == "symbol:rank:structural");
}
