// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

#include <chrono>
#include <filesystem>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/kg_topology_analysis.h>
#include <yams/metadata/knowledge_graph_store.h>

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
        REQUIRE(storeRes.has_value());
        store_ = std::move(storeRes.value());
        REQUIRE(store_ != nullptr);
    }

    ~KGStoreFixture() {
        store_.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath_, ec);
    }

    std::unique_ptr<KnowledgeGraphStore> store_;
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
        REQUIRE(ids1.has_value());
        REQUIRE(ids1.value().size() == 2);
        CHECK(ids1.value()[0] > 0);
        CHECK(ids1.value()[1] > 0);

        SECTION("Second upsert returns same IDs") {
            auto ids2 = fix.store_->upsertNodes(nodes);
            REQUIRE(ids2.has_value());
            REQUIRE(ids2.value().size() == 2);
            CHECK(ids1.value()[0] == ids2.value()[0]);
            CHECK(ids1.value()[1] == ids2.value()[1]);
        }

        SECTION("getNodeByKey returns correct node") {
            auto n1 = fix.store_->getNodeByKey("tag:alpha");
            REQUIRE(n1.has_value());
            REQUIRE(n1.value().has_value());
            CHECK(n1.value()->id == ids1.value()[0]);
        }
    }
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
    REQUIRE(ids.has_value());
    REQUIRE(ids.value().size() == 2);
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
        REQUIRE(r1.has_value());
        auto r2 = fix.store_->addEdgesUnique(edges);
        REQUIRE(r2.has_value());

        // Verify only one edge exists for (src,dst,relation)
        auto out = fix.store_->getEdgesFrom(src, std::string_view("HAS_TAG"), 100, 0);
        REQUIRE(out.has_value());
        size_t count = 0;
        for (const auto& ed : out.value()) {
            if (ed.dstNodeId == dst && ed.relation == "HAS_TAG")
                ++count;
        }
        CHECK(count == 1);
    }
}

TEST_CASE("KG Store: ensurePathNode links logical path to snapshot path", "[unit][metadata][kg]") {
    KGStoreFixture fix;

    PathNodeDescriptor descriptor;
    descriptor.snapshotId = "snap1";
    descriptor.path = "/repo/file.cpp";
    descriptor.rootTreeHash = "";
    descriptor.isDirectory = false;

    auto snapshotId = fix.store_->ensurePathNode(descriptor);
    REQUIRE(snapshotId.has_value());

    auto logicalNode = fix.store_->getNodeByKey("path:logical:/repo/file.cpp");
    REQUIRE(logicalNode.has_value());
    REQUIRE(logicalNode.value().has_value());

    auto edges = fix.store_->getEdgesFrom(logicalNode.value()->id, "path_version", 10, 0);
    REQUIRE(edges.has_value());
    REQUIRE_FALSE(edges.value().empty());
    CHECK(edges.value()[0].dstNodeId == snapshotId.value());
}

TEST_CASE("KG topology analysis summarizes semantic neighborhoods", "[unit][metadata][kg]") {
    KGStoreFixture fix;

    std::vector<KGNode> nodes = {
        KGNode{.nodeKey = "doc:a", .label = std::string("a"), .type = std::string("document")},
        KGNode{.nodeKey = "doc:b", .label = std::string("b"), .type = std::string("document")},
        KGNode{.nodeKey = "doc:c", .label = std::string("c"), .type = std::string("document")},
    };
    auto ids = fix.store_->upsertNodes(nodes);
    REQUIRE(ids.has_value());
    REQUIRE(ids.value().size() == 3);

    KGEdge ab{.srcNodeId = ids.value()[0],
              .dstNodeId = ids.value()[1],
              .relation = "semantic_neighbor",
              .weight = 0.92f};
    KGEdge ba{.srcNodeId = ids.value()[1],
              .dstNodeId = ids.value()[0],
              .relation = "semantic_neighbor",
              .weight = 0.92f};
    REQUIRE(fix.store_->addEdgesUnique({ab, ba}).has_value());

    auto topology = analyzeDocumentTopology(fix.store_.get());
    REQUIRE(topology.has_value());
    CHECK(topology->documentNodeCount == 3);
    CHECK(topology->semanticEdgeCount == 1);
    CHECK(topology->reciprocalSemanticEdgeCount == 1);
    CHECK(topology->unreciprocatedSemanticEdgeCount == 0);
    CHECK(topology->documentsWithSemanticNeighbors == 2);
    CHECK(topology->documentsWithReciprocalNeighbors == 2);
    CHECK(topology->reciprocalCommunityCount == 1);
    CHECK(topology->reciprocalSingletonDocumentCount == 1);
    CHECK(topology->largestReciprocalCommunitySize == 2);
    CHECK(topology->isolatedDocumentCount == 1);
    CHECK(topology->connectedComponentCount == 2);
    CHECK(topology->largestComponentSize == 2);
    CHECK(topology->semanticReciprocity == Approx(1.0));
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
    REQUIRE(ids.has_value());
    REQUIRE(ids.value().size() == 4);

    REQUIRE(fix.store_
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
                .has_value());

    auto topology = analyzeDocumentTopology(fix.store_.get());
    REQUIRE(topology.has_value());
    CHECK(topology->documentNodeCount == 4);
    CHECK(topology->semanticEdgeCount == 3);
    CHECK(topology->reciprocalSemanticEdgeCount == 0);
    CHECK(topology->unreciprocatedSemanticEdgeCount == 3);
    CHECK(topology->documentsWithSemanticNeighbors == 4);
    CHECK(topology->documentsWithReciprocalNeighbors == 0);
    CHECK(topology->reciprocalCommunityCount == 0);
    CHECK(topology->reciprocalSingletonDocumentCount == 4);
    CHECK(topology->largestReciprocalCommunitySize == 0);
    CHECK(topology->connectedComponentCount == 1);
    CHECK(topology->largestComponentSize == 4);
    CHECK(topology->semanticReciprocity == Approx(0.0));
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
    REQUIRE(ids.has_value());

    REQUIRE(fix.store_
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
                .has_value());

    auto topology = analyzeDocumentTopology(fix.store_.get());
    REQUIRE(topology.has_value());
    CHECK(topology->reciprocalCommunityCount == 2);
    CHECK(topology->largestReciprocalCommunitySize == 2);
    CHECK(topology->reciprocalSingletonDocumentCount == 1);
    REQUIRE(topology->reciprocalCommunitySizes.size() == 2);
    CHECK(topology->reciprocalCommunitySizes[0] == 2);
    CHECK(topology->reciprocalCommunitySizes[1] == 2);
}
