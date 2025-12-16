// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 YAMS Contributors

#include <chrono>
#include <filesystem>

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/knowledge_graph_store.h>

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
