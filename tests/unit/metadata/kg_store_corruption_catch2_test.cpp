// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <filesystem>
#include <string>

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/database.h>
#include <yams/metadata/knowledge_graph_store.h>

#include "../../common/sqlite_corruption.h"

using namespace yams;
using namespace yams::metadata;
using namespace yams::test;

// NOLINTBEGIN(bugprone-chained-comparison)
namespace {

struct ArtifactGuard {
    explicit ArtifactGuard(std::filesystem::path p) : path(std::move(p)) {}
    ~ArtifactGuard() { remove_sqlite_artifacts(path); }
    std::filesystem::path path;
};

std::filesystem::path makeSeededKgDb(std::string_view prefix) {
    auto dbPath = make_temp_sqlite_path(prefix);
    KnowledgeGraphStoreConfig cfg{};
    auto storeRes = makeSqliteKnowledgeGraphStore(dbPath.string(), cfg);
    REQUIRE(storeRes.has_value());
    auto store = std::move(storeRes.value());
    for (int i = 0; i < 300; ++i) {
        KGNode node;
        node.nodeKey = "node:" + std::to_string(i);
        node.label = "label-" + std::string(100, 'a' + (i % 26));
        node.type = "entity";
        REQUIRE(store->upsertNode(node).has_value());
    }
    store.reset();
    return dbPath;
}

} // namespace

TEST_CASE("KG healthCheck fails on corrupt KG DB", "[unit][metadata][corruption][kg]") {
    auto dbPath = makeSeededKgDb("yams_kg_corrupt_health_");
    ArtifactGuard guard{dbPath};

    corruptPage(dbPath, static_cast<std::uint64_t>(tableRootPage(dbPath, "kg_nodes")));

    KnowledgeGraphStoreConfig cfg{};
    auto storeRes = makeSqliteKnowledgeGraphStore(dbPath.string(), cfg);
    if (!storeRes.has_value()) {
        SUCCEED("store creation failed; corruption detected at open/migrate time");
        return;
    }

    auto health = storeRes.value()->healthCheck();
    REQUIRE_FALSE(health);
    CHECK(health.error().code != ErrorCode::ResourceBusy);
}

TEST_CASE("KG queries on corrupt DB return errors without crashing",
          "[unit][metadata][corruption][kg]") {
    auto dbPath = makeSeededKgDb("yams_kg_corrupt_query_");
    ArtifactGuard guard{dbPath};

    corruptPage(dbPath, static_cast<std::uint64_t>(tableRootPage(dbPath, "kg_nodes")));

    KnowledgeGraphStoreConfig cfg{};
    auto storeRes = makeSqliteKnowledgeGraphStore(dbPath.string(), cfg);
    if (!storeRes.has_value()) {
        SUCCEED("store creation failed; corruption detected at open/migrate time");
        return;
    }
    auto& store = *storeRes.value();

    auto byKey = store.getNodeByKey("node:1");
    if (byKey) {
        auto count = store.countNodesByType("entity");
        CHECK((!count || count.value() <= 300));
    }

    KGNode node;
    node.nodeKey = "node:new";
    node.type = "entity";
    auto upsert = store.upsertNode(node);
    auto health = store.healthCheck();
    CHECK((!upsert || !health));
}
// NOLINTEND(bugprone-chained-comparison)
