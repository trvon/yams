// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <yams/metadata/kg_relation_summary.h>
#include <yams/metadata/knowledge_graph_store.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>

using Catch::Matchers::ContainsSubstring;
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

struct KGRelationSummaryFixture {
    KGRelationSummaryFixture() {
        dbPath = tempDbPath("kg_relation_summary_");

        KnowledgeGraphStoreConfig cfg;
        cfg.enable_wal = false;
        auto res = makeSqliteKnowledgeGraphStore(dbPath.string(), cfg);
        REQUIRE(res.has_value());
        store = std::move(res.value());
        REQUIRE(store != nullptr);
    }

    ~KGRelationSummaryFixture() {
        store.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
    }

    std::filesystem::path dbPath;
    std::unique_ptr<KnowledgeGraphStore> store;
};

} // namespace

TEST_CASE("KGRelationSummary normalizes relation names", "[unit][metadata][kg][relations]") {
    CHECK(normalizeRelationName("  HAS-VERSION  ") == "has_version");
    CHECK(normalizeRelationName("calls") == "calls");
    CHECK(normalizeRelationName("  ") == "");
}

TEST_CASE("KGRelationSummary collects file and hash relations", "[unit][metadata][kg][relations]") {
    KGRelationSummaryFixture fix;

    KGNode fileNode;
    fileNode.nodeKey = "path:file:/repo/main.cpp";
    fileNode.type = std::string("file");
    fileNode.label = std::string("main.cpp");
    auto fileId = fix.store->upsertNode(fileNode);
    REQUIRE(fileId.has_value());

    KGNode docNode;
    docNode.nodeKey = "doc:abc123";
    docNode.type = std::string("document");
    docNode.label = std::string("main.cpp");
    auto docId = fix.store->upsertNode(docNode);
    REQUIRE(docId.has_value());

    KGNode symbolOne;
    symbolOne.nodeKey = "symbol:one";
    symbolOne.type = std::string("symbol");
    auto symOneId = fix.store->upsertNode(symbolOne);
    REQUIRE(symOneId.has_value());

    KGNode symbolTwo;
    symbolTwo.nodeKey = "symbol:two";
    symbolTwo.type = std::string("symbol");
    auto symTwoId = fix.store->upsertNode(symbolTwo);
    REQUIRE(symTwoId.has_value());

    KGEdge versionEdge;
    versionEdge.srcNodeId = fileId.value();
    versionEdge.dstNodeId = docId.value();
    versionEdge.relation = "has-version";
    REQUIRE(fix.store->addEdge(versionEdge).has_value());

    KGEdge callsOne;
    callsOne.srcNodeId = docId.value();
    callsOne.dstNodeId = symOneId.value();
    callsOne.relation = "calls";
    REQUIRE(fix.store->addEdge(callsOne).has_value());

    KGEdge callsTwo;
    callsTwo.srcNodeId = docId.value();
    callsTwo.dstNodeId = symTwoId.value();
    callsTwo.relation = "calls";
    REQUIRE(fix.store->addEdge(callsTwo).has_value());

    auto summary = collectFileRelationSummary(fix.store.get(), "/repo/main.cpp", "abc123");
    REQUIRE(summary.has_value());
    CHECK(summary->totalEdges == 3);
    REQUIRE_FALSE(summary->topRelations.empty());
    CHECK(summary->topRelations.front().first == "calls");
    CHECK(summary->topRelations.front().second == 2);

    auto machineSummary = formatRelationSummary(summary->topRelations, false);
    CHECK_THAT(machineSummary, ContainsSubstring("calls:2"));

    auto humanSummary = formatRelationSummary(summary->topRelations, true);
    CHECK_THAT(humanSummary, ContainsSubstring("calls(2)"));
}

TEST_CASE("KGRelationSummary resolves legacy file key input", "[unit][metadata][kg][relations]") {
    KGRelationSummaryFixture fix;

    KGNode legacyFileNode;
    legacyFileNode.nodeKey = "file:/repo/legacy.cpp";
    legacyFileNode.type = std::string("file");
    auto legacyId = fix.store->upsertNode(legacyFileNode);
    REQUIRE(legacyId.has_value());

    KGNode dst;
    dst.nodeKey = "symbol:legacy";
    dst.type = std::string("symbol");
    auto dstId = fix.store->upsertNode(dst);
    REQUIRE(dstId.has_value());

    KGEdge edge;
    edge.srcNodeId = legacyId.value();
    edge.dstNodeId = dstId.value();
    edge.relation = "includes";
    REQUIRE(fix.store->addEdge(edge).has_value());

    auto summary = collectFileRelationSummary(fix.store.get(), "file:/repo/legacy.cpp");
    REQUIRE(summary.has_value());
    REQUIRE_FALSE(summary->topRelations.empty());
    CHECK(summary->topRelations.front().first == "includes");
}
