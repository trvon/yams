#include <chrono>
#include <filesystem>

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/knowledge_graph_store.h>
#include <yams/search/graph_expansion.h>

using namespace yams;
using namespace yams::metadata;
using namespace yams::search;

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

struct GraphExpansionFixture {
    GraphExpansionFixture() {
        dbPath = tempDbPath("graph_expansion_catch2_test_");
        KnowledgeGraphStoreConfig cfg{};
        auto storeRes = makeSqliteKnowledgeGraphStore(dbPath.string(), cfg);
        REQUIRE(storeRes.has_value());
        store = std::shared_ptr<KnowledgeGraphStore>(std::move(storeRes.value()));
        REQUIRE(store != nullptr);
    }

    ~GraphExpansionFixture() {
        store.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
    }

    std::shared_ptr<KnowledgeGraphStore> store;
    std::filesystem::path dbPath;
};

} // namespace

TEST_CASE("Graph expansion ignores path and blob nodes", "[search][graph-expansion]") {
    GraphExpansionFixture fixture;

    std::vector<KGNode> nodes = {
        KGNode{.nodeKey = "path:logical:/corpus/prostate_cancer.txt",
               .label = std::string("prostate cancer"),
               .type = std::string("path")},
        KGNode{.nodeKey = "blob:abc123",
               .label = std::string("prostate cancer"),
               .type = std::string("blob")},
    };
    auto ids = fixture.store->upsertNodes(nodes);
    REQUIRE(ids.has_value());

    auto terms = generateGraphExpansionTerms(
        fixture.store, "prostate cancer", {},
        GraphExpansionConfig{.maxTerms = 8, .maxSeeds = 6, .maxNeighbors = 6});

    CHECK(terms.empty());
}

TEST_CASE("Graph expansion ignores text segment nodes", "[search][graph-expansion]") {
    CHECK(graphNodeExpansionWeight(std::optional<std::string>{"text_segment"},
                                   "long abstract-like segment") == 0.0f);
}

TEST_CASE("Graph expansion suppresses low-signal alias sources", "[search][graph-expansion]") {
    GraphExpansionFixture fixture;

    KGNode node{.nodeKey = "nl_entity:disease:chronic disease",
                .label = std::string("chronic disease"),
                .type = std::string("disease")};
    auto nodeId = fixture.store->upsertNode(node);
    REQUIRE(nodeId.has_value());

    REQUIRE(fixture.store
                ->addAliases({KGAlias{.nodeId = nodeId.value(),
                                      .alias = std::string("chronic disease"),
                                      .source = std::string("gliner.surface"),
                                      .confidence = 1.0f},
                              KGAlias{.nodeId = nodeId.value(),
                                      .alias = std::string("organization chronic disease"),
                                      .source = std::string("gliner.type_qualified"),
                                      .confidence = 0.95f}})
                .has_value());

    auto terms = generateGraphExpansionTerms(
        fixture.store, "chronic disease", {},
        GraphExpansionConfig{.maxTerms = 8, .maxSeeds = 6, .maxNeighbors = 6});

    bool sawSurface = false;
    bool sawTypeQualified = false;
    for (const auto& term : terms) {
        if (term.text == "chronic disease") {
            sawSurface = true;
        }
        if (term.text == "organization chronic disease") {
            sawTypeQualified = true;
        }
    }
    CHECK(sawSurface);
    CHECK_FALSE(sawTypeQualified);
}
