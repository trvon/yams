#include <chrono>
#include <filesystem>
#include <unordered_map>
#include <unordered_set>

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

// --- Graph expansion term dedup tests ---

TEST_CASE("Graph expansion term merge: max-score-wins for overlapping terms",
          "[search][graph-expansion][dedup]") {
    // Simulate the merge logic from search_engine.cpp
    std::vector<GraphExpansionTerm> existing = {
        {.text = "cancer", .score = 0.8f},
        {.text = "treatment", .score = 0.5f},
        {.text = "unique_a", .score = 0.3f},
    };

    std::vector<GraphExpansionTerm> incoming = {
        {.text = "cancer", .score = 0.6f},    // overlap, lower score
        {.text = "treatment", .score = 0.9f}, // overlap, higher score
        {.text = "unique_b", .score = 0.4f},  // new term
    };

    // O(n) merge using unordered_map (the optimized approach)
    std::unordered_map<std::string, size_t> termIndex;
    termIndex.reserve(existing.size());
    for (size_t i = 0; i < existing.size(); ++i) {
        termIndex[existing[i].text] = i;
    }
    for (const auto& term : incoming) {
        if (auto it = termIndex.find(term.text); it != termIndex.end()) {
            existing[it->second].score = std::max(existing[it->second].score, term.score);
        } else {
            termIndex[term.text] = existing.size();
            existing.push_back(term);
        }
    }

    // Verify max-score-wins
    std::unordered_map<std::string, float> scoreMap;
    for (const auto& t : existing) {
        scoreMap[t.text] = t.score;
    }
    CHECK(scoreMap.at("cancer") == 0.8f);    // kept higher existing score
    CHECK(scoreMap.at("treatment") == 0.9f); // took higher incoming score
    CHECK(scoreMap.at("unique_a") == 0.3f);  // preserved
    CHECK(scoreMap.at("unique_b") == 0.4f);  // added
}

TEST_CASE("Graph expansion term merge: no duplicates in result",
          "[search][graph-expansion][dedup]") {
    std::vector<GraphExpansionTerm> existing = {
        {.text = "alpha", .score = 1.0f},
        {.text = "beta", .score = 0.8f},
    };

    std::vector<GraphExpansionTerm> incoming = {
        {.text = "alpha", .score = 0.5f},
        {.text = "beta", .score = 0.3f},
        {.text = "gamma", .score = 0.7f},
    };

    std::unordered_map<std::string, size_t> termIndex;
    termIndex.reserve(existing.size());
    for (size_t i = 0; i < existing.size(); ++i) {
        termIndex[existing[i].text] = i;
    }
    for (const auto& term : incoming) {
        if (auto it = termIndex.find(term.text); it != termIndex.end()) {
            existing[it->second].score = std::max(existing[it->second].score, term.score);
        } else {
            termIndex[term.text] = existing.size();
            existing.push_back(term);
        }
    }

    // No duplicates
    std::unordered_set<std::string> seen;
    for (const auto& t : existing) {
        CHECK(seen.insert(t.text).second); // insert returns false if already present
    }
    CHECK(existing.size() == 3); // alpha, beta, gamma
}
