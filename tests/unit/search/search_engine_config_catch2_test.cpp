/**
 * @file search_engine_config_catch2_test.cpp
 * @brief Tests for SearchEngineConfig, CorpusProfile, ComponentResult helpers,
 *        and accumulateComponentScore (search_engine.h)
 *
 * These are header-only or inline functions at 0% coverage. Tests here
 * exercise forProfile(), detectProfile(), fusionStrategyToString(),
 * componentSourceToString(), isVectorComponent(), isTextAnchoringComponent(),
 * and accumulateComponentScore().
 */

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/search/search_engine.h>

#include <string>
#include <unordered_map>

using namespace yams::search;
using Catch::Approx;
using yams::metadata::SearchResult;

// ────────────────────────────────────────────────────────────────────────────────
// fusionStrategyToString
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("fusionStrategyToString covers all strategies", "[search][config][catch2]") {
    using FS = SearchEngineConfig::FusionStrategy;

    CHECK(std::string(SearchEngineConfig::fusionStrategyToString(FS::WEIGHTED_SUM)) ==
          "WEIGHTED_SUM");
    CHECK(std::string(SearchEngineConfig::fusionStrategyToString(FS::RECIPROCAL_RANK)) ==
          "RECIPROCAL_RANK");
    CHECK(std::string(SearchEngineConfig::fusionStrategyToString(FS::WEIGHTED_RECIPROCAL)) ==
          "WEIGHTED_RECIPROCAL");
    CHECK(std::string(SearchEngineConfig::fusionStrategyToString(FS::COMB_MNZ)) == "COMB_MNZ");
}

// ────────────────────────────────────────────────────────────────────────────────
// forProfile
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("forProfile CODE sets code-oriented weights", "[search][config][catch2]") {
    using CP = SearchEngineConfig::CorpusProfile;
    auto cfg = SearchEngineConfig::forProfile(CP::CODE);

    CHECK(cfg.corpusProfile == CP::CODE);
    CHECK(cfg.textWeight == Approx(0.40f));
    CHECK(cfg.pathTreeWeight == Approx(0.15f));
    CHECK(cfg.kgWeight == Approx(0.10f));
    CHECK(cfg.vectorWeight == Approx(0.10f));
    CHECK(cfg.entityVectorWeight == Approx(0.15f));
    CHECK(cfg.tagWeight == Approx(0.05f));
    CHECK(cfg.metadataWeight == Approx(0.05f));
}

TEST_CASE("forProfile PROSE disables entityVectorWeight", "[search][config][catch2]") {
    using CP = SearchEngineConfig::CorpusProfile;
    auto cfg = SearchEngineConfig::forProfile(CP::PROSE);

    CHECK(cfg.corpusProfile == CP::PROSE);
    CHECK(cfg.entityVectorWeight == Approx(0.00f));
    CHECK(cfg.vectorWeight == Approx(0.25f));
    CHECK(cfg.textWeight == Approx(0.45f));
}

TEST_CASE("forProfile DOCS sets documentation weights", "[search][config][catch2]") {
    using CP = SearchEngineConfig::CorpusProfile;
    auto cfg = SearchEngineConfig::forProfile(CP::DOCS);

    CHECK(cfg.corpusProfile == CP::DOCS);
    CHECK(cfg.textWeight == Approx(0.40f));
    CHECK(cfg.vectorWeight == Approx(0.20f));
    CHECK(cfg.entityVectorWeight == Approx(0.05f));
}

TEST_CASE("forProfile MIXED uses balanced defaults", "[search][config][catch2]") {
    using CP = SearchEngineConfig::CorpusProfile;
    auto cfg = SearchEngineConfig::forProfile(CP::MIXED);

    CHECK(cfg.corpusProfile == CP::MIXED);
    // Should use the member defaults, not any profile-specific overrides
    SearchEngineConfig defaults;
    CHECK(cfg.textWeight == Approx(defaults.textWeight));
    CHECK(cfg.vectorWeight == Approx(defaults.vectorWeight));
}

TEST_CASE("forProfile CUSTOM also uses defaults", "[search][config][catch2]") {
    using CP = SearchEngineConfig::CorpusProfile;
    auto cfg = SearchEngineConfig::forProfile(CP::CUSTOM);
    CHECK(cfg.corpusProfile == CP::CUSTOM);
}

// ────────────────────────────────────────────────────────────────────────────────
// detectProfile
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("detectProfile empty map returns MIXED", "[search][config][catch2]") {
    std::unordered_map<std::string, int64_t> empty;
    auto profile = SearchEngineConfig::detectProfile(empty);
    CHECK(profile == SearchEngineConfig::CorpusProfile::MIXED);
}

TEST_CASE("detectProfile dominant code extensions returns CODE", "[search][config][catch2]") {
    std::unordered_map<std::string, int64_t> counts = {
        {".py", 40}, {".cpp", 20}, {".h", 10}, {".md", 5}, {".txt", 5}};
    // 70/80 = 87.5% code
    auto profile = SearchEngineConfig::detectProfile(counts);
    CHECK(profile == SearchEngineConfig::CorpusProfile::CODE);
}

TEST_CASE("detectProfile dominant prose extensions returns PROSE", "[search][config][catch2]") {
    std::unordered_map<std::string, int64_t> counts = {
        {".md", 50}, {".txt", 20}, {".pdf", 10}, {".py", 5}};
    // 80/85 = 94% prose
    auto profile = SearchEngineConfig::detectProfile(counts);
    CHECK(profile == SearchEngineConfig::CorpusProfile::PROSE);
}

TEST_CASE("detectProfile mixed code + prose returns DOCS", "[search][config][catch2]") {
    std::unordered_map<std::string, int64_t> counts = {
        {".py", 40}, {".md", 35}, {".txt", 10}, {".dat", 15}};
    // code: 40/100 = 40%, prose: 45/100 = 45%
    auto profile = SearchEngineConfig::detectProfile(counts);
    CHECK(profile == SearchEngineConfig::CorpusProfile::DOCS);
}

TEST_CASE("detectProfile all unknown extensions returns MIXED", "[search][config][catch2]") {
    std::unordered_map<std::string, int64_t> counts = {{".xyz", 50}, {".zyx", 30}, {".qwerty", 20}};
    auto profile = SearchEngineConfig::detectProfile(counts);
    CHECK(profile == SearchEngineConfig::CorpusProfile::MIXED);
}

TEST_CASE("detectProfile case-insensitive extension matching", "[search][config][catch2]") {
    // detectProfile lowercases extensions so ".PY" counts as code
    std::unordered_map<std::string, int64_t> counts = {
        {".PY", 30}, {".CPP", 30}, {".H", 20}, {".md", 5}};
    // 80/85 = 94% code (lowercase matches)
    auto profile = SearchEngineConfig::detectProfile(counts);
    CHECK(profile == SearchEngineConfig::CorpusProfile::CODE);
}

// ────────────────────────────────────────────────────────────────────────────────
// componentSourceToString
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("componentSourceToString covers all sources", "[search][config][catch2]") {
    using S = ComponentResult::Source;
    CHECK(std::string(componentSourceToString(S::Text)) == "text");
    CHECK(std::string(componentSourceToString(S::PathTree)) == "path_tree");
    CHECK(std::string(componentSourceToString(S::KnowledgeGraph)) == "kg");
    CHECK(std::string(componentSourceToString(S::Vector)) == "vector");
    CHECK(std::string(componentSourceToString(S::EntityVector)) == "entity_vector");
    CHECK(std::string(componentSourceToString(S::Tag)) == "tag");
    CHECK(std::string(componentSourceToString(S::Metadata)) == "metadata");
    CHECK(std::string(componentSourceToString(S::Symbol)) == "symbol");
    CHECK(std::string(componentSourceToString(S::Unknown)) == "unknown");
}

// ────────────────────────────────────────────────────────────────────────────────
// isVectorComponent / isTextAnchoringComponent
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("isVectorComponent is true for Vector and EntityVector", "[search][config][catch2]") {
    using S = ComponentResult::Source;
    CHECK(isVectorComponent(S::Vector) == true);
    CHECK(isVectorComponent(S::EntityVector) == true);

    CHECK(isVectorComponent(S::Text) == false);
    CHECK(isVectorComponent(S::PathTree) == false);
    CHECK(isVectorComponent(S::KnowledgeGraph) == false);
    CHECK(isVectorComponent(S::Tag) == false);
    CHECK(isVectorComponent(S::Metadata) == false);
    CHECK(isVectorComponent(S::Symbol) == false);
    CHECK(isVectorComponent(S::Unknown) == false);
}

TEST_CASE("isTextAnchoringComponent identifies non-vector components", "[search][config][catch2]") {
    using S = ComponentResult::Source;
    CHECK(isTextAnchoringComponent(S::Text) == true);
    CHECK(isTextAnchoringComponent(S::PathTree) == true);
    CHECK(isTextAnchoringComponent(S::KnowledgeGraph) == true);
    CHECK(isTextAnchoringComponent(S::Tag) == true);
    CHECK(isTextAnchoringComponent(S::Metadata) == true);
    CHECK(isTextAnchoringComponent(S::Symbol) == true);

    CHECK(isTextAnchoringComponent(S::Vector) == false);
    CHECK(isTextAnchoringComponent(S::EntityVector) == false);
    CHECK(isTextAnchoringComponent(S::Unknown) == false);
}

// ────────────────────────────────────────────────────────────────────────────────
// accumulateComponentScore
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("accumulateComponentScore routes Text to keywordScore", "[search][config][catch2]") {
    SearchResult r;
    accumulateComponentScore(r, ComponentResult::Source::Text, 0.5);
    REQUIRE(r.keywordScore.has_value());
    CHECK(r.keywordScore.value() == Approx(0.5));
}

TEST_CASE("accumulateComponentScore routes Vector to vectorScore", "[search][config][catch2]") {
    SearchResult r;
    accumulateComponentScore(r, ComponentResult::Source::Vector, 0.3);
    REQUIRE(r.vectorScore.has_value());
    CHECK(r.vectorScore.value() == Approx(0.3));
}

TEST_CASE("accumulateComponentScore routes EntityVector to vectorScore",
          "[search][config][catch2]") {
    SearchResult r;
    accumulateComponentScore(r, ComponentResult::Source::EntityVector, 0.2);
    REQUIRE(r.vectorScore.has_value());
    CHECK(r.vectorScore.value() == Approx(0.2));
}

TEST_CASE("accumulateComponentScore routes KnowledgeGraph to kgScore", "[search][config][catch2]") {
    SearchResult r;
    accumulateComponentScore(r, ComponentResult::Source::KnowledgeGraph, 0.4);
    REQUIRE(r.kgScore.has_value());
    CHECK(r.kgScore.value() == Approx(0.4));
}

TEST_CASE("accumulateComponentScore routes PathTree to pathScore", "[search][config][catch2]") {
    SearchResult r;
    accumulateComponentScore(r, ComponentResult::Source::PathTree, 0.15);
    REQUIRE(r.pathScore.has_value());
    CHECK(r.pathScore.value() == Approx(0.15));
}

TEST_CASE("accumulateComponentScore routes Tag to tagScore", "[search][config][catch2]") {
    SearchResult r;
    accumulateComponentScore(r, ComponentResult::Source::Tag, 0.1);
    REQUIRE(r.tagScore.has_value());
    CHECK(r.tagScore.value() == Approx(0.1));
}

TEST_CASE("accumulateComponentScore routes Metadata to tagScore", "[search][config][catch2]") {
    SearchResult r;
    accumulateComponentScore(r, ComponentResult::Source::Metadata, 0.1);
    REQUIRE(r.tagScore.has_value());
    CHECK(r.tagScore.value() == Approx(0.1));
}

TEST_CASE("accumulateComponentScore routes Symbol to symbolScore", "[search][config][catch2]") {
    SearchResult r;
    accumulateComponentScore(r, ComponentResult::Source::Symbol, 0.25);
    REQUIRE(r.symbolScore.has_value());
    CHECK(r.symbolScore.value() == Approx(0.25));
}

TEST_CASE("accumulateComponentScore accumulates multiple calls", "[search][config][catch2]") {
    SearchResult r;
    accumulateComponentScore(r, ComponentResult::Source::Vector, 0.3);
    accumulateComponentScore(r, ComponentResult::Source::EntityVector, 0.2);
    REQUIRE(r.vectorScore.has_value());
    CHECK(r.vectorScore.value() == Approx(0.5));
}

TEST_CASE("accumulateComponentScore Unknown source is no-op", "[search][config][catch2]") {
    SearchResult r;
    accumulateComponentScore(r, ComponentResult::Source::Unknown, 1.0);
    CHECK(!r.keywordScore.has_value());
    CHECK(!r.vectorScore.has_value());
    CHECK(!r.kgScore.has_value());
    CHECK(!r.pathScore.has_value());
    CHECK(!r.tagScore.has_value());
    CHECK(!r.symbolScore.has_value());
}

// ────────────────────────────────────────────────────────────────────────────────
// SearchEngineConfig defaults
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("SearchEngineConfig default values", "[search][config][catch2]") {
    SearchEngineConfig cfg;

    CHECK(cfg.corpusProfile == SearchEngineConfig::CorpusProfile::MIXED);
    CHECK(cfg.maxResults == 100);
    CHECK(cfg.similarityThreshold == Approx(0.75f));
    CHECK(cfg.enableParallelExecution == true);
    CHECK(cfg.enableTieredExecution == true);
    CHECK(cfg.fusionStrategy == SearchEngineConfig::FusionStrategy::COMB_MNZ);
    CHECK(cfg.rrfK == Approx(12.0f));
    CHECK(cfg.bm25NormDivisor == Approx(25.0f));
    CHECK(cfg.symbolRank == true);
    CHECK(cfg.enableReranking == true);
    CHECK(cfg.rerankTopK == 5);
    CHECK(cfg.enableModelReranking == false);
    CHECK(cfg.useScoreBasedReranking == true);
}

// ────────────────────────────────────────────────────────────────────────────────
// SearchResponse helpers
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("SearchResponse hasResults / isComplete", "[search][config][catch2]") {
    SearchResponse resp;
    CHECK(resp.hasResults() == false);
    CHECK(resp.isComplete() == true);

    resp.results.push_back(SearchResult{});
    CHECK(resp.hasResults() == true);

    resp.timedOutComponents.push_back("slow_component");
    CHECK(resp.isComplete() == false);
}
