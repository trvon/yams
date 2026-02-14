#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/search/search_engine.h>

#include <cmath>

using yams::search::ComponentResult;
using yams::search::ResultFusion;
using yams::search::SearchEngineConfig;

namespace {

ComponentResult makeComponent(std::string hash, float score, ComponentResult::Source source,
                              size_t rank = 0) {
    ComponentResult c;
    c.documentHash = std::move(hash);
    c.filePath = c.documentHash;
    c.score = score;
    c.source = source;
    c.rank = rank;
    return c;
}

} // namespace

TEST_CASE("ResultFusion filters low-confidence vector-only results", "[search][fusion][catch2]") {
    SearchEngineConfig cfg;
    cfg.maxResults = 10;
    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_SUM;
    cfg.vectorWeight = 1.0f;
    cfg.textWeight = 1.0f;
    cfg.vectorOnlyThreshold = 0.90f;
    cfg.vectorOnlyPenalty = 1.0f;

    ResultFusion fusion(cfg);
    std::vector<ComponentResult> components;
    components.push_back(makeComponent("doc-vector-low", 0.50f, ComponentResult::Source::Vector));

    auto results = fusion.fuse(components);
    CHECK(results.empty());
}

TEST_CASE("ResultFusion keeps and penalizes high-confidence vector-only results",
          "[search][fusion][catch2]") {
    SearchEngineConfig cfg;
    cfg.maxResults = 10;
    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_SUM;
    cfg.vectorWeight = 1.0f;
    cfg.textWeight = 1.0f;
    cfg.vectorOnlyThreshold = 0.90f;
    cfg.vectorOnlyPenalty = 0.50f;

    ResultFusion fusion(cfg);
    std::vector<ComponentResult> components;
    components.push_back(
        makeComponent("doc-vector-high", 0.95f, ComponentResult::Source::Vector, 0));

    auto results = fusion.fuse(components);
    REQUIRE(results.size() > 0U);
    REQUIRE(results.size() < 2U);
    CHECK(results[0].document.sha256Hash.compare("doc-vector-high") == 0);
    CHECK(std::fabs(results[0].score - 0.475) < 1e-6);
}

TEST_CASE("ResultFusion boosts anchored hybrid agreement", "[search][fusion][catch2]") {
    SearchEngineConfig cfg;
    cfg.maxResults = 10;
    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_SUM;
    cfg.vectorWeight = 1.0f;
    cfg.textWeight = 1.0f;
    cfg.vectorOnlyThreshold = 0.0f;
    cfg.vectorOnlyPenalty = 1.0f;
    cfg.vectorBoostFactor = 0.10f;

    ResultFusion fusion(cfg);
    std::vector<ComponentResult> components;
    components.push_back(makeComponent("doc-hybrid", 0.80f, ComponentResult::Source::Text, 0));
    components.push_back(makeComponent("doc-hybrid", 0.80f, ComponentResult::Source::Vector, 0));

    auto results = fusion.fuse(components);
    REQUIRE(results.size() > 0U);
    REQUIRE(results.size() < 2U);

    const double baseScore = 1.6;
    CHECK(results[0].score > baseScore);
    CHECK(std::fabs(results[0].score - 1.76) < 1e-6);
}

TEST_CASE("Weighted reciprocal favors lexical over pure vector at equal rank",
          "[search][fusion][catch2]") {
    SearchEngineConfig cfg;
    cfg.maxResults = 10;
    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_RECIPROCAL;
    cfg.rrfK = 1.0f;
    cfg.textWeight = 1.0f;
    cfg.vectorWeight = 1.0f;
    cfg.vectorOnlyThreshold = 0.0f;
    cfg.vectorOnlyPenalty = 1.0f;
    cfg.vectorBoostFactor = 0.0f;

    ResultFusion fusion(cfg);
    std::vector<ComponentResult> components;
    components.push_back(makeComponent("doc-text", 0.80f, ComponentResult::Source::Text, 0));
    components.push_back(makeComponent("doc-vector", 1.00f, ComponentResult::Source::Vector, 0));

    auto results = fusion.fuse(components);
    REQUIRE(results.size() > 1U);

    CHECK(results[0].document.sha256Hash.compare("doc-text") == 0);
}
