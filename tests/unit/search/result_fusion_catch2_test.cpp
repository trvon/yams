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

TEST_CASE("ResultFusion COMB_MNZ backfills snippet from later anchored component",
          "[search][fusion][catch2]") {
    SearchEngineConfig cfg;
    cfg.maxResults = 10;
    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::COMB_MNZ;
    cfg.vectorWeight = 1.0f;
    cfg.textWeight = 1.0f;
    cfg.vectorOnlyThreshold = 0.0f;
    cfg.vectorOnlyPenalty = 1.0f;

    ResultFusion fusion(cfg);

    ComponentResult vector = makeComponent("doc-hybrid", 0.95f, ComponentResult::Source::Vector, 0);
    vector.snippet = std::nullopt;

    ComponentResult text = makeComponent("doc-hybrid", 0.40f, ComponentResult::Source::Text, 5);
    text.snippet = std::string("anchored snippet");

    std::vector<ComponentResult> components;
    components.push_back(vector);
    components.push_back(text);

    auto results = fusion.fuse(components);
    REQUIRE(results.size() == 1U);
    CHECK(results[0].snippet == "anchored snippet");
}

TEST_CASE("ResultFusion COMB_MNZ semantic rescue can retain below-threshold vector-only docs",
          "[search][fusion][catch2]") {
    SearchEngineConfig cfg;
    cfg.maxResults = 1;
    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::COMB_MNZ;
    cfg.textWeight = 1.0f;
    cfg.vectorWeight = 1.0f;
    cfg.vectorOnlyThreshold = 0.92f;
    cfg.vectorOnlyPenalty = 0.65f;
    cfg.semanticRescueSlots = 1;
    cfg.semanticRescueMinVectorScore = 0.30f;

    ResultFusion fusion(cfg);

    ComponentResult lexical;
    lexical.documentHash = "doc-lexical";
    lexical.filePath = "doc-lexical";
    lexical.score = 0.6f;
    lexical.source = ComponentResult::Source::Text;
    lexical.rank = 0;

    ComponentResult rescued;
    rescued.documentHash = "doc-semantic";
    rescued.filePath = "doc-semantic";
    rescued.score = 0.80f;
    rescued.source = ComponentResult::Source::Vector;
    rescued.rank = 150;

    auto results = fusion.fuse({lexical, rescued});
    REQUIRE(results.size() == 1U);
    CHECK(results[0].document.sha256Hash == "doc-semantic");
}

TEST_CASE("ResultFusion COMB_MNZ prefers stronger raw vector docs for semantic rescue",
          "[search][fusion][catch2]") {
    SearchEngineConfig cfg;
    cfg.maxResults = 2;
    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::COMB_MNZ;
    cfg.textWeight = 1.0f;
    cfg.vectorWeight = 1.0f;
    cfg.vectorOnlyThreshold = 0.92f;
    cfg.vectorOnlyPenalty = 0.65f;
    cfg.semanticRescueSlots = 1;
    cfg.semanticRescueMinVectorScore = 0.30f;

    ResultFusion fusion(cfg);

    ComponentResult lexicalA;
    lexicalA.documentHash = "doc-lexical-a";
    lexicalA.filePath = "doc-lexical-a";
    lexicalA.score = 0.8f;
    lexicalA.source = ComponentResult::Source::Text;
    lexicalA.rank = 0;

    ComponentResult lexicalB;
    lexicalB.documentHash = "doc-lexical-b";
    lexicalB.filePath = "doc-lexical-b";
    lexicalB.score = 0.7f;
    lexicalB.source = ComponentResult::Source::Text;
    lexicalB.rank = 1;

    ComponentResult rescueStrong;
    rescueStrong.documentHash = "doc-semantic-strong";
    rescueStrong.filePath = "doc-semantic-strong";
    rescueStrong.score = 0.88f;
    rescueStrong.source = ComponentResult::Source::Vector;
    rescueStrong.rank = 80;

    ComponentResult rescueWeak;
    rescueWeak.documentHash = "doc-semantic-weak";
    rescueWeak.filePath = "doc-semantic-weak";
    rescueWeak.score = 0.82f;
    rescueWeak.source = ComponentResult::Source::Vector;
    rescueWeak.rank = 10;

    auto results = fusion.fuse({lexicalA, lexicalB, rescueStrong, rescueWeak});
    REQUIRE(results.size() == 2U);
    CHECK(results[0].document.sha256Hash == "doc-lexical-a");
    CHECK(results[1].document.sha256Hash == "doc-semantic-strong");
}
