#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/search/search_result_fusion.h>

#include <algorithm>
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
    REQUIRE((results.size() > 0U));
    REQUIRE((results.size() < 2U));
    CHECK((results[0].document.sha256Hash.compare("doc-vector-high") == 0));
    CHECK((std::fabs(results[0].score - 0.475) < 1e-6));
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
    REQUIRE((results.size() > 0U));
    REQUIRE((results.size() < 2U));

    const double baseScore = 1.6;
    CHECK((results[0].score > baseScore));
    CHECK((std::fabs(results[0].score - 1.76) < 1e-6));
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
    REQUIRE((results.size() > 1U));

    CHECK((results[0].document.sha256Hash.compare("doc-text") == 0));
}

TEST_CASE("ResultFusion reserves bounded slots for topology sidecar candidates",
          "[search][fusion][topology][catch2]") {
    SearchEngineConfig cfg;
    cfg.maxResults = 2;
    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_RECIPROCAL;
    cfg.rrfK = 1.0f;
    cfg.vectorOnlyThreshold = 0.0f;
    cfg.vectorOnlyPenalty = 1.0f;
    cfg.vectorWeight = 1.0f;
    cfg.graphVectorWeight = 0.01f;

    ComponentResult baseA = makeComponent("base-a", 1.0f, ComponentResult::Source::Vector, 0);
    ComponentResult baseB = makeComponent("base-b", 0.99f, ComponentResult::Source::Vector, 1);
    ComponentResult sidecarA =
        makeComponent("sidecar-a", 0.30f, ComponentResult::Source::GraphVector, 2);
    sidecarA.debugInfo["topology_sidecar"] = "1";
    ComponentResult sidecarB =
        makeComponent("sidecar-b", 0.20f, ComponentResult::Source::GraphVector, 3);
    sidecarB.debugInfo["topology_sidecar"] = "1";

    ResultFusion baselineFusion(cfg);
    auto baseline = baselineFusion.fuse({baseA, baseB, sidecarA, sidecarB});
    REQUIRE((baseline.size() == 2U));
    CHECK(std::none_of(baseline.begin(), baseline.end(),
                       [](const auto& result) { return result.topologySidecar; }));

    cfg.topologySidecarFusionRescueSlots = 1;
    ResultFusion rescueFusion(cfg);
    auto rescued = rescueFusion.fuse({baseA, baseB, sidecarA, sidecarB});
    REQUIRE((rescued.size() == 2U));
    CHECK((std::count_if(rescued.begin(), rescued.end(),
                         [](const auto& result) { return result.topologySidecar; }) == 1));
}

TEST_CASE("ResultFusion default strategy honors component weights", "[search][fusion][catch2]") {
    SearchEngineConfig cfg;
    cfg.maxResults = 10;
    cfg.rrfK = 1.0f;
    cfg.vectorOnlyThreshold = 0.0f;
    cfg.vectorOnlyPenalty = 1.0f;
    cfg.vectorBoostFactor = 0.0f;
    cfg.textWeight = 0.10f;
    cfg.vectorWeight = 1.00f;

    ResultFusion fusion(cfg);
    std::vector<ComponentResult> components;
    components.push_back(makeComponent("doc-text", 1.00f, ComponentResult::Source::Text, 0));
    components.push_back(makeComponent("doc-vector", 1.00f, ComponentResult::Source::Vector, 0));

    auto results = fusion.fuse(components);
    REQUIRE((results.size() == 2U));
    CHECK((results[0].document.sha256Hash == "doc-vector"));
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
    REQUIRE((results.size() == 1U));
    CHECK((results[0].snippet == "anchored snippet"));
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
    REQUIRE((results.size() == 1U));
    CHECK((results[0].document.sha256Hash == "doc-semantic"));
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
    REQUIRE((results.size() == 2U));
    CHECK((results[0].document.sha256Hash == "doc-lexical-a"));
    CHECK((results[1].document.sha256Hash == "doc-semantic-strong"));
}

TEST_CASE("ResultFusion semantic rescue keeps rescued docs competitive for rerank window",
          "[search][fusion][catch2]") {
    SearchEngineConfig cfg;
    cfg.maxResults = 3;
    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::COMB_MNZ;
    cfg.textWeight = 1.0f;
    cfg.vectorWeight = 1.0f;
    cfg.vectorOnlyThreshold = 0.92f;
    cfg.vectorOnlyPenalty = 0.65f;
    cfg.semanticRescueSlots = 1;
    cfg.semanticRescueMinVectorScore = 0.30f;
    cfg.enableReranking = true;
    cfg.rerankTopK = 2;

    ResultFusion fusion(cfg);

    ComponentResult lexicalA =
        makeComponent("doc-lexical-a", 0.9f, ComponentResult::Source::Text, 0);
    ComponentResult lexicalB =
        makeComponent("doc-lexical-b", 0.85f, ComponentResult::Source::Text, 1);
    ComponentResult lexicalC =
        makeComponent("doc-lexical-c", 0.8f, ComponentResult::Source::Text, 2);
    ComponentResult rescued =
        makeComponent("doc-semantic", 0.80f, ComponentResult::Source::Vector, 150);

    auto results = fusion.fuse({lexicalA, lexicalB, lexicalC, rescued});
    REQUIRE((results.size() == 3U));
    CHECK((results[0].document.sha256Hash == "doc-lexical-a"));
    CHECK((results[1].document.sha256Hash == "doc-semantic"));
}

// P7: convex fusion — normalized per-component scores combined with component weights.
TEST_CASE("ResultFusion CONVEX normalizes per-component scores and applies weights",
          "[search][fusion][p7][catch2]") {
    SearchEngineConfig cfg;
    cfg.maxResults = 10;
    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::CONVEX;
    // Zero-out guardrails that would otherwise drop vector-only results,
    // and disable hybrid-agreement boost so the convex math is observable directly.
    cfg.vectorOnlyThreshold = 0.0f;
    cfg.vectorOnlyPenalty = 1.0f;
    cfg.vectorBoostFactor = 0.0f;
    cfg.textWeight = 0.60f;
    cfg.vectorWeight = 0.40f;

    ResultFusion fusion(cfg);
    // docA: dominates Text (raw 0.80 -> norm 1.0). docB: dominates Vector (raw 0.50 -> norm 1.0).
    // docA expected contribution = 0.60 * 1.0 = 0.60
    // docB expected contribution = 0.40 * 1.0 = 0.40
    // docA should therefore rank above docB.
    std::vector<ComponentResult> components;
    components.push_back(makeComponent("docA", 0.80f, ComponentResult::Source::Text, 0));
    components.push_back(makeComponent("docB", 0.40f, ComponentResult::Source::Text, 1));
    components.push_back(makeComponent("docA", 0.25f, ComponentResult::Source::Vector, 1));
    components.push_back(makeComponent("docB", 0.50f, ComponentResult::Source::Vector, 0));

    auto results = fusion.fuse(components);
    REQUIRE((results.size() == 2U));
    CHECK((results[0].document.sha256Hash == "docA"));
    CHECK((results[1].document.sha256Hash == "docB"));
    // docA score: textWeight*(0.80/0.80) + vectorWeight*(0.25/0.50) = 0.60 + 0.20 = 0.80
    CHECK((std::fabs(results[0].score - 0.80) < 1e-6));
    // docB score: textWeight*(0.40/0.80) + vectorWeight*(0.50/0.50) = 0.30 + 0.40 = 0.70
    CHECK((std::fabs(results[1].score - 0.70) < 1e-6));
}

TEST_CASE("ResultFusion CONVEX handles empty input and zero-weight components",
          "[search][fusion][p7][catch2]") {
    SearchEngineConfig cfg;
    cfg.maxResults = 10;
    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::CONVEX;
    cfg.vectorOnlyThreshold = 0.0f;
    cfg.vectorOnlyPenalty = 1.0f;
    cfg.textWeight = 0.0f; // zero weight on Text — should contribute nothing
    cfg.vectorWeight = 1.0f;

    ResultFusion fusion(cfg);
    CHECK(fusion.fuse({}).empty());

    std::vector<ComponentResult> components;
    components.push_back(makeComponent("docOnlyText", 0.90f, ComponentResult::Source::Text, 0));
    auto results = fusion.fuse(components);
    // Text has zero weight → doc gets a 0 score; fusion machinery may still admit it as a
    // candidate, but when it does, the score must be 0.
    if (!results.empty()) {
        CHECK((std::fabs(results[0].score) < 1e-6));
    }
}

TEST_CASE("ResultFusion covers empty input and remaining strategy branches",
          "[search][fusion][catch2]") {
    SearchEngineConfig cfg;
    cfg.maxResults = 10;
    cfg.vectorOnlyThreshold = 0.0f;
    cfg.vectorOnlyPenalty = 1.0f;
    cfg.textWeight = 1.0f;
    cfg.vectorWeight = 0.5f;
    cfg.rrfK = 1.0f;

    ResultFusion emptyFusion(cfg);
    CHECK(emptyFusion.fuse({}).empty());

    std::vector<ComponentResult> components;
    components.push_back(makeComponent("doc-a", 0.9f, ComponentResult::Source::Text, 0));
    components.push_back(makeComponent("doc-b", 0.8f, ComponentResult::Source::Vector, 1));
    components.push_back(makeComponent("doc-a", 0.7f, ComponentResult::Source::PathTree, 2));

    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::RECIPROCAL_RANK;
    ResultFusion reciprocal(cfg);
    auto reciprocalResults = reciprocal.fuse(components);
    REQUIRE((reciprocalResults.size() == 2U));
    CHECK((reciprocalResults.front().document.sha256Hash == "doc-a"));

    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::CONVEX;
    cfg.weightedLinearZScorePoolSize = 10;
    ResultFusion convex(cfg);
    auto convexResults = convex.fuse(components);
    REQUIRE((convexResults.size() == 2U));
    CHECK((convexResults.front().score >= convexResults.back().score));

    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_LINEAR_ZSCORE;
    cfg.weightedLinearZScoreUseZScore = false;
    ResultFusion linear(cfg);
    auto linearResults = linear.fuse(components);
    REQUIRE((linearResults.size() == 2U));
    CHECK((linearResults.front().score >= linearResults.back().score));
}

TEST_CASE("ResultFusion inline helpers cover relief and rescue calculations",
          "[search][fusion][catch2]") {
    SearchEngineConfig cfg;
    cfg.enableStrongVectorOnlyRelief = false;
    cfg.vectorOnlyPenalty = 0.25f;
    CHECK((strongVectorOnlyReliefStrength(cfg, 1.0, 0) == 0.0));
    CHECK((effectiveVectorOnlyPenalty(cfg, 1.0, 0) == Catch::Approx(0.25)));

    cfg.enableStrongVectorOnlyRelief = true;
    cfg.strongVectorOnlyMinScore = 0.90f;
    cfg.strongVectorOnlyTopRank = 3;
    cfg.strongVectorOnlyPenalty = 0.90f;
    CHECK((strongVectorOnlyReliefEligible(cfg, 0.95, 10)));
    CHECK((effectiveVectorOnlyPenalty(cfg, 0.95, 10) > 0.25));

    cfg.enableReranking = true;
    cfg.rerankTopK = 7;
    cfg.maxResults = 20;
    CHECK((semanticRescueWindowLimit(cfg) == 7U));
    cfg.enableReranking = false;
    CHECK((semanticRescueWindowLimit(cfg) == 20U));
}
