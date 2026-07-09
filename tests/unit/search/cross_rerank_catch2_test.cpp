#include <catch2/catch_test_macros.hpp>

#include "src/search/cross_rerank_internal.h"

#include <yams/core/types.h>

#include <string>
#include <vector>

using yams::ErrorCode;
using yams::Result;
using yams::search::SearchEngineConfig;
using yams::search::SearchResult;
using yams::search::detail::applyCrossRerank;
using yams::search::detail::CrossRerankOutcome;

namespace {

SearchResult makeResult(std::string hash, float score) {
    SearchResult r;
    r.document.sha256Hash = std::move(hash);
    r.document.filePath = "/x/" + r.document.sha256Hash;
    r.score = score;
    return r;
}

std::vector<SearchResult> window3() {
    return {makeResult("a", 0.9F), makeResult("b", 0.6F), makeResult("c", 0.3F)};
}

SearchEngineConfig blendConfig() {
    SearchEngineConfig cfg;
    cfg.rerankReplaceScores = false;
    cfg.rerankBlendWeight = 0.3F;
    cfg.rerankScoreGapThreshold = 0.0F;
    cfg.rerankSnippetMaxChars = 256;
    return cfg;
}

// Stub scorer that returns a fixed set of scores (metadataRepo unused → nullptr).
auto fixedScorer(std::vector<float> scores) {
    return [scores = std::move(scores)](const std::string&, const std::vector<std::string>&)
               -> Result<std::vector<float>> { return scores; };
}

} // namespace

TEST_CASE("applyCrossRerank reorders the window and reports Applied", "[search][rerank][catch2]") {
    auto results = window3();
    auto cfg = blendConfig();
    cfg.rerankReplaceScores = true; // pure rerank order
    // Reverse the fusion order: c highest, a lowest.
    auto outcome = applyCrossRerank(results, "q", cfg, 3, fixedScorer({0.1F, 0.5F, 0.9F}), nullptr);

    CHECK(outcome.status == CrossRerankOutcome::Status::Applied);
    CHECK(outcome.attempted);
    CHECK(results.front().document.sha256Hash == "c"); // reranked to the front
    CHECK(results.back().document.sha256Hash == "a");
    CHECK(outcome.docTraces.size() == 3);
    CHECK(outcome.components.size() == 3);
}

TEST_CASE("applyCrossRerank blend keeps fusion influence vs replace", "[search][rerank][catch2]") {
    // Fusion order a>b>c; reranker mildly prefers c. Replace flips to c; blend(0.3) keeps a on top.
    auto scorer = fixedScorer({0.0F, 0.4F, 1.0F});

    auto replaceResults = window3();
    auto replaceCfg = blendConfig();
    replaceCfg.rerankReplaceScores = true;
    applyCrossRerank(replaceResults, "q", replaceCfg, 3, scorer, nullptr);
    CHECK(replaceResults.front().document.sha256Hash == "c");

    auto blendResults = window3();
    applyCrossRerank(blendResults, "q", blendConfig(), 3, scorer, nullptr);
    CHECK(blendResults.front().document.sha256Hash == "a"); // fusion still dominates at w=0.3
}

TEST_CASE("applyCrossRerank skips on no score variance", "[search][rerank][catch2]") {
    auto results = window3();
    auto outcome = applyCrossRerank(results, "q", blendConfig(), 3,
                                    fixedScorer({0.5F, 0.5F, 0.5F}), nullptr);
    CHECK(outcome.status == CrossRerankOutcome::Status::Skipped);
    CHECK(outcome.skipReason == "no_score_variance");
    CHECK(results.front().document.sha256Hash == "a"); // unchanged
}

TEST_CASE("applyCrossRerank skips when score gap below threshold", "[search][rerank][catch2]") {
    auto results = window3();
    auto cfg = blendConfig();
    cfg.rerankScoreGapThreshold = 0.5F; // gap 1.0-0.95 = 0.05 < 0.5
    auto outcome =
        applyCrossRerank(results, "q", cfg, 3, fixedScorer({0.90F, 0.95F, 1.0F}), nullptr);
    CHECK(outcome.status == CrossRerankOutcome::Status::Skipped);
    CHECK(outcome.skipReason == "score_gap_below_threshold");
}

TEST_CASE("applyCrossRerank fails on size mismatch", "[search][rerank][catch2]") {
    auto results = window3();
    auto outcome = applyCrossRerank(results, "q", blendConfig(), 3, fixedScorer({0.1F, 0.2F}),
                                    nullptr); // 2 scores for window 3
    CHECK(outcome.status == CrossRerankOutcome::Status::Failed);
    CHECK(outcome.errorMessage == "score_size_mismatch");
}

TEST_CASE("applyCrossRerank treats NotImplemented as reranker_unavailable skip",
          "[search][rerank][catch2]") {
    auto results = window3();
    auto scorer = [](const std::string&, const std::vector<std::string>&)
        -> Result<std::vector<float>> {
        return yams::Error{ErrorCode::NotImplemented, "no rerank"};
    };
    auto outcome = applyCrossRerank(results, "q", blendConfig(), 3, scorer, nullptr);
    CHECK(outcome.status == CrossRerankOutcome::Status::Skipped);
    CHECK(outcome.skipReason == "reranker_unavailable");
    CHECK(outcome.errorMessage == "no rerank");
    CHECK(outcome.attempted);
}

TEST_CASE("applyCrossRerank skips no_candidates and unavailable before calling",
          "[search][rerank][catch2]") {
    SECTION("empty window") {
        auto results = window3();
        auto outcome = applyCrossRerank(results, "q", blendConfig(), 0,
                                        fixedScorer({0.1F}), nullptr);
        CHECK(outcome.status == CrossRerankOutcome::Status::Skipped);
        CHECK(outcome.skipReason == "no_candidates");
        CHECK_FALSE(outcome.attempted);
    }
    SECTION("null reranker") {
        auto results = window3();
        auto outcome = applyCrossRerank(results, "q", blendConfig(), 3, nullptr, nullptr);
        CHECK(outcome.status == CrossRerankOutcome::Status::Skipped);
        CHECK(outcome.skipReason == "reranker_unavailable");
        CHECK_FALSE(outcome.attempted);
    }
}
