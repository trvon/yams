#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include "src/search/lexical_rerank_internal.h"

#include <yams/core/types.h>

#include <string>
#include <unordered_map>
#include <vector>

using yams::ErrorCode;
using yams::Result;
using yams::search::SearchEngineConfig;
using yams::search::SearchResult;
using yams::search::detail::applyLexicalDeepRerank;
using yams::search::detail::LexicalRerankOutcome;
using yams::search::detail::LexicalRerankScorerFn;
using yams::search::detail::lexicalRerankTerms;

namespace {

SearchResult makeResult(int64_t id, double score) {
    SearchResult r;
    r.document.id = id;
    r.document.sha256Hash = "h" + std::to_string(id);
    r.score = score;
    return r;
}

// Fused order 1..n with descending scores.
std::vector<SearchResult> fusedList(int n) {
    std::vector<SearchResult> out;
    for (int i = 1; i <= n; ++i) {
        out.push_back(makeResult(i, 1.0 - 0.01 * i));
    }
    return out;
}

// Scores keyed by numeric id for readability; results carry hash "h<id>".
LexicalRerankScorerFn fixedScores(std::unordered_map<int64_t, double> scores,
                                  std::vector<std::string>* seenHashes = nullptr) {
    return [scores = std::move(scores), seenHashes](const std::vector<std::string>&,
                                                    const std::vector<std::string>& hashes)
               -> Result<std::unordered_map<std::string, double>> {
        if (seenHashes) {
            *seenHashes = hashes;
        }
        std::unordered_map<std::string, double> out;
        for (const auto& [id, score] : scores) {
            out["h" + std::to_string(id)] = score;
        }
        return out;
    };
}

SearchEngineConfig deepConfig(size_t depth, float weight) {
    SearchEngineConfig cfg;
    cfg.lexicalRerankDepth = depth;
    cfg.lexicalRerankWeight = weight;
    return cfg;
}

} // namespace

TEST_CASE("lexicalRerankTerms lowercases, dedupes and drops one-char tokens and stopwords",
          "[search][rerank][lexical_rerank][catch2]") {
    const auto terms = lexicalRerankTerms("Vitamin D and the vitamin-D3 is in a b\"x\"");
    CHECK(terms == std::vector<std::string>{"d3", "vitamin"});
    // Stopwords only: nothing left to score.
    CHECK(lexicalRerankTerms("what is the").empty());
}

TEST_CASE("applyLexicalDeepRerank lifts a deep lexical match into the top results",
          "[search][rerank][lexical_rerank][catch2]") {
    auto results = fusedList(25);
    std::vector<std::string> seen;
    // Doc 15 is the only strong lexical match inside the depth-20 pool.
    auto outcome = applyLexicalDeepRerank(results, "q term", deepConfig(20, 0.5F),
                                          fixedScores({{15, 30.0}, {1, 1.0}}, &seen));
    REQUIRE(outcome.status == LexicalRerankOutcome::Status::Applied);
    CHECK(seen.size() == 20); // scorer sees exactly the pool
    CHECK(results.front().document.id == 15);
    CHECK(outcome.depth == 20);
    CHECK(outcome.matched == 2);
}

TEST_CASE("applyLexicalDeepRerank keeps the tail order and below the reranked pool",
          "[search][rerank][lexical_rerank][catch2]") {
    auto results = fusedList(25);
    auto outcome =
        applyLexicalDeepRerank(results, "q term", deepConfig(20, 0.5F), fixedScores({{15, 30.0}}));
    REQUIRE(outcome.status == LexicalRerankOutcome::Status::Applied);
    double poolMin = results.front().score;
    for (size_t i = 0; i < 20; ++i) {
        poolMin = std::min(poolMin, results[i].score);
    }
    for (size_t i = 20; i < results.size(); ++i) {
        CHECK(results[i].document.id == static_cast<int64_t>(i + 1));
        CHECK(results[i].score < poolMin);
    }
}

TEST_CASE("applyLexicalDeepRerank weight 0 keeps the fused order",
          "[search][rerank][lexical_rerank][catch2]") {
    auto results = fusedList(10);
    auto outcome =
        applyLexicalDeepRerank(results, "q term", deepConfig(20, 0.0F), fixedScores({{9, 30.0}}));
    REQUIRE(outcome.status == LexicalRerankOutcome::Status::Applied);
    for (size_t i = 0; i < results.size(); ++i) {
        CHECK(results[i].document.id == static_cast<int64_t>(i + 1));
    }
}

TEST_CASE("applyLexicalDeepRerank skips without terms, matches, scorer or depth",
          "[search][rerank][lexical_rerank][catch2]") {
    SECTION("disabled") {
        auto results = fusedList(5);
        auto o = applyLexicalDeepRerank(results, "q term", deepConfig(0, 0.5F), fixedScores({}));
        CHECK(o.status == LexicalRerankOutcome::Status::Skipped);
        CHECK(o.skipReason == "disabled");
    }
    SECTION("no query terms") {
        auto results = fusedList(5);
        auto o = applyLexicalDeepRerank(results, "a ?", deepConfig(20, 0.5F), fixedScores({}));
        CHECK(o.skipReason == "no_terms");
    }
    SECTION("no lexical matches") {
        auto results = fusedList(5);
        auto o = applyLexicalDeepRerank(results, "q term", deepConfig(20, 0.5F), fixedScores({}));
        CHECK(o.skipReason == "no_matches");
        CHECK(results.front().document.id == 1);
    }
    SECTION("no scorer") {
        auto results = fusedList(5);
        auto o = applyLexicalDeepRerank(results, "q term", deepConfig(20, 0.5F), {});
        CHECK(o.skipReason == "scorer_unavailable");
    }
}

TEST_CASE("applyLexicalDeepRerank leaves results unchanged when the scorer fails",
          "[search][rerank][lexical_rerank][catch2]") {
    auto results = fusedList(5);
    LexicalRerankScorerFn failing =
        [](const std::vector<std::string>&,
           const std::vector<std::string>&) -> Result<std::unordered_map<std::string, double>> {
        return yams::Error{ErrorCode::DatabaseError, "boom"};
    };
    auto o = applyLexicalDeepRerank(results, "q term", deepConfig(20, 0.5F), failing);
    CHECK(o.status == LexicalRerankOutcome::Status::Failed);
    CHECK(o.errorMessage == "boom");
    for (size_t i = 0; i < results.size(); ++i) {
        CHECK(results[i].document.id == static_cast<int64_t>(i + 1));
        CHECK(results[i].score == Catch::Approx(1.0 - 0.01 * static_cast<double>(i + 1)));
    }
}

#include "topology_search_test_support.h"

TEST_CASE("MetadataRepository::bm25ForDocuments scores only the given ids, OR over terms",
          "[search][rerank][lexical_rerank][catch2]") {
    using yams::search::topology_test_support::TopologySearchFixture;
    TopologySearchFixture fix;
    fix.addDocument("x1", "alpha beta alpha", {1.0F, 0.0F});
    fix.addDocument("x2", "gamma delta", {0.0F, 1.0F});
    fix.addDocument("x3", "alpha only here", {0.5F, 0.5F});

    const std::string x1 = "x1";
    const std::string x2 = "x2";
    const std::string x3 = "x3";

    auto scores = fix.repo->bm25ForDocuments({"alpha", "gamma"}, {x1, x2});
    REQUIRE(scores.has_value());
    const auto& s = scores.value();
    CHECK(s.size() == 2);
    REQUIRE(s.contains(x1));
    REQUIRE(s.contains(x2));
    CHECK(s.at(x1) > 0.0);
    CHECK(s.at(x2) > 0.0);
    CHECK_FALSE(s.contains(x3)); // matches "alpha" but not in the id set

    auto none = fix.repo->bm25ForDocuments({"zeta"}, {x1, x2, x3});
    REQUIRE(none.has_value());
    CHECK(none.value().empty());

    auto empty = fix.repo->bm25ForDocuments({}, {x1});
    REQUIRE(empty.has_value());
    CHECK(empty.value().empty());
}

TEST_CASE("applyLexicalDeepRerank scores results that carry only a hash, no document id",
          "[search][rerank][lexical_rerank][catch2]") {
    // Simeon-lexical and vector hits reach fusion without a metadata id.
    auto results = fusedList(10);
    for (auto& r : results) {
        r.document.id = 0;
    }
    std::vector<std::string> seen;
    auto outcome = applyLexicalDeepRerank(results, "q term", deepConfig(20, 0.5F),
                                          fixedScores({{7, 30.0}}, &seen));
    REQUIRE(outcome.status == LexicalRerankOutcome::Status::Applied);
    CHECK(seen.front() == "h1");
    CHECK(results.front().document.sha256Hash == "h7");
}

namespace {

using yams::search::detail::SemanticScorerFn;

SemanticScorerFn fixedSemantic(std::unordered_map<int64_t, double> scores,
                               std::vector<std::string>* seenHashes = nullptr, bool fail = false) {
    return [scores = std::move(scores), seenHashes, fail](const std::string&,
                                                          const std::vector<std::string>& hashes)
               -> Result<std::unordered_map<std::string, double>> {
        if (seenHashes) {
            *seenHashes = hashes;
        }
        if (fail) {
            return yams::Error{ErrorCode::InternalError, "no table"};
        }
        std::unordered_map<std::string, double> out;
        for (const auto& [id, score] : scores) {
            out["h" + std::to_string(id)] = score;
        }
        return out;
    };
}

SearchEngineConfig staticConfig() {
    auto cfg = deepConfig(20, 0.5F);
    cfg.staticRerankDepth = 50;
    cfg.staticRerankLexicalWeight = 0.33F;
    cfg.staticRerankWeight = 0.33F;
    return cfg;
}

} // namespace

TEST_CASE("applyLexicalDeepRerank with a semantic scorer reranks the deeper static pool",
          "[search][rerank][lexical_rerank][catch2]") {
    auto results = fusedList(60);
    std::vector<std::string> lexicalSeen;
    std::vector<std::string> semanticSeen;
    // Doc 40 is outside the lexical depth (20) but inside the static depth (50).
    auto outcome = applyLexicalDeepRerank(results, "q term", staticConfig(),
                                          fixedScores({{40, 20.0}, {2, 5.0}}, &lexicalSeen),
                                          fixedSemantic({{40, 0.9}, {1, 0.1}}, &semanticSeen));
    REQUIRE(outcome.status == LexicalRerankOutcome::Status::Applied);
    CHECK(outcome.semanticApplied);
    CHECK(outcome.depth == 50);
    CHECK(semanticSeen.size() == 50);
    CHECK(lexicalSeen.size() == 50);
    CHECK(results.front().document.sha256Hash == "h40");
    // The tail beyond the static pool keeps fused order.
    for (size_t i = 50; i < results.size(); ++i) {
        CHECK(results[i].document.id == static_cast<int64_t>(i + 1));
    }
}

TEST_CASE("applyLexicalDeepRerank falls back to lexical-only when the semantic scorer fails",
          "[search][rerank][lexical_rerank][catch2]") {
    auto results = fusedList(60);
    std::vector<std::string> lexicalSeen;
    auto outcome = applyLexicalDeepRerank(results, "q term", staticConfig(),
                                          fixedScores({{15, 30.0}}, &lexicalSeen),
                                          fixedSemantic({}, nullptr, /*fail=*/true));
    REQUIRE(outcome.status == LexicalRerankOutcome::Status::Applied);
    CHECK_FALSE(outcome.semanticApplied);
    CHECK(outcome.semanticError == "no table");
    CHECK(outcome.depth == 20);
    CHECK(lexicalSeen.size() == 20);
    CHECK(results.front().document.sha256Hash == "h15");
}

TEST_CASE("applyLexicalDeepRerank applies semantic scores without any lexical match",
          "[search][rerank][lexical_rerank][catch2]") {
    auto results = fusedList(30);
    auto outcome = applyLexicalDeepRerank(results, "q term", staticConfig(), fixedScores({}),
                                          fixedSemantic({{25, 0.95}}));
    REQUIRE(outcome.status == LexicalRerankOutcome::Status::Applied);
    CHECK(outcome.semanticApplied);
    CHECK(outcome.matched == 0);
    CHECK(outcome.depth == 30);
}

TEST_CASE("applyLexicalDeepRerank ignores the semantic scorer when static depth is 0",
          "[search][rerank][lexical_rerank][catch2]") {
    auto results = fusedList(30);
    auto cfg = staticConfig();
    cfg.staticRerankDepth = 0;
    std::vector<std::string> semanticSeen;
    auto outcome = applyLexicalDeepRerank(results, "q term", cfg, fixedScores({{3, 10.0}}),
                                          fixedSemantic({{25, 0.95}}, &semanticSeen));
    REQUIRE(outcome.status == LexicalRerankOutcome::Status::Applied);
    CHECK_FALSE(outcome.semanticApplied);
    CHECK(semanticSeen.empty());
    CHECK(outcome.depth == 20);
}
