// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/document_metadata.h>
#include <yams/search/lexical_scoring.h>

#include <span>
#include <string>
#include <vector>

using Catch::Approx;
using yams::metadata::DocumentInfo;
using yams::metadata::SearchResult;
using yams::search::ComponentResult;
using yams::search::QueryIntent;
using yams::search::detail::Bm25Range;
using yams::search::detail::computeBm25Range;
using yams::search::detail::filenamePathBoost;
using yams::search::detail::LexicalScoringInputs;
using yams::search::detail::LexicalScoringOutput;
using yams::search::detail::normalizedBm25Score;
using yams::search::detail::scoreLexicalBatch;

namespace {

SearchResult makeSearchResult(const std::string& hash, const std::string& filePath,
                              const std::string& fileName, double score,
                              const std::string& snippet = {}) {
    SearchResult sr;
    sr.document.sha256Hash = hash;
    sr.document.filePath = filePath;
    sr.document.fileName = fileName;
    sr.score = score;
    sr.snippet = snippet;
    return sr;
}

} // namespace

TEST_CASE("computeBm25Range handles empty, single, multi", "[search][lexical][catch2]") {
    SECTION("empty span → uninitialized range") {
        std::vector<SearchResult> rows;
        auto r = computeBm25Range(std::span<const SearchResult>{rows});
        CHECK_FALSE(r.initialized);
    }
    SECTION("single row → min=max=score") {
        std::vector<SearchResult> rows = {makeSearchResult("h", "a.txt", "a.txt", -2.5)};
        auto r = computeBm25Range(std::span<const SearchResult>{rows});
        CHECK(r.initialized);
        CHECK(r.minScore == Approx(-2.5));
        CHECK(r.maxScore == Approx(-2.5));
    }
    SECTION("multi rows → min/max") {
        std::vector<SearchResult> rows = {
            makeSearchResult("a", "x.txt", "x.txt", -3.0),
            makeSearchResult("b", "y.txt", "y.txt", -1.0),
            makeSearchResult("c", "z.txt", "z.txt", -2.5),
        };
        auto r = computeBm25Range(std::span<const SearchResult>{rows});
        CHECK(r.initialized);
        CHECK(r.minScore == Approx(-3.0));
        CHECK(r.maxScore == Approx(-1.0));
    }
}

TEST_CASE("normalizedBm25Score inverts negative-is-better FTS5 convention",
          "[search][lexical][catch2]") {
    SECTION("min<max → (1 - (raw-min)/(max-min)) clamped [0,1]") {
        CHECK(normalizedBm25Score(-3.0, 300.0f, -3.0, -1.0) == Approx(1.0f));
        CHECK(normalizedBm25Score(-1.0, 300.0f, -3.0, -1.0) == Approx(0.0f));
        CHECK(normalizedBm25Score(-2.0, 300.0f, -3.0, -1.0) == Approx(0.5f));
    }
    SECTION("min==max → fallback -raw/divisor clamped") {
        CHECK(normalizedBm25Score(-150.0, 300.0f, -1.0, -1.0) == Approx(0.5f));
        CHECK(normalizedBm25Score(0.0, 300.0f, -1.0, -1.0) == Approx(0.0f));
        CHECK(normalizedBm25Score(-600.0, 300.0f, -1.0, -1.0) == Approx(1.0f));
    }
}

TEST_CASE("filenamePathBoost prefers name matches over path matches", "[search][lexical][catch2]") {
    CHECK(filenamePathBoost("foo", "/src/foo.cpp", "foo.cpp") > 1.0f);
    CHECK(filenamePathBoost("foo", "/src/foo.cpp", "foo.cpp") >
          filenamePathBoost("foo", "/src/foo.cpp", "other.cpp"));
    CHECK(filenamePathBoost("nomatch", "/src/a.cpp", "a.cpp") == Approx(1.0f));
    CHECK(filenamePathBoost("", "/src/a.cpp", "a.cpp") == Approx(1.0f));
}

TEST_CASE("scoreLexicalBatch empty input → empty output", "[search][lexical][catch2]") {
    std::vector<SearchResult> rows;
    LexicalScoringInputs inputs;
    inputs.candidates = std::span<const SearchResult>{rows};
    inputs.query = "anything";

    auto out = scoreLexicalBatch(inputs);
    CHECK(out.results.empty());
    CHECK(out.admissionDroppedCount == 0);
}

TEST_CASE("scoreLexicalBatch single-row batch → score maxed to 1.0 (min==max branch)",
          "[search][lexical][catch2]") {
    std::vector<SearchResult> rows = {
        makeSearchResult("h1", "/src/a.cpp", "a.cpp", -2.0),
    };
    LexicalScoringInputs inputs;
    inputs.candidates = std::span<const SearchResult>{rows};
    inputs.query = "nomatch";
    inputs.applyFilenameBoost = false;
    inputs.bm25NormDivisor = 300.0f;

    auto out = scoreLexicalBatch(inputs);
    REQUIRE(out.results.size() == 1);
    REQUIRE(out.results[0].has_value());
    CHECK(out.results[0]->score == Approx(2.0f / 300.0f));
    CHECK(out.results[0]->source == ComponentResult::Source::Text);
    CHECK(out.results[0]->rank == 0);
}

TEST_CASE("scoreLexicalBatch preserves monotonicity vs raw BM25", "[search][lexical][catch2]") {
    std::vector<SearchResult> rows = {
        makeSearchResult("a", "/src/a.cpp", "a.cpp", -5.0),
        makeSearchResult("b", "/src/b.cpp", "b.cpp", -2.0),
        makeSearchResult("c", "/src/c.cpp", "c.cpp", -1.0),
    };
    LexicalScoringInputs inputs;
    inputs.candidates = std::span<const SearchResult>{rows};
    inputs.query = "nomatch";
    inputs.applyFilenameBoost = false;

    auto out = scoreLexicalBatch(inputs);
    REQUIRE(out.results.size() == 3);
    REQUIRE(out.results[0].has_value());
    REQUIRE(out.results[1].has_value());
    REQUIRE(out.results[2].has_value());
    CHECK(out.results[0]->score >= out.results[1]->score);
    CHECK(out.results[1]->score >= out.results[2]->score);
    CHECK(out.results[0]->score == Approx(1.0f));
    CHECK(out.results[2]->score == Approx(0.0f));
}

TEST_CASE("scoreLexicalBatch applies filename boost when enabled", "[search][lexical][catch2]") {
    std::vector<SearchResult> rows = {
        makeSearchResult("a", "/src/alpha.cpp", "alpha.cpp", -2.0),
        makeSearchResult("b", "/src/other.cpp", "other.cpp", -2.0),
    };
    LexicalScoringInputs inputsOff;
    inputsOff.candidates = std::span<const SearchResult>{rows};
    inputsOff.query = "alpha";
    inputsOff.applyFilenameBoost = false;

    auto offOut = scoreLexicalBatch(inputsOff);
    REQUIRE(offOut.results.size() == 2);
    REQUIRE(offOut.results[0]->score == Approx(offOut.results[1]->score));

    LexicalScoringInputs inputsOn = inputsOff;
    inputsOn.applyFilenameBoost = true;

    auto onOut = scoreLexicalBatch(inputsOn);
    REQUIRE(onOut.results.size() == 2);
    CHECK(onOut.results[0]->score >= onOut.results[1]->score);
}

TEST_CASE("scoreLexicalBatch penalty multiplier propagates", "[search][lexical][catch2]") {
    std::vector<SearchResult> rows = {
        makeSearchResult("a", "/src/a.cpp", "a.cpp", -3.0),
        makeSearchResult("b", "/src/b.cpp", "b.cpp", -1.0),
    };

    LexicalScoringInputs base;
    base.candidates = std::span<const SearchResult>{rows};
    base.query = "nomatch";
    base.applyFilenameBoost = false;
    base.penalty = 1.0f;

    LexicalScoringInputs penalized = base;
    penalized.penalty = 0.5f;

    auto baseOut = scoreLexicalBatch(base);
    auto penalizedOut = scoreLexicalBatch(penalized);

    REQUIRE(baseOut.results.size() == 2);
    REQUIRE(penalizedOut.results.size() == 2);
    REQUIRE(baseOut.results[0].has_value());
    REQUIRE(penalizedOut.results[0].has_value());
    CHECK(penalizedOut.results[0]->score == Approx(baseOut.results[0]->score * 0.5f));
}

TEST_CASE("scoreLexicalBatch source tag + startRank pass through", "[search][lexical][catch2]") {
    std::vector<SearchResult> rows = {
        makeSearchResult("a", "/src/a.cpp", "a.cpp", -2.0),
        makeSearchResult("b", "/src/b.cpp", "b.cpp", -1.0),
    };
    LexicalScoringInputs inputs;
    inputs.candidates = std::span<const SearchResult>{rows};
    inputs.query = "nomatch";
    inputs.applyFilenameBoost = false;
    inputs.sourceTag = ComponentResult::Source::GraphText;
    inputs.startRank = 7;

    auto out = scoreLexicalBatch(inputs);
    REQUIRE(out.results.size() == 2);
    REQUIRE(out.results[0].has_value());
    REQUIRE(out.results[1].has_value());
    CHECK(out.results[0]->source == ComponentResult::Source::GraphText);
    CHECK(out.results[1]->source == ComponentResult::Source::GraphText);
    CHECK(out.results[0]->rank == 7);
    CHECK(out.results[1]->rank == 8);
}

TEST_CASE("scoreLexicalBatch admission threshold drops low-score rows",
          "[search][lexical][catch2]") {
    std::vector<SearchResult> rows = {
        makeSearchResult("a", "/src/a.cpp", "a.cpp", -5.0),
        makeSearchResult("b", "/src/b.cpp", "b.cpp", -2.0),
        makeSearchResult("c", "/src/c.cpp", "c.cpp", -1.0),
    };
    LexicalScoringInputs inputs;
    inputs.candidates = std::span<const SearchResult>{rows};
    inputs.query = "nomatch";
    inputs.applyFilenameBoost = false;
    inputs.admissionMinScore = 0.5f;

    auto out = scoreLexicalBatch(inputs);
    REQUIRE(out.results.size() == 3);
    CHECK(out.results[0].has_value());
    CHECK_FALSE(out.results[1].has_value());
    CHECK_FALSE(out.results[2].has_value());
    CHECK(out.admissionDroppedCount == 2);
}

TEST_CASE("scoreLexicalBatch intent-adaptive weighting for non-code files",
          "[search][lexical][catch2]") {
    std::vector<SearchResult> rows = {
        makeSearchResult("a", "/docs/readme.md", "readme.md", -2.0),
        makeSearchResult("b", "/src/main.cpp", "main.cpp", -2.0),
    };

    LexicalScoringInputs codeIntent;
    codeIntent.candidates = std::span<const SearchResult>{rows};
    codeIntent.query = "nomatch";
    codeIntent.applyFilenameBoost = false;
    codeIntent.applyIntentAdaptiveWeighting = true;
    codeIntent.intent = QueryIntent::Code;

    auto out = scoreLexicalBatch(codeIntent);
    REQUIRE(out.results.size() == 2);
    REQUIRE(out.results[0].has_value());
    REQUIRE(out.results[1].has_value());
    CHECK(out.results[0]->score <= out.results[1]->score);
}

TEST_CASE("scoreLexicalBatch snippet passthrough + debug info", "[search][lexical][catch2]") {
    std::vector<SearchResult> rows = {
        makeSearchResult("a", "/src/a.cpp", "a.cpp", -2.0, "hello world"),
        makeSearchResult("b", "/src/b.cpp", "b.cpp", -1.0, ""),
    };
    LexicalScoringInputs inputs;
    inputs.candidates = std::span<const SearchResult>{rows};
    inputs.query = "nomatch";
    inputs.applyFilenameBoost = false;

    auto out = scoreLexicalBatch(inputs);
    REQUIRE(out.results.size() == 2);
    REQUIRE(out.results[0].has_value());
    REQUIRE(out.results[1].has_value());
    REQUIRE(out.results[0]->snippet.has_value());
    CHECK(*out.results[0]->snippet == "hello world");
    CHECK_FALSE(out.results[1]->snippet.has_value());
    CHECK(out.results[0]->debugInfo.count("score_multiplier") == 1);
}
