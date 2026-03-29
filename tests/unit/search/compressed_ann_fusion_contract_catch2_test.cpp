// Copyright 2025 YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later OR MIT

/**
 * @file compressed_ann_fusion_contract_catch2_test.cpp
 * @brief Contract tests for CompressedANN as a vector-family source in result fusion
 *
 * These tests verify the contract that CompressedANN is treated as part of the
 * vector-family semantics, as specified in include/yams/search/search_engine.h:568.
 *
 * Contract assertions:
 * 1. componentSourceToString returns "compressed_ann" for CompressedANN source
 * 2. componentSourceWeight returns config.compressedAnnWeight
 * 3. isVectorComponent returns true for CompressedANN
 * 4. accumulateComponentScore routes CompressedANN into vectorScore
 * 5. componentSourceScoreInResult reads CompressedANN back from vectorScore
 */

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/search/search_engine.h>

#include <cmath>
#include <optional>

using yams::search::accumulateComponentScore;
using yams::search::ComponentResult;
using yams::search::componentSourceScoreInResult;
using yams::search::componentSourceToString;
using yams::search::componentSourceWeight;
using yams::search::isVectorComponent;
using yams::search::SearchEngineConfig;
using yams::search::SearchResult;

namespace {

SearchEngineConfig makeConfig() {
    SearchEngineConfig cfg;
    cfg.compressedAnnWeight = 0.15f;
    cfg.vectorWeight = 1.0f;
    cfg.textWeight = 1.0f;
    return cfg;
}

SearchResult makeSearchResult() {
    SearchResult r;
    r.document.sha256Hash = "test-doc-hash";
    r.document.filePath = "/test/path";
    r.score = 0.0;
    return r;
}

} // namespace

TEST_CASE("componentSourceToString returns 'compressed_ann' for CompressedANN source",
          "[search][fusion][compressed_ann][contract]") {
    CHECK(std::strcmp(componentSourceToString(ComponentResult::Source::CompressedANN),
                      "compressed_ann") == 0);
}

TEST_CASE("componentSourceWeight returns config.compressedAnnWeight for CompressedANN",
          "[search][fusion][compressed_ann][contract]") {
    SearchEngineConfig cfg = makeConfig();
    cfg.compressedAnnWeight = 0.25f;

    CHECK(componentSourceWeight(cfg, ComponentResult::Source::CompressedANN) ==
          Catch::Approx(cfg.compressedAnnWeight).margin(1e-6f));
}

TEST_CASE("componentSourceWeight uses default compressedAnnWeight when not explicitly set",
          "[search][fusion][compressed_ann][contract]") {
    SearchEngineConfig cfg;
    // Default compressedAnnWeight is 0.10f per search_engine.h

    CHECK(componentSourceWeight(cfg, ComponentResult::Source::CompressedANN) ==
          Catch::Approx(0.10f).margin(1e-6f));
}

TEST_CASE("isVectorComponent returns true for CompressedANN",
          "[search][fusion][compressed_ann][contract]") {
    CHECK(isVectorComponent(ComponentResult::Source::CompressedANN) == true);
}

TEST_CASE("isVectorComponent returns true for Vector (baseline verification)",
          "[search][fusion][compressed_ann][contract]") {
    CHECK(isVectorComponent(ComponentResult::Source::Vector) == true);
}

TEST_CASE("isVectorComponent returns true for EntityVector (vector-family member)",
          "[search][fusion][compressed_ann][contract]") {
    CHECK(isVectorComponent(ComponentResult::Source::EntityVector) == true);
}

TEST_CASE("isVectorComponent returns false for text components",
          "[search][fusion][compressed_ann][contract]") {
    CHECK(isVectorComponent(ComponentResult::Source::Text) == false);
    CHECK(isVectorComponent(ComponentResult::Source::GraphText) == false);
}

TEST_CASE("accumulateComponentScore routes CompressedANN into vectorScore",
          "[search][fusion][compressed_ann][contract]") {
    SearchResult r = makeSearchResult();
    REQUIRE(r.vectorScore.has_value() == false);

    accumulateComponentScore(r, ComponentResult::Source::CompressedANN, 0.75);

    CHECK(r.vectorScore.has_value() == true);
    CHECK(r.vectorScore.value() == Catch::Approx(0.75).margin(1e-9));
}

TEST_CASE("accumulateComponentScore accumulates multiple vector-family contributions",
          "[search][fusion][compressed_ann][contract]") {
    SearchResult r = makeSearchResult();

    accumulateComponentScore(r, ComponentResult::Source::Vector, 0.60);
    accumulateComponentScore(r, ComponentResult::Source::CompressedANN, 0.40);

    // Both should accumulate into vectorScore
    CHECK(r.vectorScore.has_value() == true);
    CHECK(r.vectorScore.value() == Catch::Approx(1.0).margin(1e-9));
}

TEST_CASE("accumulateComponentScore does NOT route CompressedANN to keywordScore",
          "[search][fusion][compressed_ann][contract]") {
    SearchResult r = makeSearchResult();

    accumulateComponentScore(r, ComponentResult::Source::CompressedANN, 0.50);

    CHECK(r.keywordScore.has_value() == false);
    CHECK(r.vectorScore.has_value() == true);
}

TEST_CASE("componentSourceScoreInResult reads CompressedANN from vectorScore",
          "[search][fusion][compressed_ann][contract]") {
    SearchResult r = makeSearchResult();
    r.vectorScore = 0.85;

    double score = componentSourceScoreInResult(r, ComponentResult::Source::CompressedANN);

    CHECK(score == Catch::Approx(0.85).margin(1e-9));
}

TEST_CASE("componentSourceScoreInResult returns 0 for unset vectorScore",
          "[search][fusion][compressed_ann][contract]") {
    SearchResult r = makeSearchResult();
    // vectorScore is not set

    double score = componentSourceScoreInResult(r, ComponentResult::Source::CompressedANN);

    CHECK(score == Catch::Approx(0.0).margin(1e-9));
}

TEST_CASE("componentSourceScoreInResult reads Vector from same vectorScore field",
          "[search][fusion][compressed_ann][contract]") {
    // CompressedANN and Vector share the same score field (vectorScore)
    SearchResult r = makeSearchResult();
    r.vectorScore = 0.90;

    double compressedAnnScore =
        componentSourceScoreInResult(r, ComponentResult::Source::CompressedANN);
    double vectorScore = componentSourceScoreInResult(r, ComponentResult::Source::Vector);

    CHECK(compressedAnnScore == Catch::Approx(vectorScore).margin(1e-9));
    CHECK(compressedAnnScore == Catch::Approx(0.90).margin(1e-9));
}

TEST_CASE("accumulateComponentScore and componentSourceScoreInResult are inverse operations",
          "[search][fusion][compressed_ann][contract]") {
    SearchResult r1 = makeSearchResult();
    SearchResult r2 = makeSearchResult();

    constexpr double contribution = 0.42;

    accumulateComponentScore(r1, ComponentResult::Source::CompressedANN, contribution);
    double readBack = componentSourceScoreInResult(r1, ComponentResult::Source::CompressedANN);

    CHECK(readBack == Catch::Approx(contribution).margin(1e-9));
}
