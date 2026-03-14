// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/search/graph_expansion.h>
#include <yams/search/query_expansion.h>
#include <yams/search/query_text_utils.h>

#include <algorithm>
#include <unordered_map>

using Catch::Approx;

namespace yams::search {

TEST_CASE("query_text_utils normalizes graph and entity surfaces",
          "[search][helpers][query_text][catch2]") {
    CHECK(normalizeGraphSurface("CD3 / plasma-membrane") == "cd3 plasma membrane");
    CHECK(normalizeEntityTextForKey("  Tumor   Necrosis Factor  ") == "tumor necrosis factor");
    CHECK(trimAndCollapseWhitespace("  a\n\tb   c ") == "a b c");
}

TEST_CASE("query_text_utils tokenizes lowercase and preserves query token offsets",
          "[search][helpers][query_text][catch2]") {
    auto lowered = tokenizeLower("Foo\\Bar Baz-7");
    REQUIRE(lowered.size() == 4);
    CHECK(lowered[0] == "foo");
    CHECK(lowered[1] == "bar");
    CHECK(lowered[2] == "baz");
    CHECK(lowered[3] == "7");

    auto tokens = tokenizeQueryTokens("TNF-alpha inhibits IL6");
    REQUIRE(tokens.size() == 4);
    CHECK(tokens[0].original == "TNF");
    CHECK(tokens[0].normalized == "tnf");
    CHECK(tokens[3].original == "IL6");
    CHECK(tokens[3].index == 3);
}

TEST_CASE("query_expansion generates anchored subphrases from salient terms",
          "[search][helpers][query_expansion][catch2]") {
    std::unordered_map<std::string, float> idf = {
        {"tet", 2.0f}, {"protein", 0.2f}, {"loss", 0.4f}, {"myeloid", 1.8f}, {"cancers", 1.6f}};

    auto phrases = generateAnchoredSubPhrases("TET protein loss drives myeloid cancers", 4, &idf);
    REQUIRE_FALSE(phrases.empty());
    CHECK(phrases.size() <= 4);
    CHECK(std::any_of(phrases.begin(), phrases.end(), [](const auto& phrase) {
        return phrase.find("TET") != std::string::npos ||
               phrase.find("myeloid cancers") != std::string::npos;
    }));
}

TEST_CASE("query_expansion creates fallback concepts without extractor output",
          "[search][helpers][query_expansion][catch2]") {
    std::unordered_map<std::string, float> idf = {
        {"cd3", 2.5f}, {"plasma", 0.8f}, {"membrane", 0.7f}, {"activation", 1.4f}};

    auto concepts = generateFallbackQueryConcepts("CD3 plasma membrane activation", idf, 4);
    REQUIRE_FALSE(concepts.empty());
    CHECK(concepts.size() <= 4);
    CHECK(concepts.front().confidence == Approx(0.62f).margin(0.20f));
    CHECK_FALSE(concepts.front().type.empty());
}

TEST_CASE("query_expansion aggressive fallback emits bounded weighted clauses",
          "[search][helpers][query_expansion][catch2]") {
    std::unordered_map<std::string, float> idf = {
        {"erg", 2.2f}, {"b", 0.0f}, {"wave", 0.6f}, {"bipolar", 1.8f}, {"cells", 0.9f}};

    auto clauses = generateAggressiveFtsFallbackClauses("ERG b wave bipolar cells", 6, idf);
    REQUIRE_FALSE(clauses.empty());
    CHECK(clauses.size() <= 6);
    CHECK(std::any_of(clauses.begin(), clauses.end(), [](const auto& clause) {
        return clause.query.find("ERG") != std::string::npos ||
               clause.query.find("erg") != std::string::npos;
    }));
    CHECK(std::all_of(clauses.begin(), clauses.end(), [](const auto& clause) {
        return clause.penalty >= 0.1f && clause.penalty <= 1.0f;
    }));
}

TEST_CASE("graph_expansion tokenizes biomedical query terms and weights node types",
          "[search][helpers][graph_expansion][catch2]") {
    auto terms = tokenizeKgQuery("p16INK4A accumulation in plasma-membrane cells");
    REQUIRE_FALSE(terms.empty());
    CHECK(std::find(terms.begin(), terms.end(), "p16ink4a") != terms.end());
    CHECK(std::find(terms.begin(), terms.end(), "plasma membrane") != terms.end());

    CHECK(graphNodeExpansionWeight(std::make_optional<std::string>("protein"), "CD3") ==
          Approx(1.0f));
    CHECK(graphNodeExpansionWeight(std::make_optional<std::string>("date"), "February 2008") ==
          Approx(0.0f));
    CHECK(graphNodeExpansionWeight(std::make_optional<std::string>("location"), "plasma membrane") <
          0.5f);
}

} // namespace yams::search
