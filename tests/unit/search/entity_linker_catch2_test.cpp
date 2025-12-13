// Catch2 tests for Entity Linker
// Migrated from GTest: entity_linker_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <string>
#include <vector>
#include <yams/search/entity_linker.h>

using namespace yams::search;

namespace {
bool has_node(const std::vector<LinkedEntity>& ents, const std::string& key) {
    return std::any_of(ents.begin(), ents.end(), [&](const LinkedEntity& e) {
        return e.node_key.has_value() && e.node_key.value() == key;
    });
}

const LinkedEntity* find_by_node(const std::vector<LinkedEntity>& ents, const std::string& key) {
    auto it = std::find_if(ents.begin(), ents.end(), [&](const LinkedEntity& e) {
        return e.node_key.has_value() && e.node_key.value() == key;
    });
    return it == ents.end() ? nullptr : &(*it);
}
} // namespace

TEST_CASE("SimpleHeuristicEntityLinker - AliasLookupBasic", "[search][entity][catch2]") {
    EntityLinkerConfig cfg;
    cfg.min_confidence = 0.50f;
    cfg.max_ngram = 3;
    cfg.case_insensitive = true;
    cfg.enable_alias_lookup = true;
    cfg.enable_plural_normalization = true;

    SimpleHeuristicEntityLinker linker(cfg);

    // Add alias with high prior
    REQUIRE(linker.addAlias(AliasEntry{"NYC", "geo:Q60", 0.90f}));

    const std::string text = "I love NYC in summer.";
    auto res = linker.linkText(text);
    REQUIRE(res.has_value());

    const auto& ents = res.value();
    REQUIRE_FALSE(ents.empty());
    CHECK(has_node(ents, "geo:Q60"));

    const auto* e = find_by_node(ents, "geo:Q60");
    REQUIRE(e != nullptr);

    CHECK(e->mention.text == "nyc");
    REQUIRE(e->matched_alias.has_value());
    CHECK(e->matched_alias.value() == "nyc");
    CHECK(e->confidence >= cfg.min_confidence);
}

TEST_CASE("SimpleHeuristicEntityLinker - PluralSingularNormalization", "[search][entity][catch2]") {
    EntityLinkerConfig cfg;
    cfg.min_confidence = 0.50f;
    cfg.max_ngram = 2;
    cfg.case_insensitive = true;
    cfg.enable_alias_lookup = true;
    cfg.enable_plural_normalization = true;

    SimpleHeuristicEntityLinker linker(cfg);

    // "cat" should match "cats" via singularization heuristic
    REQUIRE(linker.addAlias(AliasEntry{"cat", "animal:cat", 0.80f}));

    const std::string text = "I have two cats at home.";
    auto res = linker.linkText(text);
    REQUIRE(res.has_value());

    const auto& ents = res.value();
    REQUIRE_FALSE(ents.empty());
    CHECK(has_node(ents, "animal:cat"));

    const auto* e = find_by_node(ents, "animal:cat");
    REQUIRE(e != nullptr);

    CHECK(e->mention.text == "cats");
    REQUIRE(e->matched_alias.has_value());
    CHECK(e->matched_alias.value() == "cat");
    CHECK(e->confidence >= 0.80f);
}

TEST_CASE("SimpleHeuristicEntityLinker - NgramLongestFirst", "[search][entity][catch2]") {
    EntityLinkerConfig cfg;
    cfg.min_confidence = 0.50f;
    cfg.max_ngram = 3;
    cfg.case_insensitive = true;
    cfg.enable_alias_lookup = true;
    cfg.enable_plural_normalization = true;

    SimpleHeuristicEntityLinker linker(cfg);

    // Alias for the 2-gram phrase and also a competing 1-gram
    REQUIRE(linker.addAlias(AliasEntry{"new york", "geo:Q60", 0.90f}));
    REQUIRE(linker.addAlias(AliasEntry{"york", "geo:YORK", 0.95f}));

    const std::string text = "I love New York pizza";
    auto res = linker.linkText(text);
    REQUIRE(res.has_value());

    const auto& ents = res.value();
    // Longest-first matching should consume "new york" as a single entity
    CHECK(has_node(ents, "geo:Q60"));
    CHECK_FALSE(has_node(ents, "geo:YORK"));

    const auto* e = find_by_node(ents, "geo:Q60");
    REQUIRE(e != nullptr);
    CHECK(e->mention.text == "new york");
}

TEST_CASE("SimpleHeuristicEntityLinker - MinConfidenceThresholdFiltersLowPrior",
          "[search][entity][catch2]") {
    EntityLinkerConfig cfg;
    cfg.min_confidence = 0.60f;
    cfg.max_ngram = 1;
    cfg.case_insensitive = true;
    cfg.enable_alias_lookup = true;
    cfg.enable_plural_normalization = false;

    SimpleHeuristicEntityLinker linker(cfg);

    // Prior is below threshold
    REQUIRE(linker.addAlias(AliasEntry{"abc", "node:low", 0.58f}));

    const std::string text = "abc";
    auto res = linker.linkText(text);
    REQUIRE(res.has_value());

    const auto& ents = res.value();
    // Should filter out due to min_confidence
    CHECK(ents.empty());
}

TEST_CASE("SimpleHeuristicEntityLinker - PicksHighestPriorWhenMultipleAliases",
          "[search][entity][catch2]") {
    EntityLinkerConfig cfg;
    cfg.min_confidence = 0.50f;
    cfg.max_ngram = 1;
    cfg.case_insensitive = true;
    cfg.enable_alias_lookup = true;

    SimpleHeuristicEntityLinker linker(cfg);

    // Same alias mapping to two different nodes, different priors
    REQUIRE(linker.addAlias(AliasEntry{"NYC", "node:weaker", 0.60f}));
    REQUIRE(linker.addAlias(AliasEntry{"nyc", "node:stronger", 0.80f}));

    const std::string text = "NYC is great";
    auto res = linker.linkText(text);
    REQUIRE(res.has_value());

    const auto& ents = res.value();
    REQUIRE_FALSE(ents.empty());

    // Expect the higher-prior target
    CHECK(has_node(ents, "node:stronger"));
    CHECK_FALSE(has_node(ents, "node:weaker"));
}
