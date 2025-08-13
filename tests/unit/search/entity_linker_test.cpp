#include <gtest/gtest.h>
#include <yams/search/entity_linker.h>

#include <algorithm>
#include <string>
#include <vector>

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

TEST(SimpleHeuristicEntityLinkerTest, AliasLookupBasic) {
    EntityLinkerConfig cfg;
    cfg.min_confidence = 0.50f;
    cfg.max_ngram = 3;
    cfg.case_insensitive = true;
    cfg.enable_alias_lookup = true;
    cfg.enable_plural_normalization = true;

    SimpleHeuristicEntityLinker linker(cfg);

    // Add alias with high prior
    ASSERT_TRUE(linker.addAlias(AliasEntry{"NYC", "geo:Q60", 0.90f}));

    const std::string text = "I love NYC in summer.";
    auto res = linker.linkText(text);
    ASSERT_TRUE(res.has_value());

    const auto& ents = res.value();
    ASSERT_FALSE(ents.empty());
    ASSERT_TRUE(has_node(ents, "geo:Q60"));

    const auto* e = find_by_node(ents, "geo:Q60");
    ASSERT_NE(e, nullptr);

    // Normalization yields lowercase mention.token reconstruction ("nyc")
    EXPECT_EQ(e->mention.text, "nyc");
    ASSERT_TRUE(e->matched_alias.has_value());
    EXPECT_EQ(e->matched_alias.value(), "nyc");

    // Confidence should meet threshold and reflect prior (with small heuristic bumps)
    EXPECT_GE(e->confidence, cfg.min_confidence);
    EXPECT_GT(e->confidence, 0.89f); // defensively ensure near prior
}

TEST(SimpleHeuristicEntityLinkerTest, PluralSingularNormalization) {
    EntityLinkerConfig cfg;
    cfg.min_confidence = 0.50f;
    cfg.max_ngram = 2;
    cfg.case_insensitive = true;
    cfg.enable_alias_lookup = true;
    cfg.enable_plural_normalization = true;

    SimpleHeuristicEntityLinker linker(cfg);

    // "cat" should match "cats" via singularization heuristic
    ASSERT_TRUE(linker.addAlias(AliasEntry{"cat", "animal:cat", 0.80f}));

    const std::string text = "I have two cats at home.";
    auto res = linker.linkText(text);
    ASSERT_TRUE(res.has_value());

    const auto& ents = res.value();
    ASSERT_FALSE(ents.empty());
    ASSERT_TRUE(has_node(ents, "animal:cat"));

    const auto* e = find_by_node(ents, "animal:cat");
    ASSERT_NE(e, nullptr);

    // Mention text uses tokenized (lowercased) surface ("cats")
    EXPECT_EQ(e->mention.text, "cats");
    // Matched alias is the normalized singular form "cat"
    ASSERT_TRUE(e->matched_alias.has_value());
    EXPECT_EQ(e->matched_alias.value(), "cat");
    EXPECT_GE(e->confidence, 0.80f);
}

TEST(SimpleHeuristicEntityLinkerTest, NgramLongestFirst) {
    EntityLinkerConfig cfg;
    cfg.min_confidence = 0.50f;
    cfg.max_ngram = 3; // allow 2-gram phrase "new york"
    cfg.case_insensitive = true;
    cfg.enable_alias_lookup = true;
    cfg.enable_plural_normalization = true;

    SimpleHeuristicEntityLinker linker(cfg);

    // Alias for the 2-gram phrase and also a competing 1-gram
    ASSERT_TRUE(linker.addAlias(AliasEntry{"new york", "geo:Q60", 0.90f}));
    ASSERT_TRUE(linker.addAlias(AliasEntry{"york", "geo:YORK", 0.95f}));

    const std::string text = "I love New York pizza";
    auto res = linker.linkText(text);
    ASSERT_TRUE(res.has_value());

    const auto& ents = res.value();
    // Longest-first matching should consume "new york" as a single entity,
    // preventing a separate "york" 1-gram match on overlapping tokens.
    ASSERT_TRUE(has_node(ents, "geo:Q60"));
    EXPECT_FALSE(has_node(ents, "geo:YORK"));

    const auto* e = find_by_node(ents, "geo:Q60");
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->mention.text, "new york");
    ASSERT_TRUE(e->matched_alias.has_value());
    EXPECT_EQ(e->matched_alias.value(), "new york");
}

TEST(SimpleHeuristicEntityLinkerTest, MinConfidenceThresholdFiltersLowPrior) {
    EntityLinkerConfig cfg;
    cfg.min_confidence = 0.60f; // Require 0.60
    cfg.max_ngram = 1;
    cfg.case_insensitive = true;
    cfg.enable_alias_lookup = true;
    cfg.enable_plural_normalization = false;

    SimpleHeuristicEntityLinker linker(cfg);

    // Prior is below threshold; small heuristic on short token won't push it over 0.60
    ASSERT_TRUE(linker.addAlias(AliasEntry{"abc", "node:low", 0.58f}));

    const std::string text = "abc";
    auto res = linker.linkText(text);
    ASSERT_TRUE(res.has_value());

    const auto& ents = res.value();
    // Should filter out due to min_confidence
    EXPECT_TRUE(ents.empty());
}

TEST(SimpleHeuristicEntityLinkerTest, PicksHighestPriorWhenMultipleAliasesMapToDifferentNodes) {
    EntityLinkerConfig cfg;
    cfg.min_confidence = 0.50f;
    cfg.max_ngram = 1;
    cfg.case_insensitive = true;
    cfg.enable_alias_lookup = true;

    SimpleHeuristicEntityLinker linker(cfg);

    // Same alias mapping to two different nodes, different priors
    ASSERT_TRUE(linker.addAlias(AliasEntry{"NYC", "node:weaker", 0.60f}));
    ASSERT_TRUE(linker.addAlias(AliasEntry{"nyc", "node:stronger", 0.80f}));

    const std::string text = "NYC is great";
    auto res = linker.linkText(text);
    ASSERT_TRUE(res.has_value());

    const auto& ents = res.value();
    ASSERT_FALSE(ents.empty());

    // Expect the higher-prior target
    EXPECT_TRUE(has_node(ents, "node:stronger"));
    EXPECT_FALSE(has_node(ents, "node:weaker"));
}