#include <catch2/catch_test_macros.hpp>

#include "src/search/concept_boost_internal.h"

#include <string>
#include <vector>

using yams::search::QueryConcept;
using yams::search::SearchEngineConfig;
using yams::search::SearchResult;
using yams::search::detail::applyConceptBoost;

namespace {

SearchResult makeResult(std::string fileName, std::string snippet, float score) {
    SearchResult r;
    r.document.fileName = std::move(fileName);
    r.snippet = std::move(snippet);
    r.score = score;
    return r;
}

QueryConcept makeConcept(std::string text) {
    QueryConcept c;
    c.text = std::move(text);
    return c;
}

SearchEngineConfig boostConfig() {
    SearchEngineConfig cfg;
    cfg.conceptBoostWeight = 0.10F;
    cfg.conceptMaxBoost = 0.25F;
    cfg.conceptMaxScanResults = 200;
    return cfg;
}

} // namespace

TEST_CASE("applyConceptBoost boosts and re-sorts results matching concept terms",
          "[search][concept][catch2]") {
    std::vector<SearchResult> results;
    results.push_back(makeResult("a.txt", "nothing here", 1.0F));
    results.push_back(makeResult("b.txt", "mentions widget prominently", 1.0F));
    std::vector<QueryConcept> concepts{makeConcept("widget")};

    applyConceptBoost(results, concepts, boostConfig());

    // b matched a concept term -> boosted above the equal-scored a and re-sorted to the front.
    CHECK(results.front().document.fileName == "b.txt");
    CHECK(results.front().score > 1.0F);
    CHECK(results.back().score == 1.0F); // unmatched result unchanged
}

TEST_CASE("applyConceptBoost matches on filename as well as snippet",
          "[search][concept][catch2]") {
    std::vector<SearchResult> results{makeResult("widget_helper.cpp", "no body match", 1.0F)};
    std::vector<QueryConcept> concepts{makeConcept("widget")};

    applyConceptBoost(results, concepts, boostConfig());
    CHECK(results.front().score > 1.0F);
}

TEST_CASE("applyConceptBoost caps total boost at conceptMaxBoost",
          "[search][concept][catch2]") {
    std::vector<SearchResult> results{
        makeResult("a.txt", "widget widget gadget gizmo doohickey thing", 1.0F)};
    std::vector<QueryConcept> concepts{makeConcept("widget"), makeConcept("gadget"), makeConcept("gizmo"),
                                       makeConcept("doohickey"), makeConcept("thing")};
    auto cfg = boostConfig(); // weight 0.10 * 5 matches = 0.50 desired, capped at 0.25
    applyConceptBoost(results, concepts, cfg);
    CHECK(results.front().score <= 1.0F * (1.0F + cfg.conceptMaxBoost) + 1e-4F);
    CHECK(results.front().score > 1.0F);
}

TEST_CASE("applyConceptBoost is a no-op when disabled or inputs empty",
          "[search][concept][catch2]") {
    auto base = makeResult("b.txt", "mentions widget", 0.9F);
    std::vector<QueryConcept> concepts{makeConcept("widget")};

    SECTION("weight zero") {
        std::vector<SearchResult> results{base};
        auto cfg = boostConfig();
        cfg.conceptBoostWeight = 0.0F;
        applyConceptBoost(results, concepts, cfg);
        CHECK(results.front().score == 0.9F);
    }
    SECTION("no concepts") {
        std::vector<SearchResult> results{base};
        applyConceptBoost(results, {}, boostConfig());
        CHECK(results.front().score == 0.9F);
    }
    SECTION("empty results") {
        std::vector<SearchResult> results;
        applyConceptBoost(results, concepts, boostConfig());
        CHECK(results.empty());
    }
}
