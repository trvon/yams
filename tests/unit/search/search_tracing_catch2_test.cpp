#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/search/search_tracing.h>

using yams::search::ComponentResult;

namespace {

ComponentResult makeComponent(std::string path, float score, ComponentResult::Source source,
                              size_t rank) {
    ComponentResult c;
    c.filePath = std::move(path);
    c.documentHash = c.filePath;
    c.score = score;
    c.source = source;
    c.rank = rank;
    return c;
}

} // namespace

TEST_CASE("buildComponentHitSummaryJson preserves ranked top hit details",
          "[search][tracing][catch2]") {
    std::vector<ComponentResult> componentResults;
    componentResults.push_back(
        makeComponent("/tmp/31715818.txt", 0.91f, ComponentResult::Source::Vector, 150));
    componentResults.push_back(
        makeComponent("/tmp/31715818.txt", 0.89f, ComponentResult::Source::Vector, 151));
    componentResults.push_back(
        makeComponent("/tmp/11111111.txt", 0.95f, ComponentResult::Source::Vector, 5));
    componentResults.push_back(
        makeComponent("/tmp/22222222.txt", 0.72f, ComponentResult::Source::Text, 12));

    const auto summary = yams::search::buildComponentHitSummaryJson(componentResults, 3);

    REQUIRE(summary.contains("vector"));
    const auto& vectorSummary = summary.at("vector");
    REQUIRE(vectorSummary.contains("unique_top_doc_ids"));
    REQUIRE(vectorSummary.contains("top_hits"));

    const auto& vectorIds = vectorSummary.at("unique_top_doc_ids");
    REQUIRE(vectorIds.is_array());
    REQUIRE(vectorIds.size() == 2);
    CHECK(vectorIds.at(0).get<std::string>() == "11111111");
    CHECK(vectorIds.at(1).get<std::string>() == "31715818");

    const auto& topHits = vectorSummary.at("top_hits");
    REQUIRE(topHits.is_array());
    REQUIRE(topHits.size() == 2);
    CHECK(topHits.at(0).at("doc_id").get<std::string>() == "11111111");
    CHECK(topHits.at(0).at("rank").get<size_t>() == 6);
    CHECK(topHits.at(1).at("doc_id").get<std::string>() == "31715818");
    CHECK(topHits.at(1).at("rank").get<size_t>() == 151);
    CHECK(topHits.at(1).at("score").get<double>() == Catch::Approx(0.91));

    REQUIRE(summary.contains("text"));
    const auto& textHits = summary.at("text").at("top_hits");
    REQUIRE(textHits.is_array());
    REQUIRE(textHits.size() == 1);
    CHECK(textHits.at(0).at("doc_id").get<std::string>() == "22222222");
    CHECK(textHits.at(0).at("rank").get<size_t>() == 13);
}
