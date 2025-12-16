// CLI Recommendation Utility tests
// Catch2 migration from GTest (yams-3s4 / yams-cli)
//
// Tests for recommendation builder and output formatting.

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/recommendation_util.h>

#include <sstream>
#include <string>

using namespace yams::cli;

TEST_CASE("RecommendationUtil - deduplicates by code", "[cli][recommendation][catch2]") {
    RecommendationBuilder b;
    b.warning("CODE_A", "First");
    b.warning("CODE_A", "First"); // duplicate
    b.info("", "Loose message 1");
    b.info("", "Loose message 1"); // duplicate by message
    CHECK(b.size() == 2);
}

TEST_CASE("RecommendationUtil - orders by severity then code", "[cli][recommendation][catch2]") {
    RecommendationBuilder b;
    b.info("Z_INFO", "Info Z");
    b.critical("A_CRIT", "Critical A");
    b.warning("B_WARN", "Warn B");

    auto sorted = b.sorted();
    REQUIRE(sorted.size() == 3);
    CHECK(sorted[0].severity == RecommendationSeverity::Critical);
    CHECK(sorted[1].severity == RecommendationSeverity::Warning);
    CHECK(sorted[2].severity == RecommendationSeverity::Info);
}

TEST_CASE("RecommendationUtil - JSON serialization", "[cli][recommendation][catch2]") {
    RecommendationBuilder b;
    b.warning("W", "Warn", "detail text");
    b.info("I", "Info");

    std::string json = recommendationsToJson(b);

    // Basic shape checks
    CHECK(json.find("\"code\":\"W\"") != std::string::npos);
    CHECK(json.find("\"severity\":\"warning\"") != std::string::npos);
    CHECK(json.find("\"details\":\"detail text\"") != std::string::npos);

    // Order: critical < warning < info, we only have warning then info
    auto wpos = json.find("\"code\":\"W\"");
    auto ipos = json.find("\"code\":\"I\"");
    REQUIRE(wpos != std::string::npos);
    REQUIRE(ipos != std::string::npos);
    CHECK(wpos < ipos);
}

TEST_CASE("RecommendationUtil - text output formatting", "[cli][recommendation][catch2]") {
    RecommendationBuilder b;
    b.warning("W", "Warn msg", "context");
    b.info("I", "Info msg");

    std::ostringstream oss;
    printRecommendationsText(b, oss);
    auto out = oss.str();

    CHECK(out.find("Recommendations:") != std::string::npos);
    CHECK(out.find("Warn msg") != std::string::npos);
    CHECK(out.find("context") != std::string::npos);
    CHECK(out.find("Info msg") != std::string::npos);
}
