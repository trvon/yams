#include <sstream>
#include <gtest/gtest.h>
#include <yams/cli/recommendation_util.h>

using namespace yams::cli;

TEST(RecommendationUtil, DedupByCode) {
    RecommendationBuilder b;
    b.warning("CODE_A", "First");
    b.warning("CODE_A", "First"); // duplicate
    b.info("", "Loose message 1");
    b.info("", "Loose message 1"); // duplicate by message
    ASSERT_EQ(b.size(), 2u) << "Expected deduplication by code and message";
}

TEST(RecommendationUtil, OrderingSeverityThenCode) {
    RecommendationBuilder b;
    b.info("Z_INFO", "Info Z");
    b.critical("A_CRIT", "Critical A");
    b.warning("B_WARN", "Warn B");
    auto sorted = b.sorted();
    ASSERT_EQ(sorted.size(), 3u);
    EXPECT_EQ(sorted[0].severity, RecommendationSeverity::Critical);
    EXPECT_EQ(sorted[1].severity, RecommendationSeverity::Warning);
    EXPECT_EQ(sorted[2].severity, RecommendationSeverity::Info);
}

TEST(RecommendationUtil, JsonSerialization) {
    RecommendationBuilder b;
    b.warning("W", "Warn", "detail text");
    b.info("I", "Info");
    std::string json = recommendationsToJson(b);
    // Basic shape checks
    EXPECT_NE(json.find("\"code\":\"W\""), std::string::npos);
    EXPECT_NE(json.find("\"severity\":\"warning\""), std::string::npos);
    EXPECT_NE(json.find("\"details\":\"detail text\""), std::string::npos);
    // Order: critical < warning < info, we only have warning then info
    auto wpos = json.find("\"code\":\"W\"");
    auto ipos = json.find("\"code\":\"I\"");
    ASSERT_NE(wpos, std::string::npos);
    ASSERT_NE(ipos, std::string::npos);
    EXPECT_LT(wpos, ipos);
}

TEST(RecommendationUtil, TextOutputFormatting) {
    RecommendationBuilder b;
    b.warning("W", "Warn msg", "context");
    b.info("I", "Info msg");
    std::ostringstream oss;
    printRecommendationsText(b, oss);
    auto out = oss.str();
    EXPECT_NE(out.find("Recommendations:"), std::string::npos);
    EXPECT_NE(out.find("Warn msg"), std::string::npos);
    EXPECT_NE(out.find("context"), std::string::npos);
    EXPECT_NE(out.find("Info msg"), std::string::npos);
}
