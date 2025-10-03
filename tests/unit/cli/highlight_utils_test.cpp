#include <gtest/gtest.h>

#include <yams/cli/tui/highlight_utils.hpp>

using yams::cli::tui::computeHighlightSegments;
using yams::cli::tui::HighlightSegment;

TEST(HighlightUtilsTest, EmptyQueryProducesSingleSegment) {
    auto segments = computeHighlightSegments("example", "");
    ASSERT_EQ(segments.size(), 1u);
    EXPECT_EQ(segments[0].text, "example");
    EXPECT_FALSE(segments[0].highlighted);
}

TEST(HighlightUtilsTest, CaseInsensitiveMatchesAreHighlighted) {
    auto segments = computeHighlightSegments("Hello World", "world");
    ASSERT_EQ(segments.size(), 2u);
    EXPECT_EQ(segments[0].text, "Hello ");
    EXPECT_FALSE(segments[0].highlighted);
    EXPECT_EQ(segments[1].text, "World");
    EXPECT_TRUE(segments[1].highlighted);
}

TEST(HighlightUtilsTest, MultipleMatchesProduceAlternatingSegments) {
    auto segments = computeHighlightSegments("banana", "an");
    ASSERT_EQ(segments.size(), 4u);
    EXPECT_EQ(segments[0].text, "b");
    EXPECT_FALSE(segments[0].highlighted);
    EXPECT_EQ(segments[1].text, "an");
    EXPECT_TRUE(segments[1].highlighted);
    EXPECT_EQ(segments[2].text, "an");
    EXPECT_TRUE(segments[2].highlighted);
    EXPECT_EQ(segments[3].text, "a");
    EXPECT_FALSE(segments[3].highlighted);
}
