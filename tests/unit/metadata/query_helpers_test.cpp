#include <gtest/gtest.h>
#include <yams/metadata/query_helpers.h>

using namespace yams::metadata;

TEST(QueryHelpersTest, ExactPatternBuildsExactPath) {
    auto opts = buildQueryOptionsForSqlLikePattern("/notes/todo.md");
    ASSERT_TRUE(opts.exactPath.has_value());
    EXPECT_EQ(*opts.exactPath, "/notes/todo.md");
    EXPECT_FALSE(opts.likePattern.has_value());
}

TEST(QueryHelpersTest, DirectoryPatternSetsPrefixFlags) {
    auto opts = buildQueryOptionsForSqlLikePattern("/notes/%");
    ASSERT_TRUE(opts.pathPrefix.has_value());
    EXPECT_EQ(*opts.pathPrefix, "/notes");
    EXPECT_TRUE(opts.prefixIsDirectory);
    EXPECT_TRUE(opts.includeSubdirectories);
    EXPECT_FALSE(opts.likePattern.has_value());
}

TEST(QueryHelpersTest, ContainsPatternTargetsFts) {
    auto opts = buildQueryOptionsForSqlLikePattern("%/todo.md");
    ASSERT_TRUE(opts.containsFragment.has_value());
    EXPECT_EQ(*opts.containsFragment, "todo.md");
    EXPECT_TRUE(opts.containsUsesFts);
}

TEST(QueryHelpersTest, ExtensionPatternSetsExtensionFilter) {
    auto opts = buildQueryOptionsForSqlLikePattern("%.md");
    ASSERT_TRUE(opts.extension.has_value());
    EXPECT_EQ(*opts.extension, ".md");
}

TEST(QueryHelpersTest, FallbackKeepsLikePattern) {
    auto opts = buildQueryOptionsForSqlLikePattern("%notes%2025%");
    ASSERT_TRUE(opts.likePattern.has_value());
    EXPECT_EQ(*opts.likePattern, "%notes%2025%");
}
