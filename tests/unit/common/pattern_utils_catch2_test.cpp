// Tests for common glob/wildcard utilities.

#include <catch2/catch_test_macros.hpp>

#include <string>

#include <yams/common/pattern_utils.h>

using yams::common::brace_expand;
using yams::common::glob_match_path;
using yams::common::match_segment;
using yams::common::normalize_path;
using yams::common::wildcard_match;
using yams::common::has_wildcards;
using yams::common::glob_to_sql_like;
using yams::common::split_patterns;

TEST_CASE("common::wildcard_match basics", "[common][pattern][wildcard][catch2]") {
    CHECK(wildcard_match("", ""));
    CHECK(wildcard_match("a", "a"));
    CHECK_FALSE(wildcard_match("a", "b"));

    CHECK(wildcard_match("abc", "a?c"));
    CHECK_FALSE(wildcard_match("abc", "a?d"));

    CHECK(wildcard_match("abc", "*"));
    CHECK(wildcard_match("abc", "a*"));
    CHECK(wildcard_match("abc", "*c"));
    CHECK(wildcard_match("abc", "a*c"));
    CHECK_FALSE(wildcard_match("abc", "a*d"));

    // Backtracking behavior: star matches varying lengths.
    CHECK(wildcard_match("abbbc", "a*bc"));
    CHECK(wildcard_match("abbbc", "a*b*c"));
}

TEST_CASE("common::normalize_path normalizes separators and slashes",
          "[common][pattern][path][catch2]") {
    CHECK(normalize_path("a/b/c") == "a/b/c");
    CHECK(normalize_path("a\\b\\c") == "a/b/c");
    CHECK(normalize_path("a//b///c/") == "a/b/c");
    CHECK(normalize_path("/") == "/");
}

TEST_CASE("common::brace_expand expands simple and nested sets",
          "[common][pattern][brace][catch2]") {
    {
        const auto out = brace_expand("*.{cpp,hpp}");
        REQUIRE(out.size() == 2);
        CHECK(out[0] == "*.cpp");
        CHECK(out[1] == "*.hpp");
    }
    {
        const auto out = brace_expand("{a,{b,c}}.txt");
        REQUIRE(out.size() == 3);
        CHECK(out[0] == "a.txt");
        CHECK(out[1] == "b.txt");
        CHECK(out[2] == "c.txt");
    }
    {
        const auto out = brace_expand("no_braces");
        REQUIRE(out.size() == 1);
        CHECK(out[0] == "no_braces");
    }
}

TEST_CASE("common::match_segment supports ?, *, and character classes",
          "[common][pattern][segment][catch2]") {
    CHECK(match_segment("main.cpp", "*.cpp"));
    CHECK(match_segment("a", "?"));
    CHECK_FALSE(match_segment("", "?"));

    CHECK(match_segment("a1", "a[0-9]"));
    CHECK_FALSE(match_segment("aa", "a[0-9]"));

    CHECK(match_segment("b", "[!a]"));
    CHECK_FALSE(match_segment("a", "[!a]"));

    CHECK(match_segment("x", "[xyz]"));
    CHECK_FALSE(match_segment("q", "[xyz]"));
}

TEST_CASE("common::glob_match_path supports **, anchors, and brace alternatives",
          "[common][pattern][glob][catch2]") {
    // ** spans segments
    CHECK(glob_match_path("src/main.cpp", "src/**/main.cpp"));
    CHECK(glob_match_path("src/a/b/main.cpp", "src/**/main.cpp"));

    // implicit leading ** when unanchored
    CHECK(glob_match_path("a/b/c.cpp", "c.cpp"));
    CHECK(glob_match_path("a/b/c.cpp", "*.cpp"));

    // anchored patterns match from the beginning
    CHECK_FALSE(glob_match_path("a/src/x.cpp", "^src/*.cpp"));
    CHECK(glob_match_path("src/x.cpp", "^src/*.cpp"));

    // brace expansion
    CHECK(glob_match_path("include/x.hpp", "**/*.{cpp,hpp}"));
    CHECK(glob_match_path("src/x.cpp", "**/*.{cpp,hpp}"));
    CHECK_FALSE(glob_match_path("src/x.c", "**/*.{cpp,hpp}"));

    // path normalization during matching
    CHECK(glob_match_path("src\\a\\b\\x.cpp", "src/**/x.cpp"));

    // unanchored patterns may match in the middle (implicit leading **)
    CHECK(glob_match_path("a/b/x/b/c", "b/c"));
    CHECK(glob_match_path("a/b/x/b/c", "b/*"));
    CHECK_FALSE(glob_match_path("a/b/x/b/c", "^b/c"));

    // ** at end can match zero or more segments
    CHECK(glob_match_path("a/b", "a/**"));
    CHECK(glob_match_path("a/b/c", "a/**"));
    CHECK(glob_match_path("a", "a/**"));

    // consecutive ** behaves the same
    CHECK(glob_match_path("a/b/c", "a/**/**/c"));

    // character classes within a segment
    CHECK(glob_match_path("src/a1/file.txt", "src/a[0-9]/*.txt"));
    CHECK_FALSE(glob_match_path("src/aa/file.txt", "src/a[0-9]/*.txt"));

    // patterns with no path segments
    CHECK_FALSE(glob_match_path("a/b/c.txt", ""));
    CHECK_FALSE(glob_match_path("a/b/c.txt", "^"));
}

TEST_CASE("common::split_patterns trims and skips empties",
          "[common][pattern][split][catch2]") {
    {
        const auto out = split_patterns("");
        CHECK(out.empty());
    }

    {
        const auto out = split_patterns("  a , , b,,  c  ");
        REQUIRE(out.size() == 3);
        CHECK(out[0] == "a");
        CHECK(out[1] == "b");
        CHECK(out[2] == "c");
    }
}

TEST_CASE("common::glob_to_sql_like converts glob metacharacters",
          "[common][pattern][sql][catch2]") {
    CHECK(glob_to_sql_like("*") == "%");
    CHECK(glob_to_sql_like("?") == "_");
    CHECK(glob_to_sql_like("a*b?c") == "a%b_c");
    CHECK(glob_to_sql_like("no_wildcards") == "no_wildcards");
}

TEST_CASE("common::has_wildcards reports wildcard metacharacters",
          "[common][pattern][wildcards][catch2]") {
    CHECK_FALSE(has_wildcards("abc"));
    CHECK(has_wildcards("a*c"));
    CHECK(has_wildcards("a?c"));
}
