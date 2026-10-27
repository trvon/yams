/// @file grep_regex_test.cpp
/// @brief Catch2 unit tests for the GrepRegex wrapper (RE2 + bounded Boost.Regex fallback).

#include <chrono>
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <catch2/catch_test_macros.hpp>
#include <yams/app/services/grep_regex.hpp>

#if defined(__has_feature)
#if __has_feature(address_sanitizer)
#define YAMS_GREP_REGEX_TEST_ASAN 1
#endif
#endif

#if !defined(YAMS_GREP_REGEX_TEST_ASAN) && defined(__SANITIZE_ADDRESS__)
#define YAMS_GREP_REGEX_TEST_ASAN 1
#endif

using namespace yams::app::services;

// ---------------------------------------------------------------------------
// compile — success cases
// ---------------------------------------------------------------------------

TEST_CASE("GrepRegex::compile: simple literal pattern", "[grep_regex]") {
    auto re = GrepRegex::compile("hello", false);
    REQUIRE(re.has_value());
}

TEST_CASE("GrepRegex::compile: character class", "[grep_regex]") {
    auto re = GrepRegex::compile("[a-z]+", false);
    REQUIRE(re.has_value());
}

TEST_CASE("GrepRegex::compile: quantifiers", "[grep_regex]") {
    auto re = GrepRegex::compile("a{2,4}", false);
    REQUIRE(re.has_value());
}

TEST_CASE("GrepRegex::compile: alternation", "[grep_regex]") {
    auto re = GrepRegex::compile("foo|bar|baz", false);
    REQUIRE(re.has_value());
}

TEST_CASE("GrepRegex::compile: anchors", "[grep_regex]") {
    auto re = GrepRegex::compile("^start.*end$", false);
    REQUIRE(re.has_value());
}

TEST_CASE("GrepRegex::compile: case insensitive", "[grep_regex]") {
    auto re = GrepRegex::compile("Hello", true);
    REQUIRE(re.has_value());
    // Should match lowercase text.
    auto m = re->findFirst("say hello world");
    REQUIRE(m.has_value());
    REQUIRE(m->position == 4);
    REQUIRE(m->length == 5);
}

// ---------------------------------------------------------------------------
// compile — failure cases
// ---------------------------------------------------------------------------

TEST_CASE("GrepRegex::compile: invalid pattern returns nullopt", "[grep_regex]") {
    // Unmatched parenthesis — invalid for both RE2 and ECMAScript.
    auto re = GrepRegex::compile("(abc", false);
    REQUIRE_FALSE(re.has_value());
}

TEST_CASE("GrepRegex::compile: empty pattern compiles", "[grep_regex]") {
    // Empty pattern is valid regex (matches everything).
    auto re = GrepRegex::compile("", false);
    REQUIRE(re.has_value());
}

// ---------------------------------------------------------------------------
// findFirst
// ---------------------------------------------------------------------------

TEST_CASE("GrepRegex::findFirst: match at start", "[grep_regex]") {
    auto re = GrepRegex::compile("the", false);
    REQUIRE(re.has_value());
    auto m = re->findFirst("the quick brown fox");
    REQUIRE(m.has_value());
    REQUIRE(m->position == 0);
    REQUIRE(m->length == 3);
}

TEST_CASE("GrepRegex::findFirst: match in middle", "[grep_regex]") {
    auto re = GrepRegex::compile("brown", false);
    REQUIRE(re.has_value());
    auto m = re->findFirst("the quick brown fox");
    REQUIRE(m.has_value());
    REQUIRE(m->position == 10);
    REQUIRE(m->length == 5);
}

TEST_CASE("GrepRegex::findFirst: match at end", "[grep_regex]") {
    auto re = GrepRegex::compile("fox$", false);
    REQUIRE(re.has_value());
    auto m = re->findFirst("the quick brown fox");
    REQUIRE(m.has_value());
    REQUIRE(m->position == 16);
    REQUIRE(m->length == 3);
}

TEST_CASE("GrepRegex::findFirst: no match returns nullopt", "[grep_regex]") {
    auto re = GrepRegex::compile("cat", false);
    REQUIRE(re.has_value());
    auto m = re->findFirst("the quick brown fox");
    REQUIRE_FALSE(m.has_value());
}

TEST_CASE("GrepRegex::findFirst: regex metacharacters", "[grep_regex]") {
    auto re = GrepRegex::compile("\\d+", false);
    REQUIRE(re.has_value());
    auto m = re->findFirst("abc 123 def");
    REQUIRE(m.has_value());
    REQUIRE(m->position == 4);
    REQUIRE(m->length == 3);
}

TEST_CASE("GrepRegex::findFirst: alternation picks first match", "[grep_regex]") {
    auto re = GrepRegex::compile("cat|fox", false);
    REQUIRE(re.has_value());
    auto m = re->findFirst("the fox ate the cat");
    REQUIRE(m.has_value());
    REQUIRE(m->position == 4);
    REQUIRE(m->length == 3);
}

// ---------------------------------------------------------------------------
// findNext — offset tracking
// ---------------------------------------------------------------------------

TEST_CASE("GrepRegex::findNext: successive matches", "[grep_regex]") {
    auto re = GrepRegex::compile("the", false);
    REQUIRE(re.has_value());
    const std::string text = "the cat and the dog and the fox";
    //                         0         1         2
    //                         0123456789012345678901234567890

    auto m1 = re->findNext(text.data(), text.size(), 0);
    REQUIRE(m1.has_value());
    REQUIRE(m1->position == 0);

    auto m2 = re->findNext(text.data(), text.size(), m1->position + m1->length);
    REQUIRE(m2.has_value());
    REQUIRE(m2->position == 12);

    auto m3 = re->findNext(text.data(), text.size(), m2->position + m2->length);
    REQUIRE(m3.has_value());
    REQUIRE(m3->position == 24);

    auto m4 = re->findNext(text.data(), text.size(), m3->position + m3->length);
    REQUIRE_FALSE(m4.has_value());
}

TEST_CASE("GrepRegex::findNext: offset beyond length returns nullopt", "[grep_regex]") {
    auto re = GrepRegex::compile("x", false);
    REQUIRE(re.has_value());
    auto m = re->findNext("abc", 3, 10);
    REQUIRE_FALSE(m.has_value());
}

TEST_CASE("GrepRegex::findNext: offset at length returns nullopt", "[grep_regex]") {
    auto re = GrepRegex::compile("x", false);
    REQUIRE(re.has_value());
    auto m = re->findNext("abc", 3, 3);
    REQUIRE_FALSE(m.has_value());
}

// ---------------------------------------------------------------------------
// countMatches
// ---------------------------------------------------------------------------

TEST_CASE("GrepRegex::countMatches: zero matches", "[grep_regex]") {
    auto re = GrepRegex::compile("xyz", false);
    REQUIRE(re.has_value());
    const std::string text = "no match here";
    REQUIRE(re->countMatches(text.data(), text.size()) == 0);
}

TEST_CASE("GrepRegex::countMatches: single match", "[grep_regex]") {
    auto re = GrepRegex::compile("world", false);
    REQUIRE(re.has_value());
    const std::string text = "hello world";
    REQUIRE(re->countMatches(text.data(), text.size()) == 1);
}

TEST_CASE("GrepRegex::countMatches: multiple matches", "[grep_regex]") {
    auto re = GrepRegex::compile("ab", false);
    REQUIRE(re.has_value());
    const std::string text = "ab cd ab ef ab";
    REQUIRE(re->countMatches(text.data(), text.size()) == 3);
}

TEST_CASE("GrepRegex::countMatches: overlapping candidates (non-overlapping result)",
          "[grep_regex]") {
    // "aa" in "aaaa" — non-overlapping should find 2, not 3.
    auto re = GrepRegex::compile("aa", false);
    REQUIRE(re.has_value());
    const std::string text = "aaaa";
    REQUIRE(re->countMatches(text.data(), text.size()) == 2);
}

TEST_CASE("GrepRegex::countMatches: zero-width match advances", "[grep_regex]") {
    // Regex that can match zero-width (empty pattern).
    // countMatches should not infinite-loop; it advances by at least 1 byte.
    auto re = GrepRegex::compile("", false);
    REQUIRE(re.has_value());
    const std::string text = "ab";
    // Empty pattern matches at every position: 0, 1, 2 (after last char) → 3 matches.
    // But our implementation advances by max(length, 1), so positions 0, 1, 2 → 3.
    // The exact count depends on the regex engine behavior; just verify it terminates
    // and returns a finite count.
    size_t count = re->countMatches(text.data(), text.size());
    REQUIRE(count >= 1);
    REQUIRE(count <= text.size() + 1);
}

// ---------------------------------------------------------------------------
// usesRe2
// ---------------------------------------------------------------------------

TEST_CASE("GrepRegex::usesRe2: reflects backend", "[grep_regex]") {
    auto re = GrepRegex::compile("simple", false);
    REQUIRE(re.has_value());
    // When built with YAMS_HAS_RE2=1, a simple pattern should compile via RE2.
    // When built without RE2, it uses the backtracker.
    // We can't know at test-compile time, but the flag should be consistent.
#if YAMS_HAS_RE2
#if defined(YAMS_GREP_REGEX_TEST_ASAN)
    REQUIRE(re->usesRe2() == false);
    REQUIRE(re->fallbackReason() == "RE2 disabled under AddressSanitizer");
#else
    REQUIRE(re->usesRe2() == true);
    REQUIRE(re->fallbackReason().empty());
#endif
#else
    REQUIRE(re->usesRe2() == false);
#endif
}

// ---------------------------------------------------------------------------
// RE2 fallback to the backtracker for unsupported features
// ---------------------------------------------------------------------------

#if YAMS_HAS_RE2
TEST_CASE("GrepRegex: RE2 fallback for backreferences", "[grep_regex][re2_fallback]") {
    // Backreferences are not supported by RE2 but are valid ECMAScript.
    auto re = GrepRegex::compile("(abc)\\1", false);
    REQUIRE(re.has_value());
    // Should have fallen back to the backtracker.
    REQUIRE(re->usesRe2() == false);
    REQUIRE_FALSE(re->fallbackReason().empty());

    // Verify it actually works via the backtracker.
    auto m = re->findFirst("abcabc");
    REQUIRE(m.has_value());
    REQUIRE(m->position == 0);
    REQUIRE(m->length == 6);
}

TEST_CASE("GrepRegex: RE2 fallback for lookahead", "[grep_regex][re2_fallback]") {
    auto re = GrepRegex::compile("foo(?=bar)", false);
    REQUIRE(re.has_value());
    REQUIRE(re->usesRe2() == false);

    auto m = re->findFirst("foobar");
    REQUIRE(m.has_value());
    REQUIRE(m->position == 0);
    REQUIRE(m->length == 3); // Lookahead is zero-width.
}
#endif

// ---------------------------------------------------------------------------
// Case-insensitive matching
// ---------------------------------------------------------------------------

TEST_CASE("GrepRegex: case-insensitive findFirst", "[grep_regex]") {
    auto re = GrepRegex::compile("hello", true);
    REQUIRE(re.has_value());

    SECTION("exact case") {
        auto m = re->findFirst("hello");
        REQUIRE(m.has_value());
        REQUIRE(m->position == 0);
    }
    SECTION("upper case") {
        auto m = re->findFirst("say HELLO world");
        REQUIRE(m.has_value());
        REQUIRE(m->position == 4);
        REQUIRE(m->length == 5);
    }
    SECTION("mixed case") {
        auto m = re->findFirst("HeLLo");
        REQUIRE(m.has_value());
        REQUIRE(m->position == 0);
    }
}

TEST_CASE("GrepRegex: case-insensitive countMatches", "[grep_regex]") {
    auto re = GrepRegex::compile("the", true);
    REQUIRE(re.has_value());
    const std::string text = "The cat and THE dog and the fox";
    REQUIRE(re->countMatches(text.data(), text.size()) == 3);
}

// ---------------------------------------------------------------------------
// Move semantics
// ---------------------------------------------------------------------------

TEST_CASE("GrepRegex: move construction", "[grep_regex]") {
    auto re = GrepRegex::compile("hello", false);
    REQUIRE(re.has_value());

    GrepRegex moved(std::move(*re));
    // Moved-to object should work.
    auto m = moved.findFirst("say hello");
    REQUIRE(m.has_value());
    REQUIRE(m->position == 4);
}

TEST_CASE("GrepRegex: move assignment", "[grep_regex]") {
    auto re1 = GrepRegex::compile("hello", false);
    auto re2 = GrepRegex::compile("world", false);
    REQUIRE(re1.has_value());
    REQUIRE(re2.has_value());

    *re1 = std::move(*re2);
    // re1 should now match "world", not "hello".
    auto m = re1->findFirst("hello world");
    REQUIRE(m.has_value());
    REQUIRE(m->position == 6);
    REQUIRE(m->length == 5);
}

// ---------------------------------------------------------------------------
// Complex patterns (exercise the regex engine thoroughly)
// ---------------------------------------------------------------------------

TEST_CASE("GrepRegex: word boundary pattern", "[grep_regex]") {
    auto re = GrepRegex::compile("\\bfoo\\b", false);
    REQUIRE(re.has_value());

    SECTION("whole word match") {
        auto m = re->findFirst("foo bar");
        REQUIRE(m.has_value());
        REQUIRE(m->position == 0);
    }
    SECTION("no match inside word") {
        auto m = re->findFirst("foobar");
        REQUIRE_FALSE(m.has_value());
    }
}

TEST_CASE("GrepRegex: greedy quantifier captures longest", "[grep_regex]") {
    auto re = GrepRegex::compile("a.*b", false);
    REQUIRE(re.has_value());
    auto m = re->findFirst("a1b2b");
    REQUIRE(m.has_value());
    REQUIRE(m->position == 0);
    REQUIRE(m->length == 5); // Greedy: matches "a1b2b".
}

TEST_CASE("GrepRegex: multiline-like text (no dot-nl)", "[grep_regex]") {
    // By default, '.' does not match newline (grep convention).
    auto re = GrepRegex::compile("a.b", false);
    REQUIRE(re.has_value());
    auto m = re->findFirst("a\nb");
    // '.' should NOT match '\n' → no match.
    // The backtracker runs with match_not_dot_newline for the same behaviour.
    REQUIRE_FALSE(m.has_value());
}

// ---------------------------------------------------------------------------
// Bounded backtracking
// ---------------------------------------------------------------------------

TEST_CASE("GrepRegex: catastrophic backtracking is abandoned within budget",
          "[grep_regex][re2_fallback][redos]") {
    // Nested quantifiers with no possible match take 2^n steps in a backtracker. The
    // backreference keeps the pattern off RE2.
    auto re = GrepRegex::compile("(x+x+)+y\\1", false);
    REQUIRE(re.has_value());
    REQUIRE_FALSE(re->usesRe2());

    const std::string line(64, 'x');
    const auto start = std::chrono::steady_clock::now();
    const auto m = re->findFirst(line);
    const auto elapsed = std::chrono::steady_clock::now() - start;

    CHECK_FALSE(m.has_value());
    CHECK(re->budgetExceededCount() == 1);
    // Unbounded, this search does not finish. Bounded, it stops after at most ~1e7 states.
    CHECK(elapsed < std::chrono::seconds(10));
}

TEST_CASE("GrepRegex: backtracker keeps matching after an abandoned line",
          "[grep_regex][re2_fallback][redos]") {
    auto re = GrepRegex::compile("(x+x+)+y\\1", false);
    REQUIRE(re.has_value());
    REQUIRE_FALSE(re->findFirst(std::string(64, 'x')).has_value());
    REQUIRE(re->budgetExceededCount() == 1);

    // (x+x+) captures "xx", then y, then \1 repeats "xx".
    const auto m = re->findFirst("xxyxx");
    REQUIRE(m.has_value());
    CHECK(m->position == 0);
    CHECK(m->length == 5);
}

TEST_CASE("GrepRegex: backreference match position", "[grep_regex][re2_fallback]") {
    auto re = GrepRegex::compile("(ab)\\1", false);
    REQUIRE(re.has_value());
    const auto m = re->findFirst("xxabab");
    REQUIRE(m.has_value());
    CHECK(m->position == 2);
    CHECK(m->length == 4);
    CHECK(re->budgetExceededCount() == 0);
}

TEST_CASE("GrepRegex: lookbehind uses the backtracker", "[grep_regex][re2_fallback]") {
    auto re = GrepRegex::compile("(?<=foo)bar", false);
    REQUIRE(re.has_value());
    REQUIRE_FALSE(re->usesRe2());
    const auto m = re->findFirst("foobar bar");
    REQUIRE(m.has_value());
    CHECK(m->position == 3);
    CHECK(re->countMatches("foobar bar", 10) == 1);
}

TEST_CASE("GrepRegex: anchors see text before the search offset", "[grep_regex]") {
    // Resuming at an offset must not treat the offset as the start of the line.
    auto caret = GrepRegex::compile("^a", false);
    REQUIRE(caret.has_value());
    CHECK(caret->countMatches("aa", 2) == 1);

    auto word = GrepRegex::compile("\\bfoo", false);
    REQUIRE(word.has_value());
    // "xfoo" has no word boundary before foo; the second foo does.
    const std::string text = "xfoo foo";
    CHECK(word->countMatches(text.data(), text.size()) == 1);
    const auto m = word->findNext(text.data(), text.size(), 1);
    REQUIRE(m.has_value());
    CHECK(m->position == 5);
}

TEST_CASE("GrepRegex: backtracker anchors see text before the search offset",
          "[grep_regex][re2_fallback]") {
    // Same contract on the backtracker. The backreference forces the fallback.
    auto caret = GrepRegex::compile("^(a)\\1?", false);
    REQUIRE(caret.has_value());
    REQUIRE_FALSE(caret->usesRe2());
    CHECK(caret->countMatches("ab", 2) == 1);
    const auto m = caret->findNext("ba", 2, 1);
    CHECK_FALSE(m.has_value());
}
