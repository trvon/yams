#include <algorithm>
#include <string>
#include <vector>
#include <catch2/catch_test_macros.hpp>
#include <yams/app/services/literal_extractor.hpp>

using namespace yams::app::services;

TEST_CASE("LiteralExtractor: Pure literal patterns", "[literal_extractor]") {
    SECTION("Simple literal") {
        auto result = LiteralExtractor::extract("hello", false);
        REQUIRE(result.isComplete);
        REQUIRE(result.literals.size() == 1);
        REQUIRE(result.literals[0] == "hello");
        REQUIRE(result.longestLength == 5);
    }

    SECTION("Literal with spaces") {
        auto result = LiteralExtractor::extract("hello world", false);
        REQUIRE(result.isComplete);
        REQUIRE(result.literals.size() == 1);
        REQUIRE(result.literals[0] == "hello world");
    }
}

TEST_CASE("LiteralExtractor: Patterns with metacharacters", "[literal_extractor]") {
    SECTION("Literal prefix with metacharacter") {
        auto result = LiteralExtractor::extract("class.*", false);
        REQUIRE(!result.isComplete);
        REQUIRE(result.literals.size() == 1);
        REQUIRE(result.literals[0] == "class");
        REQUIRE(result.longestLength == 5);
    }

    SECTION("Literal surrounded by metacharacters") {
        auto result = LiteralExtractor::extract(".*error.*", false);
        REQUIRE(!result.isComplete);
        REQUIRE(result.literals.size() == 1);
        REQUIRE(result.literals[0] == "error");
    }

    SECTION("Multiple literals separated by metacharacters") {
        auto result = LiteralExtractor::extract("foo.*bar", false);
        REQUIRE(!result.isComplete);
        REQUIRE(result.literals.size() == 2);
        // Should contain both literals
        bool hasFoo = false, hasBar = false;
        for (const auto& lit : result.literals) {
            if (lit == "foo")
                hasFoo = true;
            if (lit == "bar")
                hasBar = true;
        }
        REQUIRE(hasFoo);
        REQUIRE(hasBar);
    }
}

TEST_CASE("LiteralExtractor: Escaped characters", "[literal_extractor]") {
    SECTION("Escaped dot becomes literal") {
        auto result = LiteralExtractor::extract("file\\.txt", false);
        REQUIRE(result.isComplete);
        REQUIRE(result.literals.size() == 1);
        REQUIRE(result.literals[0] == "file.txt");
    }

    SECTION("Escaped star becomes literal") {
        auto result = LiteralExtractor::extract("x\\*y", false);
        REQUIRE(result.isComplete);
        REQUIRE(result.literals.size() == 1);
        REQUIRE(result.literals[0] == "x*y");
    }
}

TEST_CASE("LiteralExtractor: Special sequences", "[literal_extractor]") {
    SECTION("Word boundary breaks literal") {
        auto result = LiteralExtractor::extract("\\bword\\b", false);
        REQUIRE(!result.isComplete);
        REQUIRE(result.literals.size() == 1);
        REQUIRE(result.literals[0] == "word");
    }

    SECTION("Digit class breaks literal") {
        auto result = LiteralExtractor::extract("id\\d+", false);
        REQUIRE(!result.isComplete);
        REQUIRE(result.literals.size() == 1);
        REQUIRE(result.literals[0] == "id");
    }
}

TEST_CASE("LiteralExtractor: Case insensitivity", "[literal_extractor]") {
    SECTION("Converts to lowercase when ignoreCase=true") {
        auto result = LiteralExtractor::extract("FooBar", true);
        REQUIRE(result.isComplete);
        REQUIRE(result.literals.size() == 1);
        REQUIRE(result.literals[0] == "foobar");
    }
}

TEST_CASE("LiteralExtractor: longest() helper", "[literal_extractor]") {
    SECTION("Returns longest literal") {
        auto result = LiteralExtractor::extract("a.*longer.*b", false);
        REQUIRE(result.longest() == "longer");
    }

    SECTION("Empty result returns empty string") {
        auto result = LiteralExtractor::extract("\\d+", false);
        REQUIRE(result.empty());
    }
}

// --- Boyer-Moore-Horspool Tests ---

TEST_CASE("BMHSearcher: Basic literal search", "[bmh]") {
    SECTION("Find single occurrence") {
        BMHSearcher searcher("hello", false);
        std::string text = "say hello world";
        REQUIRE(searcher.find(text) == 4);
    }

    SECTION("Pattern not found") {
        BMHSearcher searcher("goodbye", false);
        std::string text = "say hello world";
        REQUIRE(searcher.find(text) == std::string::npos);
    }

    SECTION("Multiple occurrences") {
        BMHSearcher searcher("the", false);
        std::string text = "the quick brown fox jumps over the lazy dog";
        auto matches = searcher.findAll(text);
        REQUIRE(matches.size() == 2);
        REQUIRE(matches[0] == 0);
        REQUIRE(matches[1] == 31);
    }
}

TEST_CASE("BMHSearcher: Case sensitivity", "[bmh]") {
    SECTION("Case-sensitive match") {
        BMHSearcher searcher("Hello", false);
        std::string text = "say hello world";
        REQUIRE(searcher.find(text) == std::string::npos);
    }

    SECTION("Case-insensitive match") {
        BMHSearcher searcher("Hello", true);
        std::string text = "say hello world";
        REQUIRE(searcher.find(text) == 4);
    }
}

TEST_CASE("BMHSearcher: Edge cases", "[bmh]") {
    SECTION("Empty pattern") {
        BMHSearcher searcher("", false);
        std::string text = "anything";
        REQUIRE(searcher.find(text) == std::string::npos);
    }

    SECTION("Pattern longer than text") {
        BMHSearcher searcher("very long pattern", false);
        std::string text = "short";
        REQUIRE(searcher.find(text) == std::string::npos);
    }

    SECTION("Single character pattern") {
        BMHSearcher searcher("x", false);
        std::string text = "axbxc";
        auto matches = searcher.findAll(text);
        REQUIRE(matches.size() == 2);
        REQUIRE(matches[0] == 1);
        REQUIRE(matches[1] == 3);
    }

    SECTION("Pattern at start") {
        BMHSearcher searcher("start", false);
        std::string text = "start of text";
        REQUIRE(searcher.find(text) == 0);
    }

    SECTION("Pattern at end") {
        BMHSearcher searcher("end", false);
        std::string text = "text at end";
        REQUIRE(searcher.find(text) == 8);
    }
}

TEST_CASE("BMHSearcher: Performance characteristics", "[bmh]") {
    SECTION("Skips efficiently on mismatches") {
        BMHSearcher searcher("pattern", false);
        // Text with no matches - should skip through quickly
        std::string text(10000, 'x');
        REQUIRE(searcher.find(text) == std::string::npos);
    }

    SECTION("Handles repeated patterns") {
        BMHSearcher searcher("abc", false);
        std::string text = "abcabcabcabcabc";
        auto matches = searcher.findAll(text);
        REQUIRE(matches.size() == 5);
    }
}

TEST_CASE("BMHSearcher: Real-world patterns", "[bmh]") {
    SECTION("Find TODO in code") {
        BMHSearcher searcher("TODO", false);
        std::string code = "// TODO: fix this\nint x = 0; // TODO: optimize";
        auto matches = searcher.findAll(code);
        REQUIRE(matches.size() == 2);
    }

    SECTION("Find function call") {
        BMHSearcher searcher("malloc", false);
        std::string code = "void* p = malloc(size);\nfree(p);";
        REQUIRE(searcher.find(code) == 10);
    }

    SECTION("Find URL pattern") {
        BMHSearcher searcher("https://", false);
        std::string text = "Visit https://example.com or https://github.com";
        auto matches = searcher.findAll(text);
        REQUIRE(matches.size() == 2);
    }
}

// ---------------------------------------------------------------------------
// BMHSearcher: Long patterns
//
// Case-sensitive search uses the standard-library path; ignore-case search uses simdMemmemCI.
// ---------------------------------------------------------------------------

TEST_CASE("BMHSearcher: Long case-sensitive pattern", "[bmh]") {
    // 20-char needle — well above the 16-byte threshold.
    const std::string needle = "namespace yams::app";

    SECTION("found in middle of text") {
        BMHSearcher searcher(needle, false);
        std::string text = "// file header\nnamespace yams::app {\n// body\n}";
        REQUIRE(searcher.find(text) == 15);
    }

    SECTION("found at start") {
        BMHSearcher searcher(needle, false);
        std::string text = "namespace yams::app::services { /* ... */ }";
        REQUIRE(searcher.find(text) == 0);
    }

    SECTION("not found") {
        BMHSearcher searcher(needle, false);
        std::string text = "namespace yams::search { /* ... */ }";
        REQUIRE(searcher.find(text) == std::string::npos);
    }

    SECTION("findAll in large text") {
        BMHSearcher searcher(needle, false);
        std::string text;
        // Build a text with the needle at known offsets.
        text += std::string(100, 'x'); // padding
        text += needle;                // match 1 at offset 100
        text += std::string(200, 'y'); // padding
        text += needle;                // match 2 at offset 100+needle.size()+200
        text += std::string(50, 'z');  // trailing padding
        auto matches = searcher.findAll(text);
        REQUIRE(matches.size() == 2);
        REQUIRE(matches[0] == 100);
        REQUIRE(matches[1] == 100 + needle.size() + 200);
    }
}

TEST_CASE("BMHSearcher: Long case-insensitive pattern (SIMD CI path)", "[bmh][simd]") {
    // 24-char needle exercises simdMemmemCI via findBMH.
    const std::string needle = "The Quick Brown Fox Jump";

    SECTION("matches uppercase haystack") {
        BMHSearcher searcher(needle, true);
        std::string text = "THE QUICK BROWN FOX JUMPS OVER THE LAZY DOG";
        REQUIRE(searcher.find(text) == 0);
    }

    SECTION("matches mixed-case haystack") {
        BMHSearcher searcher(needle, true);
        std::string text = "see the quick BROWN Fox Jump here";
        REQUIRE(searcher.find(text) == 4);
    }

    SECTION("not found") {
        BMHSearcher searcher(needle, true);
        std::string text = "the slow red cat sleeps on the warm mat";
        REQUIRE(searcher.find(text) == std::string::npos);
    }

    SECTION("findAll in large text with mixed case") {
        BMHSearcher searcher(needle, true);
        std::string text;
        text += std::string(64, '.');
        text += "THE QUICK BROWN FOX JUMP"; // match 1 at 64
        text += std::string(64, '.');
        text += "the quick brown fox jump"; // match 2 at 64+24+64 = 152
        text += std::string(64, '.');
        auto matches = searcher.findAll(text);
        REQUIRE(matches.size() == 2);
        REQUIRE(matches[0] == 64);
        REQUIRE(matches[1] == 152);
    }
}

TEST_CASE("BMHSearcher: Exactly 16-byte pattern (boundary)", "[bmh][simd]") {
    // 16 bytes is the exact threshold between short and long dispatch lanes.
    const std::string needle = "0123456789abcdef"; // exactly 16 bytes
    REQUIRE(needle.size() == 16);

    SECTION("case-sensitive") {
        BMHSearcher searcher(needle, false);
        std::string text = "prefix_0123456789abcdef_suffix";
        REQUIRE(searcher.find(text) == 7);
    }

    SECTION("case-insensitive") {
        BMHSearcher searcher(needle, true);
        std::string text = "PREFIX_0123456789ABCDEF_SUFFIX";
        REQUIRE(searcher.find(text) == 7);
    }
}

// The extracted literals are *required*: grep skips any line that lacks the longest one, and
// the trigram index skips any document lacking them. A literal that a match can avoid is a
// missed match. `admitDeferrable|NoSuchSymbol` found nothing because the longest literal,
// NoSuchSymbol, was required of every line.
TEST_CASE("LiteralExtractor: only literals every match must contain",
          "[literal_extractor][required]") {
    const auto literals = [](std::string_view pattern, bool ignoreCase = false) {
        auto result = LiteralExtractor::extract(pattern, ignoreCase);
        std::vector<std::string> out = result.literals;
        std::sort(out.begin(), out.end());
        return out;
    };
    using V = std::vector<std::string>;

    SECTION("top-level alternation requires nothing") {
        CHECK(literals("foo|barbaz").empty());
        CHECK(literals("admitDeferrable|NoSuchSymbol").empty());
    }
    SECTION("an optional or repeatable-zero atom is not required") {
        CHECK(literals("colou?r") == V{"colo", "r"});
        CHECK(literals("colou*r") == V{"colo", "r"});
        CHECK(literals("ab{0,3}cd") == V{"a", "cd"});
    }
    SECTION("an atom repeated at least once is required once") {
        CHECK(literals("ab+cd") == V{"ab", "cd"});
        CHECK(literals("ab{2,}cd") == V{"ab", "cd"});
    }
    SECTION("a character class is not a literal") {
        CHECK(literals("[abc]def") == V{"def"});
        CHECK(literals("x[^)]*y") == V{"x", "y"});
    }
    SECTION("a group is opaque; text outside it is still required") {
        CHECK(literals("(foo|bar)baz") == V{"baz"});
        CHECK(literals("pre(fix)?post") == V{"post", "pre"});
    }
    SECTION("an escaped metacharacter is a literal character") {
        CHECK(literals("a\\|b") == V{"a|b"});
        CHECK(literals("x\\(y\\)") == V{"x(y)"});
    }
    SECTION("ordinary patterns keep their literals") {
        CHECK(literals("class.*Name") == V{"Name", "class"});
        CHECK(literals("^foo$") == V{"foo"});
    }
}
