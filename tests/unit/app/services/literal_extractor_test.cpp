#include <yams/app/services/literal_extractor.hpp>
#include <catch2/catch_test_macros.hpp>
#include <string>
#include <vector>

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
            if (lit == "foo") hasFoo = true;
            if (lit == "bar") hasBar = true;
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
        REQUIRE(matches[1] == 32);
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
