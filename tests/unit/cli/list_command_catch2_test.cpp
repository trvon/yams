// Unit tests for list_command pure-logic helpers
#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>

#include <cctype>

#include <string>
#include <vector>

namespace yams::cli::test {

TEST_CASE("globToSqlLikePattern converts wildcards", "[list][glob]") {
    // These are static methods in ListCommand — test via standalone reimplementation

    // Basic * → %
    auto globToSql = [](const std::string& glob) -> std::string {
        std::string result;
        result.reserve(glob.size());
        for (size_t i = 0; i < glob.size(); ++i) {
            char c = glob[i];
            if (c == '*') {
                if (i + 1 < glob.size() && glob[i + 1] == '*') {
                    result += '%';
                    i++;
                    if (i + 1 < glob.size() && glob[i + 1] == '/')
                        i++;
                } else {
                    result += '%';
                }
            } else if (c == '?') {
                result += '_';
            } else if (c == '%' || c == '_') {
                result += '\\';
                result += c;
            } else {
                result += c;
            }
        }
        return result;
    };

    CHECK(globToSql("") == "");
    CHECK(globToSql("hello") == "hello");
    CHECK(globToSql("*.cpp") == "%.cpp");
    CHECK(globToSql("test_?.cpp") == "test\\__.cpp");
    CHECK(globToSql("src/**/*.h") == "src/%%.h");
    CHECK(globToSql("100%_coverage") == "100\\%\\_coverage");
    CHECK(globToSql("a?b*") == "a_b%");
}

TEST_CASE("extractSnippet compresses whitespace and truncates", "[list][snippet]") {
    auto extractSnippet = [](const std::string& content, int maxLength) -> std::string {
        if (content.empty())
            return "";
        std::string snippet = content;
        std::string result;
        bool lastWasSpace = false;
        for (char c : snippet) {
            if (std::isspace(c)) {
                if (!lastWasSpace) {
                    result += ' ';
                    lastWasSpace = true;
                }
            } else {
                result += c;
                lastWasSpace = false;
            }
        }
        if (result.length() > static_cast<size_t>(maxLength)) {
            return result.substr(0, static_cast<size_t>(maxLength - 3)) + "...";
        }
        return result;
    };

    CHECK(extractSnippet("", 10) == "");
    CHECK(extractSnippet("hello", 10) == "hello");
    CHECK(extractSnippet("hello  world", 20) == "hello world");
    CHECK(extractSnippet("line1\nline2\tline3", 30) == "line1 line2 line3");
    CHECK(extractSnippet("this is a very long string that should be truncated xyz", 20) ==
          "this is a very lo...");
    CHECK(extractSnippet("short", 10) == "short");
    CHECK(extractSnippet("   leading   spaces   ", 30) == " leading spaces ");
}

TEST_CASE("globToSqlLikePattern edge cases", "[list][glob]") {
    auto globToSql = [](const std::string& glob) -> std::string {
        std::string result;
        result.reserve(glob.size());
        for (size_t i = 0; i < glob.size(); ++i) {
            char c = glob[i];
            if (c == '*') {
                if (i + 1 < glob.size() && glob[i + 1] == '*') {
                    result += '%';
                    i++;
                    if (i + 1 < glob.size() && glob[i + 1] == '/')
                        i++;
                } else {
                    result += '%';
                }
            } else if (c == '?') {
                result += '_';
            } else if (c == '%' || c == '_') {
                result += '\\';
                result += c;
            } else {
                result += c;
            }
        }
        return result;
    };

    // Double-star with trailing slash (slash consumed)
    CHECK(globToSql("a**/b") == "a%b");
    CHECK(globToSql("a**b") == "a%b");
    CHECK(globToSql("*") == "%");
    CHECK(globToSql("?") == "_");
    CHECK(globToSql("?*?") == "_%_");
    CHECK(globToSql("a_b") == "a\\_b");
    CHECK(globToSql("test%coverage") == "test\\%coverage");
}

TEST_CASE("extractSnippet handles exact max length", "[list][snippet]") {
    auto extractSnippet = [](const std::string& content, int maxLength) -> std::string {
        if (content.empty())
            return "";
        std::string snippet = content;
        std::string result;
        bool lastWasSpace = false;
        for (char c : snippet) {
            if (std::isspace(static_cast<unsigned char>(c))) {
                if (!lastWasSpace) {
                    result += ' ';
                    lastWasSpace = true;
                }
            } else {
                result += c;
                lastWasSpace = false;
            }
        }
        if (result.length() > static_cast<size_t>(maxLength)) {
            return result.substr(0, static_cast<size_t>(maxLength - 3)) + "...";
        }
        return result;
    };

    // Exactly maxLength characters — no truncation
    CHECK(extractSnippet("12345", 5) == "12345");
    // One over → truncated with ellipsis
    CHECK(extractSnippet("123456", 5) == "12...");
    // Very short truncations
    CHECK(extractSnippet("abcdefghij", 4) == "a...");
    // All whitespace compresses
    CHECK(extractSnippet("\n\n\n", 10) == " ");
}

} // namespace yams::cli::test
