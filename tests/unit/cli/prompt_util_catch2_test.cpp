// CLI Prompt Utility tests
// Catch2 migration from GTest (yams-3s4 / yams-cli)
//
// Tests for prompt_util.h - Interactive CLI prompt utilities.

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/prompt_util.h>

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

using namespace yams::cli;

namespace {

/**
 * RAII helper to redirect std::cin to a custom buffer for testing.
 */
class MockStdin {
public:
    explicit MockStdin(const std::string& input) : buffer_(input), oldBuf_(std::cin.rdbuf()) {
        std::cin.rdbuf(buffer_.rdbuf());
    }

    ~MockStdin() {
        std::cin.rdbuf(oldBuf_);
        std::cin.clear(); // Clear any error flags
    }

    MockStdin(const MockStdin&) = delete;
    MockStdin& operator=(const MockStdin&) = delete;

private:
    std::istringstream buffer_;
    std::streambuf* oldBuf_;
};

/**
 * RAII helper to suppress cout during tests.
 */
class SuppressCout {
public:
    SuppressCout() : oldBuf_(std::cout.rdbuf()) { std::cout.rdbuf(sink_.rdbuf()); }

    ~SuppressCout() { std::cout.rdbuf(oldBuf_); }

    SuppressCout(const SuppressCout&) = delete;
    SuppressCout& operator=(const SuppressCout&) = delete;

private:
    std::ostringstream sink_;
    std::streambuf* oldBuf_;
};

} // namespace

// ============================================================================
// prompt_yes_no() tests
// ============================================================================

TEST_CASE("PromptUtil - YesNo accepts lowercase y", "[cli][prompt_util][catch2]") {
    MockStdin input("y\n");
    SuppressCout suppress;
    CHECK(prompt_yes_no("Continue?"));
}

TEST_CASE("PromptUtil - YesNo accepts uppercase Y", "[cli][prompt_util][catch2]") {
    MockStdin input("Y\n");
    SuppressCout suppress;
    CHECK(prompt_yes_no("Continue?"));
}

TEST_CASE("PromptUtil - YesNo accepts lowercase n", "[cli][prompt_util][catch2]") {
    MockStdin input("n\n");
    SuppressCout suppress;
    CHECK_FALSE(prompt_yes_no("Continue?"));
}

TEST_CASE("PromptUtil - YesNo accepts uppercase N", "[cli][prompt_util][catch2]") {
    MockStdin input("N\n");
    SuppressCout suppress;
    CHECK_FALSE(prompt_yes_no("Continue?"));
}

TEST_CASE("PromptUtil - YesNo empty input returns default yes", "[cli][prompt_util][catch2]") {
    MockStdin input("\n");
    SuppressCout suppress;
    CHECK(prompt_yes_no("Continue?", {.defaultYes = true}));
}

TEST_CASE("PromptUtil - YesNo empty input returns default no", "[cli][prompt_util][catch2]") {
    MockStdin input("\n");
    SuppressCout suppress;
    CHECK_FALSE(prompt_yes_no("Continue?", {.defaultYes = false}));
}

TEST_CASE("PromptUtil - YesNo invalid input returns default", "[cli][prompt_util][catch2]") {
    MockStdin input("maybe\n");
    SuppressCout suppress;
    CHECK(prompt_yes_no("Continue?", {.defaultYes = true, .retryOnInvalid = false}));
}

TEST_CASE("PromptUtil - YesNo EOF returns default", "[cli][prompt_util][catch2]") {
    MockStdin input(""); // Empty = EOF
    SuppressCout suppress;
    CHECK(prompt_yes_no("Continue?", {.defaultYes = true}));
}

TEST_CASE("PromptUtil - YesNo custom yes chars", "[cli][prompt_util][catch2]") {
    MockStdin input("o\n");
    SuppressCout suppress;
    CHECK(prompt_yes_no("Continue?", {.yesChars = "oO"}));
}

TEST_CASE("PromptUtil - YesNo custom no chars", "[cli][prompt_util][catch2]") {
    MockStdin input("x\n");
    SuppressCout suppress;
    CHECK_FALSE(prompt_yes_no("Continue?", {.noChars = "xX"}));
}

TEST_CASE("PromptUtil - YesNo retry on invalid eventually accepts", "[cli][prompt_util][catch2]") {
    MockStdin input("maybe\ny\n");
    SuppressCout suppress;
    CHECK(prompt_yes_no("Continue?", {.retryOnInvalid = true}));
}

TEST_CASE("PromptUtil - YesNo first character used", "[cli][prompt_util][catch2]") {
    MockStdin input("yes please\n");
    SuppressCout suppress;
    CHECK(prompt_yes_no("Continue?"));
}

// ============================================================================
// prompt_input() tests
// ============================================================================

TEST_CASE("PromptUtil - Input returns user input", "[cli][prompt_util][catch2]") {
    MockStdin input("hello world\n");
    SuppressCout suppress;
    CHECK(prompt_input("Enter value:") == "hello world");
}

TEST_CASE("PromptUtil - Input trims whitespace", "[cli][prompt_util][catch2]") {
    MockStdin input("  trimmed  \n");
    SuppressCout suppress;
    CHECK(prompt_input("Enter value:", {.trimWhitespace = true}) == "trimmed");
}

TEST_CASE("PromptUtil - Input preserves whitespace when disabled", "[cli][prompt_util][catch2]") {
    MockStdin input("  not trimmed  \n");
    SuppressCout suppress;
    CHECK(prompt_input("Enter value:", {.trimWhitespace = false}) == "  not trimmed  ");
}

TEST_CASE("PromptUtil - Input empty returns default", "[cli][prompt_util][catch2]") {
    MockStdin input("\n");
    SuppressCout suppress;
    CHECK(prompt_input("Enter value:", {.defaultValue = "default"}) == "default");
}

TEST_CASE("PromptUtil - Input empty with no default returns empty", "[cli][prompt_util][catch2]") {
    MockStdin input("\n");
    SuppressCout suppress;
    CHECK(prompt_input("Enter value:", {.defaultValue = "", .allowEmpty = true}) == "");
}

TEST_CASE("PromptUtil - Input EOF returns default", "[cli][prompt_util][catch2]") {
    MockStdin input("");
    SuppressCout suppress;
    CHECK(prompt_input("Enter value:", {.defaultValue = "fallback"}) == "fallback");
}

TEST_CASE("PromptUtil - Input validator accepts valid input", "[cli][prompt_util][catch2]") {
    MockStdin input("valid\n");
    SuppressCout suppress;
    auto validator = [](const std::string& s) { return s == "valid"; };
    CHECK(prompt_input("Enter value:", {.validator = validator}) == "valid");
}

TEST_CASE("PromptUtil - Input validator rejects invalid with retry", "[cli][prompt_util][catch2]") {
    MockStdin input("invalid\nvalid\n");
    SuppressCout suppress;
    auto validator = [](const std::string& s) { return s == "valid"; };
    CHECK(prompt_input("Enter value:", {.retryOnInvalid = true, .validator = validator}) ==
          "valid");
}

TEST_CASE("PromptUtil - Input validator rejects invalid returns default",
          "[cli][prompt_util][catch2]") {
    MockStdin input("invalid\n");
    SuppressCout suppress;
    auto validator = [](const std::string& s) { return s == "valid"; };
    CHECK(prompt_input("Enter value:", {.defaultValue = "fallback",
                                        .retryOnInvalid = false,
                                        .validator = validator}) == "fallback");
}

TEST_CASE("PromptUtil - Input trims tabs and newlines", "[cli][prompt_util][catch2]") {
    MockStdin input("\t\ttabbed\r\n");
    SuppressCout suppress;
    CHECK(prompt_input("Enter value:", {.trimWhitespace = true}) == "tabbed");
}

// ============================================================================
// prompt_choice() tests
// ============================================================================

TEST_CASE("PromptUtil - Choice returns selected index", "[cli][prompt_util][catch2]") {
    MockStdin input("2\n");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"first", "First Option", ""},
                                     {"second", "Second Option", ""},
                                     {"third", "Third Option", ""}};
    CHECK(prompt_choice("Select:", items) == 1); // 0-indexed
}

TEST_CASE("PromptUtil - Choice empty returns default", "[cli][prompt_util][catch2]") {
    MockStdin input("\n");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"a", "Option A", ""}, {"b", "Option B", ""}};
    CHECK(prompt_choice("Select:", items, {.defaultIndex = 1}) == 1);
}

TEST_CASE("PromptUtil - Choice first item selected", "[cli][prompt_util][catch2]") {
    MockStdin input("1\n");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"first", "First", ""}, {"second", "Second", ""}};
    CHECK(prompt_choice("Select:", items) == 0);
}

TEST_CASE("PromptUtil - Choice last item selected", "[cli][prompt_util][catch2]") {
    MockStdin input("3\n");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"a", "A", ""}, {"b", "B", ""}, {"c", "C", ""}};
    CHECK(prompt_choice("Select:", items) == 2);
}

TEST_CASE("PromptUtil - Choice invalid number returns default", "[cli][prompt_util][catch2]") {
    MockStdin input("99\n");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"a", "A", ""}, {"b", "B", ""}};
    CHECK(prompt_choice("Select:", items, {.defaultIndex = 0, .retryOnInvalid = false}) == 0);
}

TEST_CASE("PromptUtil - Choice non-numeric returns default", "[cli][prompt_util][catch2]") {
    MockStdin input("abc\n");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"a", "A", ""}, {"b", "B", ""}};
    CHECK(prompt_choice("Select:", items, {.defaultIndex = 1, .retryOnInvalid = false}) == 1);
}

TEST_CASE("PromptUtil - Choice zero is invalid", "[cli][prompt_util][catch2]") {
    MockStdin input("0\n");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"a", "A", ""}, {"b", "B", ""}};
    CHECK(prompt_choice("Select:", items, {.defaultIndex = 0, .retryOnInvalid = false}) == 0);
}

TEST_CASE("PromptUtil - Choice negative is invalid", "[cli][prompt_util][catch2]") {
    MockStdin input("-1\n");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"a", "A", ""}, {"b", "B", ""}};
    CHECK(prompt_choice("Select:", items, {.defaultIndex = 0, .retryOnInvalid = false}) == 0);
}

TEST_CASE("PromptUtil - Choice retry eventually accepts", "[cli][prompt_util][catch2]") {
    MockStdin input("invalid\n99\n2\n");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"a", "A", ""}, {"b", "B", ""}, {"c", "C", ""}};
    CHECK(prompt_choice("Select:", items, {.retryOnInvalid = true}) == 1);
}

TEST_CASE("PromptUtil - Choice EOF returns default", "[cli][prompt_util][catch2]") {
    MockStdin input("");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"a", "A", ""}, {"b", "B", ""}};
    CHECK(prompt_choice("Select:", items, {.defaultIndex = 1}) == 1);
}

TEST_CASE("PromptUtil - Choice throws on empty items", "[cli][prompt_util][catch2]") {
    SuppressCout suppress;
    std::vector<ChoiceItem> items;
    REQUIRE_THROWS_AS(prompt_choice("Select:", items), std::invalid_argument);
}

TEST_CASE("PromptUtil - Choice uses value as label when label empty",
          "[cli][prompt_util][catch2]") {
    MockStdin input("1\n");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"value_only", "", "description"}};
    CHECK(prompt_choice("Select:", items) == 0);
}

// ============================================================================
// Edge case tests
// ============================================================================

TEST_CASE("PromptUtil - YesNo whitespace only treated as empty", "[cli][prompt_util][catch2]") {
    MockStdin input("   \n");
    SuppressCout suppress;
    CHECK(prompt_yes_no("Continue?", {.defaultYes = true, .retryOnInvalid = false}));
}

TEST_CASE("PromptUtil - Input only whitespace returns default when trimmed",
          "[cli][prompt_util][catch2]") {
    MockStdin input("   \t  \n");
    SuppressCout suppress;
    CHECK(prompt_input("Value:", {.defaultValue = "default", .trimWhitespace = true}) == "default");
}

TEST_CASE("PromptUtil - Multiple prompts in sequence", "[cli][prompt_util][catch2]") {
    MockStdin input("y\nhello\n2\n");
    SuppressCout suppress;

    // First prompt: yes/no
    bool yes = prompt_yes_no("Continue?");
    CHECK(yes);

    // Second prompt: input
    std::string text = prompt_input("Enter text:");
    CHECK(text == "hello");

    // Third prompt: choice
    std::vector<ChoiceItem> items = {{"a", "A", ""}, {"b", "B", ""}};
    size_t choice = prompt_choice("Select:", items);
    CHECK(choice == 1);
}
