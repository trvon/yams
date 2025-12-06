/**
 * Tests for prompt_util.h - Interactive CLI prompt utilities.
 *
 * These tests verify that:
 * 1. prompt_yes_no() handles various input patterns correctly
 * 2. prompt_input() validates and trims input properly
 * 3. prompt_choice() handles selection and validation
 * 4. Default values are returned appropriately
 * 5. EOF handling works correctly
 *
 * Note: These tests use std::istringstream to mock stdin input.
 */

#include <gtest/gtest.h>
#include <yams/cli/prompt_util.h>

#include <iostream>
#include <sstream>
#include <string>

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

TEST(PromptUtilTest, YesNoAcceptsLowercaseY) {
    MockStdin input("y\n");
    SuppressCout suppress;
    EXPECT_TRUE(prompt_yes_no("Continue?"));
}

TEST(PromptUtilTest, YesNoAcceptsUppercaseY) {
    MockStdin input("Y\n");
    SuppressCout suppress;
    EXPECT_TRUE(prompt_yes_no("Continue?"));
}

TEST(PromptUtilTest, YesNoAcceptsLowercaseN) {
    MockStdin input("n\n");
    SuppressCout suppress;
    EXPECT_FALSE(prompt_yes_no("Continue?"));
}

TEST(PromptUtilTest, YesNoAcceptsUppercaseN) {
    MockStdin input("N\n");
    SuppressCout suppress;
    EXPECT_FALSE(prompt_yes_no("Continue?"));
}

TEST(PromptUtilTest, YesNoEmptyInputReturnsDefaultYes) {
    MockStdin input("\n");
    SuppressCout suppress;
    EXPECT_TRUE(prompt_yes_no("Continue?", {.defaultYes = true}));
}

TEST(PromptUtilTest, YesNoEmptyInputReturnsDefaultNo) {
    MockStdin input("\n");
    SuppressCout suppress;
    EXPECT_FALSE(prompt_yes_no("Continue?", {.defaultYes = false}));
}

TEST(PromptUtilTest, YesNoInvalidInputReturnsDefault) {
    MockStdin input("maybe\n");
    SuppressCout suppress;
    EXPECT_TRUE(prompt_yes_no("Continue?", {.defaultYes = true, .retryOnInvalid = false}));
}

TEST(PromptUtilTest, YesNoEOFReturnsDefault) {
    MockStdin input(""); // Empty = EOF
    SuppressCout suppress;
    EXPECT_TRUE(prompt_yes_no("Continue?", {.defaultYes = true}));
}

TEST(PromptUtilTest, YesNoCustomYesChars) {
    MockStdin input("o\n");
    SuppressCout suppress;
    EXPECT_TRUE(prompt_yes_no("Continue?", {.yesChars = "oO"}));
}

TEST(PromptUtilTest, YesNoCustomNoChars) {
    MockStdin input("x\n");
    SuppressCout suppress;
    EXPECT_FALSE(prompt_yes_no("Continue?", {.noChars = "xX"}));
}

TEST(PromptUtilTest, YesNoRetryOnInvalidEventuallyAccepts) {
    // Provide invalid input followed by valid input
    MockStdin input("maybe\ny\n");
    SuppressCout suppress;
    EXPECT_TRUE(prompt_yes_no("Continue?", {.retryOnInvalid = true}));
}

TEST(PromptUtilTest, YesNoFirstCharacterUsed) {
    // "yes" should be treated as 'y'
    MockStdin input("yes please\n");
    SuppressCout suppress;
    EXPECT_TRUE(prompt_yes_no("Continue?"));
}

// ============================================================================
// prompt_input() tests
// ============================================================================

TEST(PromptUtilTest, InputReturnsUserInput) {
    MockStdin input("hello world\n");
    SuppressCout suppress;
    EXPECT_EQ(prompt_input("Enter value:"), "hello world");
}

TEST(PromptUtilTest, InputTrimsWhitespace) {
    MockStdin input("  trimmed  \n");
    SuppressCout suppress;
    EXPECT_EQ(prompt_input("Enter value:", {.trimWhitespace = true}), "trimmed");
}

TEST(PromptUtilTest, InputPreservesWhitespaceWhenDisabled) {
    MockStdin input("  not trimmed  \n");
    SuppressCout suppress;
    EXPECT_EQ(prompt_input("Enter value:", {.trimWhitespace = false}), "  not trimmed  ");
}

TEST(PromptUtilTest, InputEmptyReturnsDefault) {
    MockStdin input("\n");
    SuppressCout suppress;
    EXPECT_EQ(prompt_input("Enter value:", {.defaultValue = "default"}), "default");
}

TEST(PromptUtilTest, InputEmptyWithNoDefaultReturnsEmpty) {
    MockStdin input("\n");
    SuppressCout suppress;
    EXPECT_EQ(prompt_input("Enter value:", {.defaultValue = "", .allowEmpty = true}), "");
}

TEST(PromptUtilTest, InputEOFReturnsDefault) {
    MockStdin input("");
    SuppressCout suppress;
    EXPECT_EQ(prompt_input("Enter value:", {.defaultValue = "fallback"}), "fallback");
}

TEST(PromptUtilTest, InputValidatorAcceptsValidInput) {
    MockStdin input("valid\n");
    SuppressCout suppress;
    auto validator = [](const std::string& s) { return s == "valid"; };
    EXPECT_EQ(prompt_input("Enter value:", {.validator = validator}), "valid");
}

TEST(PromptUtilTest, InputValidatorRejectsInvalidWithRetry) {
    MockStdin input("invalid\nvalid\n");
    SuppressCout suppress;
    auto validator = [](const std::string& s) { return s == "valid"; };
    EXPECT_EQ(prompt_input("Enter value:", {.retryOnInvalid = true, .validator = validator}),
              "valid");
}

TEST(PromptUtilTest, InputValidatorRejectsInvalidReturnsDefault) {
    MockStdin input("invalid\n");
    SuppressCout suppress;
    auto validator = [](const std::string& s) { return s == "valid"; };
    EXPECT_EQ(prompt_input("Enter value:",
                           {.defaultValue = "fallback", .retryOnInvalid = false, .validator = validator}),
              "fallback");
}

TEST(PromptUtilTest, InputTrimsTabsAndNewlines) {
    MockStdin input("\t\ttabbed\r\n");
    SuppressCout suppress;
    EXPECT_EQ(prompt_input("Enter value:", {.trimWhitespace = true}), "tabbed");
}

// ============================================================================
// prompt_choice() tests
// ============================================================================

TEST(PromptUtilTest, ChoiceReturnsSelectedIndex) {
    MockStdin input("2\n");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"first", "First Option", ""},
                                     {"second", "Second Option", ""},
                                     {"third", "Third Option", ""}};
    EXPECT_EQ(prompt_choice("Select:", items), 1); // 0-indexed
}

TEST(PromptUtilTest, ChoiceEmptyReturnsDefault) {
    MockStdin input("\n");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"a", "Option A", ""}, {"b", "Option B", ""}};
    EXPECT_EQ(prompt_choice("Select:", items, {.defaultIndex = 1}), 1);
}

TEST(PromptUtilTest, ChoiceFirstItemSelected) {
    MockStdin input("1\n");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"first", "First", ""}, {"second", "Second", ""}};
    EXPECT_EQ(prompt_choice("Select:", items), 0);
}

TEST(PromptUtilTest, ChoiceLastItemSelected) {
    MockStdin input("3\n");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {
        {"a", "A", ""}, {"b", "B", ""}, {"c", "C", ""}};
    EXPECT_EQ(prompt_choice("Select:", items), 2);
}

TEST(PromptUtilTest, ChoiceInvalidNumberReturnsDefault) {
    MockStdin input("99\n");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"a", "A", ""}, {"b", "B", ""}};
    EXPECT_EQ(prompt_choice("Select:", items, {.defaultIndex = 0, .retryOnInvalid = false}), 0);
}

TEST(PromptUtilTest, ChoiceNonNumericReturnsDefault) {
    MockStdin input("abc\n");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"a", "A", ""}, {"b", "B", ""}};
    EXPECT_EQ(prompt_choice("Select:", items, {.defaultIndex = 1, .retryOnInvalid = false}), 1);
}

TEST(PromptUtilTest, ChoiceZeroIsInvalid) {
    MockStdin input("0\n");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"a", "A", ""}, {"b", "B", ""}};
    EXPECT_EQ(prompt_choice("Select:", items, {.defaultIndex = 0, .retryOnInvalid = false}), 0);
}

TEST(PromptUtilTest, ChoiceNegativeIsInvalid) {
    MockStdin input("-1\n");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"a", "A", ""}, {"b", "B", ""}};
    EXPECT_EQ(prompt_choice("Select:", items, {.defaultIndex = 0, .retryOnInvalid = false}), 0);
}

TEST(PromptUtilTest, ChoiceRetryEventuallyAccepts) {
    MockStdin input("invalid\n99\n2\n");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"a", "A", ""}, {"b", "B", ""}, {"c", "C", ""}};
    EXPECT_EQ(prompt_choice("Select:", items, {.retryOnInvalid = true}), 1);
}

TEST(PromptUtilTest, ChoiceEOFReturnsDefault) {
    MockStdin input("");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"a", "A", ""}, {"b", "B", ""}};
    EXPECT_EQ(prompt_choice("Select:", items, {.defaultIndex = 1}), 1);
}

TEST(PromptUtilTest, ChoiceThrowsOnEmptyItems) {
    SuppressCout suppress;
    std::vector<ChoiceItem> items;
    EXPECT_THROW(prompt_choice("Select:", items), std::invalid_argument);
}

TEST(PromptUtilTest, ChoiceUsesValueAsLabelWhenLabelEmpty) {
    MockStdin input("1\n");
    SuppressCout suppress;
    std::vector<ChoiceItem> items = {{"value_only", "", "description"}};
    // Should not crash - label defaults to value
    EXPECT_EQ(prompt_choice("Select:", items), 0);
}

// ============================================================================
// Edge case tests
// ============================================================================

TEST(PromptUtilTest, YesNoWhitespaceOnlyTreatedAsEmpty) {
    MockStdin input("   \n");
    SuppressCout suppress;
    // Whitespace-only line should be treated as 'y' or 'n' based on first char
    // Actually, ' ' is not in yesChars or noChars, so it will use default
    EXPECT_TRUE(prompt_yes_no("Continue?", {.defaultYes = true, .retryOnInvalid = false}));
}

TEST(PromptUtilTest, InputOnlyWhitespaceReturnsDefaultWhenTrimmed) {
    MockStdin input("   \t  \n");
    SuppressCout suppress;
    // After trimming, this becomes empty
    EXPECT_EQ(prompt_input("Value:", {.defaultValue = "default", .trimWhitespace = true}), "default");
}

TEST(PromptUtilTest, MultiplePromptsInSequence) {
    MockStdin input("y\nhello\n2\n");
    SuppressCout suppress;

    // First prompt: yes/no
    bool yes = prompt_yes_no("Continue?");
    EXPECT_TRUE(yes);

    // Second prompt: input
    std::string text = prompt_input("Enter text:");
    EXPECT_EQ(text, "hello");

    // Third prompt: choice
    std::vector<ChoiceItem> items = {{"a", "A", ""}, {"b", "B", ""}};
    size_t choice = prompt_choice("Select:", items);
    EXPECT_EQ(choice, 1);
}
