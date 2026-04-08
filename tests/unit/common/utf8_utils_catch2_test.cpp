// Tests for UTF-8 sanitization helper.

#include <catch2/catch_test_macros.hpp>

#include <string>

#include <yams/common/utf8_utils.h>

using yams::common::ensureValidUtf8;
using yams::common::sanitizeUtf8;

TEST_CASE("common::sanitizeUtf8 preserves ASCII", "[common][utf8][catch2]") {
    CHECK(sanitizeUtf8("hello") == "hello");
    CHECK(sanitizeUtf8("") == "");
}

TEST_CASE("common::sanitizeUtf8 preserves valid multibyte sequences", "[common][utf8][catch2]") {
    // 2-byte: C2 A2
    CHECK(sanitizeUtf8(std::string("\xC2\xA2")) == std::string("\xC2\xA2"));
    // 3-byte: E2 82 AC
    CHECK(sanitizeUtf8(std::string("\xE2\x82\xAC")) == std::string("\xE2\x82\xAC"));
    // 4-byte: F0 9F 98 80
    CHECK(sanitizeUtf8(std::string("\xF0\x9F\x98\x80")) == std::string("\xF0\x9F\x98\x80"));
}

TEST_CASE("common::sanitizeUtf8 replaces invalid sequences with '?'", "[common][utf8][catch2]") {
    // Lone continuation byte
    CHECK(sanitizeUtf8(std::string("\x80")) == "?");

    // 2-byte lead followed by invalid continuation: consumes lead, keeps next byte
    CHECK(sanitizeUtf8(std::string("\xC2"
                                   "x")) == "?x");

    // Truncated 3-byte sequence at end: each invalid byte becomes '?'
    CHECK(sanitizeUtf8(std::string("\xE2\x82")) == "??");

    // Overlong starter (0xC0) is not treated as a valid lead
    CHECK(sanitizeUtf8(std::string("\xC0\x80")) == "??");
}

// --- ensureValidUtf8 tests ---

TEST_CASE("ensureValidUtf8 returns input view for valid ASCII", "[common][utf8][lazy]") {
    std::string storage;
    std::string_view input = "hello world";
    auto result = ensureValidUtf8(input, storage);
    CHECK(result == "hello world");
    CHECK(result.data() == input.data()); // Points to original, not storage
    CHECK(storage.empty());
}

TEST_CASE("ensureValidUtf8 returns input view for valid multibyte", "[common][utf8][lazy]") {
    std::string storage;
    // 2-byte
    std::string two("\xC2\xA2");
    auto r2 = ensureValidUtf8(two, storage);
    CHECK(r2 == two);
    CHECK(r2.data() == two.data());
    CHECK(storage.empty());

    // 3-byte
    std::string three("\xE2\x82\xAC");
    auto r3 = ensureValidUtf8(three, storage);
    CHECK(r3 == three);
    CHECK(r3.data() == three.data());

    // 4-byte
    std::string four("\xF0\x9F\x98\x80");
    auto r4 = ensureValidUtf8(four, storage);
    CHECK(r4 == four);
    CHECK(r4.data() == four.data());
}

TEST_CASE("ensureValidUtf8 sanitizes invalid input into storage", "[common][utf8][lazy]") {
    std::string storage;
    std::string invalid("\x80invalid");
    auto result = ensureValidUtf8(invalid, storage);
    CHECK(result == sanitizeUtf8(invalid));
    CHECK(result.data() == storage.data()); // Points into storage
    CHECK(!storage.empty());
}

TEST_CASE("ensureValidUtf8 storage reuse across calls", "[common][utf8][lazy]") {
    std::string storage;

    // First call: valid input, storage untouched
    std::string valid = "abc";
    auto r1 = ensureValidUtf8(valid, storage);
    CHECK(r1.data() == valid.data());
    CHECK(storage.empty());

    // Second call: invalid input, storage written
    std::string invalid("\xFE\xFF");
    auto r2 = ensureValidUtf8(invalid, storage);
    CHECK(r2 == sanitizeUtf8(invalid));
    CHECK(!storage.empty());
}

TEST_CASE("ensureValidUtf8 handles empty input", "[common][utf8][lazy]") {
    std::string storage;
    std::string_view empty;
    auto result = ensureValidUtf8(empty, storage);
    CHECK(result.empty());
    CHECK(storage.empty());
}
