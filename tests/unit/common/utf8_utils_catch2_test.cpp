// Tests for UTF-8 sanitization helper.

#include <catch2/catch_test_macros.hpp>

#include <string>

#include <yams/common/utf8_utils.h>

using yams::common::sanitizeUtf8;

TEST_CASE("common::sanitizeUtf8 preserves ASCII", "[common][utf8][catch2]") {
    CHECK(sanitizeUtf8("hello") == "hello");
    CHECK(sanitizeUtf8("") == "");
}

TEST_CASE("common::sanitizeUtf8 preserves valid multibyte sequences",
          "[common][utf8][catch2]") {
    // 2-byte: C2 A2
    CHECK(sanitizeUtf8(std::string("\xC2\xA2")) == std::string("\xC2\xA2"));
    // 3-byte: E2 82 AC
    CHECK(sanitizeUtf8(std::string("\xE2\x82\xAC")) == std::string("\xE2\x82\xAC"));
    // 4-byte: F0 9F 98 80
    CHECK(sanitizeUtf8(std::string("\xF0\x9F\x98\x80")) == std::string("\xF0\x9F\x98\x80"));
}

TEST_CASE("common::sanitizeUtf8 replaces invalid sequences with '?'",
          "[common][utf8][catch2]") {
    // Lone continuation byte
    CHECK(sanitizeUtf8(std::string("\x80")) == "?");

    // 2-byte lead followed by invalid continuation: consumes lead, keeps next byte
    CHECK(sanitizeUtf8(std::string("\xC2" "x")) == "?x");

    // Truncated 3-byte sequence at end: each invalid byte becomes '?'
    CHECK(sanitizeUtf8(std::string("\xE2\x82")) == "??");

    // Overlong starter (0xC0) is not treated as a valid lead
    CHECK(sanitizeUtf8(std::string("\xC0\x80")) == "??");
}
