// The two hash-prefix thresholds are a deliberate product choice (D3): a bare search token
// must reach 8 hex characters before it is treated as a hash, while an explicit hash argument
// (--hash, a resolver target) may be as short as 6. Both live in one header so the numbers
// cannot drift apart again.

#include <catch2/catch_test_macros.hpp>

#include <yams/common/hash_predicates.h>

using yams::common::isHexDigits;
using yams::common::looksLikeHashQueryToken;
using yams::common::looksLikePartialHashArgument;

TEST_CASE("isHexDigits accepts hex in either case and rejects anything else",
          "[common][hash][catch2]") {
    CHECK(isHexDigits("0123456789abcdefABCDEF"));
    CHECK(isHexDigits(""));
    CHECK_FALSE(isHexDigits("deadbeeg"));
    CHECK_FALSE(isHexDigits("dead beef"));
    CHECK_FALSE(isHexDigits("0x1234"));
}

TEST_CASE("query tokens need 8 hex characters before they look like a hash",
          "[common][hash][catch2]") {
    CHECK_FALSE(looksLikeHashQueryToken("abcdef"));
    CHECK_FALSE(looksLikeHashQueryToken("abcdef1"));
    CHECK(looksLikeHashQueryToken("abcdef12"));
    CHECK(looksLikeHashQueryToken(std::string(64, 'a')));
    CHECK_FALSE(looksLikeHashQueryToken(std::string(65, 'a')));
    CHECK_FALSE(looksLikeHashQueryToken("abcdefg1"));
}

TEST_CASE("explicit hash arguments may be as short as 6 hex characters", "[common][hash][catch2]") {
    CHECK_FALSE(looksLikePartialHashArgument("abcde"));
    CHECK(looksLikePartialHashArgument("abcdef"));
    CHECK(looksLikePartialHashArgument("abcdef1"));
    CHECK(looksLikePartialHashArgument(std::string(64, 'f')));
    CHECK_FALSE(looksLikePartialHashArgument(std::string(65, 'f')));
    CHECK_FALSE(looksLikePartialHashArgument("abcdez"));
}
