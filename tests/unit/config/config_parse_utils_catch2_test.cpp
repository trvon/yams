// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/config/detail/config_parse_utils.h>

#include <cmath>

namespace {

TEST_CASE("parseDouble accepts valid decimals with the modern from_chars path",
          "[config][parse][double]") {
    auto value = yams::config::detail::parseDouble("3.5");
    REQUIRE(value.has_value());
    CHECK(std::abs(*value - 3.5) < 1e-12);

    value = yams::config::detail::parseDouble("-0.25");
    REQUIRE(value.has_value());
    CHECK(std::abs(*value + 0.25) < 1e-12);

    value = yams::config::detail::parseDouble("1e3");
    REQUIRE(value.has_value());
    CHECK(std::abs(*value - 1000.0) < 1e-9);

    value = yams::config::detail::parseDouble("+2.5");
    REQUIRE(value.has_value());
    CHECK(std::abs(*value - 2.5) < 1e-12);

    value = yams::config::detail::parseDouble("  7.5  ");
    REQUIRE(value.has_value());
    CHECK(std::abs(*value - 7.5) < 1e-12);
}

TEST_CASE("parseDouble rejects malformed input", "[config][parse][double]") {
    CHECK_FALSE(yams::config::detail::parseDouble(""));
    CHECK_FALSE(yams::config::detail::parseDouble("abc"));
    CHECK_FALSE(yams::config::detail::parseDouble("3.5x"));
    CHECK_FALSE(yams::config::detail::parseDouble("0x1p3"));
    CHECK_FALSE(yams::config::detail::parseDouble("1.5e"));
}

TEST_CASE("parseDoubleViaStrtod mirrors the from_chars grammar for the fallback path",
          "[config][parse][double]") {
    auto value = yams::config::detail::parseDoubleViaStrtod("3.5");
    REQUIRE(value.has_value());
    CHECK(std::abs(*value - 3.5) < 1e-12);

    value = yams::config::detail::parseDoubleViaStrtod("1e-3");
    REQUIRE(value.has_value());
    CHECK(std::abs(*value - 0.001) < 1e-15);

    value = yams::config::detail::parseDoubleViaStrtod("-2.25");
    REQUIRE(value.has_value());
    CHECK(std::abs(*value + 2.25) < 1e-12);

    CHECK_FALSE(yams::config::detail::parseDoubleViaStrtod("0x1p3"));
    CHECK_FALSE(yams::config::detail::parseDoubleViaStrtod("inf"));
    CHECK_FALSE(yams::config::detail::parseDoubleViaStrtod("infinity"));
    CHECK_FALSE(yams::config::detail::parseDoubleViaStrtod("nan"));
    CHECK_FALSE(yams::config::detail::parseDoubleViaStrtod("3.5x"));
    CHECK_FALSE(yams::config::detail::parseDoubleViaStrtod("1.5e"));
    CHECK_FALSE(yams::config::detail::parseDoubleViaStrtod(""));
    CHECK_FALSE(yams::config::detail::parseDoubleViaStrtod("+"));
    CHECK_FALSE(
        yams::config::detail::parseDoubleViaStrtod(" 6.0")); // no whitespace: from_chars semantics
}

} // namespace
