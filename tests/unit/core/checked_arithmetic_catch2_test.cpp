// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/core/checked_arithmetic.h>

#include <cstdint>
#include <limits>

namespace {

TEST_CASE("checked arithmetic accepts integral results at signed boundaries",
          "[core][arithmetic]") {
    std::int64_t result = 0;

    CHECK_FALSE(
        yams::core::addOverflow(std::numeric_limits<std::int64_t>::max(), std::int64_t{0}, result));
    CHECK(result == std::numeric_limits<std::int64_t>::max());

    CHECK_FALSE(yams::core::subOverflow(std::numeric_limits<std::int64_t>::min(),
                                        std::numeric_limits<std::int64_t>::min(), result));
    CHECK(result == 0);

    CHECK_FALSE(
        yams::core::mulOverflow(std::numeric_limits<std::int64_t>::min(), std::int64_t{1}, result));
    CHECK(result == std::numeric_limits<std::int64_t>::min());
}

TEST_CASE("checked arithmetic rejects signed overflow without modifying the result",
          "[core][arithmetic]") {
    std::int64_t result = 42;

    CHECK(
        yams::core::addOverflow(std::numeric_limits<std::int64_t>::max(), std::int64_t{1}, result));
    CHECK(result == 42);

    CHECK(yams::core::addOverflow(std::numeric_limits<std::int64_t>::min(), std::int64_t{-1},
                                  result));
    CHECK(result == 42);

    CHECK(
        yams::core::subOverflow(std::numeric_limits<std::int64_t>::min(), std::int64_t{1}, result));
    CHECK(result == 42);

    CHECK(yams::core::subOverflow(std::numeric_limits<std::int64_t>::max(), std::int64_t{-1},
                                  result));
    CHECK(result == 42);

    CHECK(
        yams::core::mulOverflow(std::numeric_limits<std::int64_t>::max(), std::int64_t{2}, result));
    CHECK(result == 42);

    CHECK(yams::core::mulOverflow(std::numeric_limits<std::int64_t>::min(), std::int64_t{-1},
                                  result));
    CHECK(result == 42);
}

TEST_CASE("checked arithmetic rejects unsigned overflow and underflow", "[core][arithmetic]") {
    std::uint64_t result = 42;

    CHECK(yams::core::addOverflow(std::numeric_limits<std::uint64_t>::max(), std::uint64_t{1},
                                  result));
    CHECK(result == 42);

    CHECK(yams::core::subOverflow(std::uint64_t{0}, std::uint64_t{1}, result));
    CHECK(result == 42);

    CHECK(yams::core::mulOverflow(std::numeric_limits<std::uint64_t>::max(), std::uint64_t{2},
                                  result));
    CHECK(result == 42);

    CHECK_FALSE(yams::core::mulOverflow(std::uint64_t{0}, std::numeric_limits<std::uint64_t>::max(),
                                        result));
    CHECK(result == 0);
}

TEST_CASE("checked arithmetic rejects narrowing conversions without modifying the result",
          "[core][arithmetic]") {
    std::uint32_t result = 42;

    CHECK_FALSE(yams::core::narrowOverflow(std::uint64_t{7}, result));
    CHECK(result == 7);

    result = 42;
    CHECK(yams::core::narrowOverflow(std::numeric_limits<std::uint64_t>::max(), result));
    CHECK(result == 42);

    CHECK(yams::core::narrowOverflow(std::int64_t{-1}, result));
    CHECK(result == 42);
}

} // namespace
