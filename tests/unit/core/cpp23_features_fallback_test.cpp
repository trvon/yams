// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Forces every overridable gate to 0 so the fallback paths compile in all CI lanes,
// including lanes where the feature exists. Built as its own executable: mixing these
// definitions with the default ones in one binary would violate the ODR.
#define YAMS_HAS_CONSTEXPR_VECTOR 0
#define YAMS_HAS_CONSTEXPR_STRING 0
#define YAMS_HAS_CONSTEXPR_CONTAINERS 0
#define YAMS_HAS_RANGES 0
#define YAMS_HAS_CONSTEXPR_ALGORITHMS 0
#define YAMS_HAS_LIKELY_UNLIKELY 0
#define YAMS_HAS_EXPECTED 0
#define YAMS_HAS_STRING_CONTAINS 0
#define YAMS_HAS_FLAT_MAP 0
#define YAMS_HAS_MOVE_ONLY_FUNCTION 0
#define YAMS_HAS_REFLECTION 0

#include <yams/core/cpp23_features.hpp>
#include <yams/core/magic_numbers.hpp>

#include <array>
#include <cstdint>
#include <string>
#include <catch2/catch_test_macros.hpp>

using namespace yams::features;

namespace {
YAMS_CONSTEXPR_IF_SUPPORTED int three() {
    return 3;
}
} // namespace

TEST_CASE("Forced-off gates report 0", "[core][cpp23][fallback]") {
    STATIC_REQUIRE_FALSE(FeatureInfo::has_constexpr_containers);
    STATIC_REQUIRE_FALSE(FeatureInfo::has_ranges);
    STATIC_REQUIRE_FALSE(FeatureInfo::has_likely_unlikely);
    STATIC_REQUIRE_FALSE(FeatureInfo::has_expected);
    STATIC_REQUIRE_FALSE(FeatureInfo::has_string_contains);
    STATIC_REQUIRE_FALSE(FeatureInfo::has_flat_map);
    STATIC_REQUIRE_FALSE(FeatureInfo::has_move_only_function);
    STATIC_REQUIRE_FALSE(FeatureInfo::has_reflection);
}

TEST_CASE("string_contains fallback", "[core][cpp23][fallback]") {
    const std::string text = "hybrid search";
    CHECK(string_contains(text, "search"));
    CHECK_FALSE(string_contains(text, "vector"));
    STATIC_REQUIRE(string_contains(std::string_view{"abc"}, "c"));
}

TEST_CASE("YAMS_CONSTEXPR_IF_SUPPORTED falls back to inline", "[core][cpp23][fallback]") {
    REQUIRE(three() == 3);
    YAMS_CPP23_DEPRECATED("expands to nothing") auto value = 0;
    REQUIRE(value == 0);
}

TEST_CASE("magic_numbers runtime fallback compiles", "[core][cpp23][fallback]") {
    using yams::magic::MagicDatabaseInfo;
    STATIC_REQUIRE(std::string_view{MagicDatabaseInfo::mode()} == "runtime (JSON)");
    const std::array<std::uint8_t, 4> png{0x89, 'P', 'N', 'G'};
    // The runtime table is empty until FileTypeDetector loads JSON.
    CHECK(yams::magic::detect_mime_type(png) == "application/octet-stream");
}
