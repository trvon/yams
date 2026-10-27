// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Included first, before any standard header, so the gates see only what the header
// itself pulls in. A gate that reads a __cpp_lib_* macro without <version> fails the
// agreement checks below.
#include <yams/core/cpp23_features.hpp>

#include <string>
#include <vector>
#include <version>
#include <catch2/catch_test_macros.hpp>

#if YAMS_HAS_EXPECTED
#include <expected>
#endif
#if YAMS_HAS_FLAT_MAP
#include <flat_map>
#endif
#if YAMS_HAS_MOVE_ONLY_FUNCTION
#include <functional>
#include <memory>
#endif
#if YAMS_HAS_RANGES
#include <ranges>
#endif
#if YAMS_HAS_REFLECTION
#include <meta>
#endif

using namespace yams::features;

namespace {

// Expected gate values, computed here from the compiler's own feature-test macros.
#if defined(__cpp_lib_constexpr_vector) && __cpp_lib_constexpr_vector >= 201907L
constexpr bool kStdConstexprVector = true;
#else
constexpr bool kStdConstexprVector = false;
#endif
#if defined(__cpp_lib_constexpr_string) && __cpp_lib_constexpr_string >= 201907L
constexpr bool kStdConstexprString = true;
#else
constexpr bool kStdConstexprString = false;
#endif
#if defined(__cpp_lib_ranges) && __cpp_lib_ranges >= 201911L
constexpr bool kStdRanges = true;
#else
constexpr bool kStdRanges = false;
#endif
#if defined(__cpp_lib_constexpr_algorithms) && __cpp_lib_constexpr_algorithms >= 201806L
constexpr bool kStdConstexprAlgorithms = true;
#else
constexpr bool kStdConstexprAlgorithms = false;
#endif
#if defined(__has_cpp_attribute) && __has_cpp_attribute(likely) >= 201803L
constexpr bool kStdLikely = true;
#else
constexpr bool kStdLikely = false;
#endif
#if defined(__cpp_lib_expected) && __cpp_lib_expected >= 202202L
constexpr bool kStdExpected = true;
#else
constexpr bool kStdExpected = false;
#endif
#if defined(__cpp_lib_string_contains) && __cpp_lib_string_contains >= 202011L
constexpr bool kStdStringContains = true;
#else
constexpr bool kStdStringContains = false;
#endif
#if defined(__cpp_lib_flat_map) && __cpp_lib_flat_map >= 202207L
constexpr bool kStdFlatMap = true;
#else
constexpr bool kStdFlatMap = false;
#endif
#if defined(__cpp_lib_move_only_function) && __cpp_lib_move_only_function >= 202110L
constexpr bool kStdMoveOnlyFunction = true;
#else
constexpr bool kStdMoveOnlyFunction = false;
#endif
#if defined(__cpp_impl_reflection) && __cpp_impl_reflection >= 202506L &&                          \
    defined(__cpp_lib_reflection) && __cpp_lib_reflection >= 202506L
constexpr bool kStdReflection = true;
#else
constexpr bool kStdReflection = false;
#endif

YAMS_CONSTEXPR_IF_SUPPORTED int answer() {
    return 42;
}

} // namespace

TEST_CASE("Feature gates agree with compiler feature-test macros", "[core][cpp23]") {
    STATIC_REQUIRE(FeatureInfo::has_constexpr_vector == kStdConstexprVector);
    STATIC_REQUIRE(FeatureInfo::has_constexpr_string == kStdConstexprString);
    STATIC_REQUIRE(FeatureInfo::has_constexpr_containers ==
                   (kStdConstexprVector && kStdConstexprString));
    STATIC_REQUIRE(FeatureInfo::has_ranges == kStdRanges);
    STATIC_REQUIRE(FeatureInfo::has_constexpr_algorithms == kStdConstexprAlgorithms);
    STATIC_REQUIRE(FeatureInfo::has_likely_unlikely == kStdLikely);
    STATIC_REQUIRE(FeatureInfo::has_expected == kStdExpected);
    STATIC_REQUIRE(FeatureInfo::has_string_contains == kStdStringContains);
    STATIC_REQUIRE(FeatureInfo::has_flat_map == kStdFlatMap);
    STATIC_REQUIRE(FeatureInfo::has_move_only_function == kStdMoveOnlyFunction);
    STATIC_REQUIRE(FeatureInfo::has_reflection == kStdReflection);
}

TEST_CASE("Gates match what every supported toolchain provides", "[core][cpp23]") {
    // C++20 floor: GCC 12+/libstdc++, Clang 18+, Apple Clang 17+, MSVC 19.4x+.
    STATIC_REQUIRE(YAMS_HAS_CONSTEXPR_CONTAINERS == 1);
    STATIC_REQUIRE(YAMS_HAS_RANGES == 1);
    STATIC_REQUIRE(YAMS_HAS_CONSTEXPR_ALGORITHMS == 1);
    STATIC_REQUIRE(YAMS_HAS_LIKELY_UNLIKELY == 1);
    STATIC_REQUIRE(YAMS_CPP_VERSION >= 202002L);
}

TEST_CASE("Enabled gates expose a usable feature", "[core][cpp23]") {
#if YAMS_HAS_EXPECTED
    std::expected<int, std::string> ok{7};
    std::expected<int, std::string> err{std::unexpected(std::string{"bad"})};
    CHECK(ok.value() == 7);
    CHECK_FALSE(err.has_value());
#endif
#if YAMS_HAS_FLAT_MAP
    std::flat_map<int, int> fm;
    fm[2] = 20;
    fm[1] = 10;
    CHECK(fm.begin()->first == 1);
#endif
#if YAMS_HAS_MOVE_ONLY_FUNCTION
    auto owned = std::make_unique<int>(5);
    std::move_only_function<int()> fn = [p = std::move(owned)] { return *p; };
    CHECK(fn() == 5);
#endif
#if YAMS_HAS_RANGES
    std::vector<int> v{1, 2, 3, 4};
    auto evens = v | std::views::filter([](int x) { return x % 2 == 0; });
    CHECK(std::ranges::distance(evens) == 2);
#endif
#if YAMS_HAS_REFLECTION
    STATIC_REQUIRE(std::meta::is_type(^^int));
#endif
#if YAMS_HAS_LIKELY_UNLIKELY
    int branch = 0;
    if (FeatureInfo::cpp_version > 0) [[likely]] {
        branch = 1;
    }
    CHECK(branch == 1);
#endif
}

TEST_CASE("Compiler info and summary are defined", "[core][cpp23]") {
    REQUIRE(FeatureInfo::compiler_name != nullptr);
    REQUIRE(FeatureInfo::compiler_version > 0);
    REQUIRE(FeatureInfo::cpp_version >= 202002L);

    const std::string summary = get_feature_summary();
    if (FeatureInfo::cpp_version > 202302L) {
        CHECK(summary == "C++26");
    } else if (FeatureInfo::cpp_version > 202002L) {
        CHECK(summary == "C++23");
    } else {
        CHECK(summary == "C++20");
    }
}

TEST_CASE("String helpers", "[core][cpp23]") {
    const std::string path = "docs/notes.md";
    CHECK(string_contains(path, "notes"));
    CHECK_FALSE(string_contains(path, "absent"));
    CHECK(string_starts_with(path, "docs/"));
    CHECK(string_ends_with(path, ".md"));
    CHECK_FALSE(string_ends_with(path, "docs/notes.md.bak"));
    STATIC_REQUIRE(string_contains(std::string_view{"abc"}, "b"));
}

TEST_CASE("YAMS_CONSTEXPR_IF_SUPPORTED expands", "[core][cpp23]") {
    REQUIRE(answer() == 42);
#if YAMS_HAS_CONSTEXPR_CONTAINERS
    STATIC_REQUIRE(answer() == 42);
#endif
}

#if YAMS_HAS_CONSTEXPR_CONTAINERS
TEST_CASE("Constexpr vector and string evaluate at compile time", "[core][cpp23][constexpr]") {
    constexpr auto vecSum = [] {
        std::vector<int> v{1, 2, 3};
        int sum = 0;
        for (int x : v) {
            sum += x;
        }
        return sum;
    }();
    constexpr auto strLen = [] {
        std::string s = "hello";
        s += " world";
        return s.size();
    }();
    STATIC_REQUIRE(vecSum == 6);
    STATIC_REQUIRE(strLen == 11);
}
#endif

TEST_CASE("Deprecation macro compiles", "[core][cpp23]") {
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#elif defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#elif defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4996)
#endif
    YAMS_CPP23_DEPRECATED("test message")
    auto old_function = []() { return 42; };
    REQUIRE(old_function() == 42);
#if defined(__clang__)
#pragma clang diagnostic pop
#elif defined(__GNUC__)
#pragma GCC diagnostic pop
#elif defined(_MSC_VER)
#pragma warning(pop)
#endif
}
