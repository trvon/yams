// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

/**
 * @file cpp23_features.hpp
 * @brief Feature-test gates for C++20/23/26 library and language features.
 *
 * Each YAMS_HAS_* gate is 0 or 1 and derives from the standard feature-test macro
 * (`__cpp_*`, `__cpp_lib_*`, `__has_cpp_attribute`). Nothing in the build system sets
 * them. Each gate is wrapped in `#ifndef`, so a translation unit can force a fallback
 * with `-DYAMS_HAS_<NAME>=0`.
 *
 * Language modes in use (setup.sh, setup.ps1, conan/profiles):
 * - Linux: C++23 when the compiler passes the setup.sh probe, else C++20.
 * - macOS, iOS, Android, Windows: C++20.
 * - No configuration builds C++26. A C++26-only gate is 0 in every CI lane, so
 *   only add one when its fallback is the code that actually ships.
 *
 * Version notes below are the first stdlib/compiler release listed by
 * https://en.cppreference.com/w/cpp/compiler_support. Where a CI toolchain differs
 * from the table, the note says so.
 */

// <version> defines every __cpp_lib_* macro. Without it the library gates below depend
// on which standard headers the including file happened to pull in first.
#include <version>

#include <chrono>
#include <cstdint>
#include <string_view>

// ============================================================================
// C++20 library and language features
// ============================================================================

/**
 * @def YAMS_HAS_CONSTEXPR_VECTOR
 * @brief constexpr std::vector (C++20, P1004R2; transient allocation only).
 *
 * libstdc++ 12, libc++ 15, MSVC STL 19.29, Apple Clang 14.0.3.
 */
#ifndef YAMS_HAS_CONSTEXPR_VECTOR
#if defined(__cpp_lib_constexpr_vector) && __cpp_lib_constexpr_vector >= 201907L
#define YAMS_HAS_CONSTEXPR_VECTOR 1
#else
#define YAMS_HAS_CONSTEXPR_VECTOR 0
#endif
#endif

/**
 * @def YAMS_HAS_CONSTEXPR_STRING
 * @brief constexpr std::string (C++20, P0980R1). Same versions as constexpr vector.
 */
#ifndef YAMS_HAS_CONSTEXPR_STRING
#if defined(__cpp_lib_constexpr_string) && __cpp_lib_constexpr_string >= 201907L
#define YAMS_HAS_CONSTEXPR_STRING 1
#else
#define YAMS_HAS_CONSTEXPR_STRING 0
#endif
#endif

/**
 * @def YAMS_HAS_CONSTEXPR_CONTAINERS
 * @brief Both constexpr std::vector and std::string are available.
 *
 * These are C++20 features. The gate is 1 in the C++20 lanes too (MSVC 19.5x,
 * Apple Clang), not only under C++23.
 */
#ifndef YAMS_HAS_CONSTEXPR_CONTAINERS
#if YAMS_HAS_CONSTEXPR_VECTOR && YAMS_HAS_CONSTEXPR_STRING
#define YAMS_HAS_CONSTEXPR_CONTAINERS 1
#else
#define YAMS_HAS_CONSTEXPR_CONTAINERS 0
#endif
#endif

/**
 * @def YAMS_CONSTEXPR_IF_SUPPORTED
 * @brief `constexpr` when YAMS_HAS_CONSTEXPR_CONTAINERS, otherwise `inline`.
 */
#if YAMS_HAS_CONSTEXPR_CONTAINERS
#define YAMS_CONSTEXPR_IF_SUPPORTED constexpr
#else
#define YAMS_CONSTEXPR_IF_SUPPORTED inline
#endif

/**
 * @def YAMS_HAS_RANGES
 * @brief <ranges> (C++20, P0896R4).
 *
 * libstdc++ 10, libc++ 15 (13 partial), MSVC STL 19.29, Apple Clang 14.0.3.
 */
#ifndef YAMS_HAS_RANGES
#if defined(__cpp_lib_ranges) && __cpp_lib_ranges >= 201911L
#define YAMS_HAS_RANGES 1
#else
#define YAMS_HAS_RANGES 0
#endif
#endif

/**
 * @def YAMS_HAS_CONSTEXPR_ALGORITHMS
 * @brief constexpr <algorithm> and <utility> (C++20, P0202R3).
 *
 * libstdc++ 10, libc++ 12, MSVC STL 19.26, Apple Clang 13.
 */
#ifndef YAMS_HAS_CONSTEXPR_ALGORITHMS
#if defined(__cpp_lib_constexpr_algorithms) && __cpp_lib_constexpr_algorithms >= 201806L
#define YAMS_HAS_CONSTEXPR_ALGORITHMS 1
#else
#define YAMS_HAS_CONSTEXPR_ALGORITHMS 0
#endif
#endif

/**
 * @def YAMS_HAS_LIKELY_UNLIKELY
 * @brief [[likely]] / [[unlikely]] (C++20, P0479R5).
 *
 * GCC 9, Clang 12, MSVC 19.26, Apple Clang 13. Detected with
 * `__has_cpp_attribute(likely)`; `__cpp_attributes` stays 200809L on every compiler
 * and cannot detect it.
 */
#ifndef YAMS_HAS_LIKELY_UNLIKELY
#if defined(__has_cpp_attribute)
#if __has_cpp_attribute(likely) >= 201803L && __has_cpp_attribute(unlikely) >= 201803L
#define YAMS_HAS_LIKELY_UNLIKELY 1
#else
#define YAMS_HAS_LIKELY_UNLIKELY 0
#endif
#else
#define YAMS_HAS_LIKELY_UNLIKELY 0
#endif
#endif

// ============================================================================
// C++23 library features
// ============================================================================

/**
 * @def YAMS_HAS_EXPECTED
 * @brief std::expected (C++23, P0323R12).
 *
 * libstdc++ 12, libc++ 16, MSVC STL 19.33 (/std:c++latest), Apple Clang 15.
 * libstdc++ also requires `__cpp_concepts >= 202002L`, so Clang 18 with libstdc++
 * (the Ubuntu 24.04 CI lane) reports 0 even under -std=c++23. Clang 19+ reports 1.
 */
#ifndef YAMS_HAS_EXPECTED
#if defined(__cpp_lib_expected) && __cpp_lib_expected >= 202202L
#define YAMS_HAS_EXPECTED 1
#else
#define YAMS_HAS_EXPECTED 0
#endif
#endif

/**
 * @def YAMS_HAS_STRING_CONTAINS
 * @brief std::string::contains (C++23, P1679R3).
 *
 * libstdc++ 11, libc++ 12, MSVC STL 19.30 (/std:c++latest), Apple Clang 13.
 */
#ifndef YAMS_HAS_STRING_CONTAINS
#if defined(__cpp_lib_string_contains) && __cpp_lib_string_contains >= 202011L
#define YAMS_HAS_STRING_CONTAINS 1
#else
#define YAMS_HAS_STRING_CONTAINS 0
#endif
#endif

/**
 * @def YAMS_HAS_FLAT_MAP
 * @brief std::flat_map (C++23, P0429R9).
 *
 * libstdc++ 15, libc++ 20, MSVC STL 19.51 (/std:c++latest). Not in libstdc++ 14,
 * so the Ubuntu 24.04 CI lane reports 0.
 */
#ifndef YAMS_HAS_FLAT_MAP
#if defined(__cpp_lib_flat_map) && __cpp_lib_flat_map >= 202207L
#define YAMS_HAS_FLAT_MAP 1
#else
#define YAMS_HAS_FLAT_MAP 0
#endif
#endif

/**
 * @def YAMS_HAS_MOVE_ONLY_FUNCTION
 * @brief std::move_only_function (C++23, P0288R9).
 *
 * libstdc++ 12, MSVC STL 19.32 (/std:c++latest). libc++ does not ship it
 * (absent through libc++ 23), so macOS and iOS report 0.
 */
#ifndef YAMS_HAS_MOVE_ONLY_FUNCTION
#if defined(__cpp_lib_move_only_function) && __cpp_lib_move_only_function >= 202110L
#define YAMS_HAS_MOVE_ONLY_FUNCTION 1
#else
#define YAMS_HAS_MOVE_ONLY_FUNCTION 0
#endif
#endif

// ============================================================================
// C++26 features
// ============================================================================

/**
 * @def YAMS_HAS_REFLECTION
 * @brief Static reflection (C++26, P2996R13).
 *
 * Requires both `__cpp_impl_reflection` and `__cpp_lib_reflection` >= 202506L.
 * GCC 16 implements it under `-std=c++26 -freflection`. No Clang, Apple Clang or
 * MSVC release ships it. Nothing in YAMS uses this gate yet.
 */
#ifndef YAMS_HAS_REFLECTION
#if defined(__cpp_impl_reflection) && __cpp_impl_reflection >= 202506L &&                          \
    defined(__cpp_lib_reflection) && __cpp_lib_reflection >= 202506L
#define YAMS_HAS_REFLECTION 1
#else
#define YAMS_HAS_REFLECTION 0
#endif
#endif

// ============================================================================
// Compiler information
// ============================================================================

/**
 * @def YAMS_COMPILER_NAME
 * @brief Compiler family. Apple Clang reports "Clang" with Apple's version number.
 */
#if defined(__clang__)
#define YAMS_COMPILER_NAME "Clang"
#define YAMS_COMPILER_VERSION __clang_major__
#elif defined(__GNUC__)
#define YAMS_COMPILER_NAME "GCC"
#define YAMS_COMPILER_VERSION __GNUC__
#elif defined(_MSC_VER)
#define YAMS_COMPILER_NAME "MSVC"
#define YAMS_COMPILER_VERSION _MSC_VER
#else
#define YAMS_COMPILER_NAME "Unknown"
#define YAMS_COMPILER_VERSION 0
#endif

/**
 * @def YAMS_CPP_VERSION
 * @brief Language mode: 202002L C++20, 202302L C++23, > 202302L C++26 draft.
 *
 * Uses `_MSVC_LANG` when defined, because MSVC leaves `__cplusplus` at 199711L
 * without /Zc:__cplusplus.
 */
#if defined(_MSVC_LANG)
#define YAMS_CPP_VERSION _MSVC_LANG
#else
#define YAMS_CPP_VERSION __cplusplus
#endif

namespace yams {
namespace features {

/// Convert a Unix timestamp (seconds) to sys_seconds.
inline std::chrono::sys_seconds fromUnixTime(int64_t ts) {
    return std::chrono::sys_seconds{std::chrono::seconds{ts}};
}

/// Gate values as constexpr bools, for diagnostics and tests.
struct FeatureInfo {
    static constexpr bool has_constexpr_containers = YAMS_HAS_CONSTEXPR_CONTAINERS;
    static constexpr bool has_constexpr_vector = YAMS_HAS_CONSTEXPR_VECTOR;
    static constexpr bool has_constexpr_string = YAMS_HAS_CONSTEXPR_STRING;
    static constexpr bool has_expected = YAMS_HAS_EXPECTED;
    static constexpr bool has_string_contains = YAMS_HAS_STRING_CONTAINS;
    static constexpr bool has_ranges = YAMS_HAS_RANGES;
    static constexpr bool has_constexpr_algorithms = YAMS_HAS_CONSTEXPR_ALGORITHMS;
    static constexpr bool has_likely_unlikely = YAMS_HAS_LIKELY_UNLIKELY;
    static constexpr bool has_flat_map = YAMS_HAS_FLAT_MAP;
    static constexpr bool has_move_only_function = YAMS_HAS_MOVE_ONLY_FUNCTION;
    static constexpr bool has_reflection = YAMS_HAS_REFLECTION;
    static constexpr long cpp_version = YAMS_CPP_VERSION;

    static constexpr const char* compiler_name = YAMS_COMPILER_NAME;
    static constexpr int compiler_version = YAMS_COMPILER_VERSION;
};

/// Language mode the translation unit was compiled in: "C++20", "C++23" or "C++26".
inline const char* get_feature_summary() {
    if (FeatureInfo::cpp_version > 202302L) {
        return "C++26";
    }
    if (FeatureInfo::cpp_version > 202002L) {
        return "C++23";
    }
    return "C++20";
}

// ============================================================================
// Compatibility helpers
// ============================================================================

/// `str.contains(substr)` with a string_view fallback before C++23.
#if YAMS_HAS_STRING_CONTAINS
template <typename StringT, typename SubstrT>
constexpr bool string_contains(const StringT& str, const SubstrT& substr) {
    return std::string_view(str).contains(std::string_view(substr));
}
#else
template <typename StringT, typename SubstrT>
constexpr bool string_contains(const StringT& str, const SubstrT& substr) {
    return std::string_view(str).find(std::string_view(substr)) != std::string_view::npos;
}
#endif

/// `starts_with` for any type convertible to std::string_view.
template <typename StringT, typename PrefixT>
constexpr bool string_starts_with(const StringT& str, const PrefixT& prefix) {
    std::string_view sv(str);
    std::string_view pv(prefix);
#if defined(__cpp_lib_starts_ends_with) && __cpp_lib_starts_ends_with >= 201711L
    return sv.starts_with(pv);
#else
    return sv.size() >= pv.size() && sv.substr(0, pv.size()) == pv;
#endif
}

/// `ends_with` for any type convertible to std::string_view.
template <typename StringT, typename SuffixT>
constexpr bool string_ends_with(const StringT& str, const SuffixT& suffix) {
    std::string_view sv(str);
    std::string_view suf(suffix);
#if defined(__cpp_lib_starts_ends_with) && __cpp_lib_starts_ends_with >= 201711L
    return sv.ends_with(suf);
#else
    return sv.size() >= suf.size() && sv.substr(sv.size() - suf.size()) == suf;
#endif
}

} // namespace features
} // namespace yams

/**
 * @def YAMS_CPP23_DEPRECATED
 * @brief `[[deprecated]]` when YAMS_HAS_CONSTEXPR_CONTAINERS, otherwise empty.
 */
#if YAMS_HAS_CONSTEXPR_CONTAINERS
#define YAMS_CPP23_DEPRECATED(msg) [[deprecated("C++23 available: " msg)]]
#else
#define YAMS_CPP23_DEPRECATED(msg)
#endif

/*
 * Rules for new gates
 *
 * - Test the feature-test macro, never __cplusplus.
 * - Add a gate only when YAMS code uses it, and give it a fallback that compiles in
 *   every CI lane (Linux C++23, macOS C++20, Windows MSVC C++20).
 * - Add a FeatureInfo entry and a check in tests/unit/core/cpp23_features_test.cpp.
 * - Result<T> (include/yams/core/types.h) stays the error type. It is not a
 *   std::expected stand-in: it has no monadic API, Result<void> treats
 *   ErrorCode::Success as the value state, and a default-constructed Result<T> is an
 *   error. Do not gate it on YAMS_HAS_EXPECTED.
 */
