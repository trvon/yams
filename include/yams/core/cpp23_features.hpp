// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: Apache-2.0

#pragma once

/**
 * @file cpp23_features.hpp
 * @brief C++23 and C++26 feature detection and compatibility macros
 *
 * This header provides feature detection for modern C++ standards, allowing
 * the codebase to use advanced features when available while maintaining
 * backward compatibility with C++20 compilers.
 *
 * Feature Flags (set by Meson build system):
 * - YAMS_HAS_CONSTEXPR_VECTOR: constexpr std::vector support
 * - YAMS_HAS_CONSTEXPR_STRING: constexpr std::string support
 * - YAMS_HAS_CONSTEXPR_CONTAINERS: all constexpr containers (vector + string)
 *
 * Usage Example:
 * @code
 * #include <yams/core/cpp23_features.hpp>
 *
 * namespace yams::config {
 *
 * #if YAMS_HAS_CONSTEXPR_CONTAINERS
 *     // C++23 path: compile-time initialization
 *     constexpr auto make_magic_numbers() {
 *         std::vector<MagicNumber> numbers;
 *         numbers.push_back({...});
 *         return numbers;
 *     }
 *     inline constexpr auto MAGIC_NUMBERS = make_magic_numbers();
 * #else
 *     // C++20 fallback: runtime initialization
 *     inline const auto& get_magic_numbers() {
 *         static const auto numbers = load_from_json();
 *         return numbers;
 *     }
 * #endif
 *
 * } // namespace yams::config
 * @endcode
 */

// ============================================================================
// C++23 Feature Detection
// ============================================================================

/**
 * @def YAMS_HAS_CONSTEXPR_VECTOR
 * @brief Indicates support for constexpr std::vector
 *
 * Requires:
 * - GCC 13+
 * - Clang 16+
 * - MSVC 19.33+ (VS 2022 17.3+)
 * - Apple Clang 16+
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
 * @brief Indicates support for constexpr std::string
 *
 * Requires same compiler versions as YAMS_HAS_CONSTEXPR_VECTOR
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
 * @brief Unified flag for all C++23 constexpr container features
 *
 * Set by build system based on actual compiler capability tests.
 * When enabled, code can use constexpr std::vector, std::string, etc.
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
 * @brief Expands to constexpr when C++23 containers are available, inline otherwise
 *
 * Use this for functions that can be constexpr with C++23 but need to be
 * inline const with C++20.
 *
 * Example:
 * @code
 * YAMS_CONSTEXPR_IF_SUPPORTED auto make_config() {
 *     std::vector<Config> configs;
 *     // ... build config
 *     return configs;
 * }
 * @endcode
 */
#if YAMS_HAS_CONSTEXPR_CONTAINERS
#define YAMS_CONSTEXPR_IF_SUPPORTED constexpr
#else
#define YAMS_CONSTEXPR_IF_SUPPORTED inline
#endif

/**
 * @def YAMS_HAS_EXPECTED
 * @brief Indicates support for std::expected (C++23)
 *
 * std::expected<T, E> provides monadic error handling without exceptions.
 * Requires:
 * - GCC 12+
 * - Clang 16+
 * - MSVC 19.33+
 * - Apple Clang 15+
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
 * @brief Indicates support for std::string::contains() (C++23)
 *
 * Requires:
 * - GCC 12+
 * - Clang 12+
 * - MSVC 19.30+
 * - Apple Clang 13+
 */
#ifndef YAMS_HAS_STRING_CONTAINS
#if defined(__cpp_lib_string_contains) && __cpp_lib_string_contains >= 202011L
#define YAMS_HAS_STRING_CONTAINS 1
#else
#define YAMS_HAS_STRING_CONTAINS 0
#endif
#endif

/**
 * @def YAMS_HAS_RANGES
 * @brief Indicates support for C++20/23 ranges and views
 *
 * Requires:
 * - GCC 10+ (partial), 12+ (complete)
 * - Clang 13+ (partial), 15+ (complete)
 * - MSVC 19.30+
 * - Apple Clang 13+
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
 * @brief Indicates support for constexpr algorithms (C++20)
 *
 * Requires:
 * - GCC 10+
 * - Clang 10+
 * - MSVC 19.26+
 * - Apple Clang 12+
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
 * @brief Indicates support for [[likely]] and [[unlikely]] attributes (C++20)
 *
 * Requires:
 * - GCC 7+
 * - Clang 9+
 * - MSVC 19.20+ (VS 2019 16.3+)
 * - Apple Clang 11+
 */
#ifndef YAMS_HAS_LIKELY_UNLIKELY
#if defined(__cpp_attributes) && __cpp_attributes >= 201803L
#define YAMS_HAS_LIKELY_UNLIKELY 1
#else
#define YAMS_HAS_LIKELY_UNLIKELY 0
#endif
#endif

/**
 * @def YAMS_HAS_FLAT_MAP
 * @brief Indicates support for std::flat_map (C++23)
 *
 * Requires:
 * - GCC 12+
 * - Clang 17+
 * - MSVC 19.33+
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
 * @brief Indicates support for std::move_only_function (C++23)
 *
 * Requires:
 * - GCC 12+
 * - Clang 17+
 * - MSVC 19.33+
 */
#ifndef YAMS_HAS_MOVE_ONLY_FUNCTION
#if defined(__cpp_lib_move_only_function) && __cpp_lib_move_only_function >= 202110L
#define YAMS_HAS_MOVE_ONLY_FUNCTION 1
#else
#define YAMS_HAS_MOVE_ONLY_FUNCTION 0
#endif
#endif

// ============================================================================
// C++26 Feature Detection (Experimental)
// ============================================================================

/**
 * @def YAMS_HAS_PROFILES
 * @brief Indicates C++26 Profiles support
 *
 * Profiles provide compile-time safety guarantees (bounds, type, lifetime).
 * Expected availability:
 * - GCC 14+ (experimental, 2025)
 * - Clang 18+ (experimental, 2025)
 */
#ifndef YAMS_HAS_PROFILES
#if defined(__cpp_profiles) && __cpp_profiles >= 202400L
#define YAMS_HAS_PROFILES 1
#else
#define YAMS_HAS_PROFILES 0
#endif
#endif

/**
 * @def YAMS_PROFILE(...)
 * @brief Apply a C++26 profile annotation
 *
 * Usage:
 * @code
 * namespace storage YAMS_PROFILE(bounds) {
 *     // Compiler enforces bounds safety
 * }
 *
 * YAMS_PROFILE(lifetime)
 * Task<Result<void>> async_operation() { ... }
 * @endcode
 */
#if YAMS_HAS_PROFILES
#define YAMS_PROFILE(...) [[profile:__VA_ARGS__]]
#else
#define YAMS_PROFILE(...)
#endif

/**
 * @def YAMS_HAS_REFLECTION
 * @brief Indicates C++26 Reflection support
 *
 * Reflection enables compile-time introspection and code generation.
 * Expected availability:
 * - Clang 18+ (experimental, late 2025)
 * - GCC 15+ (2026)
 */
#ifndef YAMS_HAS_REFLECTION
#if defined(__cpp_reflection) && __cpp_reflection >= 202400L
#define YAMS_HAS_REFLECTION 1
#else
#define YAMS_HAS_REFLECTION 0
#endif
#endif

// ============================================================================
// Compiler Information
// ============================================================================

/**
 * @def YAMS_COMPILER_NAME
 * @brief Human-readable compiler name
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
 * @brief C++ standard version (202002L = C++20, 202302L = C++23)
 */
#define YAMS_CPP_VERSION __cplusplus

// ============================================================================
// Feature Summary (for diagnostics)
// ============================================================================

// Required for compatibility helpers
#include <string_view>

namespace yams {
namespace features {

/**
 * @brief Feature detection summary for diagnostics
 */
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
    static constexpr bool has_profiles = YAMS_HAS_PROFILES;
    static constexpr bool has_reflection = YAMS_HAS_REFLECTION;
    static constexpr long cpp_version = YAMS_CPP_VERSION;

    static constexpr const char* compiler_name = YAMS_COMPILER_NAME;
    static constexpr int compiler_version = YAMS_COMPILER_VERSION;
};

/**
 * @brief Get a human-readable feature summary
 */
inline const char* get_feature_summary() {
    if (FeatureInfo::has_constexpr_containers) {
        return "C++23 (constexpr containers enabled)";
    } else {
        return "C++20 (constexpr containers unavailable)";
    }
}

// ============================================================================
// Compatibility Helpers
// ============================================================================

/**
 * @brief String contains helper (C++23 compatibility)
 *
 * When std::string::contains() is available, this is a passthrough.
 * Otherwise, provides fallback using find().
 */
#if YAMS_HAS_STRING_CONTAINS
template <typename StringT, typename SubstrT>
inline constexpr bool string_contains(const StringT& str, const SubstrT& substr) {
    return str.contains(substr);
}
#else
template <typename StringT, typename SubstrT>
inline bool string_contains(const StringT& str, const SubstrT& substr) {
    return std::string_view(str).find(std::string_view(substr)) != std::string_view::npos;
}
#endif

/**
 * @brief String starts_with helper (C++20, but ensure availability)
 */
template <typename StringT, typename PrefixT>
inline bool string_starts_with(const StringT& str, const PrefixT& prefix) {
    std::string_view sv(str);
    std::string_view pv(prefix);
#if defined(__cpp_lib_starts_ends_with) && __cpp_lib_starts_ends_with >= 201711L
    return sv.starts_with(pv);
#else
    return sv.size() >= pv.size() && sv.substr(0, pv.size()) == pv;
#endif
}

/**
 * @brief String ends_with helper (C++20, but ensure availability)
 */
template <typename StringT, typename SuffixT>
inline bool string_ends_with(const StringT& str, const SuffixT& suffix) {
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

// ============================================================================
// Deprecation and Future Compatibility
// ============================================================================

/**
 * @def YAMS_CPP23_DEPRECATED
 * @brief Mark features that will be replaced by C++23 equivalents
 *
 * Use to mark code that should be replaced when C++23 becomes mandatory.
 */
#if YAMS_HAS_CONSTEXPR_CONTAINERS
#define YAMS_CPP23_DEPRECATED(msg) [[deprecated("C++23 available: " msg)]]
#else
#define YAMS_CPP23_DEPRECATED(msg)
#endif

// ============================================================================
// Usage Guidelines
// ============================================================================

/**
 * Guidelines for using C++23 features in YAMS:
 *
 * 1. Always use feature detection macros, never raw __cplusplus checks
 * 2. Provide C++20 fallback for all C++23 features
 * 3. Test on both C++20 and C++23 build configurations
 * 4. Document which features require C++23
 * 5. Use YAMS_CONSTEXPR_IF_SUPPORTED for conditional constexpr
 * 6. Use yams::features helpers for cross-version string operations
 *
 * Example Pattern - Compile-time data:
 * @code
 * namespace yams::config {
 *
 * #if YAMS_HAS_CONSTEXPR_CONTAINERS
 *     // C++23: Compile-time initialization
 *     constexpr auto make_data() {
 *         std::vector<Item> data;
 *         // populate at compile time
 *         return data;
 *     }
 *     inline constexpr auto DATA = make_data();
 *
 *     constexpr auto get_data() { return DATA; }
 * #else
 *     // C++20: Runtime initialization (current approach)
 *     inline const auto& get_data() {
 *         static const auto data = load_from_resource();
 *         return data;
 *     }
 * #endif
 *
 * } // namespace yams::config
 * @endcode
 *
 * Example Pattern - String operations:
 * @code
 * #include <yams/core/cpp23_features.hpp>
 *
 * using yams::features::string_contains;
 * using yams::features::string_starts_with;
 *
 * bool is_text_file(const std::string& path) {
 *     // Works on both C++20 and C++23
 *     return string_ends_with(path, ".txt") ||
 *            string_ends_with(path, ".md");
 * }
 *
 * bool has_keyword(const std::string& text, const std::string& keyword) {
 *     return string_contains(text, keyword);
 * }
 * @endcode
 *
 * Example Pattern - Future std::expected migration:
 * @code
 * // Current: Use Result<T> (custom type)
 * Result<Config> loadConfig(const std::string& path);
 *
 * // Future (when YAMS_HAS_EXPECTED): Migrate to std::expected
 * // #if YAMS_HAS_EXPECTED
 * // std::expected<Config, Error> loadConfig(const std::string& path);
 * // #else
 * // Result<Config> loadConfig(const std::string& path);
 * // #endif
 *
 * // Note: Result<T> and std::expected<T, E> have similar monadic APIs,
 * // making migration straightforward when std::expected becomes available.
 * @endcode
 *
 * Example Pattern - Ranges and views:
 * @code
 * #if YAMS_HAS_RANGES
 *     #include <ranges>
 *
 *     auto process_lines(const std::vector<std::string>& lines) {
 *         return lines
 *             | std::views::filter([](auto& s) { return !s.empty(); })
 *             | std::views::transform([](auto& s) { return trim(s); });
 *     }
 * #else
 *     // Fallback: manual iteration
 *     std::vector<std::string> process_lines(const std::vector<std::string>& lines) {
 *         std::vector<std::string> result;
 *         for (const auto& line : lines) {
 *             if (!line.empty()) {
 *                 result.push_back(trim(line));
 *             }
 *         }
 *         return result;
 *     }
 * #endif
 * @endcode
 */
