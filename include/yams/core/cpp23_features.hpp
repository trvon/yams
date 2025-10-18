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

namespace yams::features {

/**
 * @brief Feature detection summary for diagnostics
 */
struct FeatureInfo {
    static constexpr bool has_constexpr_containers = YAMS_HAS_CONSTEXPR_CONTAINERS;
    static constexpr bool has_constexpr_vector = YAMS_HAS_CONSTEXPR_VECTOR;
    static constexpr bool has_constexpr_string = YAMS_HAS_CONSTEXPR_STRING;
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
    if constexpr (FeatureInfo::has_constexpr_containers) {
        return "C++23 (constexpr containers enabled)";
    } else {
        return "C++20 (constexpr containers unavailable)";
    }
}

} // namespace yams::features

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
 *
 * Example Pattern:
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
 */
