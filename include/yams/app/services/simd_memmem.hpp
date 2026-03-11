#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

// Reuse platform detection from simd_newline_scanner.hpp
#include <yams/app/services/simd_newline_scanner.hpp>

namespace yams {
namespace app {
namespace services {

/// Sentinel value returned when no match is found (same as std::string::npos).
inline constexpr size_t kMemmemNpos = static_cast<size_t>(-1);

/// Find the first occurrence of @p needle in @p haystack using SIMD-accelerated
/// two-byte filtering (Lemire technique).  Falls back to scalar
/// Boyer-Moore-Horspool for platforms without SIMD and to memchr for
/// single-byte needles.
///
/// Runtime CPU feature detection selects the fastest available path on x86
/// (AVX2 > SSE2 > scalar).  ARM NEON is used unconditionally on aarch64.
///
/// @return Byte offset of the first match, or @c kMemmemNpos if not found.
size_t simdMemmem(const char* haystack, size_t haystackLen, const char* needle, size_t needleLen);

/// Case-insensitive variant (ASCII tolower during comparison).
/// Needle is assumed to already be lowercase.
size_t simdMemmemCI(const char* haystack, size_t haystackLen, const char* needle, size_t needleLen);

/// Convenience overloads accepting string_view.
inline size_t simdMemmem(std::string_view haystack, std::string_view needle) {
    return simdMemmem(haystack.data(), haystack.size(), needle.data(), needle.size());
}

inline size_t simdMemmemCI(std::string_view haystack, std::string_view needle) {
    return simdMemmemCI(haystack.data(), haystack.size(), needle.data(), needle.size());
}

} // namespace services
} // namespace app
} // namespace yams
