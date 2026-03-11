#pragma once

/// @file grep_regex.hpp
/// @brief Thin regex wrapper: prefers RE2 when available, falls back to std::regex.
///
/// RE2 is 10-50x faster than std::regex for most patterns and is thread-safe
/// for matching. When RE2 cannot compile a pattern (backreferences, lookaheads),
/// this wrapper transparently falls back to std::regex.

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

#if YAMS_HAS_RE2
#include <re2/re2.h>
#endif

#include <regex>

namespace yams::app::services {

/// Result of a single regex match (position + length within the searched text).
struct GrepRegexMatch {
    size_t position; ///< Byte offset of match start relative to the searched text
    size_t length;   ///< Length of the matched substring in bytes
};

/// High-performance regex wrapper for grep.
///
/// Construction compiles the pattern once. The compiled object is safe to share
/// across threads for matching (RE2 is inherently thread-safe; std::regex is
/// safe for const operations).
///
/// Usage:
/// @code
///     auto re = GrepRegex::compile("foo.*bar", /*ignoreCase=*/true);
///     if (!re) { /* handle error */ }
///
///     // Count all matches in a line:
///     auto m = re->findNext(ptr, len, 0);
///     while (m) {
///         ++count;
///         m = re->findNext(ptr, len, m->position + m->length);
///     }
///
///     // Find first match (for column reporting):
///     auto first = re->findFirst("hello world");
/// @endcode
class GrepRegex {
public:
    /// Compile a regex pattern.
    /// @param pattern  The regex pattern string.
    /// @param ignoreCase  If true, match case-insensitively.
    /// @return A compiled GrepRegex, or std::nullopt on invalid pattern.
    ///
    /// When YAMS_HAS_RE2=1, tries RE2 first. If RE2 rejects the pattern
    /// (unsupported features like backreferences), falls back to std::regex.
    /// When YAMS_HAS_RE2=0, always uses std::regex.
    static std::optional<GrepRegex> compile(std::string_view pattern, bool ignoreCase);

    /// Return a human-readable error string if compilation failed partially
    /// (e.g., RE2 rejected pattern but std::regex succeeded).
    /// Empty string when the primary engine compiled successfully.
    const std::string& fallbackReason() const noexcept { return fallbackReason_; }

    /// True if this instance uses RE2 (false = std::regex).
    bool usesRe2() const noexcept { return usesRe2_; }

    /// Find the next match in a raw char buffer starting at `offset`.
    /// @param data   Pointer to the buffer.
    /// @param len    Length of the buffer in bytes.
    /// @param offset Byte offset to begin searching from.
    /// @return The match, or std::nullopt if no match.
    std::optional<GrepRegexMatch> findNext(const char* data, size_t len, size_t offset = 0) const;

    /// Find the first match in a string.
    /// Convenience for column-offset extraction.
    std::optional<GrepRegexMatch> findFirst(std::string_view text) const;

    /// Count all non-overlapping matches in a string.
    size_t countMatches(const char* data, size_t len) const;

    // Movable, not copyable (RE2 is move-only).
    GrepRegex(GrepRegex&&) noexcept;
    GrepRegex& operator=(GrepRegex&&) noexcept;
    ~GrepRegex();

    // Delete copy.
    GrepRegex(const GrepRegex&) = delete;
    GrepRegex& operator=(const GrepRegex&) = delete;

private:
    GrepRegex() = default;

#if YAMS_HAS_RE2
    std::unique_ptr<re2::RE2> re2_;
#endif
    std::optional<std::regex> stdRegex_;
    bool usesRe2_ = false;
    std::string fallbackReason_;

    // Internal: translate common ECMAScript shortcuts for RE2 compatibility.
    static std::string translateForRe2(std::string_view pattern);
};

} // namespace yams::app::services
