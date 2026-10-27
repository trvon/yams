#pragma once

/// @file grep_regex.hpp
/// @brief Thin regex wrapper: prefers RE2 when available, falls back to a bounded backtracker.
///
/// RE2 matches in linear time and is thread-safe. It rejects backreferences and lookaround.
/// Those patterns go to Boost.Regex, which gives up on a search after a bounded number of
/// states instead of running for exponential time.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#if YAMS_HAS_RE2
#include <re2/re2.h>
#endif

namespace yams::app::services {

/// Result of a single regex match (position + length within the searched text).
struct GrepRegexMatch {
    size_t position; ///< Byte offset of match start relative to the searched text
    size_t length;   ///< Length of the matched substring in bytes
};

/// Regex wrapper for grep.
///
/// Construction compiles the pattern once. The compiled object is safe to share
/// across threads for matching.
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
    /// (backreferences, lookaround), falls back to the bounded backtracker.
    /// When YAMS_HAS_RE2=0, always uses the backtracker.
    static std::optional<GrepRegex> compile(std::string_view pattern, bool ignoreCase);

    /// Why RE2 was not used. Empty when RE2 compiled the pattern.
    const std::string& fallbackReason() const noexcept { return fallbackReason_; }

    /// True if this instance uses RE2 (false = backtracker).
    bool usesRe2() const noexcept { return usesRe2_; }

    /// Find the next match in a raw char buffer starting at `offset`.
    /// Bytes before `offset` are context: `^` and `\b` see them.
    /// @param data   Pointer to the buffer.
    /// @param len    Length of the buffer in bytes.
    /// @param offset Byte offset to begin searching from.
    /// @return The match, or std::nullopt if no match or if the search exceeded its budget.
    std::optional<GrepRegexMatch> findNext(const char* data, size_t len, size_t offset = 0) const;

    /// Find the first match in a string.
    /// Convenience for column-offset extraction.
    std::optional<GrepRegexMatch> findFirst(std::string_view text) const;

    /// Count all non-overlapping matches in a string.
    size_t countMatches(const char* data, size_t len) const;

    /// Number of backtracker searches abandoned because they exceeded the state budget.
    /// Each one is reported as no match.
    std::uint64_t budgetExceededCount() const noexcept {
        return budgetExceeded_.load(std::memory_order_relaxed);
    }

    // Movable, not copyable (RE2 is move-only).
    GrepRegex(GrepRegex&&) noexcept;
    GrepRegex& operator=(GrepRegex&&) noexcept;
    ~GrepRegex();

    // Delete copy.
    GrepRegex(const GrepRegex&) = delete;
    GrepRegex& operator=(const GrepRegex&) = delete;

private:
    struct Backtracker;

    GrepRegex();

#if YAMS_HAS_RE2
    std::unique_ptr<re2::RE2> re2_;
#endif
    std::unique_ptr<Backtracker> backtracker_;
    bool usesRe2_ = false;
    std::string fallbackReason_;
    mutable std::atomic<std::uint64_t> budgetExceeded_{0};

    // Internal: translate common ECMAScript shortcuts for RE2 compatibility.
    static std::string translateForRe2(std::string_view pattern);
};

} // namespace yams::app::services
