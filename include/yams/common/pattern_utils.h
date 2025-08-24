#pragma once

#include <algorithm>
#include <cctype>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace yams::common {

// String-like concept (C++20): anything convertible to std::string_view
template <class T>
concept StringLike = requires(T&& t) { std::string_view{std::forward<T>(t)}; };

/**
 * constexpr, allocation-free wildcard match supporting:
 *  - '?' matches any single character
 *  - '*' matches any sequence of characters (including empty)
 *
 * Case-sensitive. Operates on std::string_view with an iterative algorithm
 * (no recursion, no backtracking explosion).
 */
[[nodiscard]] inline constexpr bool wildcard_match(std::string_view text,
                                                   std::string_view pattern) noexcept {
    size_t t = 0;                            // index in text
    size_t p = 0;                            // index in pattern
    size_t starPos = std::string_view::npos; // last position of '*' in pattern
    size_t matchPos = 0;                     // position in text corresponding to starPos+1

    const size_t tlen = text.size();
    const size_t plen = pattern.size();

    while (t < tlen) {
        if (p < plen && (pattern[p] == '?' || pattern[p] == text[t])) {
            // Single-char match: advance both
            ++t;
            ++p;
        } else if (p < plen && pattern[p] == '*') {
            // Record star position and advance pattern
            starPos = p++;
            matchPos = t;
        } else if (starPos != std::string_view::npos) {
            // Fallback: last star matches one more character
            p = starPos + 1;
            ++matchPos;
            t = matchPos;
        } else {
            // No star to fallback to and no direct match
            return false;
        }
    }

    // Consume remaining '*' in pattern
    while (p < plen && pattern[p] == '*') {
        ++p;
    }

    return p == plen;
}

/**
 * Returns true if the pattern contains any wildcard metacharacters.
 */
[[nodiscard]] inline constexpr bool has_wildcards(std::string_view pattern) noexcept {
    for (char c : pattern) {
        if (c == '*' || c == '?')
            return true;
    }
    return false;
}

/**
 * Matches the given text against any of the provided patterns.
 * Range can be any C++20 range whose elements are StringLike (e.g., vector<string>).
 */
template <std::ranges::input_range Range>
requires StringLike<std::ranges::range_value_t<Range>>
[[nodiscard]] inline bool matches_any(std::string_view text, const Range& patterns) noexcept {
    for (const auto& pat : patterns) {
        if (wildcard_match(text, std::string_view{pat})) {
            return true;
        }
    }
    return false;
}

/**
 * Trim helpers for std::string_view (no allocation).
 */
[[nodiscard]] inline std::string_view ltrim(std::string_view s) noexcept {
    size_t i = 0;
    while (i < s.size() && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r'))
        ++i;
    return s.substr(i);
}

[[nodiscard]] inline std::string_view rtrim(std::string_view s) noexcept {
    size_t i = s.size();
    while (i > 0 && (s[i - 1] == ' ' || s[i - 1] == '\t' || s[i - 1] == '\n' || s[i - 1] == '\r'))
        --i;
    return s.substr(0, i);
}

[[nodiscard]] inline std::string_view trim(std::string_view s) noexcept {
    return rtrim(ltrim(s));
}

/**
 * Split a comma-separated list of patterns into a vector of strings.
 * - Trims whitespace around each token
 * - Skips empty entries
 */
[[nodiscard]] inline std::vector<std::string> split_patterns(std::string_view csv) {
    std::vector<std::string> out;
    size_t start = 0;
    while (start <= csv.size()) {
        size_t pos = csv.find(',', start);
        std::string_view token =
            (pos == std::string_view::npos) ? csv.substr(start) : csv.substr(start, pos - start);
        token = trim(token);
        if (!token.empty()) {
            out.emplace_back(token);
        }
        if (pos == std::string_view::npos)
            break;
        start = pos + 1;
    }
    return out;
}

/**
 * Convert a glob-style pattern to an SQL LIKE pattern:
 *  - '*' -> '%'
 *  - '?' -> '_'
 * Note: This does not escape '%' or '_' that may already be present in the input.
 */
[[nodiscard]] inline std::string glob_to_sql_like(std::string_view glob) {
    std::string like;
    like.reserve(glob.size());
    for (char c : glob) {
        if (c == '*') {
            like.push_back('%');
        } else if (c == '?') {
            like.push_back('_');
        } else {
            like.push_back(c);
        }
    }
    return like;
}

} // namespace yams::common