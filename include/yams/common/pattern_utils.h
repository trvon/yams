#pragma once

#include <algorithm>
#include <cctype>
#include <optional>
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

/** Normalize a filesystem path for glob matching.
 * - Converts '\\' to '/'
 * - Collapses consecutive '/'
 * - Removes trailing '/' (except when the path is just "/")
 */
[[nodiscard]] inline std::string normalize_path(std::string_view path) {
    std::string out;
    out.reserve(path.size());
    char prev = 0;
    for (char c : path) {
        char d = (c == '\\') ? '/' : c;
        if (d == '/' && prev == '/') {
            continue; // collapse
        }
        out.push_back(d);
        prev = d;
    }
    // strip trailing '/'
    if (out.size() > 1 && out.back() == '/')
        out.pop_back();
    return out;
}

/** Expand simple brace sets like "{cpp,hpp}" or nested like "{a,{b,c}}".
 * Returns a list of expanded patterns. If no braces are present, returns the input pattern.
 */
inline void brace_expand_impl(std::string_view pat, std::vector<std::string>& out) {
    size_t open = std::string_view::npos;
    size_t level = 0;
    for (size_t i = 0; i < pat.size(); ++i) {
        if (pat[i] == '{') {
            if (level == 0)
                open = i;
            ++level;
        } else if (pat[i] == '}') {
            if (level == 0)
                break; // unmatched; treat literally
            --level;
            if (level == 0 && open != std::string_view::npos) {
                // Split inner by commas at brace level 0 (not handling escapes).
                std::string prefix{pat.substr(0, open)};
                std::string_view inner = pat.substr(open + 1, i - open - 1);
                std::string suffix{pat.substr(i + 1)};

                size_t start = 0;
                size_t innerLevel = 0;
                for (size_t pos = 0; pos <= inner.size(); ++pos) {
                    const bool atEnd = (pos == inner.size());
                    char ch = '\0';
                    if (!atEnd) {
                        ch = inner[pos];
                        if (ch == '{') {
                            ++innerLevel;
                        } else if (ch == '}') {
                            if (innerLevel > 0)
                                --innerLevel;
                        }
                    }

                    if (atEnd || (ch == ',' && innerLevel == 0)) {
                        std::string_view tok = inner.substr(start, pos - start);
                        std::string merged;
                        merged.reserve(prefix.size() + tok.size() + suffix.size());
                        merged.append(prefix);
                        merged.append(tok);
                        merged.append(suffix);
                        brace_expand_impl(merged, out);
                        start = pos + 1;
                    }
                }
                return;
            }
        }
    }
    // no (more) braces
    out.emplace_back(pat);
}

[[nodiscard]] inline std::vector<std::string> brace_expand(std::string_view pattern) {
    std::vector<std::string> out;
    brace_expand_impl(pattern, out);
    return out;
}

/** Match a single segment (no '/') supporting *, ?, and character classes [] and [!]. */
[[nodiscard]] inline bool match_segment(std::string_view text, std::string_view pat) {
    size_t ti = 0, pi = 0;
    size_t tlen = text.size(), plen = pat.size();
    size_t star = std::string_view::npos, backtrack = 0;
    auto match_charclass = [&](char c, size_t& pidx) {
        bool neg = false;
        if (pidx < plen && pat[pidx] == '!') {
            neg = true;
            ++pidx;
        }
        bool ok = false;
        while (pidx < plen && pat[pidx] != ']') {
            if (pidx + 2 < plen && pat[pidx + 1] == '-') {
                char a = pat[pidx];
                char b = pat[pidx + 2];
                if (a <= c && c <= b)
                    ok = true;
                pidx += 3;
            } else {
                if (pat[pidx] == c)
                    ok = true;
                ++pidx;
            }
        }
        if (pidx < plen && pat[pidx] == ']')
            ++pidx; // consume ']'
        return neg ? !ok : ok;
    };

    while (ti < tlen) {
        if (pi < plen) {
            char pc = pat[pi];
            if (pc == '?') {
                ++ti;
                ++pi;
                continue;
            }
            if (pc == '*') {
                star = pi++;
                backtrack = ti;
                continue;
            }
            if (pc == '[') {
                ++pi; // consume '['
                if (ti < tlen && match_charclass(text[ti], pi)) {
                    ++ti;
                    continue;
                }
            } else if (pc == text[ti]) {
                ++ti;
                ++pi;
                continue;
            }
        }
        if (star != std::string_view::npos) {
            pi = star + 1;
            ++backtrack;
            ti = backtrack;
            continue;
        }
        return false;
    }
    while (pi < plen && pat[pi] == '*')
        ++pi;
    return pi == plen;
}

/** Glob match for normalized paths supporting ** across segments and brace sets. */
[[nodiscard]] inline bool glob_match_path(std::string_view path, std::string_view pattern) {
    // Anchor handling: leading '^' anchors at beginning
    bool anchored = false;
    if (!pattern.empty() && pattern.front() == '^') {
        anchored = true;
        pattern.remove_prefix(1);
    }

    std::string npath = normalize_path(path);
    std::string npat = normalize_path(pattern);

    // Brace expand into alternatives
    auto expanded = brace_expand(npat);
    for (const auto& patAlt : expanded) {
        // Split into segments
        std::vector<std::string_view> pseg;
        size_t start = 0;
        std::string_view pv{patAlt};
        while (start <= pv.size()) {
            size_t pos = pv.find('/', start);
            pseg.emplace_back(
                pv.substr(start, pos == std::string_view::npos ? pv.size() - start : pos - start));
            if (pos == std::string_view::npos) {
                break;
            }
            start = pos + 1;
        }
        // Path segments
        std::vector<std::string_view> tseg;
        std::string_view tv{npath};
        start = 0;
        while (start <= tv.size()) {
            size_t pos = tv.find('/', start);
            tseg.emplace_back(
                tv.substr(start, pos == std::string_view::npos ? tv.size() - start : pos - start));
            if (pos == std::string_view::npos) {
                break;
            }
            start = pos + 1;
        }

        // DP-style match with '**'
        size_t i = 0, j = 0; // i: tseg, j: pseg
        size_t starI = std::string_view::npos, starJ = std::string_view::npos;
        // If anchored, pattern must match from beginning; else allow leading skip via implicit
        // '**'. Insert an implicit leading '**' so the matcher can backtrack across multiple
        // possible starting positions (e.g., matching "b/c" against "a/b/x/b/c").
        static constexpr std::string_view kStarStar = "**";
        if (!anchored && (pseg.empty() || pseg[0] != kStarStar)) {
            pseg.insert(pseg.begin(), kStarStar);
        }

        while (i < tseg.size()) {
            if (j < pseg.size()) {
                if (pseg[j] == "**") {
                    starI = i;
                    starJ = j++;
                    continue;
                }
                if (match_segment(tseg[i], pseg[j])) {
                    ++i;
                    ++j;
                    continue;
                }
            }
            if (starJ != std::string_view::npos) {
                i = ++starI;
                j = starJ + 1;
                continue;
            }
            goto next_alt;
        }
        while (j < pseg.size() && pseg[j] == "**")
            ++j;
        if (j == pseg.size())
            return true;
    next_alt:;
    }
    return false;
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

/** Path-aware any-match using glob_match_path on each pattern. */
template <std::ranges::input_range Range>
requires StringLike<std::ranges::range_value_t<Range>>
[[nodiscard]] inline bool matches_any_path(std::string_view path, const Range& patterns) {
    for (const auto& pat : patterns) {
        if (glob_match_path(path, std::string_view{pat}))
            return true;
    }
    return false;
}

/**
 * Trim helpers for std::string_view (no allocation).
 */
[[nodiscard]] constexpr std::string_view ltrim(std::string_view s) noexcept {
    size_t i = 0;
    while (i < s.size() && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r'))
        ++i;
    return s.substr(i);
}

[[nodiscard]] constexpr std::string_view rtrim(std::string_view s) noexcept {
    size_t i = s.size();
    while (i > 0 && (s[i - 1] == ' ' || s[i - 1] == '\t' || s[i - 1] == '\n' || s[i - 1] == '\r'))
        --i;
    return s.substr(0, i);
}

[[nodiscard]] constexpr std::string_view trim(std::string_view s) noexcept {
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
