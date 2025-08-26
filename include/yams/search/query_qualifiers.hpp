#pragma once

// Shared qualifier parser for hybrid/metadata search normalization.
// Purpose:
// - Allow users to write inline scope and filter qualifiers in a single query string.
// - Produce a normalized query string for scoring (without qualifiers).
// - Return parsed scope/filter hints for downstream snippet shaping and filtering.
//
// Recognized qualifiers (case-insensitive):
//   - lines:<range>          e.g., lines:1-50
//   - pages:<range>          e.g., pages:10-12
//   - section:<quoted>       e.g., section:"Chapter 2"
//   - selector:<quoted>      e.g., selector:"div.main"
//   - name:<value>           e.g., name:paper.pdf
//   - ext:<value>            e.g., ext:.pdf
//   - mime:<value>           e.g., mime:application/pdf
//
// Accepted forms for values:
//   - key:value
//   - key:"value with spaces"
//   - key: "value with spaces"   (note the space after ':')

#include <algorithm>
#include <cctype>
#include <sstream>
#include <string>
#include <vector>

namespace yams::search {

// Scope type for extraction/snippet shaping
enum class ExtractScopeType { All, Lines, Pages, Section, Selector };

// Parsed scope information
struct ExtractScope {
    ExtractScopeType type{ExtractScopeType::All};
    std::string range;    // lines/pages range, e.g., "1-3,10"
    std::string section;  // section title
    std::string selector; // CSS/XPath selector

    // Filter-like hints (not applied by the parser; caller may map them to filters)
    std::string name;
    std::string ext;
    std::string mime;
};

// Parser output
struct ParsedQuery {
    std::string normalizedQuery;
    ExtractScope scope;
};

namespace detail {

// Trim helpers
inline std::string ltrim_copy(std::string s) {
    auto it = std::find_if(s.begin(), s.end(), [](unsigned char c) { return !std::isspace(c); });
    s.erase(s.begin(), it);
    return s;
}

inline std::string rtrim_copy(std::string s) {
    auto it = std::find_if(s.rbegin(), s.rend(), [](unsigned char c) { return !std::isspace(c); });
    s.erase(it.base(), s.end());
    return s;
}

inline std::string trim_copy(std::string s) {
    return rtrim_copy(ltrim_copy(std::move(s)));
}

// Lowercase copy
inline std::string to_lower_copy(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return s;
}

// Unquote if "..." or '...'
inline std::string unquote(std::string s) {
    if (s.size() >= 2) {
        const char a = s.front();
        const char b = s.back();
        if ((a == '"' && b == '"') || (a == '\'' && b == '\'')) {
            return s.substr(1, s.size() - 2);
        }
    }
    return s;
}

// Tokenize preserving quoted segments and allowing key: "value with spaces"
// Output tokens are whitespace-delimited, but quoted strings remain as single tokens.
inline std::vector<std::string> tokenize_preserve_quotes(const std::string& input) {
    std::vector<std::string> tokens;
    tokens.reserve(16);

    const char* s = input.c_str();
    const size_t n = input.size();
    size_t i = 0;

    auto flush = [&](std::string& buf) {
        if (!buf.empty()) {
            tokens.push_back(buf);
            buf.clear();
        }
    };

    while (i < n) {
        // Skip leading spaces
        while (i < n && std::isspace(static_cast<unsigned char>(s[i]))) {
            ++i;
        }
        if (i >= n)
            break;

        std::string tok;

        // Quoted token
        if (s[i] == '"' || s[i] == '\'') {
            const char q = s[i++];
            tok.push_back(q); // include quotes in token; caller may unquote later
            while (i < n) {
                tok.push_back(s[i]);
                if (s[i] == q) {
                    ++i;
                    break;
                }
                ++i;
            }
            tokens.push_back(tok);
            continue;
        }

        // Normal token (may contain colon)
        while (i < n && !std::isspace(static_cast<unsigned char>(s[i]))) {
            // If a quote appears inside a token, capture the quoted substring as part of the token
            if (s[i] == '"' || s[i] == '\'') {
                const char q = s[i++];
                tok.push_back(q);
                while (i < n) {
                    tok.push_back(s[i]);
                    if (s[i] == q) {
                        ++i;
                        break;
                    }
                    ++i;
                }
                continue;
            }
            tok.push_back(s[i]);
            ++i;
        }
        flush(tok);
    }

    return tokens;
}

// Try to consume a qualifier of form key:value or key: "value" from tokens[i].
// - key match is case-insensitive.
// - If value is empty after colon, will peek next token if it is quoted.
// On success: sets out_value (unmodified; may include quotes), advances i, and returns true.
// Otherwise: returns false and leaves i unchanged.
inline bool try_consume_qualifier(size_t& i, const std::vector<std::string>& toks,
                                  const std::string& key_lc, std::string& out_value) {
    if (i >= toks.size())
        return false;

    const std::string& t = toks[i];
    const auto colon_pos = t.find(':');
    if (colon_pos == std::string::npos)
        return false;

    // Extract and lowercase key part for comparison
    std::string key_part = to_lower_copy(t.substr(0, colon_pos));
    if (key_part != key_lc)
        return false;

    // Value may be in the same token (after colon)
    std::string v = trim_copy(t.substr(colon_pos + 1));
    if (!v.empty()) {
        out_value = v;
        ++i;
        return true;
    }

    // If empty, check next token for a quoted value
    if (i + 1 < toks.size()) {
        const std::string& next = toks[i + 1];
        if (!next.empty() && (next.front() == '"' || next.front() == '\'')) {
            out_value = next;
            i += 2;
            return true;
        }
    }

    // No usable value; consume current token
    ++i;
    return false;
}

// Check if token is exactly "key:" (case-insensitive)
inline bool is_exact_key_colon(const std::string& tok, const std::string& key_lc) {
    if (tok.size() != key_lc.size() + 1)
        return false;
    if (tok.back() != ':')
        return false;
    return to_lower_copy(tok.substr(0, tok.size() - 1)) == key_lc;
}

} // namespace detail

// Main entry point.
// - Parses inline qualifiers and returns a normalized query string without qualifiers.
// - Populates ExtractScope with recognized hints.
inline ParsedQuery parseQueryQualifiers(const std::string& raw) {
    using namespace detail;

    ExtractScope scope;
    std::vector<std::string> carry;

    const auto toks = tokenize_preserve_quotes(raw);

    size_t i = 0;
    while (i < toks.size()) {
        const std::string& tok = toks[i];
        std::string v;

        // lines:<range> or lines: "<range>"
        if (try_consume_qualifier(i, toks, "lines", v) || is_exact_key_colon(tok, "lines")) {
            if (!v.empty()) {
                scope.type = ExtractScopeType::Lines;
                scope.range = unquote(trim_copy(v));
                continue;
            } else {
                // If token was exactly "lines:", grab next quoted token if present
                if (i < toks.size() && (toks[i].size() > 1) &&
                    (toks[i].front() == '"' || toks[i].front() == '\'')) {
                    scope.type = ExtractScopeType::Lines;
                    scope.range = unquote(trim_copy(toks[i]));
                    ++i;
                    continue;
                }
                // else fall through to carry
            }
        }

        // pages:<range>
        if (try_consume_qualifier(i, toks, "pages", v) || is_exact_key_colon(tok, "pages")) {
            if (!v.empty()) {
                scope.type = ExtractScopeType::Pages;
                scope.range = unquote(trim_copy(v));
                continue;
            } else {
                if (i < toks.size() && (toks[i].size() > 1) &&
                    (toks[i].front() == '"' || toks[i].front() == '\'')) {
                    scope.type = ExtractScopeType::Pages;
                    scope.range = unquote(trim_copy(toks[i]));
                    ++i;
                    continue;
                }
            }
        }

        // section:<quoted>
        if (try_consume_qualifier(i, toks, "section", v) || is_exact_key_colon(tok, "section")) {
            if (!v.empty()) {
                scope.type = ExtractScopeType::Section;
                scope.section = unquote(trim_copy(v));
                continue;
            } else {
                if (i < toks.size() && (toks[i].size() > 1) &&
                    (toks[i].front() == '"' || toks[i].front() == '\'')) {
                    scope.type = ExtractScopeType::Section;
                    scope.section = unquote(trim_copy(toks[i]));
                    ++i;
                    continue;
                }
            }
        }

        // selector:<quoted>
        if (try_consume_qualifier(i, toks, "selector", v) || is_exact_key_colon(tok, "selector")) {
            if (!v.empty()) {
                scope.type = ExtractScopeType::Selector;
                scope.selector = unquote(trim_copy(v));
                continue;
            } else {
                if (i < toks.size() && (toks[i].size() > 1) &&
                    (toks[i].front() == '"' || toks[i].front() == '\'')) {
                    scope.type = ExtractScopeType::Selector;
                    scope.selector = unquote(trim_copy(toks[i]));
                    ++i;
                    continue;
                }
            }
        }

        // name:<value>
        if (try_consume_qualifier(i, toks, "name", v) || is_exact_key_colon(tok, "name")) {
            if (!v.empty()) {
                scope.name = unquote(trim_copy(v));
                continue;
            } else {
                if (i < toks.size() && (toks[i].size() > 1) &&
                    (toks[i].front() == '"' || toks[i].front() == '\'')) {
                    scope.name = unquote(trim_copy(toks[i]));
                    ++i;
                    continue;
                }
            }
        }

        // ext:<value>
        if (try_consume_qualifier(i, toks, "ext", v) || is_exact_key_colon(tok, "ext")) {
            if (!v.empty()) {
                scope.ext = unquote(trim_copy(v));
                continue;
            } else {
                if (i < toks.size() && (toks[i].size() > 1) &&
                    (toks[i].front() == '"' || toks[i].front() == '\'')) {
                    scope.ext = unquote(trim_copy(toks[i]));
                    ++i;
                    continue;
                }
            }
        }

        // mime:<value>
        if (try_consume_qualifier(i, toks, "mime", v) || is_exact_key_colon(tok, "mime")) {
            if (!v.empty()) {
                scope.mime = unquote(trim_copy(v));
                continue;
            } else {
                if (i < toks.size() && (toks[i].size() > 1) &&
                    (toks[i].front() == '"' || toks[i].front() == '\'')) {
                    scope.mime = unquote(trim_copy(toks[i]));
                    ++i;
                    continue;
                }
            }
        }

        // Not a recognized qualifier; keep token in normalized query
        carry.push_back(tok);
        ++i;
    }

    // Reconstruct normalized query
    std::string normalized;
    normalized.reserve(raw.size());
    for (size_t j = 0; j < carry.size(); ++j) {
        if (j)
            normalized.push_back(' ');
        normalized += carry[j];
    }
    normalized = trim_copy(std::move(normalized));
    if (normalized.empty()) {
        // If user only provided qualifiers, leave normalized empty
        normalized = "";
    }

    return ParsedQuery{std::move(normalized), std::move(scope)};
}

} // namespace yams::search