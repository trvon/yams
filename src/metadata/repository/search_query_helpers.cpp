#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdlib>
#include <sstream>
#include <unordered_set>

#include <yams/metadata/metadata_repository.h>

#include "document_query_filters.hpp"
#include "search_query_helpers.hpp"

namespace yams::metadata {

// Helper: escape a single term for FTS5 by wrapping in quotes
// Per SQLite docs: replace " with "" and wrap in double quotes
static std::string quoteFTS5Term(const std::string& term) {
    std::string escaped;
    escaped.reserve(term.size() + 4);
    escaped += '"';
    for (char c : term) {
        if (c == '"') {
            escaped += "\"\"";
        } else {
            escaped += c;
        }
    }
    escaped += '"';
    return escaped;
}

// Strip leading and trailing punctuation from a term.
// Preserves internal punctuation (e.g., hyphens in "sugar-sweetened").
// This is critical for FTS5 matching: "India." won't match "India" in the index.
std::string stripPunctuation(std::string term) {
    // Strip trailing punctuation
    while (!term.empty()) {
        char c = term.back();
        if (std::isalnum(static_cast<unsigned char>(c)))
            break;
        term.pop_back();
    }
    // Strip leading punctuation (e.g., "(HSC)" -> "HSC")
    while (!term.empty()) {
        char c = term.front();
        if (std::isalnum(static_cast<unsigned char>(c)))
            break;
        term.erase(term.begin());
    }
    return term;
}

// Backwards compatibility alias
static std::string stripTrailingPunctuation(std::string term) {
    return stripPunctuation(std::move(term));
}

static bool isFts5BarewordChar(unsigned char c) {
    return std::isalnum(c) || c == '_' || c == 0x1A || c >= 0x80;
}

static bool isFts5Bareword(const std::string& token) {
    if (token.empty()) {
        return false;
    }
    std::string upper;
    upper.reserve(token.size());
    for (char c : token) {
        upper.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
    }
    if (upper == "AND" || upper == "OR" || upper == "NOT") {
        return false;
    }
    for (unsigned char c : token) {
        if (!isFts5BarewordChar(c)) {
            return false;
        }
    }
    return true;
}

std::string renderFts5Token(const std::string& token, bool prefix) {
    std::string rendered = isFts5Bareword(token) ? token : quoteFTS5Term(token);
    if (prefix) {
        rendered.push_back('*');
    }
    return rendered;
}

static std::string buildSimpleFts5Query(const std::string& query, bool allowPrefixWildcard = true) {
    std::istringstream iss(query);
    std::string token;
    std::string result;
    bool first = true;

    while (iss >> token) {
        bool prefix = false;
        if (allowPrefixWildcard && token.size() > 1 && token.back() == '*') {
            prefix = true;
            token.pop_back();
        }

        token = stripPunctuation(std::move(token));
        if (token.empty()) {
            continue;
        }

        if (!first) {
            result.push_back(' ');
        }
        first = false;

        result += renderFts5Token(token, prefix);
    }

    return result.empty() ? "\"\"" : result;
}

bool hasAdvancedFts5Operators(const std::string& query) {
    bool inQuotes = false;
    bool sawQuote = false;
    std::string token;
    token.reserve(32);
    bool sawOperatorToken = false;
    bool sawNonOperatorToken = false;
    int nonOperatorTokenCount = 0;

    auto flushToken = [&](void) -> bool {
        if (token.empty()) {
            return false;
        }
        // FTS5 operators are CASE-SENSITIVE: only uppercase AND/OR/NOT/NEAR are operators
        // Lowercase "and", "or", "not" are regular search terms
        if (token == "AND" || token == "OR" || token == "NOT") {
            sawOperatorToken = true;
            token.clear();
            return false;
        }
        if (token == "NEAR" || token.rfind("NEAR/", 0) == 0) {
            sawOperatorToken = true;
            token.clear();
            return false;
        }
        // Build uppercase version for other checks
        std::string upper;
        upper.reserve(token.size());
        for (char c : token) {
            upper.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
        }
        auto colonPos = token.find(':');
        if (colonPos != std::string::npos && colonPos > 0) {
            bool validField = true;
            for (size_t i = 0; i < colonPos; ++i) {
                char c = token[i];
                if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '_')) {
                    validField = false;
                    break;
                }
            }
            if (validField) {
                if (token.find('/') == std::string::npos && token.find('\\') == std::string::npos) {
                    return true;
                }
            }
        }
        sawNonOperatorToken = true;
        ++nonOperatorTokenCount;
        token.clear();
        return false;
    };

    for (size_t i = 0; i < query.size(); ++i) {
        char c = query[i];
        if (c == '"') {
            sawQuote = true;
            if (inQuotes && i + 1 < query.size() && query[i + 1] == '"') {
                ++i;
                continue;
            }
            inQuotes = !inQuotes;
            continue;
        }
        if (!inQuotes) {
            if (std::isspace(static_cast<unsigned char>(c))) {
                if (flushToken()) {
                    return true;
                }
                continue;
            }
            if (c == '(' || c == ')') {
                if (flushToken()) {
                    return true;
                }
                // Only treat parens as grouping operators if we've seen actual FTS5 operators
                // "(DCs)" after "dendritic cells" is punctuation, not grouping
                // "(foo OR bar)" with uppercase OR is actual grouping
                if (sawOperatorToken) {
                    return true;
                }
                continue;
            }
            token.push_back(c);
        }
    }

    if (flushToken()) {
        return true;
    }

    if (sawQuote) {
        return true;
    }
    return sawOperatorToken && sawNonOperatorToken;
}

std::string sanitizeFts5UserQuery(std::string query, bool allowPrefixWildcard) {
    query.erase(0, query.find_first_not_of(" \t\n\r"));
    if (!query.empty()) {
        auto lastNonWs = query.find_last_not_of(" \t\n\r");
        if (lastNonWs != std::string::npos) {
            query.erase(lastNonWs + 1);
        }
    }

    if (query.empty()) {
        return "\"\"";
    }

    if (hasAdvancedFts5Operators(query)) {
        while (!query.empty()) {
            char lastChar = query.back();
            if (lastChar == '-' || lastChar == '+' || lastChar == '*' || lastChar == '(') {
                query.pop_back();
            } else {
                break;
            }
        }
        return query.empty() ? "\"\"" : query;
    }

    return buildSimpleFts5Query(query, allowPrefixWildcard);
}

Fts5QueryMode parseFts5ModeEnv() {
    if (const char* env = std::getenv("YAMS_FTS_MODE"); env && *env) {
        std::string mode(env);
        std::transform(mode.begin(), mode.end(), mode.begin(), ::tolower);
        if (mode == "simple") {
            return Fts5QueryMode::Simple;
        }
        if (mode == "nl" || mode == "natural") {
            return Fts5QueryMode::Natural;
        }
    }
    return Fts5QueryMode::Smart;
}

bool isLikelyNaturalLanguageQuery(std::string_view query) {
    if (hasAdvancedFts5Operators(std::string(query))) {
        return false;
    }

    if (query.find(':') != std::string_view::npos || query.find('*') != std::string_view::npos ||
        query.find('=') != std::string_view::npos || query.find('\\') != std::string_view::npos ||
        query.find('/') != std::string_view::npos) {
        return false;
    }

    std::istringstream iss{std::string(query)};
    std::string token;
    int tokenCount = 0;
    int alphaTokenCount = 0;
    double totalLen = 0.0;
    double totalAlpha = 0.0;

    while (iss >> token) {
        if (token.find('_') != std::string::npos || token.find("::") != std::string::npos) {
            return false;
        }
        tokenCount++;
        totalLen += token.size();
        int alphaCount = 0;
        for (unsigned char c : token) {
            if (std::isalpha(c)) {
                alphaCount++;
            }
        }
        totalAlpha += alphaCount;
        if (alphaCount >= 2) {
            alphaTokenCount++;
        }
    }

    if (tokenCount < 2) {
        return false;
    }

    const double avgLen = totalLen / tokenCount;
    const double alphaRatio = totalAlpha / std::max(1.0, totalLen);

    return alphaTokenCount >= 2 && avgLen >= 3.0 && alphaRatio >= 0.6;
}

static std::vector<std::string> tokenizeNaturalLanguageQuery(std::string_view query);

std::string buildNaturalLanguageFts5Query(std::string_view query, bool useOrFallback,
                                          bool autoPrefix, bool autoPhrase) {
    auto tokens = tokenizeNaturalLanguageQuery(query);
    if (tokens.empty()) {
        return "\"\"";
    }

    if (autoPhrase && tokens.size() >= 2 && tokens.size() <= 4) {
        std::string phrase;
        for (size_t i = 0; i < tokens.size(); ++i) {
            if (i > 0)
                phrase.push_back(' ');
            phrase += tokens[i];
        }
        return quoteFTS5Term(phrase);
    }

    std::string result;
    for (size_t i = 0; i < tokens.size(); ++i) {
        if (i > 0) {
            result += useOrFallback ? " OR " : " ";
        }
        const bool prefix = autoPrefix && tokens[i].size() >= 4;
        result += renderFts5Token(tokens[i], prefix);
    }

    return result.empty() ? "\"\"" : result;
}

namespace test {
std::string buildNaturalLanguageFts5QueryForTest(std::string_view query, bool useOr,
                                                 bool autoPrefix, bool autoPhrase) {
    return buildNaturalLanguageFts5Query(query, useOr, autoPrefix, autoPhrase);
}

bool isLikelyNaturalLanguageQueryForTest(std::string_view query) {
    return isLikelyNaturalLanguageQuery(query);
}
} // namespace test

std::vector<std::string> splitFTS5Terms(const std::string& trimmed) {
    std::vector<std::string> terms;
    std::string current;
    for (char c : trimmed) {
        if (c == ' ' || c == '\t') {
            if (!current.empty()) {
                terms.push_back(current);
                current.clear();
            }
        } else {
            current += c;
        }
    }
    if (!current.empty()) {
        terms.push_back(current);
    }
    return terms;
}

static std::vector<std::string> stripPunctuationTokens(const std::vector<std::string>& tokens) {
    std::vector<std::string> stripped;
    stripped.reserve(tokens.size());
    for (const auto& tok : tokens) {
        auto s = stripTrailingPunctuation(tok);
        if (!s.empty()) {
            stripped.push_back(s);
        }
    }
    return stripped;
}

static std::string joinPreview(const std::vector<std::string>& tokens) {
    std::string joined;
    joined.reserve(tokens.size() * 8);
    for (size_t i = 0; i < tokens.size(); ++i) {
        if (i > 0)
            joined += ", ";
        if (tokens[i].size() > 64) {
            joined += tokens[i].substr(0, 64);
            joined += "…";
        } else {
            joined += tokens[i];
        }
    }
    return joined;
}

std::string buildDiagnosticAltOrQuery(const std::vector<std::string>& tokens) {
    std::vector<std::string> stripped = stripPunctuationTokens(tokens);

    std::vector<std::string> altTerms;
    for (const auto& t : stripped) {
        if (t.size() >= 4) {
            altTerms.push_back(t);
        }
        if (altTerms.size() >= 5)
            break;
    }

    std::string altQuery;
    if (!altTerms.empty()) {
        for (size_t i = 0; i < altTerms.size(); ++i) {
            if (i > 0)
                altQuery += " OR ";
            altQuery += quoteFTS5Term(altTerms[i]);
        }
    }
    return altQuery;
}

static void logFtsTokensIfEnabled(const std::string& rawQuery,
                                  const std::vector<std::string>& tokens) {
    if (const char* env = std::getenv("YAMS_FTS_DEBUG_QUERY"); env && std::string(env) == "1") {
        if (hasAdvancedFts5Operators(rawQuery)) {
            return;
        }
        std::vector<std::string> stripped = stripPunctuationTokens(tokens);
        std::string previewRaw = joinPreview(tokens);
        std::string previewStripped = joinPreview(stripped);
        std::string altQuery = buildDiagnosticAltOrQuery(tokens);

        spdlog::warn(
            "[FTS5] tokens count={} raw='{}' tokens=[{}] stripped=[{}] stripped_dropped={} "
            "diag_alt_or=\"{}\"",
            tokens.size(), rawQuery, previewRaw, previewStripped, tokens.size() - stripped.size(),
            altQuery);
    }
}

// Common English stopwords that add noise to FTS5 AND queries.
// These are filtered out for natural language queries to improve recall.
static const std::unordered_set<std::string>& getStopwords() {
    static const std::unordered_set<std::string> stopwords = {
        "a",    "an",   "and",   "are",   "as",   "at",   "be",   "by",    "for", "from",
        "had",  "has",  "have",  "he",    "her",  "his",  "i",    "in",    "is",  "it",
        "its",  "no",   "not",   "of",    "on",   "or",   "she",  "that",  "the", "their",
        "them", "then", "there", "these", "they", "this", "to",   "was",   "we",  "were",
        "what", "when", "where", "which", "who",  "will", "with", "would", "you", "your",
        "very", "can",  "could", "do",    "does", "did",  "but",  "if",    "so",  "than",
        "too",  "only", "just",  "also"};
    return stopwords;
}

// Check if a term is a stopword (case-insensitive)
static bool isStopword(const std::string& term) {
    std::string lower;
    lower.reserve(term.size());
    for (char c : term) {
        lower += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return getStopwords().count(lower) > 0;
}

static std::vector<std::string> tokenizeNaturalLanguageQuery(std::string_view query) {
    std::vector<std::string> tokens;
    std::istringstream iss{std::string(query)};
    std::string term;
    while (iss >> term) {
        std::transform(term.begin(), term.end(), term.begin(), ::tolower);
        term = stripPunctuation(std::move(term));
        if (term.empty())
            continue;
        if (term.size() < 2)
            continue;
        if (isStopword(term))
            continue;
        tokens.push_back(std::move(term));
    }
    return tokens;
}

// Sanitize FTS5 query to prevent syntax errors.
// Uses FTS5's default AND semantics for multiple terms (all terms must match).
// Pass through advanced FTS5 operators (AND, OR, NOT, NEAR) for power users.
//
// For RAG/BEIR-style retrieval, consider:
// 1. Document expansion at index time (docT5query technique)
// 2. Reranking with cross-encoder after BM25 retrieval
// 3. Hybrid fusion with vector search for semantic matching
std::string sanitizeFTS5Query(const std::string& query) {
    return sanitizeFts5UserQuery(query);
}

// Snippet extraction forces additional FTS content-table reads and can dominate
// latency on large corpora. Keep disabled by default for fast retrieval/ranking.
bool includeSearchSnippets() {
    static std::atomic<int> cached{-1};
    int cachedValue = cached.load(std::memory_order_relaxed);
    if (cachedValue >= 0)
        return cachedValue == 1;

    bool enabled = false;
    if (const char* env = std::getenv("YAMS_SEARCH_INCLUDE_SNIPPET"); env && *env) {
        std::string_view value(env);
        enabled = (value != "0" && value != "false" && value != "off" && value != "no");
    }
    cached.store(enabled ? 1 : 0, std::memory_order_relaxed);
    return enabled;
}

} // namespace yams::metadata
