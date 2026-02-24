#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace yams::metadata {

enum class Fts5QueryMode { Smart, Simple, Natural };

// Core FTS token/query helpers shared by document query filters and search paths.
bool hasAdvancedFts5Operators(const std::string& query);
std::string sanitizeFts5UserQuery(std::string query, bool allowPrefixWildcard = true);
std::string stripPunctuation(std::string term);
std::string renderFts5Token(const std::string& token, bool prefix);

Fts5QueryMode parseFts5ModeEnv();
bool isLikelyNaturalLanguageQuery(std::string_view query);
std::string buildNaturalLanguageFts5Query(std::string_view query, bool useOrFallback,
                                          bool autoPrefix, bool autoPhrase);
std::vector<std::string> splitFTS5Terms(const std::string& trimmed);
std::string buildDiagnosticAltOrQuery(const std::vector<std::string>& tokens);
std::string sanitizeFTS5Query(const std::string& query);
bool includeSearchSnippets();

} // namespace yams::metadata
