#include <algorithm>
#include <numeric>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>
#include <yams/metadata/query_helpers.h>

namespace yams::metadata::sql {

namespace {

inline std::string joinWithSeparator(const std::vector<std::string>& items,
                                     std::string_view separator) {
    if (items.empty()) {
        return {};
    }

    const auto totalChars =
        std::accumulate(items.begin(), items.end(), static_cast<std::size_t>(0),
                        [](std::size_t sum, const std::string& part) { return sum + part.size(); });
    const auto separatorsSize = separator.size() * (items.size() - 1);

    std::string joined;
    joined.reserve(totalChars + separatorsSize);

    joined.append(items.front());
    for (std::size_t idx = 1; idx < items.size(); ++idx) {
        joined.append(separator);
        joined.append(items[idx]);
    }
    return joined;
}

inline std::string joinComma(const std::vector<std::string>& items) {
    return joinWithSeparator(items, ", ");
}

inline std::string joinAnd(const std::vector<std::string>& items) {
    return joinWithSeparator(items, " AND ");
}

} // namespace

namespace {
template <typename OptString>
inline void appendClause(std::string& sql, std::string_view keyword, const OptString& opt) {
    if (opt && !opt->empty()) {
        sql += ' ';
        sql += keyword;
        sql += ' ';
        sql += *opt;
    }
}

template <typename Container>
inline void appendListClause(std::string& sql, std::string_view keyword, const Container& c) {
    if (!c.empty()) {
        sql += ' ';
        sql += keyword;
        sql += ' ';
        sql += joinAnd(c);
    }
}

inline void appendLimitOffset(std::string& sql, const std::optional<int>& limit,
                              const std::optional<int>& offset) {
    if (limit && *limit > 0) {
        sql += " LIMIT ";
        sql += std::to_string(*limit);
    }
    if (offset && *offset > 0) {
        sql += " OFFSET ";
        sql += std::to_string(*offset);
    }
}
} // namespace

std::string buildSelect(const QuerySpec& spec) {
    const std::string cols = spec.columns.empty() ? std::string{"*"} : joinComma(spec.columns);
    std::string sql;
    sql.reserve(64 + cols.size() + spec.table.size());
    sql += "SELECT ";
    sql += cols;
    sql += " FROM ";
    sql += (spec.from && !spec.from->empty()) ? *spec.from : spec.table;

    appendListClause(sql, "WHERE", spec.conditions);
    appendClause(sql, "GROUP BY", spec.groupBy);
    appendClause(sql, "HAVING", spec.having);
    appendClause(sql, "ORDER BY", spec.orderBy);
    appendLimitOffset(sql, spec.limit, spec.offset);
    return sql;
}

} // namespace yams::metadata::sql

namespace yams::metadata {

Result<std::vector<DocumentInfo>>
queryDocumentsByPattern(IMetadataRepository& repo, const std::string& likePattern, int limit) {
    auto opts = buildQueryOptionsForSqlLikePattern(likePattern);
    if (limit > 0)
        opts.limit = limit;
    auto result = repo.queryDocuments(opts);
    // Fallback: if no results and no '%' wildcard, try as exact path
    if (result && result.value().empty() && !likePattern.empty() &&
        likePattern.find('%') == std::string::npos) {
        DocumentQueryOptions fallback;
        auto derived = computePathDerivedValues(likePattern);
        fallback.exactPath = derived.normalizedPath;
        if (limit > 0)
            fallback.limit = limit;
        result = repo.queryDocuments(fallback);
    }
    return result;
}

Result<std::vector<DocumentInfo>>
queryDocumentsByPattern(MetadataRepository& repo, const std::string& likePattern, int limit) {
    return queryDocumentsByPattern(static_cast<IMetadataRepository&>(repo), likePattern, limit);
}

DocumentQueryOptions buildQueryOptionsForSqlLikePattern(const std::string& pattern) {
    DocumentQueryOptions opts;
    // "%" means "all documents" — no WHERE clause needed; skip LIKE overhead
    if (pattern == "%") {
        return opts;
    }
    // Only treat '%' as a wildcard; '_' in paths should be literal
    auto has_percent = pattern.find('%') != std::string::npos;
    if (!has_percent) {
        // No wildcards - treat as exact path match
        auto derived = computePathDerivedValues(pattern);
        DocumentQueryOptions direct;
        direct.exactPath = derived.normalizedPath;
        return direct;
    }
    if (pattern.size() >= 2 && pattern.rfind("/%") == pattern.size() - 2) {
        std::string prefix = pattern.substr(0, pattern.size() - 2);
        opts.pathPrefix = prefix;
        opts.prefixIsDirectory = true;
        opts.includeSubdirectories = true;
        return opts;
    }
    if (pattern.size() >= 3 && pattern[0] == '%' && pattern[1] == '/') {
        auto pos = pattern.find_last_of('/');
        if (pos != std::string::npos && pos + 1 < pattern.size()) {
            opts.containsFragment = pattern.substr(pos + 1);
            opts.containsUsesFts = true;
            return opts;
        }
    }
    if (pattern.size() >= 2 && pattern[0] == '%' && pattern[1] == '.') {
        opts.extension = pattern.substr(1); // keep leading dot
        return opts;
    }
    // Fallback
    opts.likePattern = pattern;
    return opts;
}

namespace {
// Convert glob pattern to SQL LIKE pattern
// Examples:
//   "*.cpp" -> "%.cpp"
//   "tests/**/*.h" -> "tests/%.h"
//   "src/core/*.cpp" -> "src/core/%.cpp"
std::string globToSqlLike(const std::string& glob) {
    std::string result;
    result.reserve(glob.size());

    for (size_t i = 0; i < glob.size(); ++i) {
        char c = glob[i];
        if (c == '*') {
            // Check for ** (match any directory depth)
            if (i + 1 < glob.size() && glob[i + 1] == '*') {
                result += '%';
                i++; // Skip the second *
                // Skip following '/' if present
                if (i + 1 < glob.size() && glob[i + 1] == '/') {
                    i++;
                }
            } else {
                // Single * matches within a path component
                result += '%';
            }
        } else if (c == '?') {
            // ? in glob = _ in SQL LIKE (single character)
            result += '_';
        } else {
            // A literal '%' or '_' stays as written. The LIKE clause has no ESCAPE, so escaping
            // them made `\_` match a backslash; left alone they match a superset, and
            // keepExactGlobMatches() removes the extra rows.
            result += c;
        }
    }

    return result;
}

bool hasGlobWildcards(std::string_view globPattern) {
    return globPattern.find_first_of("*?") != std::string_view::npos;
}

char asciiLower(char c) {
    return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
}

// Matches a glob with the same rules as the LIKE clause it was translated to (whole string,
// ASCII case-insensitive, `*` and `**` match any run including '/', `?` one character), but
// with '%' and '_' taken literally.
bool globMatchesLikeLiterally(std::string_view text, std::string_view glob) {
    std::size_t t = 0;
    std::size_t g = 0;
    std::size_t starGlob = std::string_view::npos;
    std::size_t starText = 0;
    while (t < text.size()) {
        if (g < glob.size() && glob[g] == '*') {
            const std::size_t first = g;
            while (g < glob.size() && glob[g] == '*') {
                ++g;
            }
            // globToSqlLike drops the '/' after `**`, so it is optional here too.
            if (g - first >= 2 && g < glob.size() && glob[g] == '/') {
                ++g;
            }
            starGlob = g;
            starText = t;
        } else if (g < glob.size() &&
                   (glob[g] == '?' || asciiLower(glob[g]) == asciiLower(text[t]))) {
            ++g;
            ++t;
        } else if (starGlob != std::string_view::npos) {
            g = starGlob;
            t = ++starText;
        } else {
            return false;
        }
    }
    while (g < glob.size() && glob[g] == '*') {
        ++g;
    }
    return g == glob.size();
}

// The generic LIKE and the contains (`%/name`) translations both bind the text to a LIKE, where a
// literal '%' or '_' over-matches; the prefix and extension translations compare it exactly.
bool needsExactGlobRecheck(std::string_view globPattern, const DocumentQueryOptions& sqlOptions) {
    return globPattern.find_first_of("%_") != std::string_view::npos &&
           (sqlOptions.likePattern.has_value() || sqlOptions.containsFragment.has_value());
}

// A pattern with no glob wildcard names one path. Looked up through the SQL-LIKE helper, a
// literal '%' in it became a wildcard, so look it up exactly instead.
DocumentQueryOptions exactPathQuery(std::string_view path, int limit) {
    DocumentQueryOptions opts;
    opts.exactPath = computePathDerivedValues(std::string(path)).normalizedPath;
    if (limit > 0)
        opts.limit = limit;
    return opts;
}

template <typename Row>
void keepExactGlobMatches(std::vector<Row>& rows, std::string_view globPattern) {
    std::erase_if(
        rows, [&](const Row& row) { return !globMatchesLikeLiterally(row.filePath, globPattern); });
}

Result<std::vector<DocumentInfo>>
queryDocumentsForGlobPattern(IMetadataRepository& repo, std::string_view globPattern, int limit) {
    if (!hasGlobWildcards(globPattern)) {
        if (globPattern.find('%') != std::string_view::npos) {
            return repo.queryDocuments(exactPathQuery(globPattern, limit));
        }
        return queryDocumentsByPattern(repo, std::string(globPattern), limit);
    }

    std::string sqlPattern = globToSqlLike(std::string(globPattern));
    if (globPattern.find('?') != std::string_view::npos) {
        DocumentQueryOptions opts;
        opts.likePattern = sqlPattern;
        if (limit > 0)
            opts.limit = limit;
        auto result = repo.queryDocuments(opts);
        if (result && needsExactGlobRecheck(globPattern, opts)) {
            keepExactGlobMatches(result.value(), globPattern);
        }
        return result;
    }

    auto result = queryDocumentsByPattern(repo, sqlPattern, limit);
    if (result &&
        needsExactGlobRecheck(globPattern, buildQueryOptionsForSqlLikePattern(sqlPattern))) {
        keepExactGlobMatches(result.value(), globPattern);
    }
    return result;
}

Result<std::vector<GrepCandidateProjection>>
queryGrepCandidatesForGlobPattern(IMetadataRepository& repo, std::string_view globPattern,
                                  int limit) {
    if (!hasGlobWildcards(globPattern)) {
        if (globPattern.find('%') != std::string_view::npos) {
            auto opts = exactPathQuery(globPattern, limit);
            opts.usePathLocations = true;
            opts.currentPathsOnly = true;
            opts.excludeBinaryMimeTypes = true;
            return repo.queryDocumentsForGrepCandidates(opts);
        }
        return queryGrepCandidatesByPattern(repo, std::string(globPattern), limit);
    }

    std::string sqlPattern = globToSqlLike(std::string(globPattern));
    const auto lastSlash = globPattern.find_last_of('/');
    if (globPattern.find('?') != std::string_view::npos ||
        (lastSlash != std::string_view::npos && globPattern.find('*') < lastSlash)) {
        // Wildcards in directory segments are not literal path-prefix constraints.
        DocumentQueryOptions opts;
        opts.usePathLocations = true;
        opts.currentPathsOnly = true;
        opts.likePattern = sqlPattern;
        opts.excludeBinaryMimeTypes = true;
        if (limit > 0)
            opts.limit = limit;
        auto result = repo.queryDocumentsForGrepCandidates(opts);
        if (result && needsExactGlobRecheck(globPattern, opts)) {
            keepExactGlobMatches(result.value(), globPattern);
        }
        return result;
    }

    auto result = queryGrepCandidatesByPattern(repo, sqlPattern, limit);
    if (result &&
        needsExactGlobRecheck(globPattern, buildQueryOptionsForSqlLikePattern(sqlPattern))) {
        keepExactGlobMatches(result.value(), globPattern);
    }
    return result;
}
} // namespace

Result<std::vector<DocumentInfo>>
queryDocumentsByGlobPatterns(IMetadataRepository& repo,
                             const std::vector<std::string>& globPatterns, int limit) {
    if (globPatterns.empty()) {
        // No patterns = match all
        return queryDocumentsByPattern(repo, "%", limit);
    }

    if (globPatterns.size() == 1) {
        return queryDocumentsForGlobPattern(repo, globPatterns[0], limit);
    }

    // Multiple patterns - need to OR them together
    // Build a custom query with OR conditions
    std::vector<DocumentInfo> allResults;
    allResults.reserve(limit > 0 ? limit : 100);
    std::unordered_set<int64_t> seenIds;

    for (const auto& globPattern : globPatterns) {
        auto result = queryDocumentsForGlobPattern(repo, globPattern, 0); // No limit per pattern
        if (!result) {
            return result.error();
        }

        // Merge results, avoiding duplicates (O(1) lookup per doc)
        for (auto& doc : result.value()) {
            if (seenIds.insert(doc.id).second) {
                allResults.push_back(std::move(doc));
                if (limit > 0 && allResults.size() >= static_cast<size_t>(limit)) {
                    return allResults;
                }
            }
        }
    }

    return allResults;
}

Result<std::vector<GrepCandidateProjection>>
queryGrepCandidatesByPattern(IMetadataRepository& repo, const std::string& likePattern, int limit) {
    auto opts = buildQueryOptionsForSqlLikePattern(likePattern);
    opts.usePathLocations = true;
    opts.currentPathsOnly = true;
    opts.excludeBinaryMimeTypes = true;
    if (limit > 0)
        opts.limit = limit;
    auto result = repo.queryDocumentsForGrepCandidates(opts);
    // Fallback: if no results and no '%' wildcard, try as exact path
    if (result && result.value().empty() && !likePattern.empty() &&
        likePattern.find('%') == std::string::npos) {
        DocumentQueryOptions fallback;
        fallback.usePathLocations = true;
        fallback.currentPathsOnly = true;
        fallback.excludeBinaryMimeTypes = true;
        auto derived = computePathDerivedValues(likePattern);
        fallback.exactPath = derived.normalizedPath;
        if (limit > 0)
            fallback.limit = limit;
        result = repo.queryDocumentsForGrepCandidates(fallback);
    }
    return result;
}

Result<std::vector<GrepCandidateProjection>>
queryGrepCandidatesByGlobPatterns(IMetadataRepository& repo,
                                  const std::vector<std::string>& globPatterns, int limit) {
    if (globPatterns.empty()) {
        return queryGrepCandidatesByPattern(repo, "%", limit);
    }

    if (globPatterns.size() == 1) {
        return queryGrepCandidatesForGlobPattern(repo, globPatterns[0], limit);
    }

    // Multiple patterns — OR them together with O(1) dedup
    std::vector<GrepCandidateProjection> allResults;
    allResults.reserve(limit > 0 ? limit : 100);
    std::unordered_set<std::string> seenPaths;

    for (const auto& globPattern : globPatterns) {
        auto result = queryGrepCandidatesForGlobPattern(repo, globPattern, 0);
        if (!result) {
            return result.error();
        }

        for (auto& doc : result.value()) {
            if (seenPaths.insert(doc.filePath).second) {
                allResults.push_back(std::move(doc));
                if (limit > 0 && allResults.size() >= static_cast<size_t>(limit)) {
                    return allResults;
                }
            }
        }
    }

    return allResults;
}

} // namespace yams::metadata
