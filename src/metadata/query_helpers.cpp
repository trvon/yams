#include <numeric>
#include <ranges>
#include <string>
#include <string_view>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
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
    if (result && result.value().empty() && !likePattern.empty() &&
        likePattern.find('%') == std::string::npos && likePattern.find('_') == std::string::npos) {
        DocumentQueryOptions fallback;
        auto derived = computePathDerivedValues(likePattern);
        fallback.likePattern = derived.normalizedPath;
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
    auto has_wildcard =
        pattern.find('%') != std::string::npos || pattern.find('_') != std::string::npos;
    if (!has_wildcard) {
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
        } else if (c == '%' || c == '_') {
            // Escape SQL LIKE wildcards if they appear literally in glob
            result += '\\';
            result += c;
        } else {
            result += c;
        }
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
        // Single pattern - use existing optimized path
        std::string sqlPattern = globToSqlLike(globPatterns[0]);
        return queryDocumentsByPattern(repo, sqlPattern, limit);
    }

    // Multiple patterns - need to OR them together
    // Build a custom query with OR conditions
    std::vector<DocumentInfo> allResults;
    allResults.reserve(limit > 0 ? limit : 100);

    for (const auto& globPattern : globPatterns) {
        std::string sqlPattern = globToSqlLike(globPattern);
        auto result = queryDocumentsByPattern(repo, sqlPattern, 0); // No limit per pattern
        if (!result) {
            return result.error();
        }

        // Merge results, avoiding duplicates
        for (auto& doc : result.value()) {
            // Check if already in results (by id or path)
            bool isDuplicate = false;
            for (const auto& existing : allResults) {
                if (existing.id == doc.id) {
                    isDuplicate = true;
                    break;
                }
            }
            if (!isDuplicate) {
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
