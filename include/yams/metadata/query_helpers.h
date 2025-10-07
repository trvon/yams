#pragma once

#include <optional>
#include <string>
#include <vector>
#include <yams/core/types.h>
#include <yams/metadata/metadata_repository.h> // for DocumentQueryOptions

namespace yams::metadata::sql {

struct QuerySpec {
    std::string table;                // Simple table form
    std::optional<std::string> from;  // Optional full FROM clause (e.g., with JOINs)
    std::vector<std::string> columns; // empty => "*"
    std::vector<std::string> conditions;
    std::optional<std::string> orderBy;
    std::optional<std::string> groupBy;
    std::optional<std::string> having;
    std::optional<int> limit;
    std::optional<int> offset;
};

// Build a basic SELECT statement using std::format (implemented in .cpp)
std::string buildSelect(const QuerySpec& spec);

} // namespace yams::metadata::sql

namespace yams::metadata {

class IMetadataRepository;
struct DocumentInfo;

// Convenience helper widely used across the codebase to fetch documents by SQL LIKE pattern
// against file_path (e.g., "%/name" or "%.ext" or "%").
Result<std::vector<DocumentInfo>>
queryDocumentsByPattern(IMetadataRepository& repo, const std::string& likePattern, int limit = 0);
Result<std::vector<DocumentInfo>>
queryDocumentsByPattern(MetadataRepository& repo, const std::string& likePattern, int limit = 0);

// Build DocumentQueryOptions heuristically from a SQL LIKE pattern used in the CLI/tests.
DocumentQueryOptions buildQueryOptionsForSqlLikePattern(const std::string& pattern);

} // namespace yams::metadata
