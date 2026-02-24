#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include <yams/metadata/metadata_repository.h>

namespace yams::metadata {

namespace repository {

Result<std::unordered_map<std::string, std::vector<MetadataValueCount>>>
runMetadataValueCountQuery(Database& db, const std::vector<std::string>& keys,
                           const DocumentQueryOptions& options, bool needsDocumentJoin,
                           bool joinFtsForContains, bool hasPathIndexing,
                           std::string* sqlOut = nullptr);

} // namespace repository
} // namespace yams::metadata
