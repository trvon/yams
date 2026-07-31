#pragma once

#include <yams/core/types.h>
#include <yams/metadata/knowledge_graph_store.h>

#include <filesystem>
#include <string>
#include <unordered_set>
#include <vector>

namespace yams::metadata {
class IMetadataRepository;
}

namespace yams::app::services {

std::string normalizeGraphScopePath(const std::filesystem::path& path,
                                    const std::filesystem::path& scopeRoot);

std::vector<metadata::KGPathRange>
buildGraphCodeScopePathRanges(const std::filesystem::path& scopeRoot);

Result<std::unordered_set<std::string>>
buildGraphCodeScopePathSet(const std::filesystem::path& scopeRoot,
                           metadata::IMetadataRepository& repo);

} // namespace yams::app::services
