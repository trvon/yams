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

// Path ranges covering everything under scopeRoot (lexical and symlink-resolved forms), for
// `yams graph --list-type ... --scope-cwd`.
std::vector<metadata::KGPathRange>
buildGraphCwdScopePathRanges(const std::filesystem::path& scopeRoot);

Result<std::unordered_set<std::string>>
buildGraphCodeScopePathSet(const std::filesystem::path& scopeRoot,
                           metadata::IMetadataRepository& repo);

} // namespace yams::app::services
