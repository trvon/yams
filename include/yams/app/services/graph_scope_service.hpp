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

// `yams graph --scope-cwd` covers everything under scopeRoot, in its lexical and
// symlink-resolved forms.

// Path ranges for the scope, for KG node listings (`--list-type`).
std::vector<metadata::KGPathRange>
buildGraphCwdScopePathRanges(const std::filesystem::path& scopeRoot);

// Stored document paths inside the scope, for topology views (`--topology-clusters`).
Result<std::unordered_set<std::string>>
buildGraphCwdScopePathSet(const std::filesystem::path& scopeRoot,
                          metadata::IMetadataRepository& repo);

} // namespace yams::app::services
