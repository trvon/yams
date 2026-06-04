#ifndef YAMS_CLI_GRAPH_SCOPE_SUPPORT_H
#define YAMS_CLI_GRAPH_SCOPE_SUPPORT_H

#include <yams/core/types.h>

#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>

namespace yams::metadata {
class IMetadataRepository;
}

namespace yams::cli {

class YamsCLI;

extern const std::string_view kGraphScopeToCwdDescription;

std::string normalizeGraphScopePath(const std::filesystem::path& path,
                                    const std::filesystem::path& cwd);

Result<std::unordered_set<std::string>>
buildGraphScopedPathSet(const std::filesystem::path& cwd,
                        const std::shared_ptr<metadata::IMetadataRepository>& repo);

Result<std::unordered_set<std::string>>
buildGraphCurrentScopePathSet(YamsCLI* cli, const std::filesystem::path& cwd);

} // namespace yams::cli

#endif // YAMS_CLI_GRAPH_SCOPE_SUPPORT_H
