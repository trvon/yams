#include <yams/cli/graph_scope_support.h>

#include <yams/app/services/graph_scope_service.hpp>
#include <yams/metadata/metadata_repository.h>

namespace yams::cli {

const std::string_view kGraphScopeToCwdDescription = "Scoped to paths under the current directory";

std::string normalizeGraphScopePath(const std::filesystem::path& path,
                                    const std::filesystem::path& cwd) {
    return app::services::normalizeGraphScopePath(path, cwd);
}

Result<std::unordered_set<std::string>>
buildGraphScopedPathSet(const std::filesystem::path& cwd,
                        const std::shared_ptr<metadata::IMetadataRepository>& repo) {
    if (!repo) {
        return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
    }
    return app::services::buildGraphCwdScopePathSet(cwd, *repo);
}

} // namespace yams::cli
