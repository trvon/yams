#include <yams/cli/graph_scope_support.h>

#include <yams/app/services/graph_scope_service.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/metadata/metadata_repository.h>

namespace yams::cli {

const std::string_view kGraphScopeToCwdDescription =
    "Scoped to src/** and include/** via path tree (excluding tests/, benchmarks/, "
    "third_party/, node_modules/, build*)";

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
    return app::services::buildGraphCodeScopePathSet(cwd, *repo);
}

Result<std::unordered_set<std::string>>
buildGraphCurrentScopePathSet(YamsCLI* cli, const std::filesystem::path& cwd) {
    auto appCtx = cli ? cli->getAppContext() : nullptr;
    if (!appCtx || !appCtx->metadataRepo) {
        return Error{ErrorCode::NotInitialized,
                     "Path tree scoping unavailable (metadata repo not ready)"};
    }
    return buildGraphScopedPathSet(cwd, appCtx->metadataRepo);
}

} // namespace yams::cli
