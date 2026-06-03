#include <yams/cli/graph_scope_support.h>

#include <yams/cli/yams_cli.h>
#include <yams/core/magic_numbers.hpp>
#include <yams/metadata/metadata_repository.h>

namespace yams::cli {

const std::string_view kGraphScopeToCwdDescription =
    "Scoped to src/** and include/** via path tree (excluding tests/, benchmarks/, "
    "third_party/, node_modules/, build*)";

std::string normalizeGraphScopePath(const std::filesystem::path& path,
                                    const std::filesystem::path& cwd) {
    std::filesystem::path normalized = path;
    if (normalized.is_relative()) {
        normalized = cwd / normalized;
    }
    normalized = normalized.lexically_normal();
    return normalized.generic_string();
}

Result<std::unordered_set<std::string>>
buildGraphScopedPathSet(const std::filesystem::path& cwd,
                        const std::shared_ptr<metadata::IMetadataRepository>& repo) {
    if (!repo) {
        return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
    }

    std::unordered_set<std::string> paths;
    auto addPrefix = [&](std::string_view prefix) -> Result<void> {
        auto res = repo->findDocumentsByPathTreePrefix(prefix, true, 0);
        if (!res) {
            return res.error();
        }
        for (const auto& doc : res.value()) {
            paths.insert(normalizeGraphScopePath(std::filesystem::path(doc.filePath), cwd));
        }
        return Result<void>();
    };

    for (const auto& prefix : {"src", "include"}) {
        auto result = addPrefix(prefix);
        if (!result) {
            return result.error();
        }
    }

    for (auto it = paths.begin(); it != paths.end();) {
        const auto& pathStr = *it;
        std::string_view ext;
        if (auto dot = pathStr.find_last_of('.'); dot != std::string::npos) {
            ext = std::string_view(pathStr).substr(dot + 1);
        }
        auto category = yams::magic::getPruneCategory(pathStr, ext);
        if (category == yams::magic::PruneCategory::GitArtifacts ||
            yams::magic::matchesPruneGroup(category, "build") ||
            yams::magic::matchesPruneGroup(category, "packages") ||
            yams::magic::matchesPruneGroup(category, "ide-all")) {
            it = paths.erase(it);
            continue;
        }
        ++it;
    }

    return paths;
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
