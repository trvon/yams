#include <yams/app/services/graph_scope_service.hpp>

#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

#include <algorithm>

namespace yams::app::services {

namespace {

metadata::KGPathRange directoryRange(std::string path) {
    if (!path.ends_with('/')) {
        path.push_back('/');
    }
    auto upper = path;
    ++upper.back();
    return {.lower = std::move(path), .upper = std::move(upper)};
}

} // namespace

std::string normalizeGraphScopePath(const std::filesystem::path& path,
                                    const std::filesystem::path& scopeRoot) {
    auto normalized = path;
    if (normalized.is_relative()) {
        normalized = scopeRoot / normalized;
    }
    return metadata::computePathDerivedValues(normalized.lexically_normal().generic_string())
        .normalizedPath;
}

namespace {

std::vector<std::string> graphCwdScopeRoots(const std::filesystem::path& scopeRoot) {
    std::vector<std::string> roots;
    auto add = [&](const std::filesystem::path& root) {
        auto normalized = normalizeGraphScopePath(root, scopeRoot);
        while (normalized.size() > 1 && normalized.ends_with('/')) {
            normalized.pop_back();
        }
        if (!normalized.empty() && std::ranges::find(roots, normalized) == roots.end()) {
            roots.push_back(std::move(normalized));
        }
    };
    add(scopeRoot);
    // Stored paths are resolved; a cwd reached through a symlink must still match them.
    std::error_code ec;
    if (auto canonical = std::filesystem::weakly_canonical(scopeRoot, ec); !ec) {
        add(canonical);
    }
    return roots;
}

} // namespace

std::vector<metadata::KGPathRange>
buildGraphCwdScopePathRanges(const std::filesystem::path& scopeRoot) {
    std::vector<metadata::KGPathRange> ranges;
    for (auto& root : graphCwdScopeRoots(scopeRoot)) {
        ranges.push_back(directoryRange(std::move(root)));
    }
    return ranges;
}

Result<std::unordered_set<std::string>>
buildGraphCwdScopePathSet(const std::filesystem::path& scopeRoot,
                          metadata::IMetadataRepository& repo) {
    std::unordered_set<std::string> paths;
    for (auto& root : graphCwdScopeRoots(scopeRoot)) {
        const auto range = directoryRange(root);
        metadata::DocumentQueryOptions options;
        options.pathPrefix = std::move(root);
        options.prefixIsDirectory = true;
        options.includeSubdirectories = true;
        options.limit = 0;

        auto result = repo.queryDocuments(options);
        if (!result) {
            return result.error();
        }
        for (const auto& document : result.value()) {
            auto path = normalizeGraphScopePath(document.filePath, scopeRoot);
            // The prefix query matches with LIKE; keep only paths really under the root.
            if (path >= range.lower && path < range.upper) {
                paths.insert(std::move(path));
            }
        }
    }
    return paths;
}

} // namespace yams::app::services
