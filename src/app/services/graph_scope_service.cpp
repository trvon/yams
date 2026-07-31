#include <yams/app/services/graph_scope_service.hpp>

#include <yams/core/magic_numbers.hpp>
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

bool shouldPruneGraphPath(std::string_view path) {
    std::string_view extension;
    if (const auto dot = path.find_last_of('.'); dot != std::string_view::npos) {
        extension = path.substr(dot + 1);
    }
    const auto category = yams::magic::getPruneCategory(path, extension);
    return category == yams::magic::PruneCategory::GitArtifacts ||
           yams::magic::matchesPruneGroup(category, "build") ||
           yams::magic::matchesPruneGroup(category, "packages") ||
           yams::magic::matchesPruneGroup(category, "ide-all");
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

std::vector<metadata::KGPathRange>
buildGraphCodeScopePathRanges(const std::filesystem::path& scopeRoot) {
    std::vector<metadata::KGPathRange> ranges;
    ranges.reserve(4);
    for (const auto* directory : {"src", "include"}) {
        const auto lexicalPath = (scopeRoot / directory).lexically_normal().generic_string();
        const auto canonicalPath = normalizeGraphScopePath(scopeRoot / directory, scopeRoot);
        for (const auto& path : {lexicalPath, canonicalPath}) {
            auto range = directoryRange(path);
            const auto duplicate = std::ranges::any_of(ranges, [&](const auto& existing) {
                return existing.lower == range.lower && existing.upper == range.upper;
            });
            if (!duplicate) {
                ranges.push_back(std::move(range));
            }
        }
    }
    return ranges;
}

Result<std::unordered_set<std::string>>
buildGraphCodeScopePathSet(const std::filesystem::path& scopeRoot,
                           metadata::IMetadataRepository& repo) {
    std::unordered_set<std::string> paths;
    for (const auto* directory : {"src", "include"}) {
        metadata::DocumentQueryOptions options;
        options.pathPrefix = normalizeGraphScopePath(scopeRoot / directory, scopeRoot);
        options.prefixIsDirectory = true;
        options.includeSubdirectories = true;
        options.limit = 0;

        auto result = repo.queryDocuments(options);
        if (!result) {
            return result.error();
        }
        for (const auto& document : result.value()) {
            auto path = normalizeGraphScopePath(document.filePath, scopeRoot);
            if (!shouldPruneGraphPath(path)) {
                paths.insert(std::move(path));
            }
        }
    }
    return paths;
}

} // namespace yams::app::services
