#include <yams/app/services/graph_scope_service.hpp>

#include <yams/core/magic_numbers.hpp>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/node_key_utils.h>
#include <yams/metadata/path_utils.h>

#include <nlohmann/json.hpp>

#include <optional>
#include <string_view>

namespace yams::app::services {

namespace {

std::optional<std::filesystem::path> extractGraphNodePath(const metadata::KGNode& node) {
    constexpr std::pair<std::string_view, std::size_t> kPathPrefixes[] = {
        {"path:file:", 10},
        {"path:dir:", 9},
        {"path:logical:", 13},
    };
    for (const auto& [prefix, offset] : kPathPrefixes) {
        if (node.nodeKey.starts_with(prefix)) {
            return std::filesystem::path(node.nodeKey.substr(offset));
        }
    }
    if (node.nodeKey.starts_with("path:")) {
        if (const auto timestampEnd = node.nodeKey.find("Z:", 5);
            timestampEnd != std::string::npos && timestampEnd + 2 < node.nodeKey.size()) {
            return std::filesystem::path(node.nodeKey.substr(timestampEnd + 2));
        }
        if (const auto separator = node.nodeKey.find(':', 5);
            separator != std::string::npos && separator + 1 < node.nodeKey.size()) {
            return std::filesystem::path(node.nodeKey.substr(separator + 1));
        }
    }
    if (const auto sourcePath = metadata::sourcePathFromNodeKey(node.nodeKey)) {
        return std::filesystem::path(*sourcePath);
    }
    if (!node.properties.has_value()) {
        return std::nullopt;
    }
    try {
        const auto properties = nlohmann::json::parse(*node.properties);
        for (const char* key : {"file_path", "path", "document_path"}) {
            if (const auto it = properties.find(key); it != properties.end() && it->is_string()) {
                return std::filesystem::path(it->get<std::string>());
            }
        }
    } catch (...) {
        return std::nullopt;
    }
    return std::nullopt;
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

std::vector<metadata::KGNode>
filterGraphNodesToPathSet(std::vector<metadata::KGNode> nodes,
                          const std::unordered_set<std::string>& scopedPaths,
                          const std::filesystem::path& scopeRoot) {
    std::vector<metadata::KGNode> filtered;
    filtered.reserve(nodes.size());
    for (auto& node : nodes) {
        const auto path = extractGraphNodePath(node);
        if (!path.has_value()) {
            continue;
        }
        if (scopedPaths.contains(normalizeGraphScopePath(*path, scopeRoot))) {
            filtered.push_back(std::move(node));
        }
    }
    return filtered;
}

} // namespace yams::app::services
