#include <yams/cli/graph_helpers.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <filesystem>
#include <optional>
#include <string_view>

#include <yams/daemon/ipc/ipc_protocol_responses.h>
#include <yams/metadata/path_utils.h>

namespace yams::cli {

std::vector<std::string> buildGraphDocumentNameCandidates(const std::string& name,
                                                          const std::filesystem::path& cwd) {
    std::vector<std::string> candidates;
    if (name.empty()) {
        return candidates;
    }

    try {
        const std::filesystem::path input(name);
        const auto resolved = input.is_relative() ? cwd / input : input;
        std::error_code ec;
        const bool pathLike = !input.is_relative() || input.has_parent_path() ||
                              std::filesystem::exists(resolved, ec);
        if (pathLike) {
            // Same normalization ingestion applies to stored paths (lexical + symlinks resolved).
            auto stored = yams::metadata::computePathDerivedValues(
                              resolved.lexically_normal().generic_string())
                              .normalizedPath;
            if (!stored.empty()) {
                candidates.push_back(std::move(stored));
            }
        }
    } catch (const std::exception& e) {
        spdlog::trace("graph: path normalization failed for '{}': {}", name, e.what());
    }

    if (std::ranges::find(candidates, name) == candidates.end()) {
        candidates.push_back(name);
    }
    return candidates;
}

std::string extractTopRelation(const std::string& relationSummary) {
    if (relationSummary.empty()) {
        return {};
    }
    // Find the first comma or parenthesis to isolate the first relation token.
    std::size_t endPos = relationSummary.find(',');
    if (endPos == std::string::npos) {
        endPos = relationSummary.size();
    }
    std::string firstToken = relationSummary.substr(0, endPos);
    // Trim whitespace
    auto start = firstToken.find_first_not_of(" \t\n\r");
    if (start == std::string::npos) {
        return {};
    }
    auto end = firstToken.find_last_not_of(" \t\n\r");
    std::string trimmed = firstToken.substr(start, end - start + 1);
    // Strip count in parentheses, e.g. "calls(3)"
    auto paren = trimmed.find('(');
    if (paren != std::string::npos) {
        trimmed = trimmed.substr(0, paren);
    }
    // Trim again
    start = trimmed.find_first_not_of(" \t\n\r");
    if (start == std::string::npos) {
        return {};
    }
    end = trimmed.find_last_not_of(" \t\n\r");
    return trimmed.substr(start, end - start + 1);
}

namespace {

std::string stripVersionSuffix(std::string type) {
    static constexpr std::string_view kSuffix = "_version";
    if (type.size() > kSuffix.size() &&
        type.compare(type.size() - kSuffix.size(), kSuffix.size(), kSuffix) == 0) {
        type.resize(type.size() - kSuffix.size());
    }
    return type;
}

bool isSyntheticGraphPath(std::string_view value) {
    return value.starts_with("snap:") || value.starts_with("path:") || value.starts_with("blob:") ||
           value.starts_with("document:");
}

bool looksLikeFilesystemPath(std::string_view value) {
    return !value.empty() && !isSyntheticGraphPath(value) &&
           (value.find('/') != std::string_view::npos ||
            value.find('\\') != std::string_view::npos);
}

std::optional<std::string> extractGraphNodePath(const yams::daemon::GraphNode& node) {
    if (!node.documentPath.empty()) {
        return node.documentPath;
    }
    if (node.properties.empty()) {
        return std::nullopt;
    }
    try {
        auto props = nlohmann::json::parse(node.properties);
        if (props.is_object()) {
            for (const char* key : {"file_path", "path", "document_path"}) {
                auto it = props.find(key);
                if (it != props.end() && it->is_string()) {
                    return it->get<std::string>();
                }
            }
        }
    } catch (const std::exception& e) {
        spdlog::trace("Graph helper could not parse node properties: {}", e.what());
    } catch (...) {
        spdlog::trace("Graph helper could not parse node properties");
    }
    return std::nullopt;
}

int graphNodeSortRank(std::string_view displayType) {
    if (displayType == "file") {
        return 0;
    }
    if (displayType == "function" || displayType == "class" || displayType == "struct" ||
        displayType == "enum" || displayType == "trait") {
        return 1;
    }
    if (displayType == "field") {
        return 2;
    }
    if (displayType == "document" || displayType == "blob" || displayType == "directory" ||
        displayType == "path") {
        return 3;
    }
    return 4;
}

bool graphNodeHiddenByDefault(std::string_view displayType) {
    return displayType == "document" || displayType == "blob" || displayType == "directory" ||
           displayType == "path" || displayType == "field";
}

} // namespace

std::filesystem::path invocationCwdForCli() {
    auto cwd = std::filesystem::current_path();
    std::error_code ec;
    auto canonical = std::filesystem::weakly_canonical(cwd, ec);
    if (!ec && !canonical.empty()) {
        return canonical;
    }
    return cwd;
}

std::string projectPathForCli(const std::string& rawPath, const std::filesystem::path& cwd) {
    if (rawPath.empty()) {
        return {};
    }

    try {
        if (rawPath.find('/') == std::string::npos && rawPath.find('\\') == std::string::npos) {
            return rawPath;
        }

        std::filesystem::path normalizedPath(rawPath);
        if (normalizedPath.is_relative()) {
            normalizedPath = cwd / normalizedPath;
        }
        normalizedPath = normalizedPath.lexically_normal();
        auto rel = normalizedPath.lexically_relative(cwd);
        if (!rel.empty()) {
            auto relString = rel.generic_string();
            if (!relString.empty() && relString.rfind("..", 0) != 0) {
                return relString;
            }
        }

        // Outside cwd: show the full path. A shortened tail such as
        // "src/foo.cpp" would name a different file under cwd.
        return normalizedPath.generic_string();
    } catch (const std::exception& e) {
        spdlog::trace("CLI path display normalization failed for '{}': {}", rawPath, e.what());
        return rawPath;
    }
}

std::string buildGraphExploreHint(const std::string& filePath, const std::string& topRelation,
                                  int depth, const std::filesystem::path& cwd) {
    (void)topRelation;
    (void)depth;
    if (filePath.empty()) {
        return {};
    }
    auto target = filePath;
    bool global = false;
    try {
        const auto original = (cwd / std::filesystem::path(filePath)).lexically_normal();
        const auto relative = original.lexically_relative(cwd.lexically_normal());
        global = relative.empty() || *relative.begin() == "..";
        const auto displayPath = projectPathForCli(filePath, cwd);
        // A display projection can shorten paths from another repository. Only use it
        // as an executable target when it still resolves to the original path.
        target = (cwd / std::filesystem::path(displayPath)).lexically_normal() == original
                     ? displayPath
                     : original.generic_string();
    } catch (const std::exception& e) {
        spdlog::trace("Graph explore hint path parsing failed for '{}': {}", filePath, e.what());
    }
    return "yams graph --explore \"" + target + "\"" + (global ? " --global" : "");
}

std::string buildGraphSearchHint(const std::string& value, const std::filesystem::path& cwd) {
    if (value.empty()) {
        return {};
    }

    std::string token = value;
    try {
        std::filesystem::path path(value);
        if (!path.empty()) {
            if (path.is_relative()) {
                path = cwd / path;
            }
            const auto filename = std::filesystem::path(value).filename().string();
            const auto stem = std::filesystem::path(filename).stem().string();
            if (!stem.empty()) {
                token = stem;
            } else if (!filename.empty()) {
                token = filename;
            }
        }
    } catch (const std::exception& e) {
        spdlog::trace("Graph search hint path parsing failed for '{}': {}", value, e.what());
    } catch (...) {
        spdlog::trace("Graph search hint path parsing failed for '{}'", value);
    }

    if (token.empty()) {
        return {};
    }
    return "yams graph --search \"*" + token + "*\"";
}

GraphNodeCliPresentation describeGraphNodeForCli(const yams::daemon::GraphNode& node,
                                                 const std::filesystem::path& cwd) {
    GraphNodeCliPresentation presentation;
    presentation.displayType = stripVersionSuffix(node.type);
    presentation.hideByDefault = graphNodeHiddenByDefault(presentation.displayType);
    presentation.sortRank = graphNodeSortRank(presentation.displayType);

    if (const auto extractedPath = extractGraphNodePath(node);
        extractedPath.has_value() && looksLikeFilesystemPath(*extractedPath) &&
        presentation.displayType == "file") {
        presentation.displayLabel = projectPathForCli(*extractedPath, cwd);
    } else if (!node.label.empty() &&
               (!looksLikeFilesystemPath(node.label) || presentation.displayType != "file")) {
        presentation.displayLabel = node.label;
    } else if (!node.label.empty()) {
        presentation.displayLabel = projectPathForCli(node.label, cwd);
    } else if (const auto extractedPath = extractGraphNodePath(node); extractedPath.has_value()) {
        presentation.displayLabel = looksLikeFilesystemPath(*extractedPath)
                                        ? projectPathForCli(*extractedPath, cwd)
                                        : *extractedPath;
    } else {
        presentation.displayLabel = node.nodeKey;
    }

    if (presentation.displayLabel.empty()) {
        presentation.displayLabel = node.nodeKey;
    }
    return presentation;
}

CliFilePresentation describeFileForCli(const std::string& rawPath, std::string relationSummary,
                                       const std::filesystem::path& cwd, int depth) {
    CliFilePresentation presentation;
    presentation.rawPath = rawPath;
    presentation.displayPath = projectPathForCli(rawPath, cwd);
    presentation.relationSummary = std::move(relationSummary);
    presentation.graphExploreHint = buildGraphExploreHint(
        rawPath, extractTopRelation(presentation.relationSummary), depth, cwd);
    return presentation;
}

} // namespace yams::cli
