#include <yams/cli/graph_helpers.h>

#include <spdlog/spdlog.h>
#include <filesystem>
#include <nlohmann/json.hpp>
#include <optional>
#include <string_view>
#include <unordered_set>

#include <yams/metadata/path_utils.h>

namespace yams::cli {

std::vector<std::string> build_graph_file_node_candidates(const std::string& name,
                                                          const std::filesystem::path& cwd) {
    std::vector<std::string> candidates;
    std::unordered_set<std::string> seen;

    auto push_unique = [&](const std::string& value) {
        if (!value.empty() && seen.insert(value).second) {
            candidates.push_back(value);
        }
    };

    push_unique(name);

    try {
        std::filesystem::path input(name);
        if (!input.empty()) {
            if (input.is_relative()) {
                input = cwd / input;
            }
            auto absolute = std::filesystem::absolute(input).lexically_normal();
            push_unique(absolute.string());
            std::error_code ec;
            auto canonical = std::filesystem::weakly_canonical(input, ec);
            if (!ec) {
                push_unique(canonical.lexically_normal().string());
            }
        }
    } catch (const std::exception& e) {
        // Best-effort normalization only - path operations can fail on invalid inputs
        spdlog::trace("Path normalization failed for '{}': {}", name, e.what());
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

bool shouldIncludeRelationInHint(std::string_view relation) {
    static const std::unordered_set<std::string> kHighSignalRelations = {
        "calls",      "called_by",  "includes",      "imports",   "inherits",
        "implements", "references", "referenced_by", "overrides", "instantiates",
    };
    return kHighSignalRelations.contains(std::string(relation));
}

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
    } catch (...) {
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

namespace {

std::vector<std::string> splitPathSegments(const std::filesystem::path& path) {
    std::vector<std::string> segments;
    for (const auto& part : path) {
        auto s = part.generic_string();
        if (!s.empty() && s != "/") {
            segments.push_back(std::move(s));
        }
    }
    return segments;
}

} // namespace

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

        const auto cwdSegs = splitPathSegments(cwd.lexically_normal());
        const auto pathSegs = splitPathSegments(normalizedPath.lexically_normal());
        if (!cwdSegs.empty() && !pathSegs.empty()) {
            std::size_t bestLen = 0;
            std::size_t bestPathStart = 0;
            for (std::size_t len = std::min(cwdSegs.size(), pathSegs.size()); len >= 2; --len) {
                const std::size_t cwdStart = cwdSegs.size() - len;
                for (std::size_t pathStart = 0; pathStart + len <= pathSegs.size(); ++pathStart) {
                    bool match = true;
                    for (std::size_t i = 0; i < len; ++i) {
                        if (cwdSegs[cwdStart + i] != pathSegs[pathStart + i]) {
                            match = false;
                            break;
                        }
                    }
                    if (match) {
                        bestLen = len;
                        bestPathStart = pathStart;
                        break;
                    }
                }
                if (bestLen > 0 || len == 2) {
                    break;
                }
            }

            if (bestLen > 0) {
                std::filesystem::path projected;
                for (std::size_t i = bestPathStart + bestLen; i < pathSegs.size(); ++i) {
                    projected /= pathSegs[i];
                }
                if (!projected.empty()) {
                    return projected.generic_string();
                }
            }

            static const std::unordered_set<std::string> kProjectAnchors = {
                "src", "include", "tests", "docs", "examples", "benchmarks",
            };
            for (std::size_t i = 0; i < pathSegs.size(); ++i) {
                if (!kProjectAnchors.contains(pathSegs[i])) {
                    continue;
                }
                std::filesystem::path anchored;
                for (std::size_t j = i; j < pathSegs.size(); ++j) {
                    anchored /= pathSegs[j];
                }
                if (!anchored.empty()) {
                    return anchored.generic_string();
                }
            }
        }

        return normalizedPath.generic_string();
    } catch (const std::exception& e) {
        spdlog::trace("CLI path display normalization failed for '{}': {}", rawPath, e.what());
        return rawPath;
    }
}

std::string buildGraphExploreHint(const std::string& filePath, const std::string& topRelation,
                                  int depth, const std::filesystem::path& cwd) {
    if (filePath.empty()) {
        return {};
    }
    const auto displayPath = projectPathForCli(filePath, cwd);
    std::string cmd = "yams graph --name \"" + displayPath + "\"";
    if (!topRelation.empty() && shouldIncludeRelationInHint(topRelation)) {
        cmd += " -r " + topRelation;
    }
    cmd += " --depth " + std::to_string(depth);
    return cmd;
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
    } catch (...) {
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
