#include <yams/cli/graph_helpers.h>

#include <spdlog/spdlog.h>
#include <filesystem>
#include <unordered_set>

#include <yams/metadata/path_utils.h>

namespace yams::cli {

std::vector<std::string> build_graph_file_node_candidates(const std::string& name) {
    std::vector<std::string> candidates;
    std::unordered_set<std::string> seen;

    auto push_unique = [&](const std::string& value) {
        if (!value.empty() && seen.insert(value).second) {
            candidates.push_back(value);
        }
    };

    push_unique(name);

    try {
        auto derived = yams::metadata::computePathDerivedValues(name);
        if (!derived.normalizedPath.empty()) {
            push_unique(derived.normalizedPath);
        }
    } catch (const std::exception& e) {
        spdlog::trace("Path derivation failed for '{}': {}", name, e.what());
    }

    try {
        std::filesystem::path input(name);
        if (!input.empty()) {
            auto absolute = std::filesystem::absolute(input).lexically_normal();
            push_unique(absolute.string());
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

std::string buildGraphExploreHint(const std::string& filePath, const std::string& topRelation,
                                  int depth) {
    if (filePath.empty()) {
        return {};
    }
    std::string cmd = "yams graph --name \"" + filePath + "\"";
    if (!topRelation.empty()) {
        cmd += " -r " + topRelation;
    }
    cmd += " --depth " + std::to_string(depth);
    return cmd;
}

} // namespace yams::cli
