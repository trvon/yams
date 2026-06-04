#pragma once

#include <algorithm>
#include <filesystem>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::daemon {
struct GraphNode;
} // namespace yams::daemon

namespace yams::cli {

std::vector<std::string> build_graph_file_node_candidates(
    const std::string& name, const std::filesystem::path& cwd = std::filesystem::current_path());

struct CliFilePresentation {
    std::string rawPath;
    std::string displayPath;
    std::string relationSummary;
    std::string graphExploreHint;
};

struct GraphNodeCliPresentation {
    std::string displayLabel;
    std::string displayType;
    bool hideByDefault{false};
    int sortRank{100};
};

// Extract the top relation name from a human-readable summary like "calls(3), includes(2)".
// Returns empty string if the summary is empty or malformed.
std::string extractTopRelation(const std::string& relationSummary);

// Format relation/count maps consistently across CLI and MCP surfaces.
// Example: calls(3), includes(2), +1 more
inline std::string formatRelationCounts(const std::unordered_map<std::string, std::size_t>& counts,
                                        std::size_t topLimit = 3) {
    if (counts.empty()) {
        return "-";
    }

    std::vector<std::pair<std::string, std::size_t>> sorted(counts.begin(), counts.end());
    std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
        if (a.second != b.second) {
            return a.second > b.second;
        }
        return a.first < b.first;
    });

    const std::size_t shown = std::min(topLimit, sorted.size());
    std::string out;
    for (std::size_t i = 0; i < shown; ++i) {
        if (!out.empty()) {
            out += ", ";
        }
        out += sorted[i].first + "(" + std::to_string(sorted[i].second) + ")";
    }
    if (sorted.size() > shown) {
        out += ", +" + std::to_string(sorted.size() - shown) + " more";
    }
    return out;
}

// Render a path for CLI display relative to cwd when possible.
std::string projectPathForCli(const std::string& rawPath,
                              const std::filesystem::path& cwd = std::filesystem::current_path());

// Build an agent-oriented graph explore hint string for a file path.
// Relation/depth inputs are accepted for call-site compatibility but the new explore flow
// infers relevant relationships from the query.
// Returns empty string if path is empty.
std::string
buildGraphExploreHint(const std::string& filePath, const std::string& topRelation = {},
                      int depth = 2,
                      const std::filesystem::path& cwd = std::filesystem::current_path());

// Build a graph label search hint from a path-like or symbolic query.
// Uses the filename stem when given a path, otherwise the raw token.
std::string
buildGraphSearchHint(const std::string& value,
                     const std::filesystem::path& cwd = std::filesystem::current_path());

// Normalize graph-node display so CLI surfaces prefer human labels over internal snapshot paths.
GraphNodeCliPresentation
describeGraphNodeForCli(const yams::daemon::GraphNode& node,
                        const std::filesystem::path& cwd = std::filesystem::current_path());

// Bundle the common CLI presentation details for a file: repo-relative path when possible,
// relation summary, and graph explore hint.
CliFilePresentation
describeFileForCli(const std::string& rawPath, std::string relationSummary = {},
                   const std::filesystem::path& cwd = std::filesystem::current_path(),
                   int depth = 2);

} // namespace yams::cli
