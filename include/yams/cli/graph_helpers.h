#pragma once

#include <algorithm>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::cli {

std::vector<std::string> build_graph_file_node_candidates(const std::string& name);

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

// Build a graph explore hint string for a file path.
// If topRelation is empty, suggests --depth <depth>.
// If topRelation is set, suggests -r <topRelation> --depth <depth>.
// Returns empty string if path is empty.
std::string buildGraphExploreHint(const std::string& filePath, const std::string& topRelation = {},
                                  int depth = 2);

} // namespace yams::cli
