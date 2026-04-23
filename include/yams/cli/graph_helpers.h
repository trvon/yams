#pragma once

#include <string>
#include <vector>

namespace yams::cli {

std::vector<std::string> build_graph_file_node_candidates(const std::string& name);

// Extract the top relation name from a human-readable summary like "calls(3), includes(2)".
// Returns empty string if the summary is empty or malformed.
std::string extractTopRelation(const std::string& relationSummary);

// Build a graph explore hint string for a file path.
// If topRelation is empty, suggests --depth <depth>.
// If topRelation is set, suggests -r <topRelation> --depth <depth>.
// Returns empty string if path is empty.
std::string buildGraphExploreHint(const std::string& filePath, const std::string& topRelation = {},
                                  int depth = 2);

} // namespace yams::cli
