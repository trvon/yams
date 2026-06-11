#pragma once

#include <yams/core/types.h>
#include <yams/daemon/ipc/ipc_protocol_responses.h>

#include <nlohmann/json_fwd.hpp>

#include <cstdint>
#include <filesystem>
#include <iosfwd>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::cli {

struct GraphQueryRenderOptions {
    bool jsonOutput{false};
    bool verbose{false};
    std::string outputFormat{"table"};
    std::filesystem::path cwd = std::filesystem::current_path();
};

struct DocumentGraphRenderOptions {
    bool jsonOutput{false};
    int depth{1};
    std::filesystem::path cwd = std::filesystem::current_path();
};

std::optional<std::filesystem::path> extractGraphNodePath(const yams::daemon::GraphNode& node);
std::string displayGraphNodePath(const yams::daemon::GraphNode& node);

nlohmann::json
makeGraphNodeJson(const yams::daemon::GraphNode& node, bool includeDistance = false,
                  const std::unordered_map<std::int64_t, std::string>* traversalHints = nullptr);

void renderGraphNodePropertiesSection(std::ostream& out,
                                      const std::vector<yams::daemon::GraphNode>& nodes);

std::unordered_map<std::int64_t, std::string>
buildTraversalRelationHints(const yams::daemon::GraphQueryResponse& resp);

Result<void> renderGraphQueryResponse(std::ostream& out,
                                      const yams::daemon::GraphQueryResponse& resp,
                                      const GraphQueryRenderOptions& options);

Result<void> renderDocumentGraphResponse(std::ostream& out, const yams::daemon::GetResponse& resp,
                                         const DocumentGraphRenderOptions& options);

} // namespace yams::cli
