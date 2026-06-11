#pragma once

#include <yams/app/services/graph_context_service.hpp>
#include <yams/daemon/ipc/ipc_protocol_responses.h>

#include <nlohmann/json.hpp>
#include <filesystem>
#include <iosfwd>

namespace yams::cli {

app::services::GraphExploreResponse
mapGraphExploreResponseFromDaemon(const yams::daemon::GraphExploreResponse& response);

nlohmann::json makeGraphExploreJson(const app::services::GraphExploreResponse& response);

void renderGraphExploreMarkdown(std::ostream& out,
                                const app::services::GraphExploreResponse& response,
                                const std::filesystem::path& cwd = std::filesystem::current_path());

} // namespace yams::cli
