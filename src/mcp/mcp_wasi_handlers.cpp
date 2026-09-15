#include <yams/mcp/mcp_server.h>

namespace yams::mcp {

boost::asio::awaitable<Result<MCPStatusResponse>>
MCPServer::handleGetStatus(const MCPStatusRequest& request) {
    (void)request;

    MCPStatusResponse response;
    response.running = true;
    response.ready = true;
    response.overallStatus = "wasi";
    response.lifecycleState = "in_process";
    response.lastError.clear();
    response.version.clear();
    response.uptimeSeconds = 0;
    response.requestsProcessed = 0;
    response.activeConnections = 0;
    response.memoryUsageMb = 0;
    response.cpuUsagePercent = 0.0;
    response.counters = json::object();
    response.readinessStates = json::object();
    response.initProgress = json::object();
    co_return response;
}

} // namespace yams::mcp
