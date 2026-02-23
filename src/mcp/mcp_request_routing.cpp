#include <yams/mcp/mcp_server.h>

#include <spdlog/spdlog.h>

namespace yams::mcp {

namespace {

spdlog::level::level_enum parseMcpLogLevel(const std::string& level) {
    if (level == "trace")
        return spdlog::level::trace;
    if (level == "debug")
        return spdlog::level::debug;
    if (level == "info" || level == "notice")
        return spdlog::level::info;
    if (level == "warning" || level == "warn")
        return spdlog::level::warn;
    if (level == "error")
        return spdlog::level::err;
    if (level == "critical" || level == "alert" || level == "emergency")
        return spdlog::level::critical;
    return spdlog::level::info;
}

} // namespace

std::optional<MessageResult>
MCPServer::dispatchCoreMethod(const json& id, const std::string& method, const json& params) {
    if (method == "initialize") {
        spdlog::debug("MCP handling initialize request with params: {}", params.dump());
        auto initResult = initialize(params);

        spdlog::debug("MCP initialize returned: is_null={}, is_object={}, empty={}, size={}",
                      initResult.is_null(), initResult.is_object(), initResult.empty(),
                      initResult.size());

        if (initResult.contains("_initialize_error")) {
            spdlog::error("MCP initialize failed with error");
            return MessageResult{json{{"jsonrpc", "2.0"},
                                      {"id", id},
                                      {"error",
                                       {{"code", initResult["code"]},
                                        {"message", initResult["message"]},
                                        {"data", initResult["data"]}}}}};
        }

        spdlog::debug("MCP initialize successful, protocol version: {}",
                      initResult.value("protocolVersion", "unknown"));
        auto response = createResponse(id, initResult);
        spdlog::debug("MCP createResponse returned: is_null={}, is_object={}, size={}",
                      response.is_null(), response.is_object(), response.size());
        return MessageResult{std::move(response)};
    }

    if (method == "notifications/cancelled") {
        nlohmann::json cancelId;
        if (params.contains("id")) {
            cancelId = params["id"];
        } else if (params.contains("requestId")) {
            cancelId = params["requestId"];
        } else {
            spdlog::warn("notifications/cancelled missing id/requestId");
            return MessageResult{Error{ErrorCode::InvalidArgument, "Missing id/requestId"}};
        }
        cancelRequest(cancelId);
        return MessageResult{Error{ErrorCode::Success, "notification"}};
    }

    if (method == "notifications/initialized") {
        markClientInitialized();
        return MessageResult{Error{ErrorCode::Success, "notification"}};
    }

    if (method == "ping") {
        return MessageResult{createResponse(id, json::object())};
    }

    if (method == "shutdown") {
        spdlog::debug("Shutdown request received, preparing for exit");
        shutdownRequested_ = true;
        return MessageResult{createResponse(id, json::object())};
    }

    if (method == "exit") {
        spdlog::debug("Exit request received");
        if (externalShutdown_)
            *externalShutdown_ = true;
        running_ = false;
        return MessageResult{Error{ErrorCode::Success, "notification"}};
    }

    if (method == "tools/list") {
        recordEarlyFeatureUse();
        return MessageResult{createResponse(id, listTools())};
    }

    if (method == "tools/call") {
        recordEarlyFeatureUse();
        const auto toolName = params.value("name", "");
        const auto toolArgs = params.value("arguments", json::object());
        spdlog::debug("MCP tool call: '{}' with args: {}", toolName, toolArgs.dump());
        sendProgress("tool", 0.0, std::string("calling ") + toolName);
        json raw = callTool(toolName, toolArgs);

        if (raw.is_object() && raw.contains("error")) {
            json err = raw["error"];
            return MessageResult{
                json{{"jsonrpc", protocol::JSONRPC_VERSION}, {"error", err}, {"id", id}}};
        }

        sendProgress("tool", 100.0, std::string("completed ") + toolName);
        return MessageResult{createResponse(id, raw)};
    }

    if (method == "resources/list") {
        return MessageResult{createResponse(id, listResources())};
    }

    if (method == "resources/read") {
        std::string uri = params.value("uri", "");
        return MessageResult{createResponse(id, readResource(uri))};
    }

    if (method == "prompts/list") {
        return MessageResult{createResponse(id, listPrompts())};
    }

    if (method == "prompts/get") {
        std::string name = params.value("name", "");
        json args = params.value("arguments", json::object());
        return MessageResult{handlePromptGet(id, name, args)};
    }

    if (method == "logging/setLevel") {
        if (!areYamsExtensionsEnabled()) {
            return MessageResult{
                json{{"jsonrpc", protocol::JSONRPC_VERSION},
                     {"error",
                      {{"code", -32601},
                       {"message", "Method not available (extensions disabled): " + method}}},
                     {"id", id}}};
        }
        const auto level = params.value("level", "info");
        spdlog::set_level(parseMcpLogLevel(level));
        return MessageResult{createResponse(id, json::object())};
    }

    return std::nullopt;
}

} // namespace yams::mcp
