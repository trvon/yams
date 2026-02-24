#include <yams/mcp/mcp_server.h>

#if !defined(YAMS_WASI)
#include <yams/daemon/client/global_io_context.h>
#endif

#include <spdlog/spdlog.h>

#include <future>
#include <string>

namespace yams::mcp {

namespace {

json normalizeToolResultOrError(const std::string& name, const json& arguments, json result) {
    // Normalize errors: if registry returned content-based error, map to JSON-RPC error.
    // Preserve the full tool result in error.data so clients can inspect structuredContent.
    if (result.is_object() && result.value("isError", false)) {
        std::string msg = "Tool error";
        try {
            if (result.contains("content") && result["content"].is_array() &&
                !result["content"].empty()) {
                const auto& item = result["content"][0];
                msg = item.value("text", msg);
            }
        } catch (...) {
            // keep default
        }

        int code = -32602; // Invalid params by default
        if (msg.rfind("Unknown tool:", 0) == 0) {
            code = -32601; // Method not found
        }

        json data = json::object();
        data["tool"] = name;
        if (arguments.is_object()) {
            json keys = json::array();
            for (const auto& it : arguments.items()) {
                keys.push_back(it.key());
            }
            data["argumentKeys"] = std::move(keys);
        }
        data["toolResult"] = result;

        return json{{"error", json{{"code", code}, {"message", msg}, {"data", data}}}};
    }

    // Ensure result is tool-result shaped (content array) when not error
    if (result.is_object() && result.contains("content")) {
        return result; // already wrapped
    }

    // Legacy/plain result: wrap into content per MCP spec
    return yams::mcp::wrapToolResult(result, /*isError=*/false);
}

} // namespace

json MCPServer::callTool(const std::string& name, const json& arguments) {
    spdlog::info("MCP callTool invoked: name='{}', arguments={}", name, arguments.dump());

    // Resolve registry: try public toolRegistry_ first (composite tools: query/execute/session),
    // then fall back to internalRegistry_ (individual tools used for internal dispatch).
    ToolRegistry* reg = nullptr;
    if (toolRegistry_ && toolRegistry_->hasTool(name)) {
        reg = toolRegistry_.get();
    } else if (internalRegistry_ && internalRegistry_->hasTool(name)) {
        reg = internalRegistry_.get();
    } else if (toolRegistry_) {
        reg = toolRegistry_.get(); // let it produce "Unknown tool" error
    }
    if (!reg) {
        return {{"error", {{"code", -32603}, {"message", "Tool registry not initialized"}}}};
    }

    auto task = reg->callTool(name, arguments);

    boost::asio::any_io_executor exec;
#if defined(YAMS_WASI)
    exec = boost::asio::system_executor();
#else
    exec = yams::daemon::GlobalIOContext::instance().get_io_context().get_executor();
#endif
    auto promise = std::make_shared<std::promise<json>>();
    auto future = promise->get_future();

    boost::asio::co_spawn(
        exec,
        [task = std::move(task), promise]() mutable -> boost::asio::awaitable<void> {
            try {
                auto result = co_await std::move(task);
                promise->set_value(result);
            } catch (...) {
                promise->set_exception(std::current_exception());
            }
            co_return;
        },
        boost::asio::detached);

    try {
        json result = future.get();

        spdlog::debug("MCP tool '{}' returned: {}", name, result.dump());

        return normalizeToolResultOrError(name, arguments, std::move(result));

    } catch (const std::exception& e) {
        spdlog::error("MCP tool '{}' threw exception: {}", name, e.what());
        return {{"error",
                 {{"code", -32603}, {"message", std::string("Tool call failed: ") + e.what()}}}};
    }
}

boost::asio::awaitable<json> MCPServer::callToolAsync(const std::string& name,
                                                      const json& arguments) {
    spdlog::debug("MCP callToolAsync invoked: '{}'", name);

    // Resolve registry: try public toolRegistry_ first (composite tools: query/execute/session),
    // then fall back to internalRegistry_ (individual tools).
    ToolRegistry* reg = nullptr;
    if (toolRegistry_ && toolRegistry_->hasTool(name)) {
        reg = toolRegistry_.get();
    } else if (internalRegistry_ && internalRegistry_->hasTool(name)) {
        reg = internalRegistry_.get();
    } else if (toolRegistry_) {
        reg = toolRegistry_.get();
    }
    if (!reg) {
        co_return json{{"error", {{"code", -32603}, {"message", "Tool registry not initialized"}}}};
    }

    json result = co_await reg->callTool(name, arguments);
    co_return normalizeToolResultOrError(name, arguments, std::move(result));
}

} // namespace yams::mcp
