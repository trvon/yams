#include <yams/mcp/mcp_server.h>

#if !defined(YAMS_WASI)
#error "The exported yams_mcp dependency must propagate YAMS_WASI to consumers"
#endif

#include <memory>
#include <set>
#include <string>

namespace {

using json = nlohmann::json;

bool getResult(yams::mcp::MCPServer& server, const json& request, json& result) {
    auto response = server.handleRequestPublic(request);
    if (!response || !response.value().is_object() || !response.value().contains("result")) {
        return false;
    }
    result = response.value().at("result");
    return true;
}

} // namespace

int main() {
    yams::mcp::MCPServer server(std::unique_ptr<yams::mcp::ITransport>{});

    json result;
    if (!getResult(server,
                   {{"jsonrpc", "2.0"},
                    {"id", 1},
                    {"method", "initialize"},
                    {"params",
                     {{"protocolVersion", "2024-11-05"},
                      {"clientInfo", {{"name", "wasi-link-smoke"}, {"version", "1"}}},
                      {"capabilities", json::object()}}}},
                   result)) {
        return 1;
    }
    if (result.value("protocolVersion", "") != "2024-11-05") {
        return 2;
    }

    if (!getResult(
            server,
            {{"jsonrpc", "2.0"}, {"id", 2}, {"method", "tools/list"}, {"params", json::object()}},
            result)) {
        return 3;
    }
    std::set<std::string> toolNames;
    for (const auto& tool : result.value("tools", json::array())) {
        toolNames.insert(tool.value("name", ""));
    }
    if (toolNames != std::set<std::string>{"mcp.echo", "status"}) {
        return 4;
    }

    if (!getResult(server,
                   {{"jsonrpc", "2.0"},
                    {"id", 3},
                    {"method", "tools/call"},
                    {"params", {{"name", "mcp.echo"}, {"arguments", {{"text", "hello"}}}}}},
                   result)) {
        return 5;
    }
    if (!result.contains("content") || result["content"].empty() ||
        result["content"][0].value("text", "") != "echo: hello") {
        return 6;
    }

    if (!getResult(server,
                   {{"jsonrpc", "2.0"},
                    {"id", 4},
                    {"method", "tools/call"},
                    {"params", {{"name", "status"}, {"arguments", json::object()}}}},
                   result)) {
        return 7;
    }
    if (!result.contains("content") || result["content"].empty()) {
        return 8;
    }
    const auto status = json::parse(result["content"][0].value("text", ""), nullptr, false);
    if (status.is_discarded() || status.value("overallStatus", "") != "wasi" ||
        status.value("lifecycleState", "") != "in_process") {
        return 9;
    }

    if (!getResult(server,
                   {{"jsonrpc", "2.0"},
                    {"id", 5},
                    {"method", "resources/list"},
                    {"params", json::object()}},
                   result) ||
        !result.value("resources", json::array()).empty()) {
        return 10;
    }

    if (!getResult(
            server,
            {{"jsonrpc", "2.0"}, {"id", 6}, {"method", "prompts/list"}, {"params", json::object()}},
            result) ||
        !result.value("prompts", json::array()).empty()) {
        return 11;
    }

    auto unsupportedResource = server.handleRequestPublic({{"jsonrpc", "2.0"},
                                                           {"id", 7},
                                                           {"method", "resources/read"},
                                                           {"params", {{"uri", "yams://status"}}}});
    if (unsupportedResource || unsupportedResource.error().code != yams::ErrorCode::NotSupported) {
        return 12;
    }

    auto unsupportedPrompt = server.handleRequestPublic(
        {{"jsonrpc", "2.0"},
         {"id", 8},
         {"method", "prompts/get"},
         {"params", {{"name", "search_codebase"}, {"arguments", json::object()}}}});
    if (unsupportedPrompt || unsupportedPrompt.error().code != yams::ErrorCode::NotSupported) {
        return 13;
    }

    return server.isRunning() ? 14 : 0;
}
