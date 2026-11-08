// ============================================================================
// MCP Apps Extension - Tool Metadata Tests
// PBI-083: MCP Apps Support - Phase 1
//
// Test IDs: mcp-apps-tool-01 through mcp-apps-tool-07
// ============================================================================

#include <nlohmann/json.hpp>
#include <catch2/catch_test_macros.hpp>
#include <yams/mcp/mcp_server.h>

#include <set>
#include <string>

using namespace yams::mcp;
using json = nlohmann::json;

namespace {

class NullTransport : public yams::mcp::ITransport {
public:
    void send(const json&) override {}
    yams::mcp::MessageResult receive() override {
        return yams::Error{yams::ErrorCode::NotImplemented, "Null transport"};
    }
    bool isConnected() const override { return false; }
    void close() override {}
    yams::mcp::TransportState getState() const override {
        return yams::mcp::TransportState::Disconnected;
    }
};

} // namespace

TEST_CASE("MCP Apps Tool - UI Linkage With Support",
          "[mcp][apps][tool][mcp-apps-tool-01][phase1]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2025-11-25"},
                          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                          {"capabilities",
                           {{"extensions",
                             {{"io.modelcontextprotocol/ui",
                               {{"mimeTypes", json::array({"text/html;profile=mcp-app"})}}}}}}}}}};

    auto initResponse = server->handleRequestPublic(initRequest);
    REQUIRE(initResponse.has_value());

    json toolsRequest = {
        {"jsonrpc", "2.0"}, {"id", 2}, {"method", "tools/list"}, {"params", json::object()}};

    auto toolsResponse = server->handleRequestPublic(toolsRequest);
    REQUIRE(toolsResponse.has_value());

    json result = toolsResponse.value();
    REQUIRE(result.contains("result"));
    REQUIRE(result["result"].contains("tools"));

    // Check that at least one tool has UI metadata
    bool foundUITool = false;
    for (const auto& tool : result["result"]["tools"]) {
        if (tool.contains("_meta") && tool["_meta"].contains("ui")) {
            foundUITool = true;
            CHECK(tool["_meta"]["ui"].contains("resourceUri"));
            std::string uri = tool["_meta"]["ui"]["resourceUri"].get<std::string>();
            CHECK(uri.find("ui://") == 0);
        }
    }

    CHECK(foundUITool);
}

TEST_CASE("MCP Apps Tool - UI Linkage Without Support",
          "[mcp][apps][tool][mcp-apps-tool-02][phase1]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2025-11-25"},
                          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                          {"capabilities", json::object()}}}};

    auto initResponse = server->handleRequestPublic(initRequest);
    REQUIRE(initResponse.has_value());

    json toolsRequest = {
        {"jsonrpc", "2.0"}, {"id", 2}, {"method", "tools/list"}, {"params", json::object()}};

    auto toolsResponse = server->handleRequestPublic(toolsRequest);
    REQUIRE(toolsResponse.has_value());

    json result = toolsResponse.value();
    REQUIRE(result.contains("result"));
    REQUIRE(result["result"].contains("tools"));

    // Check that no tools have UI metadata
    for (const auto& tool : result["result"]["tools"]) {
        if (tool.contains("_meta")) {
            CHECK_FALSE(tool["_meta"].contains("ui"));
        }
    }
}

TEST_CASE("MCP Apps Tool - Visibility Model and App",
          "[mcp][apps][tool][mcp-apps-tool-03][phase1]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2025-11-25"},
                          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                          {"capabilities",
                           {{"extensions",
                             {{"io.modelcontextprotocol/ui",
                               {{"mimeTypes", json::array({"text/html;profile=mcp-app"})}}}}}}}}}};

    auto initResponse = server->handleRequestPublic(initRequest);
    REQUIRE(initResponse.has_value());

    json toolsRequest = {
        {"jsonrpc", "2.0"}, {"id", 2}, {"method", "tools/list"}, {"params", json::object()}};

    auto toolsResponse = server->handleRequestPublic(toolsRequest);
    REQUIRE(toolsResponse.has_value());

    json result = toolsResponse.value();
    REQUIRE(result.contains("result"));
    REQUIRE(result["result"].contains("tools"));

    // MCPServer links exactly these tools to the dashboard UI (listTools shaping in
    // mcp_server.cpp); "query" is the code-mode composite.
    const std::set<std::string> linkedTools = {"search", "grep", "graph", "query"};
    const json expectedVisibility = json::array({"model", "app"});

    int linkedSeen = 0;
    for (const auto& tool : result["result"]["tools"]) {
        REQUIRE(tool.contains("name"));
        const auto name = tool["name"].get<std::string>();
        INFO("tool " << name);

        if (!linkedTools.contains(name)) {
            if (tool.contains("_meta")) {
                CHECK_FALSE(tool["_meta"].contains("ui"));
                CHECK_FALSE(tool["_meta"].contains("ui/visibility"));
            }
            continue;
        }

        ++linkedSeen;
        REQUIRE(tool.contains("_meta"));
        const auto& meta = tool["_meta"];
        REQUIRE(meta.contains("ui"));
        CHECK(meta.at("ui").at("resourceUri") == "ui://yams/dashboard");
        CHECK(meta.at("ui").at("visibility") == expectedVisibility);
        CHECK(meta.at("ui/resourceUri") == "ui://yams/dashboard");
        CHECK(meta.at("ui/visibility") == expectedVisibility);
    }

    // At least one linked tool is listed in every tool mode.
    CHECK(linkedSeen > 0);
}
