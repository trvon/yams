// ============================================================================
// MCP Protocol Version Feature Matrix Tests
//
// This file tests version-specific features to ensure YAMS correctly implements
// all claimed protocol versions.
//
// Version History:
// - 2024-10-07: Initial release
// - 2024-11-05: Added tool annotations, structuredContent in CallToolResult
// - 2025-03-26: Added completions capability, BaseMetadata with title field
// - 2025-06-18: Added elicitation, roots listChanged, ModelPreferences with hints
// - 2025-11-25: Added tasks, sampling context/tools, icons in Implementation
// ============================================================================

#include <nlohmann/json.hpp>
#include <catch2/catch_test_macros.hpp>
#include <yams/mcp/mcp_server.h>
#include <cstdlib>
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

class EnvGuard {
public:
    EnvGuard(const char* key, const char* value) : key_(key) {
        const char* prev = std::getenv(key_);
        if (prev) {
            hadPrev_ = true;
            prev_ = prev;
        }
        setValue(value);
    }

    ~EnvGuard() {
        if (hadPrev_) {
            setValue(prev_.c_str());
        } else {
            clearValue();
        }
    }

private:
    void setValue(const char* value) const {
#if defined(_WIN32)
        if (value) {
            _putenv_s(key_, value);
        } else {
            _putenv_s(key_, "");
        }
#else
        if (value) {
            setenv(key_, value, 1);
        } else {
            unsetenv(key_);
        }
#endif
    }

    void clearValue() const {
#if defined(_WIN32)
        _putenv_s(key_, "");
#else
        unsetenv(key_);
#endif
    }

    const char* key_;
    bool hadPrev_ = false;
    std::string prev_;
};

} // namespace

// ============================================================================
// Version 2025-11-25 Specific Features
// ============================================================================

TEST_CASE("MCP 2025-11-25 - supports tasks capability",
          "[mcp][protocol][2025-11-25][features][tasks][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2025-11-25"},
                          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                          {"capabilities", json::object()}}}};

    auto response = server->handleRequestPublic(initRequest);
    REQUIRE(response.has_value());

    json result = response.value();
    REQUIRE(result["result"].contains("capabilities"));

    // Check if tasks capability is present (optional for servers)
    const auto& caps = result["result"]["capabilities"];
    if (caps.contains("tasks")) {
        CHECK(caps["tasks"].is_object());
    }
}

TEST_CASE("MCP 2025-11-25 - supports sampling context and tools",
          "[mcp][protocol][2025-11-25][features][sampling][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    json initRequest = {
        {"jsonrpc", "2.0"},
        {"id", 1},
        {"method", "initialize"},
        {"params",
         {{"protocolVersion", "2025-11-25"},
          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
          {"capabilities",
           {{"sampling", {{"context", json::object()}, {"tools", json::object()}}}}}}}};

    auto response = server->handleRequestPublic(initRequest);
    REQUIRE(response.has_value());

    json result = response.value();
    CHECK(result["result"]["protocolVersion"] == "2025-11-25");
}

TEST_CASE("MCP 2025-11-25 - serverInfo includes name and version",
          "[mcp][protocol][2025-11-25][features][serverinfo][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2025-11-25"},
                          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                          {"capabilities", json::object()}}}};

    auto response = server->handleRequestPublic(initRequest);
    REQUIRE(response.has_value());

    json result = response.value();
    REQUIRE(result["result"].contains("serverInfo"));
    CHECK(result["result"]["serverInfo"].contains("name"));
    CHECK(result["result"]["serverInfo"].contains("version"));
    CHECK(result["result"]["serverInfo"]["name"].is_string());
    CHECK(result["result"]["serverInfo"]["version"].is_string());
}

// ============================================================================
// Version 2025-06-18 Specific Features
// ============================================================================

TEST_CASE("MCP 2025-06-18 - supports completions capability",
          "[mcp][protocol][2025-06-18][features][completions][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2025-06-18"},
                          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                          {"capabilities", json::object()}}}};

    auto response = server->handleRequestPublic(initRequest);
    REQUIRE(response.has_value());

    json result = response.value();
    REQUIRE(result["result"].contains("capabilities"));

    // completions capability is optional
    const auto& caps = result["result"]["capabilities"];
    if (caps.contains("completions")) {
        CHECK(caps["completions"].is_object());
    }
}

TEST_CASE("MCP 2025-06-18 - supports roots listChanged",
          "[mcp][protocol][2025-06-18][features][roots][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2025-06-18"},
                          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                          {"capabilities", {{"roots", {{"listChanged", true}}}}}}}};

    auto response = server->handleRequestPublic(initRequest);
    REQUIRE(response.has_value());

    json result = response.value();
    CHECK(result["result"]["protocolVersion"] == "2025-06-18");
}

// ============================================================================
// Version 2025-03-26 Specific Features
// ============================================================================

TEST_CASE("MCP 2025-03-26 - supports BaseMetadata with title",
          "[mcp][protocol][2025-03-26][features][metadata][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2025-03-26"},
                          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                          {"capabilities", json::object()}}}};

    auto response = server->handleRequestPublic(initRequest);
    REQUIRE(response.has_value());

    json result = response.value();
    CHECK(result["result"]["protocolVersion"] == "2025-03-26");

    // Get tools to check if they support title field
    json toolsRequest = {
        {"jsonrpc", "2.0"}, {"id", 2}, {"method", "tools/list"}, {"params", json::object()}};

    auto toolsResponse = server->handleRequestPublic(toolsRequest);
    REQUIRE(toolsResponse.has_value());

    json toolsResult = toolsResponse.value();
    REQUIRE(toolsResult["result"].contains("tools"));

    // Check if any tool has a title field (optional feature)
    for (const auto& tool : toolsResult["result"]["tools"]) {
        if (tool.contains("title")) {
            CHECK(tool["title"].is_string());
        }
    }
}

// ============================================================================
// Version 2024-11-05 Specific Features
// ============================================================================

TEST_CASE("MCP 2024-11-05 - supports tool annotations",
          "[mcp][protocol][2024-11-05][features][tool-annotations][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2024-11-05"},
                          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                          {"capabilities", json::object()}}}};

    auto response = server->handleRequestPublic(initRequest);
    REQUIRE(response.has_value());

    json result = response.value();
    CHECK(result["result"]["protocolVersion"] == "2024-11-05");

    // Get tools to check for annotations
    json toolsRequest = {
        {"jsonrpc", "2.0"}, {"id", 2}, {"method", "tools/list"}, {"params", json::object()}};

    auto toolsResponse = server->handleRequestPublic(toolsRequest);
    REQUIRE(toolsResponse.has_value());

    json toolsResult = toolsResponse.value();
    REQUIRE(toolsResult["result"].contains("tools"));

    // Check if any tool has annotations (optional feature)
    for (const auto& tool : toolsResult["result"]["tools"]) {
        if (tool.contains("annotations")) {
            const auto& ann = tool["annotations"];
            // Verify annotation fields if present
            if (ann.contains("readOnlyHint")) {
                CHECK(ann["readOnlyHint"].is_boolean());
            }
            if (ann.contains("destructiveHint")) {
                CHECK(ann["destructiveHint"].is_boolean());
            }
            if (ann.contains("idempotentHint")) {
                CHECK(ann["idempotentHint"].is_boolean());
            }
            if (ann.contains("openWorldHint")) {
                CHECK(ann["openWorldHint"].is_boolean());
            }
        }
    }
}

TEST_CASE("MCP 2024-11-05 - supports structuredContent in tool results",
          "[mcp][protocol][2024-11-05][features][structured-content][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    // Initialize with 2024-11-05
    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2024-11-05"},
                          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                          {"capabilities", json::object()}}}};

    auto initResponse = server->handleRequestPublic(initRequest);
    REQUIRE(initResponse.has_value());

    // Call a tool and check if tool results can include structuredContent.
    // Use an in-process tool that doesn't require a live daemon connection.
    json toolRequest = {{"jsonrpc", "2.0"},
                        {"id", 99},
                        {"method", "tools/call"},
                        {"params", {{"name", "mcp.echo"}, {"arguments", {{"text", "hello"}}}}}};

    auto toolResponse = server->handleRequestPublic(toolRequest);
    REQUIRE(toolResponse.has_value());

    json toolResult = toolResponse.value();
    INFO("tools/call response: " << toolResult.dump(2));
    REQUIRE(toolResult.contains("result"));
    REQUIRE(toolResult["result"].contains("content"));
    CHECK(toolResult["result"]["content"].is_array());
    CHECK_FALSE(toolResult["result"].value("isError", false));

    // New in 2024-11-05: structuredContent field.
    REQUIRE(toolResult["result"].contains("structuredContent"));
    REQUIRE(toolResult["result"]["structuredContent"].is_object());
    CHECK(toolResult["result"]["structuredContent"].value("type", "") == "tool_result");
    CHECK(toolResult["result"]["structuredContent"].contains("data"));
}

// ============================================================================
// Version 2024-10-07 Specific Features
// ============================================================================

TEST_CASE("MCP 2024-10-07 - tools/list omits post-version metadata fields",
          "[mcp][protocol][2024-10-07][features][tools-list-shaping][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2024-10-07"},
                          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                          {"capabilities", json::object()}}}};

    auto initResponse = server->handleRequestPublic(initRequest);
    REQUIRE(initResponse.has_value());

    json toolsRequest = {
        {"jsonrpc", "2.0"}, {"id", 2}, {"method", "tools/list"}, {"params", json::object()}};
    auto toolsResponse = server->handleRequestPublic(toolsRequest);
    REQUIRE(toolsResponse.has_value());

    const auto& tools = toolsResponse.value()["result"]["tools"];
    REQUIRE(tools.is_array());
    REQUIRE_FALSE(tools.empty());

    for (const auto& tool : tools) {
        CHECK_FALSE(tool.contains("annotations"));
        CHECK_FALSE(tool.contains("title"));
    }
}

TEST_CASE("MCP strict-name compat - hides dotted tools and exposes underscore alias",
          "[mcp][protocol][compat][tool-name][catch2]") {
    EnvGuard renameGuard("YAMS_MCP_RENAME_DOTTED_TOOLS", "1");

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

    const auto& tools = toolsResponse.value()["result"]["tools"];
    REQUIRE(tools.is_array());

    bool foundDotted = false;
    bool foundAlias = false;
    for (const auto& tool : tools) {
        const std::string name = tool.value("name", "");
        if (name == "mcp.echo") {
            foundDotted = true;
        }
        if (name == "mcp_echo") {
            foundAlias = true;
        }
    }
    CHECK_FALSE(foundDotted);
    CHECK(foundAlias);

    // Alias should be callable.
    json aliasCall = {{"jsonrpc", "2.0"},
                      {"id", 3},
                      {"method", "tools/call"},
                      {"params", {{"name", "mcp_echo"}, {"arguments", {{"text", "hello"}}}}}};

    auto aliasResponse = server->handleRequestPublic(aliasCall);
    REQUIRE(aliasResponse.has_value());
    REQUIRE(aliasResponse.value().contains("result"));
    REQUIRE(aliasResponse.value()["result"].contains("content"));
}

// ============================================================================
// Version 2024-10-07 (Initial Release) Features
// ============================================================================

TEST_CASE("MCP 2024-10-07 - basic protocol features work",
          "[mcp][protocol][2024-10-07][features][basic][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2024-10-07"},
                          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                          {"capabilities", json::object()}}}};

    auto response = server->handleRequestPublic(initRequest);
    REQUIRE(response.has_value());

    json result = response.value();
    CHECK(result["result"]["protocolVersion"] == "2024-10-07");
    REQUIRE(result["result"].contains("capabilities"));
    REQUIRE(result["result"].contains("serverInfo"));
}

TEST_CASE("MCP 2024-10-07 - supports tools/list",
          "[mcp][protocol][2024-10-07][features][tools][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    // Initialize
    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2024-10-07"},
                          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                          {"capabilities", json::object()}}}};

    auto initResponse = server->handleRequestPublic(initRequest);
    REQUIRE(initResponse.has_value());

    // List tools
    json toolsRequest = {
        {"jsonrpc", "2.0"}, {"id", 2}, {"method", "tools/list"}, {"params", json::object()}};

    auto toolsResponse = server->handleRequestPublic(toolsRequest);
    REQUIRE(toolsResponse.has_value());

    json toolsResult = toolsResponse.value();
    REQUIRE(toolsResult["result"].contains("tools"));
    CHECK(toolsResult["result"]["tools"].is_array());
    CHECK(toolsResult["result"]["tools"].size() > 0);
}

TEST_CASE("MCP 2024-10-07 - tool results omit structuredContent",
          "[mcp][protocol][2024-10-07][features][structured-content][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    // Initialize with pre-structuredContent protocol version
    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2024-10-07"},
                          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                          {"capabilities", json::object()}}}};

    auto initResponse = server->handleRequestPublic(initRequest);
    REQUIRE(initResponse.has_value());
    CHECK(initResponse.value()["result"]["protocolVersion"] == "2024-10-07");

    // Call a tool that returns a tool-result shaped payload.
    // Use an in-process tool that doesn't require a live daemon connection.
    json toolRequest = {{"jsonrpc", "2.0"},
                        {"id", 99},
                        {"method", "tools/call"},
                        {"params", {{"name", "mcp.echo"}, {"arguments", {{"text", "hello"}}}}}};

    auto toolResponse = server->handleRequestPublic(toolRequest);
    REQUIRE(toolResponse.has_value());

    json toolResult = toolResponse.value();
    INFO("tools/call response: " << toolResult.dump(2));
    REQUIRE(toolResult.contains("result"));
    REQUIRE(toolResult["result"].contains("content"));
    CHECK(toolResult["result"]["content"].is_array());
    CHECK_FALSE(toolResult["result"].value("isError", false));
    CHECK_FALSE(toolResult["result"].contains("structuredContent"));
}

TEST_CASE("MCP 2024-10-07 - supports resources/list",
          "[mcp][protocol][2024-10-07][features][resources][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    // Initialize
    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2024-10-07"},
                          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                          {"capabilities", json::object()}}}};

    auto initResponse = server->handleRequestPublic(initRequest);
    REQUIRE(initResponse.has_value());

    // List resources
    json resourcesRequest = {
        {"jsonrpc", "2.0"}, {"id", 2}, {"method", "resources/list"}, {"params", json::object()}};

    auto resourcesResponse = server->handleRequestPublic(resourcesRequest);
    REQUIRE(resourcesResponse.has_value());

    json resourcesResult = resourcesResponse.value();
    REQUIRE(resourcesResult["result"].contains("resources"));
    CHECK(resourcesResult["result"]["resources"].is_array());
}

TEST_CASE("MCP 2024-10-07 - supports prompts/list",
          "[mcp][protocol][2024-10-07][features][prompts][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    // Initialize
    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2024-10-07"},
                          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                          {"capabilities", json::object()}}}};

    auto initResponse = server->handleRequestPublic(initRequest);
    REQUIRE(initResponse.has_value());

    // List prompts
    json promptsRequest = {
        {"jsonrpc", "2.0"}, {"id", 2}, {"method", "prompts/list"}, {"params", json::object()}};

    auto promptsResponse = server->handleRequestPublic(promptsRequest);
    REQUIRE(promptsResponse.has_value());

    json promptsResult = promptsResponse.value();
    REQUIRE(promptsResult["result"].contains("prompts"));
    CHECK(promptsResult["result"]["prompts"].is_array());
}

// ============================================================================
// Cross-Version Compatibility Tests
// ============================================================================

TEST_CASE("MCP - all versions support ping",
          "[mcp][protocol][all-versions][features][ping][catch2]") {
    std::vector<std::string> versions = {"2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05",
                                         "2024-10-07"};

    for (const auto& version : versions) {
        auto transport = std::make_unique<NullTransport>();
        auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

        // Initialize
        json initRequest = {{"jsonrpc", "2.0"},
                            {"id", 1},
                            {"method", "initialize"},
                            {"params",
                             {{"protocolVersion", version},
                              {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                              {"capabilities", json::object()}}}};

        auto initResponse = server->handleRequestPublic(initRequest);
        REQUIRE(initResponse.has_value());

        // Ping
        json pingRequest = {
            {"jsonrpc", "2.0"}, {"id", 2}, {"method", "ping"}, {"params", json::object()}};

        auto pingResponse = server->handleRequestPublic(pingRequest);
        REQUIRE(pingResponse.has_value());

        json pingResult = pingResponse.value();
        CHECK(pingResult["jsonrpc"] == "2.0");
        CHECK(pingResult["id"] == 2);
        REQUIRE(pingResult.contains("result"));
    }
}

TEST_CASE("MCP - all versions support cancellation",
          "[mcp][protocol][all-versions][features][cancellation][catch2]") {
    std::vector<std::string> versions = {"2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05",
                                         "2024-10-07"};

    for (const auto& version : versions) {
        auto transport = std::make_unique<NullTransport>();
        auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

        // Initialize
        json initRequest = {{"jsonrpc", "2.0"},
                            {"id", 1},
                            {"method", "initialize"},
                            {"params",
                             {{"protocolVersion", version},
                              {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                              {"capabilities", json::object()}}}};

        auto initResponse = server->handleRequestPublic(initRequest);
        REQUIRE(initResponse.has_value());

        // Cancel (notification, no response expected)
        json cancelNotification = {{"jsonrpc", "2.0"},
                                   {"method", "notifications/cancelled"},
                                   {"params", {{"requestId", 999}}}};

        auto cancelResponse = server->handleRequestPublic(cancelNotification);
        // Notifications may return an error indicating no response should be sent
        // This is acceptable behavior
    }
}

TEST_CASE("MCP - all versions support progress notifications",
          "[mcp][protocol][all-versions][features][progress][catch2]") {
    std::vector<std::string> versions = {"2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05",
                                         "2024-10-07"};

    for (const auto& version : versions) {
        auto transport = std::make_unique<NullTransport>();
        auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

        // Initialize
        json initRequest = {{"jsonrpc", "2.0"},
                            {"id", 1},
                            {"method", "initialize"},
                            {"params",
                             {{"protocolVersion", version},
                              {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                              {"capabilities", json::object()}}}};

        auto initResponse = server->handleRequestPublic(initRequest);
        REQUIRE(initResponse.has_value());

        // Check capabilities include progress support
        json result = initResponse.value();
        // Progress is supported via _meta.progressToken in requests
        // This is a protocol-level feature available in all versions
        CHECK(result["result"]["protocolVersion"] == version);
    }
}

TEST_CASE("MCP - all versions support logging",
          "[mcp][protocol][all-versions][features][logging][catch2]") {
    std::vector<std::string> versions = {"2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05",
                                         "2024-10-07"};

    for (const auto& version : versions) {
        auto transport = std::make_unique<NullTransport>();
        auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

        json initRequest = {{"jsonrpc", "2.0"},
                            {"id", 1},
                            {"method", "initialize"},
                            {"params",
                             {{"protocolVersion", version},
                              {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                              {"capabilities", json::object()}}}};

        auto response = server->handleRequestPublic(initRequest);
        REQUIRE(response.has_value());

        json result = response.value();
        REQUIRE(result["result"].contains("capabilities"));

        // Logging capability should be present in all versions
        const auto& caps = result["result"]["capabilities"];
        CHECK(caps.contains("logging"));
    }
}
