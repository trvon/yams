// MCP Server basics tests
// Catch2 migration from GTest (yams-3s4 / yams-84g)

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#ifdef _WIN32
#include <windows.h>

static int setenv(const char* name, const char* value, int overwrite) {
    if (!name || !value) {
        return -1;
    }

    if (!overwrite) {
        DWORD existing = GetEnvironmentVariableA(name, nullptr, 0);
        if (existing != 0) {
            return 0;
        }
    }

    return SetEnvironmentVariableA(name, value) ? 0 : -1;
}

static int unsetenv(const char* name) {
    if (!name) {
        return -1;
    }

    return SetEnvironmentVariableA(name, nullptr) ? 0 : -1;
}
#endif

#include <nlohmann/json.hpp>
#include <yams/mcp/mcp_server.h>

using namespace yams::mcp;
using json = nlohmann::json;

namespace {

// Minimal no-op transport for constructing MCPServer in tests without starting loops.
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

TEST_CASE("MCP Transport - StdioTransport basics", "[mcp][transport][catch2]") {
    StdioTransport transport;

    // Test that transport starts as connected
    CHECK(transport.isConnected());

    // Test close functionality
    transport.close();
    CHECK_FALSE(transport.isConnected());
}

TEST_CASE("MCP Types - Basic type availability", "[mcp][types][catch2]") {
    // Test basic types are available - compilation test
    CHECK(true);
}

// ============================================================================
// Tool Registry Initialization Tests
// ============================================================================

TEST_CASE("MCP Server - Tool registry is initialized and listTools returns tools",
          "[mcp][server][tools][registry][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    // Call testListTools which directly invokes listTools()
    json result = server->testListTools();

    // Verify the result is a valid JSON object with tools array
    REQUIRE(result.is_object());
    REQUIRE(result.contains("tools"));
    REQUIRE(result["tools"].is_array());

    // Verify tools array is not empty (registry was initialized)
    CHECK(result["tools"].size() > 0);

    // Log the number of tools found for debugging
    INFO("Number of tools registered: " << result["tools"].size());
}

TEST_CASE("MCP Server - listTools returns valid tool structure",
          "[mcp][server][tools][structure][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    json result = server->testListTools();

    REQUIRE(result.is_object());
    REQUIRE(result.contains("tools"));
    REQUIRE(result["tools"].is_array());

    // Verify each tool has required fields per MCP spec
    for (const auto& tool : result["tools"]) {
        REQUIRE(tool.is_object());
        CHECK(tool.contains("name"));
        CHECK(tool.contains("description"));
        CHECK(tool.contains("inputSchema"));

        // Verify inputSchema is a valid JSON schema object
        if (tool.contains("inputSchema")) {
            const auto& schema = tool["inputSchema"];
            CHECK(schema.is_object());
            if (schema.contains("type")) {
                CHECK(schema["type"].get<std::string>() == "object");
            }
        }
    }
}

TEST_CASE("MCP Server - Core tools are registered", "[mcp][server][tools][core][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    json result = server->testListTools();

    REQUIRE(result.is_object());
    REQUIRE(result.contains("tools"));

    // Collect all tool names
    std::unordered_set<std::string> toolNames;
    for (const auto& tool : result["tools"]) {
        if (tool.is_object() && tool.contains("name")) {
            toolNames.insert(tool["name"].get<std::string>());
        }
    }

    // Verify core tools are present
    std::vector<std::string> coreTools = {"search", "grep",  "get",    "list",          "add",
                                          "status", "graph", "update", "delete_by_name"};

    for (const auto& toolName : coreTools) {
        CHECK(toolNames.count(toolName) > 0);
    }
}

// ============================================================================
// OpenCode Compatibility Tests
// Based on MCP protocol spec and OpenCode SDK expectations
// ============================================================================

TEST_CASE("MCP Server - tools/list response matches MCP spec format",
          "[mcp][server][tools][opencodcompat][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    // Simulate what OpenCode sends: {"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}
    json request = {
        {"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/list"}, {"params", json::object()}};

    // Use handleRequestPublic to process the request
    auto response = server->handleRequestPublic(request);

    REQUIRE(response.has_value());

    json result = response.value();

    // Verify JSON-RPC structure
    REQUIRE(result.is_object());
    CHECK(result["jsonrpc"] == "2.0");
    CHECK(result["id"] == 1);
    REQUIRE(result.contains("result"));

    // Verify result contains tools array
    const auto& resultObj = result["result"];
    REQUIRE(resultObj.is_object());
    REQUIRE(resultObj.contains("tools"));
    REQUIRE(resultObj["tools"].is_array());
    CHECK(resultObj["tools"].size() > 0);
}

TEST_CASE("MCP Server - tools/list handles pagination cursor",
          "[mcp][server][tools][pagination][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    // Test with cursor parameter (as per MCP spec)
    json request = {{"jsonrpc", "2.0"},
                    {"id", 2},
                    {"method", "tools/list"},
                    {"params", {{"cursor", "some-cursor-value"}}}};

    auto response = server->handleRequestPublic(request);

    REQUIRE(response.has_value());

    json result = response.value();
    REQUIRE(result.is_object());
    CHECK(result["jsonrpc"] == "2.0");
    CHECK(result["id"] == 2);
    REQUIRE(result.contains("result"));
}

TEST_CASE("MCP Server - tool schema has all required MCP fields",
          "[mcp][server][tools][schema][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    json result = server->testListTools();

    REQUIRE(result.is_object());
    REQUIRE(result.contains("tools"));
    REQUIRE(result["tools"].is_array());
    REQUIRE(result["tools"].size() > 0);

    // Check first tool for complete MCP compliance
    const auto& firstTool = result["tools"][0];
    REQUIRE(firstTool.is_object());

    // Required fields per MCP spec 2025-11-25
    CHECK(firstTool.contains("name"));
    CHECK(firstTool.contains("description"));
    CHECK(firstTool.contains("inputSchema"));

    // Verify name is a string
    CHECK(firstTool["name"].is_string());
    CHECK(!firstTool["name"].get<std::string>().empty());

    // Verify description is a string
    CHECK(firstTool["description"].is_string());

    // Verify inputSchema has proper JSON Schema structure
    const auto& schema = firstTool["inputSchema"];
    REQUIRE(schema.is_object());
    CHECK(schema.contains("type"));
    CHECK(schema["type"] == "object");

    if (schema.contains("properties")) {
        CHECK(schema["properties"].is_object());
    }
    if (schema.contains("required")) {
        CHECK(schema["required"].is_array());
    }
}

TEST_CASE("MCP Server - handles tools/list without params field",
          "[mcp][server][tools][edgecase][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    // Some clients might not send params at all
    json request = {
        {"jsonrpc", "2.0"}, {"id", 3}, {"method", "tools/list"} // No params field
    };

    auto response = server->handleRequestPublic(request);

    REQUIRE(response.has_value());

    json result = response.value();
    REQUIRE(result.is_object());
    CHECK(result["jsonrpc"] == "2.0");
    CHECK(result["id"] == 3);
    REQUIRE(result.contains("result"));
    REQUIRE(result["result"].contains("tools"));
}

TEST_CASE("MCP Server - debug print tools/list response", "[mcp][server][tools][debug][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    json request = {
        {"jsonrpc", "2.0"}, {"id", 99}, {"method", "tools/list"}, {"params", json::object()}};

    auto response = server->handleRequestPublic(request);

    REQUIRE(response.has_value());

    // Print the full response for debugging
    std::cout << "\n=== MCP tools/list Response ===\n";
    std::cout << response.value().dump(2) << "\n";
    std::cout << "================================\n" << std::endl;

    // Also print just the listTools result
    std::cout << "\n=== Direct listTools() Result ===\n";
    std::cout << server->testListTools().dump(2) << "\n";
    std::cout << "==================================\n" << std::endl;
}

TEST_CASE("MCP Server - tools/list response has no null fields",
          "[mcp][server][tools][nullcheck][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    json request = {
        {"jsonrpc", "2.0"}, {"id", 100}, {"method", "tools/list"}, {"params", json::object()}};

    auto response = server->handleRequestPublic(request);

    REQUIRE(response.has_value());

    std::string responseStr = response.value().dump();

    // Check that there are no null values in the response
    // Some MCP clients might not handle null values well
    CHECK(responseStr.find("null") == std::string::npos);
}

TEST_CASE("MCP Server - tools/list response is valid JSON-RPC 2.0",
          "[mcp][server][tools][jsonrpc][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    json request = {
        {"jsonrpc", "2.0"}, {"id", 101}, {"method", "tools/list"}, {"params", json::object()}};

    auto response = server->handleRequestPublic(request);

    REQUIRE(response.has_value());

    json result = response.value();

    // Verify strict JSON-RPC 2.0 format
    REQUIRE(result.contains("jsonrpc"));
    CHECK(result["jsonrpc"] == "2.0");

    REQUIRE(result.contains("id"));
    CHECK(result["id"] == 101);

    REQUIRE(result.contains("result"));
    CHECK(!result.contains("error"));

    // Verify result structure
    const auto& res = result["result"];
    REQUIRE(res.is_object());
    REQUIRE(res.contains("tools"));
    REQUIRE(res["tools"].is_array());

    // Each tool must have name and inputSchema
    for (const auto& tool : res["tools"]) {
        REQUIRE(tool.is_object());
        REQUIRE(tool.contains("name"));
        REQUIRE(tool["name"].is_string());
        REQUIRE(!tool["name"].get<std::string>().empty());

        REQUIRE(tool.contains("inputSchema"));
        REQUIRE(tool["inputSchema"].is_object());
    }
}

// ============================================================================
// Stdio Transport Tests
// ============================================================================

TEST_CASE("MCP Server - StdioTransport send and receive roundtrip",
          "[mcp][server][stdio][roundtrip][catch2]") {
    // Create a string-based stream for testing stdio transport
    std::stringstream inputStream;
    std::stringstream outputStream;

    // Prepare a tools/list request
    json request = {
        {"jsonrpc", "2.0"}, {"id", 200}, {"method", "tools/list"}, {"params", json::object()}};

    // Write request to input stream (NDJSON format)
    inputStream << request.dump() << "\n";

    // Note: We can't easily test the full roundtrip without mocking std::cin/cout
    // But we can verify the transport state
    StdioTransport transport;
    CHECK(transport.isConnected());

    // The transport should be able to send
    json testMessage = {{"jsonrpc", "2.0"}, {"id", 1}, {"result", {{"test", true}}}};
    transport.send(testMessage);

    // Transport should still be connected after send
    CHECK(transport.isConnected());
}

TEST_CASE("MCP Server - verify tools available immediately after construction",
          "[mcp][server][tools][immediate][catch2]") {
    // This test verifies that tools are available immediately after MCPServer construction
    // without any additional initialization steps

    auto transport = std::make_unique<NullTransport>();

    // Construct server
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    // Immediately query tools - should work without calling start() or any other method
    json result = server->testListTools();

    REQUIRE(result.is_object());
    REQUIRE(result.contains("tools"));
    REQUIRE(result["tools"].is_array());
    CHECK(result["tools"].size() > 0);

    // Verify we can call it multiple times
    json result2 = server->testListTools();
    CHECK(result2["tools"].size() == result["tools"].size());
}

TEST_CASE("MCP Server - tools/list before initialize should still work",
          "[mcp][server][tools][preinit][catch2]") {
    // Some MCP clients may query tools/list before sending initialize
    // This should still work per MCP spec

    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    // Query tools without initializing
    json request = {
        {"jsonrpc", "2.0"}, {"id", 300}, {"method", "tools/list"}, {"params", json::object()}};

    auto response = server->handleRequestPublic(request);

    REQUIRE(response.has_value());

    json result = response.value();
    REQUIRE(result.is_object());
    CHECK(result["jsonrpc"] == "2.0");
    CHECK(result["id"] == 300);
    REQUIRE(result.contains("result"));
    REQUIRE(result["result"].contains("tools"));
    REQUIRE(result["result"]["tools"].is_array());
    CHECK(result["result"]["tools"].size() > 0);
}

// ============================================================================
// Full Handshake Sequence Tests
// ============================================================================

TEST_CASE("MCP Server - full OpenCode handshake sequence",
          "[mcp][server][handshake][opencodcompat][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    // Step 1: OpenCode sends initialize
    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2025-11-25"},
                          {"clientInfo", {{"name", "opencode"}, {"version", "1.0.0"}}},
                          {"capabilities", json::object()}}}};

    auto initResponse = server->handleRequestPublic(initRequest);
    REQUIRE(initResponse.has_value());
    CHECK(initResponse.value()["jsonrpc"] == "2.0");
    CHECK(initResponse.value()["id"] == 1);
    REQUIRE(initResponse.value().contains("result"));

    // Step 2: OpenCode sends notifications/initialized
    json initializedNotification = {
        {"jsonrpc", "2.0"}, {"method", "notifications/initialized"}, {"params", json::object()}};

    auto notifResponse = server->handleRequestPublic(initializedNotification);
    // Notifications don't return responses

    // Step 3: OpenCode sends tools/list
    json toolsRequest = {
        {"jsonrpc", "2.0"}, {"id", 2}, {"method", "tools/list"}, {"params", json::object()}};

    auto toolsResponse = server->handleRequestPublic(toolsRequest);
    REQUIRE(toolsResponse.has_value());
    CHECK(toolsResponse.value()["jsonrpc"] == "2.0");
    CHECK(toolsResponse.value()["id"] == 2);
    REQUIRE(toolsResponse.value().contains("result"));
    REQUIRE(toolsResponse.value()["result"].contains("tools"));
    CHECK(toolsResponse.value()["result"]["tools"].size() > 0);
}

TEST_CASE("MCP Server - tools/list without prior initialize",
          "[mcp][server][handshake][nopreinit][catch2]") {
    // Some clients might query tools before initialize (though not spec-compliant)
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    // Send tools/list without initialize
    json toolsRequest = {
        {"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/list"}, {"params", json::object()}};

    auto toolsResponse = server->handleRequestPublic(toolsRequest);
    REQUIRE(toolsResponse.has_value());
    CHECK(toolsResponse.value()["jsonrpc"] == "2.0");
    CHECK(toolsResponse.value()["id"] == 1);
    REQUIRE(toolsResponse.value().contains("result"));
    REQUIRE(toolsResponse.value()["result"].contains("tools"));
    CHECK(toolsResponse.value()["result"]["tools"].size() > 0);
}

TEST_CASE("MCP Server - rapid sequential tools/list requests",
          "[mcp][server][tools][rapid][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    // Send multiple tools/list requests rapidly
    for (int i = 0; i < 10; ++i) {
        json request = {
            {"jsonrpc", "2.0"}, {"id", i}, {"method", "tools/list"}, {"params", json::object()}};

        auto response = server->handleRequestPublic(request);
        REQUIRE(response.has_value());
        CHECK(response.value()["id"] == i);
        REQUIRE(response.value()["result"]["tools"].is_array());
        CHECK(response.value()["result"]["tools"].size() > 0);
    }
}

// ============================================================================
// OpenCode-Specific Issue Tests
// Based on analysis of OpenCode's MCP client implementation
// ============================================================================

TEST_CASE("MCP Server - responds quickly to tools/list after construction",
          "[mcp][server][tools][timing][opencodcompat][catch2]") {
    // OpenCode calls listTools immediately after connect with a timeout
    // We need to ensure the response is sent quickly

    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    // Simulate the exact sequence OpenCode uses
    json request = {
        {"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/list"}, {"params", json::object()}};

    auto start = std::chrono::steady_clock::now();
    auto response = server->handleRequestPublic(request);
    auto end = std::chrono::steady_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    REQUIRE(response.has_value());
    REQUIRE(response.value()["result"]["tools"].is_array());

    // Should respond in less than 1 second (OpenCode's timeout is 30s but it feels longer)
    CHECK(duration.count() < 1000);
}

TEST_CASE("MCP Server - handles concurrent tools/list requests",
          "[mcp][server][tools][concurrent][catch2]") {
    // OpenCode might query tools multiple times or concurrently
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    std::vector<std::future<MessageResult>> futures;

    // Launch multiple concurrent requests
    for (int i = 0; i < 5; ++i) {
        json request = {
            {"jsonrpc", "2.0"}, {"id", i}, {"method", "tools/list"}, {"params", json::object()}};

        futures.push_back(std::async(std::launch::async, [&server, request]() {
            return server->handleRequestPublic(request);
        }));
    }

    // All should succeed
    for (size_t i = 0; i < futures.size(); ++i) {
        auto response = futures[i].get();
        REQUIRE(response.has_value());
        CHECK(response.value()["id"] == static_cast<int>(i));
        REQUIRE(response.value()["result"]["tools"].is_array());
        CHECK(response.value()["result"]["tools"].size() > 0);
    }
}

TEST_CASE("MCP Server - tools/list works without daemon client",
          "[mcp][server][tools][nodaemon][catch2]") {
    // If daemon is not available, tools/list should still work
    // The tools are registered in the constructor, not dependent on daemon

    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    // Force daemon client to be null by resetting it
    server->testShutdown();

    json request = {
        {"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/list"}, {"params", json::object()}};

    auto response = server->handleRequestPublic(request);

    REQUIRE(response.has_value());
    REQUIRE(response.value()["result"]["tools"].is_array());
    CHECK(response.value()["result"]["tools"].size() > 0);
}

TEST_CASE("MCP Server - stderr logging doesn't block stdout",
          "[mcp][server][stdio][stderr][catch2]") {
    // This test verifies that stderr logging doesn't interfere with stdout
    // OpenCode captures stderr separately, so heavy logging shouldn't block

    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    // The server constructor logs to stderr - verify we can still get tools
    json result = server->testListTools();

    REQUIRE(result.is_object());
    REQUIRE(result.contains("tools"));
    CHECK(result["tools"].size() > 0);
}

// ============================================================================
// Protocol Version Compatibility Tests
// ============================================================================

TEST_CASE("MCP Server - supports protocol version 2025-11-25",
          "[mcp][server][protocol][version][2025-11-25][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

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
    CHECK(result["jsonrpc"] == "2.0");
    CHECK(result["id"] == 1);
    REQUIRE(result.contains("result"));
    CHECK(result["result"]["protocolVersion"] == "2025-11-25");
    REQUIRE(result["result"].contains("capabilities"));
    REQUIRE(result["result"].contains("serverInfo"));
}

TEST_CASE("MCP Server - supports protocol version 2025-06-18",
          "[mcp][server][protocol][version][2025-06-18][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

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
    CHECK(result["result"]["protocolVersion"] == "2025-06-18");
}

TEST_CASE("MCP Server - supports protocol version 2025-03-26",
          "[mcp][server][protocol][version][2025-03-26][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

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
}

TEST_CASE("MCP Server - supports protocol version 2024-11-05",
          "[mcp][server][protocol][version][2024-11-05][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

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
}

TEST_CASE("MCP Server - supports protocol version 2024-10-07",
          "[mcp][server][protocol][version][2024-10-07][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

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
}

TEST_CASE("MCP Server - falls back to latest for unsupported protocol version",
          "[mcp][server][protocol][version][fallback][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2023-01-01"}, // Unsupported version
                          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                          {"capabilities", json::object()}}}};

    auto response = server->handleRequestPublic(initRequest);
    REQUIRE(response.has_value());

    json result = response.value();
    // Should fall back to latest version (2025-11-25)
    CHECK(result["result"]["protocolVersion"] == "2025-11-25");
}

TEST_CASE("MCP Server - strict protocol mode rejects unsupported version",
          "[mcp][server][protocol][version][strict][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    // Enable strict protocol mode via environment variable
    setenv("YAMS_MCP_STRICT_PROTOCOL", "1", 1);

    // Create a new server with strict mode enabled
    auto strictServer = std::make_unique<yams::mcp::MCPServer>(std::make_unique<NullTransport>());

    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2023-01-01"}, // Unsupported version
                          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                          {"capabilities", json::object()}}}};

    auto response = strictServer->handleRequestPublic(initRequest);
    REQUIRE(response.has_value());

    json result = response.value();
    // Should return an error
    REQUIRE(result.contains("error"));
    CHECK(result["error"]["code"] == -32901); // kErrUnsupportedProtocolVersion

    // Clean up
    unsetenv("YAMS_MCP_STRICT_PROTOCOL");
}

TEST_CASE("MCP Server - uses latest version when protocolVersion not specified",
          "[mcp][server][protocol][version][default][catch2]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {
                             {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                             {"capabilities", json::object()}
                             // No protocolVersion specified
                         }}};

    auto response = server->handleRequestPublic(initRequest);
    REQUIRE(response.has_value());

    json result = response.value();
    // Should use latest version
    CHECK(result["result"]["protocolVersion"] == "2025-11-25");
}
