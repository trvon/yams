// Test for MCP initialize response format issues
// This tests the specific error: ExpectedInitResult(Some(EmptyResult(EmptyObject)))
// Catch2 migration from GTest (yams-3s4 / yams-84g)

#include <catch2/catch_test_macros.hpp>

#include <nlohmann/json.hpp>
#include <iostream>
#include <string>

using json = nlohmann::json;

namespace {

// Simulate the buildServerCapabilities() logic from mcp_server.cpp
json buildServerCapabilities() {
    json caps = {{"tools", json({{"listChanged", false}})},
                 {"prompts", json({{"listChanged", false}})},
                 {"resources", json({{"subscribe", false}, {"listChanged", false}})},
                 {"logging", json::object()}};
    caps["experimental"] = json::object();
    caps["experimental"]["cancellation"] = json::object();
    caps["experimental"]["progress"] = json::object();
    return caps;
}

// Simulate the initialize() logic from mcp_server.cpp
json initialize(const json& params, const std::string& serverName,
                const std::string& serverVersion) {
    static const std::vector<std::string> kSupported = {"2024-11-05", "2025-06-18", "2025-03-26"};
    const std::string latest = "2025-06-18";

    std::string requested = latest;
    if (params.contains("protocolVersion") && params["protocolVersion"].is_string()) {
        requested = params["protocolVersion"].get<std::string>();
    }

    std::string negotiated = latest;
    bool matched = std::find(kSupported.begin(), kSupported.end(), requested) != kSupported.end();
    if (matched) {
        negotiated = requested;
    }

    json caps = buildServerCapabilities();

    json result = {{"protocolVersion", negotiated},
                   {"serverInfo", {{"name", serverName}, {"version", serverVersion}}},
                   {"capabilities", caps}};

    return result;
}

// Simulate createResponse() from mcp_server.cpp
json createResponse(const json& id, const json& result) {
    return json{{"jsonrpc", "2.0"}, {"id", id}, {"result", result}};
}

} // namespace

TEST_CASE("MCPInitialize - Result has all required fields", "[mcp][initialize][catch2]") {
    // Test the initialize() function directly
    json params = {{"clientInfo", {{"name", "test-client"}, {"version", "1.0"}}},
                   {"protocolVersion", "2024-11-05"}};

    json result = initialize(params, "yams-mcp", "1.0.0");

    // Check result is not null or empty
    REQUIRE_FALSE(result.is_null());
    REQUIRE(result.is_object());
    REQUIRE_FALSE(result.empty());

    // MCP spec requires these fields
    REQUIRE(result.contains("protocolVersion"));
    REQUIRE(result.contains("serverInfo"));
    REQUIRE(result.contains("capabilities"));

    // Verify serverInfo structure
    const json& serverInfo = result["serverInfo"];
    REQUIRE(serverInfo.is_object());
    REQUIRE(serverInfo.contains("name"));
    REQUIRE(serverInfo.contains("version"));
    CHECK(serverInfo["name"].get<std::string>() == "yams-mcp");
    CHECK(serverInfo["version"].get<std::string>() == "1.0.0");

    // Verify capabilities structure
    const json& caps = result["capabilities"];
    REQUIRE(caps.is_object());
    REQUIRE_FALSE(caps.empty());
    CHECK(caps.contains("tools"));
    CHECK(caps.contains("prompts"));
    CHECK(caps.contains("resources"));
}

TEST_CASE("MCPInitialize - Response not null or empty", "[mcp][initialize][catch2]") {
    // Specific test for the EmptyResult/EmptyObject error
    json params = {{"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                   {"protocolVersion", "2024-11-05"}};

    json result = initialize(params, "yams-mcp", "1.0.0");

    // These are the specific checks that fail with "EmptyResult(EmptyObject)"
    REQUIRE_FALSE(result.is_null());
    REQUIRE_FALSE(result.empty());
    REQUIRE(result.is_object());
    REQUIRE(result.size() > 0);
}

TEST_CASE("MCPInitialize - With empty params", "[mcp][initialize][catch2]") {
    // Test initialize with empty params (client might not send protocolVersion)
    json params = json::object();

    json result = initialize(params, "yams-mcp", "1.0.0");

    // Should still return a valid response
    REQUIRE_FALSE(result.is_null());
    REQUIRE_FALSE(result.empty());
    CHECK(result.contains("protocolVersion"));
    CHECK(result.contains("serverInfo"));
    CHECK(result.contains("capabilities"));
}

TEST_CASE("MCPInitialize - Response serializes to JSON", "[mcp][initialize][catch2]") {
    // Test that the full response can be serialized without issues
    json params = {{"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                   {"protocolVersion", "2024-11-05"}};

    json initResult = initialize(params, "yams-mcp", "1.0.0");
    json response = createResponse(1, initResult);

    // Try to serialize to string
    std::string serialized;
    REQUIRE_NOTHROW(serialized = response.dump());

    REQUIRE_FALSE(serialized.empty());
    CHECK(serialized != "{}");
    CHECK(serialized != "null");

    // Should contain the key fields
    CHECK(serialized.find("\"result\"") != std::string::npos);
    CHECK(serialized.find("\"protocolVersion\"") != std::string::npos);
    CHECK(serialized.find("\"serverInfo\"") != std::string::npos);
    CHECK(serialized.find("\"yams-mcp\"") != std::string::npos);

    // Try to parse it back
    json parsed;
    REQUIRE_NOTHROW(parsed = json::parse(serialized));

    CHECK(parsed == response);
}

TEST_CASE("MCPInitialize - ServerInfo fields non-empty", "[mcp][initialize][catch2]") {
    // Specifically test that serverInfo fields are never empty
    json params = {{"protocolVersion", "2024-11-05"}};

    // Test with valid server info
    json result1 = initialize(params, "yams-mcp", "1.0.0");
    CHECK_FALSE(result1["serverInfo"]["name"].get<std::string>().empty());
    CHECK_FALSE(result1["serverInfo"]["version"].get<std::string>().empty());

    // Test with empty strings (shouldn't happen, but defensive check)
    json result2 = initialize(params, "", "");
    // Even with empty strings, structure should be valid
    CHECK(result2.contains("serverInfo"));
    CHECK(result2["serverInfo"].contains("name"));
    CHECK(result2["serverInfo"].contains("version"));
}

TEST_CASE("MCPInitialize - Capabilities always populated", "[mcp][initialize][catch2]") {
    // Test that capabilities are always populated
    json caps = buildServerCapabilities();

    REQUIRE_FALSE(caps.is_null());
    REQUIRE(caps.is_object());
    REQUIRE_FALSE(caps.empty());

    // Check required capability fields
    CHECK(caps.contains("tools"));
    CHECK(caps.contains("prompts"));
    CHECK(caps.contains("resources"));
    CHECK(caps.contains("experimental"));

    // Verify experimental capabilities
    CHECK(caps["experimental"].contains("cancellation"));
    CHECK(caps["experimental"].contains("progress"));
}
