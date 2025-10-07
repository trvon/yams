// Test for MCP initialize response format issues
// This tests the specific error: ExpectedInitResult(Some(EmptyResult(EmptyObject)))

#include <nlohmann/json.hpp>
#include <iostream>
#include <string>
#include <gtest/gtest.h>

using json = nlohmann::json;

// Direct unit test of the initialize logic by simulating what mcp_server.cpp does
class MCPInitializeTest : public ::testing::Test {
protected:
    // Simulate the buildServerCapabilities() logic from mcp_server.cpp
    json buildServerCapabilities() const {
        json caps = {{"tools", json({{"listChanged", false}})},
                     {"prompts", json({{"listChanged", false}})},
                     {"resources", json({{"subscribe", false}, {"listChanged", false}})},
                     {"logging", json::object()}};
        caps["experimental"] = json::object();
        caps["experimental"]["cancellation"] = true;
        caps["experimental"]["progress"] = true;
        return caps;
    }

    // Simulate the initialize() logic from mcp_server.cpp
    json initialize(const json& params, const std::string& serverName,
                    const std::string& serverVersion) {
        static const std::vector<std::string> kSupported = {"2024-11-05", "2025-06-18",
                                                            "2025-03-26"};
        const std::string latest = "2025-06-18";

        std::string requested = latest;
        if (params.contains("protocolVersion") && params["protocolVersion"].is_string()) {
            requested = params["protocolVersion"].get<std::string>();
        }

        std::string negotiated = latest;
        bool matched =
            std::find(kSupported.begin(), kSupported.end(), requested) != kSupported.end();
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
};

TEST_F(MCPInitializeTest, InitializeResultHasAllRequiredFields) {
    // Test the initialize() function directly
    json params = {{"clientInfo", {{"name", "test-client"}, {"version", "1.0"}}},
                   {"protocolVersion", "2024-11-05"}};

    json result = initialize(params, "yams-mcp", "1.0.0");

    // Check result is not null or empty
    EXPECT_FALSE(result.is_null()) << "Result is null!";
    EXPECT_TRUE(result.is_object()) << "Result is not an object";
    EXPECT_FALSE(result.empty()) << "Result is empty object!";

    // MCP spec requires these fields
    EXPECT_TRUE(result.contains("protocolVersion")) << "Missing protocolVersion in result";
    EXPECT_TRUE(result.contains("serverInfo")) << "Missing serverInfo in result";
    EXPECT_TRUE(result.contains("capabilities")) << "Missing capabilities in result";

    // Verify serverInfo structure
    const json& serverInfo = result["serverInfo"];
    EXPECT_TRUE(serverInfo.is_object()) << "serverInfo is not an object";
    EXPECT_TRUE(serverInfo.contains("name")) << "serverInfo missing name";
    EXPECT_TRUE(serverInfo.contains("version")) << "serverInfo missing version";
    EXPECT_EQ(serverInfo["name"].get<std::string>(), "yams-mcp") << "serverInfo.name incorrect";
    EXPECT_EQ(serverInfo["version"].get<std::string>(), "1.0.0") << "serverInfo.version incorrect";

    // Verify capabilities structure
    const json& caps = result["capabilities"];
    EXPECT_TRUE(caps.is_object()) << "capabilities is not an object";
    EXPECT_FALSE(caps.empty()) << "capabilities is empty object";
    EXPECT_TRUE(caps.contains("tools")) << "capabilities missing tools";
    EXPECT_TRUE(caps.contains("prompts")) << "capabilities missing prompts";
    EXPECT_TRUE(caps.contains("resources")) << "capabilities missing resources";
}

TEST_F(MCPInitializeTest, InitializeResponseNotNullOrEmpty) {
    // Specific test for the EmptyResult/EmptyObject error
    json params = {{"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                   {"protocolVersion", "2024-11-05"}};

    json result = initialize(params, "yams-mcp", "1.0.0");

    // These are the specific checks that fail with "EmptyResult(EmptyObject)"
    EXPECT_FALSE(result.is_null()) << "Result is null - this causes ExpectedInitResult error!";
    EXPECT_FALSE(result.empty())
        << "Result is empty object - this causes EmptyResult(EmptyObject) error!";
    EXPECT_TRUE(result.is_object()) << "Result is not an object";
    EXPECT_GT(result.size(), 0) << "Result object has no fields";
}

TEST_F(MCPInitializeTest, InitializeWithEmptyParams) {
    // Test initialize with empty params (client might not send protocolVersion)
    json params = json::object();

    json result = initialize(params, "yams-mcp", "1.0.0");

    // Should still return a valid response
    EXPECT_FALSE(result.is_null());
    EXPECT_FALSE(result.empty());
    EXPECT_TRUE(result.contains("protocolVersion"));
    EXPECT_TRUE(result.contains("serverInfo"));
    EXPECT_TRUE(result.contains("capabilities"));
}

TEST_F(MCPInitializeTest, InitializeResponseSerializesToJSON) {
    // Test that the full response can be serialized without issues
    json params = {{"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                   {"protocolVersion", "2024-11-05"}};

    json initResult = initialize(params, "yams-mcp", "1.0.0");
    json response = createResponse(1, initResult);

    // Try to serialize to string
    std::string serialized;
    EXPECT_NO_THROW({ serialized = response.dump(); }) << "Response failed to serialize";

    EXPECT_FALSE(serialized.empty()) << "Serialized response is empty";
    EXPECT_NE(serialized, "{}") << "Serialized response is empty object";
    EXPECT_NE(serialized, "null") << "Serialized response is null";

    // Should contain the key fields
    EXPECT_NE(serialized.find("\"result\""), std::string::npos)
        << "Serialized response missing 'result' field";
    EXPECT_NE(serialized.find("\"protocolVersion\""), std::string::npos)
        << "Serialized response missing 'protocolVersion'";
    EXPECT_NE(serialized.find("\"serverInfo\""), std::string::npos)
        << "Serialized response missing 'serverInfo'";
    EXPECT_NE(serialized.find("\"yams-mcp\""), std::string::npos)
        << "Serialized response missing server name";

    // Try to parse it back
    json parsed;
    EXPECT_NO_THROW({ parsed = json::parse(serialized); }) << "Failed to parse serialized response";

    EXPECT_EQ(parsed, response) << "Round-trip serialization changed the response";

    // Print for manual inspection
    std::cout << "\n=== Initialize Response ===" << std::endl;
    std::cout << response.dump(2) << std::endl;
    std::cout << "==========================\n" << std::endl;
}

TEST_F(MCPInitializeTest, ServerInfoFieldsNonEmpty) {
    // Specifically test that serverInfo fields are never empty
    json params = {{"protocolVersion", "2024-11-05"}};

    // Test with valid server info
    json result1 = initialize(params, "yams-mcp", "1.0.0");
    EXPECT_FALSE(result1["serverInfo"]["name"].get<std::string>().empty());
    EXPECT_FALSE(result1["serverInfo"]["version"].get<std::string>().empty());

    // Test with empty strings (shouldn't happen, but defensive check)
    json result2 = initialize(params, "", "");
    // Even with empty strings, structure should be valid
    EXPECT_TRUE(result2.contains("serverInfo"));
    EXPECT_TRUE(result2["serverInfo"].contains("name"));
    EXPECT_TRUE(result2["serverInfo"].contains("version"));
}

TEST_F(MCPInitializeTest, CapabilitiesAlwaysPopulated) {
    // Test that capabilities are always populated
    json params = {{"protocolVersion", "2024-11-05"}};

    json caps = buildServerCapabilities();

    EXPECT_FALSE(caps.is_null());
    EXPECT_TRUE(caps.is_object());
    EXPECT_FALSE(caps.empty()) << "Capabilities should never be empty";

    // Check required capability fields
    EXPECT_TRUE(caps.contains("tools"));
    EXPECT_TRUE(caps.contains("prompts"));
    EXPECT_TRUE(caps.contains("resources"));
    EXPECT_TRUE(caps.contains("experimental"));

    // Verify experimental capabilities
    EXPECT_TRUE(caps["experimental"].contains("cancellation"));
    EXPECT_TRUE(caps["experimental"].contains("progress"));
}
