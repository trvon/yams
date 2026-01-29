// ============================================================================
// MCP Apps Extension - Capability Negotiation Tests
// PBI-083: MCP Apps Support - Phase 1
//
// Test IDs: mcp-apps-cap-01 through mcp-apps-cap-05
// ============================================================================

#include <nlohmann/json.hpp>
#include <catch2/catch_test_macros.hpp>
#include <yams/mcp/mcp_server.h>

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

TEST_CASE("MCP Apps Capability - Extension Discovery",
          "[mcp][apps][capability][mcp-apps-cap-01][phase1]") {
    // Test implementation - verifying MCP Apps capability negotiation

    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

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

    auto response = server->handleRequestPublic(initRequest);
    REQUIRE(response.has_value());

    json result = response.value();
    REQUIRE(result.contains("result"));
    REQUIRE(result["result"].contains("capabilities"));
    REQUIRE(result["result"]["capabilities"].contains("extensions"));
    REQUIRE(result["result"]["capabilities"]["extensions"].contains("io.modelcontextprotocol/ui"));

    auto uiExt = result["result"]["capabilities"]["extensions"]["io.modelcontextprotocol/ui"];
    REQUIRE(uiExt.contains("mimeTypes"));
    CHECK(uiExt["mimeTypes"].is_array());
}

TEST_CASE("MCP Apps Capability - Extension Not Advertised When Disabled",
          "[mcp][apps][capability][mcp-apps-cap-02][phase1]") {
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
    REQUIRE(result.contains("result"));

    // Should NOT include UI extension
    if (result["result"]["capabilities"].contains("extensions")) {
        CHECK_FALSE(
            result["result"]["capabilities"]["extensions"].contains("io.modelcontextprotocol/ui"));
    }
}

TEST_CASE("MCP Apps Capability - Mime Type Validation Supported",
          "[mcp][apps][capability][mcp-apps-cap-03][phase1]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

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

    auto response = server->handleRequestPublic(initRequest);
    REQUIRE(response.has_value());

    json result = response.value();
    CHECK(result["result"]["protocolVersion"] == "2025-11-25");

    // UI resources should be available after successful negotiation
    // This will be tested more thoroughly in resource tests
}

TEST_CASE("MCP Apps Capability - Mime Type Validation Unsupported",
          "[mcp][apps][capability][mcp-apps-cap-04][phase1]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2025-11-25"},
                          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                          {"capabilities",
                           {{"extensions",
                             {{"io.modelcontextprotocol/ui",
                               {{"mimeTypes", json::array({"application/vnd.custom"})}}}}}}}}}};

    auto response = server->handleRequestPublic(initRequest);
    REQUIRE(response.has_value());

    json result = response.value();

    // Should NOT include UI extension (unsupported mime type)
    if (result["result"]["capabilities"].contains("extensions")) {
        CHECK_FALSE(
            result["result"]["capabilities"]["extensions"].contains("io.modelcontextprotocol/ui"));
    }

    // Server should not crash or return error
    CHECK(result.contains("result"));
}

TEST_CASE("MCP Apps Capability - Graceful Fallback",
          "[mcp][apps][capability][mcp-apps-cap-05][phase1]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    // Initialize without UI support
    json initRequest = {{"jsonrpc", "2.0"},
                        {"id", 1},
                        {"method", "initialize"},
                        {"params",
                         {{"protocolVersion", "2025-11-25"},
                          {"clientInfo", {{"name", "test"}, {"version", "1.0"}}},
                          {"capabilities", json::object()}}}};

    auto initResponse = server->handleRequestPublic(initRequest);
    REQUIRE(initResponse.has_value());

    // Call a tool that would have UI (if supported)
    json toolRequest = {{"jsonrpc", "2.0"},
                        {"id", 2},
                        {"method", "tools/call"},
                        {"params", {{"name", "status"}, {"arguments", json::object()}}}};

    auto toolResponse = server->handleRequestPublic(toolRequest);
    REQUIRE(toolResponse.has_value());

    json toolResult = toolResponse.value();

    // Should return standard text response (no UI)
    if (toolResult.contains("result")) {
        // Result should not contain UI-specific fields
        CHECK_FALSE(toolResult["result"].contains("_meta"));
    }
}
