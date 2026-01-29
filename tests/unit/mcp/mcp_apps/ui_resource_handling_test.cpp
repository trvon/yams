// ============================================================================
// MCP Apps Extension - UI Resource Handling Tests
// PBI-083: MCP Apps Support - Phase 1
//
// Test IDs: mcp-apps-res-01 through mcp-apps-res-09
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

TEST_CASE("MCP Apps Resource - Declaration With UI Support",
          "[mcp][apps][resource][mcp-apps-res-01][phase1]") {
    SKIP("Pending MCP Apps implementation");

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

    auto initResponse = server->handleRequestPublic(initRequest);
    REQUIRE(initResponse.has_value());

    json listRequest = {
        {"jsonrpc", "2.0"}, {"id", 2}, {"method", "resources/list"}, {"params", json::object()}};

    auto listResponse = server->handleRequestPublic(listRequest);
    REQUIRE(listResponse.has_value());

    json result = listResponse.value();
    REQUIRE(result.contains("result"));
    REQUIRE(result["result"].contains("resources"));

    bool foundUIResource = false;
    for (const auto& resource : result["result"]["resources"]) {
        if (resource.contains("uri") && resource["uri"].get<std::string>().find("ui://") == 0) {
            foundUIResource = true;
            CHECK(resource.contains("name"));
            CHECK(resource.contains("mimeType"));
            CHECK(resource["mimeType"] == "text/html;profile=mcp-app");
        }
    }

    CHECK(foundUIResource);
}

TEST_CASE("MCP Apps Resource - Content Retrieval Text",
          "[mcp][apps][resource][mcp-apps-res-03][phase1]") {
    SKIP("Pending MCP Apps implementation");

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

    auto initResponse = server->handleRequestPublic(initRequest);
    REQUIRE(initResponse.has_value());

    json readRequest = {{"jsonrpc", "2.0"},
                        {"id", 2},
                        {"method", "resources/read"},
                        {"params", {{"uri", "ui://yams/dashboard"}}}};

    auto readResponse = server->handleRequestPublic(readRequest);
    REQUIRE(readResponse.has_value());

    json result = readResponse.value();
    REQUIRE(result.contains("result"));
    REQUIRE(result["result"].contains("contents"));
    REQUIRE(result["result"]["contents"].is_array());
    REQUIRE(result["result"]["contents"].size() > 0);

    auto content = result["result"]["contents"][0];
    CHECK(content.contains("uri"));
    CHECK(content["uri"] == "ui://yams/dashboard");
    CHECK(content.contains("mimeType"));
    CHECK(content["mimeType"] == "text/html;profile=mcp-app");
    CHECK(content.contains("text"));
    CHECK(content["text"].is_string());
}
