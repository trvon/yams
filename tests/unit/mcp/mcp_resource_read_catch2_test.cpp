#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>
#include <yams/mcp/mcp_server.h>

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

yams::mcp::MessageResult readResourceResponse(const std::string& uri) {
    auto server = std::make_shared<yams::mcp::MCPServer>(std::make_unique<NullTransport>());
    json request = {
        {"jsonrpc", "2.0"}, {"id", 1}, {"method", "resources/read"}, {"params", {{"uri", uri}}}};

    return server->handleRequestPublic(request);
}

std::optional<json> parseResourcePayload(const json& response, const std::string& uri) {
    if (!response.contains("result") || !response["result"].contains("contents") ||
        !response["result"]["contents"].is_array() || response["result"]["contents"].size() != 1) {
        return std::nullopt;
    }

    const auto& content = response["result"]["contents"][0];
    if (!content.is_object() || content.value("uri", std::string{}) != uri ||
        content.value("mimeType", std::string{}) != "application/json" ||
        !content.contains("text") || !content["text"].is_string()) {
        return std::nullopt;
    }

    return json::parse(content["text"].get<std::string>(), nullptr, false);
}

} // namespace

TEST_CASE("MCP resources/read returns TextResourceContents array for yams://status",
          "[mcp][resources][read][status][catch2]") {
    const auto response = readResourceResponse("yams://status");
    REQUIRE(response.has_value());
    const auto payload = parseResourcePayload(response.value(), "yams://status");
    REQUIRE(payload.has_value());
    REQUIRE_FALSE(payload->is_discarded());

    REQUIRE(payload->is_object());
    if (payload->contains("error")) {
        CHECK((*payload)["error"].get<std::string>().find("status error:") == 0);
    } else {
        CHECK(payload->contains("running"));
        CHECK(payload->contains("ready"));
        CHECK(payload->contains("version"));
    }
}

TEST_CASE("MCP resources/read returns TextResourceContents array for yams://stats",
          "[mcp][resources][read][stats][catch2]") {
    const auto response = readResourceResponse("yams://stats");
    REQUIRE(response.has_value());
    const auto payload = parseResourcePayload(response.value(), "yams://stats");
    REQUIRE(payload.has_value());
    REQUIRE_FALSE(payload->is_discarded());

    REQUIRE(payload->contains("error"));
    CHECK((*payload)["error"] == "Storage not initialized");
}

TEST_CASE("MCP resources/read returns TextResourceContents array for yams://recent",
          "[mcp][resources][read][recent][catch2]") {
    const auto response = readResourceResponse("yams://recent");
    REQUIRE(response.has_value());
    const auto payload = parseResourcePayload(response.value(), "yams://recent");
    REQUIRE(payload.has_value());
    REQUIRE_FALSE(payload->is_discarded());

    REQUIRE(payload->contains("error"));
    CHECK((*payload)["error"] == "Metadata repository not initialized");
}
