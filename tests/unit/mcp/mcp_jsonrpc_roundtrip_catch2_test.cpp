// MCP JSON-RPC roundtrip tests
// Catch2 migration from GTest (yams-3s4 / yams-84g)

#include <catch2/catch_test_macros.hpp>

#include <nlohmann/json.hpp>
#include <memory>

#include <yams/mcp/mcp_server.h>

using nlohmann::json;

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

std::unique_ptr<yams::mcp::MCPServer> makeServer() {
    auto t = std::make_unique<NullTransport>();
    return std::make_unique<yams::mcp::MCPServer>(std::move(t));
}

} // namespace

TEST_CASE("MCP JsonRpc Roundtrip - Initialize then list tools", "[mcp][jsonrpc][catch2]") {
    auto server = makeServer();

    // initialize
    json initReq = {{"jsonrpc", "2.0"},
                    {"id", 1},
                    {"method", "initialize"},
                    {"params",
                     {{"protocolVersion", "2024-11-05"},
                      {"clientInfo", {{"name", "inspector"}, {"version", "1.0.0"}}}}}};

    auto initRes = server->handleRequestPublic(initReq);
    REQUIRE(initRes);
    const json& init = initRes.value();
    REQUIRE(init.is_object());
    CHECK(init.value("jsonrpc", "") == "2.0");
    CHECK(init.value("id", 0) == 1);
    REQUIRE(init.contains("result"));
    const auto& result = init.at("result");
    REQUIRE(result.is_object());
    CHECK(result.contains("protocolVersion"));
    CHECK(result.contains("serverInfo"));
    CHECK(result.contains("capabilities"));

    // tools/list
    json listReq = {
        {"jsonrpc", "2.0"}, {"id", 2}, {"method", "tools/list"}, {"params", json::object()}};
    auto listRes = server->handleRequestPublic(listReq);
    REQUIRE(listRes);
    const json& list = listRes.value();
    REQUIRE(list.is_object());
    CHECK(list.value("jsonrpc", "") == "2.0");
    CHECK(list.value("id", 0) == 2);
    REQUIRE(list.contains("result"));
    const auto& listResult = list.at("result");
    REQUIRE(listResult.is_object());
    REQUIRE(listResult.contains("tools"));
    REQUIRE(listResult.at("tools").is_array());
    // Ensure at least one tool with a name exists
    bool hasName = false;
    for (const auto& t : listResult.at("tools")) {
        if (t.is_object() && t.contains("name")) {
            hasName = true;
            break;
        }
    }
    CHECK(hasName);
}

TEST_CASE("MCP JsonRpc Roundtrip - Unknown method returns error", "[mcp][jsonrpc][catch2]") {
    auto server = makeServer();
    json badReq = {{"jsonrpc", "2.0"}, {"id", 3}, {"method", "does/not/exist"}};
    auto res = server->handleRequestPublic(badReq);
    REQUIRE(res);
    const json& r = res.value();
    REQUIRE(r.is_object());
    CHECK(r.value("jsonrpc", "") == "2.0");
    REQUIRE(r.contains("error"));
    const auto& err = r.at("error");
    REQUIRE(err.is_object());
    // -32601 "Method not found"
    CHECK(err.value("code", 0) == -32601);
}
