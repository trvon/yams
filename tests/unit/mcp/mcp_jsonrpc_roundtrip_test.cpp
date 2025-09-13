#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <nlohmann/json.hpp>
#include <memory>

#include <yams/mcp/mcp_server.h>

using nlohmann::json;
using ::testing::Contains;
using ::testing::Key;

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

TEST(MCPJsonRpcRoundtrip, InitializeThenListTools) {
    auto server = makeServer();

    // initialize
    json initReq = {{"jsonrpc", "2.0"},
                    {"id", 1},
                    {"method", "initialize"},
                    {"params",
                     {{"protocolVersion", "2024-11-05"},
                      {"clientInfo", {{"name", "inspector"}, {"version", "1.0.0"}}}}}};

    auto initRes = server->handleRequestPublic(initReq);
    ASSERT_TRUE(initRes) << "initialize returned error: " << initRes.error().message;
    const json& init = initRes.value();
    ASSERT_TRUE(init.is_object());
    EXPECT_EQ(init.value("jsonrpc", ""), "2.0");
    EXPECT_EQ(init.value("id", 0), 1);
    ASSERT_TRUE(init.contains("result"));
    const auto& result = init.at("result");
    ASSERT_TRUE(result.is_object());
    EXPECT_TRUE(result.contains("protocolVersion"));
    EXPECT_TRUE(result.contains("serverInfo"));
    EXPECT_TRUE(result.contains("capabilities"));

    // tools/list
    json listReq = {
        {"jsonrpc", "2.0"}, {"id", 2}, {"method", "tools/list"}, {"params", json::object()}};
    auto listRes = server->handleRequestPublic(listReq);
    ASSERT_TRUE(listRes) << "tools/list returned error: " << listRes.error().message;
    const json& list = listRes.value();
    ASSERT_TRUE(list.is_object());
    EXPECT_EQ(list.value("jsonrpc", ""), "2.0");
    EXPECT_EQ(list.value("id", 0), 2);
    ASSERT_TRUE(list.contains("result"));
    const auto& listResult = list.at("result");
    ASSERT_TRUE(listResult.is_object());
    ASSERT_TRUE(listResult.contains("tools"));
    ASSERT_TRUE(listResult.at("tools").is_array());
    // Ensure at least one tool with a name exists
    bool hasName = false;
    for (const auto& t : listResult.at("tools")) {
        if (t.is_object() && t.contains("name")) {
            hasName = true;
            break;
        }
    }
    EXPECT_TRUE(hasName);
}

TEST(MCPJsonRpcRoundtrip, UnknownMethodReturnsError) {
    auto server = makeServer();
    json badReq = {{"jsonrpc", "2.0"}, {"id", 3}, {"method", "does/not/exist"}};
    auto res = server->handleRequestPublic(badReq);
    ASSERT_TRUE(res) << "dispatch failed unexpectedly";
    const json& r = res.value();
    ASSERT_TRUE(r.is_object());
    EXPECT_EQ(r.value("jsonrpc", ""), "2.0");
    ASSERT_TRUE(r.contains("error"));
    const auto& err = r.at("error");
    ASSERT_TRUE(err.is_object());
    // -32601 "Method not found"
    EXPECT_EQ(err.value("code", 0), -32601);
}
