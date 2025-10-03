#include <gtest/gtest.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <string>

#include <yams/mcp/tool_registry.h>

namespace {

using yams::Error;
using yams::ErrorCode;
using yams::Result;
using yams::mcp::json;
using yams::mcp::ToolRegistry;

struct DummyRequest {
    using RequestType = DummyRequest;

    int value = 0;

    static DummyRequest fromJson(const json& j) {
        if (!j.contains("value")) {
            throw nlohmann::json::type_error::create(302, "value missing", &j);
        }
        DummyRequest req;
        req.value = j.at("value").get<int>();
        return req;
    }

    json toJson() const { return json{{"value", value}}; }
};

struct DummyResponse {
    using ResponseType = DummyResponse;

    int value = 0;

    static DummyResponse fromJson(const json& j) {
        DummyResponse resp;
        resp.value = j.value("value", 0);
        return resp;
    }

    json toJson() const { return json{{"value", value}}; }
};

json runCall(ToolRegistry& registry, std::string_view name, json args) {
    boost::asio::io_context io;
    auto future = boost::asio::co_spawn(io, registry.callTool(name, std::move(args)),
                                        boost::asio::use_future);
    io.run();
    return future.get();
}

} // namespace

TEST(MCPToolRegistryWrappersTest, WrapsSuccessfulResultPayload) {
    ToolRegistry registry;
    registry.registerTool<DummyRequest, DummyResponse>(
        "dummy",
        [](const DummyRequest& req) -> boost::asio::awaitable<Result<DummyResponse>> {
            co_return DummyResponse{req.value + 1};
        },
        json{}, std::string{"temporary description"});

    auto discovery = registry.listTools();
    ASSERT_TRUE(discovery.contains("tools"));
    ASSERT_EQ(discovery["tools"].size(), 1);
    EXPECT_EQ(discovery["tools"][0]["name"], "dummy");
    EXPECT_EQ(discovery["tools"][0]["description"], "temporary description");

    const auto response = runCall(registry, "dummy", json{{"value", 1}});
    ASSERT_TRUE(response.contains("content"));
    const auto& content = response.at("content");
    ASSERT_TRUE(content.is_array());
    ASSERT_EQ(content.size(), 1);
    EXPECT_EQ(content[0].at("type"), "text");
    EXPECT_EQ(content[0].at("text"), R"({"value":2})");
    EXPECT_FALSE(response.value("isError", false));
}

TEST(MCPToolRegistryWrappersTest, PropagatesHandlerErrorsViaHelper) {
    ToolRegistry registry;
    registry.registerTool<DummyRequest, DummyResponse>(
        "dummy-error", [](const DummyRequest&) -> boost::asio::awaitable<Result<DummyResponse>> {
            co_return Error{ErrorCode::InvalidArgument, "bad input"};
        });

    const auto response = runCall(registry, "dummy-error", json{{"value", 0}});
    ASSERT_TRUE(response.value("isError", false));
    ASSERT_TRUE(response.contains("content"));
    const auto message = response["content"][0]["text"].get<std::string>();
    EXPECT_EQ(message, "Error: bad input");
}

TEST(MCPToolRegistryWrappersTest, ReportsJsonExceptionsConsistently) {
    ToolRegistry registry;
    registry.registerTool<DummyRequest, DummyResponse>(
        "dummy-json", [](const DummyRequest& req) -> boost::asio::awaitable<Result<DummyResponse>> {
            co_return DummyResponse{req.value};
        });

    const auto response = runCall(registry, "dummy-json", json::object());
    ASSERT_TRUE(response.value("isError", false));
    const auto message = response["content"][0]["text"].get<std::string>();
    EXPECT_NE(message.find("JSON error:"), std::string::npos);
}

TEST(MCPToolRegistryWrappersTest, UnknownToolsReturnHelpfulError) {
    ToolRegistry registry;

    const auto response = runCall(registry, "not-registered", json::object());
    ASSERT_TRUE(response.value("isError", false));
    const auto message = response["content"][0]["text"].get<std::string>();
    EXPECT_EQ(message, "Unknown tool: not-registered");
}
