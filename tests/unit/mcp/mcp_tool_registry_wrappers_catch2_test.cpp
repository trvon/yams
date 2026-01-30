// MCP Tool Registry wrappers tests
// Catch2 migration from GTest (yams-3s4 / yams-84g)

#include <catch2/catch_test_macros.hpp>

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

TEST_CASE("MCP ToolRegistryWrappers - Wraps successful result payload",
          "[mcp][registry][wrappers][catch2]") {
    ToolRegistry registry;
    registry.registerTool<DummyRequest, DummyResponse>(
        "dummy",
        [](const DummyRequest& req) -> boost::asio::awaitable<Result<DummyResponse>> {
            co_return DummyResponse{req.value + 1};
        },
        json{}, std::string{"temporary description"});

    auto discovery = registry.listTools();
    REQUIRE(discovery.contains("tools"));
    REQUIRE(discovery["tools"].size() == 1);
    CHECK(discovery["tools"][0]["name"] == "dummy");
    CHECK(discovery["tools"][0]["description"] == "temporary description");

    const auto response = runCall(registry, "dummy", json{{"value", 1}});
    REQUIRE(response.contains("content"));
    const auto& content = response.at("content");
    REQUIRE(content.is_array());
    REQUIRE(content.size() == 1);
    CHECK(content[0].at("type") == "text");
    CHECK(content[0].at("text") == R"({"value":2})");
    CHECK_FALSE(response.value("isError", false));
}

TEST_CASE("MCP ToolRegistryWrappers - Propagates handler errors via helper",
          "[mcp][registry][wrappers][catch2]") {
    ToolRegistry registry;
    registry.registerTool<DummyRequest, DummyResponse>(
        "dummy-error", [](const DummyRequest&) -> boost::asio::awaitable<Result<DummyResponse>> {
            co_return Error{ErrorCode::InvalidArgument, "bad input"};
        });

    const auto response = runCall(registry, "dummy-error", json{{"value", 0}});
    REQUIRE(response.value("isError", false));
    REQUIRE(response.contains("content"));
    const auto message = response["content"][0]["text"].get<std::string>();
    CHECK(message == "Error: bad input");
}

TEST_CASE("MCP ToolRegistryWrappers - Reports JSON exceptions consistently",
          "[mcp][registry][wrappers][catch2]") {
    ToolRegistry registry;
    registry.registerTool<DummyRequest, DummyResponse>(
        "dummy-json", [](const DummyRequest& req) -> boost::asio::awaitable<Result<DummyResponse>> {
            co_return DummyResponse{req.value};
        });

    const auto response = runCall(registry, "dummy-json", json::object());
    REQUIRE(response.value("isError", false));
    const auto message = response["content"][0]["text"].get<std::string>();
    CHECK(message.find("JSON error:") != std::string::npos);
}

TEST_CASE("MCP ToolRegistryWrappers - Unknown tools return helpful error",
          "[mcp][registry][wrappers][catch2]") {
    ToolRegistry registry;

    const auto response = runCall(registry, "not-registered", json::object());
    REQUIRE(response.value("isError", false));
    const auto message = response["content"][0]["text"].get<std::string>();
    CHECK(message == "Unknown tool: not-registered");
}

TEST_CASE("MCP ToolRegistry - Duplicate registration updates annotations",
          "[mcp][registry][annotations][catch2]") {
    ToolRegistry registry;

    // First registration has no annotations
    registry.registerTool<DummyRequest, DummyResponse>(
        "dummy",
        [](const DummyRequest& req) -> boost::asio::awaitable<Result<DummyResponse>> {
            co_return DummyResponse{req.value};
        },
        json{}, std::string{"temporary description"});

    auto first = registry.listTools();
    REQUIRE(first.contains("tools"));
    REQUIRE(first["tools"].size() == 1);
    REQUIRE(first["tools"][0].contains("annotations"));
    CHECK(first["tools"][0]["annotations"].is_object());
    CHECK(first["tools"][0]["annotations"].empty());

    // Second registration uses same tool name but adds annotations
    yams::mcp::ToolAnnotation ann;
    ann.readOnlyHint = true;
    ann.idempotentHint = true;

    registry.registerTool<DummyRequest, DummyResponse>(
        "dummy",
        [](const DummyRequest& req) -> boost::asio::awaitable<Result<DummyResponse>> {
            co_return DummyResponse{req.value + 1};
        },
        json{}, std::string{"updated description"}, std::string{}, ann);

    auto second = registry.listTools();
    REQUIRE(second.contains("tools"));
    REQUIRE(second["tools"].size() == 1);
    CHECK(second["tools"][0]["description"] == "updated description");
    REQUIRE(second["tools"][0].contains("annotations"));
    CHECK(second["tools"][0]["annotations"].value("readOnlyHint", false) == true);
    CHECK(second["tools"][0]["annotations"].value("idempotentHint", false) == true);

    // Confirm handler updated too
    const auto response = runCall(registry, "dummy", json{{"value", 1}});
    CHECK_FALSE(response.value("isError", false));
    CHECK(response["content"][0]["text"].get<std::string>() == R"({"value":2})");
}
