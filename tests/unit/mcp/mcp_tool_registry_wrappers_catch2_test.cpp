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

TEST_CASE("MCP ToolRegistryWrappers - Reports std::exception consistently",
          "[mcp][registry][wrappers][catch2]") {
    ToolRegistry registry;
    registry.registerTool<DummyRequest, DummyResponse>(
        "dummy-throw",
        [](const DummyRequest& req) -> boost::asio::awaitable<Result<DummyResponse>> {
            if (req.value == 0) {
                throw std::runtime_error("boom");
            }
            co_return DummyResponse{req.value};
        });

    const auto response = runCall(registry, "dummy-throw", json{{"value", 0}});
    REQUIRE(response.value("isError", false));
    const auto message = response["content"][0]["text"].get<std::string>();
    CHECK(message == "Error: boom");
}

TEST_CASE("MCP DTO parsing - SessionStartRequest reads defaults and fields",
          "[mcp][dto][session][catch2]") {
    using yams::mcp::MCPSessionStartRequest;

    {
        const auto r = MCPSessionStartRequest::fromJson(json::object());
        CHECK(r.warm == true);
        CHECK(r.limit == 200);
        CHECK(r.snippetLen == 160);
        CHECK(r.cores == -1);
        CHECK(r.memoryGb == -1);
        CHECK(r.timeMs == -1);
        CHECK(r.aggressive == false);
    }

    {
        json j = {
            {"name", "s"},
            {"description", "d"},
            {"warm", false},
            {"limit", 123},
            {"snippet_len", 45},
            {"cores", 2},
            {"memory_gb", 8},
            {"time_ms", 9999},
            {"aggressive", true},
        };
        const auto r = MCPSessionStartRequest::fromJson(j);
        CHECK(r.name == "s");
        CHECK(r.description == "d");
        CHECK(r.warm == false);
        CHECK(r.limit == 123);
        CHECK(r.snippetLen == 45);
        CHECK(r.cores == 2);
        CHECK(r.memoryGb == 8);
        CHECK(r.timeMs == 9999);
        CHECK(r.aggressive == true);
    }
}

TEST_CASE("MCP DTO parsing - SessionWatchRequest applies aliases and toggles",
          "[mcp][dto][session][catch2]") {
    using yams::mcp::MCPSessionWatchRequest;

    json j = {
        {"session", "sess"},
        {"root", "."},
        // Cover alias path: interval_ms default 0, but interval provided.
        {"interval", 50},
        // Cover stop/disable/no_selector/no_use overrides.
        {"enabled", true},
        {"stop", true},
        {"no_selector", true},
        {"no_use", true},
    };

    const auto r = MCPSessionWatchRequest::fromJson(j);
    CHECK(r.session == "sess");
    CHECK(r.root == ".");
    CHECK(r.intervalMs == 50);
    CHECK(r.enable == false);
    CHECK(r.addSelector == false);
    CHECK(r.setCurrent == false);
    CHECK(r.allowCreate == true);
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

TEST_CASE("MCP ToolRegistry - Duplicate registration updates schema/title",
          "[mcp][registry][metadata][catch2]") {
    ToolRegistry registry;

    registry.registerTool<DummyRequest, DummyResponse>(
        "dummy-meta",
        [](const DummyRequest& req) -> boost::asio::awaitable<Result<DummyResponse>> {
            co_return DummyResponse{req.value};
        },
        json{}, std::string{"desc1"});

    // Update schema + title + description
    json schema = {
        {"type", "object"},
        {"properties", json{{"value", json{{"type", "integer"}}}}},
    };

    registry.registerTool<DummyRequest, DummyResponse>(
        "dummy-meta",
        [](const DummyRequest& req) -> boost::asio::awaitable<Result<DummyResponse>> {
            co_return DummyResponse{req.value + 10};
        },
        schema, std::string{"desc2"}, std::string{"Title"});

    const auto tools = registry.listTools();
    REQUIRE(tools.contains("tools"));
    REQUIRE(tools["tools"].is_array());
    REQUIRE(tools["tools"].size() == 1);

    const auto& t = tools["tools"][0];
    REQUIRE(t.is_object());
    CHECK(t.value("name", "") == "dummy-meta");
    CHECK(t.value("title", "") == "Title");
    CHECK(t.value("description", "") == "desc2");
    REQUIRE(t.contains("inputSchema"));
    CHECK(t["inputSchema"] == schema);

    const auto response = runCall(registry, "dummy-meta", json{{"value", 1}});
    CHECK_FALSE(response.value("isError", false));
    CHECK(response["content"][0]["text"].get<std::string>() == R"({"value":11})");
}

TEST_CASE("MCP ToolRegistry detail - readStringArray handles missing/null/array/string",
          "[mcp][registry][detail][catch2]") {
    using yams::mcp::detail::readStringArray;

    {
        std::vector<std::string> out = {"keep"};
        readStringArray(json::object(), "k", out);
        CHECK(out == std::vector<std::string>{"keep"});
    }

    {
        std::vector<std::string> out;
        readStringArray(json{{"k", nullptr}}, "k", out);
        CHECK(out.empty());
    }

    {
        std::vector<std::string> out;
        readStringArray(json{{"k", json::array({"a", 1, "b", false, "c"})}}, "k", out);
        CHECK(out == std::vector<std::string>{"a", "b", "c"});
    }

    {
        std::vector<std::string> out;
        readStringArray(json{{"k", "solo"}}, "k", out);
        CHECK(out == std::vector<std::string>{"solo"});
    }
}

TEST_CASE("MCP ToolRegistry detail - jsonValueOr returns defaults and values",
          "[mcp][registry][detail][catch2]") {
    using yams::mcp::detail::jsonValueOr;

    {
        const json j = json{{"x", 9}};
        CHECK(jsonValueOr(j, "x", uint64_t{7}) == 9u);
        CHECK(jsonValueOr(j, "missing", uint64_t{7}) == 7u);
    }

    {
        const json j = json{{"f", 2.5}};
        CHECK(jsonValueOr(j, "f", 1.5) == 2.5);
        CHECK(jsonValueOr(j, "missing", 1.5) == 1.5);
        CHECK(jsonValueOr(json{{"f", nullptr}}, "f", 1.5) == 1.5);
    }

    {
        const json def = json{{"a", 1}};
        CHECK(jsonValueOr(json{{"obj", json{{"b", 2}}}}, "obj", def) == json{{"b", 2}});
        CHECK(jsonValueOr(json::object(), "obj", def) == def);
        CHECK(jsonValueOr(json{{"obj", nullptr}}, "obj", def) == def);
    }
}

TEST_CASE("MCP ToolRegistry - registerRawTool duplicate updates handler and descriptor",
          "[mcp][registry][raw][catch2]") {
    ToolRegistry registry;

    registry.registerRawTool(
        "raw",
        [](const json&) -> boost::asio::awaitable<json> {
            co_return json{{"content", json::array({json{{"type", "text"}, {"text", "one"}}})}};
        },
        json{}, std::string{"desc1"});

    {
        const auto tools = registry.listTools();
        REQUIRE(tools.contains("tools"));
        REQUIRE(tools["tools"].size() == 1);
        const auto& t = tools["tools"][0];
        REQUIRE(t.is_object());
        CHECK(t.value("name", "") == "raw");
        CHECK(t.value("description", "") == "desc1");
        CHECK_FALSE(t.contains("title"));
        REQUIRE(t.contains("inputSchema"));
        CHECK(t["inputSchema"] == json{{"type", "object"}});
        REQUIRE(t.contains("annotations"));
        CHECK(t["annotations"].is_object());
    }

    CHECK(runCall(registry, "raw", json::object())["content"][0]["text"].get<std::string>() == "one");

    yams::mcp::ToolAnnotation ann;
    ann.readOnlyHint = true;
    json schema = {
        {"type", "object"},
        {"properties", json{{"x", json{{"type", "string"}}}}},
    };

    registry.registerRawTool(
        "raw",
        [](const json&) -> boost::asio::awaitable<json> {
            co_return json{{"content", json::array({json{{"type", "text"}, {"text", "two"}}})}};
        },
        schema, std::string{"desc2"}, std::string{"RawTitle"}, ann);

    {
        const auto tools = registry.listTools();
        REQUIRE(tools.contains("tools"));
        REQUIRE(tools["tools"].size() == 1);
        const auto& t = tools["tools"][0];
        REQUIRE(t.is_object());
        CHECK(t.value("name", "") == "raw");
        CHECK(t.value("description", "") == "desc2");
        CHECK(t.value("title", "") == "RawTitle");
        CHECK(t["inputSchema"] == schema);
        CHECK(t["annotations"].value("readOnlyHint", false) == true);
    }

    CHECK(runCall(registry, "raw", json::object())["content"][0]["text"].get<std::string>() == "two");
}
