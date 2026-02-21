// MCP Code Mode tests: pipeline query, batch execute, session action, $prev resolution
// Part of the Code Mode feature (docs/design/mcp-code-mode.md)

#include <catch2/catch_test_macros.hpp>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <set>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>
#include <yams/mcp/mcp_server.h>
#include <yams/mcp/tool_registry.h>

namespace {

using json = nlohmann::json;
using yams::mcp::MCPServer;

// ── $prev resolver tests (static, uses YAMS_TESTING public wrapper) ──

TEST_CASE("resolvePrevRefs - no refs passes through", "[mcp][codemode][prev]") {
    json params = {{"query", "hello"}, {"limit", 10}};
    json prev = {{"total", 42}};

    auto result = MCPServer::testResolvePrevRefs(params, prev);
    CHECK(result["query"] == "hello");
    CHECK(result["limit"] == 10);
}

TEST_CASE("resolvePrevRefs - $prev replaces with entire prev", "[mcp][codemode][prev]") {
    json params = {{"data", "$prev"}};
    json prev = {{"total", 42}, {"items", json::array({1, 2, 3})}};

    auto result = MCPServer::testResolvePrevRefs(params, prev);
    CHECK(result["data"] == prev);
}

TEST_CASE("resolvePrevRefs - $prev.field resolves top-level field", "[mcp][codemode][prev]") {
    json params = {{"hash", "$prev.hash"}, {"name", "static"}};
    json prev = {{"hash", "sha256-abc123"}, {"score", 0.95}};

    auto result = MCPServer::testResolvePrevRefs(params, prev);
    CHECK(result["hash"] == "sha256-abc123");
    CHECK(result["name"] == "static");
}

TEST_CASE("resolvePrevRefs - $prev.arr[N] resolves array element", "[mcp][codemode][prev]") {
    json params = {{"id", "$prev.results[0]"}};
    json prev = {{"results", json::array({"first", "second", "third"})}};

    auto result = MCPServer::testResolvePrevRefs(params, prev);
    CHECK(result["id"] == "first");
}

TEST_CASE("resolvePrevRefs - $prev.arr[N].field resolves nested field in array",
          "[mcp][codemode][prev]") {
    json params = {{"hash", "$prev.results[1].hash"}};
    json prev = {{"results", json::array({json{{"hash", "aaa"}}, json{{"hash", "bbb"}},
                                          json{{"hash", "ccc"}}})}};

    auto result = MCPServer::testResolvePrevRefs(params, prev);
    CHECK(result["hash"] == "bbb");
}

TEST_CASE("resolvePrevRefs - missing field resolves to null", "[mcp][codemode][prev]") {
    json params = {{"hash", "$prev.nonexistent"}};
    json prev = {{"total", 42}};

    auto result = MCPServer::testResolvePrevRefs(params, prev);
    CHECK(result["hash"].is_null());
}

TEST_CASE("resolvePrevRefs - out of bounds index resolves to null", "[mcp][codemode][prev]") {
    json params = {{"hash", "$prev.results[99]"}};
    json prev = {{"results", json::array({"only_one"})}};

    auto result = MCPServer::testResolvePrevRefs(params, prev);
    CHECK(result["hash"].is_null());
}

TEST_CASE("resolvePrevRefs - non-string values are not touched", "[mcp][codemode][prev]") {
    json params = {{"limit", 10}, {"fuzzy", true}, {"nested", json{{"a", 1}}}};
    json prev = {{"anything", "value"}};

    auto result = MCPServer::testResolvePrevRefs(params, prev);
    CHECK(result["limit"] == 10);
    CHECK(result["fuzzy"] == true);
    CHECK(result["nested"] == json{{"a", 1}});
}

TEST_CASE("resolvePrevRefs - non-$prev strings are not touched", "[mcp][codemode][prev]") {
    json params = {{"query", "search term"}, {"type", "hybrid"}};
    json prev = {{"anything", "value"}};

    auto result = MCPServer::testResolvePrevRefs(params, prev);
    CHECK(result["query"] == "search term");
    CHECK(result["type"] == "hybrid");
}

TEST_CASE("resolvePrevRefs - empty params returns empty", "[mcp][codemode][prev]") {
    json params = json::object();
    json prev = {{"total", 42}};

    auto result = MCPServer::testResolvePrevRefs(params, prev);
    CHECK(result.empty());
}

TEST_CASE("resolvePrevRefs - non-object returns as-is", "[mcp][codemode][prev]") {
    json params = json::array({1, 2, 3});
    json prev = {{"total", 42}};

    auto result = MCPServer::testResolvePrevRefs(params, prev);
    CHECK(result == json::array({1, 2, 3}));
}

TEST_CASE("resolvePrevRefs - deep nested path", "[mcp][codemode][prev]") {
    json params = {{"val", "$prev.a.b.c"}};
    json prev = {{"a", {{"b", {{"c", "deep_value"}}}}}};

    auto result = MCPServer::testResolvePrevRefs(params, prev);
    CHECK(result["val"] == "deep_value");
}

// ── describeOp / describeAllOps tests (require server instance) ──

// Minimal no-op transport for constructing MCPServer
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

TEST_CASE("describeOp - known op returns schema", "[mcp][codemode][describe]") {
    auto server = std::make_shared<MCPServer>(std::make_unique<NullTransport>());
    auto result = server->testDescribeOp("search");
    CHECK(result["op"] == "search");
    CHECK(result.contains("description"));
    CHECK(result.contains("paramsSchema"));
    CHECK(result["category"] == "query");
    CHECK(result["readOnly"] == true);

    // Verify schema has required fields
    auto& schema = result["paramsSchema"];
    CHECK(schema["type"] == "object");
    CHECK(schema.contains("properties"));
    CHECK(schema["properties"].contains("query"));
}

TEST_CASE("describeOp - unknown op returns error with available ops", "[mcp][codemode][describe]") {
    auto server = std::make_shared<MCPServer>(std::make_unique<NullTransport>());
    auto result = server->testDescribeOp("nonexistent");
    CHECK(result.contains("error"));
    CHECK(result.contains("available"));
    CHECK(result["available"].contains("query_ops"));
}

TEST_CASE("describeOp - write ops have correct category", "[mcp][codemode][describe]") {
    auto server = std::make_shared<MCPServer>(std::make_unique<NullTransport>());

    for (const auto& op : {"add", "update", "delete", "restore", "download"}) {
        auto result = server->testDescribeOp(op);
        CHECK(result["category"] == "execute");
        CHECK(result["readOnly"] == false);
    }
}

TEST_CASE("describeOp - session ops have correct category", "[mcp][codemode][describe]") {
    auto server = std::make_shared<MCPServer>(std::make_unique<NullTransport>());

    for (const auto& op :
         {"session_start", "session_stop", "session_pin", "session_unpin", "session_watch"}) {
        auto result = server->testDescribeOp(op);
        CHECK(result["category"] == "session");
    }
}

TEST_CASE("describeAllOps - returns all categories", "[mcp][codemode][describe]") {
    auto server = std::make_shared<MCPServer>(std::make_unique<NullTransport>());
    auto result = server->testDescribeAllOps();

    CHECK(result.contains("query_ops"));
    CHECK(result.contains("execute_ops"));
    CHECK(result.contains("session_ops"));

    // Spot check key ops are present
    auto& queryOps = result["query_ops"];
    CHECK(queryOps.is_array());
    bool hasSearch = false;
    for (const auto& op : queryOps) {
        if (op == "search")
            hasSearch = true;
    }
    CHECK(hasSearch);

    auto& execOps = result["execute_ops"];
    CHECK(execOps.is_array());
    bool hasAdd = false;
    for (const auto& op : execOps) {
        if (op == "add")
            hasAdd = true;
    }
    CHECK(hasAdd);
}

// ── Code Mode tool registration tests ──

TEST_CASE("Code Mode - registers composite tools", "[mcp][codemode][registration]") {
    auto server = std::make_shared<MCPServer>(std::make_unique<NullTransport>());

    // Use testListTools() to verify only composite tools are exposed
    auto tools = server->testListTools();
    CHECK(tools.contains("tools"));
    auto& toolArr = tools["tools"];
    CHECK(toolArr.is_array());

    // Build a set of tool names
    std::set<std::string> names;
    for (const auto& t : toolArr) {
        if (t.contains("name")) {
            names.insert(t["name"].get<std::string>());
        }
    }

    // Should have the composite tools
    CHECK(names.count("query") == 1);
    CHECK(names.count("execute") == 1);

    // Should NOT have individual tools
    CHECK(names.count("search") == 0);
    CHECK(names.count("grep") == 0);
    CHECK(names.count("list") == 0);
    CHECK(names.count("add") == 0);
}

} // namespace
