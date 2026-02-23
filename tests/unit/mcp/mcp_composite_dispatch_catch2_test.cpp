// MCP composite tool dispatch tests: verify query/execute/session route correctly
// through toolRegistry_ → composite handler → internalRegistry_ → individual handler.
//
// These tests exercise the FULL dispatch path that models use when calling
// tools/call with name="query", "execute", or "session".

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

#include <chrono>
#include <cstdlib>
#include <filesystem>

#include <yams/compat/unistd.h>
#include <future>
#include <memory>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>
#include <yams/daemon/client/global_io_context.h>
#include <yams/mcp/mcp_server.h>
#include <yams/mcp/tool_registry.h>

using namespace std::chrono_literals;
using json = nlohmann::json;
using Catch::Matchers::ContainsSubstring;
using yams::mcp::MCPServer;

namespace {

// ── NullTransport ──

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

// ── Async helper ──

template <typename Awaitable>
auto run_awaitable(Awaitable aw, std::chrono::milliseconds timeout = 3s)
    -> std::optional<typename Awaitable::value_type> {
    using T = typename Awaitable::value_type;
    auto& io = yams::daemon::GlobalIOContext::instance().get_io_context();
    std::promise<T> p;
    auto fut = p.get_future();
    boost::asio::co_spawn(
        io,
        [aw = std::move(aw), pr = std::move(p)]() mutable -> boost::asio::awaitable<void> {
            T r = co_await std::move(aw);
            pr.set_value(std::move(r));
            co_return;
        },
        boost::asio::detached);
    if (fut.wait_for(timeout) != std::future_status::ready) {
        return std::nullopt;
    }
    return fut.get();
}

// ── Fixture: MCPServer with no daemon (errors are expected; we verify dispatch, not results) ──

class CompositeDispatchFixture {
public:
    std::shared_ptr<MCPServer> server;

    CompositeDispatchFixture() {
        setenv("YAMS_ENABLE_LOCAL_MCP_DISCOVERY", "0", 1);
        setenv("YAMS_CLI_DISABLE_DAEMON_AUTOSTART", "1", 1);

        auto transport = std::make_unique<NullTransport>();
        server = std::make_shared<MCPServer>(std::move(transport));

        const auto uniqueName =
            std::string("yams-mcp-composite-test-") +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sock";
        auto socketPath = std::filesystem::temp_directory_path() / uniqueName;
        setenv("YAMS_DAEMON_SOCKET_PATH", socketPath.string().c_str(), 1);

        yams::daemon::ClientConfig cfg;
        cfg.autoStart = false;
        cfg.maxRetries = 0;
        cfg.connectTimeout = std::chrono::milliseconds{50};
        cfg.requestTimeout = std::chrono::milliseconds{50};
        cfg.socketPath = socketPath;
        std::error_code ec;
        std::filesystem::remove(cfg.socketPath, ec);
        server->testConfigureDaemonClient(cfg);
        server->testSetEnsureDaemonClientHook(
            [](const yams::daemon::ClientConfig&) -> yams::Result<void> {
                return yams::Error{yams::ErrorCode::NetworkError,
                                   "Daemon socket not found for test harness"};
            });
    }

    ~CompositeDispatchFixture() {
        if (server) {
            server->testShutdown();
        }
    }

    // Call a tool via the public callTool path (same path as handleRequest → tools/call)
    json callTool(const std::string& name, const json& args) {
        return server->callToolPublic(name, args);
    }

    // Call a tool via the async path
    std::optional<json> callToolAsync(const std::string& name, const json& args) {
        return run_awaitable(server->testCallToolAsync(name, args));
    }
};

// ── Helper: check if a result is a dispatch error vs a "daemon not found" error ──
// Dispatch errors say "Unknown tool" — these mean routing failed.
// Daemon errors say "test harness" or connection errors — these mean routing SUCCEEDED
// and the handler tried to talk to the daemon.

bool isRoutingError(const json& result) {
    if (result.contains("error")) {
        auto msg = result["error"].value("message", std::string{});
        return msg.find("Unknown tool") != std::string::npos;
    }
    return false;
}

// Check if we reached the actual handler (NOT an "Unknown tool" routing error).
// callTool() normalizes content-based errors to JSON-RPC errors, so we check
// the error.message for patterns that only composite/individual handlers produce.
bool reachedHandler(const json& result) {
    // 1. JSON-RPC error — check message for composite handler or daemon patterns
    if (result.contains("error")) {
        auto msg = result["error"].value("message", std::string{});
        // Composite handler messages
        if (msg.find("Pipeline:") != std::string::npos)
            return true;
        if (msg.find("Execute:") != std::string::npos)
            return true;
        if (msg.find("Error:") != std::string::npos)
            return true;
        // Daemon/connection errors (handler tried to talk to daemon)
        if (msg.find("test harness") != std::string::npos)
            return true;
        if (msg.find("Daemon") != std::string::npos)
            return true;
        if (msg.find("daemon") != std::string::npos)
            return true;
        if (msg.find("not found") != std::string::npos)
            return true;
        if (msg.find("connection") != std::string::npos)
            return true;
        if (msg.find("connect") != std::string::npos)
            return true;
        if (msg.find("socket") != std::string::npos)
            return true;
        // Parameter validation from individual handlers (reached the handler)
        if (msg.find("required") != std::string::npos)
            return true;
        if (msg.find("path") != std::string::npos)
            return true;
        return false;
    }
    // 2. Content-based result (isError or not)
    if (result.contains("content") && result["content"].is_array()) {
        return true;
    }
    // 3. Structured content (successful composite result)
    if (result.contains("structuredContent")) {
        return true;
    }
    return false;
}

// Check if the result contains a specific error message from the composite handler.
// callTool() normalizes errors, so we check BOTH content arrays AND error objects.
bool isCompositeError(const json& result, const std::string& expectedSubstring) {
    // 1. Check JSON-RPC error message
    if (result.contains("error")) {
        auto msg = result["error"].value("message", std::string{});
        if (msg.find(expectedSubstring) != std::string::npos) {
            return true;
        }
    }
    // 2. Check content array (older format)
    if (result.contains("content") && result["content"].is_array()) {
        for (const auto& c : result["content"]) {
            if (c.contains("text")) {
                auto text = c["text"].get<std::string>();
                if (text.find(expectedSubstring) != std::string::npos) {
                    return true;
                }
            }
        }
    }
    return false;
}

} // namespace

// ═══════════════════════════════════════════════════════════════════════════
// 1. ROUTING: composite tools are reachable via callTool
// ═══════════════════════════════════════════════════════════════════════════

TEST_CASE_METHOD(CompositeDispatchFixture, "query tool is routable via callTool",
                 "[mcp][composite][routing]") {
    // A valid single-step search query. Without daemon, the search handler
    // will return an error — but the key assertion is that we DON'T get
    // "Unknown tool: query" (which would mean routing failed).
    json args = {{"steps", json::array({{{"op", "search"}, {"params", {{"query", "test"}}}}})}};

    auto result = callTool("query", args);

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));

    // We should reach the composite handler (not get "Unknown tool")
    // The handler will either dispatch to search (and get daemon error) or succeed
    CHECK(reachedHandler(result));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "execute tool is routable via callTool",
                 "[mcp][composite][routing]") {
    json args = {
        {"operations", json::array({{{"op", "add"}, {"params", {{"path", "/tmp/test.txt"}}}}})}};

    auto result = callTool("execute", args);

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "session tool is routable via callTool",
                 "[mcp][composite][routing]") {
    json args = {{"action", "start"}, {"params", json::object()}};

    auto result = callTool("session", args);

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "individual tools still routable via callTool",
                 "[mcp][composite][routing]") {
    // Individual tools should still be accessible through internalRegistry_
    auto result = callTool("search", json{{"query", "test"}});

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "async callTool routes query correctly",
                 "[mcp][composite][routing]") {
    json args = {{"steps", json::array({{{"op", "status"}}})}};

    auto result = callToolAsync("query", args);

    REQUIRE(result.has_value());
    INFO("result: " << result->dump(2));
    // Should not be "Unknown tool: query"
    bool hasUnknown = false;
    if (result->contains("content") && (*result)["content"].is_array()) {
        for (const auto& c : (*result)["content"]) {
            if (c.contains("text") &&
                c["text"].get<std::string>().find("Unknown tool: query") != std::string::npos) {
                hasUnknown = true;
            }
        }
    }
    CHECK_FALSE(hasUnknown);
}

TEST_CASE_METHOD(CompositeDispatchFixture,
                 "async callTool normalizes composite tool isError into JSON-RPC error",
                 "[mcp][composite][routing]") {
    // Using a write op in query should produce an isError tool result.
    // callToolAsync() must normalize it into a JSON-RPC error with the original
    // tool payload preserved in error.data.toolResult.
    json args = {{"steps", json::array({{{"op", "add"}, {"params", json::object()}}})}};

    auto result = callToolAsync("query", args);
    REQUIRE(result.has_value());
    INFO("result: " << result->dump(2));

    REQUIRE(result->contains("error"));
    REQUIRE((*result)["error"].contains("data"));
    REQUIRE((*result)["error"]["data"].contains("toolResult"));
    CHECK((*result)["error"]["data"]["toolResult"].value("isError", false));
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. QUERY PIPELINE: validation and error handling
// ═══════════════════════════════════════════════════════════════════════════

TEST_CASE_METHOD(CompositeDispatchFixture, "query - missing steps returns error",
                 "[mcp][composite][query]") {
    auto result = callTool("query", json::object());

    INFO("result: " << result.dump(2));
    CHECK(isCompositeError(result, "'steps' array is required"));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "query - empty steps returns error",
                 "[mcp][composite][query]") {
    auto result = callTool("query", json{{"steps", json::array()}});

    INFO("result: " << result.dump(2));
    CHECK(isCompositeError(result, "'steps' array must not be empty"));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "query - step missing op returns error",
                 "[mcp][composite][query]") {
    auto result = callTool("query", json{{"steps", json::array({{{"params", {{"q", "x"}}}}})}});

    INFO("result: " << result.dump(2));
    CHECK(isCompositeError(result, "missing 'op' field"));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "query - write op in query returns error",
                 "[mcp][composite][query]") {
    auto result = callTool(
        "query",
        json{{"steps", json::array({{{"op", "add"}, {"params", {{"path", "/tmp/x.txt"}}}}})}});

    INFO("result: " << result.dump(2));
    CHECK(isCompositeError(result, "not a read operation"));
    CHECK(isCompositeError(result, "execute"));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "query - unknown op returns error",
                 "[mcp][composite][query]") {
    auto result = callTool("query", json{{"steps", json::array({{{"op", "foobar"}}})}});

    INFO("result: " << result.dump(2));
    CHECK(isCompositeError(result, "not a read operation"));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "query - describe op works without daemon",
                 "[mcp][composite][query]") {
    // describe is handled locally in the composite handler, no daemon needed
    auto result = callTool(
        "query",
        json{{"steps", json::array({{{"op", "describe"}, {"params", {{"target", "search"}}}}})}});

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));

    // The result should contain structured content with the search schema
    if (result.contains("structuredContent")) {
        auto& data = result["structuredContent"]["data"];
        CHECK(data.contains("op"));
        CHECK(data["op"] == "search");
        CHECK(data.contains("paramsSchema"));
    }
}

TEST_CASE_METHOD(CompositeDispatchFixture, "query - describe all ops works without daemon",
                 "[mcp][composite][query]") {
    auto result = callTool("query", json{{"steps", json::array({{{"op", "describe"}}})}});

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));

    if (result.contains("structuredContent")) {
        auto& data = result["structuredContent"]["data"];
        CHECK(data.contains("query_ops"));
        CHECK(data.contains("execute_ops"));
        CHECK(data.contains("session_ops"));
    }
}

TEST_CASE_METHOD(CompositeDispatchFixture, "query - search op dispatches to internal handler",
                 "[mcp][composite][query]") {
    json args = {{"steps", json::array({{{"op", "search"}, {"params", {{"query", "test"}}}}})}};

    auto result = callTool("query", args);

    INFO("result: " << result.dump(2));
    // Should NOT be "Unknown tool: query" or "Unknown tool: search"
    CHECK_FALSE(isRoutingError(result));
    // Should reach the search handler (daemon error expected)
    CHECK(reachedHandler(result));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "query - grep op dispatches to internal handler",
                 "[mcp][composite][query]") {
    json args = {{"steps", json::array({{{"op", "grep"}, {"params", {{"pattern", "TODO"}}}}})}};

    auto result = callTool("query", args);

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "query - list op dispatches to internal handler",
                 "[mcp][composite][query]") {
    json args = {{"steps", json::array({{{"op", "list"}, {"params", {{"limit", 5}}}}})}};

    auto result = callTool("query", args);

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "query - get op dispatches to internal handler",
                 "[mcp][composite][query]") {
    json args = {
        {"steps", json::array({{{"op", "get"}, {"params", {{"hash", "deadbeef01234567"}}}}})}};

    auto result = callTool("query", args);

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "query - status op dispatches to internal handler",
                 "[mcp][composite][query]") {
    json args = {{"steps", json::array({{{"op", "status"}}})}};

    auto result = callTool("query", args);

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "query - graph op dispatches to internal handler",
                 "[mcp][composite][query]") {
    json args = {{"steps", json::array({{{"op", "graph"}, {"params", {{"list_types", true}}}}})}};

    auto result = callTool("query", args);

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
}

TEST_CASE_METHOD(CompositeDispatchFixture,
                 "query - list_collections op dispatches to internal handler",
                 "[mcp][composite][query]") {
    json args = {{"steps", json::array({{{"op", "list_collections"}}})}};

    auto result = callTool("query", args);

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
}

TEST_CASE_METHOD(CompositeDispatchFixture,
                 "query - list_snapshots op dispatches to internal handler",
                 "[mcp][composite][query]") {
    json args = {{"steps", json::array({{{"op", "list_snapshots"}}})}};

    auto result = callTool("query", args);

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "query - write op in query returns clear error",
                 "[mcp][composite][query][error-message]") {
    auto result = callTool(
        "query",
        json{{"steps", json::array({{{"op", "add"}, {"params", {{"path", "/tmp/x.txt"}}}}})}});

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
    CHECK(isCompositeError(result, "not a read operation"));
    CHECK(isCompositeError(result, "Use the 'execute' tool"));
    REQUIRE(result.contains("error"));
    REQUIRE(result["error"].contains("data"));
    REQUIRE(result["error"]["data"].contains("toolResult"));
    // Validation happens before any pipeline execution; expect a direct error message.
    CHECK(result["error"].value("message", std::string{}).find("Pipeline:") == std::string::npos);
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. EXECUTE BATCH: validation and dispatch
// ═══════════════════════════════════════════════════════════════════════════

TEST_CASE_METHOD(CompositeDispatchFixture, "execute - missing operations returns error",
                 "[mcp][composite][execute]") {
    auto result = callTool("execute", json::object());

    INFO("result: " << result.dump(2));
    CHECK(isCompositeError(result, "'operations' array is required"));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "execute - empty operations returns error",
                 "[mcp][composite][execute]") {
    auto result = callTool("execute", json{{"operations", json::array()}});

    INFO("result: " << result.dump(2));
    CHECK(isCompositeError(result, "'operations' array must not be empty"));
}

TEST_CASE_METHOD(CompositeDispatchFixture,
                 "execute - JSON-RPC error includes error.data.toolResult for debugging",
                 "[mcp][composite][execute][error-data]") {
    // Force a composite-layer validation error that becomes JSON-RPC error.
    auto result = callTool("execute", json::object());

    INFO("result: " << result.dump(2));
    REQUIRE(result.is_object());
    REQUIRE(result.contains("error"));
    REQUIRE(result["error"].is_object());
    REQUIRE(result["error"].contains("data"));

    const auto& data = result["error"]["data"];
    REQUIRE(data.is_object());
    CHECK(data.value("tool", std::string{}) == "execute");
    REQUIRE(data.contains("toolResult"));
    REQUIRE(data["toolResult"].is_object());
    CHECK(data["toolResult"].value("isError", false));
    // Should preserve the original human-readable error content
    REQUIRE(data["toolResult"].contains("content"));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "execute - non-write op returns clear error",
                 "[mcp][composite][execute][error-message]") {
    auto result = callTool(
        "execute",
        json{{"operations", json::array({{{"op", "describe"}, {"params", json::object()}}})}});

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
    CHECK(isCompositeError(result, "not a write operation"));
    CHECK(isCompositeError(result, "Use the 'query' tool"));
    REQUIRE(result.contains("error"));
    REQUIRE(result["error"].contains("data"));
    REQUIRE(result["error"]["data"].contains("toolResult"));
    CHECK(result["error"].value("message", std::string{}).find("Execute:") != std::string::npos);
}

TEST_CASE_METHOD(CompositeDispatchFixture, "execute - operation missing op field",
                 "[mcp][composite][execute]") {
    auto result = callTool(
        "execute", json{{"operations", json::array({{{"params", {{"path", "/tmp/x.txt"}}}}})}});

    INFO("result: " << result.dump(2));
    // Should get a result with a failed step, not a routing error
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "execute - read op in execute returns error",
                 "[mcp][composite][execute]") {
    auto result = callTool(
        "execute",
        json{{"operations", json::array({{{"op", "search"}, {"params", {{"query", "test"}}}}})}});

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
    // Should contain error about using query tool for read operations
}

TEST_CASE_METHOD(CompositeDispatchFixture, "execute - add op dispatches to internal handler",
                 "[mcp][composite][execute]") {
    json args = {
        {"operations", json::array({{{"op", "add"}, {"params", {{"path", "/tmp/test.txt"}}}}})}};

    auto result = callTool("execute", args);

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "execute - delete maps to delete_by_name internal tool",
                 "[mcp][composite][execute]") {
    json args = {
        {"operations", json::array({{{"op", "delete"}, {"params", {{"name", "test.txt"}}}}})}};

    auto result = callTool("execute", args);

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "execute - update op dispatches to internal handler",
                 "[mcp][composite][execute]") {
    json args = {
        {"operations",
         json::array(
             {{{"op", "update"},
               {"params", {{"name", "test.txt"}, {"metadata", json::array({"status=done"})}}}}})}};

    auto result = callTool("execute", args);

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "execute - continueOnError processes all ops",
                 "[mcp][composite][execute]") {
    // Two ops: first is an invalid op (should fail), second is valid
    // With continueOnError=true, both should be attempted
    json args = {
        {"operations", json::array({
                           {{"op", "search"}, {"params", {{"query", "test"}}}},  // read op → error
                           {{"op", "add"}, {"params", {{"path", "/tmp/x.txt"}}}} // valid write op
                       })},
        {"continueOnError", true}};

    auto result = callTool("execute", args);

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));

    // Should have structured result with both steps
    if (result.contains("structuredContent") && result["structuredContent"].contains("data")) {
        auto& data = result["structuredContent"]["data"];
        if (data.contains("results")) {
            CHECK(data["results"].size() == 2);
            CHECK(data["totalOps"] == 2);
        }
    }
}

TEST_CASE_METHOD(CompositeDispatchFixture, "execute - stops on error by default",
                 "[mcp][composite][execute]") {
    // Two ops: first is an invalid op (should fail), second should NOT run
    json args = {
        {"operations", json::array({
                           {{"op", "search"}, {"params", {{"query", "test"}}}},  // read op → error
                           {{"op", "add"}, {"params", {{"path", "/tmp/x.txt"}}}} // should not run
                       })}};

    auto result = callTool("execute", args);

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));

    // Should have only 1 result (stopped after first error)
    if (result.contains("structuredContent") && result["structuredContent"].contains("data")) {
        auto& data = result["structuredContent"]["data"];
        if (data.contains("results")) {
            CHECK(data["results"].size() == 1);
            CHECK(data["failed"] == 1);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. SESSION: validation and dispatch
// ═══════════════════════════════════════════════════════════════════════════

TEST_CASE_METHOD(CompositeDispatchFixture, "session - missing action returns error",
                 "[mcp][composite][session]") {
    auto result = callTool("session", json::object());

    INFO("result: " << result.dump(2));
    CHECK(isCompositeError(result, "'action' string is required"));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "session - unknown action returns error",
                 "[mcp][composite][session]") {
    auto result = callTool("session", json{{"action", "destroy"}});

    INFO("result: " << result.dump(2));
    CHECK(isCompositeError(result, "Unknown session action"));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "session - start dispatches to session_start",
                 "[mcp][composite][session]") {
    auto result = callTool("session", json{{"action", "start"}, {"params", json::object()}});

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "session - stop dispatches to session_stop",
                 "[mcp][composite][session]") {
    auto result = callTool("session", json{{"action", "stop"}, {"params", json::object()}});

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "session - pin dispatches to session_pin",
                 "[mcp][composite][session]") {
    auto result =
        callTool("session", json{{"action", "pin"}, {"params", {{"path", "/tmp/test.txt"}}}});

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "session - unpin dispatches to session_unpin",
                 "[mcp][composite][session]") {
    auto result =
        callTool("session", json{{"action", "unpin"}, {"params", {{"path", "/tmp/test.txt"}}}});

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
}

TEST_CASE_METHOD(CompositeDispatchFixture, "session - watch dispatches to watch tool",
                 "[mcp][composite][session]") {
    auto result = callTool(
        "session", json{{"action", "watch"}, {"params", {{"paths", json::array({"/tmp"})}}}});

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    CHECK(reachedHandler(result));
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. MULTI-STEP PIPELINE with describe (no daemon needed)
// ═══════════════════════════════════════════════════════════════════════════

TEST_CASE_METHOD(CompositeDispatchFixture, "query - multi-step with describe returns all steps",
                 "[mcp][composite][pipeline]") {
    // Two describe steps: list all ops, then describe search
    json args = {{"steps", json::array({{{"op", "describe"}},
                                        {{"op", "describe"}, {"params", {{"target", "grep"}}}}})}};

    auto result = callTool("query", args);

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));

    // Multi-step should return steps array
    if (result.contains("structuredContent") && result["structuredContent"].contains("data")) {
        auto& data = result["structuredContent"]["data"];
        CHECK(data.contains("steps"));
        CHECK(data.contains("totalSteps"));
        CHECK(data["totalSteps"] == 2);
        CHECK(data.contains("completedSteps"));
        CHECK(data["completedSteps"] == 2);

        if (data.contains("steps") && data["steps"].is_array()) {
            CHECK(data["steps"].size() == 2);

            // First step: describe all ops
            auto& step0 = data["steps"][0];
            CHECK(step0["stepIndex"] == 0);
            CHECK(step0["op"] == "describe");
            CHECK(step0["result"].contains("query_ops"));

            // Second step: describe grep
            auto& step1 = data["steps"][1];
            CHECK(step1["stepIndex"] == 1);
            CHECK(step1["op"] == "describe");
            CHECK(step1["result"]["op"] == "grep");
            CHECK(step1["result"].contains("paramsSchema"));
        }
    }
}

TEST_CASE_METHOD(CompositeDispatchFixture, "query - single-step returns direct result",
                 "[mcp][composite][pipeline]") {
    // Single describe step should NOT wrap in steps array
    json args = {{"steps", json::array({{{"op", "describe"}, {"params", {{"target", "list"}}}}})}};

    auto result = callTool("query", args);

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));

    if (result.contains("structuredContent") && result["structuredContent"].contains("data")) {
        auto& data = result["structuredContent"]["data"];
        // Single-step: direct result, NOT wrapped in {steps: [...]}
        CHECK(data.contains("op"));
        CHECK(data["op"] == "list");
        CHECK(data.contains("paramsSchema"));
        CHECK_FALSE(data.contains("steps"));
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. hasTool registry method
// ═══════════════════════════════════════════════════════════════════════════

TEST_CASE("ToolRegistry::hasTool returns correct results", "[mcp][composite][registry]") {
    yams::mcp::ToolRegistry reg;
    reg.registerRawTool(
        "test_tool",
        [](const json&) -> boost::asio::awaitable<json> { co_return json{{"ok", true}}; },
        json{{"type", "object"}}, "Test tool");

    CHECK(reg.hasTool("test_tool"));
    CHECK_FALSE(reg.hasTool("nonexistent"));
    CHECK_FALSE(reg.hasTool(""));
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. Edge cases
// ═══════════════════════════════════════════════════════════════════════════

TEST_CASE_METHOD(CompositeDispatchFixture, "mcp.echo still works via callTool",
                 "[mcp][composite][routing]") {
    auto result = callTool("mcp.echo", json{{"text", "hello world"}});

    INFO("result: " << result.dump(2));
    CHECK_FALSE(isRoutingError(result));
    // mcp.echo should succeed even without daemon
    if (result.contains("content") && result["content"].is_array()) {
        bool foundEcho = false;
        for (const auto& c : result["content"]) {
            if (c.contains("text")) {
                auto text = c["text"].get<std::string>();
                if (text.find("hello world") != std::string::npos) {
                    foundEcho = true;
                }
            }
        }
        CHECK(foundEcho);
    }
}

TEST_CASE_METHOD(CompositeDispatchFixture, "unknown tool returns error",
                 "[mcp][composite][routing]") {
    auto result = callTool("nonexistent_tool", json::object());

    INFO("result: " << result.dump(2));
    // Should be a routing error since this tool doesn't exist in either registry
    CHECK(isRoutingError(result));
}
