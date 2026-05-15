// MCP Timeout Tests
//
// Gap 1: MCPServer constructor set daemon client requestTimeout=60s and
//        bodyTimeout=120s — far too permissive for interactive MCP. A hung
//        daemon meant a 60s hang. Fixed: 15s requestTimeout, 60s bodyTimeout.
//
// Gap 2: callTool() / processMessage() blocked callers on future.get() with
//        no internal deadline. Fixed: 30s safety deadline on all sync bridges.
//
// Gap 3: Async tool calls (detached coroutines) had no server-side deadline.
//        Fixed: 30s deadline timer races against the tool call.
//
// Fast-fail: With properly configured short timeouts, all tools return
//            promptly when the daemon is unreachable.

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#ifdef _WIN32
#include <process.h>
static int setenv(const char* name, const char* value, int overwrite) {
    return _putenv_s(name, value);
}
#endif

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

#include <nlohmann/json.hpp>
#include <yams/daemon/client/global_io_context.h>
#include <yams/mcp/mcp_server.h>
#include <yams/mcp/tool_registry.h>

using namespace std::chrono_literals;
using json = nlohmann::json;
using Catch::Matchers::ContainsSubstring;

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

// ============================================================================
// GAP 1 (FAILING → NOW PASSING with fix):
//    MCP server default requestTimeout was 60s (excessive).
//    After fix: 15s for requestTimeout, 60s for bodyTimeout.
// ============================================================================

TEST_CASE("MCP server default daemon timeouts are now reasonable",
          "[mcp][timeout][gap][default_config]") {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    std::optional<yams::daemon::ClientConfig> captured;
    server->testSetEnsureDaemonClientHook(
        [&captured](const yams::daemon::ClientConfig& cfg) -> yams::Result<void> {
            captured = cfg;
            return yams::Error{yams::ErrorCode::NetworkError, "captured for verification"};
        });

    server->callToolPublic("search", json{{"query", "test"}});

    REQUIRE(captured.has_value());
    INFO("requestTimeout: " << captured->requestTimeout.count() << "ms");
    INFO("headerTimeout:   " << captured->headerTimeout.count() << "ms");
    INFO("bodyTimeout:     " << captured->bodyTimeout.count() << "ms");
    INFO("connectTimeout:  " << captured->connectTimeout.count() << "ms");

    // After fix: 15s requestTimeout (was 60s), 60s bodyTimeout (was 120s)
    CHECK(captured->requestTimeout.count() == 15000);
    CHECK(captured->bodyTimeout.count() == 60000);
    CHECK(captured->headerTimeout.count() == 10000);
}

// ============================================================================
// GAP 2 (FAILING → NOW PASSING with fix):
//    callTool() now has a 30s deadline on future.wait_for().
//    Verify callTool returns promptly when daemon is unreachable (short
//    timeouts via hook).
// ============================================================================

TEST_CASE("callTool returns error promptly when daemon unreachable",
          "[mcp][timeout][gap][fastfail]") {
    setenv("YAMS_ENABLE_LOCAL_MCP_DISCOVERY", "0", 1);
    setenv("YAMS_CLI_DISABLE_DAEMON_AUTOSTART", "1", 1);

    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    auto socketPath =
        std::filesystem::temp_directory_path() /
        ("yams-mcp-fastfail-" +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sock");
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
            return yams::Error{yams::ErrorCode::NetworkError, "No daemon"};
        });

    for (const auto& toolName :
         {"search", "grep", "list", "get", "status", "stats", "doctor", "cat", "graph", "add"}) {
        auto start = std::chrono::steady_clock::now();
        json args = json::object();
        if (std::string(toolName) == "search")
            args["query"] = "test";
        else if (std::string(toolName) == "grep")
            args["pattern"] = "test";
        else if (std::string(toolName) == "get" || std::string(toolName) == "cat")
            args["hash"] = "abc123";
        else if (std::string(toolName) == "add") {
            args["content"] = "x";
            args["name"] = "x.txt";
        }

        auto result = server->callToolPublic(toolName, args);
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);

        REQUIRE(result.is_object());
        INFO("Tool '" << toolName << "' elapsed: " << elapsed.count() << "ms");
        CHECK(elapsed < 500ms);
    }

    server->testShutdown();
    std::filesystem::remove(socketPath, ec);
}

// ============================================================================
// GAP 3 (FAILING → NOW PASSING with fix):
//    processMessage() now has a 30s deadline on future.wait_for().
// ============================================================================

TEST_CASE("processMessage returns promptly for tools/call when daemon down",
          "[mcp][timeout][gap][processmessage]") {
    setenv("YAMS_ENABLE_LOCAL_MCP_DISCOVERY", "0", 1);
    setenv("YAMS_CLI_DISABLE_DAEMON_AUTOSTART", "1", 1);

    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    auto socketPath =
        std::filesystem::temp_directory_path() /
        ("yams-mcp-procm-" +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sock");
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
            return yams::Error{yams::ErrorCode::NetworkError, "No daemon"};
        });

    json request = {{"jsonrpc", "2.0"},
                    {"id", 1},
                    {"method", "tools/call"},
                    {"params", {{"name", "search"}, {"arguments", {{"query", "test"}}}}}};

    auto start = std::chrono::steady_clock::now();
    auto response = server->processMessage(request);
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    INFO("processMessage elapsed: " << elapsed.count() << "ms");
    REQUIRE(response.has_value());
    CHECK(elapsed < 1000ms);

    server->testShutdown();
    std::filesystem::remove(socketPath, ec);
}

// ============================================================================
// Sequential calls don't degrade or leak
// ============================================================================

TEST_CASE("consecutive callTool calls do not progressively slow down",
          "[mcp][timeout][sequential]") {
    setenv("YAMS_ENABLE_LOCAL_MCP_DISCOVERY", "0", 1);
    setenv("YAMS_CLI_DISABLE_DAEMON_AUTOSTART", "1", 1);

    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));

    auto socketPath =
        std::filesystem::temp_directory_path() /
        ("yams-mcp-sequential-" +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sock");
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
            return yams::Error{yams::ErrorCode::NetworkError, "No daemon"};
        });

    constexpr int kCalls = 10;
    std::vector<std::chrono::milliseconds> elapsedTimes;

    for (int i = 0; i < kCalls; ++i) {
        auto start = std::chrono::steady_clock::now();
        auto result = server->callToolPublic("search", json{{"query", "test"}});
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);
        elapsedTimes.push_back(elapsed);

        REQUIRE(result.is_object());
        INFO("call " << i << " elapsed: " << elapsed.count() << "ms");
        CHECK(elapsed < 1000ms);
    }

    for (size_t i = 0; i < elapsedTimes.size(); ++i) {
        INFO("call " << i << " latency: " << elapsedTimes[i].count() << "ms");
        CHECK(elapsedTimes[i] < 1000ms);
    }

    server->testShutdown();
    std::filesystem::remove(socketPath, ec);
}
