// MCP Server async execution tests
// Catch2 migration from GTest (yams-3s4 / yams-84g)

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
#include <system_error>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

#include <yams/daemon/client/global_io_context.h>
#include <yams/mcp/mcp_server.h>

using namespace std::chrono_literals;
using Catch::Matchers::ContainsSubstring;

namespace {

// Minimal no-op transport for constructing MCPServer in tests without starting loops.
class NullTransport : public yams::mcp::ITransport {
public:
    void send(const nlohmann::json&) override {}
    yams::mcp::MessageResult receive() override {
        return yams::Error{yams::ErrorCode::NotImplemented, "Null transport"};
    }
    bool isConnected() const override { return false; }
    void close() override {}
    yams::mcp::TransportState getState() const override {
        return yams::mcp::TransportState::Disconnected;
    }
};

template <typename Awaitable>
auto run_awaitable_with_timeout(Awaitable aw, std::chrono::milliseconds timeout)
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

class MCPAsyncExecFixture {
public:
    std::unique_ptr<yams::mcp::MCPServer> server;
    std::filesystem::path socketPath;

    MCPAsyncExecFixture() {
        setenv("YAMS_ENABLE_LOCAL_MCP_DISCOVERY", "0", 1);
        setenv("YAMS_CLI_DISABLE_DAEMON_AUTOSTART", "1", 1);

        auto transport = std::make_unique<NullTransport>();
        server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

        const auto uniqueName =
            std::string("yams-mcp-test-") +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sock";
        socketPath = std::filesystem::temp_directory_path() / uniqueName;
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

    ~MCPAsyncExecFixture() {
        if (server) {
            server->testShutdown();
        }
    }

    template <typename Awaitable>
    void expectAsyncCompletion(Awaitable makeAwaitable, const std::string& toolName) {
        auto start = std::chrono::steady_clock::now();
        auto result = run_awaitable_with_timeout(makeAwaitable(), std::chrono::seconds(3));
        auto elapsed = std::chrono::steady_clock::now() - start;

        REQUIRE_MESSAGE(result.has_value(), toolName << " handler timed out");
        auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed);
        CHECK_MESSAGE(elapsedMs < 500ms,
                      toolName << " harness took too long: " << elapsedMs.count() << "ms");

        if (result->has_value()) {
            SUCCEED(toolName << " handler completed successfully");
        } else {
            const auto& error = result->error();
            CHECK_MESSAGE(!error.message.empty(), toolName << " error message missing");
            CHECK_MESSAGE(error.message.find("test harness") != std::string::npos,
                          toolName << " unexpected error message: " << error.message);
        }
    }
};

} // namespace

// Helper macro for better test output
#define REQUIRE_MESSAGE(cond, msg)                                                                 \
    do {                                                                                           \
        INFO(msg);                                                                                 \
        REQUIRE(cond);                                                                             \
    } while (0)
#define CHECK_MESSAGE(cond, msg)                                                                   \
    do {                                                                                           \
        INFO(msg);                                                                                 \
        CHECK(cond);                                                                               \
    } while (0)

// Ensure async tool handlers return robustly (error or value), not crash/hang,
// when daemon is unavailable in test environment.
TEST_CASE_METHOD(MCPAsyncExecFixture, "MCP async tools return structured errors without daemon",
                 "[mcp][async][exec][catch2]") {
    SECTION("search tool returns error") {
        expectAsyncCompletion(
            [&]() -> boost::asio::awaitable<yams::Result<yams::mcp::MCPSearchResponse>> {
                yams::mcp::MCPSearchRequest req;
                req.query = "foo";
                co_return co_await server->testHandleSearchDocuments(req);
            },
            "search");
    }

    SECTION("grep tool returns error") {
        expectAsyncCompletion(
            [&]() -> boost::asio::awaitable<yams::Result<yams::mcp::MCPGrepResponse>> {
                yams::mcp::MCPGrepRequest req;
                req.pattern = "bar";
                co_return co_await server->testHandleGrepDocuments(req);
            },
            "grep");
    }

    SECTION("retrieve tool returns error") {
        expectAsyncCompletion(
            [&]() -> boost::asio::awaitable<yams::Result<yams::mcp::MCPRetrieveDocumentResponse>> {
                yams::mcp::MCPRetrieveDocumentRequest req;
                req.hash = "deadbeef";
                co_return co_await server->testHandleRetrieveDocument(req);
            },
            "retrieve");
    }

    SECTION("list tool returns error") {
        expectAsyncCompletion(
            [&]() -> boost::asio::awaitable<yams::Result<yams::mcp::MCPListDocumentsResponse>> {
                yams::mcp::MCPListDocumentsRequest req;
                co_return co_await server->testHandleListDocuments(req);
            },
            "list");
    }

    SECTION("stats tool returns error") {
        expectAsyncCompletion(
            [&]() -> boost::asio::awaitable<yams::Result<yams::mcp::MCPStatsResponse>> {
                yams::mcp::MCPStatsRequest req;
                co_return co_await server->testHandleGetStats(req);
            },
            "stats");
    }

    SECTION("callToolAsync returns structured error") {
        auto syncResult = run_awaitable_with_timeout(
            server->testCallToolAsync("search", nlohmann::json{{"query", "foo"}}),
            std::chrono::seconds(3));

        REQUIRE(syncResult.has_value());
        REQUIRE(syncResult->is_object());

        if (syncResult->contains("error")) {
            const auto& error = (*syncResult)["error"];
            REQUIRE(error.contains("message"));
            CHECK_THAT(error["message"].get<std::string>(), ContainsSubstring("test harness"));
        } else {
            INFO("callTool result: " << syncResult->dump());
            REQUIRE(syncResult->contains("isError"));
            REQUIRE((*syncResult)["isError"].is_boolean());
            CHECK((*syncResult)["isError"].get<bool>());

            REQUIRE(syncResult->contains("content"));
            const auto& content = (*syncResult)["content"];
            REQUIRE(content.is_array());
            REQUIRE_FALSE(content.empty());

            const auto& first = content.front();
            REQUIRE(first.contains("text"));
            CHECK_THAT(first["text"].get<std::string>(), ContainsSubstring("test harness"));
        }
    }
}
