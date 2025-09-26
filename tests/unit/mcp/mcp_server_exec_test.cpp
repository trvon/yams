#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <system_error>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

#include <yams/daemon/client/global_io_context.h>
#include <yams/mcp/mcp_server.h>

using namespace std::chrono_literals;

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

} // namespace

// Ensure async tool handlers return robustly (error or value), not crash/hang, when daemon is
// unavailable in test environment.
TEST(MCPAsyncExecTest, ToolsReturnStructuredErrorsWithoutDaemon) {
    setenv("YAMS_ENABLE_LOCAL_MCP_DISCOVERY", "0", 1);
    setenv("YAMS_CLI_DISABLE_DAEMON_AUTOSTART", "1", 1);

    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    const auto uniqueName =
        std::string("yams-mcp-test-") +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sock";
    const auto socketPath = std::filesystem::temp_directory_path() / uniqueName;
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

    auto expectAsyncCompletion = [&](auto makeAwaitable, const std::string& toolName) {
        auto start = std::chrono::steady_clock::now();
        auto result = run_awaitable_with_timeout(makeAwaitable(), std::chrono::seconds(3));
        auto elapsed = std::chrono::steady_clock::now() - start;
        ASSERT_TRUE(result.has_value()) << toolName << " handler timed out";
        EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed), 500ms)
            << toolName << " harness took too long";
        if (result->has_value()) {
            SUCCEED() << toolName << " handler completed successfully";
        } else {
            const auto& error = result->error();
            EXPECT_FALSE(error.message.empty()) << toolName << " error message missing";
            EXPECT_THAT(error.message, ::testing::HasSubstr("test harness"))
                << toolName << " unexpected error message";
        }
    };

    expectAsyncCompletion(
        [&]() -> boost::asio::awaitable<yams::Result<yams::mcp::MCPSearchResponse>> {
            yams::mcp::MCPSearchRequest req;
            req.query = "foo";
            co_return co_await server->testHandleSearchDocuments(req);
        },
        "search");

    expectAsyncCompletion(
        [&]() -> boost::asio::awaitable<yams::Result<yams::mcp::MCPGrepResponse>> {
            yams::mcp::MCPGrepRequest req;
            req.pattern = "bar";
            co_return co_await server->testHandleGrepDocuments(req);
        },
        "grep");

    expectAsyncCompletion(
        [&]() -> boost::asio::awaitable<yams::Result<yams::mcp::MCPRetrieveDocumentResponse>> {
            yams::mcp::MCPRetrieveDocumentRequest req;
            req.hash = "deadbeef";
            co_return co_await server->testHandleRetrieveDocument(req);
        },
        "retrieve");

    expectAsyncCompletion(
        [&]() -> boost::asio::awaitable<yams::Result<yams::mcp::MCPListDocumentsResponse>> {
            yams::mcp::MCPListDocumentsRequest req;
            co_return co_await server->testHandleListDocuments(req);
        },
        "list");

    expectAsyncCompletion(
        [&]() -> boost::asio::awaitable<yams::Result<yams::mcp::MCPStatsResponse>> {
            yams::mcp::MCPStatsRequest req;
            co_return co_await server->testHandleGetStats(req);
        },
        "stats");

    auto syncResult = run_awaitable_with_timeout(
        server->testCallToolAsync("search", nlohmann::json{{"query", "foo"}}),
        std::chrono::seconds(3));
    ASSERT_TRUE(syncResult.has_value()) << "callTool async timed out";
    ASSERT_TRUE(syncResult->is_object());
    if (syncResult->contains("error")) {
        const auto& error = (*syncResult)["error"];
        ASSERT_TRUE(error.contains("message"));
        EXPECT_THAT(error["message"].get<std::string>(), ::testing::HasSubstr("test harness"));
    } else {
        ASSERT_TRUE(syncResult->contains("isError")) << "callTool result: " << syncResult->dump();
        ASSERT_TRUE((*syncResult)["isError"].is_boolean())
            << "callTool result: " << syncResult->dump();
        EXPECT_TRUE((*syncResult)["isError"].get<bool>())
            << "callTool result: " << syncResult->dump();
        ASSERT_TRUE(syncResult->contains("content")) << "callTool result: " << syncResult->dump();
        const auto& content = (*syncResult)["content"];
        ASSERT_TRUE(content.is_array()) << "callTool result: " << syncResult->dump();
        ASSERT_FALSE(content.empty()) << "callTool result: " << syncResult->dump();
        const auto& first = content.front();
        ASSERT_TRUE(first.contains("text")) << "callTool result: " << syncResult->dump();
        EXPECT_THAT(first["text"].get<std::string>(), ::testing::HasSubstr("test harness"));
    }

    server->testShutdown();
}
