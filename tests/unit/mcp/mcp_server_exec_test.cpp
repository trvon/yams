#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <string>

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
TEST(MCPAsyncExecTest, HandlersReturnErrorsWithoutDaemon) {
    auto transport = std::make_unique<NullTransport>();
    auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport));

    // Search
    yams::mcp::MCPSearchRequest sreq;
    sreq.query = "foo";
    auto sres = run_awaitable_with_timeout(server->testHandleSearchDocuments(sreq), 3s);
    ASSERT_TRUE(sres.has_value()) << "Search handler timed out";
    EXPECT_FALSE(static_cast<bool>(sres.value())) << "Expected error without daemon";

    // Grep
    yams::mcp::MCPGrepRequest grep;
    grep.pattern = "bar";
    auto gres = run_awaitable_with_timeout(server->testHandleGrepDocuments(grep), 3s);
    ASSERT_TRUE(gres.has_value()) << "Grep handler timed out";
    EXPECT_FALSE(static_cast<bool>(gres.value())) << "Expected error without daemon";

    // Retrieve by non-existent name
    yams::mcp::MCPRetrieveDocumentRequest rreq;
    rreq.name = "does-not-exist.txt";
    auto rres = run_awaitable_with_timeout(server->testHandleRetrieveDocument(rreq), 3s);
    ASSERT_TRUE(rres.has_value()) << "Retrieve handler timed out";
    EXPECT_FALSE(static_cast<bool>(rres.value())) << "Expected error without daemon";

    // List documents (repo not initialized)
    yams::mcp::MCPListDocumentsRequest lreq;
    auto lres = run_awaitable_with_timeout(server->testHandleListDocuments(lreq), 3s);
    ASSERT_TRUE(lres.has_value()) << "ListDocuments handler timed out";
    EXPECT_FALSE(static_cast<bool>(lres.value())) << "Expected error without daemon";

    // Stats
    yams::mcp::MCPStatsRequest streq;
    auto stres = run_awaitable_with_timeout(server->testHandleGetStats(streq), 3s);
    ASSERT_TRUE(stres.has_value()) << "GetStats handler timed out";
    EXPECT_FALSE(static_cast<bool>(stres.value())) << "Expected error without daemon";
}
