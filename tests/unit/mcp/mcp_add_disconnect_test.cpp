#include <gtest/gtest.h>

#include <string>

#include <yams/mcp/mcp_server.h>

using nlohmann::json;
using yams::mcp::ITransport;
using yams::mcp::MCPServer;

namespace {
class NullTransport : public ITransport {
public:
    yams::mcp::MessageResult receive() override {
        return yams::Error{yams::ErrorCode::NetworkError, "closed"};
    }
    void send(const json&) override {}
    yams::mcp::TransportState getState() const override {
        return yams::mcp::TransportState::Connected;
    }
    bool isConnected() const override { return true; }
    void close() override {}
};
} // namespace

TEST(MCPAddDisconnectTest, ErrorContainsSocketAndHint) {
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));
#if !defined(_WIN32)
    ::setenv("XDG_RUNTIME_DIR", "/run/user/1002", 1);
#endif
    svr.setEnsureDaemonClientHook([&](const yams::daemon::ClientConfig&) -> yams::Result<void> {
        return yams::Error{yams::ErrorCode::NetworkError, "dial error"};
    });
    json args = {
        {"path", "/does/not/exist.txt"},
    };
    auto res = svr.callToolPublic("add", args);
    ASSERT_TRUE(res.contains("error"));
    auto msg = res["error"].value("message", std::string{});
    EXPECT_NE(msg.find("yams-daemon.sock"), std::string::npos);
    EXPECT_NE(msg.find("YAMS_DAEMON_SOCKET"), std::string::npos);
}
