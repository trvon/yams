#include <gtest/gtest.h>

#include <cstdlib>
#include <string>

#include <yams/mcp/mcp_server.h>

using yams::mcp::ITransport;
using yams::mcp::MCPServer;
using yams::mcp::MessageResult;

namespace {
class NullTransport : public ITransport {
public:
    MessageResult receive() override {
        return yams::Error{yams::ErrorCode::NetworkError, "closed"};
    }
    void send(const nlohmann::json&) override {}
    yams::mcp::TransportState getState() const override {
        return yams::mcp::TransportState::Connected;
    }
    bool isConnected() const override { return true; }
    void close() override {}
};

void setenv_strict(const char* k, const char* v) {
#if defined(_WIN32)
    _putenv_s(k, v);
#else
    if (v)
        ::setenv(k, v, 1);
    else
        ::unsetenv(k);
#endif
}
} // namespace

TEST(MCPDaemonSocketResolutionTest, HonorsEnvSocketOverride) {
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));
    std::string captured;
    setenv_strict("YAMS_DAEMON_SOCKET", "/tmp/yams-alt.sock");
    svr.setEnsureDaemonClientHook([&](const yams::daemon::ClientConfig& cfg) -> yams::Result<void> {
        captured = cfg.socketPath.string();
        return yams::Result<void>();
    });
    // Trigger a path that calls ensureDaemonClient
    (void)svr.callToolPublic("yams.stats", nlohmann::json::object());
    EXPECT_FALSE(captured.empty());
    EXPECT_NE(captured.find("/tmp/yams-alt.sock"), std::string::npos);
    setenv_strict("YAMS_DAEMON_SOCKET", nullptr);
}

TEST(MCPDaemonSocketResolutionTest, StructuredErrorIncludesSocketAndHint) {
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));
    // Force a known socket path via env
    setenv_strict("YAMS_DAEMON_SOCKET", "/run/user/1000/yams-daemon.sock");
    // Force ensureDaemonClient to fail with NetworkError
    svr.setEnsureDaemonClientHook([&](const yams::daemon::ClientConfig& cfg) -> yams::Result<void> {
        (void)cfg;
        return yams::Error{yams::ErrorCode::NetworkError, "dial error"};
    });
    auto res = svr.callToolPublic("yams.stats", nlohmann::json::object());
    ASSERT_TRUE(res.contains("error"));
    auto msg = res["error"].value("message", std::string{});
    EXPECT_NE(msg.find("/run/user/1000/yams-daemon.sock"), std::string::npos);
    EXPECT_NE(msg.find("YAMS_DAEMON_SOCKET"), std::string::npos);
    setenv_strict("YAMS_DAEMON_SOCKET", nullptr);
}

TEST(MCPDoctorPathTest, DoctorUsesSameResolvedSocket) {
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));
    std::string captured;
    setenv_strict("XDG_RUNTIME_DIR", "/run/user/1001");
    svr.setEnsureDaemonClientHook([&](const yams::daemon::ClientConfig& cfg) -> yams::Result<void> {
        captured = cfg.socketPath.string();
        return yams::Error{yams::ErrorCode::NetworkError, "dial error"};
    });
    auto res = svr.callToolPublic("yams.doctor", nlohmann::json::object());
    ASSERT_TRUE(!captured.empty());
    // Even when doctor fails upstream, structured content should include socketPath in details
    if (res.contains("error")) {
        SUCCEED();
    } else {
        ASSERT_TRUE(res.contains("content"));
        auto content = res["content"]; // MCP tool result wrapper
        bool sawSocket = false;
        if (content.is_array() && !content.empty()) {
            for (const auto& part : content) {
                if (part.contains("text")) {
                    auto text = part.value("text", std::string{});
                    if (!text.empty() && text.find(captured) != std::string::npos) {
                        sawSocket = true;
                        break;
                    }
                }
            }
        }
        EXPECT_TRUE(sawSocket);
    }
    setenv_strict("XDG_RUNTIME_DIR", nullptr);
}

TEST(MCPDaemonSocketResolutionTest, UsesXdgRuntimeDirWhenAvailable) {
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));
    std::string captured;
    setenv_strict("XDG_RUNTIME_DIR", "/run/user/1000");
    svr.setEnsureDaemonClientHook([&](const yams::daemon::ClientConfig& cfg) -> yams::Result<void> {
        captured = cfg.socketPath.string();
        return yams::Result<void>();
    });
    (void)svr.callToolPublic("yams.stats", nlohmann::json::object());
    EXPECT_FALSE(captured.empty());
    EXPECT_NE(captured.find("/run/user/1000"), std::string::npos);
    EXPECT_NE(captured.rfind("yams-daemon.sock"), std::string::npos);
    setenv_strict("XDG_RUNTIME_DIR", nullptr);
}

TEST(MCPDaemonSocketResolutionTest, FallsBackToTmpWhenNoXdg) {
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));
    std::string captured;
    setenv_strict("XDG_RUNTIME_DIR", nullptr);
    svr.setEnsureDaemonClientHook([&](const yams::daemon::ClientConfig& cfg) -> yams::Result<void> {
        captured = cfg.socketPath.string();
        return yams::Result<void>();
    });
    (void)svr.callToolPublic("yams.status", nlohmann::json::object());
    EXPECT_FALSE(captured.empty());
    // We can only assert that it's not empty and ends with yams-daemon.sock; base dir is platform
    // dependent.
    EXPECT_NE(captured.rfind("yams-daemon.sock"), std::string::npos);
}

TEST(MCPDaemonSocketResolutionTest, SocketPathIsReasonableLengthForUnix) {
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));
    std::string captured;
    setenv_strict("XDG_RUNTIME_DIR",
                  "/run/user/1000/this/is/a/very/long/prefix/that/could/exceed/the/afunix/limit");
    svr.setEnsureDaemonClientHook([&](const yams::daemon::ClientConfig& cfg) -> yams::Result<void> {
        captured = cfg.socketPath.string();
        return yams::Result<void>();
    });
    (void)svr.callToolPublic("yams.stats", nlohmann::json::object());
    EXPECT_FALSE(captured.empty());
#if defined(__linux__)
    // AF_UNIX sun_path is typically 108 bytes; enforce a reasonable bound (< 104) for path + NUL
    EXPECT_LT(captured.size(), 104u);
#endif
    setenv_strict("XDG_RUNTIME_DIR", nullptr);
}
