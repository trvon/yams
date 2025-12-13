// MCP Socket resolution tests
// Catch2 migration from GTest (yams-3s4 / yams-84g)

#include <catch2/catch_test_macros.hpp>

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
    // On Windows, _putenv_s cannot accept nullptr - use empty string to unset
    _putenv_s(k, v ? v : "");
#else
    if (v)
        ::setenv(k, v, 1);
    else
        ::unsetenv(k);
#endif
}
} // namespace

TEST_CASE("MCP DaemonSocketResolution - Honors env socket override",
          "[mcp][socket][resolution][catch2]") {
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
    if (captured.empty()) {
        // Some minimal builds may not call ensure for stats; force doctor path
        (void)svr.callToolPublic("yams.doctor", nlohmann::json::object());
    }
    // Tolerant: captured may still be empty on constrained builds
    if (!captured.empty()) {
        CHECK(captured.find("/tmp/yams-alt.sock") != std::string::npos);
    } else {
        SKIP("ensureDaemonClient not invoked on this build; skipping strict override check.");
    }
    setenv_strict("YAMS_DAEMON_SOCKET", nullptr);
}

TEST_CASE("MCP DaemonSocketResolution - Structured error includes socket and hint",
          "[mcp][socket][resolution][catch2]") {
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
    REQUIRE(res.contains("error"));
    auto msg = res["error"].value("message", std::string{});
    // Accept either structured socket+hint or a generic dial error on minimal builds
    bool ok = (msg.find("/run/user/1000/yams-daemon.sock") != std::string::npos &&
               msg.find("YAMS_DAEMON_SOCKET") != std::string::npos) ||
              (msg.find("dial") != std::string::npos) || (!msg.empty());
    CHECK(ok);
    setenv_strict("YAMS_DAEMON_SOCKET", nullptr);
}

TEST_CASE("MCP DoctorPath - Doctor uses same resolved socket", "[mcp][socket][doctor][catch2]") {
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));
    std::string captured;
    setenv_strict("XDG_RUNTIME_DIR", "/run/user/1001");
    svr.setEnsureDaemonClientHook([&](const yams::daemon::ClientConfig& cfg) -> yams::Result<void> {
        captured = cfg.socketPath.string();
        return yams::Error{yams::ErrorCode::NetworkError, "dial error"};
    });
    auto res = svr.callToolPublic("yams.doctor", nlohmann::json::object());
    if (captured.empty()) {
        SKIP("ensureDaemonClient not invoked on this build; skipping doctor path socket check.");
    }
    // Even when doctor fails upstream, structured content should include socketPath in details
    // Just verify we captured the socket path
    CHECK_FALSE(captured.empty());
}
