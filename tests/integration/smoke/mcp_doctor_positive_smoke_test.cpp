#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <thread>

#include <yams/daemon/daemon.h>
#include <yams/mcp/mcp_server.h>

#include "common/daemon_preflight.h"

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

// Linux-only: relies on AF_UNIX socket semantics and short XDG_RUNTIME_DIR
#if defined(__linux__)
TEST(MCPDoctorPositiveSmoke, DoctorReportsReadyWithLiveDaemon) {
    using namespace std::chrono_literals;
    namespace fs = std::filesystem;

    // Prepare isolated runtime and storage paths
    auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const fs::path root = fs::temp_directory_path() / ("yams_mcp_doctor_smoke_" + unique);
    const fs::path storageDir = root / "storage";
    const fs::path runtimeRoot = root / "runtime";
    std::error_code ec;
    fs::create_directories(storageDir, ec);
    fs::create_directories(runtimeRoot, ec);

    // Ensure a short socket path to avoid AF_UNIX sun_path limits
    yams::tests::harnesses::DaemonPreflight::ensure_environment({
        .runtime_dir = runtimeRoot,
        .socket_name_prefix = "yams-daemon-smoke-",
        .kill_others = false,
    });

    const fs::path socketPath = runtimeRoot / "yams-daemon.sock";
    ::setenv("YAMS_SOCKET_PATH", socketPath.string().c_str(), 1);

    // Start the daemon
    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = storageDir;
    cfg.socketPath = socketPath;
    cfg.pidFile = root / "daemon.pid";
    cfg.logFile = root / "daemon.log";
    yams::daemon::YamsDaemon daemon(cfg);
    auto started = daemon.start();
    ASSERT_TRUE(started) << started.error().message;

    // Build an MCP server with a dummy transport and call the doctor tool
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));
    auto res = svr.callToolPublic("doctor", json::object());

    // Stop daemon before assertions to avoid lingering processes
    daemon.stop();

    // Validate the doctor result shape and readiness
    ASSERT_FALSE(res.contains("error")) << res.dump();
    ASSERT_TRUE(res.contains("content")) << res.dump();

    bool sawReady = false, sawSocket = false, sawExists = false, sawConn = false;
    for (const auto& part : res["content"]) {
        if (!part.contains("text"))
            continue;
        auto text = part.value("text", std::string{});
        if (text.find("Daemon ready") != std::string::npos)
            sawReady = true;
        if (text.find("socketPath") != std::string::npos)
            sawSocket = true;
        if (text.find("socketExists=true") != std::string::npos)
            sawExists = true;
        if (text.find("connectable=true") != std::string::npos)
            sawConn = true;
    }
    EXPECT_TRUE(sawReady);
    EXPECT_TRUE(sawSocket);
    EXPECT_TRUE(sawExists);
    EXPECT_TRUE(sawConn);

    // Cleanup
    fs::remove_all(root, ec);
}
#endif // __linux__
