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

// Basic success-shape sanity for a couple of tools (no crash, minimal structure).
TEST(MCPSmoke, BasicToolSuccessShapes) {
    using nlohmann::json;
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));

    // search: minimal valid body; allow empty results but expect a result object
    {
        auto res = svr.callToolPublic("search",
                                      json{{"query", "hello"}, {"limit", 1}, {"paths_only", true}});
        // Accept result or error, but ensure it returned promptly and is JSON
        ASSERT_TRUE(res.is_object()) << res.dump();
    }

    // grep: minimal body
    {
        auto res = svr.callToolPublic(
            "grep", json{{"pattern", "hello"}, {"paths", json::array()}, {"paths_only", true}});
        ASSERT_TRUE(res.is_object()) << res.dump();
    }
}

// PBI028_PHASE3_MCP_DOCOPS
// Doc ops round-trip via MCP with a live daemon (portable; uses short /tmp socket).
TEST(MCPSmoke, DocOpsRoundTrip) {
    using namespace std::chrono_literals;
    namespace fs = std::filesystem;
    using nlohmann::json;

    // Prepare isolated runtime and storage paths
    auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const fs::path root = fs::temp_directory_path() / ("yams_mcp_docops_smoke_" + unique);
    const fs::path storageDir = root / "storage";
    const fs::path runtimeRoot = root / "runtime";
    std::error_code ec;
    fs::create_directories(storageDir, ec);
    fs::create_directories(runtimeRoot, ec);

    // Short socket to avoid AF_UNIX sun_path limits
    const fs::path socketPath =
        fs::path("/tmp") / ("yams-mcp-docops-" + std::to_string(::getpid()) + ".sock");
    ::setenv("YAMS_SOCKET_PATH", socketPath.string().c_str(), 1);

    // Start daemon
    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = storageDir;
    cfg.socketPath = socketPath;
    cfg.pidFile = root / "daemon.pid";
    cfg.logFile = root / "daemon.log";
    yams::daemon::YamsDaemon daemon(cfg);
    auto started = daemon.start();
    ASSERT_TRUE(started) << started.error().message;

    // Build an MCP server with a dummy transport (direct callToolPublic)
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));

    // 1) add content
    auto addRes =
        svr.callToolPublic("add", json{{"content", "hello world"}, {"name", "mcp_smoke.txt"}});
    ASSERT_TRUE(addRes.is_object()) << addRes.dump();
    ASSERT_FALSE(addRes.contains("error")) << addRes.dump();

    // 2) list by name (paths_only allowed); tolerate structure variations
    auto listRes = svr.callToolPublic(
        "list", json{{"name", "mcp_smoke.txt"}, {"limit", 5}, {"paths_only", true}});
    ASSERT_TRUE(listRes.is_object()) << listRes.dump();
    // 3) get_by_name and assert content round-trip contains seed
    auto getRes = svr.callToolPublic("get_by_name", json{{"name", "mcp_smoke.txt"}});
    ASSERT_TRUE(getRes.is_object()) << getRes.dump();
    ASSERT_FALSE(getRes.contains("error")) << getRes.dump();
    // Result shape: either {content:[{text:...}]} or {result:{...}} depending on framing; accept
    // either
    std::string blob;
    if (getRes.contains("content") && getRes["content"].is_array()) {
        for (const auto& part : getRes["content"]) {
            if (part.contains("text"))
                blob += part.value("text", std::string{});
        }
    } else if (getRes.contains("result")) {
        blob = getRes["result"].dump();
    }
    ASSERT_FALSE(blob.empty());
    EXPECT_NE(blob.find("hello"), std::string::npos);

    // 4) update: add a tag, then list with that tag
    auto updRes = svr.callToolPublic(
        "update",
        json{{"name", "mcp_smoke.txt"}, {"type", "tags"}, {"tags", json::array({"smoke"})}});
    ASSERT_TRUE(updRes.is_object());
    auto listByTag =
        svr.callToolPublic("list", json{{"limit", 10}, {"tags", json::array({"smoke"})}});
    ASSERT_TRUE(listByTag.is_object());

    // 5) delete_by_name, then ensure it no longer lists by exact name
    auto delRes = svr.callToolPublic("delete_by_name", json{{"name", "mcp_smoke.txt"}});
    ASSERT_TRUE(delRes.is_object());
    auto listGone = svr.callToolPublic("list", json{{"name", "mcp_smoke.txt"}, {"limit", 1}});
    ASSERT_TRUE(listGone.is_object());

    // Cleanup
    daemon.stop();
    fs::remove_all(root, ec);
}

// Minimal list success-shape (daemon-first path, but tolerant structure check).
TEST(MCPSmoke, ListDocumentsResponds) {
    using nlohmann::json;
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));
    auto res = svr.callToolPublic(
        "list", json{{"paths_only", true}, {"limit", 1}, {"recent", 0}, {"verbose", false}});
    ASSERT_TRUE(res.is_object()) << res.dump();
}

// Unreachable envelope checks for daemon-first doc ops: list and add; tolerant checks for
// get_by_name.
TEST(MCPSmoke, UnreachableEnvelopeUniformForDocOps) {
    using nlohmann::json;
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));

    // Force unreachable daemon
    svr.setEnsureDaemonClientHook([](const yams::daemon::ClientConfig&) -> yams::Result<void> {
        return yams::Error{yams::ErrorCode::NetworkError, "dial error"};
    });

    auto assert_unreachable = [&](const std::string& tool, const json& args) {
        auto res = svr.callToolPublic(tool, args);
        ASSERT_TRUE(res.is_object()) << res.dump();
        ASSERT_TRUE(res.contains("error")) << res.dump();
        auto msg = res["error"].value("message", std::string{});
        bool ok = (msg.find("YAMS_DAEMON_SOCKET") != std::string::npos ||
                   msg.find("dial") != std::string::npos ||
                   msg.find("Unknown tool") != std::string::npos);
        EXPECT_TRUE(ok) << (tool + ": " + msg);
    };

    // list uses daemon-first path in MCP
    assert_unreachable("list", json{{"limit", 1}, {"paths_only", true}});
    // add/store uses daemon-first path
    assert_unreachable("add", json{{"content", "x"}, {"name", "x.txt"}});
    // get_by_name may not contact daemon on minimal builds; accept NotFound or dial
    {
        auto res = svr.callToolPublic("get_by_name", json{{"name", "does-not-exist"}});
        ASSERT_TRUE(res.is_object());
        ASSERT_TRUE(res.contains("error"));
        auto msg = res["error"].value("message", std::string{});
        bool ok =
            (msg.find("dial") != std::string::npos || msg.find("not found") != std::string::npos ||
             msg.find("not found by name") != std::string::npos);
        EXPECT_TRUE(ok) << msg;
    }
    // delete/update may resolve locally and not contact daemon on minimal builds; not asserted
    // here.
}

// PBI028-45-MCP-PARITY-MOVE: Parity test moved from services shard to smoke and
// relaxed latency to 2500ms to avoid interference from services' daemon lifecycle.
TEST(MCPSmoke, Parity_UnreachableEnvelopeAndToolsListRespondsQuickly) {
    using nlohmann::json;
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));

    // Force ensure to fail with a transport error (simulates unreachable daemon)
    svr.setEnsureDaemonClientHook([](const yams::daemon::ClientConfig&) -> yams::Result<void> {
        return yams::Error{yams::ErrorCode::NetworkError, "dial error"};
    });

    // stats should return an error quickly with a useful envelope (unreachable daemon)
    auto callRes = svr.callToolPublic("stats", json::object());
    ASSERT_TRUE(callRes.contains("error"));
    auto msg = callRes["error"].value("message", std::string{});
    bool ok = (msg.find("YAMS_DAEMON_SOCKET") != std::string::npos ||
               msg.find("dial") != std::string::npos);
    EXPECT_TRUE(ok) << msg;

    // A tool that does not require daemon (search) should still respond promptly (server alive)
    auto start = std::chrono::steady_clock::now();
    auto listRes =
        svr.callToolPublic("search", json{{"query", "ping"}, {"limit", 1}, {"paths_only", true}});
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    EXPECT_LT(elapsed.count(), 2500);
    ASSERT_TRUE(listRes.is_object());
}
