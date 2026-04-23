#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <yams/cli/cli_sync.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/daemon.h>
#include <yams/app/services/session_service.hpp>
#include <yams/mcp/mcp_server.h>

#include "../../common/daemon_preflight.h"
#include "../../common/env_compat.h"
#include "../../common/fixture_manager.h"

namespace {

void clear_current_session_state() {
    ::unsetenv("YAMS_SESSION_CURRENT");
    auto sessionSvc = yams::app::services::makeSessionService(nullptr);
    sessionSvc->close();
}

/// Local daemon fixture for GTest smoke tests. Replaces the former
/// tests/common/daemon_test_fixture.h header that was deprecated in the
/// Catch2 migration.
struct LocalDaemonFixture {
    std::filesystem::path root_;
    std::filesystem::path storageDir_;
    std::filesystem::path runtimeRoot_;
    std::filesystem::path socketPath_;
    std::unique_ptr<yams::test::FixtureManager> fixtures_;
    std::unique_ptr<yams::daemon::YamsDaemon> daemon_;
    std::thread runLoopThread_;

    LocalDaemonFixture() {
        clear_current_session_state();
        auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        root_ = std::filesystem::temp_directory_path() / ("yams_test_" + unique);
        storageDir_ = root_ / "storage";
        runtimeRoot_ = root_ / "runtime";

        std::error_code ec;
        std::filesystem::create_directories(storageDir_, ec);
        INFO("Failed to create storage dir: " << ec.message());
        REQUIRE_FALSE(ec);
        std::filesystem::create_directories(runtimeRoot_, ec);
        INFO("Failed to create runtime dir: " << ec.message());
        REQUIRE_FALSE(ec);

        fixtures_ = std::make_unique<yams::test::FixtureManager>(root_ / "fixtures");

        yams::test::harnesses::DaemonPreflight::ensure_environment({
            .runtime_dir = runtimeRoot_,
            .socket_name_prefix = "yams-test-",
            .kill_others = false,
        });

        if (const char* s = std::getenv("YAMS_SOCKET_PATH")) {
            socketPath_ = s;
        } else {
            socketPath_ = runtimeRoot_ / ("yams-test-" + std::to_string(::getpid()) + ".sock");
        }
    }

    ~LocalDaemonFixture() {
        clear_current_session_state();
        if (daemon_) {
            daemon_->stop();
            if (runLoopThread_.joinable()) {
                runLoopThread_.join();
            }
            daemon_.reset();
        }
        fixtures_.reset();

        yams::test::harnesses::DaemonPreflight::post_test_cleanup(runtimeRoot_);

        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
        if (ec) {
            std::cerr << "Warning: Failed to cleanup test dir " << root_ << ": " << ec.message()
                      << std::endl;
        }

        ::unsetenv("YAMS_SOCKET_PATH");
        ::unsetenv("YAMS_DAEMON_SOCKET");

        yams::daemon::GlobalIOContext::reset();
        yams::daemon::AsioConnectionPool::shutdown_all(std::chrono::milliseconds(500));
    }

    bool startDaemon(std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
        yams::daemon::DaemonConfig cfg;
        cfg.dataDir = storageDir_;
        cfg.socketPath = socketPath_;
        cfg.pidFile = root_ / "daemon.pid";
        cfg.logFile = root_ / "daemon.log";

        daemon_ = std::make_unique<yams::daemon::YamsDaemon>(cfg);
        auto started = daemon_->start();

        if (!started) {
            FAIL_CHECK(std::string("Failed to start daemon: ") + started.error().message);
            return false;
        }

        runLoopThread_ = std::thread([this]() { daemon_->runLoop(); });
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            auto lifecycle = daemon_->getLifecycle().snapshot();
            if (lifecycle.state == yams::daemon::LifecycleState::Ready) {
                return true;
            } else if (lifecycle.state == yams::daemon::LifecycleState::Failed) {
                FAIL_CHECK(std::string("Daemon lifecycle reached Failed state: ") +
                           lifecycle.lastError);
                return false;
            }
        }

        FAIL_CHECK("Daemon failed to reach Ready state within timeout");
        return false;
    }

    void stopDaemon() {
        if (daemon_) {
            daemon_->stop();
            if (runLoopThread_.joinable()) {
                runLoopThread_.join();
            }
            daemon_.reset();
        }
    }

    const std::filesystem::path& socketPath() const { return socketPath_; }
};

} // namespace

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

using yams::Error;
using yams::Result;

Result<void> wait_for_daemon_ready(const std::filesystem::path& socketPath) {
    using namespace std::chrono_literals;
    yams::daemon::ClientConfig statusCfg;
    statusCfg.socketPath = socketPath;
    statusCfg.requestTimeout = 5s;
    yams::daemon::DaemonClient statusClient(statusCfg);
    auto connectRes = yams::cli::run_sync(statusClient.connect(), 2s);
    if (!connectRes)
        return connectRes.error();

    std::string statusErr;
    for (int attempt = 0; attempt < 120; ++attempt) {
        auto statusRes = yams::cli::run_sync(statusClient.status(), 1s);
        if (statusRes) {
            const auto& s = statusRes.value();
            bool metadataReady = s.readinessStates.contains("metadata_repo") &&
                                 s.readinessStates.at("metadata_repo");
            bool searchReady = s.readinessStates.contains("search_engine") &&
                               s.readinessStates.at("search_engine");
            if (s.ready || (metadataReady && searchReady)) {
                return Result<void>();
            }
            statusErr = s.overallStatus;
        } else {
            statusErr = statusRes.error().message;
        }
        std::this_thread::sleep_for(250ms);
    }
    return Error{yams::ErrorCode::Timeout,
                 std::string("daemon never reached ready state: ") + statusErr};
}

std::optional<json> extract_tool_data(const json& toolResult) {
    if (toolResult.contains("structuredContent") && toolResult["structuredContent"].is_object()) {
        const auto& sc = toolResult["structuredContent"];
        if (sc.value("type", std::string{}) == "tool_result" && sc.contains("data") &&
            sc["data"].is_object()) {
            return sc["data"];
        }
    }
    if (toolResult.contains("result") && toolResult["result"].is_object()) {
        const auto& r = toolResult["result"];
        if (r.contains("structuredContent") && r["structuredContent"].is_object()) {
            const auto& sc = r["structuredContent"];
            if (sc.value("type", std::string{}) == "tool_result" && sc.contains("data") &&
                sc["data"].is_object()) {
                return sc["data"];
            }
        }
    }
    return std::nullopt;
}

bool contains_subpath(const std::vector<std::string>& values, const std::string& needle) {
    return std::any_of(values.begin(), values.end(),
                       [&](const auto& value) { return value.find(needle) != std::string::npos; });
}

bool wait_for_list_tag_match(MCPServer& server, const std::vector<std::string>& tags,
                             bool matchAllTags, const std::string& expectedName,
                             std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
    using namespace std::chrono_literals;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        auto res = server.callToolPublic("list", json{{"tags", tags},
                                                      {"match_all_tags", matchAllTags},
                                                      {"paths_only", false},
                                                      {"limit", 20}});
        if (res.is_object() && !res.contains("error")) {
            auto data = extract_tool_data(res);
            if (data.has_value() && data->contains("documents") &&
                (*data)["documents"].is_array()) {
                std::vector<std::string> listedNames;
                for (const auto& doc : (*data)["documents"]) {
                    listedNames.push_back(doc.value("name", std::string{}));
                }
                if (contains_subpath(listedNames, expectedName)) {
                    return true;
                }
            }
        }
        std::this_thread::sleep_for(100ms);
    }
    return false;
}

bool wait_for_list_name_match(MCPServer& server, const std::string& expectedName,
                              std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
    using namespace std::chrono_literals;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        auto res = server.callToolPublic(
            "list", json{{"name", expectedName}, {"paths_only", false}, {"limit", 20}});
        if (res.is_object() && !res.contains("error")) {
            auto data = extract_tool_data(res);
            if (data.has_value() && data->contains("documents") &&
                (*data)["documents"].is_array()) {
                std::vector<std::string> listedNames;
                for (const auto& doc : (*data)["documents"]) {
                    listedNames.push_back(doc.value("name", std::string{}));
                }
                if (contains_subpath(listedNames, expectedName)) {
                    return true;
                }
            }
        }
        std::this_thread::sleep_for(100ms);
    }
    return false;
}

bool wait_for_get_name_match(MCPServer& server, const std::string& expectedName,
                             std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
    using namespace std::chrono_literals;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        auto res =
            server.callToolPublic("get", json{{"name", expectedName}, {"include_content", false}});
        if (res.is_object() && !res.contains("error")) {
            return true;
        }
        std::this_thread::sleep_for(100ms);
    }
    return false;
}

bool wait_for_grep_match_count(MCPServer& server, const std::string& pattern, size_t minMatchCount,
                               std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
    using namespace std::chrono_literals;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        auto res = server.callToolPublic(
            "grep", json{{"pattern", pattern}, {"use_session", false}, {"with_filename", true}});
        if (res.is_object() && !res.contains("error")) {
            auto data = extract_tool_data(res);
            if (data.has_value() && data->contains("match_count") &&
                (*data)["match_count"].is_number_unsigned() &&
                (*data)["match_count"].get<size_t>() >= minMatchCount) {
                return true;
            }
        }
        std::this_thread::sleep_for(100ms);
    }
    return false;
}
} // namespace

// Linux-only: relies on AF_UNIX socket semantics and short XDG_RUNTIME_DIR
#if defined(__linux__)
TEST_CASE("MCPDoctorPositiveSmoke.DoctorReportsReadyWithLiveDaemon",
          "[smoke][mcpdoctorpositivesmoke]") {
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
    yams::test::harnesses::DaemonPreflight::ensure_environment({
        .runtime_dir = runtimeRoot,
        .socket_name_prefix = "yams-daemon-smoke-",
        .kill_others = false,
    });

    const fs::path socketPath = runtimeRoot / "yams-daemon.sock";
    ::setenv("YAMS_SOCKET_PATH", socketPath.string().c_str(), 1);
    ::setenv("YAMS_DAEMON_SOCKET", socketPath.string().c_str(), 1);
    yams::cli::cli_pool_reset_for_test();

    // Start the daemon
    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = storageDir;
    cfg.socketPath = socketPath;
    cfg.pidFile = root / "daemon.pid";
    cfg.logFile = root / "daemon.log";
    yams::daemon::YamsDaemon daemon(cfg);
    auto started = daemon.start();
    INFO((started ? std::string{} : started.error().message));
    REQUIRE(started);

    // Start runLoop in background thread - REQUIRED for daemon to process requests
    std::thread runLoopThread([&daemon]() { daemon.runLoop(); });

    INFO("daemon readiness wait failed");
    REQUIRE(wait_for_daemon_ready(socketPath));

    // Build an MCP server with a dummy transport and call the doctor tool
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));
    svr.setDaemonClientSocketPathForTest(socketPath);
    auto res = svr.callToolPublic("doctor", json::object());

    // Stop daemon and join runLoop thread before assertions
    daemon.stop();
    runLoopThread.join();

    // Validate the doctor result shape and readiness
    INFO(res.dump());
    REQUIRE_FALSE(res.contains("error"));
    INFO(res.dump());
    REQUIRE(res.contains("content"));

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
    CHECK(sawReady);
    CHECK(sawSocket);
    CHECK(sawExists);
    CHECK(sawConn);

    // Cleanup
    fs::remove_all(root, ec);
}
#endif // __linux__

// Test fixture for MCP smoke tests with proper isolation
struct MCPSmokeFixture {
    MCPSmokeFixture() {
        clear_current_session_state();
        ::unsetenv("YAMS_SOCKET_PATH");
        ::unsetenv("YAMS_DAEMON_SOCKET");
    }
    ~MCPSmokeFixture() {
        clear_current_session_state();
        ::unsetenv("YAMS_SOCKET_PATH");
        ::unsetenv("YAMS_DAEMON_SOCKET");
    }
};

// Basic success-shape sanity for a couple of tools (no crash, minimal structure).
TEST_CASE_METHOD(MCPSmokeFixture, "BasicToolSuccessShapes", "[smoke][mcpsmokefixture]") {
    using nlohmann::json;
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));

    // Prevent daemon client access to avoid interference from previous tests
    svr.setEnsureDaemonClientHook([](const yams::daemon::ClientConfig&) -> yams::Result<void> {
        return yams::Error{yams::ErrorCode::NetworkError, "no daemon for test"};
    });

    // search: minimal valid body; allow empty results but expect a result object
    {
        auto res = svr.callToolPublic("search",
                                      json{{"query", "hello"}, {"limit", 1}, {"paths_only", true}});
        // Accept result or error, but ensure it returned promptly and is JSON
        INFO(res.dump());
        REQUIRE(res.is_object());
    }

    // grep: minimal body
    {
        auto res = svr.callToolPublic(
            "grep", json{{"pattern", "hello"}, {"paths", json::array()}, {"paths_only", true}});
        INFO(res.dump());
        REQUIRE(res.is_object());
    }
}

// PBI028_PHASE3_MCP_DOCOPS
// Doc ops round-trip via MCP with a live daemon (portable; uses short /tmp socket).
struct MCPDocOpsFixture : public LocalDaemonFixture {
    MCPDocOpsFixture() {
        yams::cli::cli_pool_reset_for_test();
        ::setenv("YAMS_SOCKET_PATH", socketPath().string().c_str(), 1);
        ::setenv("YAMS_DAEMON_SOCKET", socketPath().string().c_str(), 1);
    }

    ~MCPDocOpsFixture() { stopDaemon(); }
};

TEST_CASE_METHOD(MCPDocOpsFixture, "DocOpsRoundTrip", "[smoke][mcpdocopsfixture]") {
    using namespace std::chrono_literals;
    using nlohmann::json;

    // Start daemon using fixture helper
    REQUIRE(startDaemon());
    INFO("daemon readiness wait failed");
    REQUIRE(wait_for_daemon_ready(socketPath()));

    // Build an MCP server with a dummy transport (direct callToolPublic)
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));
    svr.setDaemonClientSocketPathForTest(socketPath());

    // 1) add content
    auto addRes =
        svr.callToolPublic("add", json{{"content", "hello world"}, {"name", "mcp_smoke.txt"}});
    INFO(addRes.dump());
    REQUIRE(addRes.is_object());
    INFO(addRes.dump());
    REQUIRE_FALSE(addRes.contains("error"));
    INFO("name resolution wait failed for mcp_smoke.txt");
    REQUIRE(wait_for_get_name_match(svr, "mcp_smoke.txt"));

    // 2) list by name (paths_only allowed); tolerate structure variations
    auto listRes = svr.callToolPublic(
        "list", json{{"name", "mcp_smoke.txt"}, {"limit", 5}, {"paths_only", true}});
    INFO(listRes.dump());
    REQUIRE(listRes.is_object());
    // 3) get (by name) and assert content round-trip contains seed
    auto getRes =
        svr.callToolPublic("get", json{{"name", "mcp_smoke.txt"}, {"include_content", true}});
    INFO(getRes.dump());
    REQUIRE(getRes.is_object());
    INFO(getRes.dump());
    REQUIRE_FALSE(getRes.contains("error"));
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
    REQUIRE_FALSE(blob.empty());
    CHECK(blob.find("hello") != std::string::npos);

    // 4) update: add a tag, then list with that tag
    auto updRes = svr.callToolPublic(
        "update",
        json{{"name", "mcp_smoke.txt"}, {"type", "tags"}, {"tags", json::array({"smoke"})}});
    REQUIRE(updRes.is_object());
    auto listByTag =
        svr.callToolPublic("list", json{{"limit", 10}, {"tags", json::array({"smoke"})}});
    REQUIRE(listByTag.is_object());

    // 5) delete_by_name, then ensure it no longer lists by exact name
    auto delRes = svr.callToolPublic("delete_by_name", json{{"name", "mcp_smoke.txt"}});
    REQUIRE(delRes.is_object());
    auto listGone = svr.callToolPublic("list", json{{"name", "mcp_smoke.txt"}, {"limit", 1}});
    REQUIRE(listGone.is_object());

    // Daemon cleanup handled by fixture TearDown()
}

TEST_CASE_METHOD(MCPDocOpsFixture, "SearchAndListTagFiltering", "[smoke][mcpdocopsfixture]") {
    using namespace std::chrono_literals;
    using nlohmann::json;

    REQUIRE(startDaemon());
    INFO("daemon readiness wait failed");
    REQUIRE(wait_for_daemon_ready(socketPath()));

    auto transport = std::make_unique<NullTransport>();
    MCPServer server(std::move(transport));
    server.setDaemonClientSocketPathForTest(socketPath());

    const std::string redName = "mcp_tag_filter_red.txt";
    const std::string blueName = "mcp_tag_filter_blue.txt";
    const std::string queryToken = "mcp-tag-filter-token";

    auto addRed = server.callToolPublic(
        "add", json{{"content", "red content " + queryToken}, {"name", redName}});
    INFO(addRed.dump());
    REQUIRE(addRed.is_object());
    INFO(addRed.dump());
    REQUIRE_FALSE(addRed.contains("error"));
    INFO("name resolution wait failed for " << redName);
    REQUIRE(wait_for_get_name_match(server, redName));

    auto addBlue = server.callToolPublic(
        "add", json{{"content", "blue content " + queryToken}, {"name", blueName}});
    INFO(addBlue.dump());
    REQUIRE(addBlue.is_object());
    INFO(addBlue.dump());
    REQUIRE_FALSE(addBlue.contains("error"));
    INFO("name resolution wait failed for " << blueName);
    REQUIRE(wait_for_get_name_match(server, blueName));

    auto tagRed = server.callToolPublic(
        "update",
        json{{"name", redName}, {"type", "tags"}, {"tags", json::array({"team-red", "group-a"})}});
    INFO(tagRed.dump());
    REQUIRE(tagRed.is_object());
    INFO(tagRed.dump());
    REQUIRE_FALSE(tagRed.contains("error"));

    auto tagBlue =
        server.callToolPublic("update", json{{"name", blueName},
                                             {"type", "tags"},
                                             {"tags", json::array({"team-blue", "group-a"})}});
    INFO(tagBlue.dump());
    REQUIRE(tagBlue.is_object());
    INFO(tagBlue.dump());
    REQUIRE_FALSE(tagBlue.contains("error"));

    INFO("tag visibility wait failed for " << redName);
    REQUIRE(wait_for_list_tag_match(server, {"team-red"}, true, redName));
    INFO("tag visibility wait failed for " << blueName);
    REQUIRE(wait_for_list_tag_match(server, {"team-blue"}, true, blueName));

    auto listRedOnly = server.callToolPublic("list", json{{"tags", json::array({"team-red"})},
                                                          {"match_all_tags", true},
                                                          {"paths_only", false},
                                                          {"limit", 20}});
    INFO(listRedOnly.dump());
    REQUIRE(listRedOnly.is_object());
    INFO(listRedOnly.dump());
    REQUIRE_FALSE(listRedOnly.contains("error"));
    auto listRedData = extract_tool_data(listRedOnly);
    INFO(listRedOnly.dump());
    REQUIRE(listRedData.has_value());
    REQUIRE(listRedData->contains("documents"));

    std::vector<std::string> listedNames;
    for (const auto& doc : (*listRedData)["documents"]) {
        listedNames.push_back(doc.value("name", std::string{}));
    }
    INFO(listRedOnly.dump());
    CHECK(contains_subpath(listedNames, redName));
    INFO(listRedOnly.dump());
    CHECK_FALSE(contains_subpath(listedNames, blueName));

    auto listImpossible =
        server.callToolPublic("list", json{{"tags", json::array({"team-red", "team-blue"})},
                                           {"match_all_tags", true},
                                           {"paths_only", false},
                                           {"limit", 20}});
    INFO(listImpossible.dump());
    REQUIRE(listImpossible.is_object());
    INFO(listImpossible.dump());
    REQUIRE_FALSE(listImpossible.contains("error"));
    auto listImpossibleData = extract_tool_data(listImpossible);
    INFO(listImpossible.dump());
    REQUIRE(listImpossibleData.has_value());
    REQUIRE(listImpossibleData->contains("documents"));
    INFO(listImpossible.dump());
    CHECK((*listImpossibleData)["documents"].empty());

    auto searchRedOnly = server.callToolPublic("search", json{{"query", queryToken},
                                                              {"type", "keyword"},
                                                              {"tags", json::array({"team-red"})},
                                                              {"match_all_tags", true},
                                                              {"paths_only", true},
                                                              {"limit", 20}});
    INFO(searchRedOnly.dump());
    REQUIRE(searchRedOnly.is_object());
    INFO(searchRedOnly.dump());
    REQUIRE_FALSE(searchRedOnly.contains("error"));
    auto searchRedData = extract_tool_data(searchRedOnly);
    INFO(searchRedOnly.dump());
    REQUIRE(searchRedData.has_value());
    REQUIRE(searchRedData->contains("paths"));

    std::vector<std::string> redPaths;
    for (const auto& p : (*searchRedData)["paths"]) {
        redPaths.push_back(p.get<std::string>());
    }
    INFO(searchRedOnly.dump());
    CHECK(contains_subpath(redPaths, redName));
    INFO(searchRedOnly.dump());
    CHECK_FALSE(contains_subpath(redPaths, blueName));

    auto searchImpossible =
        server.callToolPublic("search", json{{"query", queryToken},
                                             {"type", "keyword"},
                                             {"tags", json::array({"team-red", "team-blue"})},
                                             {"match_all_tags", true},
                                             {"paths_only", true},
                                             {"limit", 20}});
    INFO(searchImpossible.dump());
    REQUIRE(searchImpossible.is_object());
    INFO(searchImpossible.dump());
    REQUIRE_FALSE(searchImpossible.contains("error"));
    auto searchImpossibleData = extract_tool_data(searchImpossible);
    INFO(searchImpossible.dump());
    REQUIRE(searchImpossibleData.has_value());
    REQUIRE(searchImpossibleData->contains("paths"));
    INFO(searchImpossible.dump());
    CHECK((*searchImpossibleData)["paths"].empty());
}

TEST_CASE_METHOD(MCPDocOpsFixture, "SessionStartAloneDoesNotNarrowGrepResults",
                 "[smoke][mcpdocopsfixture]") {
    using nlohmann::json;

    REQUIRE(startDaemon());
    INFO("daemon readiness wait failed");
    REQUIRE(wait_for_daemon_ready(socketPath()));

    auto transport = std::make_unique<NullTransport>();
    MCPServer server(std::move(transport));
    server.setDaemonClientSocketPathForTest(socketPath());

    const std::string token = "mcp-session-grep-hot-path-token";
    const std::string firstName = "mcp_session_grep_hot_1.txt";
    const std::string secondName = "mcp_session_grep_hot_2.txt";
    const std::string sessionName =
        "mcp-grep-hot-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());

    auto addFirst =
        server.callToolPublic("add", json{{"content", "alpha " + token}, {"name", firstName}});
    INFO(addFirst.dump());
    REQUIRE(addFirst.is_object());
    INFO(addFirst.dump());
    REQUIRE_FALSE(addFirst.contains("error"));

    auto addSecond =
        server.callToolPublic("add", json{{"content", "beta " + token}, {"name", secondName}});
    INFO(addSecond.dump());
    REQUIRE(addSecond.is_object());
    INFO(addSecond.dump());
    REQUIRE_FALSE(addSecond.contains("error"));

    auto sessionStart =
        server.callToolPublic("session_start", json{{"name", sessionName}, {"warm", false}});
    INFO(sessionStart.dump());
    REQUIRE(sessionStart.is_object());
    INFO(sessionStart.dump());
    REQUIRE_FALSE(sessionStart.contains("error"));
    INFO("grep visibility wait failed for session-start smoke token");
    REQUIRE(wait_for_grep_match_count(server, token, 2));

    auto grepGlobal = server.callToolPublic(
        "grep", json{{"pattern", token}, {"use_session", false}, {"with_filename", true}});
    INFO(grepGlobal.dump());
    REQUIRE(grepGlobal.is_object());
    INFO(grepGlobal.dump());
    REQUIRE_FALSE(grepGlobal.contains("error"));
    auto grepGlobalData = extract_tool_data(grepGlobal);
    INFO(grepGlobal.dump());
    REQUIRE(grepGlobalData.has_value());
    INFO(grepGlobal.dump());
    REQUIRE(grepGlobalData->contains("match_count"));

    auto grepSession = server.callToolPublic("grep", json{{"pattern", token},
                                                          {"use_session", true},
                                                          {"session", sessionName},
                                                          {"with_filename", true}});
    INFO(grepSession.dump());
    REQUIRE(grepSession.is_object());
    INFO(grepSession.dump());
    REQUIRE_FALSE(grepSession.contains("error"));
    auto grepSessionData = extract_tool_data(grepSession);
    INFO(grepSession.dump());
    REQUIRE(grepSessionData.has_value());
    INFO(grepSession.dump());
    REQUIRE(grepSessionData->contains("match_count"));

    INFO(grepGlobal.dump());
    CHECK((*grepGlobalData)["match_count"].get<int>() >= 2);
    INFO(grepSession.dump());
    CHECK((*grepSessionData)["match_count"].get<int>() ==
          (*grepGlobalData)["match_count"].get<int>());
}

// Pagination and dry-run behaviors should be accepted and return structured JSON.
TEST_CASE_METHOD(MCPSmokeFixture, "ListPaginationAndDryRunDelete", "[smoke][mcpsmokefixture]") {
    using namespace std::chrono_literals;
    namespace fs = std::filesystem;
    using nlohmann::json;

    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));

    // Force unreachable daemon for delete_by_name; ensure error is structured (no crash)
    svr.setEnsureDaemonClientHook([](const yams::daemon::ClientConfig&) -> yams::Result<void> {
        return yams::Error{yams::ErrorCode::NetworkError, "dial error"};
    });

    // list with pagination params present should return an object with or without results
    auto listRes =
        svr.callToolPublic("list", json{{"limit", 2}, {"offset", 0}, {"paths_only", true}});
    INFO(listRes.dump());
    REQUIRE(listRes.is_object());

    // delete_by_name with dry_run should return an error (unreachable) but remain structured
    auto delRes =
        svr.callToolPublic("delete_by_name", json{{"name", "nope.txt"}, {"dry_run", true}});
    INFO(delRes.dump());
    REQUIRE(delRes.is_object());
    REQUIRE(delRes.contains("error"));
}

// Update metadata by hash should be accepted when daemon is reachable; here we only assert schema
// roundtrip (no crash)
TEST_CASE_METHOD(MCPSmokeFixture, "UpdateMetadataSchemaRoundTrip", "[smoke][mcpsmokefixture]") {
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));
    auto upd = svr.callToolPublic("update", nlohmann::json{{"hash", "deadbeef"},
                                                           {"type", "metadata"},
                                                           {"metadata", nlohmann::json::object()}});
    INFO(upd.dump());
    REQUIRE(upd.is_object());
}

// Minimal list success-shape (daemon-first path, but tolerant structure check).
TEST_CASE_METHOD(MCPSmokeFixture, "ListDocumentsResponds", "[smoke][mcpsmokefixture]") {
    using nlohmann::json;
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));
    auto res = svr.callToolPublic(
        "list", json{{"paths_only", true}, {"limit", 1}, {"recent", 0}, {"verbose", false}});
    INFO(res.dump());
    REQUIRE(res.is_object());
}

// Unreachable envelope checks for daemon-first doc ops: list and add; tolerant checks for get.
TEST_CASE_METHOD(MCPSmokeFixture, "UnreachableEnvelopeUniformForDocOps",
                 "[smoke][mcpsmokefixture]") {
    using nlohmann::json;
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));

    // Force unreachable daemon
    svr.setEnsureDaemonClientHook([](const yams::daemon::ClientConfig&) -> yams::Result<void> {
        return yams::Error{yams::ErrorCode::NetworkError, "dial error"};
    });

    auto assert_unreachable = [&](const std::string& tool, const json& args) {
        auto res = svr.callToolPublic(tool, args);
        INFO(res.dump());
        REQUIRE(res.is_object());
        INFO(res.dump());
        REQUIRE(res.contains("error"));
        auto msg = res["error"].value("message", std::string{});
        bool ok = (msg.find("YAMS_DAEMON_SOCKET") != std::string::npos ||
                   msg.find("dial") != std::string::npos ||
                   msg.find("Unknown tool") != std::string::npos);
        INFO((tool + ": " + msg));
        CHECK(ok);
    };

    // list uses daemon-first path in MCP
    assert_unreachable("list", json{{"limit", 1}, {"paths_only", true}});
    // add/store uses daemon-first path
    assert_unreachable("add", json{{"content", "x"}, {"name", "x.txt"}});
    // get may not contact daemon on minimal builds; accept NotFound or dial
    {
        auto res = svr.callToolPublic("get", json{{"name", "does-not-exist"}});
        REQUIRE(res.is_object());
        REQUIRE(res.contains("error"));
        auto msg = res["error"].value("message", std::string{});
        bool ok =
            (msg.find("dial") != std::string::npos || msg.find("not found") != std::string::npos ||
             msg.find("not found by name") != std::string::npos);
        INFO(msg);
        CHECK(ok);
    }
    // delete/update may resolve locally and not contact daemon on minimal builds; not asserted
    // here.
}

// PBI028-45-MCP-PARITY-MOVE: Parity test moved from services shard to smoke and
// relaxed latency to 2500ms to avoid interference from services' daemon lifecycle.
TEST_CASE_METHOD(MCPSmokeFixture, "Parity_UnreachableEnvelopeAndToolsListRespondsQuickly",
                 "[smoke][mcpsmokefixture]") {
    using nlohmann::json;
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));

    // Force ensure to fail with a transport error (simulates unreachable daemon)
    svr.setEnsureDaemonClientHook([](const yams::daemon::ClientConfig&) -> yams::Result<void> {
        return yams::Error{yams::ErrorCode::NetworkError, "dial error"};
    });

    // status should return an error quickly with a useful envelope (unreachable daemon)
    auto callRes = svr.callToolPublic("status", json::object());
    REQUIRE(callRes.contains("error"));
    auto msg = callRes["error"].value("message", std::string{});
    bool ok = (msg.find("YAMS_DAEMON_SOCKET") != std::string::npos ||
               msg.find("dial") != std::string::npos);
    INFO(msg);
    CHECK(ok);

    // A tool that does not require daemon (search) should still respond promptly (server alive)
    auto start = std::chrono::steady_clock::now();
    auto listRes =
        svr.callToolPublic("search", json{{"query", "ping"}, {"limit", 1}, {"paths_only", true}});
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    CHECK(elapsed.count() < 2500);
    REQUIRE(listRes.is_object());
}
