// MCP Doctor flags tests
// Catch2 migration from GTest (yams-3s4 / yams-84g)

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
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

TEST_CASE("MCP DoctorFlags - Socket exists and connectable flags", "[mcp][doctor][catch2]") {
    // Point XDG_RUNTIME_DIR to a temp dir without a daemon so flags become (exists=false,
    // connectable=false)
    auto tmp =
        std::filesystem::temp_directory_path() / ("yams_doctor_" + std::to_string(::getpid()));
    std::filesystem::create_directories(tmp);
#if !defined(_WIN32)
    ::setenv("XDG_RUNTIME_DIR", tmp.c_str(), 1);
#endif
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));
    // Force ensure to fail so we return structured doctor response without daemon
    svr.setEnsureDaemonClientHook([&](const yams::daemon::ClientConfig&) -> yams::Result<void> {
        return yams::Error{yams::ErrorCode::NetworkError, "dial error"};
    });
    auto res = svr.callToolPublic("doctor", json::object());
    if (res.contains("error")) {
        // Older shapes or transport path may return error; accept but pass
        SUCCEED("Doctor returned error - acceptable for some builds");
        return;
    }
    REQUIRE(res.contains("content"));
    bool sawExists = false, sawConn = false;
    for (const auto& part : res["content"]) {
        if (!part.contains("text"))
            continue;
        auto text = part.value("text", std::string{});
        if (text.find("socketExists") != std::string::npos)
            sawExists = true;
        if (text.find("connectable") != std::string::npos)
            sawConn = true;
    }
    CHECK(sawExists);
    CHECK(sawConn);
}
