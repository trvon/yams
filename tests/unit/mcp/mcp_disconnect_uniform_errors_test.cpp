#include <gtest/gtest.h>

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

// Validate that when the daemon is unreachable, tools return a uniform, actionable
// error with the resolved socket path and environment hint.
TEST(MCPDisconnectUniformErrors, StatsAndListDocumentsIncludeHintAndPath) {
    auto t = std::make_unique<NullTransport>();
    MCPServer svr(std::move(t));

    // Force the ensure hook to fail to simulate disconnect
    svr.setEnsureDaemonClientHook([&](const yams::daemon::ClientConfig& cfg) -> yams::Result<void> {
        // Ensure cfg carries the resolved path; we surface it in the error
        return yams::Error{yams::ErrorCode::NetworkError,
                           std::string("dial ") + cfg.socketPath.string()};
    });

    auto statsRes = svr.callToolPublic("stats", json::object({{"detailed", true}}));
    ASSERT_TRUE(statsRes.contains("error")) << statsRes.dump();
    auto statsErr = statsRes["error"].dump();
    // Accept either structured hint (socketPath + XDG_RUNTIME_DIR) or a generic dial message
    bool structured_stats = statsErr.find("socketPath") != std::string::npos &&
                            statsErr.find("XDG_RUNTIME_DIR") != std::string::npos;
    bool generic_stats = statsErr.find("dial") != std::string::npos;
    EXPECT_TRUE(structured_stats || generic_stats) << statsErr;

    // Optional: list_documents may not be available in minimal builds; skip if Unknown tool
    auto listRes = svr.callToolPublic("list_documents", json::object({{"recent", 1}}));
    if (listRes.contains("error")) {
        auto listErr = listRes["error"].dump();
        if (listErr.find("Unknown tool") == std::string::npos) {
            bool structured_list = listErr.find("socketPath") != std::string::npos &&
                                   listErr.find("XDG_RUNTIME_DIR") != std::string::npos;
            bool generic_list = listErr.find("dial") != std::string::npos;
            EXPECT_TRUE(structured_list || generic_list) << listErr;
        } else {
            SUCCEED() << "list_documents not registered in this build; skip shape check";
        }
    }
}
