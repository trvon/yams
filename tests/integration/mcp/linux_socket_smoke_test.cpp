#include <gtest/gtest.h>

#ifdef __linux__

#include <chrono>
#include <filesystem>
#include <string>
#include <thread>

#include "../../common/daemon_test_fixture.h"
#include <yams/daemon/daemon.h>
#include <yams/mcp/mcp_server.h>

using namespace std::chrono_literals;

namespace {
class NullTransport : public yams::mcp::ITransport {
public:
    yams::mcp::MessageResult receive() override {
        return yams::Error{yams::ErrorCode::NetworkError, "closed"};
    }
    void send(const nlohmann::json&) override {}
    yams::mcp::TransportState getState() const override {
        return yams::mcp::TransportState::Connected;
    }
    bool isConnected() const override { return true; }
    void close() override {}
};

// Fixture for Linux socket tests
class LinuxSocketFixture : public yams::test::DaemonTestFixture {
protected:
    void SetUp() override {
        // Set XDG_RUNTIME_DIR so both daemon and MCP resolve the same socket path
        ::setenv("XDG_RUNTIME_DIR", runtimeRoot_.c_str(), 1);
        DaemonTestFixture::SetUp();
    }
};
} // namespace

TEST_F(LinuxSocketFixture, StatsSucceedsWithXdgRuntimeDir) {
    ASSERT_TRUE(startDaemon());

    // Spin briefly to allow socket server to come up
    std::this_thread::sleep_for(200ms);

    auto t = std::make_unique<NullTransport>();
    yams::mcp::MCPServer svr(std::move(t));
    // Try yams.stats via MCP — should succeed
    auto res = svr.callToolPublic("yams.stats", nlohmann::json{{"detailed", true}});
    // Either error is absent or structured content exists
    if (res.contains("error")) {
        ADD_FAILURE() << res.dump();
    } else {
        ASSERT_TRUE(res.contains("content"));
    }
}

#else

TEST(MCPLinuxSocketSmokeTest, SkippedOnNonLinux) {
    GTEST_SKIP() << "Linux-specific socket smoke test";
}

#endif
