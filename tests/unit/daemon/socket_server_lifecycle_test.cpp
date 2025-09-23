#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <thread>

#include <yams/daemon/components/SocketServer.h>
#include <yams/daemon/components/StateComponent.h>

namespace yams::tests {

namespace {
std::filesystem::path make_temp_runtime_dir(const std::string& name) {
    auto base = std::filesystem::temp_directory_path() / "yams-socket-server-tests";
    auto dir = base / name;
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    return dir;
}
} // namespace

TEST(SocketServerLifecycleTest, RestartClearsStoppingFlag) {
    using yams::daemon::SocketServer;
    using yams::daemon::StateComponent;

    auto runtimeDir = make_temp_runtime_dir("restart-check");
    auto socketPath = runtimeDir / "ipc.sock";

    SocketServer::Config config;
    config.socketPath = socketPath;
    config.workerThreads = 1;
    config.connectionTimeout = std::chrono::milliseconds(1500);

    StateComponent state;
    state.stats.startTime = std::chrono::steady_clock::now();

    SocketServer server(config, nullptr, &state);

    auto first = server.start();
    ASSERT_TRUE(first);

    auto stopped = server.stop();
    ASSERT_TRUE(stopped);

    auto second = server.start();
    EXPECT_TRUE(second);

    EXPECT_TRUE(server.stop());

    std::error_code ec;
    std::filesystem::remove_all(runtimeDir, ec);
}

} // namespace yams::tests
