#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <string_view>
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

template <typename ResultLike> bool isPermissionDenied(const ResultLike& result) {
    if (result) {
        return false;
    }
    const std::string_view message{result.error().message};
    return message.find("Operation not permitted") != std::string_view::npos ||
           message.find("Permission denied") != std::string_view::npos;
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
    if (!first && isPermissionDenied(first)) {
        GTEST_SKIP()
            << "Skipping SocketServerLifecycleTest because UNIX domain sockets are not permitted: "
            << first.error().message;
    }
    ASSERT_TRUE(first) << (first ? "" : first.error().message);

    auto stopped = server.stop();
    ASSERT_TRUE(stopped);

    auto second = server.start();
    if (!second && isPermissionDenied(second)) {
        GTEST_SKIP() << "Skipping SocketServerLifecycleTest restart because UNIX domain sockets "
                        "are not permitted: "
                     << second.error().message;
    }
    EXPECT_TRUE(second) << (second ? "" : second.error().message);

    EXPECT_TRUE(server.stop());

    std::error_code ec;
    std::filesystem::remove_all(runtimeDir, ec);
}

} // namespace yams::tests
