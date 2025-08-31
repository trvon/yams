#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include <cerrno>
#include <cstring>

#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>

namespace yams::daemon::integration::test {

namespace fs = std::filesystem;
using namespace std::chrono_literals;

class DaemonAbruptDisconnectTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create isolated temp directory for this test
        testDir_ = fs::temp_directory_path() /
                   ("abrupt_disconnect_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(testDir_);

        // Configure daemon to use temp paths
        config_.socketPath = testDir_ / "daemon.sock";
        config_.pidFile = testDir_ / "daemon.pid";
        config_.logFile = testDir_ / "daemon.log";
        config_.workerThreads = 2;

        // Client config points to the same socket, no autostart
        clientConfig_.socketPath = config_.socketPath;
        clientConfig_.autoStart = false;

        // Start daemon
        daemon_ = std::make_unique<YamsDaemon>(config_);
        auto started = daemon_->start();
        ASSERT_TRUE(started) << "Failed to start daemon: " << started.error().message;

        // Give server a moment to get into accept loop
        std::this_thread::sleep_for(100ms);
    }

    void TearDown() override {
        if (daemon_) {
            daemon_->stop();
            daemon_.reset();
        }
        std::error_code ec;
        fs::remove_all(testDir_, ec);
    }

    // Sends a framed StatusRequest using raw UNIX socket and then
    // immediately closes the socket to simulate abrupt client exit
    void sendStatusRequestAndClose() {
        int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
        ASSERT_GE(fd, 0) << "socket() failed: " << std::strerror(errno);

        sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        std::string spath = config_.socketPath.string();
        ASSERT_LT(spath.size(), sizeof(addr.sun_path)) << "Socket path too long";
        std::strncpy(addr.sun_path, spath.c_str(), sizeof(addr.sun_path) - 1);

        int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ASSERT_EQ(rc, 0) << "connect() failed: " << std::strerror(errno);

        // Build a valid StatusRequest message
        StatusRequest sreq{};
        sreq.detailed = true;

        Message msg;
        msg.version = PROTOCOL_VERSION;
        msg.requestId =
            static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
        msg.timestamp = std::chrono::steady_clock::now();
        msg.payload = Request{std::move(sreq)};
        msg.clientVersion = "abrupt-test";

        MessageFramer framer;
        auto frameRes = framer.frame_message(msg);
        ASSERT_TRUE(frameRes) << "Failed to frame message: " << frameRes.error().message;

        auto& frame = frameRes.value();

        // Write the entire frame to the daemon
        size_t total = 0;
        while (total < frame.size()) {
            ssize_t n = ::send(fd, frame.data() + total, frame.size() - total, 0);
            if (n < 0) {
                // If send fails, we still want to close to simulate abrupt disconnect
                break;
            }
            total += static_cast<size_t>(n);
        }

        // Immediately close without reading any response
        ::close(fd);
    }

    // After we simulate abrupt closure, ensure daemon still accepts connections
    void assertDaemonHealthy() {
        DaemonClient client(clientConfig_);
        ASSERT_TRUE(client.connect());
        auto pong = client.ping();
        ASSERT_TRUE(pong);
    }

    fs::path testDir_;
    DaemonConfig config_;
    ClientConfig clientConfig_;
    std::unique_ptr<YamsDaemon> daemon_;
};

// This test simulates a client that sends a valid request and then
// abruptly disconnects while the server is writing the response payload.
// It ensures the daemon remains healthy and does not crash.
TEST_F(DaemonAbruptDisconnectTest, AbruptDisconnectDuringPayloadWrite) {
    // Run the scenario multiple times to increase likelihood of exercising races
    constexpr int kIterations = 50;

    for (int i = 0; i < kIterations; ++i) {
        sendStatusRequestAndClose();

        // Allow the server to process the write path
        std::this_thread::sleep_for(10ms);

        // Verify server is still healthy and accepts new connections
        assertDaemonHealthy();
    }
}

// Variant: Interleave abrupt disconnects with normal requests
TEST_F(DaemonAbruptDisconnectTest, MixedAbruptAndNormalRequests) {
    DaemonClient clientOK(clientConfig_);
    ASSERT_TRUE(clientOK.connect());

    constexpr int kTotal = 30;
    for (int i = 0; i < kTotal; ++i) {
        if (i % 3 == 0) {
            // Abrupt disconnect case
            sendStatusRequestAndClose();
        } else {
            // Normal request via DaemonClient
            auto pong = clientOK.ping();
            ASSERT_TRUE(pong);
            auto status = clientOK.status();
            // Status may be small, but still exercises server payload write path
            ASSERT_TRUE(status);
        }

        std::this_thread::sleep_for(5ms);
    }

    // Final health check
    assertDaemonHealthy();
}

} // namespace yams::daemon::integration::test