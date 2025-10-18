#include <chrono>
#include <cstring>
#include <filesystem>
#include <optional>
#include <thread>
#include <unistd.h>
#include <vector>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "test_async_helpers.h"
#include "test_daemon_harness.h"
#include <yams/daemon/client/daemon_client.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace {
int connect_unix(const fs::path& sock) {
    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::string sp = sock.string();
    if (sp.size() >= sizeof(addr.sun_path)) {
        ::close(fd);
        return -1;
    }
    std::strncpy(addr.sun_path, sp.c_str(), sizeof(addr.sun_path) - 1);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

bool fd_is_closed(int fd) {
    char c = 0;
    ssize_t n = ::send(fd, &c, 1, MSG_NOSIGNAL);
    if (n <= 0)
        return true;
    char buf[1];
    n = ::recv(fd, buf, 1, MSG_DONTWAIT);
    return (n == 0) || (n < 0 && (errno == ECONNRESET));
}
} // namespace

TEST(DaemonShutdownDrainIntegration, ShutdownRpcSetsStopRequestedAndStops) {
    yams::test::DaemonHarness h;
    ASSERT_TRUE(h.start(3s));

    yams::daemon::ClientConfig cc;
    cc.socketPath = h.socketPath();
    cc.autoStart = false;
    yams::daemon::DaemonClient client(cc);
    auto shut = yams::cli::run_sync(client.shutdown(true), 3s);
    ASSERT_TRUE(shut);

    auto* d = h.daemon();
    ASSERT_NE(d, nullptr);
    const auto deadline = std::chrono::steady_clock::now() + 1s;
    while (!d->isStopRequested() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(10ms);
    }
    EXPECT_TRUE(d->isStopRequested());

    auto stopped = d->stop();
    ASSERT_TRUE(stopped) << (stopped ? "" : stopped.error().message);
    EXPECT_FALSE(fs::exists(h.socketPath()));
}

TEST(DaemonShutdownDrainIntegration, ShutdownDrainsActiveConnections) {
    yams::test::DaemonHarness h;
    ASSERT_TRUE(h.start(3s));

    std::vector<int> fds;
    for (int i = 0; i < 4; ++i) {
        int fd = connect_unix(h.socketPath());
        ASSERT_GE(fd, 0) << "connect_unix failed: " << std::strerror(errno);
        fds.push_back(fd);
    }

    yams::daemon::ClientConfig cc;
    cc.socketPath = h.socketPath();
    cc.autoStart = false;
    yams::daemon::DaemonClient client(cc);
    ASSERT_TRUE(yams::cli::run_sync(client.shutdown(true), 3s));

    std::this_thread::sleep_for(100ms);

    auto* d = h.daemon();
    ASSERT_NE(d, nullptr);
    auto res = d->stop();
    ASSERT_TRUE(res) << (res ? "" : res.error().message);

    for (int fd : fds) {
        EXPECT_TRUE(fd_is_closed(fd));
        ::close(fd);
    }
    EXPECT_FALSE(fs::exists(h.socketPath()));
}
