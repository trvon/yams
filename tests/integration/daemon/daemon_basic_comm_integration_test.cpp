#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>
#include <gtest/gtest.h>

#include <yams/daemon/daemon.h>
// Ensure Boost.Asio awaitable is declared before including daemon_client.h
#include "test_async_helpers.h"
#include <boost/asio/awaitable.hpp>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

class DaemonBasicCommIT : public ::testing::Test {
protected:
    fs::path testRoot_;
    fs::path storageDir_;
    fs::path xdgRuntimeDir_;
    fs::path socketPath_;
    std::unique_ptr<yams::daemon::YamsDaemon> daemon_;

    void SetUp() override {
        auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        testRoot_ = fs::temp_directory_path() / ("yams_daemon_basic_comm_it_" + unique);
        storageDir_ = testRoot_ / "storage";
        xdgRuntimeDir_ = testRoot_ / "xdg";
        fs::create_directories(storageDir_);
        fs::create_directories(xdgRuntimeDir_);

        // Use a short AF_UNIX socket path to satisfy sun_path limits (typically 104 bytes)
        socketPath_ = fs::path("/tmp") / ("yams-it-" + unique + ".sock");

        yams::daemon::DaemonConfig cfg;
        cfg.dataDir = storageDir_;
        cfg.socketPath = socketPath_;
        cfg.pidFile = testRoot_ / "daemon.pid";
        cfg.logFile = testRoot_ / "daemon.log";

        daemon_ = std::make_unique<yams::daemon::YamsDaemon>(cfg);
        auto started = daemon_->start();
        ASSERT_TRUE(started) << started.error().message;
        std::this_thread::sleep_for(200ms);
    }

    void TearDown() override {
        if (daemon_)
            daemon_->stop();
        std::error_code ec;
        fs::remove_all(testRoot_, ec);
    }
};

TEST_F(DaemonBasicCommIT, StatusPingListRoundtrip) {
    yams::daemon::ClientConfig ccfg;
    ccfg.socketPath = socketPath_;
    ccfg.autoStart = false;
    ccfg.requestTimeout = 10s;
    yams::daemon::DaemonClient client(ccfg);

    // Status
    auto st = yams::test_async::res(client.status(), 5s);
    ASSERT_TRUE(st) << st.error().message;
    EXPECT_TRUE(st.value().running);

    // Ping
    auto pg = yams::test_async::res(client.ping(), 5s);
    ASSERT_TRUE(pg) << pg.error().message;

    // List (empty store)
    yams::daemon::ListRequest lreq;
    lreq.limit = 5;
    auto lr = yams::test_async::res(client.list(lreq), 5s);
    ASSERT_TRUE(lr) << lr.error().message;
}
