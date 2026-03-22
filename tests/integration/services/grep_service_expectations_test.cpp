#include <gtest/gtest.h>

#ifdef _WIN32
#include <process.h>
#define getpid _getpid
#endif

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <thread>
#include <vector>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/system/error_code.hpp>

#include "common/test_data_generator.h"
#include <yams/app/services/document_ingestion_service.h>
#include <yams/app/services/factory.hpp>
#include <yams/app/services/retrieval_service.h>
#include <yams/app/services/services.hpp>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>

// Windows daemon IPC tests are unstable due to socket shutdown race conditions
#ifdef _WIN32
#define SKIP_DAEMON_TEST_ON_WINDOWS()                                                              \
    GTEST_SKIP() << "Daemon IPC tests unstable on Windows - see windows-daemon-ipc-plan.md"
#else
#define SKIP_DAEMON_TEST_ON_WINDOWS() ((void)0)
#endif

using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace {
size_t countOpenFds() {
    std::error_code ec;
    size_t count = 0;
    for (const auto& entry : fs::directory_iterator("/dev/fd", ec)) {
        (void)entry;
        ++count;
    }
    return count;
}
} // namespace

class GrepServiceExpectationsIT : public ::testing::Test {
protected:
    fs::path root_;
    fs::path storageDir_;
    fs::path socketPath_;
    std::unique_ptr<yams::daemon::YamsDaemon> daemon_;
    std::thread runLoopThread_;

    static bool canBindUnixSocketHere();

    bool waitForDocumentVisible(const std::string& hash,
                                std::chrono::milliseconds timeout = 5000ms) {
        auto* sm = daemon_->getServiceManager();
        auto ctx = sm->getAppContext();
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            auto doc = ctx.metadataRepo->getDocumentByHash(hash);
            if (doc && doc.value().has_value())
                return true;
            std::this_thread::sleep_for(50ms);
        }
        return false;
    }

    void SetUp() override {
        // Skip on Windows - daemon IPC tests are unstable there
        SKIP_DAEMON_TEST_ON_WINDOWS();

        if (!canBindUnixSocketHere()) {
            GTEST_SKIP() << "Skipping: AF_UNIX not available in this environment.";
        }
        GTEST_LOG_(INFO) << "GrepServiceExpectationsIT SetUp fds(before)=" << countOpenFds();
        yams::daemon::AsioConnectionPool::shutdown_all(std::chrono::milliseconds(500));
        yams::daemon::GlobalIOContext::reset();
        auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        root_ = fs::temp_directory_path() / ("yams_grep_it_" + unique);
        storageDir_ = root_ / "storage";
        fs::create_directories(storageDir_);
        fs::create_directories(root_ / "ingest");
        socketPath_ = fs::path("/tmp") / ("yams-grep-" + unique + ".sock");

        yams::daemon::DaemonConfig cfg;
        cfg.dataDir = storageDir_;
        cfg.socketPath = socketPath_;
        cfg.pidFile = root_ / "daemon.pid";
        cfg.logFile = root_ / "daemon.log";
        cfg.enableModelProvider = true;
        cfg.useMockModelProvider = true;
        daemon_ = std::make_unique<yams::daemon::YamsDaemon>(cfg);
        auto started = daemon_->start();
        ASSERT_TRUE(started) << started.error().message;

        // Start runLoop in background thread - CRITICAL for processing requests
        runLoopThread_ = std::thread([this]() { daemon_->runLoop(); });

        // Wait for daemon to reach Ready state (up to 15s for CI)
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
        while (std::chrono::steady_clock::now() < deadline) {
            auto lifecycle = daemon_->getLifecycle().snapshot();
            if (lifecycle.state == yams::daemon::LifecycleState::Ready) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        // Additional settling time
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        GTEST_LOG_(INFO) << "GrepServiceExpectationsIT SetUp fds(after)=" << countOpenFds();
    }

    void TearDown() override {
        GTEST_LOG_(INFO) << "GrepServiceExpectationsIT TearDown fds(before)=" << countOpenFds();
        if (daemon_) {
            daemon_->stop();
            if (runLoopThread_.joinable()) {
                runLoopThread_.join();
            }
        }
        // Ensure global client pools are torn down between tests to avoid FD leaks across
        // long integration runs.
        yams::daemon::AsioConnectionPool::shutdown_all(std::chrono::milliseconds(500));
        yams::daemon::GlobalIOContext::reset();
        std::error_code ec;
        fs::remove_all(root_, ec);
        GTEST_LOG_(INFO) << "GrepServiceExpectationsIT TearDown fds(after)=" << countOpenFds();
    }
};

bool GrepServiceExpectationsIT::canBindUnixSocketHere() {
    try {
        boost::asio::io_context io;
        boost::asio::local::stream_protocol::acceptor acceptor(io);
        auto path = std::filesystem::path("/tmp") /
                    (std::string("yams-grep-probe-") + std::to_string(::getpid()) + ".sock");
        std::error_code ec;
        std::filesystem::remove(path, ec);
        boost::system::error_code bec;
        acceptor.open(boost::asio::local::stream_protocol::endpoint(path.string()).protocol(), bec);
        if (bec)
            return false;
        acceptor.bind(boost::asio::local::stream_protocol::endpoint(path.string()), bec);
        if (bec)
            return false;
        acceptor.close();
        std::filesystem::remove(path, ec);
        return true;
    } catch (...) {
        return false;
    }
}
