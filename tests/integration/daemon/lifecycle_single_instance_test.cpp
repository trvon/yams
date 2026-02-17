// Data-dir single-instance enforcement integration tests
// Validates that two daemons cannot share the same data-dir

#define CATCH_CONFIG_MAIN
#include <spdlog/spdlog.h>
#include <boost/asio/io_context.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/system/error_code.hpp>
#ifndef _WIN32
#include <fcntl.h>
#include <sys/file.h>
#endif
#include <filesystem>
#include <fstream>
#include <memory>
#include <random>
#include <thread>
#include "test_async_helpers.h"
#include "test_daemon_harness.h"
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>
#include <yams/compat/unistd.h>

using namespace yams::daemon;
using namespace yams::test;
using namespace std::chrono_literals;

namespace {

// Custom harness that allows specifying a shared data directory
class SharedDataDirHarness {
public:
    explicit SharedDataDirHarness(const std::filesystem::path& sharedDataDir,
                                  const std::string& suffix = "")
        : sharedDataDir_(sharedDataDir) {
        namespace fs = std::filesystem;
        auto id = random_id() + suffix;

        // Use shared data-dir but unique socket/pid paths
#ifdef _WIN32
        sock_ = fs::temp_directory_path() / ("daemon_shared_" + id + ".sock");
#else
        sock_ = std::filesystem::path("/tmp") / ("daemon_shared_" + id + ".sock");
#endif
        auto root = fs::temp_directory_path() / ("yams_shared_" + id);
        fs::create_directories(root);
        pid_ = root / ("daemon_" + id + ".pid");
        log_ = root / ("daemon_" + id + ".log");
        root_ = root;

        // Ensure shared data dir exists
        fs::create_directories(sharedDataDir_);
    }

    ~SharedDataDirHarness() {
        stop();
        cleanup();
    }

    bool start(std::chrono::milliseconds timeout = 10s) {
        DaemonConfig cfg;
        cfg.dataDir = sharedDataDir_; // Use shared data directory
        cfg.socketPath = sock_;
        cfg.pidFile = pid_;
        cfg.logFile = log_;
        cfg.enableModelProvider = false;
        cfg.useMockModelProvider = false;
        cfg.autoLoadPlugins = false;

        daemon_ = std::make_unique<YamsDaemon>(cfg);

        auto s = daemon_->start();
        if (!s) {
            spdlog::error("[SharedDataDirHarness] daemon_->start() failed: {}", s.error().message);
            startError_ = s.error().message;
            return false;
        }

        runLoopThread_ = std::thread([this]() { daemon_->runLoop(); });

        // Poll for usable lifecycle and socket acceptance.
        auto deadline = std::chrono::steady_clock::now() + timeout;
        std::string lastStatus = "not probed";
        while (std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(100ms);
            auto lifecycle = daemon_->getLifecycle().snapshot();
            if (lifecycle.state == LifecycleState::Ready ||
                lifecycle.state == LifecycleState::Degraded) {
                if (socketResponsive(&lastStatus)) {
                    spdlog::info("[SharedDataDirHarness] Daemon usable at socket: {} "
                                 "(lifecycle={}, probe='{}')",
                                 sock_.string(), static_cast<int>(lifecycle.state), lastStatus);
                    return true;
                }
            } else if (lifecycle.state == LifecycleState::Failed) {
                spdlog::error("[SharedDataDirHarness] Daemon failed: {}", lifecycle.lastError);
                return false;
            }
        }
        auto lifecycle = daemon_->getLifecycle().snapshot();
        spdlog::error("[SharedDataDirHarness] Timeout waiting for usable daemon state/socket "
                      "(lifecycle={}, lastProbe='{}')",
                      static_cast<int>(lifecycle.state), lastStatus);
        return false;
    }

    void stop() {
        if (daemon_) {
            daemon_->stop();
            if (runLoopThread_.joinable()) {
                runLoopThread_.join();
            }
            yams::daemon::AsioConnectionPool::shutdown_all(500ms);
#ifndef _WIN32
            yams::daemon::GlobalIOContext::safe_restart();
#endif
            daemon_.reset();

            auto deadline = std::chrono::steady_clock::now() + 2s;
            while (std::filesystem::exists(sock_) && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(20ms);
            }
        }
    }

    const std::filesystem::path& socketPath() const { return sock_; }
    const std::filesystem::path& dataDir() const { return sharedDataDir_; }
    const std::string& startError() const { return startError_; }
    YamsDaemon* daemon() const { return daemon_.get(); }

private:
    bool socketResponsive(std::string* statusOut) const {
        boost::asio::io_context io;
        boost::asio::local::stream_protocol::socket socket(io);
        boost::system::error_code ec;
        socket.connect(boost::asio::local::stream_protocol::endpoint(sock_.string()), ec);
        if (ec) {
            if (statusOut) {
                *statusOut = "connect failed: " + ec.message();
            }
            return false;
        }
        if (statusOut) {
            *statusOut = "socket-accept-ok";
        }
        return true;
    }

    static std::string random_id() {
        static const char* cs = "abcdefghijklmnopqrstuvwxyz0123456789";
        thread_local std::mt19937_64 rng{std::random_device{}()};
        std::uniform_int_distribution<size_t> dist(0, 35);
        std::string out;
        out.reserve(8);
        for (int i = 0; i < 8; ++i)
            out.push_back(cs[dist(rng)]);
        return out;
    }

    void cleanup() {
        namespace fs = std::filesystem;
        std::error_code ec;
        if (!root_.empty())
            fs::remove_all(root_, ec);
    }

    std::unique_ptr<YamsDaemon> daemon_;
    std::thread runLoopThread_;
    std::filesystem::path sharedDataDir_;
    std::filesystem::path root_, sock_, pid_, log_;
    std::string startError_;
};

} // namespace

TEST_CASE("Data-dir lock prevents concurrent access", "[daemon][lifecycle][single-instance]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    // Create a shared data directory for both daemons
    auto sharedDataDir =
        std::filesystem::temp_directory_path() / ("yams_shared_data_" + std::to_string(getpid()));
    std::filesystem::create_directories(sharedDataDir);

    // Cleanup on exit
    struct Cleanup {
        std::filesystem::path dir;
        ~Cleanup() {
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
        }
    } cleanup{sharedDataDir};

    SECTION("second daemon starts after first releases lock") {
        // NOTE: Two YamsDaemon instances cannot safely coexist in the same
        // process because they share ~14 global singletons (GlobalIOContext,
        // InternalEventBus, TuneAdvisor, ResourceGovernor, etc.) with no
        // per-instance isolation.  The "takeover via shutdown RPC" path works
        // correctly across separate OS processes because flock + waitForProcessExit
        // serialise the transition.  In-process, the race between daemon1's
        // teardown and daemon2's init over those singletons causes SIGSEGV.
        //
        // We therefore test sequential takeover: daemon1 starts, we verify
        // it holds the data-dir lock, stop it, then verify daemon2 can start
        // on the same data-dir.  This exercises the same lock-acquire path
        // (acquireDataDirLock) without overlapping two daemon lifetimes.

        auto lockFile = sharedDataDir / ".yams-lock";

        // --- Phase 1: daemon1 acquires the data-dir lock ---
        {
            SharedDataDirHarness harness1(sharedDataDir, "_first");
            REQUIRE(harness1.start());
            REQUIRE(std::filesystem::exists(harness1.socketPath()));
            REQUIRE(std::filesystem::exists(lockFile));

            // Verify lock file contains daemon1 info
            {
                std::ifstream f(lockFile);
                std::string content((std::istreambuf_iterator<char>(f)),
                                    std::istreambuf_iterator<char>());
                REQUIRE(content.find("\"pid\"") != std::string::npos);
                REQUIRE(content.find("\"socket\"") != std::string::npos);
                REQUIRE(content.find(harness1.socketPath().string()) != std::string::npos);
            }

            // Verify that flock is actually held (non-blocking attempt must fail)
#ifndef _WIN32
            {
                int probe_fd = open(lockFile.c_str(), O_RDONLY);
                REQUIRE(probe_fd >= 0);
                int rc = flock(probe_fd, LOCK_EX | LOCK_NB);
                // rc should be -1/EWOULDBLOCK since daemon1 holds the lock
                CHECK(rc == -1);
                close(probe_fd);
            }
#endif

            harness1.stop();
        }

        // Wait for socket/lock cleanup to propagate
        std::this_thread::sleep_for(300ms);

        // --- Phase 2: daemon2 acquires the same data-dir lock ---
        {
            SharedDataDirHarness harness2(sharedDataDir, "_second");
            REQUIRE(harness2.start(15s));
            REQUIRE(std::filesystem::exists(harness2.socketPath()));

            // Lock file should now reference daemon2
            {
                std::ifstream f(lockFile);
                std::string content((std::istreambuf_iterator<char>(f)),
                                    std::istreambuf_iterator<char>());
                REQUIRE(content.find(harness2.socketPath().string()) != std::string::npos);
            }

            harness2.stop();
        }
    }

    SECTION("lock file is created in data-dir") {
        SharedDataDirHarness harness(sharedDataDir, "_lock_test");
        REQUIRE(harness.start());

        // Verify lock file exists
        auto lockFile = sharedDataDir / ".yams-lock";
        REQUIRE(std::filesystem::exists(lockFile));

        // Verify lock file contains valid JSON with our PID
        std::ifstream f(lockFile);
        std::string content((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
        REQUIRE(!content.empty());
        REQUIRE(content.find("\"pid\"") != std::string::npos);
        REQUIRE(content.find("\"socket\"") != std::string::npos);

        harness.stop();
    }
}

TEST_CASE("Data-dir lock released on shutdown", "[daemon][lifecycle][single-instance]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    auto sharedDataDir =
        std::filesystem::temp_directory_path() / ("yams_release_test_" + std::to_string(getpid()));
    std::filesystem::create_directories(sharedDataDir);

    struct Cleanup {
        std::filesystem::path dir;
        ~Cleanup() {
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
        }
    } cleanup{sharedDataDir};

    SECTION("new daemon can start after previous shutdown") {
        {
            SharedDataDirHarness harness1(sharedDataDir, "_release1");
            REQUIRE(harness1.start());
            harness1.stop();
        }

        // Allow time for cleanup
        std::this_thread::sleep_for(500ms);

        {
            SharedDataDirHarness harness2(sharedDataDir, "_release2");
            // Should start immediately without needing takeover
            REQUIRE(harness2.start(10s));
            harness2.stop();
        }
    }
}
