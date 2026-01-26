// Lightweight RAII harness to start/stop a YamsDaemon for integration tests
#pragma once

#include <spdlog/spdlog.h>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include "test_async_helpers.h"
#include <yams/compat/unistd.h>
#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/daemon.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/local/stream_protocol.hpp>

// Windows daemon IPC tests are currently unstable due to socket shutdown race conditions
// The daemon's connection handler coroutines crash during cleanup when sockets are forcibly closed
// See: docs/developer/windows-daemon-ipc-plan.md
#ifdef _WIN32
#define SKIP_DAEMON_TEST_ON_WINDOWS()                                                              \
    SKIP("Daemon IPC tests unstable on Windows - see windows-daemon-ipc-plan.md")
#else
#define SKIP_DAEMON_TEST_ON_WINDOWS() ((void)0)
#endif

namespace yams::test {

// Options struct must be defined outside DaemonHarness to avoid Clang issue with
// default member initializers in nested structs used as default function arguments.
// See: https://bugs.llvm.org/show_bug.cgi?id=36684
struct DaemonHarnessOptions {
    bool enableModelProvider = true;
    bool useMockModelProvider = true;
    bool autoLoadPlugins = false;
    bool isolateState = false;
    bool skipSocketVerificationOnReady = false;
    bool configureModelPool = false;
    bool modelPoolLazyLoading = true;
    std::vector<std::string> preloadModels;
    std::optional<std::filesystem::path> pluginDir;
    // Per-plugin configuration: plugin name -> JSON config string
    std::map<std::string, std::string> pluginConfigs;
};

class DaemonHarness {
public:
    using Options = DaemonHarnessOptions;

    explicit DaemonHarness(Options options = Options()) : options_(std::move(options)) {
        namespace fs = std::filesystem;
        // Ensure mock provider and disable ABI plugins for stability
        // Create temp working dir
        auto id = random_id();
        root_ = fs::temp_directory_path() / (std::string("yams_it_") + id);
        fs::create_directories(root_);
        data_ = root_ / "data";
        fs::create_directories(data_);
        // Use platform-appropriate temp path for socket
        // On Windows, AF_UNIX sockets work but need a valid Windows path
        // On Unix, use /tmp for short paths to avoid AF_UNIX length limits
#ifdef _WIN32
        sock_ = fs::temp_directory_path() / ("daemon_" + id + ".sock");
#else
        sock_ = std::filesystem::path("/tmp") / ("daemon_" + id + ".sock");
#endif
        pid_ = root_ / ("daemon_" + id + ".pid");
        log_ = root_ / ("daemon_" + id + ".log");

        // NOTE: Do NOT create daemon instance in constructor!
        // Creating YamsDaemon initializes ServiceManager which spawns worker threads.
        // If start() is never called but destructor runs, thread cleanup can crash.
        // Defer daemon creation until start() is explicitly called.
    }

    ~DaemonHarness() {
        // Explicitly stop daemon before cleanup to avoid crashes
        stop();
        cleanup();
    }

    // Pre-runLoop callback type: called after daemon->start() succeeds but before runLoop thread
    // starts. Use this to set signal check hooks or external shutdown flags BEFORE the runLoop
    // begins reading them, avoiding race conditions.
    using PreRunLoopCallback = std::function<void(yams::daemon::YamsDaemon*)>;

    bool start(std::chrono::milliseconds timeout = std::chrono::seconds(10),
               PreRunLoopCallback preRunLoopCallback = nullptr) {
        // Always create new daemon instance on start()
        // (daemon cannot be restarted after stop() - must create new instance)
        spdlog::info("[DaemonHarness] Creating new daemon instance...");

        if (options_.isolateState) {
            originalCwd_ = std::filesystem::current_path();
            std::filesystem::current_path(root_);
            originalXdgState_ = std::getenv("XDG_STATE_HOME") ? std::getenv("XDG_STATE_HOME") : "";
            setenv("XDG_STATE_HOME", (root_ / "state").string().c_str(), 1);
            std::filesystem::create_directories(root_ / "state" / "yams" / "sessions");
            isolateStateActive_ = true;
        }

        yams::daemon::DaemonConfig cfg;
        cfg.dataDir = data_;
        cfg.socketPath = sock_;
        cfg.pidFile = pid_;
        cfg.logFile = log_;
        cfg.enableModelProvider = options_.enableModelProvider;
        cfg.autoLoadPlugins = options_.autoLoadPlugins;
        cfg.useMockModelProvider = options_.useMockModelProvider;
        if (options_.pluginDir) {
            cfg.pluginDir = *options_.pluginDir;
        }
        if (options_.configureModelPool) {
            cfg.modelPoolConfig.lazyLoading = options_.modelPoolLazyLoading;
            cfg.modelPoolConfig.preloadModels = options_.preloadModels;
        }
        // Pass plugin-specific configurations
        cfg.pluginConfigs = options_.pluginConfigs;
        daemon_ = std::make_unique<yams::daemon::YamsDaemon>(cfg);
        spdlog::info("[DaemonHarness] Daemon instance created");

        spdlog::info("[DaemonHarness] Calling daemon_->start()...");
        auto s = daemon_->start();
        if (!s) {
            spdlog::error("[DaemonHarness] daemon_->start() failed!");
            return false;
        }
        spdlog::info("[DaemonHarness] daemon_->start() succeeded, starting runLoop thread...");

        // Call pre-runLoop callback if provided (for setting up signal hooks, etc.)
        // This runs BEFORE the runLoop thread starts, avoiding race conditions.
        if (preRunLoopCallback) {
            spdlog::info("[DaemonHarness] Invoking pre-runLoop callback...");
            preRunLoopCallback(daemon_.get());
        }

        // Start runLoop in background thread - this triggers async initialization
        // Without runLoop(), ServiceManager::startAsyncInit() is never called
        runLoopThread_ = std::thread([this]() { daemon_->runLoop(); });
        // Give runLoop time to enter and trigger async init
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        spdlog::info("[DaemonHarness] runLoop thread started, polling for daemon Ready state...");

        // Poll for daemon to reach Ready state in lifecycle FSM
        // This ensures ServiceManager has completed initialization, not just socket availability
        auto deadline = std::chrono::steady_clock::now() + timeout;
        bool lifecycleReady = false;

        while (std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            // Check lifecycle state first
            auto lifecycle = daemon_->getLifecycle().snapshot();
            if (lifecycle.state == yams::daemon::LifecycleState::Ready) {
                lifecycleReady = true;
                spdlog::info("[DaemonHarness] Daemon lifecycle reached Ready state");
                break;
            } else if (lifecycle.state == yams::daemon::LifecycleState::Failed) {
                spdlog::error("[DaemonHarness] Daemon lifecycle reached Failed state: {}",
                              lifecycle.lastError);
                return false;
            }
        }

        if (!lifecycleReady) {
            auto lifecycle = daemon_->getLifecycle().snapshot();
            spdlog::error(
                "[DaemonHarness] Timeout waiting for Ready state after {}ms (current state: {})",
                timeout.count(), static_cast<int>(lifecycle.state));
            return false;
        }

        // Verify socket connectivity as final sanity check
        // Use simple socket connection instead of DaemonClient to avoid polluting
        // the shared connection pool (DaemonClient destructor shuts down shared pools)
        if (options_.skipSocketVerificationOnReady) {
            spdlog::info(
                "[DaemonHarness] Skipping socket verification after Ready (test override)");
            return true;
        }

        spdlog::info("[DaemonHarness] Verifying socket connectivity at: {}", sock_.string());
        bool socketOk = verifySocketConnectivity(sock_, std::chrono::seconds(5));
        if (socketOk) {
            spdlog::info("[DaemonHarness] Socket connection verified, daemon fully ready at: {}",
                         sock_.string());
            return true;
        }
        spdlog::error("[DaemonHarness] Daemon Ready but socket connection failed at: {}",
                      sock_.string());
        return false;
    }

    void stop() {
        // Join runLoop thread first (daemon->stop() will cause runLoop() to return)
        if (runLoopThread_.joinable()) {
            spdlog::info("[DaemonHarness] Stopping runLoop thread...");
        }

        if (daemon_) {
            spdlog::info("[DaemonHarness] Stopping daemon (running={})...", daemon_->isRunning());

            // Wait for daemon to fully stop - it handles GlobalIOContext::reset() internally
            auto stopResult = daemon_->stop();
            if (!stopResult) {
                spdlog::warn("[DaemonHarness] Daemon stop returned error: {}",
                             stopResult.error().message);
            }

            // Join runLoop thread after stop() - daemon->stop() causes runLoop() to return
            if (runLoopThread_.joinable()) {
                spdlog::info("[DaemonHarness] Joining runLoop thread...");
                runLoopThread_.join();
                spdlog::info("[DaemonHarness] runLoop thread joined");
            }

            // Wait for daemon to report it's no longer running
            int isRunningRetries = 0;
            while (daemon_->isRunning() && isRunningRetries < 50) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                isRunningRetries++;
            }
            if (isRunningRetries > 0) {
                spdlog::info("[DaemonHarness] Waited {}ms for isRunning() to become false",
                             isRunningRetries * 10);
            }

            spdlog::info("[DaemonHarness] Daemon stopped (running={}), resetting instance...",
                         daemon_->isRunning());

            // Reset GlobalIOContext to clean up threads and io_context state
            // This is critical for test isolation - without it, threads accumulate across test
            // cases Temporarily unset YAMS_TESTING to allow reset() to actually work
            const char* yams_testing = std::getenv("YAMS_TESTING");
            const char* yams_safe = std::getenv("YAMS_TEST_SAFE_SINGLE_INSTANCE");
            if (yams_testing) {
                unsetenv("YAMS_TESTING");
            }
            if (yams_safe) {
                unsetenv("YAMS_TEST_SAFE_SINGLE_INSTANCE");
            }

            // Shutdown all connection pools BEFORE resetting GlobalIOContext
            // This ensures all client-side connections are properly closed while
            // the io_context is still running, preventing stale connections in
            // subsequent test sections that create new daemons
            yams::daemon::AsioConnectionPool::shutdown_all(std::chrono::milliseconds(500));
            spdlog::info("[DaemonHarness] Connection pools shut down");

            // Windows needs additional time before GlobalIOContext::reset()
            // Windows thread cleanup is slower than Unix
#ifdef _WIN32
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            // Skip GlobalIOContext operations on Windows for now - causes SIGSEGV
            // The io_context threads will be cleaned up when the process exits
            spdlog::info("[DaemonHarness] Skipping GlobalIOContext::restart() on Windows");
#else
            // Use safe_restart() instead of restart() to handle teardown race conditions.
            // safe_restart() checks is_destroyed() before attempting restart, avoiding
            // SIGSEGV when static destruction is in progress.
            yams::daemon::GlobalIOContext::safe_restart();
            spdlog::info("[DaemonHarness] GlobalIOContext safe_restart complete");
#endif

            // Restore environment variables
            if (yams_testing) {
                setenv("YAMS_TESTING", yams_testing, 1);
            }
            if (yams_safe) {
                setenv("YAMS_TEST_SAFE_SINGLE_INSTANCE", yams_safe, 1);
            }

            if (isolateStateActive_) {
                std::error_code ec;
                if (!originalCwd_.empty()) {
                    std::filesystem::current_path(originalCwd_, ec);
                }
                if (originalXdgState_.empty()) {
                    unsetenv("XDG_STATE_HOME");
                } else {
                    setenv("XDG_STATE_HOME", originalXdgState_.c_str(), 1);
                }
                isolateStateActive_ = false;
            }

            // Reset daemon so it can be recreated on next start()
            daemon_.reset();

            // CRITICAL: Allow OS to fully release thread resources (macOS needs this)
            // Each daemon creates ~48 threads (32 SocketServer + 16 ServiceManager).
            // macOS has strict per-process thread creation rate limits and needs time
            // to reclaim thread resources before next daemon starts creating threads.
            // Testing shows: 250ms → crashes at daemon #6, 500ms allows ~10 daemons, 750ms more
            // conservative. Increased to 750ms for improved stability on resource-constrained CI
            // runners.
            std::this_thread::sleep_for(std::chrono::milliseconds(
                750)); // Wait for socket file to be removed by daemon shutdown
            int socketRetries = 0;
            while (std::filesystem::exists(sock_) && socketRetries < 50) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                socketRetries++;
            }

            if (socketRetries > 0) {
                spdlog::info("[DaemonHarness] Waited {}ms for socket file removal",
                             socketRetries * 20);
            }

            // Verify socket is truly gone
            if (std::filesystem::exists(sock_)) {
                spdlog::warn("[DaemonHarness] Socket file still exists after {}ms",
                             socketRetries * 20);
            }

            // Additional brief wait to ensure OS fully releases socket
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            spdlog::info(
                "[DaemonHarness] Stop complete (isRunning check: {}ms, socket cleanup: {}ms)",
                isRunningRetries * 10, socketRetries * 20);
        }
    }

    const std::filesystem::path& socketPath() const { return sock_; }
    const std::filesystem::path& dataDir() const { return data_; }
    const std::filesystem::path& rootDir() const { return root_; }
    yams::daemon::YamsDaemon* daemon() const { return daemon_.get(); }

private:
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

    // Simple socket connectivity check that doesn't use the shared connection pool
    // This avoids DaemonClient's destructor from shutting down the pool before
    // the actual test fixture's client can use it
    static bool verifySocketConnectivity(const std::filesystem::path& socketPath,
                                         std::chrono::milliseconds timeout) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            try {
                // Create fresh socket for each attempt (can't reuse after failed connect)
                boost::asio::io_context io;
                boost::asio::local::stream_protocol::socket sock(io);
                boost::system::error_code ec;

                sock.connect(boost::asio::local::stream_protocol::endpoint(socketPath.string()),
                             ec);
                if (!ec) {
                    sock.close();
                    return true;
                }
            } catch (const std::exception& e) {
                spdlog::debug("[DaemonHarness] Socket verification attempt failed: {}", e.what());
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        spdlog::error("[DaemonHarness] Socket verification timed out after {}ms", timeout.count());
        return false;
    }
    void cleanup() {
        namespace fs = std::filesystem;
        std::error_code ec;
        if (!root_.empty())
            fs::remove_all(root_, ec);
    }

    std::unique_ptr<yams::daemon::YamsDaemon> daemon_;
    std::thread runLoopThread_; // Background thread for runLoop
    std::filesystem::path root_, data_, sock_, pid_, log_;
    std::filesystem::path originalCwd_;
    std::string originalXdgState_;
    bool isolateStateActive_ = false;
    Options options_;
};

} // namespace yams::test
