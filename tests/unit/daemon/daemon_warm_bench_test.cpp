#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <gtest/gtest.h>
#include <yams/daemon/daemon.h>

namespace yams::daemon::bench {

namespace fs = std::filesystem;
using namespace std::chrono_literals;

static bool isSocketPermissionDenied(const yams::Error& error) {
    const std::string_view message{error.message};
    return message.find("Operation not permitted") != std::string_view::npos ||
           message.find("Permission denied") != std::string_view::npos;
}

TEST(DaemonBench, WarmLatencyFast) {
    // Only run when explicitly requested (bench suite or local env)
    const char* fast = ::getenv("YAMS_BENCH_FAST");
    const char* enable = ::getenv("YAMS_ENABLE_DAEMON_BENCH");
    if (!fast || std::string_view(fast) != "1" || !enable || std::string_view(enable) != "1") {
        GTEST_SKIP()
            << "Bench disabled (set YAMS_BENCH_FAST=1 and YAMS_ENABLE_DAEMON_BENCH=1 to run)";
    }

    fs::path runtime_root =
        fs::temp_directory_path() / ("ydbench_" + std::to_string(::getpid()) + "_" +
                                     std::to_string(static_cast<unsigned long>(
                                         reinterpret_cast<uintptr_t>(&runtime_root) & 0xffff)));
    DaemonConfig cfg;
    cfg.dataDir = runtime_root / "data";
    cfg.socketPath = runtime_root / "sock";
    cfg.pidFile = runtime_root / "daemon.pid";
    cfg.logFile = runtime_root / "daemon.log";
    cfg.workerThreads = 1;
    cfg.maxMemoryGb = 0.5;
    cfg.enableModelProvider = false; // keep startup minimal
    cfg.autoLoadPlugins = false;

    // Tighten init timeouts and avoid vector DB
    ::setenv("YAMS_DB_OPEN_TIMEOUT_MS", "1000", 1);
    ::setenv("YAMS_DB_MIGRATE_TIMEOUT_MS", "1500", 1);
    ::setenv("YAMS_SEARCH_BUILD_TIMEOUT_MS", "1000", 1);
    ::setenv("YAMS_DISABLE_VECTORS", "1", 1);

    std::error_code se;
    fs::create_directories(cfg.dataDir, se);

    // Single start/stop timing as a smoke warm benchmark
    auto t0 = std::chrono::steady_clock::now();

    std::unique_ptr<YamsDaemon> d = std::make_unique<YamsDaemon>(cfg);
    auto start = d->start();
    if (!start) {
        if (isSocketPermissionDenied(start.error())) {
            GTEST_SKIP() << "Skipping: UNIX domain sockets not permitted: "
                         << start.error().message;
        }
        FAIL() << "Failed to start daemon: " << start.error().message;
    }

    // Simple readiness wait; avoid IPC. Sleep briefly to simulate warm path.
    std::this_thread::sleep_for(50ms);
    d->stop();

    auto t1 = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

    // Guardrail: should complete within a reasonable bound in CI.
    // Keep assertion lenient to avoid flakes across machines.
    EXPECT_LT(ms, 5000) << "Warm start/stop exceeded 5s: " << ms << "ms";

    // Cleanup
    std::error_code ec;
    fs::remove_all(runtime_root, ec);
}

} // namespace yams::daemon::bench
