#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include "../common/env_compat.h"
#include <yams/compat/unistd.h>
#include <yams/daemon/daemon.h>

namespace fs = std::filesystem;
using steady_clock_t = std::chrono::steady_clock;
using namespace std::chrono_literals;

static bool isSocketPermissionDenied(const yams::Error& error) {
    const std::string_view message{error.message};
    return message.find("Operation not permitted") != std::string_view::npos ||
           message.find("Permission denied") != std::string_view::npos;
}

int main() {
    const char* enable = std::getenv("YAMS_ENABLE_DAEMON_BENCH");
    if (!enable || std::string_view(enable) != "1") {
        std::cout << "Skipping daemon warm latency bench (set YAMS_ENABLE_DAEMON_BENCH=1)\n";
        return 0; // treat skip as pass for Meson runner
    }

    fs::path runtime_root =
        fs::temp_directory_path() / ("ydbench_" + std::to_string(::getpid()) + "_" +
                                     std::to_string(static_cast<unsigned long>(
                                         reinterpret_cast<uintptr_t>(&runtime_root) & 0xffff)));

    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = runtime_root / "data";
    cfg.socketPath = runtime_root / "sock";
    cfg.pidFile = runtime_root / "daemon.pid";
    cfg.logFile = runtime_root / "daemon.log";
    cfg.maxMemoryGb = 1;
    cfg.enableModelProvider = false;
    cfg.autoLoadPlugins = false;

    // Tighten init timeouts and avoid vector DB
    ::setenv("YAMS_DB_OPEN_TIMEOUT_MS", "1000", 1);
    ::setenv("YAMS_DB_MIGRATE_TIMEOUT_MS", "1500", 1);
    ::setenv("YAMS_SEARCH_BUILD_TIMEOUT_MS", "1000", 1);
    ::setenv("YAMS_DISABLE_VECTORS", "1", 1);

    std::error_code se;
    fs::create_directories(cfg.dataDir, se);

    auto t0 = steady_clock_t::now();

    auto daemon = std::make_unique<yams::daemon::YamsDaemon>(cfg);
    auto start = daemon->start();
    if (!start) {
        if (isSocketPermissionDenied(start.error())) {
            std::cout << "Skipping: UNIX domain sockets not permitted: " << start.error().message
                      << "\n";
            return 0;
        }
        std::cerr << "Failed to start daemon: " << start.error().message << "\n";
        return 1;
    }

    std::this_thread::sleep_for(50ms);
    daemon->stop();

    auto t1 = steady_clock_t::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    std::cout << "Warm start/stop: " << ms << " ms\n";

    std::error_code ec;
    fs::remove_all(runtime_root, ec);

    if (ms >= 5000) {
        std::cerr << "Warm start/stop exceeded 5s: " << ms << " ms\n";
        return 2;
    }
    return 0;
}
