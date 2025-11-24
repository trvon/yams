// Integration harness preflight: ensure daemon operations are test-safe.
#pragma once
#include <cstdlib>
#include <filesystem>
#include <string>
#include <yams/compat/unistd.h>

namespace yams::tests::harnesses {
struct DaemonPreflightConfig {
    std::filesystem::path runtime_dir; // directory for pid/socket
    std::string socket_name_prefix{"yams-test-sock-"};
    bool kill_others{false}; // never kill user/system daemons in tests
};

struct DaemonPreflight {
    static void ensure_environment(const DaemonPreflightConfig& cfg) {
        // 1) Prevent tests from stopping user/system daemons.
        ::setenv("YAMS_DAEMON_KILL_OTHERS", cfg.kill_others ? "1" : "0", 1);

        // 2) Ensure a unique runtime dir per test process. Keep AF_UNIX path short.
        std::error_code ec;
        auto pid = static_cast<unsigned long>(::getpid());
        auto runtime_root = cfg.runtime_dir;
        auto unique_dir = runtime_root / (std::string("run-") + std::to_string(pid));
        std::filesystem::create_directories(unique_dir, ec);

        auto make_paths = [&](const std::filesystem::path& dir) {
            auto sock = (dir / (cfg.socket_name_prefix + std::to_string(pid) + ".sock")).string();
            auto pidf = (dir / "yamsd.pid").string();
            return std::pair<std::string, std::string>(sock, pidf);
        };
        auto [socket_path, pid_path] = make_paths(unique_dir);

#if defined(__APPLE__) || defined(__unix__)
        // macOS/Linux AF_UNIX sun_path is typically 104-108 bytes. If too long, fall back to /tmp.
        const size_t kUnixMax = 100; // keep margin
        if (socket_path.size() > kUnixMax) {
            runtime_root = std::filesystem::path("/tmp");
            unique_dir = runtime_root / (std::string("yamsr-") + std::to_string(pid));
            std::filesystem::create_directories(unique_dir, ec);
            auto p = make_paths(unique_dir);
            socket_path = p.first;
            pid_path = p.second;
        }
#endif
        ::setenv("YAMS_RUNTIME_DIR", unique_dir.string().c_str(), 1);
        ::setenv("YAMS_SOCKET_PATH", socket_path.c_str(), 1);
        ::setenv("YAMS_PID_FILE", pid_path.c_str(), 1);
    }

    static void post_test_cleanup(const std::filesystem::path& runtime_root) {
        std::error_code ec;
        std::filesystem::remove_all(runtime_root, ec);
    }
};
} // namespace yams::tests::harnesses
