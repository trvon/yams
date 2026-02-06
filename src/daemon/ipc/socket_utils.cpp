#include <yams/daemon/ipc/socket_utils.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string_view> // PBI-058 Task 058-16

#ifndef _WIN32
#include <unistd.h>
#endif

namespace yams::daemon::socket_utils {

namespace fs = std::filesystem;

static inline fs::path get_xdg_runtime_dir() {
    if (const char* p = std::getenv("XDG_RUNTIME_DIR")) {
        if (*p)
            return fs::path(p);
    }
    return {};
}

static inline bool can_write_dir(const fs::path& dir) {
    std::error_code ec;
    if (!fs::exists(dir, ec))
        return false;
    auto probe = dir / ".yams-writable-probe";
    std::ofstream f(probe);
    if (!f.good())
        return false;
    f << "ok";
    f.close();
    fs::remove(probe, ec);
    return true;
}

std::filesystem::path resolve_socket_path() {
#ifdef _WIN32
    // 1) Explicit environment override
    if (const char* env = std::getenv("YAMS_DAEMON_SOCKET")) {
        if (*env)
            return fs::path(env);
    }
    // 2) Use LOCALAPPDATA/yams or temp directory
    if (const char* localAppData = std::getenv("LOCALAPPDATA")) {
        auto yamDir = fs::path(localAppData) / "yams";
        std::error_code ec;
        fs::create_directories(yamDir, ec);
        if (can_write_dir(yamDir))
            return yamDir / "yams-daemon.sock";
    }
    return fs::temp_directory_path() / "yams-daemon.sock";
#else
    // 1) Explicit environment override
    if (const char* env = std::getenv("YAMS_DAEMON_SOCKET")) {
        if (*env)
            return fs::path(env);
    }
    // 2) Root vs user defaults
    bool is_root = (::geteuid() == 0);
    if (is_root) {
        return fs::path("/var/run/yams-daemon.sock");
    }
    // 3) XDG_RUNTIME_DIR or /tmp per-user fallback
    if (auto xdg = get_xdg_runtime_dir(); !xdg.empty() && can_write_dir(xdg)) {
        return xdg / "yams-daemon.sock";
    }
    uid_t uid = ::getuid();
    return fs::path("/tmp") / ("yams-daemon-" + std::to_string(uid) + ".sock");
#endif
}

std::filesystem::path resolve_socket_path_config_first() {
    // 1) Env override wins (all platforms)
    if (const char* env = std::getenv("YAMS_DAEMON_SOCKET")) {
        if (*env)
            return fs::path(env);
    }
    // 2) config.toml if present
    try {
        fs::path cfgPath;
#ifdef _WIN32
        // Windows: APPDATA/yams/config.toml
        if (const char* appData = std::getenv("APPDATA")) {
            cfgPath = fs::path(appData) / "yams" / "config.toml";
        }
#else
        if (const char* xdg = std::getenv("XDG_CONFIG_HOME")) {
            cfgPath = fs::path(xdg) / "yams" / "config.toml";
        } else if (const char* home = std::getenv("HOME")) {
            cfgPath = fs::path(home) / ".config" / "yams" / "config.toml";
        }
#endif
        std::error_code cfgEc;
        if (!cfgPath.empty() && fs::exists(cfgPath, cfgEc)) {
            std::ifstream in(cfgPath);
            if (in) {
                std::string line;
                bool inDaemon = false;

                // String view trim helper - avoids unnecessary allocations (PBI-058 Task 058-16)
                auto trim = [](std::string_view s) -> std::string_view {
                    auto issp = [](unsigned char c) { return std::isspace(c); };
                    // Trim leading whitespace
                    auto start =
                        std::find_if(s.begin(), s.end(), [&](unsigned char c) { return !issp(c); });
                    if (start == s.end())
                        return {};
                    // Trim trailing whitespace
                    auto end = std::find_if(s.rbegin(), s.rend(), [&](unsigned char c) {
                                   return !issp(c);
                               }).base();
                    return s.substr(start - s.begin(), end - start);
                };

                while (std::getline(in, line)) {
                    std::string_view line_view = trim(line);
                    if (line_view.empty() || line_view[0] == '#')
                        continue;
                    if (line_view.starts_with("[daemon]")) {
                        inDaemon = true;
                        continue;
                    }
                    if (inDaemon && !line_view.empty() && line_view[0] == '[') {
                        inDaemon = false;
                    }
                    if (!inDaemon)
                        continue;

                    constexpr std::string_view key = "socket_path";
                    auto pos = line_view.find(key);
                    if (pos == std::string_view::npos)
                        continue;
                    auto eq = line_view.find('=', pos + key.size());
                    if (eq == std::string_view::npos)
                        continue;

                    std::string_view rhs = trim(line_view.substr(eq + 1));
                    if (!rhs.empty() && (rhs.front() == '"' || rhs.front() == '\'')) {
                        char q = rhs.front();
                        auto endq = rhs.find_last_of(q);
                        if (endq != std::string_view::npos && endq > 0) {
                            std::string_view val = rhs.substr(1, endq - 1);
                            if (!val.empty())
                                return fs::path(val);
                        }
                    } else if (!rhs.empty()) {
                        return fs::path(rhs);
                    }
                }
            }
        }
    } catch (...) {
        // Ignore parse errors and fall back
    }
    // 3) default resolution
    return resolve_socket_path();
}

} // namespace yams::daemon::socket_utils
