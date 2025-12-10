#pragma once

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

namespace yams::config {

// String trimming utilities
inline void ltrim(std::string& s) {
    s.erase(s.begin(),
            std::find_if(s.begin(), s.end(), [](unsigned char ch) { return !std::isspace(ch); }));
}

inline void rtrim(std::string& s) {
    s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) { return !std::isspace(ch); })
                .base(),
            s.end());
}

inline void trim(std::string& s) {
    ltrim(s);
    rtrim(s);
}

// Quote handling
inline std::string unquote(std::string val) {
    trim(val);
    if (val.size() >= 2 && ((val.front() == '"' && val.back() == '"') ||
                            (val.front() == '\'' && val.back() == '\''))) {
        return val.substr(1, val.size() - 2);
    }
    return val;
}

// Tilde expansion
inline std::filesystem::path expand_tilde(const std::string& path) {
    if (!path.empty() && path[0] == '~') {
        const char* home = std::getenv("HOME");
        if (home) {
            return std::filesystem::path(home) / path.substr(2);
        }
    }
    return path;
}

// Terminal sanitization
inline std::string sanitize_for_terminal(std::string_view in) {
    std::string out;
    out.reserve(in.size());
    for (unsigned char c : in) {
        if (c >= 0x20 && c <= 0x7E) {
            out.push_back(static_cast<char>(c));
        } else if (c == '\n' || c == '\r' || c == '\t') {
            out.push_back(static_cast<char>(c));
        } else {
            out.push_back('?');
        }
    }
    return out;
}

// Time parsing
inline std::chrono::milliseconds parse_ms(std::string_view s) {
    try {
        return std::chrono::milliseconds(std::stol(std::string(s)));
    } catch (...) {
        return std::chrono::milliseconds(0);
    }
}

// Parse a value from TOML config file
std::string parse_config_value(const std::filesystem::path& config_path, const std::string& section,
                               const std::string& key);

// Parse a comma- or TOML-array-separated list of paths into filesystem paths.
// Accepts forms like "a,b" or ["a", "b"]. Tilde expansion is applied.
std::vector<std::filesystem::path> parse_path_list(const std::string& raw);

// Get standard config path
std::filesystem::path get_config_path(const std::string& override_path = "");

// Platform-specific directory resolution (follows OS best practices)
// Windows: APPDATA (roaming) for config, LOCALAPPDATA for data/cache/runtime
// Unix/macOS: XDG Base Directory Specification

/// Returns the user config directory
/// Windows: %APPDATA%\yams
/// Unix: $XDG_CONFIG_HOME/yams or ~/.config/yams
std::filesystem::path get_config_dir();

/// Returns the user data directory (databases, indices, persistent storage)
/// Windows: %LOCALAPPDATA%\yams
/// Unix: $XDG_DATA_HOME/yams or ~/.local/share/yams
std::filesystem::path get_data_dir();

/// Returns the user cache directory (temporary files, caches)
/// Windows: %LOCALAPPDATA%\yams\cache
/// Unix: $XDG_CACHE_HOME/yams or ~/.cache/yams
std::filesystem::path get_cache_dir();

/// Returns the runtime directory (sockets, PIDs, ephemeral files)
/// Windows: %LOCALAPPDATA%\yams
/// Unix: $XDG_RUNTIME_DIR/yams or /tmp/yams-$UID
std::filesystem::path get_runtime_dir();

// Daemon-specific config resolution (env → config → defaults)
std::filesystem::path resolve_socket_path_from_config();
std::filesystem::path resolve_data_dir_from_config();

} // namespace yams::config
