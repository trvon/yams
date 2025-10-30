#include <fstream>
#include <map>
#include <yams/config/config_helpers.h>

namespace yams::config {

std::string parse_config_value(const std::filesystem::path& config_path, const std::string& section,
                               const std::string& key) {
    std::ifstream file(config_path);
    if (!file) {
        return "";
    }

    std::string line;
    std::string currentSection;
    bool in_target_section = section.empty();

    while (std::getline(file, line)) {
        trim(line);

        // Skip comments and empty lines
        if (line.empty() || line[0] == '#') {
            continue;
        }

        // Check for section headers [section]
        if (line[0] == '[') {
            size_t end = line.find(']');
            if (end != std::string::npos) {
                currentSection = line.substr(1, end - 1);
                trim(currentSection);
                in_target_section = (section.empty() || currentSection == section);
            }
            continue;
        }

        // Parse key-value pairs
        if (in_target_section) {
            size_t eq = line.find('=');
            if (eq != std::string::npos) {
                std::string k = line.substr(0, eq);
                std::string v = line.substr(eq + 1);

                trim(k);
                trim(v);

                // Remove inline comments
                size_t comment = v.find('#');
                if (comment != std::string::npos) {
                    v = v.substr(0, comment);
                    trim(v);
                }

                if (k == key) {
                    return unquote(v);
                }
            }
        }
    }

    return "";
}

std::filesystem::path get_config_path(const std::string& override_path) {
    if (!override_path.empty()) {
        return std::filesystem::path(override_path);
    }

    const char* xdgConfigHome = std::getenv("XDG_CONFIG_HOME");
    const char* homeEnv = std::getenv("HOME");

    std::filesystem::path configHome;
    if (xdgConfigHome) {
        configHome = std::filesystem::path(xdgConfigHome);
    } else if (homeEnv) {
        configHome = std::filesystem::path(homeEnv) / ".config";
    } else {
        return std::filesystem::path("~/.config") / "yams" / "config.toml";
    }

    return configHome / "yams" / "config.toml";
}

// Helper to parse path-specific config values with tilde expansion
static std::filesystem::path parse_config_path_value(const std::filesystem::path& config_path,
                                                     const std::string& target_section,
                                                     const std::string& target_key) {
    if (!std::filesystem::exists(config_path))
        return {};

    std::ifstream f(config_path);
    std::string line, current_section;

    while (std::getline(f, line)) {
        trim(line);
        if (line.empty() || line[0] == '#')
            continue;

        if (line.front() == '[') {
            trim(line);
            if (line.size() >= 2 && line.back() == ']') {
                current_section = line.substr(1, line.size() - 2);
                trim(current_section);
            }
            continue;
        }

        auto pos = line.find('=');
        if (pos == std::string::npos)
            continue;

        std::string key = line.substr(0, pos);
        std::string val = line.substr(pos + 1);
        trim(key);
        val = unquote(val);

        // Support both "daemon.socket_path" and "[daemon] socket_path"
        if (key == target_section + "." + target_key ||
            (current_section == target_section && key == target_key)) {
            return expand_tilde(val);
        }
    }
    return {};
}

std::filesystem::path resolve_socket_path_from_config() {
    // 1) YAMS_DAEMON_SOCKET env
    if (const char* env = std::getenv("YAMS_DAEMON_SOCKET"); env && *env) {
        return std::filesystem::path(env);
    }

    // 2) config.toml daemon.socket_path
    std::filesystem::path config_path;
    if (const char* cfg_env = std::getenv("YAMS_CONFIG"); cfg_env && *cfg_env) {
        config_path = std::filesystem::path(cfg_env);
    } else {
        config_path = get_config_path();
    }

    if (!config_path.empty()) {
        if (auto result = parse_config_path_value(config_path, "daemon", "socket_path");
            !result.empty()) {
            return result;
        }
    }

    // 3) Default
    return {};
}

std::filesystem::path resolve_data_dir_from_config() {
    // 1) YAMS_STORAGE or YAMS_DATA_DIR env
    if (const char* env = std::getenv("YAMS_STORAGE"); env && *env) {
        return std::filesystem::path(env);
    }
    if (const char* env = std::getenv("YAMS_DATA_DIR"); env && *env) {
        return std::filesystem::path(env);
    }

    // 2) config.toml core.data_dir
    std::filesystem::path config_path;
    if (const char* cfg_env = std::getenv("YAMS_CONFIG"); cfg_env && *cfg_env) {
        config_path = std::filesystem::path(cfg_env);
    } else {
        config_path = get_config_path();
    }

    if (!config_path.empty()) {
        if (auto result = parse_config_path_value(config_path, "core", "data_dir");
            !result.empty()) {
            return result;
        }
    }

    // 3) XDG/HOME defaults
    if (const char* xdg_data = std::getenv("XDG_DATA_HOME")) {
        return std::filesystem::path(xdg_data) / "yams";
    }
    if (const char* home = std::getenv("HOME")) {
        return std::filesystem::path(home) / ".local" / "share" / "yams";
    }

    return std::filesystem::current_path() / "yams_data";
}

} // namespace yams::config
