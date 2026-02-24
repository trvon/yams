#include <fstream>
#include <map>
#include <sstream>
#include <yams/config/config_helpers.h>

#ifdef _WIN32
#include <shlobj.h>
#include <windows.h>
#else
#include <unistd.h> // For getuid()
#endif

namespace yams::config {

// Platform-specific directory helpers
std::filesystem::path get_config_dir() {
#ifdef _WIN32
    // Windows: Use APPDATA (roaming) for config that should sync
    if (const char* appData = std::getenv("APPDATA")) {
        return std::filesystem::path(appData) / "yams";
    }
    // Fallback to USERPROFILE
    if (const char* userProfile = std::getenv("USERPROFILE")) {
        return std::filesystem::path(userProfile) / "AppData" / "Roaming" / "yams";
    }
#else
    // Unix: Follow XDG Base Directory Specification
    if (const char* xdgConfig = std::getenv("XDG_CONFIG_HOME")) {
        return std::filesystem::path(xdgConfig) / "yams";
    }
    if (const char* home = std::getenv("HOME")) {
        return std::filesystem::path(home) / ".config" / "yams";
    }
#endif
    // Last resort: current directory
    return std::filesystem::current_path() / ".yams";
}

std::filesystem::path get_data_dir() {
    // Check YAMS_DATA_DIR environment variable first (used by test fixtures)
    if (const char* dataDir = std::getenv("YAMS_DATA_DIR"); dataDir && *dataDir) {
        return std::filesystem::path(dataDir);
    }
    // Also check legacy YAMS_STORAGE env var for backwards compatibility
    if (const char* storage = std::getenv("YAMS_STORAGE"); storage && *storage) {
        return std::filesystem::path(storage);
    }
#ifdef _WIN32
    // Windows: Use LOCALAPPDATA for local data (databases, indices)
    if (const char* localAppData = std::getenv("LOCALAPPDATA")) {
        return std::filesystem::path(localAppData) / "yams";
    }
    // Fallback to USERPROFILE
    if (const char* userProfile = std::getenv("USERPROFILE")) {
        return std::filesystem::path(userProfile) / "AppData" / "Local" / "yams";
    }
#else
    // Unix: Follow XDG Base Directory Specification
    if (const char* xdgData = std::getenv("XDG_DATA_HOME")) {
        return std::filesystem::path(xdgData) / "yams";
    }
    if (const char* home = std::getenv("HOME")) {
        return std::filesystem::path(home) / ".local" / "share" / "yams";
    }
#endif
    // Last resort: current directory
    return std::filesystem::current_path() / "yams_data";
}

std::filesystem::path get_cache_dir() {
#ifdef _WIN32
    // Windows: Use LOCALAPPDATA\yams\cache
    if (const char* localAppData = std::getenv("LOCALAPPDATA")) {
        return std::filesystem::path(localAppData) / "yams" / "cache";
    }
    // Fallback to USERPROFILE
    if (const char* userProfile = std::getenv("USERPROFILE")) {
        return std::filesystem::path(userProfile) / "AppData" / "Local" / "yams" / "cache";
    }
#else
    // Unix: Follow XDG Base Directory Specification
    if (const char* xdgCache = std::getenv("XDG_CACHE_HOME")) {
        return std::filesystem::path(xdgCache) / "yams";
    }
    if (const char* home = std::getenv("HOME")) {
        return std::filesystem::path(home) / ".cache" / "yams";
    }
#endif
    // Last resort
    return std::filesystem::temp_directory_path() / "yams-cache";
}

std::filesystem::path get_runtime_dir() {
#ifdef _WIN32
    // Windows: Use LOCALAPPDATA for runtime (sockets, PIDs)
    // Note: Windows doesn't have XDG_RUNTIME_DIR equivalent, so we use LOCALAPPDATA
    if (const char* localAppData = std::getenv("LOCALAPPDATA")) {
        return std::filesystem::path(localAppData) / "yams";
    }
    // Fallback to USERPROFILE
    if (const char* userProfile = std::getenv("USERPROFILE")) {
        return std::filesystem::path(userProfile) / "AppData" / "Local" / "yams";
    }
#else
    // Unix: Use XDG_RUNTIME_DIR (typically /run/user/$UID)
    if (const char* xdgRuntime = std::getenv("XDG_RUNTIME_DIR")) {
        return std::filesystem::path(xdgRuntime) / "yams";
    }
    // Fallback to /tmp with user-specific subdirectory
    std::string tmpDir = "/tmp/yams-" + std::to_string(getuid());
    return std::filesystem::path(tmpDir);
#endif
    // Last resort
    return std::filesystem::temp_directory_path() / "yams-runtime";
}

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
    // Check YAMS_CONFIG environment variable first (used by test fixtures)
    if (const char* configEnv = std::getenv("YAMS_CONFIG"); configEnv && *configEnv) {
        return std::filesystem::path(configEnv);
    }
    // Use the platform-specific config directory helper
    return get_config_dir() / "config.toml";
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

    // 3) Use platform-specific data directory
    return get_data_dir();
}

std::string resolve_daemon_mode_from_config() {
    if (const char* env = std::getenv("YAMS_EMBEDDED"); env && *env) {
        std::string value = env;
        trim(value);
        if (!value.empty()) {
            return value;
        }
    }

    std::filesystem::path config_path;
    if (const char* cfg_env = std::getenv("YAMS_CONFIG"); cfg_env && *cfg_env) {
        config_path = std::filesystem::path(cfg_env);
    } else {
        config_path = get_config_path();
    }

    if (!config_path.empty()) {
        auto value = parse_config_value(config_path, "daemon", "mode");
        trim(value);
        return value;
    }

    return {};
}

std::vector<std::filesystem::path> parse_path_list(const std::string& raw) {
    std::vector<std::filesystem::path> out;
    if (raw.empty())
        return out;

    std::string s = raw;
    trim(s);
    if (s.empty())
        return out;

    // Strip surrounding brackets for TOML arrays
    if (s.front() == '[' && s.back() == ']') {
        s = s.substr(1, s.size() - 2);
    }

    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, ',')) {
        trim(item);
        item = unquote(item);
        if (item.empty())
            continue;
        out.emplace_back(expand_tilde(item));
    }

    return out;
}

// ============================================================================
// Full TOML config parsing and writing utilities
// ============================================================================

std::map<std::string, std::string> parse_simple_toml(const std::filesystem::path& path) {
    std::map<std::string, std::string> config;
    std::ifstream file(path);
    if (!file) {
        return config;
    }

    std::string line;
    std::string currentSection;

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
                if (!currentSection.empty()) {
                    currentSection += ".";
                }
            }
            continue;
        }

        // Parse key-value pairs
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            std::string key = line.substr(0, eq);
            std::string value = line.substr(eq + 1);

            trim(key);
            trim(value);

            // Remove inline comments (but be careful with quoted strings)
            bool inQuote = false;
            for (size_t i = 0; i < value.size(); ++i) {
                if (value[i] == '"' || value[i] == '\'') {
                    inQuote = !inQuote;
                } else if (value[i] == '#' && !inQuote) {
                    value = value.substr(0, i);
                    trim(value);
                    break;
                }
            }

            value = unquote(value);
            config[currentSection + key] = value;
        }
    }

    return config;
}

DimensionConfig read_dimension_config(const std::filesystem::path& config_path) {
    DimensionConfig result{};

    if (!std::filesystem::exists(config_path)) {
        return result;
    }

    auto config = parse_simple_toml(config_path);

    auto parseSize = [](const std::string& str) -> std::optional<size_t> {
        if (str.empty()) {
            return std::nullopt;
        }
        try {
            return static_cast<size_t>(std::stoul(str));
        } catch (...) {
            return std::nullopt;
        }
    };

    // Check various keys for embedding dimension
    if (auto it = config.find("embeddings.embedding_dim"); it != config.end()) {
        result.embeddings = parseSize(it->second);
    }
    if (auto it = config.find("vector_database.embedding_dim"); it != config.end()) {
        result.vectorDb = parseSize(it->second);
    }
    if (auto it = config.find("vector_index.dimension"); it != config.end()) {
        result.index = parseSize(it->second);
    }
    // Also check for typo variant
    if (!result.index) {
        if (auto it = config.find("vector_index.dimenions"); it != config.end()) {
            result.index = parseSize(it->second);
        }
    }

    return result;
}

bool write_dimension_config(const std::filesystem::path& config_path, size_t dim) {
    std::map<std::string, std::string> values;
    values["embeddings.embedding_dim"] = std::to_string(dim);
    values["vector_database.embedding_dim"] = std::to_string(dim);
    values["vector_index.dimension"] = std::to_string(dim);
    return write_config_values(config_path, values);
}

bool write_config_value(const std::filesystem::path& config_path, const std::string& key,
                        const std::string& value) {
    std::map<std::string, std::string> values;
    values[key] = value;
    return write_config_values(config_path, values);
}

bool write_config_values(const std::filesystem::path& config_path,
                         const std::map<std::string, std::string>& values) {
    try {
        // Ensure parent directories exist
        std::filesystem::create_directories(config_path.parent_path());

        // Read existing config
        auto config = parse_simple_toml(config_path);

        // Merge new values
        for (const auto& [key, value] : values) {
            config[key] = value;
        }

        // Group by section for writing
        std::map<std::string, std::map<std::string, std::string>> sections;
        for (const auto& [fullKey, val] : config) {
            size_t dot = fullKey.find('.');
            if (dot != std::string::npos) {
                std::string section = fullKey.substr(0, dot);
                std::string subkey = fullKey.substr(dot + 1);
                sections[section][subkey] = val;
            } else {
                // Top-level key (no section)
                sections[""][fullKey] = val;
            }
        }

        // Write back to file
        std::ofstream file(config_path, std::ios::trunc);
        if (!file) {
            return false;
        }

        // Write top-level keys first (if any)
        if (auto it = sections.find(""); it != sections.end()) {
            for (const auto& [key, val] : it->second) {
                file << key << " = \"" << val << "\"\n";
            }
            sections.erase(it);
        }

        // Write each section
        for (const auto& [section, kvs] : sections) {
            file << "\n[" << section << "]\n";
            for (const auto& [key, val] : kvs) {
                file << key << " = \"" << val << "\"\n";
            }
        }

        return true;
    } catch (...) {
        return false;
    }
}

} // namespace yams::config
