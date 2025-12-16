// CLI Data Directory Resolution tests
// Catch2 migration from GTest (yams-3s4 / yams-cli)
//
// Tests for data directory resolution precedence.

#include <catch2/catch_test_macros.hpp>

#include <yams/config/config_helpers.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

#ifdef _WIN32
#include <process.h>
#endif

namespace fs = std::filesystem;

namespace {

/**
 * RAII helper to set and restore environment variables.
 */
class ScopedEnv {
public:
    ScopedEnv(const char* name, const char* value) : name_(name) {
        if (const char* old = std::getenv(name)) {
            hadValue_ = true;
            oldValue_ = old;
        }
        set(value);
    }

    ~ScopedEnv() {
        if (hadValue_) {
            set(oldValue_.c_str());
        } else {
            unset();
        }
    }

    ScopedEnv(const ScopedEnv&) = delete;
    ScopedEnv& operator=(const ScopedEnv&) = delete;

private:
    void set(const char* value) {
#ifdef _WIN32
        _putenv_s(name_.c_str(), value);
#else
        setenv(name_.c_str(), value, 1);
#endif
    }

    void unset() {
#ifdef _WIN32
        _putenv_s(name_.c_str(), "");
#else
        unsetenv(name_.c_str());
#endif
    }

    std::string name_;
    std::string oldValue_;
    bool hadValue_{false};
};

/**
 * RAII helper to create a temporary config file.
 */
class TempConfig {
public:
    explicit TempConfig(const std::string& content) {
        auto pid =
#ifdef _WIN32
            _getpid();
#else
            getpid();
#endif
        tempDir_ = fs::temp_directory_path() / ("yams_test_" + std::to_string(pid));
        fs::create_directories(tempDir_);
        configPath_ = tempDir_ / "config.toml";
        std::ofstream f(configPath_);
        f << content;
    }

    ~TempConfig() {
        std::error_code ec;
        fs::remove_all(tempDir_, ec);
    }

    fs::path path() const { return configPath_; }
    std::string pathStr() const { return configPath_.string(); }

    TempConfig(const TempConfig&) = delete;
    TempConfig& operator=(const TempConfig&) = delete;

private:
    fs::path tempDir_;
    fs::path configPath_;
};

} // namespace

// ============================================================================
// get_data_dir() - Platform default tests
// ============================================================================

TEST_CASE("DataDirResolution - get_data_dir returns non-empty path", "[cli][data_dir][catch2]") {
    auto dataDir = yams::config::get_data_dir();
    CHECK_FALSE(dataDir.empty());
}

TEST_CASE("DataDirResolution - get_data_dir contains yams", "[cli][data_dir][catch2]") {
    auto dataDir = yams::config::get_data_dir();
    CHECK(dataDir.string().find("yams") != std::string::npos);
}

#ifndef _WIN32
TEST_CASE("DataDirResolution - get_data_dir respects XDG_DATA_HOME", "[cli][data_dir][catch2][!mayfail]") {
    ScopedEnv xdg("XDG_DATA_HOME", "/custom/data");
    auto dataDir = yams::config::get_data_dir();
    CHECK(dataDir == fs::path("/custom/data/yams"));
}
#else
TEST_CASE("DataDirResolution - get_data_dir respects LOCALAPPDATA", "[cli][data_dir][catch2]") {
    const char* localAppData = std::getenv("LOCALAPPDATA");
    if (localAppData) {
        auto dataDir = yams::config::get_data_dir();
        CHECK(dataDir.string().find("yams") != std::string::npos);
    }
}
#endif

// ============================================================================
// get_config_dir() - Platform default tests
// ============================================================================

TEST_CASE("DataDirResolution - get_config_dir returns non-empty path", "[cli][data_dir][catch2]") {
    auto configDir = yams::config::get_config_dir();
    CHECK_FALSE(configDir.empty());
}

TEST_CASE("DataDirResolution - get_config_dir contains yams", "[cli][data_dir][catch2]") {
    auto configDir = yams::config::get_config_dir();
    CHECK(configDir.string().find("yams") != std::string::npos);
}

// ============================================================================
// resolve_data_dir_from_config() - Precedence tests
// ============================================================================

TEST_CASE("DataDirResolution - env YAMS_STORAGE takes precedence", "[cli][data_dir][catch2]") {
    ScopedEnv storage("YAMS_STORAGE", "/env/storage/path");
    ScopedEnv dataDir("YAMS_DATA_DIR", "/env/data/path");

    auto resolved = yams::config::resolve_data_dir_from_config();
    CHECK(resolved == fs::path("/env/storage/path"));
}

TEST_CASE("DataDirResolution - env YAMS_DATA_DIR used if no storage", "[cli][data_dir][catch2]") {
    ScopedEnv storage("YAMS_STORAGE", "");
    ScopedEnv dataDir("YAMS_DATA_DIR", "/env/data/path");

    auto resolved = yams::config::resolve_data_dir_from_config();
    CHECK(resolved == fs::path("/env/data/path"));
}

TEST_CASE("DataDirResolution - config data_dir used with YAMS_CONFIG env", "[cli][data_dir][catch2]") {
    TempConfig config(R"(
[core]
data_dir = "/from/config/file"
)");
    ScopedEnv storage("YAMS_STORAGE", "");
    ScopedEnv dataDir("YAMS_DATA_DIR", "");
    ScopedEnv configEnv("YAMS_CONFIG", config.pathStr().c_str());

    auto resolved = yams::config::resolve_data_dir_from_config();
    CHECK(resolved == fs::path("/from/config/file"));
}

TEST_CASE("DataDirResolution - env takes precedence over config", "[cli][data_dir][catch2]") {
    TempConfig config(R"(
[core]
data_dir = "/from/config/file"
)");
    ScopedEnv storage("YAMS_STORAGE", "/env/wins");
    ScopedEnv configEnv("YAMS_CONFIG", config.pathStr().c_str());

    auto resolved = yams::config::resolve_data_dir_from_config();
    CHECK(resolved == fs::path("/env/wins"));
}

TEST_CASE("DataDirResolution - falls back to platform default", "[cli][data_dir][catch2]") {
    ScopedEnv storage("YAMS_STORAGE", "");
    ScopedEnv dataDir("YAMS_DATA_DIR", "");
    ScopedEnv configEnv("YAMS_CONFIG", "/nonexistent/config.toml");

    auto resolved = yams::config::resolve_data_dir_from_config();
    CHECK_FALSE(resolved.empty());
    CHECK(resolved.string().find("yams") != std::string::npos);
}

// ============================================================================
// parse_config_value() - Config file parsing tests
// ============================================================================

TEST_CASE("DataDirResolution - parse_config_value finds key", "[cli][data_dir][catch2]") {
    TempConfig config(R"(
[core]
data_dir = "/test/path"
)");

    auto value = yams::config::parse_config_value(config.path(), "core", "data_dir");
    CHECK(value == "/test/path");
}

TEST_CASE("DataDirResolution - parse_config_value handles quoted values", "[cli][data_dir][catch2]") {
    TempConfig config(R"(
[core]
data_dir = "/path/with spaces"
)");

    auto value = yams::config::parse_config_value(config.path(), "core", "data_dir");
    CHECK(value == "/path/with spaces");
}

TEST_CASE("DataDirResolution - parse_config_value handles single quotes", "[cli][data_dir][catch2]") {
    TempConfig config(R"(
[core]
data_dir = '/single/quoted'
)");

    auto value = yams::config::parse_config_value(config.path(), "core", "data_dir");
    CHECK(value == "/single/quoted");
}

TEST_CASE("DataDirResolution - parse_config_value ignores comments", "[cli][data_dir][catch2]") {
    TempConfig config(R"(
[core]
# This is a comment
data_dir = "/actual/value" # inline comment
)");

    auto value = yams::config::parse_config_value(config.path(), "core", "data_dir");
    CHECK(value == "/actual/value");
}

TEST_CASE("DataDirResolution - parse_config_value returns empty for missing key", "[cli][data_dir][catch2]") {
    TempConfig config(R"(
[core]
other_key = "value"
)");

    auto value = yams::config::parse_config_value(config.path(), "core", "data_dir");
    CHECK(value.empty());
}

TEST_CASE("DataDirResolution - parse_config_value returns empty for missing section", "[cli][data_dir][catch2]") {
    TempConfig config(R"(
[other]
data_dir = "/wrong/section"
)");

    auto value = yams::config::parse_config_value(config.path(), "core", "data_dir");
    CHECK(value.empty());
}

TEST_CASE("DataDirResolution - parse_config_value handles empty file", "[cli][data_dir][catch2]") {
    TempConfig config("");

    auto value = yams::config::parse_config_value(config.path(), "core", "data_dir");
    CHECK(value.empty());
}

TEST_CASE("DataDirResolution - parse_config_value returns empty for nonexistent file", "[cli][data_dir][catch2]") {
    auto value = yams::config::parse_config_value("/nonexistent/config.toml", "core", "data_dir");
    CHECK(value.empty());
}

// ============================================================================
// Tilde expansion tests
// ============================================================================

#ifndef _WIN32
TEST_CASE("DataDirResolution - expand_tilde expands home", "[cli][data_dir][catch2]") {
    ScopedEnv home("HOME", "/home/testuser");

    auto expanded = yams::config::expand_tilde("~/some/path");
    CHECK(expanded == fs::path("/home/testuser/some/path"));
}

TEST_CASE("DataDirResolution - expand_tilde leaves absolute paths unchanged", "[cli][data_dir][catch2]") {
    auto expanded = yams::config::expand_tilde("/absolute/path");
    CHECK(expanded == fs::path("/absolute/path"));
}

TEST_CASE("DataDirResolution - expand_tilde leaves relative paths unchanged", "[cli][data_dir][catch2]") {
    auto expanded = yams::config::expand_tilde("relative/path");
    CHECK(expanded == fs::path("relative/path"));
}
#endif

// ============================================================================
// Values must NOT contain description strings
// ============================================================================

TEST_CASE("DataDirResolution - resolved path does not contain descriptions", "[cli][data_dir][catch2]") {
    std::vector<std::string> badPatterns = {
        "[Deprecated]", "Single path to file", "use '-' for stdin",
        "Data directory for storage", "especially useful for", "File or directory paths"
    };

    auto dataDir = yams::config::get_data_dir();
    auto configDir = yams::config::get_config_dir();
    auto cacheDir = yams::config::get_cache_dir();
    auto runtimeDir = yams::config::get_runtime_dir();

    for (const auto& pattern : badPatterns) {
        INFO("Checking pattern: " << pattern);
        CHECK(dataDir.string().find(pattern) == std::string::npos);
        CHECK(configDir.string().find(pattern) == std::string::npos);
        CHECK(cacheDir.string().find(pattern) == std::string::npos);
        CHECK(runtimeDir.string().find(pattern) == std::string::npos);
    }
}

TEST_CASE("DataDirResolution - config value does not contain descriptions", "[cli][data_dir][catch2]") {
    TempConfig config(R"(
[core]
data_dir = "/proper/path"
)");

    auto value = yams::config::parse_config_value(config.path(), "core", "data_dir");

    CHECK(value == "/proper/path");
    CHECK(value.find("[Deprecated]") == std::string::npos);
    CHECK(value.find("file/directory") == std::string::npos);
    CHECK(value.find("Data directory") == std::string::npos);
}

// ============================================================================
// socket_path resolution tests
// ============================================================================

TEST_CASE("DataDirResolution - socket_path env takes precedence", "[cli][data_dir][catch2]") {
    ScopedEnv socketEnv("YAMS_DAEMON_SOCKET", "/env/yams.sock");

    auto resolved = yams::config::resolve_socket_path_from_config();
    CHECK(resolved == fs::path("/env/yams.sock"));
}

TEST_CASE("DataDirResolution - socket_path from config file", "[cli][data_dir][catch2]") {
    TempConfig config(R"(
[daemon]
socket_path = "/from/config/yams.sock"
)");
    ScopedEnv socketEnv("YAMS_DAEMON_SOCKET", "");
    ScopedEnv configEnv("YAMS_CONFIG", config.pathStr().c_str());

    auto resolved = yams::config::resolve_socket_path_from_config();
    CHECK(resolved == fs::path("/from/config/yams.sock"));
}

TEST_CASE("DataDirResolution - socket_path env overrides config", "[cli][data_dir][catch2]") {
    TempConfig config(R"(
[daemon]
socket_path = "/from/config/yams.sock"
)");
    ScopedEnv socketEnv("YAMS_DAEMON_SOCKET", "/env/wins.sock");
    ScopedEnv configEnv("YAMS_CONFIG", config.pathStr().c_str());

    auto resolved = yams::config::resolve_socket_path_from_config();
    CHECK(resolved == fs::path("/env/wins.sock"));
}

// ============================================================================
// parse_path_list() tests
// ============================================================================

TEST_CASE("DataDirResolution - parse_path_list handles comma-separated", "[cli][data_dir][catch2]") {
    auto paths = yams::config::parse_path_list("/path/a,/path/b,/path/c");
    REQUIRE(paths.size() == 3);
    CHECK(paths[0] == fs::path("/path/a"));
    CHECK(paths[1] == fs::path("/path/b"));
    CHECK(paths[2] == fs::path("/path/c"));
}

TEST_CASE("DataDirResolution - parse_path_list handles TOML array", "[cli][data_dir][catch2]") {
    auto paths = yams::config::parse_path_list(R"(["/path/a", "/path/b"])");
    REQUIRE(paths.size() == 2);
    CHECK(paths[0] == fs::path("/path/a"));
    CHECK(paths[1] == fs::path("/path/b"));
}

TEST_CASE("DataDirResolution - parse_path_list handles empty string", "[cli][data_dir][catch2]") {
    auto paths = yams::config::parse_path_list("");
    CHECK(paths.empty());
}

TEST_CASE("DataDirResolution - parse_path_list handles whitespace", "[cli][data_dir][catch2]") {
    auto paths = yams::config::parse_path_list("  /path/a  ,  /path/b  ");
    REQUIRE(paths.size() == 2);
    CHECK(paths[0] == fs::path("/path/a"));
    CHECK(paths[1] == fs::path("/path/b"));
}

#ifndef _WIN32
TEST_CASE("DataDirResolution - parse_path_list expands tilde", "[cli][data_dir][catch2]") {
    ScopedEnv home("HOME", "/home/testuser");

    auto paths = yams::config::parse_path_list("~/path/a,~/path/b");
    REQUIRE(paths.size() == 2);
    CHECK(paths[0] == fs::path("/home/testuser/path/a"));
    CHECK(paths[1] == fs::path("/home/testuser/path/b"));
}
#endif
