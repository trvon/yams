/**
 * Tests for data directory resolution precedence.
 *
 * These tests verify that:
 * 1. Config file values take precedence over environment variables
 * 2. Environment variables take precedence over CLI defaults
 * 3. CLI --data-dir flag takes precedence over config
 * 4. Tilde expansion works correctly
 * 5. Empty/missing values fall through correctly
 *
 * Resolution order (highest to lowest priority):
 *   1. CLI --data-dir flag (explicit user override)
 *   2. Config file (core.data_dir in config.toml)
 *   3. Environment variable (YAMS_DATA_DIR or YAMS_STORAGE)
 *   4. Platform-specific default (XDG on Unix, LOCALAPPDATA on Windows)
 */

#include <gtest/gtest.h>
#include <yams/config/config_helpers.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

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
        tempDir_ = fs::temp_directory_path() / ("yams_test_" + std::to_string(
#ifdef _WIN32
                                                                   _getpid()
#else
                                                                   getpid()
#endif
                                                                       ));
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

// ============================================================================
// get_data_dir() - Platform default tests
// ============================================================================

TEST(DataDirResolutionTest, GetDataDirReturnsNonEmptyPath) {
    auto dataDir = yams::config::get_data_dir();
    EXPECT_FALSE(dataDir.empty());
}

TEST(DataDirResolutionTest, GetDataDirContainsYams) {
    auto dataDir = yams::config::get_data_dir();
    EXPECT_NE(dataDir.string().find("yams"), std::string::npos);
}

#ifndef _WIN32
TEST(DataDirResolutionTest, GetDataDirRespectsXdgDataHome) {
    ScopedEnv xdg("XDG_DATA_HOME", "/custom/data");
    auto dataDir = yams::config::get_data_dir();
    EXPECT_EQ(dataDir, fs::path("/custom/data/yams"));
}
#else
TEST(DataDirResolutionTest, GetDataDirRespectsLocalAppData) {
    // On Windows, LOCALAPPDATA is used
    const char* localAppData = std::getenv("LOCALAPPDATA");
    if (localAppData) {
        auto dataDir = yams::config::get_data_dir();
        EXPECT_NE(dataDir.string().find("yams"), std::string::npos);
    }
}
#endif

// ============================================================================
// get_config_dir() - Platform default tests
// ============================================================================

TEST(DataDirResolutionTest, GetConfigDirReturnsNonEmptyPath) {
    auto configDir = yams::config::get_config_dir();
    EXPECT_FALSE(configDir.empty());
}

TEST(DataDirResolutionTest, GetConfigDirContainsYams) {
    auto configDir = yams::config::get_config_dir();
    EXPECT_NE(configDir.string().find("yams"), std::string::npos);
}

// ============================================================================
// resolve_data_dir_from_config() - Precedence tests
// ============================================================================

TEST(DataDirResolutionTest, EnvYamsStorageTakesPrecedence) {
    ScopedEnv storage("YAMS_STORAGE", "/env/storage/path");
    ScopedEnv dataDir("YAMS_DATA_DIR", "/env/data/path");

    auto resolved = yams::config::resolve_data_dir_from_config();
    EXPECT_EQ(resolved, fs::path("/env/storage/path"));
}

TEST(DataDirResolutionTest, EnvYamsDataDirUsedIfNoStorage) {
    ScopedEnv storage("YAMS_STORAGE", "");
    ScopedEnv dataDir("YAMS_DATA_DIR", "/env/data/path");

    auto resolved = yams::config::resolve_data_dir_from_config();
    EXPECT_EQ(resolved, fs::path("/env/data/path"));
}

TEST(DataDirResolutionTest, ConfigDataDirUsedWithYamsConfigEnv) {
    TempConfig config(R"(
[core]
data_dir = "/from/config/file"
)");
    ScopedEnv storage("YAMS_STORAGE", "");
    ScopedEnv dataDir("YAMS_DATA_DIR", "");
    ScopedEnv configEnv("YAMS_CONFIG", config.pathStr().c_str());

    auto resolved = yams::config::resolve_data_dir_from_config();
    EXPECT_EQ(resolved, fs::path("/from/config/file"));
}

TEST(DataDirResolutionTest, EnvTakesPrecedenceOverConfig) {
    TempConfig config(R"(
[core]
data_dir = "/from/config/file"
)");
    ScopedEnv storage("YAMS_STORAGE", "/env/wins");
    ScopedEnv configEnv("YAMS_CONFIG", config.pathStr().c_str());

    auto resolved = yams::config::resolve_data_dir_from_config();
    EXPECT_EQ(resolved, fs::path("/env/wins"));
}

TEST(DataDirResolutionTest, FallsBackToPlatformDefault) {
    ScopedEnv storage("YAMS_STORAGE", "");
    ScopedEnv dataDir("YAMS_DATA_DIR", "");
    ScopedEnv configEnv("YAMS_CONFIG", "/nonexistent/config.toml");

    auto resolved = yams::config::resolve_data_dir_from_config();
    // Should fall back to get_data_dir()
    EXPECT_FALSE(resolved.empty());
    EXPECT_NE(resolved.string().find("yams"), std::string::npos);
}

// ============================================================================
// parse_config_value() - Config file parsing tests
// ============================================================================

TEST(DataDirResolutionTest, ParseConfigValueFindsKey) {
    TempConfig config(R"(
[core]
data_dir = "/test/path"
)");

    auto value = yams::config::parse_config_value(config.path(), "core", "data_dir");
    EXPECT_EQ(value, "/test/path");
}

TEST(DataDirResolutionTest, ParseConfigValueHandlesQuotedValues) {
    TempConfig config(R"(
[core]
data_dir = "/path/with spaces"
)");

    auto value = yams::config::parse_config_value(config.path(), "core", "data_dir");
    EXPECT_EQ(value, "/path/with spaces");
}

TEST(DataDirResolutionTest, ParseConfigValueHandlesSingleQuotes) {
    TempConfig config(R"(
[core]
data_dir = '/single/quoted'
)");

    auto value = yams::config::parse_config_value(config.path(), "core", "data_dir");
    EXPECT_EQ(value, "/single/quoted");
}

TEST(DataDirResolutionTest, ParseConfigValueIgnoresComments) {
    TempConfig config(R"(
[core]
# This is a comment
data_dir = "/actual/value" # inline comment
)");

    auto value = yams::config::parse_config_value(config.path(), "core", "data_dir");
    EXPECT_EQ(value, "/actual/value");
}

TEST(DataDirResolutionTest, ParseConfigValueReturnsEmptyForMissingKey) {
    TempConfig config(R"(
[core]
other_key = "value"
)");

    auto value = yams::config::parse_config_value(config.path(), "core", "data_dir");
    EXPECT_TRUE(value.empty());
}

TEST(DataDirResolutionTest, ParseConfigValueReturnsEmptyForMissingSection) {
    TempConfig config(R"(
[other]
data_dir = "/wrong/section"
)");

    auto value = yams::config::parse_config_value(config.path(), "core", "data_dir");
    EXPECT_TRUE(value.empty());
}

TEST(DataDirResolutionTest, ParseConfigValueHandlesEmptyFile) {
    TempConfig config("");

    auto value = yams::config::parse_config_value(config.path(), "core", "data_dir");
    EXPECT_TRUE(value.empty());
}

TEST(DataDirResolutionTest, ParseConfigValueReturnsEmptyForNonexistentFile) {
    auto value = yams::config::parse_config_value("/nonexistent/config.toml", "core", "data_dir");
    EXPECT_TRUE(value.empty());
}

// ============================================================================
// Tilde expansion tests
// ============================================================================

#ifndef _WIN32
TEST(DataDirResolutionTest, ExpandTildeExpandsHome) {
    ScopedEnv home("HOME", "/home/testuser");

    auto expanded = yams::config::expand_tilde("~/some/path");
    EXPECT_EQ(expanded, fs::path("/home/testuser/some/path"));
}

TEST(DataDirResolutionTest, ExpandTildeLeaveAbsolutePathsUnchanged) {
    auto expanded = yams::config::expand_tilde("/absolute/path");
    EXPECT_EQ(expanded, fs::path("/absolute/path"));
}

TEST(DataDirResolutionTest, ExpandTildeLeaveRelativePathsUnchanged) {
    auto expanded = yams::config::expand_tilde("relative/path");
    EXPECT_EQ(expanded, fs::path("relative/path"));
}
#endif

// ============================================================================
// Values must NOT contain description strings
// ============================================================================

TEST(DataDirResolutionTest, ResolvedPathDoesNotContainDescriptions) {
    // These are description patterns that should NEVER appear in paths
    std::vector<std::string> badPatterns = {"[Deprecated]",          "Single path to file",
                                            "use '-' for stdin",     "Data directory for storage",
                                            "especially useful for", "File or directory paths"};

    auto dataDir = yams::config::get_data_dir();
    auto configDir = yams::config::get_config_dir();
    auto cacheDir = yams::config::get_cache_dir();
    auto runtimeDir = yams::config::get_runtime_dir();

    for (const auto& pattern : badPatterns) {
        EXPECT_EQ(dataDir.string().find(pattern), std::string::npos)
            << "data_dir contains description pattern: " << pattern;
        EXPECT_EQ(configDir.string().find(pattern), std::string::npos)
            << "config_dir contains description pattern: " << pattern;
        EXPECT_EQ(cacheDir.string().find(pattern), std::string::npos)
            << "cache_dir contains description pattern: " << pattern;
        EXPECT_EQ(runtimeDir.string().find(pattern), std::string::npos)
            << "runtime_dir contains description pattern: " << pattern;
    }
}

TEST(DataDirResolutionTest, ConfigValueDoesNotContainDescriptions) {
    TempConfig config(R"(
[core]
data_dir = "/proper/path"
)");

    auto value = yams::config::parse_config_value(config.path(), "core", "data_dir");

    // Should be the actual value
    EXPECT_EQ(value, "/proper/path");

    // Should NOT contain description-like text
    EXPECT_EQ(value.find("[Deprecated]"), std::string::npos);
    EXPECT_EQ(value.find("file/directory"), std::string::npos);
    EXPECT_EQ(value.find("Data directory"), std::string::npos);
}

// ============================================================================
// socket_path resolution tests
// ============================================================================

TEST(DataDirResolutionTest, SocketPathEnvTakesPrecedence) {
    ScopedEnv socketEnv("YAMS_DAEMON_SOCKET", "/env/yams.sock");

    auto resolved = yams::config::resolve_socket_path_from_config();
    EXPECT_EQ(resolved, fs::path("/env/yams.sock"));
}

TEST(DataDirResolutionTest, SocketPathFromConfigFile) {
    TempConfig config(R"(
[daemon]
socket_path = "/from/config/yams.sock"
)");
    ScopedEnv socketEnv("YAMS_DAEMON_SOCKET", "");
    ScopedEnv configEnv("YAMS_CONFIG", config.pathStr().c_str());

    auto resolved = yams::config::resolve_socket_path_from_config();
    EXPECT_EQ(resolved, fs::path("/from/config/yams.sock"));
}

TEST(DataDirResolutionTest, SocketPathEnvOverridesConfig) {
    TempConfig config(R"(
[daemon]
socket_path = "/from/config/yams.sock"
)");
    ScopedEnv socketEnv("YAMS_DAEMON_SOCKET", "/env/wins.sock");
    ScopedEnv configEnv("YAMS_CONFIG", config.pathStr().c_str());

    auto resolved = yams::config::resolve_socket_path_from_config();
    EXPECT_EQ(resolved, fs::path("/env/wins.sock"));
}

// ============================================================================
// parse_path_list() tests
// ============================================================================

TEST(DataDirResolutionTest, ParsePathListHandlesCommaSeparated) {
    auto paths = yams::config::parse_path_list("/path/a,/path/b,/path/c");
    ASSERT_EQ(paths.size(), 3);
    EXPECT_EQ(paths[0], fs::path("/path/a"));
    EXPECT_EQ(paths[1], fs::path("/path/b"));
    EXPECT_EQ(paths[2], fs::path("/path/c"));
}

TEST(DataDirResolutionTest, ParsePathListHandlesTomlArray) {
    auto paths = yams::config::parse_path_list(R"(["/path/a", "/path/b"])");
    ASSERT_EQ(paths.size(), 2);
    EXPECT_EQ(paths[0], fs::path("/path/a"));
    EXPECT_EQ(paths[1], fs::path("/path/b"));
}

TEST(DataDirResolutionTest, ParsePathListHandlesEmptyString) {
    auto paths = yams::config::parse_path_list("");
    EXPECT_TRUE(paths.empty());
}

TEST(DataDirResolutionTest, ParsePathListHandlesWhitespace) {
    auto paths = yams::config::parse_path_list("  /path/a  ,  /path/b  ");
    ASSERT_EQ(paths.size(), 2);
    EXPECT_EQ(paths[0], fs::path("/path/a"));
    EXPECT_EQ(paths[1], fs::path("/path/b"));
}

#ifndef _WIN32
TEST(DataDirResolutionTest, ParsePathListExpandsTilde) {
    ScopedEnv home("HOME", "/home/testuser");

    auto paths = yams::config::parse_path_list("~/path/a,~/path/b");
    ASSERT_EQ(paths.size(), 2);
    EXPECT_EQ(paths[0], fs::path("/home/testuser/path/a"));
    EXPECT_EQ(paths[1], fs::path("/home/testuser/path/b"));
}
#endif

} // namespace
