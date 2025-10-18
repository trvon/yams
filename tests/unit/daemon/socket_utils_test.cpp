#include <gtest/gtest.h>
#include <yams/daemon/ipc/socket_utils.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>

using namespace yams::daemon::socket_utils;
namespace fs = std::filesystem;

class SocketUtilsTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Save original environment
        save_env("YAMS_DAEMON_SOCKET");
        save_env("XDG_RUNTIME_DIR");
        save_env("XDG_CONFIG_HOME");
        save_env("HOME");

        // Create temp directories for testing
        temp_dir_ = fs::temp_directory_path() / ("yams_test_" + random_suffix());
        fs::create_directories(temp_dir_);
        // XDG_CONFIG_HOME/yams/config.toml is the expected path
        config_base_ = temp_dir_ / "config";
        config_dir_ = config_base_ / "yams";
        fs::create_directories(config_dir_);
    }

    void TearDown() override {
        // Restore environment
        for (const auto& [key, val] : saved_env_) {
            if (val.has_value()) {
                setenv(key.c_str(), val.value().c_str(), 1);
            } else {
                unsetenv(key.c_str());
            }
        }

        // Cleanup temp directories
        std::error_code ec;
        fs::remove_all(temp_dir_, ec);
    }

    void save_env(const char* name) {
        if (const char* val = std::getenv(name)) {
            saved_env_[name] = std::string(val);
        } else {
            saved_env_[name] = std::nullopt;
        }
    }

    std::string random_suffix() {
        return std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    }

    void write_config(const std::string& content) {
        auto config_file = config_dir_ / "config.toml";
        std::ofstream out(config_file);
        out << content;
        out.close();
    }

    fs::path temp_dir_;
    fs::path config_base_; // For XDG_CONFIG_HOME
    fs::path config_dir_;  // Actual config directory (config_base_/yams)
    std::map<std::string, std::optional<std::string>> saved_env_;
};

// Test environment variable override (highest priority)
TEST_F(SocketUtilsTest, EnvOverrideTakesPrecedence) {
    auto test_path = "/tmp/custom-daemon.sock";
    setenv("YAMS_DAEMON_SOCKET", test_path, 1);

    auto result = resolve_socket_path();
    EXPECT_EQ(result.string(), test_path);

    result = resolve_socket_path_config_first();
    EXPECT_EQ(result.string(), test_path);
}

// Test XDG_RUNTIME_DIR resolution
TEST_F(SocketUtilsTest, XdgRuntimeDirResolution) {
    unsetenv("YAMS_DAEMON_SOCKET");

    auto xdg_dir = temp_dir_ / "runtime";
    fs::create_directories(xdg_dir);
    setenv("XDG_RUNTIME_DIR", xdg_dir.c_str(), 1);

    auto result = resolve_socket_path();
    EXPECT_EQ(result.parent_path(), xdg_dir);
    EXPECT_EQ(result.filename(), "yams-daemon.sock");
}

// Test /tmp fallback for non-root when XDG not available
TEST_F(SocketUtilsTest, TmpFallbackForNonRoot) {
    unsetenv("YAMS_DAEMON_SOCKET");
    unsetenv("XDG_RUNTIME_DIR");

    // Only test if not root
    if (::geteuid() != 0) {
        auto result = resolve_socket_path();
        EXPECT_EQ(result.parent_path(), fs::path("/tmp"));

        // Should include UID in filename
        auto filename = result.filename().string();
        EXPECT_TRUE(filename.find("yams-daemon-") == 0);
        EXPECT_TRUE(filename.find(std::to_string(::getuid())) != std::string::npos);
    }
}

// Test /var/run for root
TEST_F(SocketUtilsTest, VarRunForRoot) {
    unsetenv("YAMS_DAEMON_SOCKET");
    unsetenv("XDG_RUNTIME_DIR");

    // Only test if root
    if (::geteuid() == 0) {
        auto result = resolve_socket_path();
        EXPECT_EQ(result.string(), "/var/run/yams-daemon.sock");
    } else {
        GTEST_SKIP() << "Test requires root privileges";
    }
}

// Test config file parsing
TEST_F(SocketUtilsTest, ConfigFileResolution) {
    unsetenv("YAMS_DAEMON_SOCKET");

    auto custom_socket = temp_dir_ / "custom.sock";
    write_config(R"(
# YAMS Configuration
[daemon]
socket_path = ")" +
                 custom_socket.string() + R"("
)");

    setenv("XDG_CONFIG_HOME", config_base_.c_str(), 1);

    auto result = resolve_socket_path_config_first();
    EXPECT_EQ(result, custom_socket);
}

// Test config file with single quotes
TEST_F(SocketUtilsTest, ConfigFileSingleQuotes) {
    unsetenv("YAMS_DAEMON_SOCKET");

    auto custom_socket = temp_dir_ / "single-quote.sock";
    write_config(R"(
[daemon]
socket_path = ')" +
                 custom_socket.string() + R"('
)");

    setenv("XDG_CONFIG_HOME", config_base_.c_str(), 1);

    auto result = resolve_socket_path_config_first();
    EXPECT_EQ(result, custom_socket);
}

// Test config file without quotes
TEST_F(SocketUtilsTest, ConfigFileNoQuotes) {
    unsetenv("YAMS_DAEMON_SOCKET");

    auto custom_socket = temp_dir_ / "no-quotes.sock";
    write_config(R"(
[daemon]
socket_path = )" +
                 custom_socket.string());

    setenv("XDG_CONFIG_HOME", config_base_.c_str(), 1);

    auto result = resolve_socket_path_config_first();
    EXPECT_EQ(result, custom_socket);
}

// Test config file with comments
TEST_F(SocketUtilsTest, ConfigFileWithComments) {
    unsetenv("YAMS_DAEMON_SOCKET");

    auto custom_socket = temp_dir_ / "commented.sock";
    write_config(R"(
# This is a comment
[daemon]
# socket_path = "/tmp/wrong.sock"
socket_path = ")" +
                 custom_socket.string() + R"("
# another comment
)");

    setenv("XDG_CONFIG_HOME", config_base_.c_str(), 1);

    auto result = resolve_socket_path_config_first();
    EXPECT_EQ(result, custom_socket);
}

// Test config file fallback to default
TEST_F(SocketUtilsTest, ConfigFileFallbackToDefault) {
    unsetenv("YAMS_DAEMON_SOCKET");
    unsetenv("XDG_RUNTIME_DIR");

    // Point to non-existent config
    setenv("XDG_CONFIG_HOME", "/nonexistent/path", 1);

    auto result = resolve_socket_path_config_first();

    // Should fall back to default resolution
    EXPECT_FALSE(result.empty());
}

// Test HOME fallback for config
TEST_F(SocketUtilsTest, HomeFallbackForConfig) {
    unsetenv("YAMS_DAEMON_SOCKET");
    unsetenv("XDG_CONFIG_HOME");

    auto home_dir = temp_dir_ / "home";
    fs::create_directories(home_dir / ".config" / "yams");
    setenv("HOME", home_dir.c_str(), 1);

    auto custom_socket = temp_dir_ / "home-config.sock";
    std::ofstream out(home_dir / ".config" / "yams" / "config.toml");
    out << "[daemon]\n";
    out << "socket_path = \"" << custom_socket.string() << "\"\n";
    out.close();

    auto result = resolve_socket_path_config_first();
    EXPECT_EQ(result, custom_socket);
}

// Test empty environment variable
TEST_F(SocketUtilsTest, EmptyEnvVariableIgnored) {
    setenv("YAMS_DAEMON_SOCKET", "", 1);
    unsetenv("XDG_RUNTIME_DIR");

    auto result = resolve_socket_path();

    // Should not use empty string, should fall back
    EXPECT_FALSE(result.empty());
    EXPECT_NE(result.string(), "");
}

// Test config section isolation
TEST_F(SocketUtilsTest, ConfigSectionIsolation) {
    unsetenv("YAMS_DAEMON_SOCKET");

    auto custom_socket = temp_dir_ / "section-test.sock";
    write_config(R"(
[other_section]
socket_path = "/tmp/wrong.sock"

[daemon]
socket_path = ")" +
                 custom_socket.string() + R"("

[another_section]
socket_path = "/tmp/also-wrong.sock"
)");

    setenv("XDG_CONFIG_HOME", config_base_.c_str(), 1);

    auto result = resolve_socket_path_config_first();
    EXPECT_EQ(result, custom_socket);
}
