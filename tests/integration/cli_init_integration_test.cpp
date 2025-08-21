#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <regex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#if defined(_WIN32)
#include <windows.h>
#else
#include <sys/wait.h>
#endif

namespace fs = std::filesystem;

// ---------- Helpers ----------

static std::string shellQuote(const std::string& s) {
#if defined(_WIN32)
    // Basic quoting for Windows cmd - not used in CI for this project (macOS/Linux primarily)
    std::string out = "\"";
    for (char c : s) {
        if (c == '"')
            out += "\\\"";
        else
            out += c;
    }
    out += "\"";
    return out;
#else
    // Quote with single quotes, escape internal single quotes
    std::string out = "'";
    for (char c : s) {
        if (c == '\'') {
            out += "'\\''";
        } else {
            out += c;
        }
    }
    out += "'";
    return out;
#endif
}

struct CommandResult {
    int exit_code = -1;
    std::string stdout_str;
};

static CommandResult runCommandCapture(const std::string& command) {
    CommandResult result;
    std::array<char, 4096> buffer{};

#if defined(_WIN32)
    // Windows - fallback: use _popen
    FILE* pipe = _popen(command.c_str(), "r");
    if (!pipe) {
        result.exit_code = -1;
        return result;
    }
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
        result.stdout_str.append(buffer.data());
    }
    int rc = _pclose(pipe);
    result.exit_code = rc;
#else
    // POSIX
    FILE* pipe = popen(command.c_str(), "r");
    if (!pipe) {
        result.exit_code = -1;
        return result;
    }
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
        result.stdout_str.append(buffer.data());
    }
    int status = pclose(pipe);
    if (status == -1) {
        result.exit_code = -1;
    } else if (WIFEXITED(status)) {
        result.exit_code = WEXITSTATUS(status);
    } else {
        result.exit_code = status;
    }
#endif

    return result;
}

static bool setEnvVar(const char* name, const std::string& value) {
#if defined(_WIN32)
    return (_putenv_s(name, value.c_str()) == 0);
#else
    return (setenv(name, value.c_str(), 1) == 0);
#endif
}

static std::string getEnvVar(const char* name) {
    const char* v = std::getenv(name);
    return v ? std::string(v) : std::string();
}

static std::string getYamsBinary() {
    // Allow override via environment; fallback to "yams" in PATH
    auto fromEnv = getEnvVar("YAMS_CLI");
    if (!fromEnv.empty())
        return fromEnv;
#if defined(_WIN32)
    return "yams.exe";
#else
    return "yams";
#endif
}

static fs::path makeTempDir(const std::string& prefix = "yams_cli_it_") {
    auto base = fs::temp_directory_path();
    auto now = std::chrono::system_clock::now().time_since_epoch().count();
    std::string name = prefix + std::to_string(now);
    fs::path dir = base / name;
    fs::create_directories(dir);
    return dir;
}

static std::string readFileToString(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

static auto lastWriteTime(const fs::path& p) -> fs::file_time_type {
    return fs::exists(p) ? fs::last_write_time(p) : fs::file_time_type::min();
}

static bool isMaskedApiKeyPresent(const std::string& text) {
    // Expect something like: api_keys = ["abcdef********..."]
    // 6 hex chars followed by one or more asterisks
    std::regex maskedRe(R"(api_keys\s*=\s*\["[0-9a-f]{6}\*+"\])");
    return std::regex_search(text, maskedRe);
}

// ---------- Tests ----------

TEST(CLIInitIntegration, CreatesXDGLayoutAndMasksSecrets) {
    // Arrange
    const auto tmp = makeTempDir("yams_cli_it_xdg_");
    const fs::path xdg_config = tmp / "config";
    const fs::path xdg_data = tmp / "data";
    fs::create_directories(xdg_config);
    fs::create_directories(xdg_data);

    ASSERT_TRUE(setEnvVar("XDG_CONFIG_HOME", xdg_config.string()));
    ASSERT_TRUE(setEnvVar("XDG_DATA_HOME", xdg_data.string()));

    const fs::path storage_dir = tmp / "store";
    fs::create_directories(storage_dir);

    const auto yams = getYamsBinary();

    // Act: run non-interactive init with explicit storage and print sanitized config
    std::ostringstream cmd;
    cmd << shellQuote(yams) << " " << "--storage " << shellQuote(storage_dir.string()) << " "
        << "init --non-interactive --print";

    const auto res = runCommandCapture(cmd.str());

    // Assert: exit code success or 0
    ASSERT_EQ(res.exit_code, 0) << "stdout:\n" << res.stdout_str;

    // Check sanitized output contains masked api key and does not leak full secret
    EXPECT_TRUE(isMaskedApiKeyPresent(res.stdout_str)) << "Config print should mask API keys";

    // Assert XDG layout artifacts
    const fs::path config_toml = xdg_config / "yams" / "config.toml";
    const fs::path keys_dir = xdg_config / "yams" / "keys";
    const fs::path priv_key = keys_dir / "ed25519.pem";
    const fs::path pub_key = keys_dir / "ed25519.pub";

    EXPECT_TRUE(fs::exists(config_toml)) << "Expected config at: " << config_toml;
    EXPECT_TRUE(fs::exists(keys_dir) && fs::is_directory(keys_dir)) << "Expected keys dir";
    EXPECT_TRUE(fs::exists(priv_key)) << "Expected private key";
    EXPECT_TRUE(fs::exists(pub_key)) << "Expected public key";

    // Assert data artifacts (db + storage)
    const fs::path db_file = storage_dir / "yams.db";
    const fs::path storage_path = storage_dir / "storage";
    EXPECT_TRUE(fs::exists(db_file)) << "Expected DB file at: " << db_file;
    EXPECT_TRUE(fs::exists(storage_path) && fs::is_directory(storage_path))
        << "Expected storage dir";

    // Cleanup
    fs::remove_all(tmp);
}

TEST(CLIInitIntegration, IdempotentWithoutForcePreservesFiles) {
    // Arrange
    const auto tmp = makeTempDir("yams_cli_it_idem_");
    const fs::path xdg_config = tmp / "config";
    const fs::path xdg_data = tmp / "data";
    fs::create_directories(xdg_config);
    fs::create_directories(xdg_data);
    ASSERT_TRUE(setEnvVar("XDG_CONFIG_HOME", xdg_config.string()));
    ASSERT_TRUE(setEnvVar("XDG_DATA_HOME", xdg_data.string()));

    const fs::path storage_dir = tmp / "store";
    fs::create_directories(storage_dir);

    const auto yams = getYamsBinary();

    // First run
    {
        std::ostringstream cmd;
        cmd << shellQuote(yams) << " " << "--storage " << shellQuote(storage_dir.string()) << " "
            << "init --non-interactive";
        const auto res = runCommandCapture(cmd.str());
        ASSERT_EQ(res.exit_code, 0) << "First init failed. stdout:\n" << res.stdout_str;
    }

    const fs::path config_toml = xdg_config / "yams" / "config.toml";
    const fs::path keys_dir = xdg_config / "yams" / "keys";
    const fs::path priv_key = keys_dir / "ed25519.pem";
    const fs::path pub_key = keys_dir / "ed25519.pub";

    ASSERT_TRUE(fs::exists(config_toml));
    ASSERT_TRUE(fs::exists(priv_key));
    ASSERT_TRUE(fs::exists(pub_key));

    auto t_config_before = lastWriteTime(config_toml);
    auto t_priv_before = lastWriteTime(priv_key);
    auto t_pub_before = lastWriteTime(pub_key);

    // Sleep to guarantee mtime resolution differences if files were changed
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));

    // Second run without --force should be idempotent (no changes)
    {
        std::ostringstream cmd;
        cmd << shellQuote(yams) << " " << "--storage " << shellQuote(storage_dir.string()) << " "
            << "init --non-interactive";
        const auto res = runCommandCapture(cmd.str());
        ASSERT_EQ(res.exit_code, 0) << "Second init (idempotent) failed. stdout:\n"
                                    << res.stdout_str;
    }

    auto t_config_after = lastWriteTime(config_toml);
    auto t_priv_after = lastWriteTime(priv_key);
    auto t_pub_after = lastWriteTime(pub_key);

    EXPECT_EQ(t_config_before, t_config_after) << "Config should not change without --force";
    EXPECT_EQ(t_priv_before, t_priv_after) << "Private key should not change without --force";
    EXPECT_EQ(t_pub_before, t_pub_after) << "Public key should not change without --force";

    // Cleanup
    fs::remove_all(tmp);
}

TEST(CLIInitIntegration, ForceOverwritesAndChangesTimestamps) {
    // Arrange
    const auto tmp = makeTempDir("yams_cli_it_force_");
    const fs::path xdg_config = tmp / "config";
    const fs::path xdg_data = tmp / "data";
    fs::create_directories(xdg_config);
    fs::create_directories(xdg_data);
    ASSERT_TRUE(setEnvVar("XDG_CONFIG_HOME", xdg_config.string()));
    ASSERT_TRUE(setEnvVar("XDG_DATA_HOME", xdg_data.string()));

    const fs::path storage_dir = tmp / "store";
    fs::create_directories(storage_dir);

    const auto yams = getYamsBinary();

    // First run
    {
        std::ostringstream cmd;
        cmd << shellQuote(yams) << " " << "--storage " << shellQuote(storage_dir.string()) << " "
            << "init --non-interactive";
        const auto res = runCommandCapture(cmd.str());
        ASSERT_EQ(res.exit_code, 0) << "First init failed. stdout:\n" << res.stdout_str;
    }

    const fs::path config_toml = xdg_config / "yams" / "config.toml";
    const fs::path keys_dir = xdg_config / "yams" / "keys";
    const fs::path priv_key = keys_dir / "ed25519.pem";
    const fs::path pub_key = keys_dir / "ed25519.pub";

    ASSERT_TRUE(fs::exists(config_toml));
    ASSERT_TRUE(fs::exists(priv_key));
    ASSERT_TRUE(fs::exists(pub_key));

    auto t_config_before = lastWriteTime(config_toml);
    auto t_priv_before = lastWriteTime(priv_key);
    auto t_pub_before = lastWriteTime(pub_key);

    // Sleep to guarantee mtime resolution differences
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));

    // Second run with --force should overwrite and thus change timestamps
    {
        std::ostringstream cmd;
        cmd << shellQuote(yams) << " " << "--storage " << shellQuote(storage_dir.string()) << " "
            << "init --non-interactive --force --print";
        const auto res = runCommandCapture(cmd.str());
        ASSERT_EQ(res.exit_code, 0) << "Forced init failed. stdout:\n" << res.stdout_str;
        // As part of force run, ensure masked keys are printed
        EXPECT_TRUE(isMaskedApiKeyPresent(res.stdout_str))
            << "Forced init --print should mask API keys";
    }

    auto t_config_after = lastWriteTime(config_toml);
    auto t_priv_after = lastWriteTime(priv_key);
    auto t_pub_after = lastWriteTime(pub_key);

    EXPECT_NE(t_config_before, t_config_after) << "Config should change with --force";
    EXPECT_NE(t_priv_before, t_priv_after) << "Private key should change with --force";
    EXPECT_NE(t_pub_before, t_pub_after) << "Public key should change with --force";

    // Cleanup
    fs::remove_all(tmp);
}