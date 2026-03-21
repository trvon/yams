// CLI Config Command tests
// Catch2 migration from GTest (yams-3s4 / yams-cli)
//
// Tests for config embeddings command.
// Note: Original tests used ASSERT_EXIT (death tests) which don't have
// a direct Catch2 equivalent. These tests validate the command setup
// and model discovery logic without using process exit checks.

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "../../common/test_helpers_catch2.h"

#ifdef _WIN32
#include <process.h>
#define getpid _getpid
#endif

namespace fs = std::filesystem;

namespace {

/**
 * Test fixture for config command embeddings tests with model directory setup.
 */
struct ConfigCommandFixture {
    fs::path testHome;
    fs::path testDataHome;
    fs::path testConfigHome;
    std::optional<yams::test::ScopedEnvVar> configEnv;
    std::optional<yams::test::ScopedEnvVar> dataEnv;
    std::optional<yams::test::ScopedEnvVar> nonInteractiveEnv;
    std::optional<yams::test::ScopedEnvVar> xdgDataEnv;
    std::optional<yams::test::ScopedEnvVar> xdgConfigEnv;
    std::optional<yams::test::ScopedEnvVar> disableDaemonEnv;

    ConfigCommandFixture() {
        // A unique directory for all test artifacts
        const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        testHome = fs::temp_directory_path() /
                   ("yams_config_test_" + std::to_string(::getpid()) + "_" + std::to_string(now));
        fs::create_directories(testHome);

        testDataHome = testHome / "data";
        fs::create_directories(testDataHome);

        testConfigHome = testHome / "config";
        fs::create_directories(testConfigHome);

        // Use YAMS_CONFIG and YAMS_DATA_DIR for cross-platform isolation
        // These are checked first by get_config_path() and get_data_dir()
        configEnv.emplace("YAMS_CONFIG", (testConfigHome / "config.toml").string());
        dataEnv.emplace("YAMS_DATA_DIR", testDataHome.string());
        // Skip interactive prompts (e.g., migration confirmation)
        nonInteractiveEnv.emplace(std::string("YAMS_NON_INTERACTIVE"),
                                  std::optional<std::string>("1"));
        // Disable daemon autostart to prevent cleanup crashes
        disableDaemonEnv.emplace(std::string("YAMS_CLI_DISABLE_DAEMON_AUTOSTART"),
                                 std::optional<std::string>("1"));

        // Also set XDG vars for Unix compatibility
        xdgDataEnv.emplace("XDG_DATA_HOME", testDataHome.string());
        xdgConfigEnv.emplace("XDG_CONFIG_HOME", testConfigHome.string());

        // Pre-seed a v3 config so command tests are not coupled to migration behavior.
        const fs::path configPath = testConfigHome / "config.toml";
        std::ofstream configFile(configPath, std::ios::trunc);
        configFile << "[version]\n";
        configFile << "config_version = 3\n";
        configFile.close();
    }

    ~ConfigCommandFixture() {
        configEnv.reset();
        dataEnv.reset();
        nonInteractiveEnv.reset();
        disableDaemonEnv.reset();
        xdgDataEnv.reset();
        xdgConfigEnv.reset();

        std::error_code ec;
        fs::remove_all(testHome, ec);
    }

    void createModel(const std::string& modelName) {
        // get_data_dir() returns YAMS_DATA_DIR directly (without /yams suffix)
        // getAvailableModels() searches get_data_dir() / "models"
        fs::path modelsRoot = testDataHome / "models";
        fs::path modelDir = modelsRoot / modelName;
        fs::create_directories(modelDir);
        std::ofstream(modelDir / "model.onnx").close();
    }

    void overwriteConfig(const std::string& content) {
        std::ofstream out(testConfigHome / "config.toml", std::ios::trunc);
        out << content;
    }

    int runCommand(const std::vector<std::string>& args) {
        auto cli = std::make_unique<yams::cli::YamsCLI>();
        std::vector<char*> argv;
        argv.reserve(args.size());
        for (const auto& arg : args) {
            argv.push_back(const_cast<char*>(arg.c_str()));
        }
        return cli->run(static_cast<int>(argv.size()), argv.data());
    }
};

class CaptureStdout {
public:
    CaptureStdout() : oldCout_(std::cout.rdbuf(buffer_.rdbuf())) {}
    ~CaptureStdout() { std::cout.rdbuf(oldCout_); }

    std::string str() const { return buffer_.str(); }

    CaptureStdout(const CaptureStdout&) = delete;
    CaptureStdout& operator=(const CaptureStdout&) = delete;

private:
    std::ostringstream buffer_;
    std::streambuf* oldCout_;
};

auto findBuiltYamsCli() -> fs::path {
    const fs::path cwd = fs::current_path();
    const std::vector<fs::path> candidates = {
        cwd / "build" / "coverage" / "tools" / "yams-cli" / "yams-cli",
        cwd / "build" / "release" / "tools" / "yams-cli" / "yams-cli",
        cwd / "build" / "debug" / "tools" / "yams-cli" / "yams-cli",
    };
    for (const auto& candidate : candidates) {
        if (fs::exists(candidate)) {
            return candidate;
        }
    }
    return {};
}

} // namespace

TEST_CASE("ConfigCommand - set model finds model in XDG data home",
          "[cli][config][embeddings][catch2]") {
    ConfigCommandFixture fixture;
    fixture.createModel("test-model");

    // Run the config embeddings model command
    // Note: Original test used ASSERT_EXIT to check for exit code 0
    // In Catch2, we just run and check return code directly
    int rc = fixture.runCommand({"yams", "config", "embeddings", "model", "test-model"});
    CHECK(rc == 0);
}

TEST_CASE("ConfigCommand - set model fails for missing model",
          "[cli][config][embeddings][catch2][.death_test]") {
    ConfigCommandFixture fixture;
    const fs::path yamsCli = findBuiltYamsCli();
    REQUIRE_FALSE(yamsCli.empty());

    const fs::path out = fixture.testHome / "missing-model.out";
    const std::string cmd = "'" + yamsCli.string() + "' config embeddings model no-model-here > '" +
                            out.string() + "' 2>&1";

    const int rc = std::system(cmd.c_str());
    REQUIRE(rc != 0);

    std::ifstream input(out);
    REQUIRE(input.good());
    const std::string output((std::istreambuf_iterator<char>(input)),
                             std::istreambuf_iterator<char>());
    CHECK((output.find("not found") != std::string::npos ||
           output.find("No local embedding model directories were found") != std::string::npos));
    CHECK(output.find("Available models:") != std::string::npos);
}

TEST_CASE("ConfigCommand - config command parses without crash", "[cli][config][catch2]") {
    ConfigCommandFixture fixture;

    // Just verify the command parses and runs without crashing
    // Return code may vary based on setup
    (void)fixture.runCommand({"yams", "config", "--help"});
    SUCCEED();
}

TEST_CASE("ConfigCommand - storage command writes S3 fallback settings",
          "[cli][config][storage][catch2]") {
    ConfigCommandFixture fixture;
    const fs::path fallbackDir = fixture.testDataHome / "s3-fallback";

    const int rc = fixture.runCommand({"yams", "config", "storage", "--engine", "s3", "--s3-url",
                                       "s3://example-bucket/prefix", "--s3-region", "us-west-2",
                                       "--s3-fallback-policy", "fallback_local_if_configured",
                                       "--s3-fallback-local-data-dir", fallbackDir.string()});
    REQUIRE(rc == 0);

    const auto cfg = yams::config::parse_simple_toml(fixture.testConfigHome / "config.toml");
    REQUIRE(cfg.count("storage.engine") == 1);
    CHECK(cfg.at("storage.engine") == "s3");
    REQUIRE(cfg.count("storage.s3.url") == 1);
    CHECK(cfg.at("storage.s3.url") == "s3://example-bucket/prefix");
    REQUIRE(cfg.count("storage.s3.region") == 1);
    CHECK(cfg.at("storage.s3.region") == "us-west-2");
    REQUIRE(cfg.count("storage.s3.fallback_policy") == 1);
    CHECK(cfg.at("storage.s3.fallback_policy") == "fallback_local_if_configured");
    REQUIRE(cfg.count("storage.s3.fallback_local_data_dir") == 1);
    CHECK(cfg.at("storage.s3.fallback_local_data_dir") == fallbackDir.string());
}

TEST_CASE("ConfigCommand - storage clear fallback local data dir",
          "[cli][config][storage][catch2]") {
    ConfigCommandFixture fixture;
    const fs::path fallbackDir = fixture.testDataHome / "to-clear";

    const int setRc = fixture.runCommand(
        {"yams", "config", "storage", "--s3-fallback-local-data-dir", fallbackDir.string()});
    REQUIRE(setRc == 0);

    const int clearRc =
        fixture.runCommand({"yams", "config", "storage", "--s3-clear-fallback-local-data-dir"});
    REQUIRE(clearRc == 0);

    const auto cfg = yams::config::parse_simple_toml(fixture.testConfigHome / "config.toml");
    REQUIRE(cfg.count("storage.s3.fallback_local_data_dir") == 1);
    CHECK(cfg.at("storage.s3.fallback_local_data_dir").empty());
}

TEST_CASE("ConfigCommand - auto-migrates v2 to v3 and preserves storage settings",
          "[cli][config][storage][migration][catch2]") {
    ConfigCommandFixture fixture;
    fixture.overwriteConfig("[version]\n"
                            "config_version = 2\n"
                            "migrated_from = 1\n"
                            "[storage]\n"
                            "engine = \"s3\"\n"
                            "[storage.s3]\n"
                            "url = \"s3://bucket/prefix\"\n"
                            "region = \"us-east-1\"\n");

    const int rc = fixture.runCommand({"yams", "config", "list"});
    REQUIRE(rc == 0);

    const auto cfg = yams::config::parse_simple_toml(fixture.testConfigHome / "config.toml");
    REQUIRE(cfg.count("version.config_version") == 1);
    CHECK(cfg.at("version.config_version") == "3");
    REQUIRE(cfg.count("storage.engine") == 1);
    CHECK(cfg.at("storage.engine") == "s3");
    REQUIRE(cfg.count("storage.s3.url") == 1);
    CHECK(cfg.at("storage.s3.url") == "s3://bucket/prefix");
    REQUIRE(cfg.count("storage.s3.region") == 1);
    CHECK(cfg.at("storage.s3.region") == "us-east-1");
    REQUIRE(cfg.count("storage.s3.fallback_policy") == 1);
    CHECK(cfg.at("storage.s3.fallback_policy") == "strict");
}

TEST_CASE("ConfigCommand - storage command writes explicit R2 temp credential settings",
          "[cli][config][storage][r2][catch2]") {
    ConfigCommandFixture fixture;

    const int rc = fixture.runCommand(
        {"yams", "config", "storage", "--s3-r2-auth-mode", "temp_credentials", "--s3-r2-api-token",
         "cf-test-token-not-real-1234567890-abcdef", "--s3-r2-account-id",
         "00000000000000000000000000000000", "--s3-r2-parent-access-key-id",
         "11111111111111111111111111111111", "--s3-r2-permission", "object-read-only",
         "--s3-r2-ttl-seconds", "1800"});
    REQUIRE(rc == 0);

    const auto cfg = yams::config::parse_simple_toml(fixture.testConfigHome / "config.toml");
    REQUIRE(cfg.count("storage.s3.r2.auth_mode") == 1);
    CHECK(cfg.at("storage.s3.r2.auth_mode") == "temp_credentials");
    REQUIRE(cfg.count("storage.s3.r2.api_token") == 1);
    CHECK(cfg.at("storage.s3.r2.api_token") == "cf-test-token-not-real-1234567890-abcdef");
    REQUIRE(cfg.count("storage.s3.r2.account_id") == 1);
    CHECK(cfg.at("storage.s3.r2.account_id") == "00000000000000000000000000000000");
    REQUIRE(cfg.count("storage.s3.r2.parent_access_key_id") == 1);
    CHECK(cfg.at("storage.s3.r2.parent_access_key_id") == "11111111111111111111111111111111");
    REQUIRE(cfg.count("storage.s3.r2.permission") == 1);
    CHECK(cfg.at("storage.s3.r2.permission") == "object-read-only");
    REQUIRE(cfg.count("storage.s3.r2.ttl_seconds") == 1);
    CHECK(cfg.at("storage.s3.r2.ttl_seconds") == "1800");
}

TEST_CASE("ConfigCommand - tuning profile persists and status reports it",
          "[cli][config][tuning][catch2]") {
    ConfigCommandFixture fixture;

    const int setRc = fixture.runCommand({"yams", "config", "tuning", "profile", "aggressive"});
    REQUIRE(setRc == 0);

    const auto cfg = yams::config::parse_simple_toml(fixture.testConfigHome / "config.toml");
    REQUIRE(cfg.count("tuning.profile") == 1);
    CHECK(cfg.at("tuning.profile") == "aggressive");

    CaptureStdout capture;
    const int statusRc = fixture.runCommand({"yams", "config", "tuning", "status"});
    REQUIRE(statusRc == 0);

    const std::string output = capture.str();
    CHECK(output.find("Tuning Configuration") != std::string::npos);
    CHECK(output.find("Profile: aggressive") != std::string::npos);
}

TEST_CASE("ConfigCommand - path-tree enable and mode update config",
          "[cli][config][search][path-tree][catch2]") {
    ConfigCommandFixture fixture;

    const int enableRc = fixture.runCommand(
        {"yams", "config", "search", "path-tree", "enable", "--mode", "preferred"});
    REQUIRE(enableRc == 0);

    auto cfg = yams::config::parse_simple_toml(fixture.testConfigHome / "config.toml");
    REQUIRE(cfg.count("search.path_tree.enable") == 1);
    CHECK(cfg.at("search.path_tree.enable") == "true");
    REQUIRE(cfg.count("search.path_tree.mode") == 1);
    CHECK(cfg.at("search.path_tree.mode") == "preferred");

    const int modeRc =
        fixture.runCommand({"yams", "config", "search", "path-tree", "mode", "fallback"});
    REQUIRE(modeRc == 0);

    cfg = yams::config::parse_simple_toml(fixture.testConfigHome / "config.toml");
    REQUIRE(cfg.count("search.path_tree.mode") == 1);
    CHECK(cfg.at("search.path_tree.mode") == "fallback");

    CaptureStdout capture;
    const int statusRc = fixture.runCommand({"yams", "config", "search", "path-tree", "status"});
    REQUIRE(statusRc == 0);

    const std::string output = capture.str();
    CHECK(output.find("Path-tree traversal configuration") != std::string::npos);
    CHECK(output.find("Mode       : fallback") != std::string::npos);
}

TEST_CASE("ConfigCommand - grammar auto download flags update config",
          "[cli][config][grammar][catch2]") {
    ConfigCommandFixture fixture;

    const int enableRc = fixture.runCommand({"yams", "config", "grammar", "auto-enable"});
    REQUIRE(enableRc == 0);

    auto cfg = yams::config::parse_simple_toml(fixture.testConfigHome / "config.toml");
    REQUIRE(cfg.count("plugins.symbol_extraction.auto_download_grammars") == 1);
    CHECK(cfg.at("plugins.symbol_extraction.auto_download_grammars") == "true");

    CaptureStdout pathCapture;
    const int pathRc = fixture.runCommand({"yams", "config", "grammar", "path"});
    REQUIRE(pathRc == 0);
    CHECK(pathCapture.str().find((fixture.testDataHome / "grammars").string()) !=
          std::string::npos);

    const int disableRc = fixture.runCommand({"yams", "config", "grammar", "auto-disable"});
    REQUIRE(disableRc == 0);

    cfg = yams::config::parse_simple_toml(fixture.testConfigHome / "config.toml");
    REQUIRE(cfg.count("plugins.symbol_extraction.auto_download_grammars") == 1);
    CHECK(cfg.at("plugins.symbol_extraction.auto_download_grammars") == "false");
}

TEST_CASE("ConfigCommand - plugins status reports trust and defaults",
          "[cli][config][plugins][catch2]") {
    ConfigCommandFixture fixture;

    CaptureStdout capture;
    const int rc = fixture.runCommand({"yams", "config", "plugins", "status"});
    REQUIRE(rc == 0);

    const std::string output = capture.str();
    CHECK(output.find("Daemon Plugin Configuration") != std::string::npos);
    CHECK(output.find("Trust file: ") != std::string::npos);
    CHECK(output.find("Default plugin roots") != std::string::npos);
    CHECK(output.find("plugin_dir_strict: false") != std::string::npos);
}
