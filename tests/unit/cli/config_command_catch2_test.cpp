// CLI Config Command tests
// Catch2 migration from GTest (yams-3s4 / yams-cli)
//
// Tests for config embeddings command.
// Note: Original tests used ASSERT_EXIT (death tests) which don't have
// a direct Catch2 equivalent. These tests validate the command setup
// and model discovery logic without using process exit checks.

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/yams_cli.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>

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
        testHome = fs::temp_directory_path() / ("yams_config_test_" + std::to_string(::getpid()));
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
        fs::path modelsRoot = testDataHome / "yams" / "models";
        fs::path modelDir = modelsRoot / modelName;
        fs::create_directories(modelDir);
        std::ofstream(modelDir / "model.onnx").close();
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
    // SKIP: This test cannot be run in Catch2 because the CLI command calls std::exit(1)
    // on failure, which terminates the entire test process. The original GTest test used
    // ASSERT_EXIT which forks a new process. Catch2 doesn't have a direct equivalent.
    // Tagged with [.death_test] to exclude from default runs.
    SKIP("Death test - CLI calls std::exit() which terminates test process");

    ConfigCommandFixture fixture;
    // Don't create any models

    // Run the config embeddings model command
    // This will call std::exit(1) which terminates the test process
    int rc = fixture.runCommand({"yams", "config", "embeddings", "model", "no-model-here"});
    CHECK(rc != 0); // Should fail with non-zero exit
}

TEST_CASE("ConfigCommand - config command parses without crash", "[cli][config][catch2]") {
    ConfigCommandFixture fixture;

    // Just verify the command parses and runs without crashing
    // Return code may vary based on setup
    (void)fixture.runCommand({"yams", "config", "--help"});
    SUCCEED();
}
