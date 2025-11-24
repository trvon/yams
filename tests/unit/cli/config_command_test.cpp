#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <yams/cli/yams_cli.h>

#ifdef _WIN32
#include <process.h>
#define getpid _getpid
static int setenv(const char* name, const char* value, int overwrite) {
    return _putenv_s(name, value);
}
static int unsetenv(const char* name) {
    return _putenv_s(name, "");
}
#endif

namespace fs = std::filesystem;

class ConfigCommandEmbeddingsTest : public ::testing::Test {
protected:
    fs::path testHome_;
    fs::path testDataHome_;
    fs::path testConfigHome_;

    void SetUp() override {
        // A unique directory for all test artifacts
        testHome_ = fs::temp_directory_path() / ("yams_config_test_" + std::to_string(::getpid()));
        fs::create_directories(testHome_);

        testDataHome_ = testHome_ / "data";
        fs::create_directories(testDataHome_);

        testConfigHome_ = testHome_ / "config";
        fs::create_directories(testConfigHome_);

        setenv("XDG_DATA_HOME", testDataHome_.string().c_str(), 1);
        setenv("XDG_CONFIG_HOME", testConfigHome_.string().c_str(), 1);
    }

    void TearDown() override {
        fs::remove_all(testHome_);
        unsetenv("XDG_DATA_HOME");
        unsetenv("XDG_CONFIG_HOME");
    }

    void createModel(const std::string& modelName) {
        fs::path modelsRoot = testDataHome_ / "yams" / "models";
        fs::path modelDir = modelsRoot / modelName;
        fs::create_directories(modelDir);
        std::ofstream(modelDir / "model.onnx").close();
    }
};

TEST_F(ConfigCommandEmbeddingsTest, SetModelFindsModelInXDGDataHome) {
    createModel("test-model");

    // This lambda will be run in a forked process by ASSERT_EXIT
    auto test_run = []() {
        yams::cli::YamsCLI cli;
        const char* argv[] = {"yams", "config", "embeddings", "model", "test-model"};
        cli.run(std::size(argv), const_cast<char**>(argv));
        std::exit(0); // Success
    };

    // We expect the process to exit with code 0, meaning success.
    // Any other exit code or crash indicates a failure.
    ASSERT_EXIT(test_run(), ::testing::ExitedWithCode(0), ".*");
}

TEST_F(ConfigCommandEmbeddingsTest, SetModelFailsForMissingModel) {
    // Don't create any models

    auto test_run = []() {
        yams::cli::YamsCLI cli;
        const char* argv[] = {"yams", "config", "embeddings", "model", "no-model-here"};
        cli.run(std::size(argv), const_cast<char**>(argv));
        std::exit(0); // Should not be reached
    };

    // We expect the process to exit with code 1, as per the error handling in config_command.cpp
    ASSERT_EXIT(test_run(), ::testing::ExitedWithCode(1), ".*");
}
