/**
 * @file cli_test_fixture.h
 * @brief Test fixture for CLI command tests with proper isolation
 *
 * This fixture provides:
 * - Unique temp directory per test (auto-cleaned on destruction)
 * - Isolated YAMS_CONFIG and YAMS_DATA_DIR environment
 * - Factory method to create fresh YamsCLI instances
 *
 * Usage:
 *   class MyCliTest : public CliTestFixture {};
 *
 *   TEST_F(MyCliTest, SomeCommand) {
 *       auto cli = makeCli();
 *       const char* argv[] = {"yams", "status"};
 *       cli->run(2, const_cast<char**>(argv));
 *   }
 */

#pragma once

#include <gtest/gtest.h>
#include <yams/cli/yams_cli.h>

#include <filesystem>
#include <memory>
#include <optional>
#include <string>

#include "test_helpers.h"

namespace yams::test {

/**
 * @brief RAII fixture for CLI tests with automatic isolation and cleanup
 */
class CliTestFixture : public ::testing::Test {
protected:
    void SetUp() override {
        // Create unique temp directory for this test
        tempDir_ = make_temp_dir("yams_cli_test_");
        std::filesystem::create_directories(tempDir_);

        // Isolate config and data directories
        configEnv_.emplace("YAMS_CONFIG", (tempDir_ / "config.toml").string());
        dataEnv_.emplace("YAMS_DATA_DIR", (tempDir_ / "data").string());
    }

    void TearDown() override {
        // Reset environment first (before removing directory)
        configEnv_.reset();
        dataEnv_.reset();

        // Clean up temp directory
        std::error_code ec;
        std::filesystem::remove_all(tempDir_, ec);
    }

    /**
     * @brief Create a fresh YamsCLI instance for this test
     * @return Unique pointer to a new YamsCLI
     *
     * Each call creates a NEW instance - do not reuse across run() calls.
     * CLI11 subcommands are registered on first run(), so calling run()
     * twice on the same instance will fail with "subcommand already exists".
     */
    [[nodiscard]] std::unique_ptr<cli::YamsCLI> makeCli() const {
        return std::make_unique<cli::YamsCLI>();
    }

    /**
     * @brief Run a CLI command with the given arguments
     * @param args Vector of argument strings (including "yams" as first element)
     * @return Exit code from CLI
     *
     * This is a convenience method that creates a fresh CLI instance,
     * builds argv from the args, and runs the command.
     */
    int runCommand(const std::vector<std::string>& args) {
        auto cli = makeCli();
        std::vector<char*> argv;
        argv.reserve(args.size());
        for (const auto& arg : args) {
            argv.push_back(const_cast<char*>(arg.c_str()));
        }
        return cli->run(static_cast<int>(argv.size()), argv.data());
    }

    /**
     * @brief Get the data directory path for this test
     */
    [[nodiscard]] std::filesystem::path dataDir() const { return tempDir_ / "data"; }

    /**
     * @brief Get the config file path for this test
     */
    [[nodiscard]] std::filesystem::path configPath() const { return tempDir_ / "config.toml"; }

    /**
     * @brief Get the temp directory root for this test
     */
    [[nodiscard]] const std::filesystem::path& tempDir() const { return tempDir_; }

private:
    std::filesystem::path tempDir_;
    std::optional<ScopedEnvVar> configEnv_;
    std::optional<ScopedEnvVar> dataEnv_;
};

} // namespace yams::test
