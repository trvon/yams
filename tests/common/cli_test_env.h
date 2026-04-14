// Catch2 replacement for tests/common/cli_test_fixture.h.
// Provides a RAII helper that isolates a CLI test's config/data directory
// and supplies a factory for fresh YamsCLI instances. Intended to be
// constructed inside a TEST_CASE body or TEST_CASE_METHOD fixture struct.
#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <yams/cli/yams_cli.h>

#include "test_helpers_catch2.h"

namespace yams::test {

/// RAII helper that creates a unique temp directory for a CLI test,
/// sets the YAMS_CONFIG/YAMS_DATA_DIR environment variables for the
/// duration of its lifetime, and cleans up on destruction.
class CliTestEnv {
public:
    explicit CliTestEnv(std::string_view prefix = "yams_cli_test_")
        : tempDir_(make_temp_dir(prefix)) {
        std::filesystem::create_directories(tempDir_);
        configEnv_.emplace("YAMS_CONFIG", (tempDir_ / "config.toml").string());
        dataEnv_.emplace("YAMS_DATA_DIR", (tempDir_ / "data").string());
        nonInteractiveEnv_.emplace("YAMS_NON_INTERACTIVE", "1");
    }

    CliTestEnv(const CliTestEnv&) = delete;
    CliTestEnv& operator=(const CliTestEnv&) = delete;

    ~CliTestEnv() {
        configEnv_.reset();
        dataEnv_.reset();
        nonInteractiveEnv_.reset();

        std::error_code ec;
        std::filesystem::remove_all(tempDir_, ec);
    }

    /// Create a fresh YamsCLI instance. Each call returns a new object because
    /// CLI11 subcommands are registered lazily on the first run() — reusing
    /// an instance across run() calls fails.
    [[nodiscard]] std::unique_ptr<cli::YamsCLI> makeCli() const {
        return std::make_unique<cli::YamsCLI>();
    }

    /// Run a CLI command synchronously. Returns the exit code.
    int runCommand(const std::vector<std::string>& args) {
        auto cli = makeCli();
        std::vector<char*> argv;
        argv.reserve(args.size());
        for (const auto& arg : args) {
            argv.push_back(const_cast<char*>(arg.c_str()));
        }
        return cli->run(static_cast<int>(argv.size()), argv.data());
    }

    [[nodiscard]] std::filesystem::path dataDir() const { return tempDir_ / "data"; }
    [[nodiscard]] std::filesystem::path configPath() const { return tempDir_ / "config.toml"; }
    [[nodiscard]] const std::filesystem::path& tempDir() const { return tempDir_; }

private:
    std::filesystem::path tempDir_;
    std::optional<ScopedEnvVar> configEnv_;
    std::optional<ScopedEnvVar> dataEnv_;
    std::optional<ScopedEnvVar> nonInteractiveEnv_;
};

} // namespace yams::test
