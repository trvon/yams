// CLI Model Command tests
// Catch2 migration from GTest (yams-3s4 / yams-cli)
//
// Tests for model command CLI parsing.

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/global_io_context.h>

#include <filesystem>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

// Include test helpers for ScopedEnvVar
#include "../../common/test_helpers_catch2.h"

namespace fs = std::filesystem;

namespace {

/**
 * RAII helper for CLI tests with proper isolation and cleanup.
 */
struct CliTestHelper {
    fs::path tempDir;
    std::optional<yams::test::ScopedEnvVar> configEnv;
    std::optional<yams::test::ScopedEnvVar> dataEnv;
    std::optional<yams::test::ScopedEnvVar> nonInteractiveEnv;
    std::optional<yams::test::ScopedEnvVar> disableDaemonEnv;

    CliTestHelper() {
        tempDir = yams::test::make_temp_dir("yams_cli_catch2_test_");
        fs::create_directories(tempDir);

        configEnv.emplace("YAMS_CONFIG", (tempDir / "config.toml").string());
        dataEnv.emplace("YAMS_DATA_DIR", (tempDir / "data").string());
        nonInteractiveEnv.emplace(std::string("YAMS_NON_INTERACTIVE"),
                                  std::optional<std::string>("1"));
        // Disable daemon autostart to prevent cleanup crashes
        disableDaemonEnv.emplace(std::string("YAMS_CLI_DISABLE_DAEMON_AUTOSTART"),
                                 std::optional<std::string>("1"));
    }

    ~CliTestHelper() {
        yams::daemon::GlobalIOContext::reset();

        configEnv.reset();
        dataEnv.reset();
        nonInteractiveEnv.reset();
        disableDaemonEnv.reset();

        std::error_code ec;
        fs::remove_all(tempDir, ec);
    }

    std::unique_ptr<yams::cli::YamsCLI> makeCli() const {
        return std::make_unique<yams::cli::YamsCLI>();
    }

    int runCommand(const std::vector<std::string>& args) {
        auto cli = makeCli();
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

} // namespace

TEST_CASE("ModelCommand - list flag parses and runs", "[cli][model][catch2]") {
    CliTestHelper helper;

    // Test --list flag
    int rc = helper.runCommand({"yams", "model", "--list"});
    CHECK(rc == 0);
}

TEST_CASE("ModelCommand - info flag parses without crash", "[cli][model][catch2]") {
    CliTestHelper helper;

    // Test --info flag (may return non-zero if model not installed, but should not crash)
    (void)helper.runCommand({"yams", "model", "--info", "all-MiniLM-L6-v2"});
    SUCCEED();
}

TEST_CASE("ModelCommand - provider handles unavailable daemon without crashing",
          "[cli][model][catch2]") {
    CliTestHelper helper;
    CaptureStdout capture;

    const int rc = helper.runCommand({"yams", "model", "provider"});

    CHECK(rc == 0);
    CHECK(capture.str().find("Model Provider") != std::string::npos);
}

TEST_CASE("ModelCommand - list with daemon details enabled falls back cleanly",
          "[cli][model][catch2]") {
    CliTestHelper helper;
    yams::test::ScopedEnvVar daemonListEnv("YAMS_MODEL_LIST_WITH_DAEMON",
                                           std::optional<std::string>("1"));
    CaptureStdout capture;

    const int rc = helper.runCommand({"yams", "model", "--list"});

    CHECK(rc == 0);
    CHECK(capture.str().find("Available ONNX Models:") != std::string::npos);
}
