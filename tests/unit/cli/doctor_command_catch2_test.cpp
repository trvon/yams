// CLI Doctor Command tests
// Catch2 migration from GTest (yams-3s4 / yams-cli)
//
// Tests for doctor command subcommands.

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/yams_cli.h>

#include <algorithm>
#include <filesystem>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

// Include test helpers for ScopedEnvVar
#include "../../common/test_helpers_catch2.h"

namespace fs = std::filesystem;
using namespace yams::cli;

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
        nonInteractiveEnv.emplace(std::string("YAMS_NON_INTERACTIVE"), std::optional<std::string>("1"));
        // Disable daemon autostart to prevent cleanup crashes
        disableDaemonEnv.emplace(std::string("YAMS_CLI_DISABLE_DAEMON_AUTOSTART"), std::optional<std::string>("1"));
    }

    ~CliTestHelper() {
        configEnv.reset();
        dataEnv.reset();
        nonInteractiveEnv.reset();
        disableDaemonEnv.reset();

        std::error_code ec;
        fs::remove_all(tempDir, ec);
    }

    std::unique_ptr<YamsCLI> makeCli() const {
        return std::make_unique<YamsCLI>();
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

/**
 * RAII helper to capture and restore cout.
 */
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

TEST_CASE("DoctorCommand - prune help does not run doctor diagnostics", "[cli][doctor][catch2]") {
    CliTestHelper helper;

    // When running `yams doctor prune` without arguments (help mode),
    // it should NOT execute the default doctor diagnostics (runAll)
    CaptureStdout capture;

    auto cli = helper.makeCli();
    const char* argv[] = {"yams", "doctor", "prune"};
    cli->run(3, const_cast<char**>(argv));

    std::string output = capture.str();

    // Should show prune help text
    CHECK(output.find("YAMS Doctor Prune") != std::string::npos);
    CHECK(output.find("Composite Categories") != std::string::npos);

    // Should NOT show doctor diagnostics output
    CHECK(output.find("Collecting system information") == std::string::npos);
    CHECK(output.find("Daemon Status") == std::string::npos);
}

TEST_CASE("DoctorCommand - prune with category does not run doctor diagnostics", "[cli][doctor][catch2]") {
    CliTestHelper helper;

    CaptureStdout capture;

    auto cli = helper.makeCli();
    // Parse args: yams doctor prune --category build-artifacts
    const char* argv[] = {"yams", "doctor", "prune", "--category", "build-artifacts"};
    // Note: This will fail if daemon not running, but that's OK for the test
    // We're just checking it doesn't run doctor diagnostics
    cli->run(5, const_cast<char**>(argv));

    std::string output = capture.str();

    // Should NOT show doctor diagnostics output
    CHECK(output.find("Collecting system information") == std::string::npos);
}

// Note: We don't test bare "yams doctor" because it actually runs diagnostics
// which may hang or take a long time. The key fix is ensuring subcommands
// like "prune" don't trigger diagnostics.

TEST_CASE("DoctorCommand - prune help displays without errors", "[cli][doctor][catch2]") {
    CliTestHelper helper;

    // Test that the UI frames are built correctly
    CaptureStdout capture;

    auto cli = helper.makeCli();
    const char* argv[] = {"yams", "doctor", "prune"};

    // Should not throw
    REQUIRE_NOTHROW(cli->run(3, const_cast<char**>(argv)));

    std::string output = capture.str();

    // Check for proper formatting
    CHECK(output.find("YAMS Doctor Prune") != std::string::npos);
    CHECK(output.find("build-artifacts") != std::string::npos);
    CHECK(output.find("Usage Examples") != std::string::npos);

    // Should not contain error messages (case-insensitive check)
    std::string lowerOutput = output;
    std::transform(lowerOutput.begin(), lowerOutput.end(), lowerOutput.begin(), ::tolower);

    // Allow "error" in words like "stderr" but not standalone
    bool hasError = lowerOutput.find(" error") != std::string::npos ||
                    lowerOutput.find("error ") != std::string::npos ||
                    lowerOutput.find("failed") != std::string::npos;
    CHECK_FALSE(hasError);
}
