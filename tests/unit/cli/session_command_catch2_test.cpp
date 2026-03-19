// CLI Session Command tests
// Catch2 migration from GTest (yams-3s4 / yams-cli)
//
// Tests for session command CLI parsing.

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/yams_cli.h>

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

TEST_CASE("SessionCommand - diff subcommand parses", "[cli][session][catch2][.daemon_required]") {
    CliTestHelper helper;
    CaptureStdout capture;

    const int rc = helper.runCommand({"yams", "session", "diff", "--json"});

    CHECK(rc == 0);
    CHECK(capture.str().find("terminate") == std::string::npos);
}

TEST_CASE("SessionCommand - warm parses with budgets", "[cli][session][catch2][.daemon_required]") {
    CliTestHelper helper;
    CaptureStdout capture;

    const int startRc = helper.runCommand({"yams", "session", "start", "warm-test"});
    REQUIRE(startRc == 0);

    const int useRc = helper.runCommand({"yams", "session", "use", "warm-test"});
    REQUIRE(useRc == 0);

    const int warmRc = helper.runCommand(
        {"yams", "session", "warm", "--limit", "10", "--cores", "2", "--snippet-len", "80"});

    CHECK(warmRc == 0);
    const auto output = capture.str();
    CHECK((output.find("Warming (daemon) requested.") != std::string::npos ||
           output.find("Warmed (local)") != std::string::npos));
}
