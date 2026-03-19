// CLI Daemon Command tests
// Catch2 migration from GTest (yams-3s4 / yams-cli)
//
// Tests for daemon command CLI parsing.

#include <catch2/catch_test_macros.hpp>

#include <CLI/CLI.hpp>

#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>

#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "../../common/test_helpers_catch2.h"

// Factory is declared in command registry translation unit
namespace yams::cli {
std::unique_ptr<ICommand> createDaemonCommand();
}

using yams::cli::ICommand;

namespace {

namespace fs = std::filesystem;

struct CliTestHelper {
    fs::path tempDir;
    fs::path socketPath;
    std::optional<yams::test::ScopedEnvVar> configEnv;
    std::optional<yams::test::ScopedEnvVar> dataEnv;
    std::optional<yams::test::ScopedEnvVar> nonInteractiveEnv;
    std::optional<yams::test::ScopedEnvVar> disableDaemonEnv;

    CliTestHelper() {
        tempDir = yams::test::make_temp_dir("yams_cli_daemon_catch2_test_");
        fs::create_directories(tempDir / "data");
        socketPath = tempDir / "test-daemon.sock";

        configEnv.emplace("YAMS_CONFIG", (tempDir / "config.toml").string());
        dataEnv.emplace("YAMS_DATA_DIR", (tempDir / "data").string());
        nonInteractiveEnv.emplace(std::string("YAMS_NON_INTERACTIVE"),
                                  std::optional<std::string>("1"));
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

} // namespace

TEST_CASE("DaemonCommand - parses unordered flags with help without executing",
          "[cli][daemon][catch2]") {
    CLI::App app{"yams"};
    auto daemonCmd = yams::cli::createDaemonCommand();
    daemonCmd->registerCommand(app, /*cli*/ nullptr);

    std::vector<const char*> argv = {
        "yams",        "daemon",         "start",           "--foreground",
        "--log-level", "debug",          "--socket",        "/tmp/yams-test.sock",
        "--data-dir",  "/tmp/yams-data", "--pid-file",      "/tmp/yams.pid",
        "--config",    "/tmp/yams.toml", "--daemon-binary", "/usr/bin/true",
        "--help" // ensure callback is not executed
    };

    // Expect a help exception; parse should accept all options before emitting help
    bool gotHelp = false;
    bool gotParseError = false;
    std::string parseErrorMsg;

    try {
        app.parse(static_cast<int>(argv.size()), const_cast<char**>(argv.data()));
        FAIL("Expected CLI::CallForHelp to be thrown");
    } catch (const CLI::CallForHelp&) {
        gotHelp = true;
    } catch (const CLI::ParseError& e) {
        gotParseError = true;
        parseErrorMsg = e.what();
    }

    CHECK(gotHelp);
    if (gotParseError) {
        FAIL("Unexpected parse error: " << parseErrorMsg);
    }
}

TEST_CASE("DaemonCommand - status reports non-running daemon cleanly", "[cli][daemon][catch2]") {
    CliTestHelper helper;
    CaptureStdout capture;

    const int rc =
        helper.runCommand({"yams", "daemon", "status", "--socket", helper.socketPath.string()});
    const std::string output = capture.str();

    CHECK(rc == 0);
    CHECK(output.find("YAMS daemon is ") != std::string::npos);
    CHECK(output.find("terminate") == std::string::npos);
}

TEST_CASE("DaemonCommand - doctor prints diagnostics without daemon", "[cli][daemon][catch2]") {
    CliTestHelper helper;
    CaptureStdout capture;

    const int rc =
        helper.runCommand({"yams", "daemon", "doctor", "--socket", helper.socketPath.string()});
    const std::string output = capture.str();

    CHECK(rc == 0);
    CHECK(output.find("=== YAMS Daemon Doctor ===") != std::string::npos);
    CHECK(output.find("IPC & Files:") != std::string::npos);
    CHECK(output.find("PID Check:") != std::string::npos);
}

TEST_CASE("DaemonCommand - parses equals-style flags with help", "[cli][daemon][catch2]") {
    CLI::App app{"yams"};
    auto daemonCmd = yams::cli::createDaemonCommand();
    daemonCmd->registerCommand(app, /*cli*/ nullptr);

    std::vector<const char*> argv = {"yams",
                                     "daemon",
                                     "start",
                                     "--log-level=debug",
                                     "--socket=/tmp/yams-test.sock",
                                     "--data-dir=/tmp/yams-data",
                                     "--pid-file=/tmp/yams.pid",
                                     "--config=/tmp/yams.toml",
                                     "--daemon-binary=/usr/bin/true",
                                     "--foreground",
                                     "--help"};

    bool gotHelp = false;
    bool gotParseError = false;
    std::string parseErrorMsg;

    try {
        app.parse(static_cast<int>(argv.size()), const_cast<char**>(argv.data()));
        FAIL("Expected CLI::CallForHelp to be thrown");
    } catch (const CLI::CallForHelp&) {
        gotHelp = true;
    } catch (const CLI::ParseError& e) {
        gotParseError = true;
        parseErrorMsg = e.what();
    }

    CHECK(gotHelp);
    if (gotParseError) {
        FAIL("Unexpected parse error: " << parseErrorMsg);
    }
}

TEST_CASE("DaemonCommand - parses status detailed UX flags", "[cli][daemon][catch2]") {
    CLI::App app{"yams"};
    auto daemonCmd = yams::cli::createDaemonCommand();
    daemonCmd->registerCommand(app, /*cli*/ nullptr);

    std::vector<const char*> argv = {
        "yams",  "daemon", "status", "--socket=/tmp/yams-test.sock", "-d",
        "--help" // ensure callback is not executed
    };

    bool gotHelp = false;
    bool gotParseError = false;
    std::string parseErrorMsg;

    try {
        app.parse(static_cast<int>(argv.size()), const_cast<char**>(argv.data()));
        FAIL("Expected CLI::CallForHelp to be thrown");
    } catch (const CLI::CallForHelp&) {
        gotHelp = true;
    } catch (const CLI::ParseError& e) {
        gotParseError = true;
        parseErrorMsg = e.what();
    }

    CHECK(gotHelp);
    if (gotParseError) {
        FAIL("Unexpected parse error: " << parseErrorMsg);
    }
}
