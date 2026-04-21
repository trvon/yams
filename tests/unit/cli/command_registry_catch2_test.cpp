#include <catch2/catch_test_macros.hpp>

#include <yams/cli/command_registry.h>
#include <yams/cli/yams_cli.h>

#include <filesystem>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "../../common/test_helpers_catch2.h"

namespace fs = std::filesystem;

namespace {

struct CliTestHelper {
    fs::path tempDir;
    fs::path dataDir;
    fs::path configPath;
    std::optional<yams::test::ScopedEnvVar> configEnv;
    std::optional<yams::test::ScopedEnvVar> dataEnv;
    std::optional<yams::test::ScopedEnvVar> nonInteractiveEnv;
    std::optional<yams::test::ScopedEnvVar> disableDaemonEnv;

    CliTestHelper() {
        tempDir = yams::test::make_temp_dir("yams_command_registry_catch2_test_");
        dataDir = tempDir / "data";
        configPath = tempDir / "config.toml";
        fs::create_directories(dataDir);

        configEnv.emplace("YAMS_CONFIG", configPath.string());
        dataEnv.emplace("YAMS_DATA_DIR", dataDir.string());
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

TEST_CASE("CommandRegistry - minimal fast path names stay registered", "[cli][registry][catch2]") {
    yams::cli::YamsCLI cli;

    CHECK(yams::cli::CommandRegistry::registerMinimalCommandSet(&cli, "status"));
    CHECK(yams::cli::CommandRegistry::registerMinimalCommandSet(&cli, "list"));
    CHECK(yams::cli::CommandRegistry::registerMinimalCommandSet(&cli, "search"));
    CHECK_FALSE(yams::cli::CommandRegistry::registerMinimalCommandSet(&cli, "graph"));
    CHECK_FALSE(yams::cli::CommandRegistry::registerMinimalCommandSet(&cli, "missing"));
}

TEST_CASE("YamsCLI - command registration preserves help entrypoints", "[cli][registry][catch2]") {
    CliTestHelper helper;

    const auto runHelp = [&](const std::string& commandName) {
        CaptureStdout capture;
        const int rc = helper.runCommand({"yams", commandName, "--help"});
        const std::string output = capture.str();
        CHECK(rc == 0);
        CHECK_FALSE(output.empty());
        CHECK(output.find(commandName) != std::string::npos);
        CHECK(output.find("terminate") == std::string::npos);
    };

    SECTION("minimal status help works") {
        runHelp("status");
    }
    SECTION("minimal list help works") {
        runHelp("list");
    }
    SECTION("minimal search help works") {
        runHelp("search");
    }
    SECTION("full graph help still works") {
        runHelp("graph");
    }
}
