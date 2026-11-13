#include <catch2/catch_test_macros.hpp>

#include <yams/cli/command_catalog.h>
#include <yams/cli/command_registry.h>
#include <yams/cli/yams_cli.h>

#include <algorithm>
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

TEST_CASE("YamsCLI rejects unknown arguments instead of silently ignoring them",
          "[cli][parser][audit]") {
    CliTestHelper helper;
    CaptureStdout capture;
    SECTION("Unknown global option") {
        CHECK(helper.runCommand({"yams", "--definitely-invalid", "completion", "bash"}) != 0);
    }
    SECTION("Unknown command option") {
        CHECK(helper.runCommand({"yams", "completion", "bash", "--definitely-invalid"}) != 0);
    }
    SECTION("Multiple top-level commands") {
        CHECK(helper.runCommand({"yams", "completion", "bash", "completion", "zsh"}) != 0);
    }
    CHECK(capture.str().empty());
}

TEST_CASE("YamsCLI treats command names as retrieval arguments", "[cli][parser][audit]") {
    CliTestHelper helper;
    for (const std::string command : {"grep", "get", "cat", "list", "search"}) {
        INFO(command);
        CaptureStdout capture;
        const auto result = helper.runCommand({"yams", "--json", command, "completion", "--help"});
        CHECK(result == 0);
        CHECK(capture.str().find("Usage: yams " + command) != std::string::npos);
    }
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
    SECTION("full p2p help still works") {
        runHelp("p2p");
    }
}

namespace {

// One entry per line under "Subcommands:" in `yams --help`: the primary
// name first, then its aliases ("delete, rm" yields {"delete", "rm"}).
std::vector<std::vector<std::string>> helpListedCommandNames(const std::string& help) {
    std::vector<std::vector<std::string>> lines;
    std::istringstream in(help);
    std::string line;
    bool inSubcommands = false;
    while (std::getline(in, line)) {
        if (line.rfind("Subcommands:", 0) == 0) {
            inSubcommands = true;
            continue;
        }
        if (!inSubcommands || line.size() < 3 || line[0] != ' ' || line[2] == ' ')
            continue;
        const auto end = line.find("  ", 2);
        std::string field = line.substr(2, end == std::string::npos ? end : end - 2);
        std::vector<std::string> names;
        std::size_t start = 0;
        while (start < field.size()) {
            auto comma = field.find(',', start);
            auto token = field.substr(start, comma == std::string::npos ? comma : comma - start);
            token.erase(0, token.find_first_not_of(' '));
            token.erase(token.find_last_not_of(' ') + 1);
            if (!token.empty())
                names.push_back(token);
            if (comma == std::string::npos)
                break;
            start = comma + 1;
        }
        if (!names.empty())
            lines.push_back(std::move(names));
    }
    return lines;
}

} // namespace

TEST_CASE("YamsCLI - every command named in --help dispatches", "[cli][registry][catch2]") {
    CliTestHelper helper;
    std::string help;
    {
        CaptureStdout capture;
        REQUIRE(helper.runCommand({"yams", "--help"}) == 0);
        help = capture.str();
    }
    const auto lines = helpListedCommandNames(help);
    REQUIRE(lines.size() >= 20);

    const auto catalog = yams::cli::topLevelCommandNames();
    for (const auto& names : lines) {
        INFO("primary command: " << names.front());
        CHECK(std::find(catalog.begin(), catalog.end(), names.front()) != catalog.end());
        for (const auto& name : names) {
            INFO("command: " << name);
            CHECK(yams::cli::CommandRegistry::isRegisteredTopLevelCommand(name));
            CaptureStdout capture;
            CHECK(helper.runCommand({"yams", name, "--help"}) == 0);
        }
    }
}

TEST_CASE("YamsCLI - every completion candidate dispatches", "[cli][registry][catch2]") {
    CliTestHelper helper;
    for (const auto& name : yams::cli::topLevelCommandNames()) {
        INFO("completion candidate: " << name);
        CaptureStdout capture;
        CHECK(helper.runCommand({"yams", name, "--help"}) == 0);
    }
}
