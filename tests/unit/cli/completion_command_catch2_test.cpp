#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include <yams/cli/yams_cli.h>

#include "../../common/test_helpers_catch2.h"

using Catch::Matchers::ContainsSubstring;

namespace {

namespace fs = std::filesystem;

struct CompletionCliTestHelper {
    fs::path tempDir;
    std::optional<yams::test::ScopedEnvVar> configEnv;
    std::optional<yams::test::ScopedEnvVar> dataEnv;
    std::optional<yams::test::ScopedEnvVar> nonInteractiveEnv;
    std::optional<yams::test::ScopedEnvVar> disableDaemonEnv;

    CompletionCliTestHelper() {
        tempDir = yams::test::make_temp_dir("yams_cli_completion_catch2_test_");
        fs::create_directories(tempDir / "data");

        {
            std::ofstream cfg(tempDir / "config.toml");
            cfg << "[core]\n";
            cfg << "data_dir = \"" << (tempDir / "data").string() << "\"\n";
        }

        configEnv.emplace("YAMS_CONFIG", (tempDir / "config.toml").string());
        dataEnv.emplace("YAMS_DATA_DIR", (tempDir / "data").string());
        nonInteractiveEnv.emplace(std::string("YAMS_NON_INTERACTIVE"),
                                  std::optional<std::string>("1"));
        disableDaemonEnv.emplace(std::string("YAMS_CLI_DISABLE_DAEMON_AUTOSTART"),
                                 std::optional<std::string>("1"));
    }

    ~CompletionCliTestHelper() {
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

std::string lineContaining(const std::string& text, std::string_view needle) {
    const auto pos = text.find(needle);
    if (pos == std::string::npos) {
        return {};
    }
    const auto lineEnd = text.find('\n', pos);
    return text.substr(pos, lineEnd == std::string::npos ? std::string::npos : lineEnd - pos);
}

} // namespace

TEST_CASE("CompletionCommand - bash script includes registered top-level commands",
          "[cli][completion][catch2]") {
    CompletionCliTestHelper helper;
    CaptureStdout capture;

    const int rc = helper.runCommand({"yams", "completion", "bash"});
    const std::string output = capture.str();

    REQUIRE((rc == 0));
    CHECK((output.rfind("#!/bin/bash\n", 0) == 0));
    CHECK((output.find("Configuration successfully migrated") == std::string::npos));
    CHECK_THAT(output, ContainsSubstring("restore "));
    CHECK_THAT(output, ContainsSubstring("grep "));
    CHECK_THAT(output, ContainsSubstring("status "));
    CHECK_THAT(output, ContainsSubstring("daemon "));
    CHECK_THAT(output, ContainsSubstring("doctor "));
    CHECK_THAT(output, ContainsSubstring("graph "));
    CHECK_THAT(output, ContainsSubstring("diff "));
}

TEST_CASE("CompletionCommand - bash status and stats use current flags",
          "[cli][completion][catch2]") {
    CompletionCliTestHelper helper;
    CaptureStdout capture;

    const int rc = helper.runCommand({"yams", "completion", "bash"});
    const std::string output = capture.str();

    REQUIRE((rc == 0));
    const std::string statusLine = lineContaining(output, "local status_flags=");
    REQUIRE_FALSE(statusLine.empty());

    CHECK_THAT(statusLine, ContainsSubstring("--no-physical"));
    CHECK_THAT(statusLine, ContainsSubstring("--corpus"));
    CHECK((statusLine.find("--format") == std::string::npos));
    CHECK_THAT(output, ContainsSubstring("status|stats)"));
}

TEST_CASE("CompletionCommand - zsh and fish status aliases no longer advertise format",
          "[cli][completion][catch2]") {
    CompletionCliTestHelper helper;

    CaptureStdout zshCapture;
    const int zshRc = helper.runCommand({"yams", "completion", "zsh"});
    const std::string zshOutput = zshCapture.str();
    REQUIRE((zshRc == 0));
    CHECK((zshOutput.rfind("#compdef yams\n", 0) == 0));
    CHECK((zshOutput.find("Configuration successfully migrated") == std::string::npos));
    CHECK_THAT(zshOutput, ContainsSubstring("status|stats"));
    CHECK_THAT(zshOutput, ContainsSubstring("--no-physical"));
    CHECK_THAT(zshOutput, ContainsSubstring("--corpus"));
    CHECK((zshOutput.find("--format[Output format]:format:(text json)") == std::string::npos));
    CHECK_THAT(zshOutput, ContainsSubstring("case $line[1] in"));

    CaptureStdout fishCapture;
    const int fishRc = helper.runCommand({"yams", "completion", "fish"});
    const std::string fishOutput = fishCapture.str();
    REQUIRE((fishRc == 0));
    CHECK_THAT(
        fishOutput,
        ContainsSubstring("__fish_yams_using_command status; or __fish_yams_using_command stats"));
    CHECK_THAT(fishOutput, ContainsSubstring("-l no-physical"));
    CHECK_THAT(fishOutput, ContainsSubstring("-l corpus"));
    CHECK((fishOutput.find("__fish_yams_using_command stats' -l format") == std::string::npos));
}

TEST_CASE("CompletionCommand - PowerShell status aliases no longer advertise format",
          "[cli][completion][catch2]") {
    CompletionCliTestHelper helper;
    CaptureStdout capture;

    const int rc = helper.runCommand({"yams", "completion", "powershell"});
    const std::string output = capture.str();

    REQUIRE((rc == 0));
    CHECK_THAT(output, ContainsSubstring("'status' = @("));
    CHECK_THAT(output, ContainsSubstring("'stats' = @("));
    CHECK_THAT(output, ContainsSubstring("--no-physical"));
    CHECK_THAT(output, ContainsSubstring("--corpus"));
    CHECK((output.find("'stats' = @(\n            @{ Name = '--format'") == std::string::npos));
}
