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
#include <yams/compat/unistd.h>

#include "../../common/test_helpers_catch2.h"

#ifdef _WIN32
#define WIFEXITED(x) ((x) != -1)
#define WEXITSTATUS(x) (x)
#else
#include <sys/wait.h>
#endif

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

auto findBuiltYamsCli() -> fs::path {
    std::vector<fs::path> bases;
    std::error_code ec;
    auto cwd = fs::current_path(ec);
    if (!ec) {
        bases.push_back(cwd);
        if (cwd.parent_path().filename() == "builddir-tsan" ||
            cwd.parent_path().filename() == "builddir-asan" ||
            cwd.parent_path().filename() == "builddir-ubsan" ||
            cwd.parent_path().filename() == "builddir-nosan" ||
            cwd.parent_path().filename() == "builddir") {
            bases.push_back(cwd.parent_path().parent_path());
            bases.push_back(cwd.parent_path());
        }
        if (cwd.filename() == "asan" || cwd.filename() == "debug" || cwd.filename() == "release" ||
            cwd.filename() == "coverage") {
            bases.push_back(cwd.parent_path().parent_path());
        } else if (cwd.filename() == "builddir" || cwd.filename() == "builddir-nosan" ||
                   cwd.filename() == "builddir-asan" || cwd.filename() == "builddir-tsan" ||
                   cwd.filename() == "builddir-ubsan") {
            bases.push_back(cwd.parent_path());
        }
    }

    if (const char* sourceRoot = std::getenv("MESON_SOURCE_ROOT")) {
        bases.emplace_back(sourceRoot);
    }

    const std::vector<fs::path> suffixes = {
        fs::path("build") / "debug" / "tools" / "yams-cli" / "yams-cli",
        fs::path("tools") / "yams-cli" / "yams-cli",
        fs::path("builddir-tsan") / "tools" / "yams-cli" / "yams-cli",
        fs::path("builddir-asan") / "tools" / "yams-cli" / "yams-cli",
        fs::path("builddir") / "tools" / "yams-cli" / "yams-cli",
        fs::path("builddir-nosan") / "tools" / "yams-cli" / "yams-cli",
        fs::path("build") / "asan" / "tools" / "yams-cli" / "yams-cli",
        fs::path("build") / "coverage" / "tools" / "yams-cli" / "yams-cli",
        fs::path("build") / "release" / "tools" / "yams-cli" / "yams-cli",
    };

    for (const auto& base : bases) {
        for (const auto& suffix : suffixes) {
            const auto candidate = base / suffix;
            if (fs::exists(candidate)) {
                return candidate;
            }
        }
    }
    return {};
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
    CHECK_THAT(output, ContainsSubstring("'restore'"));
    CHECK_THAT(output, ContainsSubstring("'grep'"));
    CHECK_THAT(output, ContainsSubstring("'status'"));
    CHECK_THAT(output, ContainsSubstring("'daemon'"));
    CHECK_THAT(output, ContainsSubstring("'doctor'"));
    CHECK_THAT(output, ContainsSubstring("'graph'"));
    CHECK_THAT(output, ContainsSubstring("'diff'"));
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

TEST_CASE("CompletionCommand - bash completes nested subcommands from command tree",
          "[cli][completion][catch2]") {
#ifdef _WIN32
    SKIP("bash nested completion smoke test requires Unix shell support");
#else
    const fs::path yamsCli = findBuiltYamsCli();
    REQUIRE_FALSE(yamsCli.empty());

    const fs::path tempDir = yams::test::make_temp_dir("yams_bash_completion_tree_");
    const fs::path dataDir = tempDir / "data";
    fs::create_directories(dataDir);
    const fs::path script = tempDir / "yams.bash";
    const fs::path out = tempDir / "bash-tree.out";

    const std::string cmd =
        "env YAMS_CONFIG='" + (tempDir / "missing-config.toml").string() + "' YAMS_DATA_DIR='" +
        dataDir.string() +
        "' YAMS_NON_INTERACTIVE=1 YAMS_CLI_DISABLE_DAEMON_AUTOSTART=1 bash -lc "
        "'set -euo pipefail; \"" +
        yamsCli.string() + "\" completion bash > \"" + script.string() + "\"; source \"" +
        script.string() +
        "\"; COMP_WORDS=(yams config em); COMP_CWORD=2; _yams_completion; printf \"CONFIG:%s\\n\" "
        "\"${COMPREPLY[*]}\"; COMP_WORDS=(yams plugin trust re); COMP_CWORD=3; "
        "_yams_completion; printf \"PLUGIN:%s\\n\" \"${COMPREPLY[*]}\"' > '" +
        out.string() + "'";

    const int rc = std::system(cmd.c_str());
    REQUIRE(WIFEXITED(rc));
    REQUIRE((WEXITSTATUS(rc) == 0));

    std::ifstream input(out);
    REQUIRE(input.good());
    std::string configLine;
    std::string pluginLine;
    std::getline(input, configLine);
    std::getline(input, pluginLine);

    CHECK_THAT(configLine, ContainsSubstring("CONFIG:embeddings"));
    CHECK_THAT(pluginLine, ContainsSubstring("PLUGIN:remove"));
    CHECK_THAT(pluginLine, ContainsSubstring("reset"));

    std::error_code ec;
    fs::remove_all(tempDir, ec);
#endif
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
    CHECK_THAT(zshOutput, ContainsSubstring("_yams_subcommand_names_for()"));
    CHECK_THAT(zshOutput, ContainsSubstring("command_path=\"$(_yams_command_path $CURRENT)\""));
    CHECK_THAT(zshOutput, ContainsSubstring("compdef _yams yams"));
    CHECK_THAT(zshOutput, ContainsSubstring("'config')"));
    CHECK_THAT(zshOutput, ContainsSubstring("'embeddings:Manage embedding configuration'"));
    CHECK_THAT(zshOutput, ContainsSubstring("'plugin trust')"));
    CHECK_THAT(zshOutput, ContainsSubstring("'remove:Remove a trusted plugin path'"));

    CaptureStdout fishCapture;
    const int fishRc = helper.runCommand({"yams", "completion", "fish"});
    const std::string fishOutput = fishCapture.str();
    REQUIRE((fishRc == 0));
    CHECK_THAT(fishOutput,
               ContainsSubstring("__fish_yams_path_is status; or __fish_yams_path_is stats"));
    CHECK_THAT(fishOutput, ContainsSubstring("-l no-physical"));
    CHECK_THAT(fishOutput, ContainsSubstring("-l corpus"));
    CHECK((fishOutput.find("__fish_yams_path_is stats' -l format") == std::string::npos));
    CHECK_THAT(fishOutput, ContainsSubstring("case 'config'"));
    CHECK_THAT(fishOutput, ContainsSubstring("'embeddings' 'Manage embedding configuration'"));
    CHECK_THAT(fishOutput, ContainsSubstring("case 'plugin trust'"));
    CHECK_THAT(fishOutput, ContainsSubstring("'remove' 'Remove a trusted plugin path'"));
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
    CHECK_THAT(output, ContainsSubstring("'config' = @("));
    CHECK_THAT(output, ContainsSubstring("'plugin trust' = @("));
    CHECK_THAT(output, ContainsSubstring("--no-physical"));
    CHECK_THAT(output, ContainsSubstring("--corpus"));
    CHECK((output.find("'stats' = @(\n            @{ Name = '--format'") == std::string::npos));
}

TEST_CASE("CompletionCommand - sourced zsh completion registers compdef",
          "[cli][completion][catch2]") {
#ifdef _WIN32
    SKIP("zsh completion smoke test requires zsh and Unix process substitution");
#else
    const fs::path yamsCli = findBuiltYamsCli();
    REQUIRE_FALSE(yamsCli.empty());

    const fs::path tempDir = yams::test::make_temp_dir("yams_zsh_completion_source_");
    const fs::path dataDir = tempDir / "data";
    fs::create_directories(dataDir);
    const fs::path configPath = tempDir / "missing-config.toml";
    const fs::path out = tempDir / "zsh-sourced-registration.out";
    const fs::path err = tempDir / "zsh-sourced-registration.err";
    const std::string cmd =
        "env YAMS_CONFIG='" + configPath.string() + "' YAMS_DATA_DIR='" + dataDir.string() +
        "' YAMS_NON_INTERACTIVE=1 YAMS_CLI_DISABLE_DAEMON_AUTOSTART=1 "
        "zsh -fc 'autoload -U compinit; compinit; source <(\"" +
        yamsCli.string() + "\" completion zsh); print -- ${_comps[yams]:-missing}' > '" +
        out.string() + "' 2> '" + err.string() + "'";

    const int rc = std::system(cmd.c_str());
    REQUIRE(WIFEXITED(rc));
    REQUIRE((WEXITSTATUS(rc) == 0));

    std::ifstream input(out);
    REQUIRE(input.good());
    std::string registered;
    std::getline(input, registered);
    CHECK((registered == "_yams"));

    std::ifstream errInput(err);
    REQUIRE(errInput.good());
    const std::string stderrOutput((std::istreambuf_iterator<char>(errInput)),
                                   std::istreambuf_iterator<char>());
    CHECK(stderrOutput.empty());

    std::error_code ec;
    fs::remove_all(tempDir, ec);
#endif
}
