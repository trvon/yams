// Failures must exit non-zero: scripts and agents read the exit code, not the
// text. Each row here exited 0 in the 2026-11-05 CLI surface review.
#include "../../common/test_helpers_catch2.h"
#include <catch2/catch_test_macros.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#ifndef _WIN32
#include <sys/wait.h>
#endif

namespace {

struct Captured {
    int rc;
    std::string out;
    std::string err;
};

Captured runCli(std::vector<std::string> args) {
    std::ostringstream out;
    std::ostringstream err;
    std::istringstream in;
    auto* oldOut = std::cout.rdbuf(out.rdbuf());
    auto* oldErr = std::cerr.rdbuf(err.rdbuf());
    auto* oldIn = std::cin.rdbuf(in.rdbuf());
    int rc = 0;
    {
        yams::cli::YamsCLI cli;
        std::vector<char*> argv;
        for (auto& arg : args)
            argv.push_back(arg.data());
        rc = cli.run(static_cast<int>(argv.size()), argv.data());
    }
    std::cout.rdbuf(oldOut);
    std::cerr.rdbuf(oldErr);
    std::cin.rdbuf(oldIn);
    std::cin.clear();
    return {rc, out.str(), err.str()};
}

std::filesystem::path builtYamsCli() {
    const auto root = yams::config::getenv_optional("MESON_BUILD_ROOT");
    if (!root || root->empty())
        return {};
#ifdef _WIN32
    const auto cli = std::filesystem::path(*root) / "tools" / "yams-cli" / "yams-cli.exe";
#else
    const auto cli = std::filesystem::path(*root) / "tools" / "yams-cli" / "yams-cli";
#endif
    std::error_code ec;
    return std::filesystem::exists(cli, ec) ? cli : std::filesystem::path{};
}

// Exit status of a shell command, stdout and stderr discarded.
int runSubprocess(const std::string& command) {
#ifdef _WIN32
    const int status = std::system((command + " >NUL 2>&1 <NUL").c_str());
    return status;
#else
    const int status = std::system((command + " >/dev/null 2>&1 </dev/null").c_str());
    return WIFEXITED(status) ? WEXITSTATUS(status) : 128;
#endif
}

struct EmbeddedCorpus {
    std::filesystem::path root = yams::test::make_temp_dir("yams_exit_codes_");
    std::string data = (root / "data").string();
    yams::test::ScopedEnvVar embedded{"YAMS_EMBEDDED", std::string("1")};
    yams::test::ScopedEnvVar inDaemon{"YAMS_IN_DAEMON", std::nullopt};
    yams::test::ScopedEnvVar socket{"YAMS_DAEMON_SOCKET", std::nullopt};
    yams::test::ScopedEnvVar socketAlias{"YAMS_DAEMON_SOCKET_PATH", std::nullopt};
    yams::test::ScopedEnvVar config{"YAMS_CONFIG", (root / "config.toml").string()};
    yams::test::ScopedEnvVar dataEnv{"YAMS_DATA_DIR", data};
    yams::test::ScopedEnvVar storage{"YAMS_STORAGE", data};
    yams::test::ScopedEnvVar vectors{"YAMS_DISABLE_VECTORS", std::string("1")};
    yams::test::ScopedEnvVar models{"YAMS_SKIP_MODEL_LOADING", std::string("1")};
    yams::test::ScopedEnvVar watcher{"YAMS_DISABLE_SESSION_WATCHER", std::string("1")};
    yams::test::ScopedEnvVar autostart{"YAMS_CLI_DISABLE_DAEMON_AUTOSTART", std::string("1")};
    yams::test::ScopedEnvVar nonInteractive{"YAMS_NON_INTERACTIVE", std::string("1")};

    ~EmbeddedCorpus() {
        std::error_code ec;
        std::filesystem::remove_all(root, ec);
    }
};

} // namespace

TEST_CASE("Failed invocations exit non-zero", "[cli][exit-code][contract]") {
    EmbeddedCorpus corpus;

    SECTION("add of a path that does not exist") {
        const auto missing = (corpus.root / "no-such-file.txt").string();
        const auto r = runCli({"yams", "--data-dir", corpus.data, "add", missing});
        INFO(r.out << r.err);
        CHECK(r.rc != 0);
        CHECK(r.out.find("queued") == std::string::npos);
    }
    SECTION("delete with no selector is a usage error") {
        const auto r = runCli({"yams", "--data-dir", corpus.data, "delete"});
        INFO(r.out << r.err);
        CHECK(r.rc == 2);
    }
    SECTION("bare yams prints usage and exits 2") {
        const auto r = runCli({"yams"});
        CHECK(r.rc == 2);
        CHECK(r.err.find("Usage") != std::string::npos);
    }
    SECTION("unimplemented auth subcommands exit non-zero") {
        // Run the real binary: an in-process std::exit(0) would end this test
        // runner with a passing status.
        const auto cli = builtYamsCli();
        if (cli.empty())
            SKIP("MESON_BUILD_ROOT is not set; run through meson test");
        // Required arguments are supplied so each stub's callback actually runs.
        for (const std::string sub :
             {"keygen", "list-keys", "revoke some-key-id", "token", "api-key --name probe"}) {
            INFO("auth " << sub);
            CHECK(runSubprocess("\"" + cli.string() + "\" auth " + sub) == 1);
        }
    }
    SECTION("global options without a command are a usage error") {
        const auto r = runCli({"yams", "--data-dir", corpus.data, "--json"});
        CHECK(r.rc == 2);
        CHECK(r.err.find("Usage") != std::string::npos);
    }
    SECTION("an unknown option is rejected before the command runs") {
        // config set writes from its parse callback.
        const auto r = runCli({"yams", "--data-dir", corpus.data, "config", "set",
                               "cli.budget_chars", "7", "--no-such-flag"});
        CHECK(r.rc == 2);
        std::ifstream config(corpus.root / "config.toml");
        const std::string written((std::istreambuf_iterator<char>(config)), {});
        CHECK(written.find("budget_chars") == std::string::npos);
    }
    SECTION("an unknown flag after a subcommand is rejected") {
        const auto r =
            runCli({"yams", "--data-dir", corpus.data, "search", "--no-such-flag", "evidence"});
        INFO(r.out << r.err);
        CHECK(r.rc != 0);
        CHECK(r.err.find("--no-such-flag") != std::string::npos);
    }
}
