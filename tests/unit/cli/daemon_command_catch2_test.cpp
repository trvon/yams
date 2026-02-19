// CLI Daemon Command tests
// Catch2 migration from GTest (yams-3s4 / yams-cli)
//
// Tests for daemon command CLI parsing.

#include <catch2/catch_test_macros.hpp>

#include <CLI/CLI.hpp>

#include <yams/cli/command.h>

#include <memory>
#include <string>
#include <vector>

// Factory is declared in command registry translation unit
namespace yams::cli {
std::unique_ptr<ICommand> createDaemonCommand();
}

using yams::cli::ICommand;

namespace {

CLI::App* getSub(CLI::App& app, const std::string& name) {
    for (auto* sub : app.get_subcommands()) {
        if (sub->get_name() == name)
            return sub;
    }
    return nullptr;
}

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
