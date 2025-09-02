#include <CLI/CLI.hpp>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include <yams/cli/command.h>

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

TEST(DaemonCommandCLITest, ParsesUnorderedFlagsWithHelpWithoutExecuting) {
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
    try {
        app.parse(static_cast<int>(argv.size()), const_cast<char**>(argv.data()));
        FAIL() << "Expected CLI::CallForHelp to be thrown";
    } catch (const CLI::CallForHelp&) {
        SUCCEED();
    } catch (const CLI::ParseError& e) {
        FAIL() << "Unexpected parse error: " << e.what();
    }
}

TEST(DaemonCommandCLITest, ParsesEqualsStyleFlagsWithHelp) {
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

    try {
        app.parse(static_cast<int>(argv.size()), const_cast<char**>(argv.data()));
        FAIL() << "Expected CLI::CallForHelp to be thrown";
    } catch (const CLI::CallForHelp&) {
        SUCCEED();
    } catch (const CLI::ParseError& e) {
        FAIL() << "Unexpected parse error: " << e.what();
    }
}
