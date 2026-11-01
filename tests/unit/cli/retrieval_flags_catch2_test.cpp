#include <fstream>
#include "../../common/test_helpers_catch2.h"
#include <catch2/catch_test_macros.hpp>
#include <CLI/CLI.hpp>
#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>

namespace yams::cli {
std::unique_ptr<ICommand> createSearchCommand();
std::unique_ptr<ICommand> createGrepCommand();
std::unique_ptr<ICommand> createListCommand();
std::unique_ptr<ICommand> createGetCommand();
std::unique_ptr<ICommand> createCatCommand();
std::unique_ptr<ICommand> createGraphCommand();
std::unique_ptr<ICommand> createAddCommand();
std::unique_ptr<ICommand> createStatusCommand();
} // namespace yams::cli

TEST_CASE("Every retrieval command exposes its budget and quiet flags", "[cli][retrieval][flags]") {
    using namespace yams::cli;
    YamsCLI cli;
    using Factory = std::unique_ptr<ICommand> (*)();
    for (Factory factory : {createSearchCommand, createGrepCommand, createListCommand,
                            createGetCommand, createCatCommand, createGraphCommand}) {
        CLI::App root;
        auto command = factory();
        command->registerCommand(root, &cli);
        auto* sub = root.get_subcommand(command->getName());
        INFO(command->getName());
        for (const auto* flag : {"--budget-chars", "--quiet"}) {
            REQUIRE(sub->get_option_no_throw(flag) != nullptr);
            CHECK_FALSE(sub->get_option(flag)->get_description().empty());
        }
    }
}

TEST_CASE("Retrieval budget environment defaults yield to explicit CLI values",
          "[cli][retrieval][flags]") {
    yams::test::ScopedEnvVar budget("YAMS_BUDGET_CHARS", std::string("120"));
    yams::cli::YamsCLI cli;
    CHECK(cli.outputBudgetChars() == 120);
    CLI::App app;
    cli.registerReadOptions(app);
    const char* args[]{"yams", "--budget-chars", "50"};
    REQUIRE_NOTHROW(app.parse(3, args));
    CHECK(cli.outputBudgetChars() == 50);
}

TEST_CASE("Retrieval config permits explicitly empty ephemeral exclusions",
          "[cli][retrieval][config]") {
    const auto root = yams::test::make_temp_dir("cli-retrieval-config-");
    struct Cleanup {
        std::filesystem::path path;
        ~Cleanup() {
            std::error_code error;
            std::filesystem::remove_all(path, error);
        }
    } cleanup{root};
    const auto config = root / "config.toml";
    std::ofstream(config) << "[version]\nconfig_version = 3\n[search]\nexclude_paths = []\n";
    yams::test::ScopedEnvVar selected("YAMS_CONFIG", config.string());
    yams::test::ScopedEnvVar compatibility("YAMS_CONFIG_PATH", std::nullopt);
    yams::cli::YamsCLI cli;
    CHECK(cli.retrievalOutputConfig().excludePaths.empty());
}

TEST_CASE("Retrieval selection flags are discoverable through help", "[cli][retrieval][flags]") {
    using namespace yams::cli;
    YamsCLI cli;
    CLI::App root;
    auto grep = createGrepCommand();
    auto search = createSearchCommand();
    auto cat = createCatCommand();
    auto get = createGetCommand();
    grep->registerCommand(root, &cli);
    search->registerCommand(root, &cli);
    cat->registerCommand(root, &cli);
    get->registerCommand(root, &cli);
    for (const auto* command : {"grep", "search"}) {
        auto* sub = root.get_subcommand(command);
        REQUIRE(sub->get_option_no_throw("--compact") != nullptr);
        REQUIRE(sub->get_option_no_throw("--include-ephemeral") != nullptr);
        REQUIRE(sub->get_option_no_throw("--worktree") != nullptr);
    }
    REQUIRE(root.get_subcommand("grep")->get_option_no_throw("--max-results") != nullptr);
    REQUIRE(root.get_subcommand("search")->get_option_no_throw("--scores") != nullptr);
    auto add = createAddCommand();
    auto status = createStatusCommand();
    add->registerCommand(root, &cli);
    status->registerCommand(root, &cli);
    REQUIRE(root.get_subcommand("status")->get_option_no_throw("--repo") != nullptr);
    auto* changed = root.get_subcommand("add")->get_option("--changed");
    CHECK(changed->get_expected_min() == 0);
    CHECK(changed->get_expected_max() == 1);
    const char* args[]{"yams", "add", "--changed"};
    REQUIRE_NOTHROW(root.parse(3, args));
    CHECK(changed->count() == 1);
    for (const auto* command : {"cat", "get"}) {
        auto* sub = root.get_subcommand(command);
        for (const auto* flag : {"--max-bytes", "--lines", "--around", "--context"})
            REQUIRE(sub->get_option_no_throw(flag) != nullptr);
    }
}
