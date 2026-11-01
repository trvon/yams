#include <fstream>
#include <functional>
#include "../../common/test_helpers_catch2.h"
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <catch2/catch_test_macros.hpp>
#include <CLI/CLI.hpp>
#include <yams/cli/command.h>
#include <yams/cli/repo_files.h>
#include <yams/cli/yams_cli.h>
#include <yams/crypto/hasher.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

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

TEST_CASE("add --changed retires a missing path without deleting shared content",
          "[cli][retrieval][path-retirement]") {
    using namespace yams;
    const auto root = std::filesystem::canonical(test::make_temp_dir("cli-path-retirement-"));
    struct Cleanup {
        std::filesystem::path path;
        ~Cleanup() {
            std::error_code error;
            std::filesystem::remove_all(path, error);
        }
    } cleanup{root};
    const auto data = root / "data";
    const auto worktree = root / "worktree";
    std::filesystem::create_directories(data);
    std::filesystem::create_directories(worktree);
    REQUIRE(cli::runCliProcess({"git", "init", "--quiet", worktree.string()}).exitCode == 0);

    // Pin an isolated endpoint and disable autostart to protect the operator's
    // daemon. Assert path/content state below, not a no-RPC product contract.
    const auto isolatedSocket =
        std::filesystem::temp_directory_path() /
        ("ypr410-" + std::to_string(std::hash<std::string>{}(root.string())) + ".sock");
    REQUIRE_FALSE(std::filesystem::exists(isolatedSocket));
    test::ScopedEnvVar socket("YAMS_DAEMON_SOCKET_PATH", isolatedSocket.string());
    test::ScopedEnvVar noAutostart("YAMS_CLI_DISABLE_DAEMON_AUTOSTART", std::string("1"));
    test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::string("0"));
    test::ScopedEnvVar config("YAMS_CONFIG", (root / "config.toml").string());
    test::ScopedEnvVar configPath("YAMS_CONFIG_PATH", (root / "config.toml").string());

    const auto oldPath = worktree / "old.txt";
    auto livePath = root / "other-worktree" / "live.txt";
    SECTION("a live alias outside this worktree preserves its shared CAS identity") {}
    SECTION("an already ingested Git rename retires its old location") {
        std::ofstream(oldPath) << "shared bytes\n";
        REQUIRE(cli::runCliProcess({"git", "-C", worktree.string(), "add", "old.txt"}).exitCode ==
                0);
        REQUIRE(cli::runCliProcess({"git", "-C", worktree.string(), "mv", "old.txt", "renamed.txt"})
                    .exitCode == 0);
        livePath = worktree / "renamed.txt";
    }
    std::filesystem::create_directories(livePath.parent_path());
    std::ofstream(livePath) << "shared bytes\n";
    if (livePath.parent_path() == worktree) {
        REQUIRE(
            cli::runCliProcess({"git", "-C", worktree.string(), "add", "renamed.txt"}).exitCode ==
            0);
    }
    const auto hash = crypto::createSHA256Hasher()->hashFile(livePath);
    metadata::ConnectionPool pool((data / "yams.db").string());
    REQUIRE(pool.initialize());
    metadata::MetadataRepository repository(pool);
    metadata::DocumentInfo original;
    original.filePath = oldPath.generic_string();
    original.fileName = "old.txt";
    original.sha256Hash = hash;
    const auto derived = metadata::computePathDerivedValues(original.filePath);
    original.pathPrefix = derived.pathPrefix;
    original.reversePath = derived.reversePath;
    original.pathHash = derived.pathHash;
    original.parentHash = derived.parentHash;
    original.pathDepth = derived.pathDepth;
    auto inserted = repository.insertDocument(original);
    REQUIRE(inserted);
    REQUIRE(repository.recordDocumentPath(original, inserted.value(), {}));
    auto alias = original;
    alias.filePath = livePath.generic_string();
    alias.fileName = livePath.filename().string();
    REQUIRE(repository.recordDocumentPath(alias, inserted.value(), {}));

    boost::asio::io_context io;
    cli::YamsCLI cli;
    cli.setDataPath(data);
    auto freshness = cli.repoFreshness(cli::readRepoFiles(worktree, true));
    REQUIRE(freshness);
    REQUIRE(freshness.value().changed.empty());
    REQUIRE(freshness.value().missing == std::vector<std::filesystem::path>{oldPath});
    CLI::App app;
    auto command = cli::createAddCommand();
    command->setExecutor(io.get_executor());
    command->registerCommand(app, &cli);
    const auto directory = worktree.string();
    const char* args[]{"yams", "add", "--changed", directory.c_str()};
    REQUIRE_NOTHROW(app.parse(4, args));
    auto completion = boost::asio::co_spawn(io, command->executeAsync(), boost::asio::use_future);
    io.run();
    auto result = completion.get();
    INFO((result ? "completed" : result.error().message));
    // The baseline may fail its unsafe hash-delete RPC on the isolated endpoint.
    // The regression is the still-visible path, independent of transport success.
    auto missing = repository.findDocumentByExactPath(oldPath.generic_string());
    REQUIRE(missing);
    CHECK_FALSE(missing.value());
    auto live = repository.findDocumentByExactPath(livePath.generic_string());
    REQUIRE(live);
    REQUIRE(live.value());
    CHECK(live.value()->sha256Hash == hash);
    auto content = repository.getDocumentByHash(hash);
    REQUIRE(content);
    REQUIRE(content.value());
    CHECK(result.has_value());
    auto references = repository.getDocumentPathReferences(inserted.value());
    REQUIRE(references);
    REQUIRE(references.value().size() == 1);
    CHECK(references.value().front().filePath == livePath.generic_string());
    auto refreshed = cli.repoFreshness(cli::readRepoFiles(worktree, true));
    REQUIRE(refreshed);
    CHECK_FALSE(refreshed.value().stale());
}

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
