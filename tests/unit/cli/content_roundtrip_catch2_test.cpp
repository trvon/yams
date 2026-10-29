#include <nlohmann/json.hpp>
#include "../../../tools/yams-cli/logging.h"
#include "../../common/test_helpers_catch2.h"
#include <catch2/catch_test_macros.hpp>
#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/crypto/hasher.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <span>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace yams::cli {
std::unique_ptr<ICommand> createCatCommand();
std::unique_ptr<ICommand> createGetCommand();
} // namespace yams::cli

TEST_CASE("CLI cat accepts exactly one content selector", "[cli][cat][parser]") {
    CLI::App app{"yams"};
    auto command = yams::cli::createCatCommand();
    command->registerCommand(app, nullptr);
    app.get_subcommand("cat")->callback([] {});
    const std::string hash(64, 'a');
    SECTION("Explicit hash hydration") {
        REQUIRE_NOTHROW(app.parse("cat --hash " + hash + " --raw"));
    }
    SECTION("Positional hash remains supported") {
        REQUIRE_NOTHROW(app.parse("cat " + hash));
    }
    SECTION("Positional path remains supported") {
        REQUIRE_NOTHROW(app.parse("cat evidence.txt"));
    }
    SECTION("Explicit hash prefix remains supported") {
        REQUIRE_NOTHROW(app.parse("cat --hash abcdef"));
    }
    SECTION("Explicit hash must be hexadecimal") {
        REQUIRE_THROWS_AS(app.parse("cat --hash " + std::string(64, 'z')), CLI::ParseError);
    }
    SECTION("Explicit hash must not be empty") {
        REQUIRE_THROWS_AS(app.parse("cat --hash"), CLI::ParseError);
    }
    SECTION("A selector is required") {
        REQUIRE_THROWS_AS(app.parse("cat --raw"), CLI::ParseError);
    }
    SECTION("Conflicting selectors are rejected") {
        REQUIRE_THROWS_AS(app.parse("cat evidence.txt --hash " + hash), CLI::ParseError);
    }
}

TEST_CASE("CLI get rejects conflicting selectors and retrieval modes",
          "[cli][get][parser][audit]") {
    CLI::App app{"yams"};
    auto command = yams::cli::createGetCommand();
    command->registerCommand(app, nullptr);
    app.get_subcommand("get")->callback([] {});
    const std::string hash(64, 'a');
    SECTION("Hash and name are conflicting selectors") {
        REQUIRE_THROWS_AS(app.parse("get --hash " + hash + " --name evidence.txt"),
                          CLI::ParseError);
    }
    SECTION("Positional and explicit selectors conflict") {
        REQUIRE_THROWS_AS(app.parse("get evidence.txt --hash " + hash), CLI::ParseError);
    }
    SECTION("Raw and extracted representations conflict") {
        REQUIRE_THROWS_AS(app.parse("get --hash " + hash + " --raw --extract"), CLI::ParseError);
    }
    SECTION("Version selection must be unambiguous") {
        REQUIRE_THROWS_AS(app.parse("get --hash " + hash + " --latest --oldest"), CLI::ParseError);
    }
    SECTION("Text and binary selection conflict") {
        REQUIRE_THROWS_AS(app.parse("get --hash " + hash + " --text --binary"), CLI::ParseError);
    }
    SECTION("Chunk size must not be zero") {
        REQUIRE_THROWS_AS(app.parse("get --hash " + hash + " --chunk-size 0"), CLI::ParseError);
    }
    SECTION("Chunk size must fit the IPC field") {
        REQUIRE_THROWS_AS(app.parse("get --hash " + hash + " --chunk-size 4294967296"),
                          CLI::ParseError);
    }
    SECTION("Explicit selection and raw rendering remain compatible") {
        REQUIRE_NOTHROW(app.parse("get --hash " + hash + " --raw"));
    }
    SECTION("Filter-only retrieval remains supported") {
        REQUIRE_NOTHROW(app.parse("get --mime text/plain"));
    }
    SECTION("Compound selection is not silently accepted by hydration") {
        REQUIRE_THROWS_AS(app.parse("get --hash " + hash + " --mime text/plain"), CLI::ParseError);
        REQUIRE_THROWS_AS(app.parse("get --type text --mime text/plain"), CLI::ParseError);
    }
}

namespace {
struct StreamCapture {
    std::ostringstream output;
    std::istringstream input;
    std::streambuf* oldOutput;
    std::streambuf* oldInput;
    explicit StreamCapture(const std::string& payload)
        : input(payload), oldOutput(std::cout.rdbuf(output.rdbuf())),
          oldInput(std::cin.rdbuf(input.rdbuf())) {}
    ~StreamCapture() {
        std::cout.rdbuf(oldOutput);
        std::cin.rdbuf(oldInput);
        std::cin.clear();
    }
};

std::pair<int, std::string> runCli(std::vector<std::string> args, const std::string& payload = {}) {
    yams::cli::YamsCLI cli;
    std::vector<char*> argv;
    for (auto& arg : args)
        argv.push_back(arg.data());
    StreamCapture capture(payload);
    const auto code = cli.run(static_cast<int>(argv.size()), argv.data());
    return {code, capture.output.str()};
}
} // namespace

TEST_CASE("CLI startup diagnostics do not contaminate content output", "[cli][logging]") {
    const auto previousLogger = spdlog::default_logger();
    std::ostringstream diagnostics;
    struct RestoreLogging {
        std::shared_ptr<spdlog::logger> previous;
        ~RestoreLogging() { spdlog::set_default_logger(std::move(previous)); }
    } restore{previousLogger};
    StreamCapture content("");
    yams::cli::initializeCliLogging(diagnostics);
    spdlog::warn("startup-warning-probe");
    spdlog::default_logger()->flush();
    std::cout << "immutable content";
    CHECK(content.output.str() == "immutable content");
    CHECK(diagnostics.str().find("startup-warning-probe") != std::string::npos);
}

TEST_CASE("CLI acknowledged stdin content hydrates immediately by full hash", "[cli][roundtrip]") {
    const auto root = yams::test::make_temp_dir("yams_content_roundtrip_");
    struct Cleanup {
        std::filesystem::path root;
        ~Cleanup() {
            std::error_code ec;
            std::filesystem::remove_all(root, ec);
        }
    } cleanup{root};
    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::string("1"));
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);
    yams::test::ScopedEnvVar socket("YAMS_DAEMON_SOCKET", std::nullopt);
    yams::test::ScopedEnvVar socketAlias("YAMS_DAEMON_SOCKET_PATH", std::nullopt);
    yams::test::ScopedEnvVar config("YAMS_CONFIG", (root / "config.toml").string());
    yams::test::ScopedEnvVar data("YAMS_DATA_DIR", (root / "data").string());
    yams::test::ScopedEnvVar storage("YAMS_STORAGE", (root / "data").string());
    yams::test::ScopedEnvVar vectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar models("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar watcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));
    yams::test::ScopedEnvVar autostart("YAMS_CLI_DISABLE_DAEMON_AUTOSTART", std::string("1"));
    std::string payload = "Immutable evidence\nsecond line\n\tindent\n";
    bool checkSearch = true;
    SECTION("Text is searchable and hydrates byte-for-byte") {}
    SECTION("Multi-chunk binary content preserves embedded NUL bytes") {
        payload.assign(131073, 'x');
        payload[65535] = '\0';
        payload[65536] = '\n';
        payload.back() = '\0';
        checkSearch = false;
    }
    const auto expectedHash =
        yams::crypto::SHA256Hasher::hash(std::as_bytes(std::span(payload.data(), payload.size())));
    const auto [added, output] =
        runCli({"yams", "--data-dir", (root / "data").string(), "--json", "add", "-", "--name",
                "evidence.txt", "--metadata", "task=cli-roundtrip,phase=checkpoint",
                "--no-embeddings", "--sync"},
               payload);
    INFO(output);
    REQUIRE(added == 0);
    const auto response = nlohmann::json::parse(output);
    REQUIRE(response.at("summary").at("failed") == 0);
    const auto hash = response.at("results").at(0).at("hash").get<std::string>();
    REQUIRE(hash.size() == 64);
    CHECK(hash == expectedHash);
    const auto [retrieved, content] =
        runCli({"yams", "--data-dir", (root / "data").string(), "cat", hash, "--raw"});
    INFO("Retrieved bytes: " << content.size());
    REQUIRE(retrieved == 0);
    CHECK(content == payload);

    const auto [explicitCode, explicitContent] =
        runCli({"yams", "--data-dir", (root / "data").string(), "cat", "--hash", hash, "--raw"});
    REQUIRE(explicitCode == 0);
    CHECK(explicitContent == payload);

    // Explicit corpus selection must not leak content from the default corpus
    // or reuse an embedded host belonging to another data directory.
    const auto [wrongCorpusCode, wrongCorpusContent] =
        runCli({"yams", "--data-dir", (root / "other-data").string(), "cat", hash, "--raw"});
    CHECK(wrongCorpusCode != 0);
    CHECK(wrongCorpusContent.empty());

    const auto [getCode, getContent] =
        runCli({"yams", "--data-dir", (root / "data").string(), "get", "--hash", hash, "--raw"});
    REQUIRE(getCode == 0);
    CHECK(getContent == payload);

    if (!checkSearch)
        return;

    const auto [jsonCode, jsonContent] =
        runCli({"yams", "--data-dir", (root / "data").string(), "--json", "get", "--hash", hash});
    REQUIRE(jsonCode == 0);
    REQUIRE(nlohmann::json::accept(jsonContent));
    const auto document = nlohmann::json::parse(jsonContent);
    CHECK(document.at("hash") == hash);
    CHECK(document.at("content") == payload);
    CHECK(document.at("metadata").at("task") == "cli-roundtrip");
    CHECK(document.at("truncated") == false);

    const auto [trailingJsonCode, trailingJsonOutput] =
        runCli({"yams", "--data-dir", (root / "data").string(), "get", "--hash", hash, "--json"});
    REQUIRE(trailingJsonCode == 0);
    REQUIRE(nlohmann::json::accept(trailingJsonOutput));
    CHECK(nlohmann::json::parse(trailingJsonOutput).at("hash") == hash);

    const auto [prefixCode, prefixOutput] = runCli({"yams", "--data-dir", (root / "data").string(),
                                                    "--json", "get", "--hash", hash.substr(0, 12)});
    REQUIRE(prefixCode == 0);
    CHECK(nlohmann::json::parse(prefixOutput).at("hash") == hash);

    const auto [metadataCode, metadataOutput] =
        runCli({"yams", "--data-dir", (root / "data").string(), "--json", "get", "--hash", hash,
                "--metadata-only"});
    REQUIRE(metadataCode == 0);
    const auto metadata = nlohmann::json::parse(metadataOutput);
    CHECK(metadata.at("hash") == hash);
    CHECK(metadata.at("content").is_null());
    CHECK(metadata.at("metadata").at("task") == "cli-roundtrip");

    const auto outputFile = root / "retrieved.json";
    const auto [fileCode, fileOutput] =
        runCli({"yams", "--data-dir", (root / "data").string(), "--json", "get", "--hash", hash,
                "-o", outputFile.string()});
    REQUIRE(fileCode == 0);
    CHECK(fileOutput.empty());
    std::ifstream retrievedFile(outputFile);
    const auto fileDocument = nlohmann::json::parse(retrievedFile);
    CHECK(fileDocument.at("content") == payload);
    CHECK(fileDocument.at("hash") == hash);

    const auto [missingCode, missingOutput] =
        runCli({"yams", "--data-dir", (root / "data").string(), "--json", "get", "--hash",
                std::string(64, '0')});
    CHECK(missingCode != 0);
    CHECK(missingOutput.empty());

    const auto [searched, searchOutput] =
        runCli({"yams", "--data-dir", (root / "data").string(), "--json", "search", "Immutable",
                "--type", "keyword"});
    INFO(searchOutput);
    REQUIRE(searched == 0);
    const auto results = nlohmann::json::parse(searchOutput).at("results");
    REQUIRE_FALSE(results.empty());
    CHECK(results.at(0).at("hash") == hash);
    CHECK(results.at(0).at("hydration").at("hash") == hash);

    const auto [grepped, grepOutput] =
        runCli({"yams", "--data-dir", (root / "data").string(), "grep", "Immutable", "--json"});
    INFO(grepOutput);
    REQUIRE(grepped == 0);
    const auto matches = nlohmann::json::parse(grepOutput).at("matches");
    REQUIRE_FALSE(matches.empty());
    CHECK(matches.at(0).at("hash") == hash);
    CHECK(matches.at(0).at("hydration").at("hash") == hash);
}
