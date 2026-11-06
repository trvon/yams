// Name resolution must never substitute an unrelated document for a name
// that does not exist. `cat`, `get` and `update --name` share this path.
#include <nlohmann/json.hpp>
#include "../../common/test_helpers_catch2.h"
#include <catch2/catch_test_macros.hpp>
#include <yams/cli/yams_cli.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

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

struct EmbeddedCorpus {
    std::filesystem::path root = yams::test::make_temp_dir("yams_name_resolution_");
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

    ~EmbeddedCorpus() {
        std::error_code ec;
        std::filesystem::remove_all(root, ec);
    }

    std::string add(const std::string& name, const std::string& payload) {
        const auto [rc, out] = runCli({"yams", "--data-dir", data, "--json", "add", "-", "--name",
                                       name, "--no-embeddings", "--sync"},
                                      payload);
        INFO(out);
        REQUIRE(rc == 0);
        return nlohmann::json::parse(out).at("results").at(0).at("hash").get<std::string>();
    }
};

} // namespace

TEST_CASE("Name resolution fails closed for names that are not stored",
          "[cli][name-resolution][contract]") {
    EmbeddedCorpus corpus;
    const std::string payload = "Immutable evidence report\nquarterly ledger\n";
    corpus.add("evidence.txt", payload);

    SECTION("an exact stored name still resolves") {
        const auto [rc, out] = runCli({"yams", "--data-dir", corpus.data, "cat", "evidence.txt"});
        CHECK(rc == 0);
        CHECK(out == payload);
    }

    // Each missing name shares words with the stored document, so a
    // search-based fallback would pick it.
    for (const std::string missing :
         {"/no/such/Immutable-evidence-report.md", "Immutable evidence report.pdf",
          "quarterly-ledger-2027.xlsx"}) {
        DYNAMIC_SECTION("cat " << missing) {
            const auto [rc, out] = runCli({"yams", "--data-dir", corpus.data, "cat", missing});
            CHECK(rc != 0);
            CHECK(out.empty());
        }
        DYNAMIC_SECTION("get " << missing) {
            const auto [rc, out] = runCli({"yams", "--data-dir", corpus.data, "get", missing});
            CHECK(rc != 0);
            CHECK(out.find("Immutable evidence report") == std::string::npos);
        }
    }
}

TEST_CASE("update --name does not write to a different document",
          "[cli][name-resolution][contract]") {
    EmbeddedCorpus corpus;
    const auto hash = corpus.add("evidence.txt", "Immutable evidence report\n");

    const auto [rc, out] = runCli({"yams", "--data-dir", corpus.data, "update", "--name",
                                   "Immutable evidence report.pdf", "--metadata", "misrouted=yes"});
    INFO(out);
    CHECK(rc != 0);
    CHECK(out.find("updated successfully") == std::string::npos);
    CHECK(out.find(hash.substr(0, 12)) == std::string::npos);
}

TEST_CASE("A loose name that matches several documents is ambiguous",
          "[cli][name-resolution][contract]") {
    EmbeddedCorpus corpus;
    corpus.add("alpha-notes.txt", "alpha body\n");
    corpus.add("beta-notes.txt", "beta body\n");

    const auto [rc, out] = runCli({"yams", "--data-dir", corpus.data, "cat", "notes"});
    CHECK(rc != 0);
    CHECK(out.empty());

    SECTION("a loose name with one match still resolves") {
        corpus.add("gamma-report.txt", "gamma body\n");
        const auto [uniqueRc, uniqueOut] =
            runCli({"yams", "--data-dir", corpus.data, "cat", "gamma"});
        CHECK(uniqueRc == 0);
        CHECK(uniqueOut == "gamma body\n");
    }
}

TEST_CASE("update --name with a loose name or glob refuses several matches",
          "[cli][name-resolution][contract]") {
    EmbeddedCorpus corpus;
    corpus.add("report-alpha.txt", "alpha\n");
    corpus.add("report-beta.txt", "beta\n");

    for (const std::string name : {"report", "report-*.txt"}) {
        DYNAMIC_SECTION("update --name " << name) {
            const auto [rc, out] = runCli(
                {"yams", "--data-dir", corpus.data, "update", "--name", name, "--metadata", "k=v"});
            INFO(out);
            CHECK(rc != 0);
            CHECK(out.find("updated successfully") == std::string::npos);
        }
    }
}

TEST_CASE("A relative path resolves by suffix of the stored path",
          "[cli][name-resolution][contract]") {
    EmbeddedCorpus corpus;
    const auto dir = corpus.root / "tree" / "sub";
    std::filesystem::create_directories(dir);
    {
        std::ofstream file(dir / "leaf.txt");
        file << "leaf body\n";
    }
    const auto [added, addOut] = runCli({"yams", "--data-dir", corpus.data, "add",
                                         (dir / "leaf.txt").string(), "--no-embeddings", "--sync"});
    INFO(addOut);
    REQUIRE(added == 0);

    const auto [rc, out] = runCli({"yams", "--data-dir", corpus.data, "cat", "sub/leaf.txt"});
    CHECK(rc == 0);
    CHECK(out == "leaf body\n");
}
