// `status` must say which data directory it reports on, and must not report
// success for a data directory that holds no store.
#include <nlohmann/json.hpp>
#include "../../common/test_helpers_catch2.h"
#include <catch2/catch_test_macros.hpp>
#include <yams/cli/yams_cli.h>

#include <filesystem>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace {

std::pair<int, std::string> runCli(std::vector<std::string> args, const std::string& payload = {}) {
    std::ostringstream out;
    std::istringstream in(payload);
    auto* oldOut = std::cout.rdbuf(out.rdbuf());
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
    std::cin.rdbuf(oldIn);
    std::cin.clear();
    return {rc, out.str()};
}

struct EmbeddedCorpus {
    std::filesystem::path root = yams::test::make_temp_dir("yams_status_data_dir_");
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
};

} // namespace

TEST_CASE("status names the data directory it reports on", "[cli][status][data-dir]") {
    EmbeddedCorpus corpus;
    const auto [added, addOut] = runCli({"yams", "--data-dir", corpus.data, "add", "-", "--name",
                                         "evidence.txt", "--no-embeddings", "--sync"},
                                        "status probe\n");
    INFO(addOut);
    REQUIRE(added == 0);

    SECTION("human output") {
        const auto [rc, out] = runCli({"yams", "--data-dir", corpus.data, "status"});
        INFO(out);
        CHECK(rc == 0);
        CHECK(out.find("Data dir") != std::string::npos);
        CHECK(out.find(corpus.data) != std::string::npos);
    }
    SECTION("json output") {
        const auto [rc, out] = runCli({"yams", "--data-dir", corpus.data, "status", "--json"});
        INFO(out);
        REQUIRE(rc == 0);
        const auto j = nlohmann::json::parse(out);
        REQUIRE(j.contains("dataDir"));
        CHECK(std::filesystem::path(j.at("dataDir").get<std::string>()).lexically_normal() ==
              std::filesystem::path(corpus.data).lexically_normal());
    }
}

TEST_CASE("status fails for a data directory with no store", "[cli][status][data-dir]") {
    EmbeddedCorpus corpus;
    const auto missing = (corpus.root / "never-initialized").string();

    const auto [rc, out] = runCli({"yams", "--data-dir", missing, "status"});
    INFO(out);
    CHECK(rc != 0);
}
