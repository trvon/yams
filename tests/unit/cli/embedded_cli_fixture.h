// In-process CLI runs against a private embedded corpus.
#pragma once

#include <nlohmann/json.hpp>
#include "../../common/test_helpers_catch2.h"
#include <catch2/catch_test_macros.hpp>
#include <yams/cli/yams_cli.h>

#include <filesystem>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

namespace yams::test {

struct CliRun {
    int rc = 0;
    std::string out;
    std::string err;
};

inline CliRun runEmbeddedCli(std::vector<std::string> args, const std::string& payload = {}) {
    std::ostringstream out;
    std::ostringstream err;
    std::istringstream in(payload);
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

struct EmbeddedCorpus {
    std::filesystem::path root;
    std::string data;
    ScopedEnvVar embedded{"YAMS_EMBEDDED", std::string("1")};
    ScopedEnvVar inDaemon{"YAMS_IN_DAEMON", std::nullopt};
    ScopedEnvVar socket{"YAMS_DAEMON_SOCKET", std::nullopt};
    ScopedEnvVar socketAlias{"YAMS_DAEMON_SOCKET_PATH", std::nullopt};
    ScopedEnvVar config;
    ScopedEnvVar dataEnv;
    ScopedEnvVar storage;
    ScopedEnvVar vectors{"YAMS_DISABLE_VECTORS", std::string("1")};
    ScopedEnvVar models{"YAMS_SKIP_MODEL_LOADING", std::string("1")};
    ScopedEnvVar watcher{"YAMS_DISABLE_SESSION_WATCHER", std::string("1")};
    ScopedEnvVar autostart{"YAMS_CLI_DISABLE_DAEMON_AUTOSTART", std::string("1")};
    ScopedEnvVar nonInteractive{"YAMS_NON_INTERACTIVE", std::string("1")};

    explicit EmbeddedCorpus(const std::string& prefix)
        : root(make_temp_dir(prefix)), data((root / "data").string()),
          config("YAMS_CONFIG", (root / "config.toml").string()), dataEnv("YAMS_DATA_DIR", data),
          storage("YAMS_STORAGE", data) {}

    ~EmbeddedCorpus() {
        std::error_code ec;
        std::filesystem::remove_all(root, ec);
    }

    CliRun run(std::vector<std::string> args, const std::string& payload = {}) const {
        args.insert(args.begin(), {"yams", "--data-dir", data});
        return runEmbeddedCli(std::move(args), payload);
    }

    // Adds `payload` from stdin under `name`; returns the content hash.
    std::string addText(const std::string& name, const std::string& payload) const {
        const auto r =
            run({"--json", "add", "-", "--name", name, "--no-embeddings", "--sync"}, payload);
        INFO(r.out << r.err);
        REQUIRE(r.rc == 0);
        return nlohmann::json::parse(r.out).at("results").at(0).at("hash").get<std::string>();
    }

    // Adds a file by path so the stored path is `path`.
    void addFile(const std::filesystem::path& path) const {
        const auto r = run({"add", path.string(), "--no-embeddings", "--sync"});
        INFO(r.out << r.err);
        REQUIRE(r.rc == 0);
    }
};

} // namespace yams::test
