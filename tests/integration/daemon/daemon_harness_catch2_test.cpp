// SPDX-License-Identifier: GPL-3.0-or-later

#define CATCH_CONFIG_MAIN
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>

#include "test_daemon_harness.h"

#include <fstream>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace std::chrono_literals;
using yams::test::DaemonHarness;
using yams::test::ScopedEnvVar;
using yams::test::TempDirGuard;
using yams::test::write_file;

namespace {

std::string readText(const std::filesystem::path& path) {
    std::ifstream input(path);
    return {std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
}

DaemonHarness::Options ownedPathOptions(const std::filesystem::path& root) {
    DaemonHarness::Options options;
    options.rootDir = root;
    options.preserveRoot = true;
    options.dataDir = root / "data";
    options.socketPath = root / "daemon.sock";
    options.pidFile = root / "daemon.pid";
    options.logFile = root / "daemon.log";
    return options;
}

bool pathWithin(const std::filesystem::path& candidate, const std::filesystem::path& root) {
    const auto normalizedRoot = root.lexically_normal();
    const auto normalizedCandidate = candidate.lexically_normal();
    auto rootIt = normalizedRoot.begin();
    auto candidateIt = normalizedCandidate.begin();
    for (; rootIt != normalizedRoot.end(); ++rootIt, ++candidateIt) {
        if (rootIt->empty()) {
            continue;
        }
        if (candidateIt == normalizedCandidate.end() || *candidateIt != *rootIt) {
            return false;
        }
    }
    return true;
}

// Ambient variables a developer machine may export. The isolation contract is that none of them
// reach a harness-owned daemon.
constexpr std::string_view kDecoyDataMarker = "DECOY_DATA_MUST_NOT_BE_USED";

} // namespace

TEST_CASE("DaemonHarness full isolation ignores ambient developer config and data",
          "[daemon][harness][config][isolation]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    TempDirGuard decoy{"yams_decoy_"};
    const auto decoyHome = decoy.path() / "home";
    const auto decoyConfigHome = decoy.path() / "xdg_config";
    const auto decoyConfig = decoyConfigHome / "yams" / "config.toml";
    const auto decoyData = decoy.path() / std::string{kDecoyDataMarker};
    const auto decoySocket = decoy.path() / "decoy.sock";
    const auto decoyRuntime = decoy.path() / "xdg_runtime";
    std::filesystem::create_directories(decoyRuntime);
    const std::string decoyContents = "config_version = 3\n[core]\ndata_dir = \"" +
                                      decoyData.string() + "\"\n# " +
                                      std::string{kDecoyDataMarker} + "\n";
    write_file(decoyConfig, decoyContents);
    write_file(decoyHome / ".config" / "yams" / "config.toml", decoyContents);

    const std::pair<const char*, std::string> ambient[] = {
        {"HOME", decoyHome.string()},
        {"XDG_CONFIG_HOME", decoyConfigHome.string()},
        {"XDG_STATE_HOME", (decoy.path() / "xdg_state").string()},
        {"XDG_DATA_HOME", (decoy.path() / "xdg_data").string()},
        {"XDG_CACHE_HOME", (decoy.path() / "xdg_cache").string()},
        {"XDG_RUNTIME_DIR", decoyRuntime.string()},
        {"YAMS_EMBED_BACKEND", "decoy-backend"},
        {"YAMS_PREFERRED_MODEL", "decoy-model"},
        {"YAMS_RERANKER_MODEL", "decoy-reranker"},
        {"YAMS_EMBED_DIM", "7"},
        {"YAMS_CONFIG", decoyConfig.string()},
        {"YAMS_CONFIG_PATH", decoyConfig.string()},
        {"YAMS_DATA_DIR", decoyData.string()},
        {"YAMS_STORAGE", decoyData.string()},
        {"YAMS_DAEMON_SOCKET", decoySocket.string()},
        {"YAMS_DAEMON_SOCKET_PATH", decoySocket.string()},
    };
    std::vector<ScopedEnvVar> ambientGuards;
    for (const auto& [name, value] : ambient) {
        ambientGuards.emplace_back(name, value);
    }

    DaemonHarness::Options options;
    options.enableModelProvider = false;
    options.useMockModelProvider = false;
    options.enableAutoRepair = false;
    options.requireReadyLifecycle = true;
    // No explicit data/config/socket paths: the harness alone must keep the run contained.
    options.isolateEnvironment = true;

    std::filesystem::path observedConfig;
    options.configureDaemon = [&](yams::daemon::DaemonConfig& config) {
        observedConfig = config.configFilePath;
    };

    DaemonHarness harness{std::move(options)};
    REQUIRE(harness.startWithRetry(30s, 1, [](yams::daemon::YamsDaemon*) {}));
    const auto& root = harness.rootDir();

    REQUIRE_FALSE(observedConfig.empty());
    CHECK(pathWithin(observedConfig, root));
    CHECK(pathWithin(harness.dataDir(), root));

    for (const char* name :
         {"HOME", "XDG_CONFIG_HOME", "XDG_STATE_HOME", "XDG_DATA_HOME", "XDG_CACHE_HOME",
          "XDG_RUNTIME_DIR", "YAMS_CONFIG", "YAMS_CONFIG_PATH", "YAMS_DATA_DIR", "YAMS_STORAGE"}) {
        const auto value = yams::config::getenv_optional(name);
        INFO(name << "=" << value.value_or("<unset>"));
        REQUIRE(value.has_value());
        CHECK(pathWithin(*value, root));
    }
    CHECK((yams::config::getenv_optional("YAMS_DAEMON_SOCKET") == harness.socketPath().string()));
    const auto legacySocket = yams::config::getenv_optional("YAMS_DAEMON_SOCKET_PATH");
    CHECK((!legacySocket.has_value() || legacySocket == harness.socketPath().string()));

    // Ambient-path resolvers used by the daemon and its components see only the temp root.
    CHECK(pathWithin(yams::config::get_config_path(), root));
    CHECK(pathWithin(yams::config::get_config_dir(), root));
    CHECK(pathWithin(yams::config::get_data_dir(), root));
    CHECK(pathWithin(yams::config::get_cache_dir(), root));
    CHECK(pathWithin(yams::config::get_state_dir(), root));
    CHECK(pathWithin(yams::config::get_runtime_dir(), root));
    const auto statusFile = yams::config::get_daemon_status_file();
    CHECK(pathWithin(statusFile, root));
    CHECK(std::filesystem::exists(statusFile));
    // Model/backend overrides outrank TOML in ConfigResolver, so they must not be in effect.
    for (const char* name :
         {"YAMS_EMBED_BACKEND", "YAMS_PREFERRED_MODEL", "YAMS_RERANKER_MODEL", "YAMS_EMBED_DIM"}) {
        INFO(name);
        CHECK_FALSE(yams::config::getenv_optional(name).has_value());
    }
    CHECK((readText(observedConfig).find(kDecoyDataMarker) == std::string::npos));

    harness.stop();
    CHECK(harness.shutdownSucceeded());

    // The decoy was never read as config nor populated as data, and the ambient env is back.
    CHECK_FALSE(std::filesystem::exists(decoyData));
    CHECK(std::filesystem::is_empty(decoyRuntime));
    CHECK((readText(decoyConfig) == decoyContents));
    for (const auto& [name, value] : ambient) {
        const auto restored = yams::config::getenv_optional(name);
        INFO(name << " restored=" << restored.value_or("<unset>"));
        CHECK((restored == std::optional<std::string>{value}));
    }
}

TEST_CASE("DaemonHarness rolls back owned endpoints when startup callback fails",
          "[daemon][harness][cleanup][startup]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    TempDirGuard fixture{"yams_daemon_harness_owned_"};
    auto options = ownedPathOptions(fixture.path());
    options.enableModelProvider = false;
    options.useMockModelProvider = false;
    options.enableAutoRepair = false;
    options.isolateConfig = true;
    options.socketPath = std::filesystem::path{"/tmp"} /
                         ("yams_harness_" + fixture.path().filename().string() + ".sock");

    DaemonHarness harness{std::move(options)};
    CHECK_FALSE(harness.start(5s, [](yams::daemon::YamsDaemon*) {
        throw std::runtime_error{"synthetic pre-runLoop failure"};
    }));
    CHECK((harness.daemon() == nullptr));
    CHECK_FALSE(std::filesystem::exists(harness.socketPath()));
    CHECK_FALSE(std::filesystem::exists(harness.proxySocketPath()));
    CHECK_FALSE(std::filesystem::exists(harness.pidPath()));
    CHECK(std::filesystem::exists(fixture.path()));
}

TEST_CASE("DaemonHarness preserves endpoints claimed after fixture construction",
          "[daemon][harness][cleanup][ownership]") {
    TempDirGuard fixture{"yams_daemon_harness_preexisting_"};
    const auto options = ownedPathOptions(fixture.path());
    DaemonHarness harness{options};
    const auto socketPath = harness.socketPath();
    const auto proxyPath = harness.proxySocketPath();
    const auto pidPath = harness.pidPath();
    write_file(socketPath, "pre-existing socket");
    write_file(proxyPath, "pre-existing proxy");
    write_file(pidPath, "pre-existing pid");

    CHECK_FALSE(harness.start(1s));
    CHECK(std::filesystem::exists(socketPath));
    CHECK(std::filesystem::exists(proxyPath));
    CHECK(std::filesystem::exists(pidPath));
}

TEST_CASE("DaemonHarness restores process state when configuration throws",
          "[daemon][harness][config][exception]") {
    TempDirGuard fixture{"yams_daemon_harness_exception_"};
    ScopedEnvVar ambientConfig{"YAMS_CONFIG_PATH", "/host/config/exception-sentinel.toml"};
    auto options = ownedPathOptions(fixture.path());
    options.isolateState = true;
    options.isolateConfig = true;
    int configureCalls = 0;
    options.configureDaemon = [&](yams::daemon::DaemonConfig&) {
        ++configureCalls;
        throw std::runtime_error{"synthetic configuration failure"};
    };

    const auto originalCwd = std::filesystem::current_path();
    DaemonHarness harness{std::move(options)};
    CHECK_FALSE(harness.start(1s));
    CHECK_FALSE(harness.start(1s));
    CHECK((configureCalls == 2));
    CHECK((std::filesystem::current_path() == originalCwd));
    CHECK((yams::config::getenv_optional("YAMS_CONFIG_PATH") ==
           std::optional<std::string>{"/host/config/exception-sentinel.toml"}));
}

TEST_CASE("DaemonHarness isolates config and observes lifecycle cleanup",
          "[daemon][harness][config][lifecycle]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    ScopedEnvVar ambientConfig{"YAMS_CONFIG_PATH", "/host/config/must-not-be-read.toml"};
    DaemonHarness::Options options;
    options.enableModelProvider = false;
    options.useMockModelProvider = false;
    options.enableAutoRepair = false;
    options.isolateState = true;
    options.isolateConfig = true;
    options.requireReadyLifecycle = true;

    std::filesystem::path observedConfig;
    options.configureDaemon = [&](yams::daemon::DaemonConfig& config) {
        observedConfig = config.configFilePath;
    };

    DaemonHarness harness{std::move(options)};
    REQUIRE(harness.startWithRetry(30s, 1, [](yams::daemon::YamsDaemon*) {}));
    REQUIRE_FALSE(observedConfig.empty());
    CHECK((observedConfig.parent_path().parent_path().parent_path() == harness.rootDir()));
    CHECK((readText(observedConfig).find("config_version = 3") != std::string::npos));
    CHECK((yams::config::getenv_optional("YAMS_CONFIG_PATH") == observedConfig.string()));

    harness.stop();
    CHECK((harness.daemon() == nullptr));
    CHECK(harness.shutdownSucceeded());
    CHECK_FALSE(std::filesystem::exists(harness.socketPath()));
    CHECK_FALSE(std::filesystem::exists(harness.proxySocketPath()));
    CHECK_FALSE(std::filesystem::exists(harness.pidPath()));
    const auto restoredConfig = yams::config::getenv_optional("YAMS_CONFIG_PATH");
    INFO("restored YAMS_CONFIG_PATH=" << restoredConfig.value_or("<unset>"));
    CHECK((restoredConfig == std::optional<std::string>{"/host/config/must-not-be-read.toml"}));
}
