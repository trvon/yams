// SPDX-License-Identifier: GPL-3.0-or-later

#define CATCH_CONFIG_MAIN
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>

#include "test_daemon_harness.h"

#include <fstream>
#include <iterator>
#include <string>

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

} // namespace

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
