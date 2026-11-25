// SPDX-License-Identifier: GPL-3.0-or-later
// Does starting a daemon report a daemon that never came up?

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/startup_handshake.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "../../common/test_helpers_catch2.h"

#ifndef _WIN32
#include <unistd.h>

namespace {

yams::daemon::ClientConfig isolatedConfig(const std::filesystem::path& root,
                                          const std::string& launcher) {
    std::filesystem::create_directories(root / "data");
    yams::daemon::ClientConfig config;
    config.socketPath = root / "d.sock";
    config.pidFile = root / "d.pid";
    config.dataDir = root / "data";
    config.daemonBinary = launcher; // never the real daemon: nothing is left running
    return config;
}

} // namespace

TEST_CASE("startDaemon reports a launcher that exits with a failure status",
          "[daemon][client][start]") {
    yams::test::TempDirGuard tempDir{"yams_client_start_fail_"};
    const auto result =
        yams::daemon::DaemonClient::startDaemon(isolatedConfig(tempDir.path(), "false"));
    REQUIRE_FALSE(result);
    CHECK(result.error().message.find("exited") != std::string::npos);
}

TEST_CASE("startDaemon reports a daemon binary that cannot be executed",
          "[daemon][client][start]") {
    yams::test::TempDirGuard tempDir{"yams_client_start_missing_"};
    const auto missing = (tempDir.path() / "no-such-daemon").string();
    const auto result =
        yams::daemon::DaemonClient::startDaemon(isolatedConfig(tempDir.path(), missing));
    REQUIRE_FALSE(result);
    CHECK(result.error().message.find("no-such-daemon") != std::string::npos);
}

TEST_CASE("startDaemon accepts a launcher that reports success", "[daemon][client][start]") {
    yams::test::TempDirGuard tempDir{"yams_client_start_ok_"};
    const auto result =
        yams::daemon::DaemonClient::startDaemon(isolatedConfig(tempDir.path(), "true"));
    CHECK(result.has_value());
}

TEST_CASE("startDaemon hands the daemon its socket, PID file and log file",
          "[daemon][client][start][isolation]") {
    yams::test::TempDirGuard tempDir{"yams_client_start_args_"};
    const auto argvFile = tempDir.path() / "argv.txt";
    const auto launcher = tempDir.path() / "record.sh";
    {
        std::ofstream out(launcher);
        out << "#!/bin/sh\nprintf '%s\\n' \"$@\" > '" << argvFile.string() << "'\nexit 0\n";
    }
    std::filesystem::permissions(launcher, std::filesystem::perms::owner_all,
                                 std::filesystem::perm_options::add);

    auto config = isolatedConfig(tempDir.path(), launcher.string());
    config.logFile = tempDir.path() / "data" / "daemon.log";
    REQUIRE(yams::daemon::DaemonClient::startDaemon(config).has_value());

    std::vector<std::string> argv;
    std::ifstream in(argvFile);
    for (std::string line; std::getline(in, line);) {
        argv.push_back(line);
    }
    const auto after = [&](const std::string& flag) -> std::string {
        for (std::size_t i = 0; i + 1 < argv.size(); ++i) {
            if (argv[i] == flag) {
                return argv[i + 1];
            }
        }
        return {};
    };
    CHECK(after("--socket") == config.socketPath.string());
    CHECK(after("--pid-file") == config.pidFile.string());
    CHECK(after("--log-file") == config.logFile.string());
    CHECK(after("--data-dir") == config.dataDir.string());
}

TEST_CASE("startup handshake carries the daemon's start result to its launcher",
          "[daemon][client][start][handshake]") {
    using yams::daemon::StartupHandshake;

    SECTION("a daemon that started reports success") {
        auto pipe = StartupHandshake::open();
        REQUIRE(pipe.has_value());
        pipe->report(true);
        CHECK(pipe->await() == 0);
    }
    SECTION("a daemon that failed to start reports failure") {
        auto pipe = StartupHandshake::open();
        REQUIRE(pipe.has_value());
        pipe->report(false);
        CHECK(pipe->await() == 1);
    }
    SECTION("a daemon that died before reporting is a failure") {
        auto pipe = StartupHandshake::open();
        REQUIRE(pipe.has_value());
        pipe->closeWriter();
        CHECK(pipe->await() == 1);
    }
    SECTION("the reporting end stays clear of the standard descriptors") {
        auto pipe = StartupHandshake::open();
        REQUIRE(pipe.has_value());
        CHECK(pipe->writerFd() > STDERR_FILENO);
    }
}
#endif
