// SPDX-License-Identifier: GPL-3.0-or-later
// Does starting a daemon report a daemon that never came up?

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/startup_handshake.h>

#include <filesystem>
#include <string>

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
