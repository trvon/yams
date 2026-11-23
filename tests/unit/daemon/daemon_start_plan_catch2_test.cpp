// SPDX-License-Identifier: GPL-3.0-or-later
// Which socket, PID file and log file does `yams daemon start` hand a daemon, and when may a
// start (or a starting daemon) replace a daemon that is already running?

#include <catch2/catch_test_macros.hpp>

#include <yams/config/config_helpers.h>
#include <yams/daemon/client/daemon_start_plan.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <optional>

#include "../../common/isolated_daemon_env.h"

#ifndef _WIN32
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#endif

namespace {

namespace fs = std::filesystem;
namespace dc = yams::daemon::client;
using yams::config::RuntimePathOverrides;

// The user's default config names a shared socket and PID file, as in the 2026-11-22 report
// (socket_path = "/tmp/yams-daemon.sock", PID file /tmp/yams-daemon.pid).
struct SharedDaemonSetup {
    yams::test::IsolatedDaemonEnv env{"yams_start_plan_"};
    fs::path sharedSocket = env.root() / "shared" / "yams-daemon.sock";
    fs::path sharedPid = env.root() / "shared" / "yams-daemon.pid";

    SharedDaemonSetup() {
        fs::create_directories(sharedSocket.parent_path());
        env.writeUserConfig(sharedSocket, sharedPid);
    }

    dc::DaemonStartPlan plan(const RuntimePathOverrides& overrides,
                             const std::optional<fs::path>& pidFlag = {},
                             const std::optional<fs::path>& logFlag = {}) const {
        auto effective = yams::config::resolve_runtime_paths(overrides);
        REQUIRE(effective.has_value());
        return dc::planDaemonStart(effective.value(), yams::config::resolve_shared_runtime_paths(),
                                   pidFlag, logFlag);
    }
};

} // namespace

TEST_CASE("start plan: an explicit --socket beats YAMS_DAEMON_SOCKET and config socket_path",
          "[daemon][start-plan][isolation]") {
    SharedDaemonSetup setup;
    const auto envSocket = setup.env.root() / "env" / "env.sock";
    setup.env.set("YAMS_DAEMON_SOCKET", envSocket);
    const auto flagSocket = setup.env.root() / "x" / "yft.sock";

    RuntimePathOverrides overrides;
    overrides.socketPath = flagSocket;
    CHECK(setup.plan(overrides).socketPath == flagSocket);

    // Without the flag the environment beats the config file.
    CHECK(setup.plan({}).socketPath == envSocket);
}

TEST_CASE("start plan: the shared default daemon keeps its socket and PID file",
          "[daemon][start-plan][isolation]") {
    SharedDaemonSetup setup;
    const auto plan = setup.plan({});
    CHECK(plan.sharedSocket);
    CHECK(plan.sharedDataDir);
    CHECK(plan.socketPath == setup.sharedSocket);
    CHECK(plan.pidFile == setup.sharedPid);
    CHECK(plan.logFile.empty()); // the daemon's default log
}

TEST_CASE("start plan: a non-default socket never gets the shared PID file",
          "[daemon][start-plan][isolation]") {
    SharedDaemonSetup setup;
    const auto flagSocket = setup.env.root() / "x" / "yft.sock";

    SECTION("--socket without --pid-file derives the PID file from the socket") {
        RuntimePathOverrides overrides;
        overrides.socketPath = flagSocket;
        const auto plan = setup.plan(overrides);
        CHECK_FALSE(plan.sharedSocket);
        CHECK(plan.pidFile == setup.env.root() / "x" / "yft.pid");
        CHECK(plan.pidFile != setup.sharedPid);
    }

    SECTION("YAMS_DAEMON_SOCKET derives the PID file the same way") {
        setup.env.set("YAMS_DAEMON_SOCKET", flagSocket);
        const auto plan = setup.plan({});
        CHECK(plan.pidFile == setup.env.root() / "x" / "yft.pid");
    }

    SECTION("--config copied from the user's config (same pid_file) still derives") {
        // Start #2 of the report: a copy of the user config with only socket_path changed.
        const auto copy = setup.env.root() / "copy" / "config.toml";
        yams::test::IsolatedDaemonEnv::writeConfig(copy, flagSocket, setup.sharedPid);
        RuntimePathOverrides overrides;
        overrides.configFile = copy;
        const auto plan = setup.plan(overrides);
        CHECK(plan.socketPath == flagSocket);
        CHECK(plan.pidFile == setup.env.root() / "x" / "yft.pid");
    }

    SECTION("a config naming its own PID file keeps it") {
        const auto copy = setup.env.root() / "copy" / "config.toml";
        const auto ownPid = setup.env.root() / "copy" / "own.pid";
        yams::test::IsolatedDaemonEnv::writeConfig(copy, flagSocket, ownPid);
        RuntimePathOverrides overrides;
        overrides.configFile = copy;
        CHECK(setup.plan(overrides).pidFile == ownPid);
    }

    SECTION("--pid-file wins") {
        RuntimePathOverrides overrides;
        overrides.socketPath = flagSocket;
        const auto flagPid = setup.env.root() / "flag.pid";
        CHECK(setup.plan(overrides, flagPid).pidFile == flagPid);
    }
}

TEST_CASE("start plan: the log follows a non-default data dir or socket",
          "[daemon][start-plan][isolation]") {
    SharedDaemonSetup setup;
    const auto scratchData = setup.env.root() / "scratch";
    const auto flagSocket = setup.env.root() / "x" / "yft.sock";

    SECTION("non-default data dir: <data dir>/daemon.log") {
        RuntimePathOverrides overrides;
        overrides.socketPath = flagSocket;
        overrides.dataDir = scratchData;
        const auto plan = setup.plan(overrides);
        CHECK_FALSE(plan.sharedDataDir);
        CHECK(plan.dataDir == scratchData);
        CHECK(plan.logFile == scratchData / "daemon.log");
    }

    SECTION("non-default socket only: beside the socket") {
        RuntimePathOverrides overrides;
        overrides.socketPath = flagSocket;
        CHECK(setup.plan(overrides).logFile == setup.env.root() / "x" / "yft.log");
    }

    SECTION("--log-file wins") {
        RuntimePathOverrides overrides;
        overrides.dataDir = scratchData;
        const auto flagLog = setup.env.root() / "flag.log";
        CHECK(setup.plan(overrides, std::nullopt, flagLog).logFile == flagLog);
    }

    SECTION("the default log is never chosen for a scratch target") {
        RuntimePathOverrides overrides;
        overrides.dataDir = scratchData;
        const auto log = setup.plan(overrides).logFile;
        CHECK_FALSE(log.empty());
        CHECK(log.string().find((setup.env.root() / "state").string()) == std::string::npos);
    }
}

TEST_CASE("start conflict: a live daemon serving another data dir is refused, not replaced",
          "[daemon][start-plan][isolation]") {
    const fs::path ours = "/scratch/A";
    CHECK(dc::classifyLiveDaemonStore(fs::path("/scratch/A/"), ours) == dc::LiveDaemonStore::Same);
    CHECK(dc::classifyLiveDaemonStore(fs::path("/home/u/.local/share/yams"), ours) ==
          dc::LiveDaemonStore::Other);
    CHECK(dc::classifyLiveDaemonStore(std::nullopt, ours) == dc::LiveDaemonStore::Unknown);
    CHECK(dc::classifyLiveDaemonStore(fs::path{}, ours) == dc::LiveDaemonStore::Unknown);

    const auto message =
        dc::otherStoreRefusal("/tmp/yams-daemon.sock", "/home/u/.local/share/yams", ours);
    CHECK(message.find("/home/u/.local/share/yams") != std::string::npos);
    CHECK(message.find("/scratch/A") != std::string::npos);
    CHECK(message.find("/tmp/yams-daemon.sock") != std::string::npos);
}

TEST_CASE("pid-file holder: only a daemon serving the same data dir may be replaced",
          "[daemon][start-plan][isolation]") {
    const fs::path ours = "/scratch/B";
    // Record names its data dir.
    CHECK(dc::mayReplaceDaemonHoldingPidFile(42, fs::path("/scratch/B"), std::nullopt, ours));
    CHECK_FALSE(
        dc::mayReplaceDaemonHoldingPidFile(42, fs::path("/home/u/.local/share/yams"), 42, ours));
    // Older record without a data dir: our data dir's lock must name that PID.
    CHECK(dc::mayReplaceDaemonHoldingPidFile(42, std::nullopt, 42, ours));
    CHECK_FALSE(dc::mayReplaceDaemonHoldingPidFile(42, std::nullopt, 7, ours));
    CHECK_FALSE(dc::mayReplaceDaemonHoldingPidFile(42, std::nullopt, std::nullopt, ours));
    CHECK_FALSE(dc::mayReplaceDaemonHoldingPidFile(0, fs::path("/scratch/B"), 0, ours));
}

TEST_CASE("data-dir lock: the recorded PID is read back", "[daemon][start-plan][isolation]") {
    yams::test::IsolatedDaemonEnv env{"yams_start_plan_lock_"};
    const auto dir = env.root() / "store";
    fs::create_directories(dir);
    CHECK_FALSE(dc::readDataDirLockPid(dir).has_value());
    std::ofstream(dir / ".yams-lock") << R"({"pid":4242,"socket":"/x.sock","timestamp":1})";
    CHECK(dc::readDataDirLockPid(dir) == std::optional<int>{4242});
    std::ofstream(dir / ".yams-lock") << "garbage";
    CHECK_FALSE(dc::readDataDirLockPid(dir).has_value());
}

#ifndef _WIN32
TEST_CASE("socket probe: a listening socket is in use, a stale file is not",
          "[daemon][start-plan][isolation]") {
    yams::test::IsolatedDaemonEnv env{"yams_sp_"};
    const auto path = env.root() / "l.sock";
    REQUIRE(path.native().size() < sizeof(sockaddr_un::sun_path));

    CHECK_FALSE(dc::unixSocketAcceptsConnections(path)); // nothing there

    const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    REQUIRE(fd >= 0);
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
    REQUIRE(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    REQUIRE(::listen(fd, 4) == 0);
    CHECK(dc::unixSocketAcceptsConnections(path));

    ::close(fd); // socket file stays behind, nobody listens
    CHECK(fs::exists(path));
    CHECK_FALSE(dc::unixSocketAcceptsConnections(path));
}
#endif
