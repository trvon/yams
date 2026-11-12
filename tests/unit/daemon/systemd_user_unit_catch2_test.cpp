// SPDX-License-Identifier: GPL-3.0-or-later
// When may a client hand daemon start-up to the packaged systemd user unit?

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/client/systemd_user_unit.h>

namespace {

using yams::config::ResolvedRuntimePaths;
using yams::config::RuntimePathSource;

ResolvedRuntimePaths defaultPaths() {
    ResolvedRuntimePaths p;
    p.configFile = {"/home/u/.config/yams/config.toml", RuntimePathSource::PlatformDefault, ""};
    p.dataDir = {"/home/u/.local/share/yams", RuntimePathSource::PlatformDefault, ""};
    p.runtimeDir = {"/run/user/1000/yams", RuntimePathSource::PlatformDefault, ""};
    p.socketPath = {"/run/user/1000/yams-daemon.sock", RuntimePathSource::PlatformDefault, ""};
    return p;
}

} // namespace

TEST_CASE("user unit: default paths match the unit", "[daemon][systemd-user-unit]") {
    CHECK(yams::daemon::client::userUnitMatchesPaths(defaultPaths()));
}

TEST_CASE("user unit: paths from the shared config file still match",
          "[daemon][systemd-user-unit]") {
    // The unit reads the same ~/.config/yams/config.toml, e.g. data_dir from `yams init`.
    auto p = defaultPaths();
    p.dataDir.source = RuntimePathSource::ConfigFile;
    p.socketPath.source = RuntimePathSource::ConfigFile;
    CHECK(yams::daemon::client::userUnitMatchesPaths(p));
}

TEST_CASE("user unit: flag or environment overrides do not match", "[daemon][systemd-user-unit]") {
    for (auto source : {RuntimePathSource::Explicit, RuntimePathSource::Environment}) {
        auto data = defaultPaths();
        data.dataDir.source = source;
        CHECK_FALSE(yams::daemon::client::userUnitMatchesPaths(data));

        auto socket = defaultPaths();
        socket.socketPath.source = source;
        CHECK_FALSE(yams::daemon::client::userUnitMatchesPaths(socket));

        auto config = defaultPaths();
        config.configFile.source = source;
        CHECK_FALSE(yams::daemon::client::userUnitMatchesPaths(config));
    }
    auto runtime = defaultPaths();
    runtime.runtimeDir.source = RuntimePathSource::Explicit;
    CHECK_FALSE(yams::daemon::client::userUnitMatchesPaths(runtime));
}

TEST_CASE("user unit: only whitelisted systemctl verbs run", "[daemon][systemd-user-unit]") {
    // Rejected before any process is spawned, on every platform.
    CHECK_FALSE(yams::daemon::client::runUserUnit("kill"));
    CHECK_FALSE(yams::daemon::client::runUserUnit("start; rm -rf /"));
}

TEST_CASE("lifecycle scope: the default daemon may use the unit, discovery and the PID file",
          "[daemon][systemd-user-unit][lifecycle-scope]") {
    const auto scope = yams::daemon::client::lifecycleScope(false, false, defaultPaths());
    CHECK(scope.userUnit);
    CHECK(scope.anyLiveDaemon);
    CHECK(scope.pidFile);
}

TEST_CASE("lifecycle scope: a socket from the environment stays on that socket",
          "[daemon][systemd-user-unit][lifecycle-scope]") {
    // `YAMS_DAEMON_SOCKET=/tmp/x.sock yams daemon stop` stopped the user's systemd unit.
    for (auto source : {RuntimePathSource::Explicit, RuntimePathSource::Environment}) {
        auto socket = defaultPaths();
        socket.socketPath.source = source;
        auto scope = yams::daemon::client::lifecycleScope(false, false, socket);
        CHECK_FALSE(scope.userUnit);
        CHECK_FALSE(scope.anyLiveDaemon);
        CHECK_FALSE(scope.pidFile);

        auto config = defaultPaths();
        config.configFile.source = source;
        scope = yams::daemon::client::lifecycleScope(false, false, config);
        CHECK_FALSE(scope.userUnit);
        CHECK_FALSE(scope.anyLiveDaemon);
        CHECK_FALSE(scope.pidFile);

        auto data = defaultPaths();
        data.dataDir.source = source;
        scope = yams::daemon::client::lifecycleScope(false, false, data);
        CHECK_FALSE(scope.userUnit);
        CHECK_FALSE(scope.anyLiveDaemon);
    }
}

TEST_CASE("lifecycle scope: --socket does not make the shared PID file authoritative",
          "[daemon][systemd-user-unit][lifecycle-scope]") {
    // `yams daemon stop --socket /tmp/x.sock` signalled the daemon named by the default PID file.
    auto scope = yams::daemon::client::lifecycleScope(true, false, defaultPaths());
    CHECK_FALSE(scope.userUnit);
    CHECK_FALSE(scope.anyLiveDaemon);
    CHECK_FALSE(scope.pidFile);

    scope = yams::daemon::client::lifecycleScope(true, true, defaultPaths());
    CHECK(scope.pidFile); // named by --pid-file

    auto configured = defaultPaths();
    configured.configFile.source = RuntimePathSource::Environment;
    configured.pidFile.source = RuntimePathSource::ConfigFile;
    scope = yams::daemon::client::lifecycleScope(false, false, configured);
    CHECK(scope.pidFile); // named by the private config
    CHECK_FALSE(scope.userUnit);
}
