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
