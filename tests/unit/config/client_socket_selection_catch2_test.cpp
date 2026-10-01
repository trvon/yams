// SPDX-License-Identifier: GPL-3.0-or-later
// Client daemon-socket selection: explicit choices, the per-user daemon, and the packaged
// system service socket (/run/yams/yams-daemon.sock).

#include <catch2/catch_test_macros.hpp>

#include <yams/config/config_helpers.h>

#include <filesystem>
#include <fstream>
#include <set>

namespace {

namespace fs = std::filesystem;
using yams::config::DaemonSocketProbe;
using yams::config::ResolvedRuntimePaths;
using yams::config::RuntimePathSource;

// Function-pointer probes read this fake filesystem state.
std::set<fs::path>& presentSockets() {
    static std::set<fs::path> sockets;
    return sockets;
}
std::set<fs::path>& connectableSockets() {
    static std::set<fs::path> sockets;
    return sockets;
}

bool fakePresent(const fs::path& p) {
    return presentSockets().count(p) > 0;
}
bool fakeConnectable(const fs::path& p) {
    return fakePresent(p) && connectableSockets().count(p) > 0;
}

const fs::path kUserSocket{"/run/user/1000/yams-daemon.sock"};
const fs::path kSystemSocket{"/run/yams/yams-daemon.sock"};

struct SelectionFixture {
    SelectionFixture() {
        presentSockets().clear();
        connectableSockets().clear();
        paths.socketPath = {kUserSocket, RuntimePathSource::PlatformDefault, "platform default"};
        paths.dataDir = {"/home/u/.local/share/yams", RuntimePathSource::PlatformDefault,
                         "platform data default"};
        paths.runtimeDir = {"/run/user/1000/yams", RuntimePathSource::PlatformDefault,
                            "platform runtime default"};
    }
    ~SelectionFixture() {
        presentSockets().clear();
        connectableSockets().clear();
    }

    void systemDaemonReachable() {
        presentSockets().insert(kSystemSocket);
        connectableSockets().insert(kSystemSocket);
    }

    fs::path select() const {
        return yams::config::select_client_socket_path(
            paths, DaemonSocketProbe{&fakePresent, &fakeConnectable}, kSystemSocket);
    }

    ResolvedRuntimePaths paths;
};

} // namespace

TEST_CASE_METHOD(SelectionFixture, "client socket: system daemon used when nothing per-user exists",
                 "[config][socket][system-daemon]") {
    systemDaemonReachable();
    CHECK(select() == kSystemSocket);
}

TEST_CASE_METHOD(SelectionFixture, "client socket: per-user default kept without a system daemon",
                 "[config][socket][system-daemon]") {
    CHECK(select() == kUserSocket);
}

TEST_CASE_METHOD(SelectionFixture,
                 "client socket: inaccessible system socket keeps the per-user default",
                 "[config][socket][system-daemon]") {
    // Not a member of the yams group: the socket exists but connect() would be refused.
    presentSockets().insert(kSystemSocket);
    CHECK(select() == kUserSocket);
}

TEST_CASE_METHOD(SelectionFixture, "client socket: a live per-user daemon wins over the system one",
                 "[config][socket][system-daemon]") {
    systemDaemonReachable();
    presentSockets().insert(kUserSocket);
    connectableSockets().insert(kUserSocket);
    CHECK(select() == kUserSocket);
}

TEST_CASE_METHOD(SelectionFixture,
                 "client socket: explicit and configured sockets are never replaced",
                 "[config][socket][system-daemon]") {
    systemDaemonReachable();
    const fs::path custom{"/srv/yams/custom.sock"};
    for (auto source : {RuntimePathSource::Explicit, RuntimePathSource::Environment,
                        RuntimePathSource::ConfigFile}) {
        paths.socketPath = {custom, source, "chosen"};
        CHECK(select() == custom);
    }
}

TEST_CASE_METHOD(SelectionFixture, "client socket: explicit runtime dir keeps its socket",
                 "[config][socket][system-daemon]") {
    systemDaemonReachable();
    paths.runtimeDir.source = RuntimePathSource::Explicit;
    CHECK(select() == kUserSocket);
}

TEST_CASE_METHOD(SelectionFixture,
                 "client socket: a personal data directory keeps the per-user daemon",
                 "[config][socket][system-daemon]") {
    systemDaemonReachable();
    for (auto source : {RuntimePathSource::Explicit, RuntimePathSource::Environment,
                        RuntimePathSource::ConfigFile}) {
        paths.dataDir.source = source;
        CHECK(select() == kUserSocket);
    }
}

TEST_CASE_METHOD(SelectionFixture, "client socket: no system socket on platforms without one",
                 "[config][socket][system-daemon]") {
    systemDaemonReachable();
    CHECK(yams::config::select_client_socket_path(
              paths, DaemonSocketProbe{&fakePresent, &fakeConnectable}, fs::path{}) == kUserSocket);
}

TEST_CASE("client socket: packaged system socket path", "[config][socket][system-daemon]") {
#ifdef _WIN32
    CHECK(yams::config::system_daemon_socket_path().empty());
#else
    CHECK(yams::config::system_daemon_socket_path() == kSystemSocket);
#endif
}

#ifndef _WIN32
TEST_CASE("client socket: default probe rejects regular files and missing paths",
          "[config][socket][system-daemon]") {
    const auto probe = yams::config::default_daemon_socket_probe();
    REQUIRE(probe.socketPresent != nullptr);
    REQUIRE(probe.socketConnectable != nullptr);
    const auto regular = fs::temp_directory_path() / "yams-client-socket-probe-regular";
    {
        std::ofstream(regular) << "x";
    }
    CHECK_FALSE(probe.socketPresent(regular));
    CHECK_FALSE(probe.socketConnectable(regular));
    CHECK_FALSE(probe.socketPresent(regular.string() + ".missing"));
    std::error_code ec;
    fs::remove(regular, ec);
}
#endif
