#include <yams/daemon/client/systemd_user_unit.h>

#include <array>
#include <cstdlib>
#include <string>

#if defined(__linux__)
#include <sys/wait.h>
#endif

namespace yams::daemon::client {

bool userUnitMatchesPaths(const yams::config::ResolvedRuntimePaths& paths) {
    using yams::config::RuntimePathSource;
    const auto inheritable = [](RuntimePathSource source) {
        return source == RuntimePathSource::PlatformDefault ||
               source == RuntimePathSource::ConfigFile;
    };
    return inheritable(paths.socketPath.source) && inheritable(paths.dataDir.source) &&
           paths.runtimeDir.source == RuntimePathSource::PlatformDefault &&
           paths.configFile.source == RuntimePathSource::PlatformDefault;
}

#if defined(__linux__)
namespace {

bool userManagerReachable() {
    return yams::config::getenv_nonempty("XDG_RUNTIME_DIR").has_value();
}

int systemctlUser(std::string_view args) {
    std::string cmd = "systemctl --user --quiet ";
    cmd.append(args);
    cmd.append(" >/dev/null 2>&1");
    const int rc = std::system(cmd.c_str()); // NOLINT(cert-env33-c): fixed command line
    if (rc == -1) {
        return -1;
    }
    return WIFEXITED(rc) ? WEXITSTATUS(rc) : -1;
}

} // namespace

UserUnitState queryUserUnit() {
    if (!userManagerReachable()) {
        return UserUnitState::Unavailable;
    }
    const std::string unit(kUserUnitName);
    if (systemctlUser("is-active " + unit) == 0) {
        return UserUnitState::Active;
    }
    // `cat` succeeds only for a unit file the user manager can load.
    if (systemctlUser("cat " + unit) == 0) {
        return UserUnitState::Inactive;
    }
    return UserUnitState::Unavailable;
}

bool runUserUnit(std::string_view verb) {
    static constexpr std::array<std::string_view, 5> kAllowed = {"start", "stop", "restart",
                                                                 "enable --now", "disable --now"};
    bool allowed = false;
    for (const auto v : kAllowed) {
        allowed = allowed || v == verb;
    }
    if (!allowed || !userManagerReachable()) {
        return false;
    }
    std::string args(verb);
    args.push_back(' ');
    args.append(kUserUnitName);
    return systemctlUser(args) == 0;
}
#else
UserUnitState queryUserUnit() {
    return UserUnitState::Unavailable;
}

bool runUserUnit(std::string_view) {
    return false;
}
#endif

} // namespace yams::daemon::client
