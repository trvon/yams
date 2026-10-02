#pragma once

#include <yams/config/config_helpers.h>

#include <string_view>

namespace yams::daemon::client {

/// Name of the per-user unit the Linux packages ship in /usr/lib/systemd/user/.
inline constexpr std::string_view kUserUnitName = "yams-daemon.service";

/// What the user's systemd manager knows about the yams-daemon user unit.
enum class UserUnitState {
    Unavailable, ///< no user manager reachable, or no such unit
    Inactive,    ///< unit file present (enabled or not), not running
    Active,      ///< unit running (or starting)
};

/// True when the packaged/installed user unit would run the daemon this client expects: the
/// unit starts `yams-daemon --foreground` with default path resolution, so it only matches when
/// the socket and data directory come from defaults or the shared config file (not a flag or
/// environment override the unit cannot see) and no other config file was named.
bool userUnitMatchesPaths(const yams::config::ResolvedRuntimePaths& paths);

/// Queries `systemctl --user` (Linux only; Unavailable elsewhere or without a user manager).
UserUnitState queryUserUnit();

/// Runs `systemctl --user <verb> yams-daemon.service`; returns true on exit status 0.
/// verb must be one of: start, stop, restart, enable --now, disable --now.
bool runUserUnit(std::string_view verb);

} // namespace yams::daemon::client
