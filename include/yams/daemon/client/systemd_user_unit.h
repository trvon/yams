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

/// Which daemon a lifecycle command (stop, restart) is allowed to act on.
struct LifecycleScope {
    bool userUnit{false};      ///< may be handed to the systemd user unit
    bool anyLiveDaemon{false}; ///< may fall back to whichever daemon is running
    bool pidFile{false};       ///< the resolved PID file identifies the target daemon
};

/// A command aimed at the default daemon may use the user unit, discovery of a live daemon
/// and the shared PID file. One aimed elsewhere (a socket, data directory or config file from
/// a flag or the environment) must touch only the daemon on its own socket; its PID file
/// counts only when it was named by flag, environment or that config.
LifecycleScope lifecycleScope(bool socketFlag, bool pidFileFlag,
                              const yams::config::ResolvedRuntimePaths& paths);

/// Queries `systemctl --user` (Linux only; Unavailable elsewhere or without a user manager).
UserUnitState queryUserUnit();

/// Runs `systemctl --user <verb> yams-daemon.service`; returns true on exit status 0.
/// verb must be one of: start, stop, restart, enable --now, disable --now.
bool runUserUnit(std::string_view verb);

} // namespace yams::daemon::client
