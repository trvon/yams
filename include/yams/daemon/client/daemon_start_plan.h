#pragma once

#include <yams/config/config_helpers.h>

#include <filesystem>
#include <optional>
#include <string>

namespace yams::daemon::client {

/// Paths one `yams daemon start` hands to the daemon it launches.
struct DaemonStartPlan {
    std::filesystem::path socketPath;
    std::filesystem::path pidFile;
    std::filesystem::path logFile; ///< empty: the daemon picks its default log
    std::filesystem::path dataDir;
    std::filesystem::path configFile;
    bool sharedSocket{false};  ///< the target socket is the shared (default) daemon's socket
    bool sharedDataDir{false}; ///< the target data dir is the shared daemon's data dir
};

/// Works out the launch paths from `effective` (flags, environment and config, as resolved by
/// resolve_runtime_paths) and `shared` (resolve_shared_runtime_paths: the user's default daemon).
///
/// - The socket and data dir come from `effective` as is: a flag beats the environment, which
///   beats the config file.
/// - The PID file is `pidFileFlag` if given. A target on the shared socket keeps the resolved PID
///   file. Any other target gets `<socket dir>/<socket stem>.pid`, unless its config names a PID
///   file of its own; it never reuses the shared daemon's PID file.
/// - The log file is `logFileFlag` if given; `<data dir>/daemon.log` for a data dir that is not
///   the shared one; `<socket dir>/<socket stem>.log` for a socket that is not the shared one;
///   otherwise empty (the daemon default).
DaemonStartPlan planDaemonStart(const yams::config::ResolvedRuntimePaths& effective,
                                const yams::config::ResolvedRuntimePaths& shared,
                                const std::optional<std::filesystem::path>& pidFileFlag = {},
                                const std::optional<std::filesystem::path>& logFileFlag = {});

/// True when two runtime paths name the same location (lexically, then by filesystem identity).
bool sameRuntimePath(const std::filesystem::path& lhs, const std::filesystem::path& rhs);

/// Whose store a daemon already running on the target socket serves.
enum class LiveDaemonStore {
    Same,    ///< the live daemon serves the data dir this start targets
    Other,   ///< the live daemon serves a different data dir
    Unknown, ///< the live daemon did not report its data dir
};

LiveDaemonStore classifyLiveDaemonStore(const std::optional<std::filesystem::path>& liveDataDir,
                                        const std::filesystem::path& targetDataDir);

/// Message for refusing to start because the target socket belongs to a daemon serving another
/// store. `liveDataDir` may be empty when the live daemon did not report one.
std::string otherStoreRefusal(const std::filesystem::path& socketPath,
                              const std::filesystem::path& liveDataDir,
                              const std::filesystem::path& targetDataDir);

/// May a starting daemon stop the live daemon (PID `existingPid`) that holds its PID file?
/// Only when that daemon serves the same data dir: its PID record names that data dir, or
/// (for a record that names none) the target data dir's lock file names its PID.
bool mayReplaceDaemonHoldingPidFile(int existingPid,
                                    const std::optional<std::filesystem::path>& recordedDataDir,
                                    const std::optional<int>& targetDataDirLockPid,
                                    const std::filesystem::path& targetDataDir);

/// PID recorded in `<dataDir>/.yams-lock`, if any.
std::optional<int> readDataDirLockPid(const std::filesystem::path& dataDir);

/// True when a process accepts connections on the Unix socket at `socketPath` (a live daemon,
/// not a stale socket file). Always false on Windows.
bool unixSocketAcceptsConnections(const std::filesystem::path& socketPath);

} // namespace yams::daemon::client
