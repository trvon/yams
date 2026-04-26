#pragma once

#include <yams/core/types.h>

#include <chrono>
#include <filesystem>
#include <string>

namespace yams::daemon {

struct StoragePreflightResult {
    bool reachable{false};
    bool slow{false};
    std::chrono::milliseconds probeDuration{0};
    std::string detail;
};

/**
 * @brief Bounded-deadline probe that the daemon's data dir is reachable and writable.
 *
 * Catches the case where the data dir lives on a hung NFS/SMB/external mount, where
 * subsequent operations (lock acquisition, sqlite3_open) would block in
 * uninterruptible kernel sleep with no visible progress.
 *
 * The probe runs on a detached thread so a stuck syscall cannot block the caller
 * past the hard deadline. On timeout we deliberately leak the detached thread —
 * a thread blocked in kernel I/O cannot be cancelled, and the daemon is going to
 * exit anyway.
 */
Result<StoragePreflightResult>
probeStorage(const std::filesystem::path& dataDir,
             std::chrono::milliseconds hardDeadline = std::chrono::seconds(2),
             std::chrono::milliseconds slowThreshold = std::chrono::milliseconds(500));

} // namespace yams::daemon
