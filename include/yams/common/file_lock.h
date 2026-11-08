// yams/common/file_lock.h - exclusive advisory lock on a lock file
#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>

#include <yams/core/types.h>

namespace yams::common {

/// Exclusive advisory lock on `lockFile` (POSIX flock / Windows LockFileEx), held from a
/// successful acquire() until release() or destruction. Locks belong to the open file, so two
/// locks on the same file conflict within one process as well as across processes, and the OS
/// releases the lock if the process dies. The lock file is created if missing and never
/// deleted.
///
/// Implemented in src/plugins/file_lock.cpp (yams_plugins_mgmt, its only user so far).
class ExclusiveFileLock {
public:
    ExclusiveFileLock() = default;
    ExclusiveFileLock(const ExclusiveFileLock&) = delete;
    ExclusiveFileLock& operator=(const ExclusiveFileLock&) = delete;
    ~ExclusiveFileLock();

    /// Waits up to `timeout` for the lock. ResourceExhausted means another holder kept it for
    /// the whole wait; IOError means the lock file could not be opened or locked.
    Result<void> acquire(const std::filesystem::path& lockFile, std::chrono::milliseconds timeout);

    /// Releases the lock if held. Safe to call more than once.
    void release() noexcept;

    [[nodiscard]] bool held() const noexcept { return handle_ != kNoHandle; }

private:
    Result<void> openLockFile(const std::filesystem::path& lockFile);
    Result<bool> tryLockFile();

    // A POSIX file descriptor or a Windows HANDLE, stored opaquely so this header needs no
    // platform headers. -1 is "none" for both (INVALID_HANDLE_VALUE is -1 as an intptr_t).
    static constexpr std::intptr_t kNoHandle = -1;
    std::intptr_t handle_{kNoHandle};
};

} // namespace yams::common
