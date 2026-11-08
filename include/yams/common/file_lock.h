// yams/common/file_lock.h - exclusive advisory lock on a lock file
#pragma once

#include <cerrno>
#include <chrono>
#include <filesystem>
#include <string>
#include <system_error>
#include <thread>

#include <yams/core/types.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <Windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#endif

namespace yams::common {

/// Exclusive advisory lock on `lockFile` (POSIX flock / Windows LockFileEx), held from a
/// successful acquire() until destruction. Locks belong to the open file, so two locks on the
/// same file conflict within one process as well as across processes, and the OS releases the
/// lock if the process dies. The lock file is created if missing and never deleted.
class ExclusiveFileLock {
public:
    ExclusiveFileLock() = default;
    ExclusiveFileLock(const ExclusiveFileLock&) = delete;
    ExclusiveFileLock& operator=(const ExclusiveFileLock&) = delete;
    ~ExclusiveFileLock() { releaseLock(); }

    /// Waits up to `timeout` for the lock. ResourceExhausted means another holder kept it for
    /// the whole wait; IOError means the lock file could not be opened or locked.
    Result<void> acquire(const std::filesystem::path& lockFile, std::chrono::milliseconds timeout) {
        releaseLock();
        if (auto opened = openLockFile(lockFile); !opened) {
            return opened;
        }
        constexpr auto kRetryInterval = std::chrono::milliseconds(50);
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        for (;;) {
            auto locked = tryLockFile();
            if (!locked) {
                releaseLock();
                return locked.error();
            }
            if (locked.value()) {
                return {};
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                releaseLock();
                return Error{ErrorCode::ResourceExhausted,
                             "Timed out waiting for lock " + lockFile.string()};
            }
            // Cross-process lock with a bounded wait: there is nothing to block on but the
            // lock itself, so poll it.
            std::this_thread::sleep_for(kRetryInterval);
        }
    }

    [[nodiscard]] bool held() const noexcept {
#ifdef _WIN32
        return handle_ != INVALID_HANDLE_VALUE;
#else
        return fd_ >= 0;
#endif
    }

private:
#ifdef _WIN32
    Result<void> openLockFile(const std::filesystem::path& lockFile) {
        handle_ = ::CreateFileW(lockFile.c_str(), GENERIC_READ | GENERIC_WRITE,
                                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr,
                                OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (handle_ == INVALID_HANDLE_VALUE) {
            return Error{ErrorCode::IOError, "Cannot open lock file " + lockFile.string() +
                                                 " (error " + std::to_string(::GetLastError()) +
                                                 ")"};
        }
        return {};
    }

    Result<bool> tryLockFile() {
        OVERLAPPED position{};
        if (::LockFileEx(handle_, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0,
                         &position)) {
            return true;
        }
        const DWORD error = ::GetLastError();
        if (error == ERROR_LOCK_VIOLATION || error == ERROR_IO_PENDING) {
            return false;
        }
        return Error{ErrorCode::IOError, "Cannot lock file (error " + std::to_string(error) + ")"};
    }

    void releaseLock() noexcept {
        if (handle_ == INVALID_HANDLE_VALUE) {
            return;
        }
        OVERLAPPED position{};
        ::UnlockFileEx(handle_, 0, 1, 0, &position);
        ::CloseHandle(handle_);
        handle_ = INVALID_HANDLE_VALUE;
    }

    HANDLE handle_{INVALID_HANDLE_VALUE};
#else
    Result<void> openLockFile(const std::filesystem::path& lockFile) {
        fd_ = ::open(lockFile.string().c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
        if (fd_ < 0) {
            const int error = errno;
            return Error{ErrorCode::IOError, "Cannot open lock file " + lockFile.string() + ": " +
                                                 std::generic_category().message(error)};
        }
        return {};
    }

    Result<bool> tryLockFile() {
        for (;;) {
            if (::flock(fd_, LOCK_EX | LOCK_NB) == 0) {
                return true;
            }
            const int error = errno;
            if (error == EINTR) {
                continue;
            }
            if (error == EWOULDBLOCK || error == EAGAIN) {
                return false;
            }
            return Error{ErrorCode::IOError,
                         "Cannot lock file: " + std::generic_category().message(error)};
        }
    }

    void releaseLock() noexcept {
        if (fd_ >= 0) {
            ::close(fd_); // releases the flock
            fd_ = -1;
        }
    }

    int fd_{-1};
#endif
};

} // namespace yams::common
