/**
 * @file file_lock.cpp
 * @brief yams::common::ExclusiveFileLock (declared in yams/common/file_lock.h)
 */

#include <yams/common/file_lock.h>

#include <cerrno>
#include <string>
#include <system_error>
#include <thread>

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

ExclusiveFileLock::~ExclusiveFileLock() {
    release();
}

Result<void> ExclusiveFileLock::acquire(const std::filesystem::path& lockFile,
                                        std::chrono::milliseconds timeout) {
    release();
    if (auto opened = openLockFile(lockFile); !opened) {
        return opened;
    }
    constexpr auto kRetryInterval = std::chrono::milliseconds(50);
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        auto locked = tryLockFile();
        if (!locked) {
            release();
            return locked.error();
        }
        if (locked.value()) {
            return {};
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            release();
            return Error{ErrorCode::ResourceExhausted,
                         "Timed out waiting for lock " + lockFile.string()};
        }
        // Cross-process lock with a bounded wait: there is nothing to block on but the lock
        // itself, so poll it.
        std::this_thread::sleep_for(kRetryInterval);
    }
}

#ifdef _WIN32

namespace {
HANDLE toHandle(std::intptr_t value) {
    return reinterpret_cast<HANDLE>(value);
}
} // namespace

Result<void> ExclusiveFileLock::openLockFile(const std::filesystem::path& lockFile) {
    HANDLE handle = ::CreateFileW(lockFile.c_str(), GENERIC_READ | GENERIC_WRITE,
                                  FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr,
                                  OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        return Error{ErrorCode::IOError, "Cannot open lock file " + lockFile.string() + " (error " +
                                             std::to_string(::GetLastError()) + ")"};
    }
    handle_ = reinterpret_cast<std::intptr_t>(handle);
    return {};
}

Result<bool> ExclusiveFileLock::tryLockFile() {
    OVERLAPPED position{};
    if (::LockFileEx(toHandle(handle_), LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, 1,
                     0, &position)) {
        return true;
    }
    const DWORD error = ::GetLastError();
    if (error == ERROR_LOCK_VIOLATION || error == ERROR_IO_PENDING) {
        return false;
    }
    return Error{ErrorCode::IOError, "Cannot lock file (error " + std::to_string(error) + ")"};
}

void ExclusiveFileLock::release() noexcept {
    if (handle_ == kNoHandle) {
        return;
    }
    OVERLAPPED position{};
    ::UnlockFileEx(toHandle(handle_), 0, 1, 0, &position);
    ::CloseHandle(toHandle(handle_));
    handle_ = kNoHandle;
}

#else

Result<void> ExclusiveFileLock::openLockFile(const std::filesystem::path& lockFile) {
    const int fd =
        ::open(lockFile.string().c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd < 0) {
        const int error = errno;
        return Error{ErrorCode::IOError, "Cannot open lock file " + lockFile.string() + ": " +
                                             std::generic_category().message(error)};
    }
    handle_ = fd;
    return {};
}

Result<bool> ExclusiveFileLock::tryLockFile() {
    for (;;) {
        if (::flock(static_cast<int>(handle_), LOCK_EX | LOCK_NB) == 0) {
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

void ExclusiveFileLock::release() noexcept {
    if (handle_ != kNoHandle) {
        ::close(static_cast<int>(handle_)); // releases the flock
        handle_ = kNoHandle;
    }
}

#endif

} // namespace yams::common
