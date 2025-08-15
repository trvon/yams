#include <spdlog/spdlog.h>
#include <yams/storage/storage_engine.h>

#ifdef __unix__

#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <sys/resource.h>
#include <sys/stat.h>

namespace yams::storage::platform {

// POSIX-specific atomic rename with proper error handling
Result<void> atomicRename(const std::filesystem::path& from, const std::filesystem::path& to) {
    // Use rename() for atomic operation on POSIX systems
    if (::rename(from.c_str(), to.c_str()) == 0) {
        return {};
    }

    int err = errno;

    // Handle specific error cases
    switch (err) {
        case EEXIST: {
            // Target already exists - for content-addressed storage this is OK
            // Verify the content matches
            std::error_code ec;
            if (std::filesystem::file_size(from, ec) == std::filesystem::file_size(to, ec)) {
                // Assume same content, remove source
                std::filesystem::remove(from, ec);
                return {};
            }
            return Result<void>(ErrorCode::HashMismatch);
        }

        case EACCES:
        case EPERM:
            spdlog::error("Permission denied: rename {} to {}", from.string(), to.string());
            return Result<void>(ErrorCode::PermissionDenied);

        case ENOSPC:
            spdlog::error("No space left on device");
            return Result<void>(ErrorCode::StorageFull);

        case ENOENT:
            spdlog::error("Source file not found: {}", from.string());
            return Result<void>(ErrorCode::FileNotFound);

        default:
            spdlog::error("Rename failed: {} (errno: {})", std::strerror(err), err);
            return Result<void>(ErrorCode::Unknown);
    }
}

// POSIX-specific file sync to ensure durability
Result<void> syncFile(const std::filesystem::path& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        spdlog::error("Failed to open file for sync: {}", path.string());
        return Result<void>(ErrorCode::FileNotFound);
    }

    // Ensure data is written to disk
    int result = ::fsync(fd);
    ::close(fd);

    if (result != 0) {
        spdlog::error("Failed to sync file: {} (errno: {})", path.string(), errno);
        return Result<void>(ErrorCode::Unknown);
    }

    return {};
}

// POSIX-specific directory sync
Result<void> syncDirectory(const std::filesystem::path& dir) {
    int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        spdlog::error("Failed to open directory for sync: {}", dir.string());
        return Result<void>(ErrorCode::FileNotFound);
    }

    // Sync directory metadata
    int result = ::fsync(fd);
    ::close(fd);

    if (result != 0) {
        spdlog::error("Failed to sync directory: {} (errno: {})", dir.string(), errno);
        return Result<void>(ErrorCode::Unknown);
    }

    return {};
}

// Use O_TMPFILE for truly atomic file creation (Linux 3.11+)
Result<void> atomicWriteOptimized(const std::filesystem::path& path,
                                  std::span<const std::byte> data) {
#ifdef O_TMPFILE
    auto dirPath = path.parent_path();

    // Try to use O_TMPFILE for atomic creation
    int fd = ::open(dirPath.c_str(), O_TMPFILE | O_WRONLY, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

    if (fd >= 0) {
        // Write data
        size_t written = 0;
        size_t remaining = data.size();
        const char* ptr = reinterpret_cast<const char*>(data.data());

        while (remaining > 0) {
            ssize_t result = ::write(fd, ptr + written, remaining);
            if (result < 0) {
                if (errno == EINTR)
                    continue;
                ::close(fd);
                return Result<void>(ErrorCode::Unknown);
            }
            const size_t uresult = static_cast<size_t>(result);
            written += uresult;
            remaining -= uresult;
        }

        // Sync data to disk
        if (::fsync(fd) != 0) {
            ::close(fd);
            return Result<void>(ErrorCode::Unknown);
        }

        // Link the file into the filesystem
        std::string procPath = std::string("/proc/self/fd/") + std::to_string(fd);
        if (::linkat(AT_FDCWD, procPath.c_str(), AT_FDCWD, path.c_str(), AT_SYMLINK_FOLLOW) == 0) {
            ::close(fd);
            return {};
        }

        // Handle EEXIST specially for content-addressed storage
        if (errno == EEXIST) {
            ::close(fd);
            return {};
        }

        ::close(fd);
    }
#endif

    // Fallback to regular implementation
    return Result<void>(ErrorCode::Unknown);
}

// Get file descriptor count for monitoring
size_t getOpenFileDescriptorCount() {
    size_t count = 0;

    // Count open file descriptors in /proc/self/fd
    try {
        for ([[maybe_unused]] const auto& entry :
             std::filesystem::directory_iterator("/proc/self/fd")) {
            count++;
        }
    } catch (...) {
        // Fallback: use getrlimit
        struct rlimit rlim;
        if (getrlimit(RLIMIT_NOFILE, &rlim) == 0) {
            // Estimate based on trying to dup() file descriptors
            int maxFd = std::min(static_cast<int>(rlim.rlim_cur), 10000);
            for (int fd = 0; fd < maxFd; ++fd) {
                if (fcntl(fd, F_GETFD) != -1) {
                    count++;
                }
            }
        }
    }

    return count;
}

// Set file permissions for security
Result<void> setSecurePermissions(const std::filesystem::path& path) {
    // Set read-only permissions for stored objects
    if (::chmod(path.c_str(), S_IRUSR | S_IRGRP | S_IROTH) != 0) {
        spdlog::warn("Failed to set permissions on {}: {}", path.string(), std::strerror(errno));
        // Not a fatal error
    }

    return {};
}

} // namespace yams::storage::platform

#endif // __unix__