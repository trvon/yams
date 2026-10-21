// Private disposable workspace ownership. No scratch data is a recovery authority.
#pragma once

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <vector>
#include <yams/core/types.h>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/stat.h>
#endif

namespace yams::memory_sync::detail {
struct SweepFileError : std::runtime_error {
    ErrorCode code;
    SweepFileError(ErrorCode value, const char* message)
        : std::runtime_error(message), code(value) {}
};

class SweepFile {
    using File = std::unique_ptr<std::FILE, decltype(&std::fclose)>;
    static constexpr std::string_view kMarker = "YAMS disposable memory-sync workspace v1\n";
    // A root allocation lease serializes quota checks and reservation, including adapter
    // callers sharing a backend. Per-generation leases also protect older pinned views.
    static constexpr std::size_t kMaxActive = 4;
    static constexpr std::size_t kMaxEntries = 1024;
    std::filesystem::path directory_;
    File lease_{nullptr, std::fclose};
    bool owned_{false};
#ifdef YAMS_TESTING
    static inline thread_local std::function<void()> allocationObserver_;
#endif

    static bool ownedName(std::string_view name) {
        constexpr std::string_view prefix = "sync-sweep-";
        if (!name.starts_with(prefix))
            return false;
        name.remove_prefix(prefix.size());
        const auto separator = name.find('-');
        if (separator == 0 || separator == std::string_view::npos || separator + 1 == name.size())
            return false;
        for (std::size_t i = 0; i < name.size(); ++i) {
            if (i != separator && (name[i] < '0' || name[i] > '9'))
                return false;
        }
        return name.size() <= 41; // two uint64 decimal components and their separator
    }
    static File openFile(const std::filesystem::path& path, bool create) {
        std::FILE* stream = nullptr;
#ifdef _WIN32
        const auto status = std::filesystem::symlink_status(path);
        if (std::filesystem::is_symlink(status))
            throw SweepFileError(ErrorCode::IOError, "scratch lease must not be a symlink");
        stream = _wfopen(path.c_str(), create ? L"w+bx" : L"r+b");
#else
        const int flags = O_RDWR | O_CLOEXEC | O_NOFOLLOW | (create ? O_CREAT | O_EXCL : 0);
        const int fd = ::open(path.c_str(), flags, S_IRUSR | S_IWUSR);
        if (fd >= 0) {
            struct stat status{};
            if (::fstat(fd, &status) != 0 || !S_ISREG(status.st_mode) ||
                status.st_uid != ::geteuid() || status.st_nlink != 1) {
                ::close(fd);
                throw SweepFileError(ErrorCode::IOError,
                                     "scratch lease is not an owned regular file");
            }
            stream = ::fdopen(fd, "r+b");
            if (!stream)
                ::close(fd);
        }
#endif
        return File{stream, std::fclose};
    }
    static bool tryLock(std::FILE* stream) {
#ifdef _WIN32
        OVERLAPPED position{};
        const auto handle = reinterpret_cast<HANDLE>(_get_osfhandle(_fileno(stream)));
        if (LockFileEx(handle, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0,
                       &position))
            return true;
        if (GetLastError() == ERROR_LOCK_VIOLATION)
            return false;
#else
        if (::flock(::fileno(stream), LOCK_EX | LOCK_NB) == 0)
            return true;
        if (errno == EWOULDBLOCK || errno == EAGAIN)
            return false;
#endif
        throw SweepFileError(ErrorCode::IOError, "cannot lock memory-sync scratch lease");
    }
    static File lockAllocation(const std::filesystem::path& root) {
        const auto path = root / "allocation.lease";
        auto lease = openFile(path, false);
        if (!lease) {
            lease = openFile(path, true);
            if (!lease) {
                lease = openFile(path, false); // another allocator may have created it
            }
        }
        if (!lease) {
            throw SweepFileError(ErrorCode::IOError, "cannot open scratch allocation lease");
        }
        if (!tryLock(lease.get())) {
            throw SweepFileError(ErrorCode::ResourceExhausted,
                                 "memory-sync scratch allocation is busy");
        }
        // This coordination file stays empty and is never truncated or deleted. There is
        // no partially written initialization marker that could strand recovery after a crash.
        // Ownership/regular-file/link checks are the same as for a generation's native lease.
        if (std::fgetc(lease.get()) != EOF || std::ferror(lease.get()) != 0) {
            throw SweepFileError(ErrorCode::IOError,
                                 "unexpected scratch allocation lease contents");
        }
        return lease;
    }
    static void checkOwnedDirectory(const std::filesystem::path& directory) {
        if (!std::filesystem::is_directory(std::filesystem::symlink_status(directory)))
            throw SweepFileError(ErrorCode::IOError, "scratch workspace must not follow a symlink");
#ifndef _WIN32
        struct stat status{};
        if (::lstat(directory.c_str(), &status) != 0 || status.st_uid != ::geteuid())
            throw SweepFileError(ErrorCode::IOError, "scratch workspace has a different owner");
#endif
    }
    static void destroyDirectory(const std::filesystem::path& directory, File& lease) {
        // Validate the entire small directory before removing anything. Never follow links,
        // recurse, or delete an unexpected user file just because its parent matches a name.
        std::vector<std::filesystem::path> files;
        for (const auto& entry : std::filesystem::directory_iterator(directory)) {
            const auto name = entry.path().filename().string();
            if ((name != "lease" && name != "winners.sqlite" && name != "winners.sqlite-journal" &&
                 name != "winners.sqlite-wal" && name != "winners.sqlite-shm") ||
                !std::filesystem::is_regular_file(entry.symlink_status())) {
                throw SweepFileError(ErrorCode::IOError, "unrecognized file in scratch workspace");
            }
#ifndef _WIN32
            struct stat status{};
            if (::lstat(entry.path().c_str(), &status) != 0 || status.st_uid != ::geteuid() ||
                status.st_nlink != 1)
                throw SweepFileError(ErrorCode::IOError, "scratch file ownership changed");
#endif
            if (name != "lease")
                files.push_back(entry.path());
        }
        for (const auto& file : files)
            std::filesystem::remove(file);
        // Windows CRT handles do not share deletion. All DB files are already gone, so
        // releasing the lock here cannot let another process open a live database.
        lease.reset();
        std::filesystem::remove(directory / "lease");
        std::filesystem::remove(directory);
    }
    static void scavenge(const std::filesystem::path& root,
                         const std::function<Result<void>()>& cooperate) {
        std::size_t entries = 0;
        std::size_t active = 0;
        for (const auto& entry : std::filesystem::directory_iterator(root)) {
            if (++entries > kMaxEntries)
                throw SweepFileError(ErrorCode::ResourceExhausted,
                                     "scratch cleanup entry budget exhausted");
            if (cooperate) {
                auto ready = cooperate();
                if (!ready)
                    throw SweepFileError(ready.error().code, ready.error().message.c_str());
            }
            if (!ownedName(entry.path().filename().string()))
                continue;
            checkOwnedDirectory(entry.path());
            auto lease = openFile(entry.path() / "lease", false);
            if (!lease) {
                if (std::filesystem::is_empty(entry.path())) {
                    std::filesystem::remove(entry.path()); // crashed before reserving its lease
                    continue;
                }
                throw SweepFileError(ErrorCode::IOError,
                                     "unrecognized scratch workspace without a lease");
            }
            if (!tryLock(lease.get())) {
                ++active;
                continue;
            }
            std::array<char, kMarker.size() + 1> marker{};
            const auto length = std::fread(marker.data(), 1, marker.size(), lease.get());
            const bool initialized =
                length == kMarker.size() && std::string_view(marker.data(), length) == kMarker;
            const bool reservedOnly =
                length == 0 && !std::filesystem::exists(entry.path() / "winners.sqlite");
            if (std::ferror(lease.get()) || (!initialized && !reservedOnly))
                throw SweepFileError(ErrorCode::IOError, "unrecognized scratch ownership marker");
            destroyDirectory(entry.path(), lease);
        }
        if (active >= kMaxActive)
            throw SweepFileError(ErrorCode::ResourceExhausted,
                                 "too many pinned memory-sync generations");
    }

public:
    std::filesystem::path path;
    explicit SweepFile(const std::filesystem::path& root,
                       const std::function<Result<void>()>& cooperate = {}) {
        std::filesystem::create_directories(root);
        checkOwnedDirectory(root);
        const auto allocationLease = lockAllocation(root);
        scavenge(root, cooperate);
#ifdef YAMS_TESTING
        if (allocationObserver_)
            allocationObserver_();
#endif
        static std::atomic<std::uint64_t> sequence{0};
        try {
            for (int attempt = 0; attempt < 16; ++attempt) {
                const auto stamp = static_cast<std::uint64_t>(
                    std::chrono::steady_clock::now().time_since_epoch().count());
                directory_ = root / ("sync-sweep-" + std::to_string(stamp) + "-" +
                                     std::to_string(sequence.fetch_add(1)));
                if (!std::filesystem::create_directory(directory_))
                    continue;
                owned_ = true;
                std::filesystem::permissions(directory_, std::filesystem::perms::owner_all);
                lease_ = openFile(directory_ / "lease", true);
                if (!lease_ || !tryLock(lease_.get()))
                    throw SweepFileError(ErrorCode::IOError,
                                         "cannot reserve scratch ownership lease");
                if (std::fwrite(kMarker.data(), 1, kMarker.size(), lease_.get()) !=
                        kMarker.size() ||
                    std::fflush(lease_.get()) != 0)
                    throw SweepFileError(ErrorCode::IOError,
                                         "cannot write scratch ownership marker");
#ifdef _WIN32
                if (_commit(_fileno(lease_.get())) != 0)
#else
                if (::fsync(::fileno(lease_.get())) != 0)
#endif
                    throw SweepFileError(ErrorCode::IOError,
                                         "cannot flush scratch ownership marker");
                path = directory_ / "winners.sqlite";
                auto reserved = openFile(path, true);
                if (!reserved)
                    throw SweepFileError(ErrorCode::IOError, "cannot reserve scratch database");
                return;
            }
            throw SweepFileError(ErrorCode::ResourceExhausted,
                                 "cannot allocate a unique scratch workspace");
        } catch (...) {
            if (owned_) {
                try {
                    destroyDirectory(directory_, lease_);
                } catch (const std::exception&) {
                    std::fputs("memory-sync scratch cleanup failed; owned artifact retained\n",
                               stderr);
                }
            }
            throw;
        }
    }
    ~SweepFile() {
        if (owned_) {
            try {
                destroyDirectory(directory_, lease_);
            } catch (const std::exception&) {
                std::fputs("memory-sync scratch cleanup failed; owned artifact retained\n", stderr);
            }
        }
    }
#ifdef YAMS_TESTING
    static void testingSetAllocationObserver(std::function<void()> observer) {
        allocationObserver_ = std::move(observer);
    }
#endif
    SweepFile(const SweepFile&) = delete;
    SweepFile& operator=(const SweepFile&) = delete;
};
} // namespace yams::memory_sync::detail
