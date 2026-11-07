/*
 * yams/src/downloader/disk_writer.cpp
 *
 * DiskWriter implementation:
 * - Repo-local staging under [storage.stagingDir] for atomic finalize to CAS
 * - Atomic rename into CAS when on the same filesystem
 * - EXDEV fallback: copy + fsync + replace when cross-device rename is detected
 * - Restrictive permissions for staging (0600) and read-only CAS objects (0444) on POSIX
 *
 * This is an MVP implementation focused on correctness and portability.
 */

#include <yams/common/fs_utils.h>
#include <yams/downloader/downloader.hpp>

#include <spdlog/spdlog.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <random>
#include <system_error>

#include <filesystem>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#if defined(_WIN32)
#define NOMINMAX
#include <Windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#endif

namespace yams::downloader {

namespace fs = std::filesystem;

namespace {

#if defined(__APPLE__)
void best_effort_preallocate(const fs::path& file, std::uint64_t targetSize) {
    if (targetSize == 0)
        return;

    int fd = ::open(file.c_str(), O_RDWR);
    if (fd < 0)
        return;

    fstore_t store{};
    store.fst_flags = F_ALLOCATECONTIG;
    store.fst_posmode = F_PEOFPOSMODE;
    store.fst_offset = 0;
    store.fst_length = static_cast<off_t>(targetSize);

    if (fcntl(fd, F_PREALLOCATE, &store) == -1) {
        store.fst_flags = F_ALLOCATEALL;
        (void)fcntl(fd, F_PREALLOCATE, &store);
    }

    ::close(fd);
}
#else
void best_effort_preallocate(const fs::path&, std::uint64_t) {}
#endif

} // namespace

// ---------- Helpers (platform-specific sync) ----------

static Expected<void> fsync_file(const fs::path& p) {
#if defined(_WIN32)
    HANDLE h = CreateFileW(p.wstring().c_str(), GENERIC_READ | GENERIC_WRITE,
                           FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr,
                           OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (h == INVALID_HANDLE_VALUE) {
        return Error{ErrorCode::IoError, "CreateFile failed for fsync: " + p.string()};
    }
    if (!FlushFileBuffers(h)) {
        CloseHandle(h);
        return Error{ErrorCode::IoError, "FlushFileBuffers failed for: " + p.string()};
    }
    CloseHandle(h);
    return Expected<void>{};
#else
    int fd = ::open(p.c_str(), O_RDONLY);
    if (fd < 0) {
        return Error{ErrorCode::IoError, "open() failed for fsync: " + p.string()};
    }
#if defined(__APPLE__)
    // On macOS, F_FULLFSYNC is stricter than fsync; do both, tolerating failures.
    (void)::fsync(fd);
    (void)fcntl(fd, F_FULLFSYNC);
#else
    if (::fsync(fd) != 0) {
        ::close(fd);
        return Error{ErrorCode::IoError, "fsync() failed for: " + p.string()};
    }
#endif
    ::close(fd);
    return Expected<void>{};
#endif
}

static Expected<void> fsync_dir(const fs::path& dir) {
#if defined(_WIN32)
    HANDLE h =
        CreateFileW(dir.wstring().c_str(), GENERIC_READ | GENERIC_WRITE,
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING,
                    FILE_FLAG_BACKUP_SEMANTICS, // allow opening a directory
                    nullptr);
    if (h == INVALID_HANDLE_VALUE) {
        return Error{ErrorCode::IoError, "CreateFile (directory) failed for: " + dir.string()};
    }
    if (!FlushFileBuffers(h)) {
        CloseHandle(h);
        return Error{ErrorCode::IoError,
                     "FlushFileBuffers (directory) failed for: " + dir.string()};
    }
    CloseHandle(h);
    return Expected<void>{};
#else
    int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        return Error{ErrorCode::IoError, "open(O_DIRECTORY) failed for: " + dir.string()};
    }
#if defined(__APPLE__)
    (void)::fsync(fd);
    (void)fcntl(fd, F_FULLFSYNC);
#else
    if (::fsync(fd) != 0) {
        ::close(fd);
        return Error{ErrorCode::IoError, "fsync(dir) failed for: " + dir.string()};
    }
#endif
    ::close(fd);
    return Expected<void>{};
#endif
}

static void set_file_readonly_if_possible(const fs::path& p) noexcept {
#if !defined(_WIN32)
    std::error_code ec;
    fs::permissions(p, fs::perms::owner_read | fs::perms::group_read | fs::perms::others_read,
                    fs::perm_options::replace, ec);
    if (ec) {
        spdlog::debug("Failed to set read-only permissions for {}: {}", p.string(), ec.message());
    }
#else
    (void)p;
#endif
}

static void ensure_dir_perms(const fs::path& dir, bool private_dir) {
#if !defined(_WIN32)
    std::error_code ec;
    if (private_dir) {
        fs::permissions(dir, fs::perms::owner_all, fs::perm_options::replace, ec);
    } else {
        fs::permissions(dir,
                        fs::perms::owner_all | fs::perms::group_read | fs::perms::group_exec |
                            fs::perms::others_read | fs::perms::others_exec,
                        fs::perm_options::replace, ec);
    }
    if (ec) {
        spdlog::debug("Failed to set permissions for dir {}: {}", dir.string(), ec.message());
    }
#else
    (void)dir;
    (void)private_dir;
#endif
}

static void ensure_file_private(const fs::path& p) {
#if !defined(_WIN32)
    std::error_code ec;
    fs::permissions(p, fs::perms::owner_read | fs::perms::owner_write, fs::perm_options::replace,
                    ec);
    if (ec) {
        spdlog::debug("Failed to set private file perms on {}: {}", p.string(), ec.message());
    }
#else
    (void)p;
#endif
}

// ---------- DiskWriter implementation ----------

class DiskWriter final : public IDiskWriter {
public:
    Expected<fs::path> createStagingFile(const StorageConfig& storage, std::string_view sessionId,
                                         std::string_view tempExtension) override {
        std::error_code ec;

        // Create staging directory: data/staging/downloader
        fs::path stagingRoot = storage.stagingDir;
        fs::path stagingDir = stagingRoot / "downloader";
        yams::common::ensureDirectories(stagingDir);
        if (ec) {
            return Error{ErrorCode::IoError,
                         "Failed to create staging dir: " + stagingDir.string()};
        }
        ensure_dir_perms(stagingDir, /*private_dir=*/true);

        // Unique filename: <sessionId>.<timestamp>.part (or provided extension)
        auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::steady_clock::now().time_since_epoch())
                          .count();
        std::string fn = std::string(sessionId) + "." + std::to_string(now_ns);
        if (!tempExtension.empty()) {
            if (tempExtension.front() == '.') {
                fn += std::string(tempExtension);
            } else {
                fn.push_back('.');
                fn += std::string(tempExtension);
            }
        } else {
            fn += ".part";
        }

        fs::path stagingFile = stagingDir / fn;

        // Create the file with restrictive perms; use ofstream to ensure portability
        {
            std::ofstream os(stagingFile, std::ios::binary | std::ios::out | std::ios::trunc);
            if (!os.good()) {
                return Error{ErrorCode::IoError,
                             "Failed to create staging file: " + stagingFile.string()};
            }
        }
        ensure_file_private(stagingFile);

        return stagingFile;
    }

    Expected<std::filesystem::path>
    createOrOpenStagingFile(const StorageConfig& storage, std::string_view sessionId,
                            std::string_view tempExtension,
                            [[maybe_unused]] std::optional<std::uint64_t> expectedSize,
                            std::uint64_t& currentSize) override {
        std::error_code ec;

        // Ensure staging directory exists with restrictive permissions
        fs::path stagingRoot = storage.stagingDir;
        fs::path stagingDir = stagingRoot / "downloader";
        yams::common::ensureDirectories(stagingDir);
        ensure_dir_perms(stagingDir, /*private_dir=*/true);

        // Deterministic file name for resume: "<sessionId>.part" (or custom extension)
        std::string fn = std::string(sessionId);
        if (tempExtension.empty()) {
            fn += ".part";
        } else {
            if (tempExtension.front() == '.') {
                fn += std::string(tempExtension);
            } else {
                fn.push_back('.');
                fn += std::string(tempExtension);
            }
        }
        fs::path stagingFile = stagingDir / fn;

        currentSize = 0;
        if (fs::exists(stagingFile)) {
            // Existing partial file: determine current size
            std::error_code fe;
            auto sz = fs::file_size(stagingFile, fe);
            if (!fe) {
                currentSize = static_cast<std::uint64_t>(sz);
            }
        } else {
            // Create new staging file with restrictive permissions
            std::ofstream os(stagingFile, std::ios::binary | std::ios::out | std::ios::trunc);
            if (!os.good()) {
                return Error{ErrorCode::IoError,
                             "Failed to create staging file: " + stagingFile.string()};
            }
            ensure_file_private(stagingFile);
        }

        if (expectedSize && currentSize == 0) {
            best_effort_preallocate(stagingFile, *expectedSize);
        }

        return stagingFile;
    }

    Expected<void> writeAt(const fs::path& stagingFile, std::uint64_t offset,
                           std::span<const std::byte> data) override {
        std::fstream fsio(stagingFile, std::ios::binary | std::ios::in | std::ios::out);
        if (!fsio.good()) {
            return Error{ErrorCode::IoError,
                         "Failed to open staging for write: " + stagingFile.string()};
        }

        fsio.seekp(static_cast<std::streamoff>(offset), std::ios::beg);
        if (!fsio.good()) {
            return Error{ErrorCode::IoError, "seekp failed on: " + stagingFile.string()};
        }

        fsio.write(reinterpret_cast<const char*>(data.data()),
                   static_cast<std::streamsize>(data.size()));
        if (!fsio.good()) {
            return Error{ErrorCode::IoError, "write failed on: " + stagingFile.string()};
        }

        // Do not close with flush-only; OS caching is fine; durability ensured in sync()
        fsio.close();
        return Expected<void>{};
    }

    Expected<void> sync(const fs::path& stagingFile, const StorageConfig& storage) override {
        (void)storage; // not needed for file sync
        auto r = fsync_file(stagingFile);
        if (!r.ok())
            return r;
        // Also sync the staging directory to persist directory entries
        auto dir = stagingFile.parent_path();
        return fsync_dir(dir);
    }

    Expected<fs::path> finalizeToCas(const fs::path& stagingFile, std::string_view hashHex,
                                     const StorageConfig& storage) override {
        // Resolve CAS final path
        fs::path objectsDir = storage.objectsDir;
        fs::path finalPath = casPathForSha256(objectsDir, hashHex);

        // Ensure parent directory exists
        std::error_code ec;
        yams::common::ensureDirectories(finalPath.parent_path());
        if (ec) {
            return Error{ErrorCode::IoError,
                         "Failed to create CAS dir: " + finalPath.parent_path().string()};
        }
        ensure_dir_perms(finalPath.parent_path(), /*private_dir=*/false);

        // If final already exists (dedup), remove staging and return success
        if (fs::exists(finalPath)) {
            spdlog::debug("CAS object already exists, discarding staging: {}",
                          stagingFile.string());
            std::error_code del_ec;
            fs::remove(stagingFile, del_ec);
            return finalPath;
        }

        // Sync staging file prior to rename/copy
        auto r = fsync_file(stagingFile);
        if (!r.ok())
            return Error{r.error().code, "Failed to fsync staging: " + stagingFile.string()};

        // Attempt atomic rename
        std::error_code ren_ec;
        fs::rename(stagingFile, finalPath, ren_ec);
        if (ren_ec) {
            // Detect cross-device link (EXDEV) to perform fallback copy
            if (ren_ec == std::errc::cross_device_link) {
                spdlog::warn("Cross-device rename detected; performing copy+fsync+replace for {}",
                             finalPath.string());
                auto copy_ok = detail::copyFileDurablyReplacing(stagingFile, finalPath);
                if (!copy_ok.ok()) {
                    return copy_ok.error();
                }
                // Cleanup staging after successful copy
                std::error_code del_ec;
                fs::remove(stagingFile, del_ec);
            } else {
                return Error{ErrorCode::IoError, "rename() failed (" + ren_ec.message() +
                                                     ") from " + stagingFile.string() + " to " +
                                                     finalPath.string()};
            }
        }

        // Set read-only perms for immutable CAS objects (best-effort)
        set_file_readonly_if_possible(finalPath);

        // fsync CAS directory to persist the new entry
        auto rr = fsync_dir(finalPath.parent_path());
        if (!rr.ok()) {
            spdlog::debug("fsync on CAS dir failed (continuing): {}",
                          finalPath.parent_path().string());
        }

        return finalPath;
    }

    void cleanup(const fs::path& stagingFile) noexcept override {
        std::error_code ec;
        fs::remove(stagingFile, ec);
        if (ec) {
            spdlog::debug("cleanup: failed to remove staging file {}: {}", stagingFile.string(),
                          ec.message());
        }
    }

private:
};

namespace detail {

Expected<void> copyFileDurablyReplacing(const fs::path& src, const fs::path& dst) {
    // Write beside dst and rename into place: copying straight onto dst left a truncated file at
    // the content-addressed path when the copy failed, and that path is trusted by name.
    // Each call gets its own temp file: two jobs publishing the same hash must not share one, or
    // one rename could publish a file the other is still writing.
    static std::atomic<std::uint64_t> sequence{0};
    static const std::uint64_t processSalt = std::random_device{}();
    const auto temp =
        dst.parent_path() / (dst.filename().string() + ".partial." + std::to_string(processSalt) +
                             "." + std::to_string(sequence.fetch_add(1)));
    const auto fail = [&](Error error) -> Expected<void> {
        std::error_code ec;
        fs::remove(temp, ec);
        return error;
    };
    // Streams cannot tell a read error from end of file on every platform (libc++ opens a
    // directory and then reports EOF), so require a regular file and check the byte count.
    std::error_code statEc;
    if (!fs::is_regular_file(src, statEc)) {
        return fail(
            Error{ErrorCode::IoError, "copy: source is not a regular file: " + src.string()});
    }
    const auto expectedSize = fs::file_size(src, statEc);
    if (statEc) {
        return fail(Error{ErrorCode::IoError, "copy: cannot size source: " + src.string()});
    }
    std::uintmax_t copied = 0;
    {
        std::ifstream is(src, std::ios::binary);
        if (!is.good()) {
            return fail(Error{ErrorCode::IoError, "copy: failed to open source: " + src.string()});
        }
        std::ofstream os(temp, std::ios::binary | std::ios::trunc);
        if (!os.good()) {
            return fail(Error{ErrorCode::IoError, "copy: failed to open " + temp.string()});
        }
        std::vector<char> buffer(1 << 20);
        while (is.good()) {
            is.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            const std::streamsize got = is.gcount();
            if (got > 0) {
                copied += static_cast<std::uintmax_t>(got);
                os.write(buffer.data(), got);
                if (!os.good()) {
                    return fail(
                        Error{ErrorCode::IoError, "copy: write failed for " + temp.string()});
                }
            }
        }
        if (!is.eof() || copied != expectedSize) {
            return fail(Error{ErrorCode::IoError, "copy: read failed for source: " + src.string()});
        }
        os.flush();
        if (!os.good()) {
            return fail(Error{ErrorCode::IoError, "copy: flush failed for " + temp.string()});
        }
    }
    if (auto synced = fsync_file(temp); !synced.ok()) {
        return fail(synced.error());
    }
#if defined(_WIN32)
    // Replace explicitly: the standard library's rename is not guaranteed to overwrite here.
    if (MoveFileExW(temp.wstring().c_str(), dst.wstring().c_str(),
                    MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH) == 0) {
        return fail(Error{ErrorCode::IoError, "copy: rename into place failed for " + dst.string() +
                                                  ": error " + std::to_string(GetLastError())});
    }
#else
    std::error_code ec;
    fs::rename(temp, dst, ec);
    if (ec) {
        return fail(Error{ErrorCode::IoError, "copy: rename into place failed for " + dst.string() +
                                                  ": " + ec.message()});
    }
#endif
    return fsync_dir(dst.parent_path());
}

} // namespace detail

/// Factory (optional)
std::unique_ptr<IDiskWriter> makeDiskWriter() {
    return std::make_unique<DiskWriter>();
}

} // namespace yams::downloader
