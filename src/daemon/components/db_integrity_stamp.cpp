#include "../../../include/yams/daemon/components/db_integrity_stamp.h"

#include <atomic>
#include <cerrno>
#include <cinttypes>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <unordered_map>
#include <vector>

#if defined(_WIN32)
#include <windows.h>
#else
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>
#endif

namespace yams::daemon {
namespace {

constexpr std::uint64_t kStampVersion = 1;
const std::string_view kCleanState = "clean";
const std::string_view kInProgressState = "in_progress";

std::atomic_uint64_t gClaimSequence{0};

#if defined(_WIN32)
std::wstring windowsPath(std::string_view path) {
    if (path.empty()) {
        return {};
    }
    const int size = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path.data(),
                                         static_cast<int>(path.size()), nullptr, 0);
    if (size <= 0) {
        return {};
    }
    std::wstring wide(static_cast<std::size_t>(size), L'\0');
    if (MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path.data(),
                            static_cast<int>(path.size()), wide.data(), size) != size) {
        return {};
    }
    return wide;
}
#endif

struct DbFingerprint {
    std::uintmax_t size{0};
    std::int64_t mtimeTicks{0};
    bool identityAvailable{false};
    std::uint64_t device{0};
    std::uint64_t inode{0};
};

struct StampRecord {
    std::uint64_t version{0};
    std::string state;
    DbFingerprint fingerprint;
};

template <typename Integer> bool parseInteger(std::string_view text, Integer& value) {
    static_assert(std::is_integral<Integer>::value, "integrity stamp fields must be integers");
    if (text.empty() || text.front() == '+' || text.front() == ' ' || text.front() == '\t' ||
        text.front() == '\n' || text.front() == '\r') {
        return false;
    }

    std::string owned(text);
    char* end = nullptr;
    errno = 0;
    if constexpr (std::is_signed<Integer>::value) {
        const auto parsed = std::strtoimax(owned.c_str(), &end, 10);
        if (errno == ERANGE || end != owned.c_str() + owned.size() ||
            parsed < static_cast<std::intmax_t>(std::numeric_limits<Integer>::min()) ||
            parsed > static_cast<std::intmax_t>(std::numeric_limits<Integer>::max())) {
            return false;
        }
        value = static_cast<Integer>(parsed);
    } else {
        if (text.front() == '-') {
            return false;
        }
        const auto parsed = std::strtoumax(owned.c_str(), &end, 10);
        if (errno == ERANGE || end != owned.c_str() + owned.size() ||
            parsed > static_cast<std::uintmax_t>(std::numeric_limits<Integer>::max())) {
            return false;
        }
        value = static_cast<Integer>(parsed);
    }
    return true;
}

struct DbFingerprintRead {
    bool available{false};
    DbFingerprint fingerprint;
    Error error;
};

DbFingerprintRead fingerprintDatabase(const std::string& dbPath) {
    DbFingerprint fingerprint;
#if defined(_WIN32)
    const auto wideDbPath = windowsPath(dbPath);
    HANDLE handle = wideDbPath.empty()
                        ? INVALID_HANDLE_VALUE
                        : CreateFileW(wideDbPath.c_str(), FILE_READ_ATTRIBUTES,
                                      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                                      nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        return {false,
                {},
                Error{ErrorCode::IOError, "cannot open database identity for integrity stamp"}};
    }
    BY_HANDLE_FILE_INFORMATION info{};
    const bool identityRead = GetFileInformationByHandle(handle, &info) != 0;
    CloseHandle(handle);
    if (!identityRead) {
        return {false,
                {},
                Error{ErrorCode::IOError, "cannot read database identity for integrity stamp"}};
    }
    if ((info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0) {
        return {
            false,
            {},
            Error{ErrorCode::InvalidArgument, "database path is not a regular file: " + dbPath}};
    }
    fingerprint.size = (static_cast<std::uintmax_t>(info.nFileSizeHigh) << 32U) |
                       static_cast<std::uintmax_t>(info.nFileSizeLow);
    const auto fileTimeTicks =
        (static_cast<std::uint64_t>(info.ftLastWriteTime.dwHighDateTime) << 32U) |
        static_cast<std::uint64_t>(info.ftLastWriteTime.dwLowDateTime);
    if (fileTimeTicks > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
        return {false,
                {},
                Error{ErrorCode::InvalidData,
                      "database timestamp cannot be represented in integrity stamp"}};
    }
    fingerprint.mtimeTicks = static_cast<std::int64_t>(fileTimeTicks);
    fingerprint.identityAvailable = true;
    fingerprint.device = static_cast<std::uint64_t>(info.dwVolumeSerialNumber);
    fingerprint.inode = (static_cast<std::uint64_t>(info.nFileIndexHigh) << 32U) |
                        static_cast<std::uint64_t>(info.nFileIndexLow);
#else
    struct stat info{};
    if (::stat(dbPath.c_str(), &info) != 0) {
        const auto code = errno == ENOENT ? ErrorCode::FileNotFound : ErrorCode::IOError;
        return {false, {}, Error{code, "database file unavailable for integrity stamp: " + dbPath}};
    }
    if (!S_ISREG(info.st_mode)) {
        return {
            false,
            {},
            Error{ErrorCode::InvalidArgument, "database path is not a regular file: " + dbPath}};
    }
    fingerprint.size = static_cast<std::uintmax_t>(info.st_size);
#if defined(__APPLE__)
    const auto& modified = info.st_mtimespec;
#else
    const auto& modified = info.st_mtim;
#endif
    // __int128 is a GCC/Clang extension; __extension__ marks the use as intentional for -Wpedantic.
    __extension__ typedef __int128 WideTicks;
    const WideTicks modifiedTicks = static_cast<WideTicks>(modified.tv_sec) * 1'000'000'000 +
                                    static_cast<WideTicks>(modified.tv_nsec);
    if (modifiedTicks < std::numeric_limits<std::int64_t>::min() ||
        modifiedTicks > std::numeric_limits<std::int64_t>::max()) {
        return {false,
                {},
                Error{ErrorCode::InvalidData,
                      "database timestamp cannot be represented in integrity stamp"}};
    }
    fingerprint.mtimeTicks = static_cast<std::int64_t>(modifiedTicks);
    fingerprint.identityAvailable = true;
    fingerprint.device = static_cast<std::uint64_t>(info.st_dev);
    fingerprint.inode = static_cast<std::uint64_t>(info.st_ino);
#endif
    return {true, fingerprint, {}};
}

bool pathExists(const std::string& path, std::error_code& ec) {
#if defined(_WIN32)
    const auto widePath = windowsPath(path);
    if (widePath.empty()) {
        ec = std::make_error_code(std::errc::invalid_argument);
        return false;
    }
    if (GetFileAttributesW(widePath.c_str()) != INVALID_FILE_ATTRIBUTES) {
        ec.clear();
        return true;
    }
    const auto error = GetLastError();
    if (error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND) {
        ec.clear();
        return false;
    }
    ec = std::error_code(static_cast<int>(error), std::system_category());
    return false;
#else
    struct stat info{};
    if (::stat(path.c_str(), &info) == 0) {
        ec.clear();
        return true;
    }
    if (errno == ENOENT || errno == ENOTDIR) {
        ec.clear();
        return false;
    }
    ec = std::error_code(errno, std::generic_category());
    return false;
#endif
}

std::uintmax_t pathFileSize(const std::string& path, std::error_code& ec) {
#if defined(_WIN32)
    const auto widePath = windowsPath(path);
    if (widePath.empty()) {
        ec = std::make_error_code(std::errc::invalid_argument);
        return 0;
    }
    WIN32_FILE_ATTRIBUTE_DATA info{};
    if (GetFileAttributesExW(widePath.c_str(), GetFileExInfoStandard, &info) == 0) {
        ec = std::error_code(static_cast<int>(GetLastError()), std::system_category());
        return 0;
    }
    ec.clear();
    return (static_cast<std::uintmax_t>(info.nFileSizeHigh) << 32U) |
           static_cast<std::uintmax_t>(info.nFileSizeLow);
#else
    struct stat info{};
    if (::stat(path.c_str(), &info) != 0) {
        ec = std::error_code(errno, std::generic_category());
        return 0;
    }
    ec.clear();
    return static_cast<std::uintmax_t>(info.st_size);
#endif
}

void removePath(const std::string& path, std::error_code& ec) {
#if defined(_WIN32)
    const auto widePath = windowsPath(path);
    if (widePath.empty()) {
        ec = std::make_error_code(std::errc::invalid_argument);
        return;
    }
    if (DeleteFileW(widePath.c_str()) != 0) {
        ec.clear();
        return;
    }
    const auto error = GetLastError();
    if (error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND) {
        ec.clear();
        return;
    }
    ec = std::error_code(static_cast<int>(error), std::system_category());
#else
    if (::unlink(path.c_str()) == 0 || errno == ENOENT) {
        ec.clear();
        return;
    }
    ec = std::error_code(errno, std::generic_category());
#endif
}

bool sqliteSidecarExists(const std::string& dbPath, std::error_code& ec) {
    for (const auto suffix : {"-wal", "-shm"}) {
        if (pathExists(dbPath + suffix, ec)) {
            return true;
        }
        if (ec) {
            return true;
        }
    }
    return false;
}

std::string serializeRecord(std::string_view state, const DbFingerprint* fingerprint = nullptr) {
    std::string output;
    output.reserve(192);
    output += "version=" + std::to_string(kStampVersion) + "\n";
    output += "state=";
    output += state;
    output += '\n';
    if (fingerprint) {
        output += "size=" + std::to_string(fingerprint->size) + "\n";
        output += "mtime_ticks=" + std::to_string(fingerprint->mtimeTicks) + "\n";
        output += "identity_available=";
        output += fingerprint->identityAvailable ? "1\n" : "0\n";
        output += "device=" + std::to_string(fingerprint->device) + "\n";
        output += "inode=" + std::to_string(fingerprint->inode) + "\n";
    }
    return output;
}

Result<void> flushFile(const std::string& path) {
#if defined(_WIN32)
    const auto widePath = windowsPath(path);
    HANDLE handle = widePath.empty()
                        ? INVALID_HANDLE_VALUE
                        : CreateFileW(widePath.c_str(), GENERIC_WRITE,
                                      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                                      nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        return Error{ErrorCode::IOError, "cannot open integrity stamp for flush"};
    }
    const bool flushed = FlushFileBuffers(handle) != 0;
    CloseHandle(handle);
    if (!flushed) {
        return Error{ErrorCode::IOError, "cannot flush integrity stamp"};
    }
#else
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return Error{ErrorCode::IOError, "cannot open integrity stamp for fsync"};
    }
    const int rc = ::fsync(fd);
    ::close(fd);
    if (rc != 0) {
        return Error{ErrorCode::IOError, "cannot fsync integrity stamp"};
    }
#endif
    return {};
}

Result<void> syncStampDirectory(const std::string& path) {
#if defined(_WIN32)
    (void)path;
#else
    std::string parent = ".";
    if (const auto separator = path.find_last_of('/'); separator != std::string::npos) {
        parent = separator == 0 ? "/" : path.substr(0, separator);
    }
    const int dirFd = ::open(parent.c_str(), O_RDONLY);
    if (dirFd < 0) {
        return Error{ErrorCode::IOError, "cannot open integrity stamp directory for fsync"};
    }
    const int syncResult = ::fsync(dirFd);
    ::close(dirFd);
    if (syncResult != 0) {
        return Error{ErrorCode::IOError, "cannot fsync integrity stamp directory"};
    }
#endif
    return {};
}

Result<void> replaceAtomically(const std::string& path, std::string_view contents) {
    auto tmpPath = path;
    tmpPath += ".tmp";
    {
        std::ofstream output(tmpPath, std::ios::binary | std::ios::trunc);
        if (!output) {
            return Error{ErrorCode::IOError, "cannot create integrity stamp temp file: " + tmpPath};
        }
        output.write(contents.data(), static_cast<std::streamsize>(contents.size()));
        output.flush();
        if (!output) {
            std::error_code removeEc;
            removePath(tmpPath, removeEc);
            return Error{ErrorCode::IOError, "cannot write integrity stamp temp file: " + tmpPath};
        }
    }

    if (auto flushed = flushFile(tmpPath); !flushed) {
        std::error_code removeEc;
        removePath(tmpPath, removeEc);
        return flushed;
    }

#if defined(_WIN32)
    const auto wideTmpPath = windowsPath(tmpPath);
    const auto widePath = windowsPath(path);
    if (wideTmpPath.empty() || widePath.empty() ||
        MoveFileExW(wideTmpPath.c_str(), widePath.c_str(),
                    MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH) == 0) {
        std::error_code removeEc;
        removePath(tmpPath, removeEc);
        return Error{ErrorCode::IOError, "cannot atomically replace integrity stamp"};
    }
#else
    if (::rename(tmpPath.c_str(), path.c_str()) != 0) {
        const std::error_code renameError(errno, std::generic_category());
        (void)::unlink(tmpPath.c_str());
        return Error{ErrorCode::IOError,
                     "cannot atomically replace integrity stamp: " + renameError.message()};
    }

    if (auto synced = syncStampDirectory(path); !synced) {
        (void)::unlink(path.c_str());
        return synced;
    }
#endif
    return {};
}

struct StampRead {
    bool available{false};
    StampRecord record;
};

StampRead readStamp(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        return {};
    }

    std::unordered_map<std::string, std::string> fields;
    std::string line;
    while (std::getline(input, line)) {
        const auto separator = line.find('=');
        if (separator == std::string::npos) {
            return {};
        }
        fields.emplace(line.substr(0, separator), line.substr(separator + 1));
    }
    if (!input.eof()) {
        return {};
    }

    StampRecord record;
    if (!parseInteger(fields["version"], record.version)) {
        return {};
    }
    record.state = fields["state"];
    if (record.state != kCleanState) {
        return {true, record};
    }

    std::uint64_t identityAvailable = 0;
    if (!parseInteger(fields["size"], record.fingerprint.size) ||
        !parseInteger(fields["mtime_ticks"], record.fingerprint.mtimeTicks) ||
        !parseInteger(fields["identity_available"], identityAvailable) || identityAvailable > 1 ||
        !parseInteger(fields["device"], record.fingerprint.device) ||
        !parseInteger(fields["inode"], record.fingerprint.inode)) {
        return {};
    }
    record.fingerprint.identityAvailable = identityAvailable == 1;
    return {true, record};
}

bool fingerprintsMatch(const DbFingerprint& expected, const DbFingerprint& actual) {
    if (expected.size != actual.size || expected.mtimeTicks != actual.mtimeTicks) {
        return false;
    }
    if (expected.identityAvailable != actual.identityAvailable) {
        return false;
    }
    return !expected.identityAvailable ||
           (expected.device == actual.device && expected.inode == actual.inode);
}

// Stamp paths are UTF-8 strings (see windowsPath); keep that encoding across std::filesystem.
std::filesystem::path toFsPath(const std::string& utf8) {
    return std::filesystem::path(std::u8string(utf8.begin(), utf8.end()));
}

std::string fromFsPath(const std::filesystem::path& path) {
    const auto utf8 = path.u8string();
    return std::string(utf8.begin(), utf8.end());
}

// Conservative: anything other than "no such process" counts as alive.
bool processIsAlive(std::uint64_t pid) {
#if defined(_WIN32)
    if (pid > std::numeric_limits<DWORD>::max()) {
        return false;
    }
    HANDLE handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, static_cast<DWORD>(pid));
    if (handle == nullptr) {
        return GetLastError() != ERROR_INVALID_PARAMETER;
    }
    DWORD exitCode = 0;
    const bool queried = GetExitCodeProcess(handle, &exitCode) != 0;
    CloseHandle(handle);
    return !queried || exitCode == STILL_ACTIVE;
#else
    if (pid > static_cast<std::uint64_t>(std::numeric_limits<pid_t>::max())) {
        return false;
    }
    if (::kill(static_cast<pid_t>(pid), 0) == 0) {
        return true;
    }
    return errno != ESRCH;
#endif
}

struct StampClaim {
    bool succeeded{false};
    std::string path;
    Error error;
};

#if defined(_WIN32)
// Renames the stamp to `claimPath` as one exclusive claim.
//
// MoveFileExW is not one: it opens the source sharing read, write and delete, then renames
// through that handle, so two concurrent callers can both open the stamp and both "succeed",
// the second renaming the first's claim file away (and their opens make each other's reads
// fail with ERROR_SHARING_VIOLATION). Both consumers then reject a valid stamp. Opening the
// stamp with no sharing makes the open itself the claim; a concurrent consumer waits for it
// and then finds no stamp. The wait also covers short-lived opens by scanners and indexers.
Result<void> claimStampExclusively(const std::wstring& stampPath, const std::wstring& claimPath) {
    if (stampPath.empty() || claimPath.empty()) {
        return Error{ErrorCode::InvalidArgument, "integrity stamp path is not valid UTF-8"};
    }
    constexpr ULONGLONG kSharingWaitMs = 2000;
    const auto deadline = GetTickCount64() + kSharingWaitMs;
    HANDLE handle = INVALID_HANDLE_VALUE;
    for (;;) {
        handle = CreateFileW(stampPath.c_str(), DELETE | SYNCHRONIZE, 0, nullptr, OPEN_EXISTING,
                             FILE_ATTRIBUTE_NORMAL, nullptr);
        if (handle != INVALID_HANDLE_VALUE) {
            break;
        }
        const auto error = GetLastError();
        if ((error != ERROR_SHARING_VIOLATION && error != ERROR_ACCESS_DENIED) ||
            GetTickCount64() >= deadline) {
            return Error{ErrorCode::IOError,
                         "cannot atomically claim integrity stamp: error " + std::to_string(error)};
        }
        Sleep(1);
    }

    const auto nameBytes = claimPath.size() * sizeof(wchar_t);
    // FILE_RENAME_INFO ends in a variable-length name; size the buffer for header plus name.
    constexpr std::size_t kRenameHeaderBytes = sizeof(FILE_RENAME_INFO);
    std::vector<unsigned char> buffer(kRenameHeaderBytes + nameBytes, 0);
    auto* renameInfo = reinterpret_cast<FILE_RENAME_INFO*>(buffer.data());
    renameInfo->ReplaceIfExists = FALSE;
    renameInfo->RootDirectory = nullptr;
    renameInfo->FileNameLength = static_cast<DWORD>(nameBytes);
    std::memcpy(renameInfo->FileName, claimPath.data(), nameBytes);
    const bool renamed = SetFileInformationByHandle(handle, FileRenameInfo, renameInfo,
                                                    static_cast<DWORD>(buffer.size())) != 0;
    const auto renameError = GetLastError();
    CloseHandle(handle);
    if (!renamed) {
        return Error{ErrorCode::IOError, "cannot atomically claim integrity stamp: error " +
                                             std::to_string(renameError)};
    }
    return {};
}
#endif

StampClaim claimStamp(const std::string& stampPath) {
    auto claimPath = stampPath;
    claimPath += ".claim.";
#if defined(_WIN32)
    claimPath += std::to_string(GetCurrentProcessId());
#else
    claimPath += std::to_string(static_cast<std::uint64_t>(::getpid()));
#endif
    claimPath += ".";
    claimPath += std::to_string(gClaimSequence.fetch_add(1, std::memory_order_relaxed));

#if defined(_WIN32)
    if (auto claimed = claimStampExclusively(windowsPath(stampPath), windowsPath(claimPath));
        !claimed) {
        return {false, {}, Error{claimed.error().code, claimed.error().message}};
    }
#else
    if (::rename(stampPath.c_str(), claimPath.c_str()) != 0) {
        const std::error_code renameError(errno, std::generic_category());
        return {false,
                {},
                Error{ErrorCode::IOError,
                      "cannot atomically claim integrity stamp: " + renameError.message()}};
    }
    if (auto synced = syncStampDirectory(stampPath); !synced) {
        return {false, {}, Error{synced.error().code, synced.error().message}};
    }
#endif
    return {true, claimPath, {}};
}

} // namespace

std::string dbIntegrityStampPath(const std::string& dbPath) {
    return dbPath + ".integrity";
}

Result<void> clearDbCleanShutdownSidecars(const std::string& dbPath) {
    const std::string walPath = dbPath + "-wal";
    const std::string shmPath = dbPath + "-shm";
    std::error_code ec;
    if (pathExists(walPath, ec)) {
        const auto walSize = pathFileSize(walPath, ec);
        if (ec) {
            return Error{ErrorCode::IOError, "cannot read SQLite WAL sidecar: " + ec.message()};
        }
        if (walSize != 0) {
            return Error{ErrorCode::InvalidState,
                         "non-empty SQLite WAL prevents clean-shutdown stamp"};
        }
        removePath(walPath, ec);
        if (ec) {
            return Error{ErrorCode::IOError,
                         "cannot remove empty SQLite WAL sidecar: " + ec.message()};
        }
    } else if (ec) {
        return Error{ErrorCode::IOError, "cannot inspect SQLite WAL sidecar: " + ec.message()};
    }

    if (pathExists(shmPath, ec)) {
        // SQLite SHM is fixed-size coordination metadata and may remain non-empty after every
        // handle is closed. Once the WAL is empty and the caller has proven quiescence, it is
        // safe for the database owner to remove the orphaned SHM sidecar.
        removePath(shmPath, ec);
        if (ec) {
            return Error{ErrorCode::IOError,
                         "cannot remove SQLite SHM sidecar after clean checkpoint: " +
                             ec.message()};
        }
    } else if (ec) {
        return Error{ErrorCode::IOError, "cannot inspect SQLite SHM sidecar: " + ec.message()};
    }
    return {};
}

Result<void> publishDbCleanShutdownStamp(const std::string& dbPathString) {
    std::error_code sidecarEc;
    if (sqliteSidecarExists(dbPathString, sidecarEc)) {
        if (sidecarEc) {
            return Error{ErrorCode::IOError,
                         "cannot inspect SQLite sidecars: " + sidecarEc.message()};
        }
        return Error{ErrorCode::InvalidState,
                     "SQLite WAL/SHM sidecar prevents clean-shutdown stamp"};
    }

    auto fingerprint = fingerprintDatabase(dbPathString);
    if (!fingerprint.available) {
        return fingerprint.error;
    }
    return replaceAtomically(dbIntegrityStampPath(dbPathString),
                             serializeRecord(kCleanState, &fingerprint.fingerprint));
}

DbIntegrityStampDecision consumeDbCleanShutdownStamp(const std::string& dbPathString) {
    DbIntegrityStampDecision decision;
    const std::string stampPath = dbIntegrityStampPath(dbPathString);

    auto claimedStamp = claimStamp(stampPath);
    if (!claimedStamp.succeeded) {
        std::error_code existsEc;
        const bool stampStillExists = pathExists(stampPath, existsEc);
        decision.invalidationPersisted = !stampStillExists && !existsEc;
        decision.reason =
            stampStillExists || existsEc ? claimedStamp.error.message : "stamp missing";
        return decision;
    }

    const auto record = readStamp(claimedStamp.path);
    // The atomic rename is the single-consumer claim and the durable invalidation. Recreating an
    // in-progress stamp at the canonical path would let another consumer claim that marker and
    // race this consumer, causing both to reject an otherwise valid clean-shutdown proof.
    decision.invalidationPersisted = true;

    std::error_code sidecarEc;
    decision.sidecarPresent = sqliteSidecarExists(dbPathString, sidecarEc);
    const auto fingerprint = fingerprintDatabase(dbPathString);
    if (sidecarEc) {
        decision.reason = "sidecar stat failed";
    } else if (decision.sidecarPresent) {
        decision.reason = "SQLite sidecar present";
    } else if (!record.available) {
        decision.reason = "stamp missing or malformed";
    } else if (record.record.version != kStampVersion || record.record.state != kCleanState) {
        decision.reason = "stamp is not clean or has unsupported version";
    } else if (!fingerprint.available) {
        decision.reason = fingerprint.error.message;
    } else if (!fingerprintsMatch(record.record.fingerprint, fingerprint.fingerprint)) {
        decision.reason = "database fingerprint changed";
    } else {
        decision.trustedCleanShutdown = true;
        decision.reason = "trusted clean shutdown";
    }

    std::error_code removeEc;
    removePath(claimedStamp.path, removeEc);
    return decision;
}

DbIntegrityStampSweep sweepStaleDbIntegrityStampArtifacts(const std::string& dbPath,
                                                          std::chrono::seconds minAge) {
    namespace fs = std::filesystem;
    DbIntegrityStampSweep sweep;

    const auto stampPath = toFsPath(dbIntegrityStampPath(dbPath));
    const auto stampName = fromFsPath(stampPath.filename());
    const std::string tmpName = stampName + ".tmp";
    const std::string claimPrefix = stampName + ".claim.";
    const fs::path dir = stampPath.has_parent_path() ? stampPath.parent_path() : fs::path(".");

    std::vector<std::string> stale;
    std::error_code ec;
    for (fs::directory_iterator it(dir, ec), end; !ec && it != end; it.increment(ec)) {
        const auto name = fromFsPath(it->path().filename());
        const bool isTmp = name == tmpName;
        const bool isClaim = name.size() > claimPrefix.size() && name.starts_with(claimPrefix);
        if (!isTmp && !isClaim) {
            continue; // includes the live stamp itself
        }

        std::error_code timeEc;
        const auto written = fs::last_write_time(it->path(), timeEc);
        if (timeEc || fs::file_time_type::clock::now() - written < minAge) {
            continue;
        }
        if (isClaim) {
            // `<stamp>.claim.<pid>.<sequence>`: keep it unless its owner is provably gone.
            const std::string_view rest = std::string_view(name).substr(claimPrefix.size());
            const auto dot = rest.find('.');
            std::uint64_t pid = 0;
            std::uint64_t sequence = 0;
            if (dot == std::string_view::npos || !parseInteger(rest.substr(0, dot), pid) ||
                !parseInteger(rest.substr(dot + 1), sequence) || pid == 0 || processIsAlive(pid)) {
                continue;
            }
        }

        stale.push_back(fromFsPath(it->path()));
    }
    if (ec) {
        sweep.errors.push_back("cannot scan " + fromFsPath(dir) + ": " + ec.message());
    }

    for (const auto& path : stale) {
        std::error_code removeEc;
        removePath(path, removeEc);
        if (removeEc) {
            sweep.errors.push_back("cannot remove " + path + ": " + removeEc.message());
        } else {
            sweep.removed.push_back(path);
        }
    }
    return sweep;
}

Result<void> invalidateDbCleanShutdownStamp(const std::string& dbPath) {
    const std::string stampPath = dbIntegrityStampPath(dbPath);
    auto claimedStamp = claimStamp(stampPath);
    if (!claimedStamp.succeeded) {
        std::error_code existsEc;
        if (pathExists(stampPath, existsEc) || existsEc) {
            return claimedStamp.error;
        }
    }

    auto invalidated = replaceAtomically(stampPath, serializeRecord(kInProgressState));
    if (claimedStamp.succeeded) {
        std::error_code removeEc;
        removePath(claimedStamp.path, removeEc);
    }
    return invalidated;
}

} // namespace yams::daemon
