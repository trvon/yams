/**
 * @file plugin_installer.cpp
 * @brief Implementation of the plugin installer
 */

#include <yams/common/file_lock.h>
#include <yams/common/fs_utils.h>
#include <yams/common/hash_predicates.h>
#include <yams/config/config_helpers.h>
#include <yams/crypto/hasher.h>
#include <yams/daemon/resource/plugin_trust.h>
#include <yams/plugins/plugin_dir_names.h>
#include <yams/plugins/plugin_installer.hpp>

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <exception>
#include <fstream>
#include <optional>
#include <regex>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#ifdef _WIN32
#include <ShlObj.h>
#include <Windows.h>
#else
#include <pwd.h>
#include <unistd.h>
#endif

// Archive extraction (minitar or platform-specific)
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <archive.h>
#include <archive_entry.h>
#include <curl/curl.h>

namespace yams::plugins {

namespace fs = std::filesystem;

namespace {

// cURL progress callback data
struct ProgressData {
    InstallProgressCallback callback;
    InstallProgress progress;
};

// cURL progress callback
int curlProgressCallback(void* clientp, curl_off_t dltotal, curl_off_t dlnow,
                         curl_off_t /*ultotal*/, curl_off_t /*ulnow*/) {
    auto* data = static_cast<ProgressData*>(clientp);
    if (data && data->callback) {
        data->progress.bytesDownloaded = static_cast<uint64_t>(dlnow);
        data->progress.totalBytes = dltotal > 0 ? static_cast<uint64_t>(dltotal) : 0;
        if (dltotal > 0) {
            data->progress.progress = static_cast<float>(dlnow) / static_cast<float>(dltotal);
        }
        data->callback(data->progress);
    }
    return 0;
}

// cURL write callback
size_t curlWriteToFile(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* file = static_cast<std::ofstream*>(userdata);
    if (file && file->is_open()) {
        file->write(ptr, static_cast<std::streamsize>(size * nmemb));
        return size * nmemb;
    }
    return 0;
}

bool isSafePathSegment(std::string_view value, bool allowPlus = false) {
    if (value.empty() || value == "." || value == "..") {
        return false;
    }
    for (unsigned char ch : value) {
        if (std::isalnum(ch) || ch == '_' || ch == '-' || ch == '.' || (allowPlus && ch == '+')) {
            continue;
        }
        return false;
    }
    return true;
}

Result<void> validatePluginName(std::string_view name) {
    // Reserved names (a leading '.') hold the installer's own bookkeeping, so a plugin
    // directory can never be mistaken for it.
    if (!isSafePathSegment(name) || isReservedPluginDirName(name)) {
        return Error{ErrorCode::InvalidArgument,
                     "Invalid plugin name. Use only letters, numbers, '.', '_' and '-', and do "
                     "not start it with '.'."};
    }
    return {};
}

Result<void> validatePluginVersion(std::string_view version) {
    if (!isSafePathSegment(version, true)) {
        return Error{ErrorCode::InvalidArgument,
                     "Invalid plugin version. Use only letters, numbers, '.', '_', '-' and '+'."};
    }
    return {};
}

// Archive entry names must stay relative and never step upward. "." components ("./lib.so")
// and the empty trailing component of a directory entry ("plugin/") are harmless and are what
// `tar czf` / `tar -C dir .` produce. This only validates the entry *name*; isPathWithin() is a
// lexical check on the joined path, and link targets are validated separately in
// extractArchive().
bool isRelativePathSafe(const fs::path& path) {
    if (path.empty() || path.is_absolute() || path.has_root_name() || path.has_root_directory()) {
        return false;
    }
    for (const auto& part : path) {
        if (part == "..") {
            return false;
        }
    }
    return true;
}

fs::path absoluteLexical(const fs::path& path) {
    std::error_code ec;
    auto absolute = fs::absolute(path, ec);
    if (ec) {
        return path.lexically_normal();
    }
    return absolute.lexically_normal();
}

bool isPathWithin(const fs::path& root, const fs::path& child) {
    const auto rootNorm = absoluteLexical(root);
    const auto childNorm = absoluteLexical(child);
    auto rootIt = rootNorm.begin();
    auto childIt = childNorm.begin();
    for (; rootIt != rootNorm.end(); ++rootIt, ++childIt) {
        if (childIt == childNorm.end() || *rootIt != *childIt) {
            return false;
        }
    }
    return true;
}

Result<void> removeAllScoped(const fs::path& path, const fs::path& root,
                             std::string_view description) {
    const auto rootNorm = absoluteLexical(root);
    const auto pathNorm = absoluteLexical(path);
    if (pathNorm == rootNorm || !isPathWithin(rootNorm, pathNorm)) {
        return Error{ErrorCode::InvalidPath, "Refusing recursive delete outside scoped " +
                                                 std::string(description) +
                                                 " root: " + path.string()};
    }

    std::error_code ec;
    fs::remove_all(pathNorm, ec);
    if (ec) {
        return Error{ErrorCode::IOError,
                     "Failed to remove " + std::string(description) + ": " + ec.message()};
    }
    return {};
}

// SHA-256 of a file in the "sha256:<lowercase hex>" form; empty on failure.
std::string computeSha256(const fs::path& filepath) {
    try {
        return "sha256:" + yams::crypto::SHA256Hasher{}.hashFile(filepath);
    } catch (const std::exception& e) {
        spdlog::warn("Failed to hash '{}': {}", filepath.string(), e.what());
        return "";
    }
}

// Canonicalize a SHA-256 digest to the "sha256:<lowercase hex>" form computeSha256() produces.
// Accepts an optional "sha256:" prefix (any case) and upper- or lower-case hex.
Result<std::string> normalizeSha256(std::string_view value) {
    constexpr std::string_view kPrefix = "sha256:";
    if (value.size() >= kPrefix.size()) {
        bool hasPrefix = true;
        for (size_t i = 0; i < kPrefix.size(); ++i) {
            if (std::tolower(static_cast<unsigned char>(value[i])) != kPrefix[i]) {
                hasPrefix = false;
                break;
            }
        }
        if (hasPrefix) {
            value.remove_prefix(kPrefix.size());
        }
    }

    constexpr size_t kSha256HexLength = 64;
    if (value.size() != kSha256HexLength || !yams::common::isHexDigits(value)) {
        return Error{ErrorCode::InvalidArgument,
                     "Malformed plugin checksum: expected sha256:<64 hex characters>"};
    }

    std::string normalized(kPrefix);
    normalized.reserve(kPrefix.size() + kSha256HexLength);
    for (unsigned char ch : value) {
        normalized.push_back(static_cast<char>(std::tolower(ch)));
    }
    return normalized;
}

// Owns a libarchive reader/disk-writer pair so every exit path frees both.
struct ArchivePair {
    struct archive* reader{archive_read_new()};
    struct archive* writer{archive_write_disk_new()};

    ArchivePair() = default;
    ArchivePair(const ArchivePair&) = delete;
    ArchivePair& operator=(const ArchivePair&) = delete;
    ~ArchivePair() {
        if (reader != nullptr) {
            archive_read_free(reader);
        }
        if (writer != nullptr) {
            archive_write_free(writer);
        }
    }
};

// Builds an error carrying libarchive's message. Must be called while `handle` is still alive.
Error archiveError(ErrorCode code, struct archive* handle, const std::string& what) {
    const char* detail = handle != nullptr ? archive_error_string(handle) : nullptr;
    return Error{code, what + ": " + (detail != nullptr ? detail : "unknown libarchive error")};
}

// The components of `path` below `root` ("a/b/c" -> {a, b, c}), or nullopt if it is not below.
std::optional<std::vector<fs::path>> componentsBelow(const fs::path& root, const fs::path& path) {
    const fs::path rel = absoluteLexical(path).lexically_relative(absoluteLexical(root));
    if (rel.empty()) {
        return std::nullopt;
    }
    std::vector<fs::path> parts;
    for (const auto& part : rel) {
        if (part == "..") {
            return std::nullopt;
        }
        if (!part.empty() && part != ".") {
            parts.push_back(part);
        }
    }
    return parts;
}

// A symlink may only point at a relative target that stays inside `root`. The target is walked
// one component at a time from the link's directory, and a ".." that would climb above `root`
// is rejected even if later components would come back down by name: "../evil/x" from
// root/evil names a sibling of `root` once `root` is moved somewhere else.
//
// ".." is allowed only as a leading run ("../libfoo.so.1", common for versioned libraries):
// those steps walk the link's own ancestors, which are real directories because
// SECURE_SYMLINKS refuses to create an entry under a symlinked parent. A ".." after a name could
// step back out of a symlinked directory, so it is rejected. A link that names itself, its own
// directory or one of that directory's ancestors ("sub -> .", "d/up -> ..", "d/x -> ../d") is
// a cycle and is rejected; loops through several links are caught by prepareExtractedPlugin().
bool isSymlinkTargetSafe(const fs::path& root, const fs::path& linkPath, const fs::path& target) {
    if (target.empty() || target.is_absolute() || target.has_root_name() ||
        target.has_root_directory()) {
        return false;
    }
    auto linkParts = componentsBelow(root, linkPath);
    if (!linkParts || linkParts->empty()) {
        return false;
    }
    std::vector<fs::path> resolved(linkParts->begin(), linkParts->end() - 1); // link's directory
    bool seenName = false;
    for (const auto& part : target) {
        if (part.empty() || part == ".") {
            continue;
        }
        if (part == "..") {
            if (seenName || resolved.empty()) {
                return false;
            }
            resolved.pop_back();
            continue;
        }
        seenName = true;
        resolved.push_back(part);
    }
    if (resolved == *linkParts) {
        return false; // the link itself
    }
    const size_t dirDepth = linkParts->size() - 1;
    const bool isDirOrAncestor = resolved.size() <= dirDepth &&
                                 std::equal(resolved.begin(), resolved.end(), linkParts->begin());
    return !isDirOrAncestor;
}

// Windows has two kinds of symlink, and only a directory symlink can be traversed into a
// directory. libarchive makes a file symlink unless the tar entry marks the link as a
// directory link, which ordinary tar files do not, so "current -> lib" fails with "Access is
// denied". A link that does not open as a directory but whose target is one is recreated as
// a directory symlink. Elsewhere the first check always passes and nothing changes.
Result<void> repairDirectorySymlink(const fs::path& link, const fs::path& target) {
    std::error_code ec;
    if (fs::is_directory(fs::status(link, ec)) ||
        !fs::is_directory(link.parent_path() / target, ec)) {
        return {};
    }
    fs::remove(link, ec);
    if (!ec) {
        fs::create_directory_symlink(target, link, ec);
    }
    if (ec) {
        return Error{ErrorCode::IOError, "Failed to recreate directory symlink " +
                                             link.filename().string() + ": " + ec.message()};
    }
    return {};
}

// Archive metadata that is never plugin content, at any depth: macOS resource forks and
// Finder files that archivers on macOS add next to the real entries.
bool isArchiveJunk(const fs::path& name) {
    const std::string s = name.filename().string();
    return s == "__MACOSX" || s == ".DS_Store" || s.rfind("._", 0) == 0;
}

// Extract a tar.gz archive into `destDir`, refusing any entry that could write outside it.
Result<void> extractArchive(const fs::path& archivePath, const fs::path& destDir) {
    ArchivePair handles;
    if (handles.reader == nullptr || handles.writer == nullptr) {
        return Error{ErrorCode::InternalError, "Failed to initialize libarchive"};
    }
    struct archive* a = handles.reader;
    struct archive* ext = handles.writer;

    archive_read_support_format_tar(a);
    archive_read_support_filter_gzip(a);

    // Entry paths are rewritten below to absolute paths under the (canonical) extraction root,
    // so ARCHIVE_EXTRACT_SECURE_NOABSOLUTEPATHS cannot be used; absolute names, root names and
    // ".." components are rejected by isRelativePathSafe() before the rewrite instead.
    // SECURE_SYMLINKS stops a later entry from being written through an extracted symlink.
    // ACLs and file flags are not restored: plugins do not need them, and flags such as
    // macOS uchg would make the installed files undeletable.
    archive_write_disk_set_options(ext, ARCHIVE_EXTRACT_TIME | ARCHIVE_EXTRACT_PERM |
                                            ARCHIVE_EXTRACT_SECURE_SYMLINKS |
                                            ARCHIVE_EXTRACT_SECURE_NODOTDOT);

    if (archive_read_open_filename(a, archivePath.string().c_str(), 10240) != ARCHIVE_OK) {
        return archiveError(ErrorCode::CorruptedData, a, "Failed to open plugin archive");
    }

    if (!yams::common::ensureDirectories(destDir)) {
        return Error{ErrorCode::IOError, "Failed to create destination directory"};
    }
    // Canonical root: no symlinked prefix (e.g. /var -> /private/var on macOS) for
    // SECURE_SYMLINKS to trip over, and a stable base for the containment checks.
    std::error_code ec;
    const fs::path root = fs::canonical(destDir, ec);
    if (ec) {
        return Error{ErrorCode::IOError, "Failed to resolve extraction directory: " + ec.message()};
    }

    struct archive_entry* entry = nullptr;
    for (;;) {
        int r = archive_read_next_header(a, &entry);
        if (r == ARCHIVE_EOF) {
            break;
        }
        if (r == ARCHIVE_WARN) {
            spdlog::warn("Plugin archive header warning: {}", archive_error_string(a));
        } else if (r != ARCHIVE_OK) {
            return archiveError(ErrorCode::CorruptedData, a, "Failed to read plugin archive");
        }

        const char* rawName = archive_entry_pathname(entry);
        if (rawName == nullptr) {
            return Error{ErrorCode::CorruptedData, "Archive entry is missing a path"};
        }
        const std::string name{rawName};

        if (archive_entry_hardlink(entry) != nullptr) {
            return Error{ErrorCode::InvalidPath,
                         "Hard link entries are not allowed in plugin archives: " + name};
        }

        const fs::path relativeEntryPath{name};
        if (!isRelativePathSafe(relativeEntryPath)) {
            return Error{ErrorCode::InvalidPath, "Archive entry escapes extraction root: " + name};
        }

        fs::path entryPath = (root / relativeEntryPath).lexically_normal();
        if (!entryPath.has_filename()) {
            entryPath = entryPath.parent_path(); // drop a directory entry's trailing separator
        }
        if (!isPathWithin(root, entryPath)) {
            return Error{ErrorCode::InvalidPath, "Archive entry escapes extraction root: " + name};
        }

        const auto fileType = archive_entry_filetype(entry);

        // "." / "./" name the extraction root itself. A directory entry for it carries nothing
        // but metadata, which must not be applied to the root; anything else would replace it.
        if (entryPath == root) {
            if (fileType != AE_IFDIR) {
                return Error{ErrorCode::InvalidPath,
                             "Archive entry would replace the extraction root: " + name};
            }
            if (archive_read_data_skip(a) != ARCHIVE_OK) {
                return archiveError(ErrorCode::CorruptedData, a, "Failed to read plugin archive");
            }
            continue;
        }

        if (fileType == AE_IFLNK) {
            const char* rawTarget = archive_entry_symlink(entry);
            if (rawTarget == nullptr ||
                !isSymlinkTargetSafe(root, entryPath, fs::path{std::string{rawTarget}})) {
                return Error{ErrorCode::InvalidPath,
                             "Symlink entry points outside the extraction root: " + name};
            }
        } else if (fileType != AE_IFREG && fileType != AE_IFDIR) {
            return Error{ErrorCode::InvalidPath,
                         "Unsupported entry type in plugin archive: " + name};
        }

        archive_entry_set_pathname(entry, entryPath.string().c_str());

        r = archive_write_header(ext, entry);
        if (r == ARCHIVE_WARN) {
            spdlog::warn("Plugin archive entry '{}' extracted with warning: {}", name,
                         archive_error_string(ext));
        } else if (r != ARCHIVE_OK) {
#ifdef _WIN32
            if (fileType == AE_IFLNK) {
                // Creating a symlink needs Developer Mode or SeCreateSymbolicLinkPrivilege.
                return archiveError(ErrorCode::PermissionDenied, ext,
                                    "Cannot create symlink " + name +
                                        " from the plugin archive (Windows requires Developer "
                                        "Mode or the 'Create symbolic links' privilege)");
            }
#endif
            return archiveError(ErrorCode::CorruptedData, ext, "Failed to extract " + name);
        }

        if (fileType == AE_IFREG && archive_entry_size(entry) > 0) {
            const void* buff = nullptr;
            size_t size = 0;
            la_int64_t offset = 0;
            for (;;) {
                r = archive_read_data_block(a, &buff, &size, &offset);
                if (r == ARCHIVE_EOF) {
                    break;
                }
                if (r != ARCHIVE_OK) {
                    return archiveError(ErrorCode::CorruptedData, a, "Failed to read " + name);
                }
                if (archive_write_data_block(ext, buff, size, offset) != ARCHIVE_OK) {
                    return archiveError(ErrorCode::CorruptedData, ext, "Failed to write " + name);
                }
            }
        }

        r = archive_write_finish_entry(ext);
        if (r == ARCHIVE_WARN) {
            spdlog::warn("Plugin archive entry '{}' finished with warning: {}", name,
                         archive_error_string(ext));
        } else if (r != ARCHIVE_OK) {
            return archiveError(ErrorCode::CorruptedData, ext, "Failed to finish " + name);
        }
    }

    // Applies deferred directory metadata; a failure here leaves the tree incomplete.
    const int closeResult = archive_write_close(ext);
    if (closeResult != ARCHIVE_OK && closeResult != ARCHIVE_WARN) {
        return archiveError(ErrorCode::CorruptedData, ext, "Failed to finalize plugin extraction");
    }

    return {};
}

// Files whose presence at a directory's top level marks it as the plugin itself.
bool isPluginRootMarker(const fs::path& path) {
    const std::string name = path.filename().string();
    if (name == "yams-plugin.json" || name == "manifest.json") {
        return true;
    }
    const std::string ext = path.extension().string();
    return ext == ".so" || ext == ".dylib" || ext == ".dll";
}

// The directory whose contents become the installed plugin, ignoring archive junk
// (isArchiveJunk): the extraction root if it holds a manifest or plugin library itself;
// otherwise its only top-level directory if there is exactly one (the "<name>/..." layout,
// possibly with README or LICENSE files beside it); otherwise the root.
Result<fs::path> selectPluginSourceDir(const fs::path& extractDir) {
    std::error_code ec;
    std::vector<fs::path> directories;
    for (auto it = fs::directory_iterator(extractDir, ec); !ec && it != fs::directory_iterator();
         it.increment(ec)) {
        const fs::path& child = it->path();
        if (isArchiveJunk(child)) {
            continue;
        }
        std::error_code typeEc;
        if (fs::symlink_status(child, typeEc).type() == fs::file_type::directory) {
            directories.push_back(child);
        } else if (isPluginRootMarker(child)) {
            return extractDir;
        }
    }
    if (ec) {
        return Error{ErrorCode::IOError, "Failed to list extracted plugin: " + ec.message()};
    }
    if (directories.size() == 1) {
        return directories.front();
    }
    return extractDir;
}

// Archive modes are kept (plugins may ship executables), but the installer must always be able
// to traverse, update and delete what it extracted: a 0555 directory cannot be emptied. Grants
// owner rwx to a directory and owner rw to a regular file; other types are left alone (chmod
// would follow a symlink).
Result<void> grantOwnerAccess(const fs::path& path, const fs::file_status& status) {
    fs::perms wanted = fs::perms::none;
    if (fs::is_directory(status)) {
        wanted = fs::perms::owner_all;
    } else if (fs::is_regular_file(status)) {
        wanted = fs::perms::owner_read | fs::perms::owner_write;
    } else {
        return {};
    }
    if ((status.permissions() & wanted) == wanted) {
        return {};
    }
    std::error_code ec;
    fs::permissions(path, wanted, fs::perm_options::add, ec);
    if (ec) {
        return Error{ErrorCode::IOError, "Failed to make plugin file " + path.filename().string() +
                                             " writable: " + ec.message()};
    }
    return {};
}

// grantOwnerAccess() for `root` and every entry under it, without following links.
Result<void> ensureOwnerAccess(const fs::path& root) {
    std::error_code ec;
    const auto rootStatus = fs::symlink_status(root, ec);
    if (ec) {
        return Error{ErrorCode::IOError,
                     "Failed to inspect plugin directory " + root.string() + ": " + ec.message()};
    }
    if (auto granted = grantOwnerAccess(root, rootStatus); !granted) {
        return granted;
    }
    auto it = fs::recursive_directory_iterator(root, ec);
    for (; !ec && it != fs::recursive_directory_iterator(); it.increment(ec)) {
        std::error_code entryEc;
        const auto status = it->symlink_status(entryEc);
        if (entryEc) {
            return Error{ErrorCode::IOError, "Failed to inspect plugin file " +
                                                 it->path().filename().string() + ": " +
                                                 entryEc.message()};
        }
        if (auto granted = grantOwnerAccess(it->path(), status); !granted) {
            return granted;
        }
    }
    if (ec) {
        return Error{ErrorCode::IOError, "Failed to scan plugin files: " + ec.message()};
    }
    return {};
}

// Prepares the chosen plugin directory for installation in one pass that never follows links
// or throws: grants owner access (directories before they are entered), removes archive junk
// at any depth, checks that every symlink still resolves inside the plugin directory (a link
// that only stayed inside the wider extraction root would point at a sibling of the installed
// plugin after the move) and does not loop, and returns the size of the regular files.
Result<std::uint64_t> prepareExtractedPlugin(const fs::path& pluginRoot) {
    std::error_code ec;
    std::error_code rootEc;
    if (auto granted = grantOwnerAccess(pluginRoot, fs::symlink_status(pluginRoot, rootEc));
        !granted) {
        return granted.error();
    }
    std::uint64_t totalBytes = 0;
    auto it = fs::recursive_directory_iterator(pluginRoot, ec);
    for (; !ec && it != fs::recursive_directory_iterator(); it.increment(ec)) {
        const fs::path path = it->path();
        std::error_code entryEc;
        const auto status = it->symlink_status(entryEc);
        if (entryEc) {
            return Error{ErrorCode::IOError, "Failed to inspect plugin file " +
                                                 path.filename().string() + ": " +
                                                 entryEc.message()};
        }

        if (isArchiveJunk(path)) {
            if (fs::is_directory(status)) {
                it.disable_recursion_pending();
                (void)ensureOwnerAccess(path);
            }
            if (auto removed = removeAllScoped(path, pluginRoot, "archive metadata"); !removed) {
                return removed.error();
            }
            continue;
        }

        if (fs::is_symlink(status)) {
            const fs::path target = fs::read_symlink(path, entryEc);
            if (entryEc || !isSymlinkTargetSafe(pluginRoot, path, target)) {
                return Error{ErrorCode::InvalidPath,
                             "Symlink points outside the plugin directory: " +
                                 path.filename().string()};
            }
            if (auto repaired = repairDirectorySymlink(path, target); !repaired) {
                return repaired.error();
            }
            std::error_code followEc;
            (void)fs::status(path, followEc);
            if (followEc && followEc != std::errc::no_such_file_or_directory) {
                return Error{ErrorCode::InvalidPath, "Symlink cannot be resolved (" +
                                                         followEc.message() +
                                                         "): " + path.filename().string()};
            }
            continue;
        }

        if (auto granted = grantOwnerAccess(path, status); !granted) {
            return granted.error();
        }
        if (fs::is_regular_file(status)) {
            const auto size = fs::file_size(path, entryEc);
            if (entryEc) {
                return Error{ErrorCode::IOError, "Failed to size plugin file " +
                                                     path.filename().string() + ": " +
                                                     entryEc.message()};
            }
            totalBytes += size;
        }
    }
    if (ec) {
        return Error{ErrorCode::IOError, "Failed to scan extracted plugin: " + ec.message()};
    }
    return totalBytes;
}

// Installs stage under <targetDir>/.yams-staging, on the same filesystem as the installed
// plugins, so putting a new tree in place is always a rename. Each install owns one directory
// there named "<plugin>@<unique>"; '@' cannot occur in a plugin name, so the owning plugin of
// any staging directory can be read back from its name. Installs and uninstalls into one
// target directory are serialized by kInstallLockName, so everything under .yams-staging that
// is not the current install's is a leftover.
constexpr std::string_view kStagingDirName = ".yams-staging";
constexpr std::string_view kInstallLockName = ".yams-install.lock";
constexpr std::chrono::seconds kInstallLockTimeout{30};

Result<void> lockTargetDir(yams::common::ExclusiveFileLock& lock, const fs::path& targetDir) {
    auto locked = lock.acquire(targetDir / std::string(kInstallLockName), kInstallLockTimeout);
    if (!locked && locked.error().code == ErrorCode::ResourceExhausted) {
        return Error{ErrorCode::OperationInProgress,
                     "Another plugin install or uninstall is running in " + targetDir.string()};
    }
    return locked;
}

bool isStagingDirOf(std::string_view dirName, const std::string& plugin) {
    return dirName.size() > plugin.size() + 1 && dirName.substr(0, plugin.size()) == plugin &&
           dirName[plugin.size()] == '@';
}

// `dir` must be a real directory, not a symlink or other file, whose canonical path lies
// directly inside `canonicalParent`. Staging through anything else would write, and later
// recursively delete, outside the plugin directory.
Result<void> verifyRealDirectoryIn(const fs::path& dir, const fs::path& canonicalParent) {
    std::error_code ec;
    const auto status = fs::symlink_status(dir, ec);
    if (ec || fs::is_symlink(status) || !fs::is_directory(status)) {
        return Error{ErrorCode::InvalidPath,
                     dir.string() + " is not a real directory (a symlink or another file is in "
                                    "its place); remove it and retry"};
    }
    const fs::path canonical = fs::canonical(dir, ec);
    if (ec) {
        return Error{ErrorCode::IOError, "Failed to resolve " + dir.string() + ": " + ec.message()};
    }
    if (canonical.parent_path() != canonicalParent) {
        return Error{ErrorCode::InvalidPath,
                     dir.string() + " resolves outside " + canonicalParent.string()};
    }
    return {};
}

// Owns the private directory an install downloads and verifies its archive in, outside the
// target directory's install lock, and removes it on every exit path.
struct ScopedDownloadDir {
    fs::path path;

    explicit ScopedDownloadDir(fs::path dir) : path(std::move(dir)) {}
    ScopedDownloadDir(const ScopedDownloadDir&) = delete;
    ScopedDownloadDir& operator=(const ScopedDownloadDir&) = delete;
    ~ScopedDownloadDir() {
        if (path.empty()) {
            return;
        }
        if (auto removed = removeAllScoped(path, path.parent_path(), "plugin download"); !removed) {
            spdlog::warn("Failed to remove plugin download directory '{}': {}", path.string(),
                         removed.error().message);
        }
    }
};

// Owns one install's staging directory and removes it on every exit path (restoring owner
// access first so a read-only directory from the archive cannot block the removal), unless
// `keep` is set because it holds a previous version that could not be restored. The staging
// root is removed too once empty; this runs while the install still holds the target lock.
struct ScopedStagingDir {
    fs::path path;
    fs::path stagingRoot;
    bool keep{false};

    ScopedStagingDir(fs::path dir, fs::path root)
        : path(std::move(dir)), stagingRoot(std::move(root)) {}
    ScopedStagingDir(const ScopedStagingDir&) = delete;
    ScopedStagingDir& operator=(const ScopedStagingDir&) = delete;
    ~ScopedStagingDir() {
        if (keep) {
            return;
        }
        (void)ensureOwnerAccess(path);
        if (auto removed = removeAllScoped(path, stagingRoot, "plugin staging"); !removed) {
            spdlog::warn("Failed to remove plugin staging directory '{}': {}", path.string(),
                         removed.error().message);
        }
        std::error_code ec;
        fs::remove(stagingRoot, ec); // only succeeds when empty
    }
};

// Removes staging directories of `plugin` other than `keepDir`: with the target lock held they
// are leftovers of earlier installs (for example a failed upgrade's preserved previous
// version), which a successful install or an uninstall makes obsolete. Best effort, logged.
void removeLeftoverStaging(const fs::path& stagingRoot, const std::string& plugin,
                           const fs::path& keepDir) {
    std::vector<fs::path> leftovers;
    std::error_code ec;
    for (auto it = fs::directory_iterator(stagingRoot, ec); !ec && it != fs::directory_iterator();
         it.increment(ec)) {
        if (it->path() != keepDir && isStagingDirOf(it->path().filename().string(), plugin)) {
            leftovers.push_back(it->path());
        }
    }
    for (const auto& dir : leftovers) {
        (void)ensureOwnerAccess(dir);
        if (auto removed = removeAllScoped(dir, stagingRoot, "leftover plugin staging"); removed) {
            spdlog::info("Removed leftover plugin staging directory '{}'", dir.string());
        } else {
            spdlog::warn("Could not remove leftover plugin staging directory '{}': {}",
                         dir.string(), removed.error().message);
        }
    }
}

// Version of the plugin installed at `pluginDir`: nullopt when nothing is there, "unknown"
// when it has no readable manifest.
Result<std::optional<std::string>> installedVersionAt(const fs::path& pluginDir) {
    std::error_code existsEc;
    if (!fs::exists(pluginDir, existsEc)) {
        return std::optional<std::string>(std::nullopt);
    }

    // Try external-plugin then package-style manifest names.
    for (const auto& manifestName : {"yams-plugin.json", "manifest.json"}) {
        fs::path manifestPath = pluginDir / manifestName;
        if (!fs::exists(manifestPath)) {
            continue;
        }

        std::ifstream file(manifestPath);
        if (!file) {
            continue;
        }

        try {
            nlohmann::json manifest;
            file >> manifest;
            return std::optional<std::string>(manifest.value("version", "unknown"));
        } catch (const std::exception& e) {
            spdlog::debug("Failed to parse plugin manifest '{}': {}", manifestPath.string(),
                          e.what());
            // Malformed manifests fall through to the conservative "unknown" version.
        }
    }

    return std::optional<std::string>("unknown");
}

// Records in `result` whether `pluginDir` already holds an install, and refuses to reinstall the
// same version without --force.
Result<void> checkInstalled(const fs::path& pluginDir, const RemotePluginInfo& info,
                            const InstallOptions& options, InstallResult& result) {
    result.wasUpgrade = false;
    result.previousVersion.clear();
    auto installedVer = installedVersionAt(pluginDir);
    if (installedVer && installedVer.value()) {
        result.wasUpgrade = true;
        // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
        result.previousVersion = *installedVer.value();
        if (!options.force && result.previousVersion == info.version) {
            return Error{ErrorCode::InvalidOperation,
                         "Plugin " + info.name + "@" + info.version +
                             " is already installed. Use --force to reinstall."};
        }
    }
    return {};
}

} // namespace

class PluginInstallerImpl : public IPluginInstaller {
public:
    PluginInstallerImpl(std::shared_ptr<IPluginRepoClient> repoClient, fs::path installDir,
                        fs::path trustFile)
        : repoClient_(std::move(repoClient)), installDir_(std::move(installDir)),
          trustFile_(std::move(trustFile)) {
        if (installDir_.empty()) {
            installDir_ = getDefaultPluginInstallDir();
        }
        if (trustFile_.empty()) {
            trustFile_ = getDefaultPluginTrustFile();
        }
    }

    Result<InstallResult> install(const std::string& nameOrUrl,
                                  const InstallOptions& options) override {
        auto startTime = std::chrono::steady_clock::now();
        InstallResult result;

        if (options.autoLoad && !options.dryRun && !options.loadAfterInstall) {
            return Error{ErrorCode::InvalidArgument,
                         "autoLoad requires a loadAfterInstall callback"};
        }

        auto notifyProgress = [&](InstallProgress::Stage stage, const std::string& msg,
                                  float pct = 0.0f) {
            if (options.onProgress) {
                InstallProgress progress;
                progress.stage = stage;
                progress.message = msg;
                progress.progress = pct;
                options.onProgress(progress);
            }
        };

        // Parse name[@version]
        std::string pluginName = nameOrUrl;
        std::optional<std::string> version = options.version;
        bool isUrl = nameOrUrl.find("://") != std::string::npos;

        if (!isUrl) {
            // Check for name@version format
            auto atPos = nameOrUrl.find('@');
            if (atPos != std::string::npos) {
                pluginName = nameOrUrl.substr(0, atPos);
                version = nameOrUrl.substr(atPos + 1);
            }
        }

        notifyProgress(InstallProgress::Stage::Querying, "Fetching plugin metadata...");

        RemotePluginInfo info;
        if (isUrl) {
            // Direct URL install - minimal metadata
            info.downloadUrl = nameOrUrl;
            // Try to extract name from URL pattern first, fall back to stem-based extraction:
            // /plugins/<name>/<version>/<name>-<version>.tar.gz
            std::regex urlPattern(R"(/plugins/([^/]+)/([^/]+)/[^/]+\.tar\.gz$)");
            std::smatch match;
            if (std::regex_search(nameOrUrl, match, urlPattern)) {
                info.name = match[1].str();
                info.version = match[2].str();
            } else {
                info.name = fs::path(nameOrUrl).stem().string();
                info.version = version.value_or("unknown");
            }
        } else {
            auto infoRes = repoClient_->get(pluginName, version);
            if (!infoRes) {
                return infoRes.error();
            }
            info = infoRes.value();
        }

        if (auto nameValidation = validatePluginName(info.name); !nameValidation) {
            return nameValidation.error();
        }
        if (auto versionValidation = validatePluginVersion(info.version); !versionValidation) {
            return versionValidation.error();
        }

        result.pluginName = info.name;
        result.version = info.version;

        // Determine install directory
        fs::path targetDir = options.installDir.empty() ? installDir_ : options.installDir;
        fs::path pluginDir = targetDir / info.name;

        // Check if already installed (before the checksum checks, so a reinstall without
        // --force reports the real reason). Look in the effective target directory, which is the
        // one the replace logic below acts on.
        // This early check avoids a pointless download; it is repeated under the install lock.
        if (auto installed = checkInstalled(pluginDir, info, options, result); !installed) {
            return installed.error();
        }

        // Every installed archive is verified before it is extracted or trusted. With no
        // checksum from the caller or the repository there is nothing to verify against, so
        // refuse rather than install (and auto-trust) an unverified download. A dry run
        // downloads and trusts nothing, so it only reports that a checksum will be required.
        std::optional<std::string> callerChecksum;
        if (options.checksum && !options.checksum->empty()) {
            auto normalized = normalizeSha256(*options.checksum);
            if (!normalized) {
                return normalized.error(); // InvalidArgument: the caller's value is malformed
            }
            callerChecksum = normalized.value();
        }
        std::optional<std::string> repoChecksum;
        if (!info.checksum.empty()) {
            auto normalized = normalizeSha256(info.checksum);
            if (normalized) {
                repoChecksum = normalized.value();
            } else if (callerChecksum) {
                spdlog::warn("The plugin repository published a malformed checksum for {}@{} "
                             "('{}'); verifying against --checksum only, without a "
                             "cross-check.",
                             info.name, info.version, info.checksum);
            } else {
                return Error{ErrorCode::InvalidData,
                             "The plugin repository published a malformed checksum for " +
                                 info.name + "@" + info.version + ": " + info.checksum};
            }
        }
        if (callerChecksum && repoChecksum && *callerChecksum != *repoChecksum) {
            return Error{ErrorCode::InvalidData,
                         "--checksum " + *callerChecksum +
                             " does not match the checksum the plugin repository published for " +
                             info.name + "@" + info.version + " (" + *repoChecksum + ")"};
        }

        std::string expectedChecksum = callerChecksum.value_or(repoChecksum.value_or(""));
        if (expectedChecksum.empty()) {
            if (!options.dryRun) {
                const std::string reason =
                    isUrl
                        ? std::string("direct URL installs do not carry one")
                        : std::string("the plugin repository did not publish one for this release");
                return Error{ErrorCode::InvalidData,
                             "Refusing to install " + info.name + "@" + info.version +
                                 " without a SHA-256 checksum (" + reason +
                                 "). Supply the expected digest with --checksum sha256:<hex> "
                                 "(accepted by `yams plugin install` and "
                                 "`yams plugin update <name>`)."};
            }
            result.checksumRequired = true;
        }

        if (options.dryRun) {
            result.installedPath = pluginDir;
            result.checksum = expectedChecksum;
            result.sizeBytes = info.sizeBytes;
            spdlog::info("[dry-run] Would install {} {} to {}", info.name, info.version,
                         pluginDir.string());
            return result;
        }

        // Download and verify outside the target directory's install lock, so a slow download
        // never blocks other installs. The archive goes to a fresh private directory.
        const ScopedDownloadDir download{
            yams::common::createTempDirectory("yams-plugin-download-")};
        if (download.path.empty()) {
            return Error{ErrorCode::IOError, "Failed to create a plugin download directory"};
        }
        std::error_code ec;
        fs::permissions(download.path, fs::perms::owner_all, fs::perm_options::replace, ec);
        if (ec) {
            return Error{ErrorCode::IOError,
                         "Failed to restrict the plugin download directory: " + ec.message()};
        }
        const fs::path archivePath = download.path / "archive.tar.gz";

        notifyProgress(InstallProgress::Stage::Downloading, "Downloading " + info.name + "...");
        auto downloadRes = downloadFile(info.downloadUrl, archivePath, options);
        if (!downloadRes) {
            return downloadRes.error();
        }

        notifyProgress(InstallProgress::Stage::Verifying, "Verifying integrity...", 0.0f);
        const std::string actualChecksum = computeSha256(archivePath);
        if (actualChecksum.empty()) {
            return Error{ErrorCode::IOError,
                         "Failed to compute checksum of downloaded plugin archive"};
        }
        if (actualChecksum != expectedChecksum) {
            return Error{ErrorCode::HashMismatch, "Checksum mismatch: expected " +
                                                      expectedChecksum + ", got " + actualChecksum};
        }
        result.checksum = actualChecksum;

        // Everything that reads or changes the target directory runs under its install lock.
        // The staging guard is declared after the lock in this scope, so its cleanup (including
        // removing the empty staging root) finishes before the lock is released.
        if (!yams::common::ensureDirectories(targetDir)) {
            return Error{ErrorCode::IOError, "Failed to create plugin directory"};
        }
        {
            yams::common::ExclusiveFileLock targetLock;
            if (auto locked = lockTargetDir(targetLock, targetDir); !locked) {
                return locked.error();
            }

            // Re-check now that no other install can change the target directory: one that
            // finished while this one was downloading may have installed this version already.
            if (auto installed = checkInstalled(pluginDir, info, options, result); !installed) {
                return installed.error();
            }

            // The target directory itself may be a symlink (a relocated plugin directory); that
            // is resolved once here and only used to check that staging stays inside it.
            const fs::path canonicalTarget = fs::canonical(targetDir, ec);
            if (ec) {
                return Error{ErrorCode::IOError, "Failed to resolve plugin directory " +
                                                     targetDir.string() + ": " + ec.message()};
            }
            const fs::path stagingRoot = targetDir / std::string(kStagingDirName);
            fs::create_directories(stagingRoot, ec);
            if (ec) {
                return Error{ErrorCode::IOError,
                             "Failed to create plugin staging directory: " + ec.message()};
            }
            // create_directories accepts an existing symlink to a directory; staging through
            // one would extract (and later clean up) outside the plugin directory.
            if (auto real = verifyRealDirectoryIn(stagingRoot, canonicalTarget); !real) {
                return real.error();
            }
            const fs::path stagingDir =
                yams::common::createTempDirectory(info.name + "@", stagingRoot);
            if (stagingDir.empty()) {
                return Error{ErrorCode::IOError,
                             "Failed to create a plugin staging directory under " +
                                 stagingRoot.string()};
            }
            ScopedStagingDir staging{stagingDir, stagingRoot};
            if (auto real = verifyRealDirectoryIn(stagingDir, canonicalTarget / kStagingDirName);
                !real) {
                staging.keep = true; // never clean up through an unexpected path
                return real.error();
            }

            notifyProgress(InstallProgress::Stage::Extracting, "Extracting archive...");
            const fs::path extractDir = staging.path / "extracted";
            auto extractRes = extractArchive(archivePath, extractDir);
            if (!extractRes) {
                return extractRes.error();
            }

            // Plugin content is either the archive root or its single top-level directory.
            // Validate it before touching any existing installation.
            auto sourceDirRes = selectPluginSourceDir(extractDir);
            if (!sourceDirRes) {
                return sourceDirRes.error();
            }
            const fs::path sourceDir = sourceDirRes.value();
            auto scanRes = prepareExtractedPlugin(sourceDir);
            if (!scanRes) {
                return scanRes.error();
            }

            notifyProgress(InstallProgress::Stage::Installing, "Installing plugin...");

            // Upgrade: move the existing installation into this install's staging directory,
            // then the new tree into place; the staging cleanup deletes the old one. Both are
            // renames on one filesystem. If the second fails, the previous version is moved
            // back.
            fs::path previousDir;
            if (fs::symlink_status(pluginDir, ec).type() != fs::file_type::not_found && !ec) {
                previousDir = staging.path / "previous";
                fs::rename(pluginDir, previousDir, ec);
                if (ec) {
                    return Error{ErrorCode::IOError,
                                 "Failed to set aside the existing plugin: " + ec.message()};
                }
            }
            ec.clear();

            fs::rename(sourceDir, pluginDir, ec);
            if (ec) {
                std::string message = "Failed to install plugin: " + ec.message();
                if (!previousDir.empty()) {
                    std::error_code restoreEc;
                    fs::rename(previousDir, pluginDir, restoreEc);
                    if (restoreEc) {
                        staging.keep = true;
                        spdlog::error("Failed to restore previous plugin '{}' from '{}': {}",
                                      pluginDir.string(), previousDir.string(),
                                      restoreEc.message());
                        message += ". Restoring the previous version also failed (" +
                                   restoreEc.message() + "); it is preserved at " +
                                   previousDir.string() + " and can be moved back to " +
                                   pluginDir.string();
                    }
                }
                return Error{ErrorCode::IOError, message};
            }
            removeLeftoverStaging(stagingRoot, info.name, staging.path);

            result.installedPath = pluginDir;
            result.sizeBytes = scanRes.value();
        } // staging cleaned up, then the install lock released

        // Add to trust list
        if (options.autoTrust) {
            notifyProgress(InstallProgress::Stage::Trusting, "Adding to trust list...");
            addToTrustList(pluginDir);
        }

        // Optionally ask the caller to load the plugin into a running daemon.
        if (options.autoLoad && options.loadAfterInstall) {
            result.autoLoadAttempted = true;
            notifyProgress(InstallProgress::Stage::Loading, "Loading plugin...");
            auto loadResult = options.loadAfterInstall(result);
            if (loadResult) {
                result.autoLoadSucceeded = true;
                result.autoLoadMessage = "Loaded into daemon";
            } else {
                result.autoLoadMessage = loadResult.error().message;
                spdlog::warn("Plugin installed but auto-load failed for {}: {}", result.pluginName,
                             result.autoLoadMessage);
            }
        }

        notifyProgress(InstallProgress::Stage::Complete, "Installation complete!");

        auto endTime = std::chrono::steady_clock::now();
        result.elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

        return result;
    }

    Result<void> uninstall(const std::string& name, bool removeFromTrust) override {
        if (auto nameValidation = validatePluginName(name); !nameValidation) {
            return nameValidation.error();
        }
        fs::path pluginDir = installDir_ / name;

        std::error_code existsEc;
        if (!fs::is_directory(installDir_, existsEc)) {
            return Error{ErrorCode::NotFound, "Plugin not installed: " + name};
        }

        yams::common::ExclusiveFileLock targetLock;
        if (auto locked = lockTargetDir(targetLock, installDir_); !locked) {
            return locked.error();
        }
        // Checked under the lock: an upgrade in flight may have moved the plugin into staging.
        if (!fs::exists(pluginDir, existsEc)) {
            return Error{ErrorCode::NotFound, "Plugin not installed: " + name};
        }

        if (removeFromTrust) {
            removeFromTrustList(pluginDir);
        }

        // Installs from older builds may contain read-only directories from the archive.
        (void)ensureOwnerAccess(pluginDir);
        auto removeRes = removeAllScoped(pluginDir, installDir_, "plugin install");
        if (!removeRes) {
            return removeRes.error();
        }

        // A failed upgrade may have preserved a previous version under .yams-staging.
        // Only a real staging directory inside the plugin directory is cleaned up.
        const fs::path stagingRoot = installDir_ / std::string(kStagingDirName);
        std::error_code ec;
        const fs::path canonicalInstallDir = fs::canonical(installDir_, ec);
        if (!ec && fs::symlink_status(stagingRoot, ec).type() != fs::file_type::not_found) {
            if (auto real = verifyRealDirectoryIn(stagingRoot, canonicalInstallDir); real) {
                removeLeftoverStaging(stagingRoot, name, {});
                fs::remove(stagingRoot, ec); // only succeeds when empty
            } else {
                spdlog::warn("Not cleaning plugin staging: {}", real.error().message);
            }
        }

        return {};
    }

    Result<std::vector<std::string>> listInstalled() override {
        std::vector<std::string> installed;

        if (!fs::exists(installDir_)) {
            return installed;
        }

        std::error_code ec;
        fs::directory_iterator it(installDir_, ec);
        if (ec) {
            return Error{ErrorCode::IOError, "Cannot list plugin directory " +
                                                 installDir_.string() + ": " + ec.message()};
        }
        for (const fs::directory_iterator end; it != end; it.increment(ec)) {
            auto name = it->path().filename().string();
            if (name.empty() || isReservedPluginDirName(name)) {
                continue;
            }
            std::error_code typeEc;
            if (it->is_directory(typeEc)) {
                installed.push_back(std::move(name));
            }
        }
        if (ec) { // a failed increment ends the loop with `it` at end
            return Error{ErrorCode::IOError, "Cannot list plugin directory " +
                                                 installDir_.string() + ": " + ec.message()};
        }

        return installed;
    }

    Result<std::optional<std::string>> installedVersion(const std::string& name) override {
        if (auto nameValidation = validatePluginName(name); !nameValidation) {
            return nameValidation.error();
        }
        return installedVersionAt(installDir_ / name);
    }

    Result<std::map<std::string, std::string>> checkUpdates(const std::string& name) override {
        std::map<std::string, std::string> updates;

        auto installed = listInstalled();
        if (!installed) {
            return installed.error();
        }

        for (const auto& pluginName : installed.value()) {
            if (!name.empty() && pluginName != name) {
                continue;
            }

            auto currentVer = installedVersion(pluginName);
            if (!currentVer || !currentVer.value()) {
                continue;
            }

            auto remoteInfo = repoClient_->get(pluginName);
            if (!remoteInfo) {
                continue; // Plugin not in repository
            }

            // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
            if (remoteInfo.value().version != *currentVer.value()) {
                updates[pluginName] = remoteInfo.value().version;
            }
        }

        return updates;
    }

private:
    Result<void> downloadFile(const std::string& url, const fs::path& destPath,
                              const InstallOptions& options) {
        CURL* curl = curl_easy_init();
        if (!curl) {
            return Error{ErrorCode::NetworkError, "Failed to initialize cURL"};
        }

        std::ofstream file(destPath, std::ios::binary);
        if (!file) {
            curl_easy_cleanup(curl);
            return Error{ErrorCode::IOError, "Failed to create file: " + destPath.string()};
        }

        ProgressData progressData;
        progressData.callback = options.onProgress;
        progressData.progress.stage = InstallProgress::Stage::Downloading;
        progressData.progress.message = "Downloading...";

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curlWriteToFile);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &file);
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
        // An HTTP error status is a failed download, not an archive to checksum.
        curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
        curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
        curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, curlProgressCallback);
        curl_easy_setopt(curl, CURLOPT_XFERINFODATA, &progressData);

        CURLcode res = curl_easy_perform(curl);
        long httpStatus = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpStatus);
        file.close();
        curl_easy_cleanup(curl);

        if (res != CURLE_OK) {
            std::error_code removeEc;
            fs::remove(destPath, removeEc);
            if (res == CURLE_HTTP_RETURNED_ERROR) {
                return Error{httpStatus == 404 ? ErrorCode::NotFound : ErrorCode::NetworkError,
                             "Download failed: HTTP " + std::to_string(httpStatus) + " for " + url};
            }
            return Error{ErrorCode::NetworkError,
                         std::string("Download failed: ") + curl_easy_strerror(res)};
        }

        return {};
    }

    void addToTrustList(const fs::path& pluginPath) {
        auto loadResult = yams::daemon::plugin_trust::loadTrustStore(
            trustFile_, yams::config::get_daemon_plugin_trust_file(),
            yams::config::get_legacy_plugin_trust_file());
        auto trusted = std::move(loadResult.entries);
        trusted.insert(yams::daemon::plugin_trust::normalizePath(pluginPath));

        if (!yams::daemon::plugin_trust::writeTrustStore(trustFile_, trusted)) {
            spdlog::warn("Failed to persist plugin trust list '{}'.", trustFile_.string());
        }
    }

    void removeFromTrustList(const fs::path& pluginPath) {
        auto loadResult = yams::daemon::plugin_trust::loadTrustStore(
            trustFile_, yams::config::get_daemon_plugin_trust_file(),
            yams::config::get_legacy_plugin_trust_file());
        if (!loadResult.loaded && !fs::exists(trustFile_)) {
            return;
        }

        auto trusted = std::move(loadResult.entries);
        trusted.erase(yams::daemon::plugin_trust::normalizePath(pluginPath));

        if (!yams::daemon::plugin_trust::writeTrustStore(trustFile_, trusted)) {
            spdlog::warn("Failed to update plugin trust list '{}' during removal.",
                         trustFile_.string());
        }
    }

    std::shared_ptr<IPluginRepoClient> repoClient_;
    fs::path installDir_;
    fs::path trustFile_;
};

std::unique_ptr<IPluginInstaller> makePluginInstaller(std::shared_ptr<IPluginRepoClient> repoClient,
                                                      const fs::path& installDir,
                                                      const fs::path& trustFile) {
    return std::make_unique<PluginInstallerImpl>(std::move(repoClient), installDir, trustFile);
}

fs::path getDefaultPluginInstallDir() {
    // Check environment variable first
    if (auto envDir = yams::config::getenv_optional("YAMS_PLUGIN_DIR")) {
        return fs::path(*envDir);
    }

#ifdef _WIN32
    // Windows: %LOCALAPPDATA%/yams/plugins
    wchar_t* localAppData = nullptr;
    if (SUCCEEDED(SHGetKnownFolderPath(FOLDERID_LocalAppData, 0, nullptr, &localAppData))) {
        fs::path result = fs::path(localAppData) / "yams" / "plugins";
        CoTaskMemFree(localAppData);
        return result;
    }
    if (auto localAppData = yams::config::getenv_optional("LOCALAPPDATA")) {
        return fs::path(*localAppData) / "yams" / "plugins";
    }
    return fs::path(".") / "yams" / "plugins";
#else
    // Unix: ~/.local/lib/yams/plugins
    fs::path homeDir;
    if (auto home = yams::config::getenv_optional("HOME")) {
        homeDir = *home;
    } else {
        struct passwd pwd;
        struct passwd* pw = nullptr;
        std::array<char, 16384> buffer{};
        if (getpwuid_r(getuid(), &pwd, buffer.data(), buffer.size(), &pw) == 0 && pw != nullptr &&
            pw->pw_dir != nullptr) {
            homeDir = pw->pw_dir;
        } else {
            homeDir = "/tmp";
        }
    }
    return homeDir / ".local" / "lib" / "yams" / "plugins";
#endif
}

fs::path getDefaultPluginTrustFile() {
    return yams::config::get_daemon_plugin_trust_file();
}

} // namespace yams::plugins
