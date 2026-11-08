/**
 * @file plugin_installer.cpp
 * @brief Implementation of the plugin installer
 */

#include <yams/common/fs_utils.h>
#include <yams/common/hash_predicates.h>
#include <yams/config/config_helpers.h>
#include <yams/crypto/hasher.h>
#include <yams/daemon/resource/plugin_trust.h>
#include <yams/plugins/plugin_installer.hpp>

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
    if (!isSafePathSegment(name)) {
        return Error{ErrorCode::InvalidArgument,
                     "Invalid plugin name. Use only letters, numbers, '.', '_' and '-'."};
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

Result<fs::path> makePluginTempDir(const std::string& name, const std::string& version) {
    auto tempDir = yams::common::createTempDirectory("yams-plugin-" + name + "-" + version + "-");
    if (tempDir.empty()) {
        return Error{ErrorCode::IOError, "Failed to create plugin temp directory"};
    }
    return tempDir;
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
// step back out of a symlinked directory, so it is rejected. A link that names itself is
// rejected too; longer cycles are caught by scanExtractedPlugin().
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
    return resolved != *linkParts;
}

// Walks the chosen plugin directory without following links and without throwing. Every
// symlink must still resolve inside it (a link that only stayed inside the wider extraction
// root would point at a sibling of the installed plugin after the move) and must not loop.
// Returns the total size of regular files; links are not counted.
Result<std::uint64_t> scanExtractedPlugin(const fs::path& pluginRoot) {
    std::error_code ec;
    std::uint64_t totalBytes = 0;
    auto it = fs::recursive_directory_iterator(pluginRoot, ec);
    for (; !ec && it != fs::recursive_directory_iterator(); it.increment(ec)) {
        const fs::path& path = it->path();
        const auto status = it->symlink_status(ec);
        if (ec) {
            break;
        }
        if (fs::is_symlink(status)) {
            const fs::path target = fs::read_symlink(path, ec);
            if (ec || !isSymlinkTargetSafe(pluginRoot, path, target)) {
                return Error{ErrorCode::InvalidPath,
                             "Symlink points outside the plugin directory: " +
                                 path.filename().string()};
            }
            std::error_code followEc;
            (void)fs::status(path, followEc);
            if (followEc && followEc != std::errc::no_such_file_or_directory) {
                return Error{ErrorCode::InvalidPath, "Symlink cannot be resolved (" +
                                                         followEc.message() +
                                                         "): " + path.filename().string()};
            }
        } else if (fs::is_regular_file(status)) {
            const auto size = fs::file_size(path, ec);
            if (ec) {
                break;
            }
            totalBytes += size;
        }
    }
    if (ec) {
        return Error{ErrorCode::IOError, "Failed to scan extracted plugin: " + ec.message()};
    }
    return totalBytes;
}

// Top-level archive metadata that is never plugin content: macOS resource forks and Finder
// files that archivers on macOS add next to the real entries.
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
    archive_write_disk_set_options(ext, ARCHIVE_EXTRACT_TIME | ARCHIVE_EXTRACT_PERM |
                                            ARCHIVE_EXTRACT_ACL | ARCHIVE_EXTRACT_FFLAGS |
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

// The directory whose contents become the installed plugin: the extraction root, unless it
// holds exactly one entry and that entry is a real directory (the "<name>/..." layout).
// Top-level archive junk (isArchiveJunk) is removed first so it neither changes that decision
// nor gets installed.
Result<fs::path> selectPluginSourceDir(const fs::path& extractDir) {
    std::error_code ec;
    std::vector<fs::path> children;
    for (auto it = fs::directory_iterator(extractDir, ec); !ec && it != fs::directory_iterator();
         it.increment(ec)) {
        children.push_back(it->path());
    }
    if (ec) {
        return Error{ErrorCode::IOError, "Failed to list extracted plugin: " + ec.message()};
    }

    std::vector<fs::path> content;
    for (const auto& child : children) {
        if (!isArchiveJunk(child)) {
            content.push_back(child);
            continue;
        }
        if (auto removed = removeAllScoped(child, extractDir, "archive metadata"); !removed) {
            return removed.error();
        }
    }

    if (content.size() == 1 &&
        fs::symlink_status(content.front(), ec).type() == fs::file_type::directory) {
        return content.front();
    }
    return extractDir;
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
        // --force reports the real reason).
        auto installedVer = installedVersion(info.name);
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
            if (!normalized) {
                return Error{ErrorCode::InvalidData,
                             "The plugin repository published a malformed checksum for " +
                                 info.name + "@" + info.version + ": " + info.checksum};
            }
            repoChecksum = normalized.value();
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

        // Create a unique scoped temp directory for download/extract.
        auto tempDirRes = makePluginTempDir(info.name, info.version);
        if (!tempDirRes) {
            return tempDirRes.error();
        }
        fs::path tempDir = tempDirRes.value();
        const fs::path tempRoot = fs::temp_directory_path();
        std::error_code ec;
        fs::path archivePath = tempDir / (info.name + "-" + info.version + ".tar.gz");

        // Download
        notifyProgress(InstallProgress::Stage::Downloading, "Downloading " + info.name + "...");
        auto downloadRes = downloadFile(info.downloadUrl, archivePath, options);
        if (!downloadRes) {
            (void)removeAllScoped(tempDir, tempRoot, "plugin temp");
            return downloadRes.error();
        }

        // Verify checksum
        notifyProgress(InstallProgress::Stage::Verifying, "Verifying integrity...", 0.0f);
        const std::string actualChecksum = computeSha256(archivePath);
        if (actualChecksum.empty()) {
            (void)removeAllScoped(tempDir, tempRoot, "plugin temp");
            return Error{ErrorCode::IOError,
                         "Failed to compute checksum of downloaded plugin archive"};
        }
        if (actualChecksum != expectedChecksum) {
            (void)removeAllScoped(tempDir, tempRoot, "plugin temp");
            return Error{ErrorCode::HashMismatch, "Checksum mismatch: expected " +
                                                      expectedChecksum + ", got " + actualChecksum};
        }
        result.checksum = actualChecksum;

        // Extract
        notifyProgress(InstallProgress::Stage::Extracting, "Extracting archive...");
        fs::path extractDir = tempDir / "extracted";
        auto extractRes = extractArchive(archivePath, extractDir);
        if (!extractRes) {
            (void)removeAllScoped(tempDir, tempRoot, "plugin temp");
            return extractRes.error();
        }

        // Plugin content is either the archive root or its single top-level directory. Validate
        // it before touching any existing installation.
        auto sourceDirRes = selectPluginSourceDir(extractDir);
        if (!sourceDirRes) {
            (void)removeAllScoped(tempDir, tempRoot, "plugin temp");
            return sourceDirRes.error();
        }
        const fs::path sourceDir = sourceDirRes.value();
        auto scanRes = scanExtractedPlugin(sourceDir);
        if (!scanRes) {
            (void)removeAllScoped(tempDir, tempRoot, "plugin temp");
            return scanRes.error();
        }

        // Install (move to plugin directory)
        notifyProgress(InstallProgress::Stage::Installing, "Installing plugin...");

        if (!yams::common::ensureDirectories(targetDir)) {
            (void)removeAllScoped(tempDir, tempRoot, "plugin temp");
            return Error{ErrorCode::IOError, "Failed to create plugin directory"};
        }

        // Stage an upgrade: set the existing installation aside, put the new one in place, and
        // only then delete the old one, so a failed move or copy restores the previous plugin.
        fs::path backupDir;
        if (fs::symlink_status(pluginDir, ec).type() != fs::file_type::not_found && !ec) {
            backupDir =
                targetDir /
                ("." + info.name + ".yams-backup-" +
                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
            fs::rename(pluginDir, backupDir, ec);
            if (ec) {
                (void)removeAllScoped(tempDir, tempRoot, "plugin temp");
                return Error{ErrorCode::IOError,
                             "Failed to set aside the existing plugin: " + ec.message()};
            }
        }
        ec.clear();

        fs::rename(sourceDir, pluginDir, ec);
        if (ec) {
            // If rename fails (cross-device), fall back to copy. Symlinks were validated to stay
            // inside the plugin, so copy them as links rather than following them.
            ec.clear();
            fs::copy(sourceDir, pluginDir,
                     fs::copy_options::recursive | fs::copy_options::copy_symlinks, ec);
        }
        if (ec) {
            const std::string message = "Failed to install plugin: " + ec.message();
            std::error_code cleanupEc;
            if (fs::symlink_status(pluginDir, cleanupEc).type() != fs::file_type::not_found) {
                (void)removeAllScoped(pluginDir, targetDir, "partial plugin install");
            }
            if (!backupDir.empty()) {
                fs::rename(backupDir, pluginDir, cleanupEc);
                if (cleanupEc) {
                    spdlog::error("Failed to restore previous plugin '{}' from '{}': {}",
                                  pluginDir.string(), backupDir.string(), cleanupEc.message());
                }
            }
            (void)removeAllScoped(tempDir, tempRoot, "plugin temp");
            return Error{ErrorCode::IOError, message};
        }
        if (!backupDir.empty()) {
            if (auto removed = removeAllScoped(backupDir, targetDir, "previous plugin"); !removed) {
                spdlog::warn("Installed {} but could not remove the previous copy at '{}': {}",
                             info.name, backupDir.string(), removed.error().message);
            }
        }

        result.installedPath = pluginDir;
        result.sizeBytes = scanRes.value();

        // Cleanup temp directory
        (void)removeAllScoped(tempDir, tempRoot, "plugin temp");

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

        if (!fs::exists(pluginDir)) {
            return Error{ErrorCode::NotFound, "Plugin not installed: " + name};
        }

        if (removeFromTrust) {
            removeFromTrustList(pluginDir);
        }

        auto removeRes = removeAllScoped(pluginDir, installDir_, "plugin install");
        if (!removeRes) {
            return removeRes.error();
        }

        return {};
    }

    Result<std::vector<std::string>> listInstalled() override {
        std::vector<std::string> installed;

        if (!fs::exists(installDir_)) {
            return installed;
        }

        for (const auto& entry : fs::directory_iterator(installDir_)) {
            if (entry.is_directory()) {
                installed.push_back(entry.path().filename().string());
            }
        }

        return installed;
    }

    Result<std::optional<std::string>> installedVersion(const std::string& name) override {
        if (auto nameValidation = validatePluginName(name); !nameValidation) {
            return nameValidation.error();
        }
        fs::path pluginDir = installDir_ / name;

        if (!fs::exists(pluginDir)) {
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
