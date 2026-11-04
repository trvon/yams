#include "windows_path.h" // IWYU pragma: keep -- used by _WIN32 path handling

#include <yams/common/fs_utils.h>
#include <yams/crypto/hasher.h>
#include <yams/profiling.h>
#include <yams/storage/object_storage_plugin_loader.h>
#include <yams/storage/storage_backend.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <fstream>
#include <functional>
#include <limits>
#include <optional>
#include <regex>
#include <system_error>
#include <unordered_map>
#include <utility>

namespace yams::storage {

namespace {

#ifdef YAMS_TESTING
thread_local FilesystemListingStats* listingStats = nullptr;
thread_local std::function<void(std::string_view)>* readObserver = nullptr;

FilesystemListingStats* listingStatsFor(std::string_view prefix) {
    return listingStats && (!listingStats->prefix || *listingStats->prefix == prefix) ? listingStats
                                                                                      : nullptr;
}
#endif

#ifdef _WIN32
// Add the extended-length prefix only when the path actually needs it. Keeping short paths in the
// ordinary Win32 spelling preserves every existing path semantic (canonicalisation, iteration,
// relative-path derivation) that the backend relies on.
std::filesystem::path longPath(const std::filesystem::path& path) {
    if (path.empty() || path.native().size() < detail::kWindowsMaxPath) {
        return path;
    }
    auto nativeForm = path;
    nativeForm.make_preferred();
    auto extended = detail::extendedWindowsPath(nativeForm.native());
    if (!extended) {
        return path;
    }
    return std::filesystem::path(std::move(extended).value());
}

// Inverse of longPath, for comparisons and logical key derivation.
std::filesystem::path plainPath(const std::filesystem::path& path) {
    if (path.empty() || !detail::isExtendedWindowsPath(path.native())) {
        return path;
    }
    return std::filesystem::path(detail::stripExtendedWindowsPath(path.native()));
}

// Enumeration always runs in the extended namespace on Windows, even when the directory being
// opened is short: the iterator appends descendant names itself, so a short root can still build
// child paths past MAX_PATH and fail partway through the walk. Callers must derive keys through
// plainPath().
std::filesystem::path traversalPath(const std::filesystem::path& path) {
    if (path.empty()) {
        return path;
    }
    auto nativeForm = path;
    nativeForm.make_preferred();
    auto extended = detail::extendedWindowsPath(nativeForm.native());
    if (!extended) {
        return path;
    }
    return std::filesystem::path(std::move(extended).value());
}
#else
std::filesystem::path longPath(const std::filesystem::path& path) {
    return path;
}
std::filesystem::path plainPath(const std::filesystem::path& path) {
    return path;
}
std::filesystem::path traversalPath(const std::filesystem::path& path) {
    return path;
}
#endif

void observePeakRetainedKeys(std::string_view prefix, std::size_t retainedKeys) {
#ifdef YAMS_TESTING
    if (auto* observed = listingStatsFor(prefix)) {
        observed->peakRetainedKeys = std::max(observed->peakRetainedKeys, retainedKeys);
    }
#else
    (void)prefix;
    (void)retainedKeys;
#endif
}

} // namespace

#ifdef YAMS_TESTING
FilesystemListingStats* testing_setFilesystemListingStats(FilesystemListingStats* stats) noexcept {
    return std::exchange(listingStats, stats);
}

std::function<void(std::string_view)>*
testing_setFilesystemReadObserver(std::function<void(std::string_view)>* observer) noexcept {
    return std::exchange(readObserver, observer);
}
#endif

Result<void> IStorageBackend::clear() {
    auto keys = list("");
    if (!keys) {
        return keys.error();
    }
    for (const auto& key : keys.value()) {
        if (auto removed = remove(key); !removed) {
            return removed.error();
        }
    }
    return {};
}

Result<ObjectListPage> IStorageBackend::listPage(std::string_view prefix,
                                                 std::optional<std::string_view> cursor,
                                                 std::size_t limit) const {
    if (limit == 0) {
        return Error{ErrorCode::InvalidArgument, "object-list page limit must be positive"};
    }
    auto listed = list(prefix);
    if (!listed) {
        return listed.error();
    }
    auto keys = std::move(listed.value());
    std::ranges::sort(keys);
    auto begin = cursor ? std::ranges::upper_bound(keys, *cursor) : keys.begin();
    const auto count = std::min(limit, static_cast<std::size_t>(std::distance(begin, keys.end())));
    ObjectListPage page;
    page.keys.assign(begin, std::next(begin, static_cast<std::ptrdiff_t>(count)));
    if (count != static_cast<std::size_t>(std::distance(begin, keys.end()))) {
        page.nextCursor = page.keys.back();
    }
    return page;
}

// Registry for custom backends
static std::unordered_map<std::string, std::function<std::unique_ptr<IStorageBackend>()>>&
getBackendRegistry() {
    static std::unordered_map<std::string, std::function<std::unique_ptr<IStorageBackend>()>>
        registry;
    return registry;
}

std::unique_ptr<IStorageBackend> StorageBackendFactory::create(const BackendConfig& config) {
    std::string type = config.type;

    // Convert to lowercase for case-insensitive comparison
    std::transform(type.begin(), type.end(), type.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    // Check for custom registered backends first
    auto& registry = getBackendRegistry();
    auto it = registry.find(type);
    if (it != registry.end()) {
        auto backend = it->second();
        if (auto result = backend->initialize(config); !result) {
            spdlog::error("Failed to initialize {} backend: {}", type, result.error().message);
            return nullptr;
        }
        return backend;
    }

    // Plugin-prefixed type: plugin:<name>
    if (type.rfind("plugin:", 0) == 0) {
        std::string name = type.substr(7);
        if (auto pluginBackend = tryCreatePluginBackendByName(name, config)) {
            return pluginBackend;
        }
        spdlog::error("Requested plugin '{}' not found or failed to initialize", name);
        return nullptr;
    }

    // Built-in backends
    if (type == "filesystem" || type == "local" || type == "file") {
        auto backend = std::make_unique<FilesystemBackend>();
        if (auto result = backend->initialize(config); !result) {
            spdlog::error("Failed to initialize filesystem backend: {}", result.error().message);
            return nullptr;
        }
        return backend;
    }

    if (type == "s3") {
        if (auto pluginBackend = tryCreateS3PluginBackend(config)) {
            return pluginBackend;
        }
        spdlog::error("S3 backend plugin not found or failed to initialize");
        return nullptr;
    }

    if (type == "http" || type == "https" || type == "ftp") {
        auto backend = std::make_unique<URLBackend>();
        if (auto result = backend->initialize(config); !result) {
            spdlog::error("Failed to initialize URL backend: {}", result.error().message);
            return nullptr;
        }
        return backend;
    }

    if (auto pluginBackend = tryCreatePluginBackendByName(type, config)) {
        return pluginBackend;
    }

    spdlog::error("Unknown storage backend type: {}", config.type);
    return nullptr;
}

BackendConfig StorageBackendFactory::parseURL(const std::string& url) {
    BackendConfig config;

    // Check if it's a local path (no scheme or file:// scheme)
    if (url.find("://") == std::string::npos || url.starts_with("file://")) {
        config.type = "filesystem";

        if (url.starts_with("file://")) {
            config.localPath = url.substr(7); // Remove "file://" prefix
        } else {
            config.localPath = url;
        }

        return config;
    }

    // Parse URL for remote backends
    std::regex urlRegex(R"(^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?$)");
    std::smatch match;

    if (!std::regex_match(url, match, urlRegex)) {
        spdlog::error("Invalid URL format: {}", url);
        return config;
    }

    std::string scheme = match[2].str();

    // Set type based on scheme
    if (scheme == "s3") {
        config.type = "s3";
    } else if (scheme == "http" || scheme == "https") {
        config.type = scheme;
    } else if (scheme == "ftp" || scheme == "ftps") {
        config.type = "ftp";
    } else {
        spdlog::warn("Unknown URL scheme '{}', defaulting to filesystem", scheme);
        config.type = "filesystem";
        config.localPath = url;
        return config;
    }

    config.url = url;

    // Parse query parameters for additional configuration
    std::string query = match[7].str();
    if (!query.empty()) {
        std::regex paramRegex("([^&=]+)=([^&]*)");
        auto paramsBegin = std::sregex_iterator(query.begin(), query.end(), paramRegex);
        auto paramsEnd = std::sregex_iterator();

        for (auto i = paramsBegin; i != paramsEnd; ++i) {
            std::string key = (*i)[1].str();
            std::string value = (*i)[2].str();

            // URL decode value (basic implementation)
            size_t pos = 0;
            while ((pos = value.find('+', pos)) != std::string::npos) {
                value[pos] = ' ';
            }

            // Map known parameters
            if (key == "cache_size") {
                try {
                    config.cacheSize = std::stoull(value);
                } catch (...) {
                    spdlog::warn("Invalid cache_size parameter: {}", value);
                }
            } else if (key == "cache_ttl") {
                try {
                    config.cacheTTL = std::stoull(value);
                } catch (...) {
                    spdlog::warn("Invalid cache_ttl parameter: {}", value);
                }
            } else if (key == "timeout") {
                try {
                    config.requestTimeout = std::stoull(value);
                } catch (...) {
                    spdlog::warn("Invalid timeout parameter: {}", value);
                }
            }
        }
    }

    return config;
}

void StorageBackendFactory::registerBackend(
    const std::string& type, std::function<std::unique_ptr<IStorageBackend>()> factory) {
    auto& registry = getBackendRegistry();
    registry[type] = std::move(factory);
    spdlog::info("Registered custom storage backend: {}", type);
}

// FilesystemBackend implementation
namespace {

constexpr std::size_t kMaxFilesystemObjectKeyBytes = 4096;

Result<void> validateFilesystemObjectKey(std::string_view key) {
    if (key.empty() || key.size() > kMaxFilesystemObjectKeyBytes) {
        return Error{ErrorCode::InvalidPath, "filesystem backend key has an invalid length"};
    }
    if (key.front() == '/' || key.back() == '/' ||
        (key.size() >= 2 && std::isalpha(static_cast<unsigned char>(key[0])) && key[1] == ':')) {
        return Error{ErrorCode::InvalidPath, "filesystem backend key must be relative"};
    }

    std::size_t segmentStart = 0;
    for (std::size_t index = 0; index <= key.size(); ++index) {
        if (index < key.size()) {
            const auto ch = static_cast<unsigned char>(key[index]);
            if (ch == '\\' || ch < 0x20 || ch == 0x7f) {
                return Error{ErrorCode::InvalidPath,
                             "filesystem backend key contains a forbidden character"};
            }
            if (ch != '/') {
                continue;
            }
        }
        const auto segment = key.substr(segmentStart, index - segmentStart);
        if (segment.empty() || segment == "." || segment == "..") {
            return Error{ErrorCode::InvalidPath,
                         "filesystem backend key contains an invalid path segment"};
        }
        segmentStart = index + 1;
    }
    return {};
}

bool pathIsWithin(const std::filesystem::path& root, const std::filesystem::path& candidate) {
    const auto normalizedRoot = root.lexically_normal();
    const auto normalizedCandidate = candidate.lexically_normal();
    auto rootIt = normalizedRoot.begin();
    auto candidateIt = normalizedCandidate.begin();
    for (; rootIt != normalizedRoot.end(); ++rootIt, ++candidateIt) {
        if (candidateIt == normalizedCandidate.end() || *rootIt != *candidateIt) {
            return false;
        }
    }
    return true;
}

Result<void> verifyFilesystemContainment(const std::filesystem::path& root,
                                         const std::filesystem::path& candidate) {
    if (!pathIsWithin(root, candidate)) {
        return Error{ErrorCode::InvalidPath, "filesystem backend key escapes the object root"};
    }

    std::error_code ec;
    const auto canonicalRoot = plainPath(std::filesystem::weakly_canonical(longPath(root), ec));
    if (ec) {
        return Error{ErrorCode::InvalidPath,
                     "failed to resolve filesystem backend root: " + ec.message()};
    }
    const auto canonicalParent =
        plainPath(std::filesystem::weakly_canonical(longPath(candidate.parent_path()), ec));
    if (ec || !pathIsWithin(canonicalRoot, canonicalParent)) {
        return Error{ErrorCode::InvalidPath,
                     "filesystem backend key crosses a symlink outside the object root"};
    }

    const auto status = std::filesystem::symlink_status(longPath(candidate), ec);
    if (!ec && std::filesystem::is_symlink(status)) {
        return Error{ErrorCode::InvalidPath,
                     "filesystem backend object path must not be a symbolic link"};
    }
    return {};
}

// Layout. Flat keys (no '/') live at objects/HH/HH/<key>, sharded by the SHA-256 of the key.
// Hierarchical keys live at objects/ns/<dir>/.../<leaf>.k, so a prefix listing walks only the
// subtree its prefix names instead of every object. Earlier builds sharded hierarchical keys
// by hash too; a listing moves those into place (see visitKeys), and the marker file records
// that none remain.
//
// Leaves carry a ".k" suffix and a directory component that already ends in ".k" or ".d" gets
// ".d" appended. Leaf and directory names are then disjoint, so "a/b" and "a/b/c" coexist.
constexpr std::string_view kNamespaceDir = "ns";
constexpr std::string_view kLayoutMarker = "layout-namespaced-v1";
constexpr std::string_view kLeafSuffix = ".k";
constexpr std::string_view kEscapeSuffix = ".d";

bool isHierarchicalKey(std::string_view key) {
    return key.find('/') != std::string_view::npos;
}

bool hasReservedSuffix(std::string_view name) {
    return name.ends_with(kLeafSuffix) || name.ends_with(kEscapeSuffix);
}

std::string encodeDirectory(std::string_view component) {
    std::string name(component);
    if (hasReservedSuffix(component)) {
        name += kEscapeSuffix;
    }
    return name;
}

std::optional<std::string> decodeDirectory(std::string_view name) {
    if (name.ends_with(kEscapeSuffix)) {
        name.remove_suffix(kEscapeSuffix.size());
        if (!hasReservedSuffix(name)) {
            return std::nullopt;
        }
        return std::string(name);
    }
    if (name.ends_with(kLeafSuffix)) {
        return std::nullopt;
    }
    return std::string(name);
}

std::optional<std::string> decodeLeaf(std::string_view name) {
    if (name.size() <= kLeafSuffix.size() || !name.ends_with(kLeafSuffix)) {
        return std::nullopt;
    }
    name.remove_suffix(kLeafSuffix.size());
    return std::string(name);
}

// Encodes '/'-separated directory components; `dirs` must already be a valid key.
std::filesystem::path encodeDirectories(std::string_view dirs) {
    std::filesystem::path out;
    std::size_t start = 0;
    while (true) {
        const auto slash = dirs.find('/', start);
        out /= encodeDirectory(dirs.substr(start, slash - start));
        if (slash == std::string_view::npos) {
            return out;
        }
        start = slash + 1;
    }
}

// `key` must be a valid hierarchical key.
std::filesystem::path namespacedRelativePath(std::string_view key) {
    const auto slash = key.rfind('/');
    return encodeDirectories(key.substr(0, slash)) /
           (std::string(key.substr(slash + 1)) + std::string(kLeafSuffix));
}

std::filesystem::path legacyRelativePath(std::string_view key) {
    auto hasher = crypto::createSHA256Hasher();
    const auto hash = hasher->hash(
        std::span<const std::byte>(reinterpret_cast<const std::byte*>(key.data()), key.size()));
    return std::filesystem::path(hash.substr(0, 2)) / hash.substr(2, 2) / std::string(key);
}

bool writeLayoutMarker(const std::filesystem::path& basePath) {
    std::ofstream marker(longPath(basePath / kLayoutMarker), std::ios::trunc);
    marker << "1\n";
    return static_cast<bool>(marker);
}

enum class Migration { Moved, Superseded, Failed };

// Moves one legacy object into the namespaced tree without ever replacing a newer copy: the hard
// link fails when the destination exists, and then the legacy copy is simply retired.
Migration migrateLegacyObject(const std::filesystem::path& objectsDir,
                              const std::filesystem::path& legacyPath, std::string_view key) {
    if (!validateFilesystemObjectKey(key)) {
        return Migration::Failed;
    }
    const auto target = objectsDir / kNamespaceDir / namespacedRelativePath(key);
    std::error_code ec;
    if (!yams::common::ensureDirectories(longPath(target.parent_path()), ec) ||
        !verifyFilesystemContainment(objectsDir, target)) {
        return Migration::Failed;
    }
    std::filesystem::create_hard_link(longPath(legacyPath), longPath(target), ec);
    if (!ec || ec == std::errc::file_exists) {
        const bool linked = !ec;
        std::filesystem::remove(longPath(legacyPath), ec);
        return linked ? Migration::Moved : Migration::Superseded;
    }
    // Another backend on the same root may have moved it first.
    if (!std::filesystem::exists(longPath(legacyPath)) &&
        std::filesystem::exists(longPath(target))) {
        return Migration::Superseded;
    }
    return Migration::Failed;
}

// Removes now-empty legacy shard directories, deepest first. Directories that still hold flat
// keys are not empty and stay.
Result<void> pruneEmptyDirectories(const std::filesystem::path& dir, bool skipNamespace,
                                   const std::function<Result<void>()>& checkCancelled) {
    std::error_code ec;
    std::vector<std::filesystem::path> children;
    for (std::filesystem::directory_iterator it(traversalPath(dir), ec), end; !ec && it != end;
         it.increment(ec)) {
        if (skipNamespace && it->path().filename() == kNamespaceDir) {
            continue;
        }
        if (it->is_directory(ec) && !it->is_symlink(ec)) {
            children.push_back(it->path());
        }
    }
    for (const auto& child : children) {
        if (auto ready = checkCancelled(); !ready) {
            return ready;
        }
        if (auto pruned = pruneEmptyDirectories(child, false, checkCancelled); !pruned) {
            return pruned;
        }
        std::filesystem::remove(child, ec); // fails, harmlessly, while the directory has entries
    }
    return {};
}

} // namespace

Result<void> FilesystemBackend::initialize(const BackendConfig& config) {
    basePath_ = config.localPath;

    if (basePath_.empty()) {
        // Use default path
        const char* homeEnv =
            std::getenv("HOME"); // NOLINT(concurrency-mt-unsafe): process config snapshot
        std::filesystem::path homeDir =
            homeEnv ? std::filesystem::path(homeEnv) : std::filesystem::current_path();

        const char* xdgDataEnv =
            std::getenv("XDG_DATA_HOME"); // NOLINT(concurrency-mt-unsafe): process config snapshot
        std::filesystem::path dataHome =
            xdgDataEnv ? std::filesystem::path(xdgDataEnv) : (homeDir / ".local" / "share");

        basePath_ = dataHome / "yams" / "storage";
    }

    std::error_code ec;
#ifdef _WIN32
    // Resolve and normalize the configured root once. The extended-length prefix is deliberately
    // NOT applied here: it is added per operation, and only for paths that actually reach
    // MAX_PATH, so short-path semantics (listing, canonicalisation, iteration) stay identical to
    // the non-Windows code.
    auto absolutePath = std::filesystem::absolute(basePath_, ec);
    if (ec) {
        return Error{ErrorCode::InvalidPath, "Failed to resolve storage path: " + ec.message()};
    }
    basePath_ = absolutePath.lexically_normal();
#endif

    // Create storage directories
    if (!yams::common::ensureDirectories(longPath(basePath_ / "objects"), ec)) {
        return Result<void>(Error{ErrorCode::PermissionDenied,
                                  "Failed to create storage directory: " + ec.message()});
    }

    if (!yams::common::ensureDirectories(longPath(basePath_ / "temp"), ec)) {
        return Result<void>(
            Error{ErrorCode::PermissionDenied, "Failed to create temp directory: " + ec.message()});
    }

    // Without the marker, the root predates the namespaced layout or is new. A new root has
    // nothing in objects/ besides the namespace tree, so it can be marked right away.
    bool legacy = !std::filesystem::exists(longPath(basePath_ / kLayoutMarker));
    if (legacy) {
        legacy = false;
        for (std::filesystem::directory_iterator it(longPath(basePath_ / "objects"), ec), end;
             !ec && it != end; it.increment(ec)) {
            if (it->path().filename() != kNamespaceDir) {
                legacy = true;
                break;
            }
        }
        if (!legacy) {
            (void)writeLayoutMarker(basePath_);
        }
    }
    legacyHierarchical_.store(legacy, std::memory_order_release);

    spdlog::info("Initialized filesystem backend at: {}", basePath_.string());
    return {};
}

bool FilesystemBackend::legacyHierarchicalPending() const {
    if (!legacyHierarchical_.load(std::memory_order_acquire)) {
        return false;
    }
    // Another backend on the same root may have finished the migration.
    if (std::filesystem::exists(longPath(basePath_ / kLayoutMarker))) {
        legacyHierarchical_.store(false, std::memory_order_release);
        return false;
    }
    return true;
}

Result<std::filesystem::path> FilesystemBackend::getLegacyObjectPath(std::string_view key) const {
    if (auto valid = validateFilesystemObjectKey(key); !valid) {
        return valid.error();
    }
    const auto objectsRoot = basePath_ / "objects";
    auto objectPath = objectsRoot / legacyRelativePath(key);
    if (auto contained = verifyFilesystemContainment(objectsRoot, objectPath); !contained) {
        return contained.error();
    }
    return objectPath;
}

Result<std::filesystem::path> FilesystemBackend::getObjectPath(std::string_view key) const {
    if (auto valid = validateFilesystemObjectKey(key); !valid) {
        return valid.error();
    }

    const auto objectsRoot = basePath_ / "objects";
    const auto objectPath = isHierarchicalKey(key)
                                ? objectsRoot / kNamespaceDir / namespacedRelativePath(key)
                                : objectsRoot / legacyRelativePath(key);
    if (auto contained = verifyFilesystemContainment(objectsRoot, objectPath); !contained) {
        return contained.error();
    }
    return objectPath;
}

Result<void> FilesystemBackend::ensureDirectoryExists(const std::filesystem::path& path) const {
    std::error_code ec;
    if (!yams::common::ensureDirectories(longPath(path.parent_path()), ec)) {
        return Result<void>(
            Error{ErrorCode::PermissionDenied, "Failed to create directory: " + ec.message()});
    }
    return {};
}

// After a hierarchical key is written in the namespaced tree, its pre-migration copy is stale.
void FilesystemBackend::retireLegacyCopy(std::string_view key) const {
    if (!isHierarchicalKey(key) || !legacyHierarchicalPending()) {
        return;
    }
    if (auto legacy = getLegacyObjectPath(key)) {
        std::error_code ec;
        std::filesystem::remove(longPath(legacy.value()), ec);
    }
}

// Finds the file holding `key`. While legacy objects remain, a listing may move one between the
// two checks, so the namespaced path is checked again after the legacy one.
Result<std::optional<std::filesystem::path>>
FilesystemBackend::resolveExisting(std::string_view key) const {
    auto resolvedPath = getObjectPath(key);
    if (!resolvedPath) {
        return resolvedPath.error();
    }
    const auto& objectPath = resolvedPath.value();
    if (std::filesystem::exists(longPath(objectPath))) {
        return std::optional{objectPath};
    }
    if (!isHierarchicalKey(key) || !legacyHierarchicalPending()) {
        return std::optional<std::filesystem::path>{};
    }
    auto legacy = getLegacyObjectPath(key);
    if (!legacy) {
        return legacy.error();
    }
    if (std::filesystem::exists(longPath(legacy.value()))) {
        return std::optional{legacy.value()};
    }
    if (std::filesystem::exists(longPath(objectPath))) {
        return std::optional{objectPath};
    }
    return std::optional<std::filesystem::path>{};
}

Result<void> FilesystemBackend::store(std::string_view key, std::span<const std::byte> data) {
    auto resolvedPath = getObjectPath(key);
    if (!resolvedPath) {
        return resolvedPath.error();
    }
    auto objectPath = resolvedPath.value();

    if (auto result = ensureDirectoryExists(objectPath); !result) {
        return result;
    }
    // Re-resolve after directory creation so a pre-existing symlink cannot redirect the write.
    resolvedPath = getObjectPath(key);
    if (!resolvedPath) {
        return resolvedPath.error();
    }
    objectPath = resolvedPath.value();

    // Write to temp file first for atomicity
    // Use hash of key for temp filename to avoid path issues with keys containing slashes
    auto hasher = crypto::createSHA256Hasher();
    auto tempHash = hasher->hash(
        std::span<const std::byte>(reinterpret_cast<const std::byte*>(key.data()), key.size()));
    auto tempPath = basePath_ / "temp" / (tempHash + ".tmp");

    // clear() may remove the owned namespace between service generations. Recreate the
    // temporary staging directory lazily so the same backend can be restarted safely.
    std::error_code ec;
    if (!yams::common::ensureDirectories(longPath(basePath_ / "temp"), ec)) {
        return Result<void>(
            Error{ErrorCode::PermissionDenied, "Failed to create temp directory: " + ec.message()});
    }

    {
        std::ofstream file(longPath(tempPath), std::ios::binary);
        if (!file) {
            return Result<void>(Error{ErrorCode::PermissionDenied, "Failed to create temp file"});
        }

        file.write(reinterpret_cast<const char*>(data.data()),
                   static_cast<std::streamsize>(data.size()));

        if (!file) {
            std::filesystem::remove(longPath(tempPath));
            return Result<void>(Error{ErrorCode::Unknown, "Failed to write data"});
        }
    }

    // Atomic rename
    ec.clear();
    std::filesystem::rename(longPath(tempPath), longPath(objectPath), ec);

    if (ec) {
        std::filesystem::remove(longPath(tempPath));

        // Check if file already exists (not an error for content-addressed storage)
        if (std::filesystem::exists(longPath(objectPath))) {
            retireLegacyCopy(key);
            return {};
        }

        return Result<void>(Error{
            ErrorCode::Unknown,
            "Failed to rename file: " + ec.message() + " (code=" + std::to_string(ec.value()) +
                ", source_chars=" + std::to_string(tempPath.native().size()) +
                ", destination_chars=" + std::to_string(objectPath.native().size()) + ")"});
    }

    retireLegacyCopy(key);
    return {};
}

Result<std::vector<std::byte>> FilesystemBackend::retrieve(std::string_view key) const {
    YAMS_ZONE_SCOPED_N("storage_backend::filesystem_retrieve");
#ifdef YAMS_TESTING
    if (readObserver) {
        (*readObserver)(key);
    }
#endif
    auto resolved = resolveExisting(key);
    if (!resolved) {
        return resolved.error();
    }
    if (!resolved.value()) {
        return Result<std::vector<std::byte>>(Error{ErrorCode::ChunkNotFound});
    }

    std::ifstream file(longPath(*resolved.value()), std::ios::binary | std::ios::ate);
    if (!file && isHierarchicalKey(key)) {
        // A concurrent migration may have moved the legacy file after it was resolved.
        auto retried = resolveExisting(key);
        if (retried && retried.value()) {
            file.clear();
            file.open(longPath(*retried.value()), std::ios::binary | std::ios::ate);
        }
    }
    if (!file) {
        return Result<std::vector<std::byte>>(Error{ErrorCode::PermissionDenied});
    }

    auto fileSize = file.tellg();
    file.seekg(0, std::ios::beg);

    std::vector<std::byte> buffer(static_cast<size_t>(fileSize));
    file.read(reinterpret_cast<char*>(buffer.data()), fileSize);

    if (!file) {
        return Result<std::vector<std::byte>>(Error{ErrorCode::Unknown, "Failed to read file"});
    }

    return buffer;
}

Result<bool> FilesystemBackend::exists(std::string_view key) const {
    auto resolved = resolveExisting(key);
    if (!resolved) {
        return resolved.error();
    }
    return Result<bool>(resolved.value().has_value());
}

Result<void> FilesystemBackend::remove(std::string_view key) {
    auto resolvedPath = getObjectPath(key);
    if (!resolvedPath) {
        return resolvedPath.error();
    }
    const auto& objectPath = resolvedPath.value();

    // Legacy copy first: a migration that links it after this point fails on the missing
    // source, and one that linked it before is undone by the namespaced removal below.
    if (isHierarchicalKey(key) && legacyHierarchicalPending()) {
        auto legacy = getLegacyObjectPath(key);
        if (!legacy) {
            return legacy.error();
        }
        std::error_code legacyEc;
        std::filesystem::remove(longPath(legacy.value()), legacyEc);
        if (legacyEc && std::filesystem::exists(longPath(legacy.value()))) {
            return Error{ErrorCode::PermissionDenied,
                         "Failed to remove file: " + legacyEc.message()};
        }
    }

    std::error_code ec;
    std::filesystem::remove(longPath(objectPath), ec);

    if (ec && std::filesystem::exists(longPath(objectPath))) {
        return Result<void>(
            Error{ErrorCode::PermissionDenied, "Failed to remove file: " + ec.message()});
    }

    return {};
}

Result<void> FilesystemBackend::clear() {
    // Remove only backend-owned structures. Never recursively delete basePath_ itself:
    // callers may have placed the backend beneath a directory containing unrelated markers.
    for (const auto* component : {"objects", "temp"}) {
        std::error_code ec;
        std::filesystem::remove_all(longPath(basePath_ / component), ec);
        if (ec && std::filesystem::exists(longPath(basePath_ / component))) {
            return Error{ErrorCode::PermissionDenied,
                         "Failed to clear filesystem backend: " + ec.message()};
        }
    }
    // Nothing legacy can remain. The marker goes too, so clear() leaves no owned files behind;
    // the next initialize() of the empty root writes it again.
    legacyHierarchical_.store(false, std::memory_order_release);
    std::error_code markerEc;
    std::filesystem::remove(longPath(basePath_ / kLayoutMarker), markerEc);
    return {};
}

// Lists keys under `prefix`. Hierarchical keys come from the namespaced subtree the prefix names.
// The hash-sharded tree is walked only when flat keys can match (a prefix without '/') or while
// legacy hierarchical objects remain. Such a pass moves each one into the namespaced tree before
// that tree is walked, so every key is visited exactly once, and the first complete pass with
// nothing left to move retires the legacy tree for good.
template <typename Visitor>
Result<void> FilesystemBackend::visitKeys(std::string_view prefix, Visitor&& visitor,
                                          const bool* optionalProbeComplete) const {
    std::function<Result<void>()> checkCancelled = [this]() -> Result<void> {
        if (cancelled_.load(std::memory_order_relaxed)) {
            return Error{ErrorCode::OperationCancelled, "filesystem listing cancelled"};
        }
        return listingWorkCheck_ ? listingWorkCheck_() : Result<void>{};
    };
    if (auto ready = checkCancelled(); !ready) {
        return ready.error();
    }
#ifdef YAMS_TESTING
    auto* observed = listingStatsFor(prefix);
    if (observed) {
        ++observed->walks;
    }
#endif
    // Check even directories and non-matching entries. A blocked filesystem syscall is not
    // interruptible, but traversal stops between entries.
    const auto onEntry = [&]() -> Result<void> {
#ifdef YAMS_TESTING
        if (observed) {
            ++observed->entriesVisited;
            if (observed->onEntry) {
                observed->onEntry();
            }
        }
#endif
        return checkCancelled();
    };
    const auto emit = [&](std::string key) -> bool {
        if (!prefix.empty() && !key.starts_with(prefix)) {
            return false;
        }
#ifdef YAMS_TESTING
        if (observed) {
            ++observed->matchingKeys;
        }
#endif
        visitor(std::move(key));
        // Only an optional cache probe may stop early. Its caller must reject the snapshot;
        // authoritative listing callers never supply this signal.
        return optionalProbeComplete && *optionalProbeComplete;
    };

    const auto objectsDir = basePath_ / "objects";
    if (!std::filesystem::exists(longPath(objectsDir))) {
        return checkCancelled();
    }

    try {
        const bool flatKeysMayMatch = prefix.find('/') == std::string_view::npos;
        const bool migrate = legacyHierarchicalPending();
        if (flatKeysMayMatch || migrate) {
            std::size_t unmigrated = 0;
            for (auto it = std::filesystem::recursive_directory_iterator(traversalPath(objectsDir));
                 it != std::filesystem::recursive_directory_iterator(); ++it) {
                if (auto ready = onEntry(); !ready) {
                    return ready.error();
                }
                const auto& entry = *it;
                if (it.depth() == 0 && entry.path().filename() == kNamespaceDir) {
                    it.disable_recursion_pending();
                    continue;
                }
                if (!entry.is_regular_file()) {
                    continue;
                }
                // Derive the key lexically from the de-prefixed entry path. filesystem::relative()
                // canonicalises both operands, which is unsafe when one carries the extended
                // Win32 prefix and the other does not.
                std::string pathStr =
                    plainPath(entry.path()).lexically_relative(objectsDir).generic_string();
                std::string key = pathStr.length() > 5 && pathStr[2] == '/' && pathStr[5] == '/'
                                      ? pathStr.substr(6)
                                      : std::move(pathStr);
                if (migrate && isHierarchicalKey(key)) {
                    if (migrateLegacyObject(objectsDir, plainPath(entry.path()), key) !=
                        Migration::Failed) {
                        continue; // now in the namespaced tree, which is walked next
                    }
                    ++unmigrated;
                }
                if (emit(std::move(key))) {
                    return checkCancelled();
                }
            }
            if (migrate && unmigrated == 0) {
                if (auto pruned = pruneEmptyDirectories(objectsDir, true, checkCancelled);
                    !pruned) {
                    return pruned.error();
                }
                if (writeLayoutMarker(basePath_)) {
                    legacyHierarchical_.store(false, std::memory_order_release);
                    spdlog::info("[FilesystemBackend] moved legacy objects to the namespaced "
                                 "layout at {}",
                                 basePath_.string());
                }
            } else if (unmigrated > 0) {
                spdlog::warn("[FilesystemBackend] {} legacy objects could not be moved at {}",
                             unmigrated, basePath_.string());
            }
        }

        // Start at the deepest directory every matching key shares: the prefix up to its last
        // '/'. A prefix whose directories are not a valid key cannot match a stored key.
        auto start = objectsDir / kNamespaceDir;
        std::string keyBase;
        if (const auto slash = prefix.rfind('/'); slash != std::string_view::npos) {
            const auto dirs = prefix.substr(0, slash);
            if (!validateFilesystemObjectKey(dirs)) {
                return checkCancelled();
            }
            start /= encodeDirectories(dirs);
            keyBase = std::string(prefix.substr(0, slash + 1));
        }
        if (!std::filesystem::is_directory(longPath(start))) {
            return checkCancelled();
        }
        for (auto it = std::filesystem::recursive_directory_iterator(traversalPath(start));
             it != std::filesystem::recursive_directory_iterator(); ++it) {
            if (auto ready = onEntry(); !ready) {
                return ready.error();
            }
            const auto& entry = *it;
            const auto name = plainPath(entry.path()).filename().string();
            if (entry.is_directory()) {
                if (!decodeDirectory(name)) {
                    it.disable_recursion_pending(); // not written by this layout
                }
                continue;
            }
            if (!entry.is_regular_file()) {
                continue;
            }
            auto leaf = decodeLeaf(name);
            if (!leaf) {
                continue;
            }
            std::string key = keyBase;
            const auto relative = plainPath(entry.path()).lexically_relative(start).parent_path();
            for (const auto& component : relative) {
                // Directories were validated on the way down.
                key += *decodeDirectory(component.string());
                key += '/';
            }
            key += *leaf;
            if (emit(std::move(key))) {
                return checkCancelled();
            }
        }
    } catch (const std::filesystem::filesystem_error& e) {
        return Error{ErrorCode::PermissionDenied, e.what()};
    }
    return checkCancelled();
}

Result<std::vector<std::string>> FilesystemBackend::list(std::string_view prefix) const {
    std::vector<std::string> results;
    auto visited = visitKeys(prefix, [&](std::string key) {
        results.push_back(std::move(key));
        observePeakRetainedKeys(prefix, results.size());
    });
    if (!visited) {
        return visited.error();
    }
    return results;
}

Result<std::optional<std::vector<std::string>>>
FilesystemBackend::listBoundedSnapshot(std::string_view prefix, std::size_t maxKeys,
                                       std::size_t maxKeyBytes) const {
    std::vector<std::string> keys;
    std::size_t retainedBytes = 0;
    bool oversized = false;
    auto visited = visitKeys(
        prefix,
        [&](std::string key) {
            if (keys.size() >= maxKeys || key.size() > maxKeyBytes - retainedBytes) {
                oversized = true;
                return;
            }
            retainedBytes += key.size();
            keys.push_back(std::move(key));
            observePeakRetainedKeys(prefix, keys.size());
        },
        &oversized);
    if (!visited) {
        return visited.error();
    }
    if (oversized) {
        return std::optional<std::vector<std::string>>{};
    }
    std::ranges::sort(keys);
    return std::optional<std::vector<std::string>>{std::move(keys)};
}

Result<ObjectListPage> FilesystemBackend::listPage(std::string_view prefix,
                                                   std::optional<std::string_view> cursor,
                                                   std::size_t limit) const {
#ifdef YAMS_TESTING
    if (auto* observed = listingStatsFor(prefix)) {
        ++observed->pageCalls;
    }
#endif
    if (limit == 0) {
        return Error{ErrorCode::InvalidArgument, "object-list page limit must be positive"};
    }
    // Retain the smallest eligible page plus one look-ahead key in a max-heap. The
    // directory walk remains complete so traversal failures never return partial success.
    const auto retainedLimit = limit == std::numeric_limits<std::size_t>::max() ? limit : limit + 1;
    std::vector<std::string> keys;
    auto visited = visitKeys(prefix, [&](std::string key) {
        if (cursor && std::string_view{key} <= *cursor) {
            return;
        }
        if (keys.size() < retainedLimit) {
            keys.push_back(std::move(key));
            std::push_heap(keys.begin(), keys.end());
        } else if (key < keys.front()) {
            std::pop_heap(keys.begin(), keys.end());
            keys.back() = std::move(key);
            std::push_heap(keys.begin(), keys.end());
        }
        observePeakRetainedKeys(prefix, keys.size());
    });
    if (!visited) {
        return visited.error();
    }

    std::sort_heap(keys.begin(), keys.end());
    const bool hasMore = keys.size() > limit;
    if (hasMore) {
        keys.pop_back();
    }

    ObjectListPage page;
    if (hasMore) {
        page.nextCursor = keys.back();
    }
    page.keys = std::move(keys);
    return page;
}

Result<::yams::StorageStats> FilesystemBackend::getStats() const {
    ::yams::StorageStats stats;

    try {
        for (const auto& entry :
             std::filesystem::recursive_directory_iterator(traversalPath(basePath_ / "objects"))) {
            if (entry.is_regular_file()) {
                stats.totalObjects++;
                stats.totalBytes += entry.file_size();
            }
        }
    } catch (const std::filesystem::filesystem_error& e) {
        spdlog::warn("Failed to compute storage stats: {}", e.what());
    }

    return stats;
}

std::future<Result<void>> FilesystemBackend::storeAsync(std::string_view key,
                                                        std::span<const std::byte> data) {
    return std::async(
        std::launch::async,
        [this, key = std::string(key), data = std::vector<std::byte>(data.begin(), data.end())]() {
            return store(key, data);
        });
}

std::future<Result<std::vector<std::byte>>>
FilesystemBackend::retrieveAsync(std::string_view key) const {
    return std::async(std::launch::async,
                      [this, key = std::string(key)]() { return retrieve(key); });
}

} // namespace yams::storage
