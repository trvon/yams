#include <yams/common/fs_utils.h>
#include <yams/crypto/hasher.h>
#include <yams/profiling.h>
#include <yams/storage/object_storage_plugin_loader.h>
#include <yams/storage/storage_backend.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <fstream>
#include <regex>
#include <unordered_map>

namespace yams::storage {

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
    const auto canonicalRoot = std::filesystem::weakly_canonical(root, ec);
    if (ec) {
        return Error{ErrorCode::InvalidPath,
                     "failed to resolve filesystem backend root: " + ec.message()};
    }
    const auto canonicalParent = std::filesystem::weakly_canonical(candidate.parent_path(), ec);
    if (ec || !pathIsWithin(canonicalRoot, canonicalParent)) {
        return Error{ErrorCode::InvalidPath,
                     "filesystem backend key crosses a symlink outside the object root"};
    }

    const auto status = std::filesystem::symlink_status(candidate, ec);
    if (!ec && std::filesystem::is_symlink(status)) {
        return Error{ErrorCode::InvalidPath,
                     "filesystem backend object path must not be a symbolic link"};
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

    // Create storage directories
    std::error_code ec;
    if (!yams::common::ensureDirectories(basePath_ / "objects", ec)) {
        return Result<void>(Error{ErrorCode::PermissionDenied,
                                  "Failed to create storage directory: " + ec.message()});
    }

    if (!yams::common::ensureDirectories(basePath_ / "temp", ec)) {
        return Result<void>(
            Error{ErrorCode::PermissionDenied, "Failed to create temp directory: " + ec.message()});
    }

    spdlog::info("Initialized filesystem backend at: {}", basePath_.string());
    return {};
}

Result<std::filesystem::path> FilesystemBackend::getObjectPath(std::string_view key) const {
    if (auto valid = validateFilesystemObjectKey(key); !valid) {
        return valid.error();
    }

    std::string keyStr(key);
    auto hasher = crypto::createSHA256Hasher();
    const auto hash = hasher->hash(
        std::span<const std::byte>(reinterpret_cast<const std::byte*>(key.data()), key.size()));

    const auto objectsRoot = basePath_ / "objects";
    const auto objectPath = hash.length() >= 4
                                ? objectsRoot / hash.substr(0, 2) / hash.substr(2, 2) / keyStr
                                : objectsRoot / keyStr;
    if (auto contained = verifyFilesystemContainment(objectsRoot, objectPath); !contained) {
        return contained.error();
    }
    return objectPath;
}

Result<void> FilesystemBackend::ensureDirectoryExists(const std::filesystem::path& path) const {
    std::error_code ec;
    if (!yams::common::ensureDirectories(path.parent_path(), ec)) {
        return Result<void>(
            Error{ErrorCode::PermissionDenied, "Failed to create directory: " + ec.message()});
    }
    return {};
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
    if (!yams::common::ensureDirectories(basePath_ / "temp", ec)) {
        return Result<void>(
            Error{ErrorCode::PermissionDenied, "Failed to create temp directory: " + ec.message()});
    }

    {
        std::ofstream file(tempPath, std::ios::binary);
        if (!file) {
            return Result<void>(Error{ErrorCode::PermissionDenied, "Failed to create temp file"});
        }

        file.write(reinterpret_cast<const char*>(data.data()),
                   static_cast<std::streamsize>(data.size()));

        if (!file) {
            std::filesystem::remove(tempPath);
            return Result<void>(Error{ErrorCode::Unknown, "Failed to write data"});
        }
    }

    // Atomic rename
    ec.clear();
    std::filesystem::rename(tempPath, objectPath, ec);

    if (ec) {
        std::filesystem::remove(tempPath);

        // Check if file already exists (not an error for content-addressed storage)
        if (std::filesystem::exists(objectPath)) {
            return {};
        }

        return Result<void>(Error{ErrorCode::Unknown, "Failed to rename file: " + ec.message()});
    }

    return {};
}

Result<std::vector<std::byte>> FilesystemBackend::retrieve(std::string_view key) const {
    YAMS_ZONE_SCOPED_N("storage_backend::filesystem_retrieve");
    auto resolvedPath = getObjectPath(key);
    if (!resolvedPath) {
        return resolvedPath.error();
    }
    const auto& objectPath = resolvedPath.value();

    if (!std::filesystem::exists(objectPath)) {
        return Result<std::vector<std::byte>>(Error{ErrorCode::ChunkNotFound});
    }

    std::ifstream file(objectPath, std::ios::binary | std::ios::ate);
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
    auto resolvedPath = getObjectPath(key);
    if (!resolvedPath) {
        return resolvedPath.error();
    }
    return Result<bool>(std::filesystem::exists(resolvedPath.value()));
}

Result<void> FilesystemBackend::remove(std::string_view key) {
    auto resolvedPath = getObjectPath(key);
    if (!resolvedPath) {
        return resolvedPath.error();
    }
    const auto& objectPath = resolvedPath.value();

    std::error_code ec;
    std::filesystem::remove(objectPath, ec);

    if (ec && std::filesystem::exists(objectPath)) {
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
        std::filesystem::remove_all(basePath_ / component, ec);
        if (ec && std::filesystem::exists(basePath_ / component)) {
            return Error{ErrorCode::PermissionDenied,
                         "Failed to clear filesystem backend: " + ec.message()};
        }
    }
    return {};
}

Result<std::vector<std::string>> FilesystemBackend::list(std::string_view prefix) const {
    std::vector<std::string> results;

    // Check if objects directory exists
    auto objectsDir = basePath_ / "objects";
    if (!std::filesystem::exists(objectsDir)) {
        return results; // Return empty list if no objects stored yet
    }

    try {
        for (const auto& entry : std::filesystem::recursive_directory_iterator(objectsDir)) {
            if (entry.is_regular_file()) {
                auto relativePath = std::filesystem::relative(entry.path(), basePath_ / "objects");
                std::string pathStr = relativePath.string();

                // Normalize path separators to forward slashes first
                std::replace(pathStr.begin(), pathStr.end(), '\\', '/');

                // Extract the original key from the hash-sharded path
                // Hash-based sharding creates: HH/HH/original_key
                // where HH/HH are hex chars from the key's SHA256 hash
                std::string key;

                // Check if this is a sharded path (XX/YY/...)
                // Hash sharding always uses 2-char hex directories
                if (pathStr.length() > 5 && pathStr[2] == '/' && pathStr[5] == '/') {
                    // This is a sharded path - everything after "HH/HH/" is the original key
                    key = pathStr.substr(6); // Skip "HH/HH/"
                } else {
                    // Not sharded - use the whole path as the key
                    key = pathStr;
                }

                if (prefix.empty() || key.starts_with(prefix)) {
                    results.push_back(key);
                }
            }
        }
    } catch (const std::filesystem::filesystem_error& e) {
        return Result<std::vector<std::string>>(Error{ErrorCode::PermissionDenied, e.what()});
    }

    return results;
}

Result<ObjectListPage> FilesystemBackend::listPage(std::string_view prefix,
                                                   std::optional<std::string_view> cursor,
                                                   std::size_t limit) const {
    if (limit == 0) {
        return Error{ErrorCode::InvalidArgument, "object-list page limit must be positive"};
    }
    // Filesystem listings are local. Keep only the requested page resident while
    // walking hash shards; remote implementations override with native tokens.
    auto listed = list(prefix);
    if (!listed) {
        return listed.error();
    }
    auto keys = std::move(listed.value());
    std::ranges::sort(keys);
    auto begin = cursor ? std::ranges::upper_bound(keys, *cursor) : keys.begin();
    const auto remaining = static_cast<std::size_t>(std::distance(begin, keys.end()));
    const auto count = std::min(limit, remaining);
    ObjectListPage page;
    page.keys.assign(begin, std::next(begin, static_cast<std::ptrdiff_t>(count)));
    if (count < remaining) {
        page.nextCursor = page.keys.back();
    }
    return page;
}

Result<::yams::StorageStats> FilesystemBackend::getStats() const {
    ::yams::StorageStats stats;

    try {
        for (const auto& entry :
             std::filesystem::recursive_directory_iterator(basePath_ / "objects")) {
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
