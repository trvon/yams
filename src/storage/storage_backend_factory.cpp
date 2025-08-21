#include <yams/storage/storage_backend.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <fstream>
#include <regex>
#include <unordered_map>

namespace yams::storage {

// Registry for custom backends
static std::unordered_map<std::string, std::function<std::unique_ptr<IStorageBackend>()>>& 
getBackendRegistry() {
    static std::unordered_map<std::string, std::function<std::unique_ptr<IStorageBackend>()>> registry;
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
    
    // Built-in backends
    if (type == "filesystem" || type == "local" || type == "file") {
        auto backend = std::make_unique<FilesystemBackend>();
        if (auto result = backend->initialize(config); !result) {
            spdlog::error("Failed to initialize filesystem backend: {}", result.error().message);
            return nullptr;
        }
        return backend;
    }
    
    if (type == "s3" || type == "http" || type == "https" || type == "ftp") {
        auto backend = std::make_unique<URLBackend>();
        if (auto result = backend->initialize(config); !result) {
            spdlog::error("Failed to initialize URL backend: {}", result.error().message);
            return nullptr;
        }
        return backend;
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
            config.localPath = url.substr(7);  // Remove "file://" prefix
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

void StorageBackendFactory::registerBackend(const std::string& type,
                                           std::function<std::unique_ptr<IStorageBackend>()> factory) {
    auto& registry = getBackendRegistry();
    registry[type] = std::move(factory);
    spdlog::info("Registered custom storage backend: {}", type);
}

// FilesystemBackend implementation
Result<void> FilesystemBackend::initialize(const BackendConfig& config) {
    basePath_ = config.localPath;
    
    if (basePath_.empty()) {
        // Use default path
        const char* homeEnv = std::getenv("HOME");
        std::filesystem::path homeDir = homeEnv ? std::filesystem::path(homeEnv) 
                                                 : std::filesystem::current_path();
        
        const char* xdgDataEnv = std::getenv("XDG_DATA_HOME");
        std::filesystem::path dataHome = xdgDataEnv ? std::filesystem::path(xdgDataEnv)
                                                     : (homeDir / ".local" / "share");
        
        basePath_ = dataHome / "yams" / "storage";
    }
    
    // Create storage directories
    std::error_code ec;
    std::filesystem::create_directories(basePath_ / "objects", ec);
    if (ec) {
        return Result<void>(Error{ErrorCode::PermissionDenied, 
                           "Failed to create storage directory: " + ec.message()});
    }
    
    std::filesystem::create_directories(basePath_ / "temp", ec);
    if (ec) {
        return Result<void>(Error{ErrorCode::PermissionDenied,
                           "Failed to create temp directory: " + ec.message()});
    }
    
    spdlog::info("Initialized filesystem backend at: {}", basePath_.string());
    return {};
}

std::filesystem::path FilesystemBackend::getObjectPath(std::string_view key) const {
    // Use sharding for better filesystem performance
    std::string keyStr(key);
    
    if (keyStr.length() >= 4) {
        // Create two-level sharding: ab/cd/rest_of_key
        return basePath_ / "objects" / keyStr.substr(0, 2) / keyStr.substr(2, 2) / keyStr;
    }
    
    return basePath_ / "objects" / keyStr;
}

Result<void> FilesystemBackend::ensureDirectoryExists(const std::filesystem::path& path) const {
    std::error_code ec;
    std::filesystem::create_directories(path.parent_path(), ec);
    
    if (ec) {
        return Result<void>(Error{ErrorCode::PermissionDenied,
                           "Failed to create directory: " + ec.message()});
    }
    
    return {};
}

Result<void> FilesystemBackend::store(std::string_view key, std::span<const std::byte> data) {
    auto objectPath = getObjectPath(key);
    
    // Ensure parent directory exists
    if (auto result = ensureDirectoryExists(objectPath); !result) {
        return result;
    }
    
    // Write to temp file first for atomicity
    auto tempPath = basePath_ / "temp" / (std::string(key) + ".tmp");
    
    {
        std::ofstream file(tempPath, std::ios::binary);
        if (!file) {
            return Result<void>(Error{ErrorCode::PermissionDenied,
                               "Failed to create temp file"});
        }
        
        file.write(reinterpret_cast<const char*>(data.data()),
                   static_cast<std::streamsize>(data.size()));
        
        if (!file) {
            std::filesystem::remove(tempPath);
            return Result<void>(Error{ErrorCode::Unknown, "Failed to write data"});
        }
    }
    
    // Atomic rename
    std::error_code ec;
    std::filesystem::rename(tempPath, objectPath, ec);
    
    if (ec) {
        std::filesystem::remove(tempPath);
        
        // Check if file already exists (not an error for content-addressed storage)
        if (std::filesystem::exists(objectPath)) {
            return {};
        }
        
        return Result<void>(Error{ErrorCode::Unknown,
                           "Failed to rename file: " + ec.message()});
    }
    
    return {};
}

Result<std::vector<std::byte>> FilesystemBackend::retrieve(std::string_view key) const {
    auto objectPath = getObjectPath(key);
    
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
        return Result<std::vector<std::byte>>(Error{ErrorCode::Unknown,
                                              "Failed to read file"});
    }
    
    return buffer;
}

Result<bool> FilesystemBackend::exists(std::string_view key) const {
    auto objectPath = getObjectPath(key);
    return Result<bool>(std::filesystem::exists(objectPath));
}

Result<void> FilesystemBackend::remove(std::string_view key) {
    auto objectPath = getObjectPath(key);
    
    std::error_code ec;
    std::filesystem::remove(objectPath, ec);
    
    if (ec && std::filesystem::exists(objectPath)) {
        return Result<void>(Error{ErrorCode::PermissionDenied,
                           "Failed to remove file: " + ec.message()});
    }
    
    return {};
}

Result<std::vector<std::string>> FilesystemBackend::list(std::string_view prefix) const {
    std::vector<std::string> results;
    
    try {
        for (const auto& entry : std::filesystem::recursive_directory_iterator(basePath_ / "objects")) {
            if (entry.is_regular_file()) {
                auto relativePath = std::filesystem::relative(entry.path(), basePath_ / "objects");
                std::string pathStr = relativePath.string();
                
                // Extract the original key from the sharded path
                // Sharding creates: ab/cd/original_key_here
                // We need to skip the first two directory levels if they exist
                std::string key;
                size_t slashCount = 0;
                size_t pos = 0;
                
                // Count and skip first 2 directory levels (sharding directories)
                for (size_t i = 0; i < pathStr.length(); ++i) {
                    if (pathStr[i] == '/' || pathStr[i] == '\\') {
                        slashCount++;
                        if (slashCount == 2) {
                            pos = i + 1;
                            break;
                        }
                    }
                }
                
                // If we have sharding (2+ slashes), use the part after sharding
                // Otherwise use the whole path (for short keys without sharding)
                if (slashCount >= 2 && pos < pathStr.length()) {
                    key = pathStr.substr(pos);
                } else {
                    key = pathStr;
                }
                
                // Normalize path separators to forward slashes
                std::replace(key.begin(), key.end(), '\\', '/');
                
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

Result<::yams::StorageStats> FilesystemBackend::getStats() const {
    ::yams::StorageStats stats;
    
    try {
        for (const auto& entry : std::filesystem::recursive_directory_iterator(basePath_ / "objects")) {
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
    return std::async(std::launch::async, [this, key = std::string(key),
                                           data = std::vector<std::byte>(data.begin(), data.end())]() {
        return store(key, data);
    });
}

std::future<Result<std::vector<std::byte>>> FilesystemBackend::retrieveAsync(std::string_view key) const {
    return std::async(std::launch::async, [this, key = std::string(key)]() {
        return retrieve(key);
    });
}

} // namespace yams::storage