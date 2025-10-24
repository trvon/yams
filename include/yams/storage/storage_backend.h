#pragma once

#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <vector>
#include <yams/core/types.h>

namespace yams::storage {

/**
 * Storage backend configuration
 * Note: keep fields unique; this header is widely included.
 */
struct BackendConfig {
    // Backend selection and addressing
    std::string type = "filesystem"; // filesystem, s3, http, ftp
    std::string url;                 // Remote URL (e.g., s3://bucket/prefix)
    std::filesystem::path localPath; // Local base path for filesystem backend

    // Credentials and headers (for remote backends)
    std::unordered_map<std::string, std::string> credentials;

    // Client-side cache for remote reads
    size_t cacheSize = 256 * 1024 * 1024; // 256MB
    size_t cacheTTL = 3600;               // 1 hour (seconds)

    // Performance tuning
    size_t maxConcurrentOps = 10; // Max in-flight operations
    size_t requestTimeout = 30;   // Per-request timeout (seconds)

    // Retry policy (exponential backoff with jitter)
    int maxRetries = 3;    // Attempts for transient failures
    int baseRetryMs = 100; // Base backoff delay
    int jitterMs = 50;     // Random jitter range

    // Range GET support
    bool enableRangeGets = true;

    // S3-specific options (scaffold/placeholder)
    std::string region;
    bool usePathStyle = false;
    std::string checksumAlgorithm;
    std::string sseKmsKeyId;
    std::string storageClass;
};

/**
 * Abstract interface for storage backends
 * Implementations can be local filesystem, S3, HTTP, FTP, etc.
 */
class IStorageBackend {
public:
    virtual ~IStorageBackend() = default;

    /**
     * Initialize the backend with configuration
     */
    virtual Result<void> initialize(const BackendConfig& config) = 0;

    /**
     * Store data with the given key
     */
    virtual Result<void> store(std::string_view key, std::span<const std::byte> data) = 0;

    /**
     * Retrieve data for the given key
     */
    virtual Result<std::vector<std::byte>> retrieve(std::string_view key) const = 0;

    /**
     * Check if a key exists
     */
    virtual Result<bool> exists(std::string_view key) const = 0;

    /**
     * Remove data for the given key
     */
    virtual Result<void> remove(std::string_view key) = 0;

    /**
     * List all keys with optional prefix filter
     */
    virtual Result<std::vector<std::string>> list(std::string_view prefix = "") const = 0;

    /**
     * Get storage statistics
     */
    virtual Result<::yams::StorageStats> getStats() const = 0;

    /**
     * Async store operation
     */
    virtual std::future<Result<void>> storeAsync(std::string_view key,
                                                 std::span<const std::byte> data) = 0;

    /**
     * Async retrieve operation
     */
    virtual std::future<Result<std::vector<std::byte>>>
    retrieveAsync(std::string_view key) const = 0;

    /**
     * Get backend type identifier
     */
    virtual std::string getType() const = 0;

    /**
     * Check if backend is remote (network-based)
     */
    virtual bool isRemote() const = 0;

    /**
     * Flush any pending operations or caches
     */
    virtual Result<void> flush() = 0;
};

/**
 * Local filesystem backend implementation
 */
class FilesystemBackend : public IStorageBackend {
public:
    FilesystemBackend() = default;

    Result<void> initialize(const BackendConfig& config) override;
    Result<void> store(std::string_view key, std::span<const std::byte> data) override;
    Result<std::vector<std::byte>> retrieve(std::string_view key) const override;
    Result<bool> exists(std::string_view key) const override;
    Result<void> remove(std::string_view key) override;
    Result<std::vector<std::string>> list(std::string_view prefix) const override;
    Result<::yams::StorageStats> getStats() const override;

    std::future<Result<void>> storeAsync(std::string_view key,
                                         std::span<const std::byte> data) override;
    std::future<Result<std::vector<std::byte>>> retrieveAsync(std::string_view key) const override;

    std::string getType() const override { return "filesystem"; }
    bool isRemote() const override { return false; }
    Result<void> flush() override { return {}; }

private:
    std::filesystem::path basePath_;
    std::filesystem::path getObjectPath(std::string_view key) const;
    Result<void> ensureDirectoryExists(const std::filesystem::path& path) const;
};

/**
 * URL-based backend implementation (S3, HTTP, FTP)
 * Uses libcurl for network operations
 */
class URLBackend : public IStorageBackend {
public:
    URLBackend();
    ~URLBackend();

    Result<void> initialize(const BackendConfig& config) override;
    Result<void> store(std::string_view key, std::span<const std::byte> data) override;
    Result<std::vector<std::byte>> retrieve(std::string_view key) const override;
    Result<bool> exists(std::string_view key) const override;
    Result<void> remove(std::string_view key) override;
    Result<std::vector<std::string>> list(std::string_view prefix) const override;
    Result<::yams::StorageStats> getStats() const override;

    std::future<Result<void>> storeAsync(std::string_view key,
                                         std::span<const std::byte> data) override;
    std::future<Result<std::vector<std::byte>>> retrieveAsync(std::string_view key) const override;

    std::string getType() const override;
    bool isRemote() const override { return true; }
    Result<void> flush() override;

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * Factory for creating storage backends
 */
class StorageBackendFactory {
public:
    /**
     * Create a backend from configuration
     * Automatically detects type from URL scheme or explicit type
     */
    static std::unique_ptr<IStorageBackend> create(const BackendConfig& config);

    /**
     * Parse a storage URL and create appropriate backend config
     * Examples:
     *   - /path/to/local/storage -> filesystem backend
     *   - s3://bucket/prefix -> S3 backend
     *   - http://server/path -> HTTP backend
     *   - ftp://server/path -> FTP backend
     */
    static BackendConfig parseURL(const std::string& url);

    /**
     * Register a custom backend implementation
     */
    static void registerBackend(const std::string& type,
                                std::function<std::unique_ptr<IStorageBackend>()> factory);

    template <typename Backend,
              typename = std::enable_if_t<std::is_base_of_v<IStorageBackend, Backend>>>
    static void registerBackendType(const std::string& type) {
        registerBackend(type, []() { return std::make_unique<Backend>(); });
    }
};

} // namespace yams::storage