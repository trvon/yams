#pragma once

#include <yams/core/concepts.h>
#include <yams/core/types.h>

#include <atomic>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <shared_mutex>
#include <span>
#include <string_view>
#include <thread>
#include <vector>

#include <yams/compression/compression_header.h>

namespace yams::storage {

// Forward declarations
class AtomicFileWriter;

// Storage engine configuration
struct StorageConfig {
    std::filesystem::path basePath;
    size_t shardDepth = 2;
    size_t mutexPoolSize = 1024;
    bool enableCompression = false;
    size_t maxConcurrentReaders = 1000;
    size_t maxConcurrentWriters = 100;
};

// Storage operation statistics
struct StorageStats {
    std::atomic<uint64_t> totalObjects{0};
    std::atomic<uint64_t> totalBytes{0};
    std::atomic<uint64_t> writeOperations{0};
    std::atomic<uint64_t> readOperations{0};
    std::atomic<uint64_t> deleteOperations{0};
    std::atomic<uint64_t> failedOperations{0};

    // Copy constructor
    StorageStats(const StorageStats& other) noexcept
        : totalObjects(other.totalObjects.load()), totalBytes(other.totalBytes.load()),
          writeOperations(other.writeOperations.load()),
          readOperations(other.readOperations.load()),
          deleteOperations(other.deleteOperations.load()),
          failedOperations(other.failedOperations.load()) {}

    // Default constructor
    StorageStats() = default;

    // Calculate deduplication ratio
    double getDeduplicationRatio() const noexcept {
        auto total = totalBytes.load();
        return total > 0 ? 1.0 - (static_cast<double>(totalObjects) / total) : 0.0;
    }
};

// Storage engine interface
class IStorageEngine {
public:
    virtual ~IStorageEngine() = default;

    struct RawObject {
        std::vector<std::byte> data;
        std::optional<compression::CompressionHeader> header;
    };

    // Core operations
    virtual Result<void> store(std::string_view hash, std::span<const std::byte> data) = 0;
    virtual Result<std::vector<std::byte>> retrieve(std::string_view hash) const = 0;
    virtual Result<RawObject> retrieveRaw(std::string_view hash) const = 0;
    virtual Result<bool> exists(std::string_view hash) const noexcept = 0;
    virtual Result<void> remove(std::string_view hash) = 0;

    // Async operations
    virtual std::future<Result<void>> storeAsync(std::string_view hash,
                                                 std::span<const std::byte> data) = 0;
    virtual std::future<Result<std::vector<std::byte>>>
    retrieveAsync(std::string_view hash) const = 0;
    virtual std::future<Result<RawObject>> retrieveRawAsync(std::string_view hash) const = 0;

    // Batch operations
    virtual std::vector<Result<void>>
    storeBatch(const std::vector<std::pair<std::string, std::vector<std::byte>>>& items) = 0;

    // Statistics
    virtual StorageStats getStats() const noexcept = 0;
    virtual Result<uint64_t> getStorageSize() const = 0;
};

// Main storage engine implementation
class StorageEngine : public IStorageEngine {
public:
    explicit StorageEngine(StorageConfig config);
    ~StorageEngine();

    // Delete copy, enable move
    StorageEngine(const StorageEngine&) = delete;
    StorageEngine& operator=(const StorageEngine&) = delete;
    StorageEngine(StorageEngine&&) noexcept;
    StorageEngine& operator=(StorageEngine&&) noexcept;

    // Core operations
    Result<void> store(std::string_view hash, std::span<const std::byte> data) override;
    Result<std::vector<std::byte>> retrieve(std::string_view hash) const override;
    Result<RawObject> retrieveRaw(std::string_view hash) const override;
    Result<bool> exists(std::string_view hash) const noexcept override;
    Result<void> remove(std::string_view hash) override;

    // Async operations with C++20 coroutines
    std::future<Result<void>> storeAsync(std::string_view hash,
                                         std::span<const std::byte> data) override;

    std::future<Result<std::vector<std::byte>>> retrieveAsync(std::string_view hash) const override;
    std::future<Result<RawObject>> retrieveRawAsync(std::string_view hash) const override;

    // Batch operations implementation
    std::vector<Result<void>>
    storeBatch(const std::vector<std::pair<std::string, std::vector<std::byte>>>& items) override {
        std::vector<Result<void>> results;
        results.reserve(items.size());

        for (const auto& [hash, data] : items) {
            results.push_back(store(hash, data));
        }

        return results;
    }
    // Expose base storage path (useful for wrappers to log the true root)
    std::filesystem::path getBasePath() const;

    // Statistics and maintenance
    StorageStats getStats() const noexcept override;
    Result<uint64_t> getStorageSize() const override;

    // Maintenance operations
    Result<void> verify() const;
    Result<void> compact();
    Result<void> cleanupTempFiles();

private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;

    // Path management
    [[nodiscard]] std::filesystem::path getObjectPath(std::string_view hash) const;
    [[nodiscard]] std::filesystem::path getTempPath() const;

    // Internal operations
    Result<void> ensureDirectoryExists(const std::filesystem::path& path) const;
    Result<void> atomicWrite(const std::filesystem::path& path, std::span<const std::byte> data);
};

// Atomic file writer for safe concurrent writes
class AtomicFileWriter {
public:
    AtomicFileWriter() = default;
    ~AtomicFileWriter() = default;

    // Write data atomically using rename
    template <typename T>
    [[nodiscard]] Result<void> write(const std::filesystem::path& path, const T& data) {
        using Elem = std::remove_reference_t<decltype(*data.data())>;
        auto rawSpan = std::span<const Elem>(data.data(), data.size());
        return writeImpl(path, std::as_bytes(rawSpan));
    }

    // Batch atomic writes
    std::future<std::vector<Result<void>>>
    writeBatch(std::vector<std::pair<std::filesystem::path, std::vector<std::byte>>> items);

private:
    Result<void> writeImpl(const std::filesystem::path& path, std::span<const std::byte> data);

    // Generate unique temporary filename
    [[nodiscard]] std::filesystem::path generateTempName(const std::filesystem::path& target) const;
};

// Factory function
std::unique_ptr<IStorageEngine> createStorageEngine(StorageConfig config);

// Utility functions
Result<void> initializeStorage(const std::filesystem::path& basePath);
Result<bool> validateStorageIntegrity(const std::filesystem::path& basePath);

} // namespace yams::storage
