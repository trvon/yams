#include <spdlog/spdlog.h>
#include <yams/core/atomic_utils.h>
#include <yams/storage/storage_engine.h>
#if defined(YAMS_HAS_STD_FORMAT) && YAMS_HAS_STD_FORMAT
#include <format>
namespace yamsfmt = std;
#else
#include <spdlog/fmt/fmt.h>
namespace yamsfmt = fmt;
#endif

#include <algorithm>
#include <array>
#include <fstream>
#include <random>
#include <source_location>
#include <thread>

namespace yams::storage {

// Constants
constexpr size_t TEMP_NAME_LENGTH = 16;
// Removed unused retry helpers; reintroduce if backoff is implemented

// Implementation details
struct StorageEngine::Impl {
    StorageConfig config;
    mutable std::shared_mutex globalMutex;
    mutable StorageStats stats;

    // Mutex pool for per-hash write synchronization
    struct MutexPool {
        std::vector<std::unique_ptr<std::mutex>> mutexes;

        explicit MutexPool(size_t size) {
            mutexes.reserve(size);
            for (size_t i = 0; i < size; ++i) {
                mutexes.emplace_back(std::make_unique<std::mutex>());
            }
        }

        std::mutex& getMutex(std::string_view hash) {
            auto hashValue = std::hash<std::string_view>{}(hash);
            auto index = hashValue % mutexes.size();
            return *mutexes[index];
        }
    } writeMutexPool;

    explicit Impl(StorageConfig cfg)
        : config(std::move(cfg)), writeMutexPool(config.mutexPoolSize) {
        // Initialize storage directory
        std::error_code ec;
        std::filesystem::create_directories(config.basePath / "objects", ec);
        if (ec) {
            throw std::runtime_error(
                yamsfmt::format("Failed to create storage directory: {}", ec.message()));
        }

        std::filesystem::create_directories(config.basePath / "temp", ec);
        if (ec) {
            throw std::runtime_error(
                yamsfmt::format("Failed to create temp directory: {}", ec.message()));
        }
    }
};

StorageEngine::StorageEngine(StorageConfig config)
    : pImpl(std::make_unique<Impl>(std::move(config))) {
    spdlog::debug("Initialized storage engine at: {}", pImpl->config.basePath.string());
}

StorageEngine::~StorageEngine() = default;

StorageEngine::StorageEngine(StorageEngine&&) noexcept = default;
StorageEngine& StorageEngine::operator=(StorageEngine&&) noexcept = default;

std::filesystem::path StorageEngine::getObjectPath(std::string_view hash) const {
    // For manifest files, use a different path structure
    if (hash.ends_with(".manifest")) {
        // Store manifests in manifests/ab/cdef...manifest
        auto baseHash = hash.substr(0, hash.length() - 9); // Remove ".manifest"
        if (baseHash.length() < pImpl->config.shardDepth) {
            throw std::invalid_argument("Hash too short for sharding");
        }
        auto prefix = baseHash.substr(0, pImpl->config.shardDepth);
        auto suffix = baseHash.substr(pImpl->config.shardDepth);
        return pImpl->config.basePath / "manifests" / std::string(prefix) /
               (std::string(suffix) + ".manifest");
    }

    if (hash.length() < pImpl->config.shardDepth) {
        throw std::invalid_argument("Hash too short for sharding");
    }

    // Create sharded path: objects/ab/cdef...
    auto prefix = hash.substr(0, pImpl->config.shardDepth);
    auto suffix = hash.substr(pImpl->config.shardDepth);

    return pImpl->config.basePath / "objects" / std::string(prefix) / std::string(suffix);
}

std::filesystem::path StorageEngine::getTempPath() const {
    // Generate random temp filename
    static thread_local std::random_device rd;
    static thread_local std::mt19937 gen(rd());
    static thread_local std::uniform_int_distribution<> dis(0, 15);

    std::string tempName;
    tempName.reserve(TEMP_NAME_LENGTH);

    for (size_t i = 0; i < TEMP_NAME_LENGTH; ++i) {
        tempName += yamsfmt::format("{:x}", dis(gen));
    }

    return pImpl->config.basePath / "temp" / tempName;
}

Result<void> StorageEngine::ensureDirectoryExists(const std::filesystem::path& path) const {
    std::error_code ec;
    std::filesystem::create_directories(path.parent_path(), ec);

    if (ec) {
        spdlog::error("Failed to create directory {}: {}", path.parent_path().string(),
                      ec.message());
        return Result<void>(ErrorCode::PermissionDenied);
    }

    return {};
}

Result<void> StorageEngine::atomicWrite(const std::filesystem::path& path,
                                        std::span<const std::byte> data) {
    // Ensure parent directory exists
    if (auto result = ensureDirectoryExists(path); !result) {
        return result;
    }

    // Write to temporary file
    auto tempPath = getTempPath();

    {
        std::ofstream file(tempPath, std::ios::binary);
        if (!file) {
            spdlog::error("Failed to create temp file: {}", tempPath.string());
            return Result<void>(ErrorCode::PermissionDenied);
        }

        file.write(reinterpret_cast<const char*>(data.data()),
                   static_cast<std::streamsize>(data.size()));
        if (!file) {
            std::filesystem::remove(tempPath);
            return Result<void>(ErrorCode::Unknown);
        }
    }

    // Atomic rename
    std::error_code ec;
    std::filesystem::rename(tempPath, path, ec);

    if (ec) {
        // Clean up temp file
        std::filesystem::remove(tempPath);

        // Check if file already exists (not an error for content-addressed storage)
        if (std::filesystem::exists(path)) {
            return {};
        }

        spdlog::error("Failed to rename {} to {}: {}", tempPath.string(), path.string(),
                      ec.message());
        return Result<void>(ErrorCode::Unknown);
    }

    return {};
}

std::filesystem::path StorageEngine::getBasePath() const {
    return pImpl->config.basePath;
}

Result<void> StorageEngine::store(std::string_view hash, std::span<const std::byte> data) {
    // Allow manifest keys (hash.manifest) and regular hashes
    bool isManifest = hash.ends_with(".manifest");
    if (!isManifest && hash.length() != HASH_STRING_SIZE) {
        spdlog::error("Invalid hash length for store: expected {} characters, got {} for hash '{}'",
                      HASH_STRING_SIZE, hash.length(), hash);
        return Result<void>(ErrorCode::InvalidArgument);
    }

    auto objectPath = getObjectPath(hash);

    // Quick existence check without locking
    if (std::filesystem::exists(objectPath)) {
        spdlog::debug("Object {} already exists", hash);
        return {};
    }

    // Acquire per-hash write lock
    auto& hashMutex = pImpl->writeMutexPool.getMutex(hash);
    std::lock_guard<std::mutex> lock(hashMutex);

    // Double-check after acquiring lock
    if (std::filesystem::exists(objectPath)) {
        return {};
    }

    // Perform atomic write
    auto result = atomicWrite(objectPath, data);

    if (result) {
        // Update statistics
        pImpl->stats.totalObjects.fetch_add(1);
        pImpl->stats.totalBytes.fetch_add(data.size());
        pImpl->stats.writeOperations.fetch_add(1);

        spdlog::debug("Stored object {} ({} bytes)", hash, data.size());
    } else {
        pImpl->stats.failedOperations.fetch_add(1);
    }

    return result;
}

Result<IStorageEngine::RawObject> StorageEngine::retrieveRaw(std::string_view hash) const {
    // Allow manifest keys (hash.manifest) and regular hashes
    bool isManifest = hash.ends_with(".manifest");
    if (!isManifest && hash.length() != HASH_STRING_SIZE) {
        spdlog::error(
            "Invalid hash length for retrieve: expected {} characters, got {} for hash '{}'",
            HASH_STRING_SIZE, hash.length(), hash);
        return Result<IStorageEngine::RawObject>(ErrorCode::InvalidArgument);
    }

    auto objectPath = getObjectPath(hash);

    // Check existence
    if (!std::filesystem::exists(objectPath)) {
        return Result<IStorageEngine::RawObject>(ErrorCode::ChunkNotFound);
    }

    // Read file
    std::ifstream file(objectPath, std::ios::binary | std::ios::ate);
    if (!file) {
        pImpl->stats.failedOperations.fetch_add(1);
        return Result<IStorageEngine::RawObject>(ErrorCode::PermissionDenied);
    }

    auto fileSize = file.tellg();
    file.seekg(0, std::ios::beg);

    std::vector<std::byte> data(static_cast<size_t>(fileSize));
    file.read(reinterpret_cast<char*>(data.data()), fileSize);

    if (!file) {
        pImpl->stats.failedOperations.fetch_add(1);
        return Result<IStorageEngine::RawObject>(ErrorCode::CorruptedData);
    }

    // Update statistics
    pImpl->stats.readOperations.fetch_add(1);

    IStorageEngine::RawObject obj;
    obj.data = std::move(data);
    obj.header = std::nullopt;
    return obj;
}

Result<std::vector<std::byte>> StorageEngine::retrieve(std::string_view hash) const {
    auto rawResult = retrieveRaw(hash);
    if (!rawResult) {
        return rawResult.error();
    }
    return std::move(rawResult.value().data);
}

Result<bool> StorageEngine::exists(std::string_view hash) const noexcept {
    try {
        // Allow manifest keys (hash.manifest) and regular hashes
        bool isManifest = hash.ends_with(".manifest");
        if (!isManifest && hash.length() != HASH_STRING_SIZE) {
            spdlog::error(
                "Invalid hash length for exists: expected {} characters, got {} for hash '{}'",
                HASH_STRING_SIZE, hash.length(), hash);
            return Result<bool>(ErrorCode::InvalidArgument);
        }

        auto objectPath = getObjectPath(hash);

        // Check existence
        return std::filesystem::exists(objectPath);

    } catch (const std::exception& e) {
        spdlog::error("Error checking existence of {}: {}", hash, e.what());
        return Result<bool>(ErrorCode::Unknown);
    }
}

Result<void> StorageEngine::remove(std::string_view hash) {
    // Allow manifest keys (hash.manifest) and regular hashes
    bool isManifest = hash.ends_with(".manifest");
    if (!isManifest && hash.length() != HASH_STRING_SIZE) {
        spdlog::error(
            "Invalid hash length for remove: expected {} characters, got {} for hash '{}'",
            HASH_STRING_SIZE, hash.length(), hash);
        return Result<void>(ErrorCode::InvalidArgument);
    }

    auto objectPath = getObjectPath(hash);

    // Acquire per-hash write lock
    auto& hashMutex = pImpl->writeMutexPool.getMutex(hash);
    std::lock_guard<std::mutex> lock(hashMutex);

    // Check existence
    if (!std::filesystem::exists(objectPath)) {
        return Result<void>(ErrorCode::ChunkNotFound);
    }

    // Get file size before deletion
    auto fileSize = std::filesystem::file_size(objectPath);

    // Remove file
    std::error_code ec;
    if (!std::filesystem::remove(objectPath, ec)) {
        pImpl->stats.failedOperations.fetch_add(1);
        return Result<void>(ErrorCode::PermissionDenied);
    }

    // Update statistics using saturating subtraction to prevent underflow
    core::saturating_sub(pImpl->stats.totalObjects, uint64_t{1});
    core::saturating_sub(pImpl->stats.totalBytes, static_cast<uint64_t>(fileSize));
    pImpl->stats.deleteOperations.fetch_add(1);

    spdlog::debug("Removed object {} ({} bytes)", hash, fileSize);

    return {};
}

std::future<Result<void>> StorageEngine::storeAsync(std::string_view hash,
                                                    std::span<const std::byte> data) {
    // Create a copy of the data for async operation
    std::vector<std::byte> dataCopy(data.begin(), data.end());
    std::string hashCopy(hash);

    return std::async(std::launch::async,
                      [this, hash = std::move(hashCopy), data = std::move(dataCopy)]() {
                          return store(hash, data);
                      });
}

std::future<Result<std::vector<std::byte>>>
StorageEngine::retrieveAsync(std::string_view hash) const {
    std::string hashCopy(hash);

    return std::async(std::launch::async,
                      [this, hash = std::move(hashCopy)]() { return retrieve(hash); });
}

std::future<Result<IStorageEngine::RawObject>>
StorageEngine::retrieveRawAsync(std::string_view hash) const {
    std::string hashCopy(hash);

    return std::async(std::launch::async,
                      [this, hash = std::move(hashCopy)]() { return retrieveRaw(hash); });
}

StorageStats StorageEngine::getStats() const noexcept {
    return pImpl->stats;
}

Result<uint64_t> StorageEngine::getStorageSize() const {
    try {
        uint64_t totalSize = 0;

        for (const auto& entry :
             std::filesystem::recursive_directory_iterator(pImpl->config.basePath / "objects")) {
            if (entry.is_regular_file()) {
                totalSize += entry.file_size();
            }
        }

        return totalSize;

    } catch (const std::exception& e) {
        spdlog::error("Failed to calculate storage size: {}", e.what());
        return Result<uint64_t>(ErrorCode::Unknown);
    }
}

Result<void> StorageEngine::verify() const {
    spdlog::debug("Starting storage verification...");

    size_t verifiedCount = 0;
    size_t errorCount = 0;

    try {
        for (const auto& entry :
             std::filesystem::recursive_directory_iterator(pImpl->config.basePath / "objects")) {
            if (entry.is_regular_file()) {
                // TODO: Verify file integrity by recalculating hash
                verifiedCount++;
            }
        }

        spdlog::debug("Storage verification complete: {} objects verified, {} errors",
                      verifiedCount, errorCount);

        return errorCount == 0 ? Result<void>{} : Result<void>(ErrorCode::CorruptedData);

    } catch (const std::exception& e) {
        spdlog::error("Storage verification failed: {}", e.what());
        return Result<void>(ErrorCode::Unknown);
    }
}

Result<void> StorageEngine::compact() {
    // TODO: Implement storage compaction
    spdlog::debug("Storage compaction not yet implemented");
    return {};
}

Result<void> StorageEngine::cleanupTempFiles() {
    spdlog::debug("Cleaning up temporary files...");

    size_t cleanedCount = 0;
    std::error_code ec;

    try {
        auto tempDir = pImpl->config.basePath / "temp";

        for (const auto& entry : std::filesystem::directory_iterator(tempDir)) {
            if (entry.is_regular_file()) {
                // Remove temp files older than 1 hour
                auto lastWrite = entry.last_write_time();
                auto now = std::filesystem::file_time_type::clock::now();
                auto age = now - lastWrite;

                if (age > std::chrono::hours(1)) {
                    std::filesystem::remove(entry.path(), ec);
                    if (!ec) {
                        cleanedCount++;
                    }
                }
            }
        }

        spdlog::debug("Cleaned up {} temporary files", cleanedCount);
        return {};

    } catch (const std::exception& e) {
        spdlog::error("Temp file cleanup failed: {}", e.what());
        return Result<void>(ErrorCode::Unknown);
    }
}

// AtomicFileWriter implementation
std::filesystem::path
AtomicFileWriter::generateTempName(const std::filesystem::path& target) const {
    auto timestamp = std::chrono::system_clock::now().time_since_epoch().count();
    std::random_device rngDevice;
    std::mt19937 gen(rngDevice());
    constexpr int RAND_ID_MIN = 1000;
    constexpr int RAND_ID_MAX = 9999;
    std::uniform_int_distribution<> dist(RAND_ID_MIN, RAND_ID_MAX);
    return {yamsfmt::format("{}.tmp.{}.{}", target.string(), timestamp, dist(gen))};
}

auto AtomicFileWriter::writeImpl(const std::filesystem::path& path, std::span<const std::byte> data)
    -> Result<void> {
    auto tempPath = generateTempName(path);

    // RAII cleanup guard
    struct TempFileGuard {
        std::filesystem::path path;
        bool success = false;

        ~TempFileGuard() {
            if (!success) {
                std::error_code removeError;
                std::filesystem::remove(path, removeError);
            }
        }
    } guard{.path = tempPath};

    // Write to temp file
    {
        std::ofstream file(tempPath, std::ios::binary);
        if (!file) {
            return Result<void>(ErrorCode::PermissionDenied);
        }

        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): Required by ofstream::write
        file.write(reinterpret_cast<const char*>(data.data()),
                   static_cast<std::streamsize>(data.size()));
        if (!file) {
            return {ErrorCode::Unknown};
        }
    }

    // Atomic rename
    std::error_code renameError;
    std::filesystem::rename(tempPath, path, renameError);

    if (renameError) {
        return {ErrorCode::Unknown};
    }

    guard.success = true;
    return {};
}

auto AtomicFileWriter::writeBatch(
    std::vector<std::pair<std::filesystem::path, std::vector<std::byte>>> items)
    -> std::future<std::vector<Result<void>>> {
    return std::async(std::launch::async, [this, items = std::move(items)]() {
        std::vector<Result<void>> results;
        results.reserve(items.size());

        for (const auto& [path, data] : items) {
            results.push_back(writeImpl(path, data));
        }

        return results;
    });
}

// Factory function
auto createStorageEngine(StorageConfig config) -> std::unique_ptr<IStorageEngine> {
    return std::make_unique<StorageEngine>(std::move(config));
}

// Utility functions
auto initializeStorage(const std::filesystem::path& basePath) -> Result<void> {
    try {
        std::error_code errorCode;

        // Create directory structure
        std::filesystem::create_directories(basePath / "objects", errorCode);
        if (errorCode) {
            return {ErrorCode::PermissionDenied};
        }

        std::filesystem::create_directories(basePath / "temp", errorCode);
        if (errorCode) {
            return {ErrorCode::PermissionDenied};
        }

        std::filesystem::create_directories(basePath / "manifests", errorCode);
        if (errorCode) {
            return {ErrorCode::PermissionDenied};
        }

        spdlog::debug("Initialized storage at: {}", basePath.string());
        return {};

    } catch (const std::exception& e) {
        spdlog::error("Failed to initialize storage: {}", e.what());
        return {ErrorCode::Unknown};
    }
}

auto validateStorageIntegrity(const std::filesystem::path& basePath) -> Result<bool> {
    try {
        // Check required directories exist
        if (!std::filesystem::exists(basePath / "objects") ||
            !std::filesystem::exists(basePath / "temp")) {
            return false;
        }

        // TODO: Additional integrity checks

        return true;

    } catch (const std::exception& e) {
        spdlog::error("Storage validation failed: {}", e.what());
        return {ErrorCode::Unknown};
    }
}

} // namespace yams::storage
