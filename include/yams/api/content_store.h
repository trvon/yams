#pragma once

#include <yams/core/types.h>
#include <yams/api/content_metadata.h>
#include <yams/api/progress_reporter.h>

#include <chrono>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace yams::api {

// Forward declarations
struct ContentStoreStats;

// Store operation result
struct StoreResult {
    std::string contentHash;               // SHA-256 hash of the content
    uint64_t bytesStored = 0;             // Total bytes stored
    uint64_t bytesDeduped = 0;            // Bytes saved by deduplication
    std::chrono::milliseconds duration{0}; // Operation duration
    
    // Calculate deduplication ratio
    [[nodiscard]] double dedupRatio() const noexcept {
        return bytesStored > 0 ? 
            static_cast<double>(bytesDeduped) / bytesStored : 0.0;
    }
};

// Retrieve operation result
struct RetrieveResult {
    bool found = false;                    // Whether content was found
    uint64_t size = 0;                     // Size of retrieved content
    ContentMetadata metadata;              // Associated metadata
    std::chrono::milliseconds duration{0}; // Operation duration
};

// Content store statistics
struct ContentStoreStats {
    uint64_t totalObjects = 0;             // Total unique objects
    uint64_t totalBytes = 0;               // Total bytes stored
    uint64_t uniqueBlocks = 0;             // Unique blocks in storage
    uint64_t deduplicatedBytes = 0;        // Bytes saved by dedup
    uint64_t storeOperations = 0;          // Total store operations
    uint64_t retrieveOperations = 0;       // Total retrieve operations
    uint64_t deleteOperations = 0;         // Total delete operations
    TimePoint lastOperation;               // Last operation timestamp
    
    // Calculate deduplication ratio
    [[nodiscard]] double dedupRatio() const noexcept {
        return totalBytes > 0 ? 
            1.0 - (static_cast<double>(uniqueBlocks) / totalBytes) : 0.0;
    }
};

// Health status for the content store
struct HealthStatus {
    bool isHealthy = true;
    std::string status = "OK";
    std::vector<std::string> warnings;
    std::vector<std::string> errors;
    TimePoint lastCheck;
};

// Main content store interface
class IContentStore {
public:
    virtual ~IContentStore() = default;
    
    // File-based operations
    virtual Result<StoreResult> store(
        const std::filesystem::path& path,
        const ContentMetadata& metadata = {},
        ProgressCallback progress = nullptr) = 0;
    
    virtual Result<RetrieveResult> retrieve(
        const std::string& hash,
        const std::filesystem::path& outputPath,
        ProgressCallback progress = nullptr) = 0;
    
    // Stream-based operations
    virtual Result<StoreResult> storeStream(
        std::istream& stream,
        const ContentMetadata& metadata = {},
        ProgressCallback progress = nullptr) = 0;
    
    virtual Result<RetrieveResult> retrieveStream(
        const std::string& hash,
        std::ostream& output,
        ProgressCallback progress = nullptr) = 0;
    
    // Memory-based operations
    virtual Result<StoreResult> storeBytes(
        std::span<const std::byte> data,
        const ContentMetadata& metadata = {}) = 0;
    
    virtual Result<std::vector<std::byte>> retrieveBytes(
        const std::string& hash) = 0;
    
    // Management operations
    virtual Result<bool> exists(const std::string& hash) const = 0;
    virtual Result<bool> remove(const std::string& hash) = 0;
    virtual Result<ContentMetadata> getMetadata(const std::string& hash) const = 0;
    virtual Result<void> updateMetadata(
        const std::string& hash, 
        const ContentMetadata& metadata) = 0;
    
    // Batch operations
    virtual std::vector<Result<StoreResult>> storeBatch(
        const std::vector<std::filesystem::path>& paths,
        const std::vector<ContentMetadata>& metadata = {}) = 0;
    
    virtual std::vector<Result<bool>> removeBatch(
        const std::vector<std::string>& hashes) = 0;
    
    // Statistics and health
    virtual ContentStoreStats getStats() const = 0;
    virtual HealthStatus checkHealth() const = 0;
    
    // Maintenance operations
    virtual Result<void> verify(ProgressCallback progress = nullptr) = 0;
    virtual Result<void> compact(ProgressCallback progress = nullptr) = 0;
    virtual Result<void> garbageCollect(ProgressCallback progress = nullptr) = 0;
};

// Configuration for content store
struct ContentStoreConfig {
    std::filesystem::path storagePath;     // Base storage path
    size_t chunkSize = DEFAULT_CHUNK_SIZE; // Chunk size for deduplication
    bool enableCompression = true;         // Enable compression
    std::string compressionType = "zstd";  // Compression algorithm
    size_t compressionLevel = 3;           // Compression level
    bool enableDeduplication = true;       // Enable deduplication
    size_t maxConcurrentOps = 10;          // Max concurrent operations
    bool enableIntegrityChecks = true;     // Enable integrity verification
    std::chrono::seconds gcInterval{3600}; // Garbage collection interval
    
    // Validate configuration
    [[nodiscard]] Result<void> validate() const {
        if (storagePath.empty()) {
            return Result<void>(ErrorCode::InvalidArgument);
        }
        
        if (chunkSize < MIN_CHUNK_SIZE || chunkSize > MAX_CHUNK_SIZE) {
            return Result<void>(ErrorCode::InvalidArgument);
        }
        
        if (maxConcurrentOps == 0) {
            return Result<void>(ErrorCode::InvalidArgument);
        }
        
        return Result<void>();
    }
};

} // namespace yams::api