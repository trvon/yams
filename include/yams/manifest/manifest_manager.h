#pragma once

#include <yams/chunking/chunker.h>
#include <yams/core/concepts.h>
#include <yams/core/types.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <numeric>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace yams::manifest {

// Forward declarations
struct Manifest;
struct ChunkRef;
class ManifestSerializer;

// Concepts for manifest operations
template <typename T>
concept ManifestSerializable = requires(T t, std::span<const std::byte> data) {
    { t.serialize() } -> std::convertible_to<std::vector<std::byte>>;
    { T::deserialize(data) } -> std::same_as<Result<T>>;
};

template <typename T>
concept ChunkProvider = requires(T t, const std::string& hash) {
    { t.getChunk(hash) } -> std::convertible_to<Result<std::vector<std::byte>>>;
    { t.hasChunk(hash) } -> std::convertible_to<bool>;
};

template <typename T>
concept ChunkConsumer = requires(T t, const std::string& hash, std::span<const std::byte> data) {
    { t.storeChunk(hash, data) } -> std::convertible_to<Result<void>>;
};

// Chunk reference in manifest
struct ChunkRef {
    std::string hash;   // SHA-256 hash
    uint64_t offset;    // Offset in original file
    uint32_t size;      // Chunk size in bytes
    uint32_t flags = 0; // Reserved flags

    // C++20 spaceship operator for comparisons
    auto operator<=>(const ChunkRef&) const = default;

    // Validation
    [[nodiscard]] bool isValid() const noexcept {
        return hash.length() == HASH_STRING_SIZE && size > 0;
    }
};

// File manifest structure
struct Manifest {
    static constexpr uint32_t CURRENT_VERSION = 1;

    // Core metadata
    uint32_t version = CURRENT_VERSION;
    std::string fileHash;                             // Complete file hash
    uint64_t fileSize = 0;                            // Total file size
    std::string originalName;                         // Original filename
    std::string mimeType;                             // MIME type
    std::chrono::system_clock::time_point createdAt;  // Creation time
    std::chrono::system_clock::time_point modifiedAt; // Modification time

    // Chunk mapping
    std::vector<ChunkRef> chunks;

    // Extensible metadata
    std::unordered_map<std::string, std::string> metadata;

    // Integrity
    uint32_t checksum = 0;            // CRC32 checksum
    std::string compression = "none"; // Compression type
    uint32_t uncompressedSize = 0;    // Size before compression

    // C++20 spaceship operator (excluding metadata which doesn't support it)
    auto operator<=>(const Manifest& other) const {
        return std::tie(version, fileHash, fileSize, originalName, mimeType, createdAt, modifiedAt,
                        chunks, checksum, compression, uncompressedSize) <=>
               std::tie(other.version, other.fileHash, other.fileSize, other.originalName,
                        other.mimeType, other.createdAt, other.modifiedAt, other.chunks,
                        other.checksum, other.compression, other.uncompressedSize);
    }

    // Equality comparison
    bool operator==(const Manifest& other) const = default;

    // Validation using ranges
    [[nodiscard]] bool isValid() const noexcept {
        return !fileHash.empty() && fileHash.length() == HASH_STRING_SIZE && !chunks.empty() &&
               fileSize > 0 && std::all_of(chunks.begin(), chunks.end(), [](const auto& chunk) {
                   return chunk.isValid();
               });
    }

    // Calculate total size from chunks
    [[nodiscard]] uint64_t calculateTotalSize() const noexcept {
        return std::accumulate(chunks.begin(), chunks.end(), uint64_t{0},
                               [](uint64_t acc, const auto& chunk) { return acc + chunk.size; });
    }

    // Get chunk count
    [[nodiscard]] size_t getChunkCount() const noexcept { return chunks.size(); }
};

// Manifest statistics
struct ManifestStats {
    size_t totalManifests = 0;
    size_t totalChunks = 0;
    uint64_t totalFileSize = 0;
    uint64_t totalManifestSize = 0;
    double avgChunksPerFile = 0.0;
    double avgManifestSize = 0.0;
    std::chrono::milliseconds avgSerializationTime{0};
    std::chrono::milliseconds avgDeserializationTime{0};
};

// Chunk provider interface
class IChunkProvider {
public:
    virtual ~IChunkProvider() = default;
    virtual Result<std::vector<std::byte>> getChunk(const std::string& hash) const = 0;
};

// Main manifest manager interface
class IManifestManager {
public:
    virtual ~IManifestManager() = default;

    // Core operations
    virtual Result<Manifest> createManifest(const FileInfo& fileInfo,
                                            const std::vector<ChunkRef>& chunks) = 0;

    virtual Result<std::vector<std::byte>> serialize(const Manifest& manifest) const = 0;
    virtual Result<Manifest> deserialize(std::span<const std::byte> data) const = 0;

    // File reconstruction
    virtual Result<void> reconstructFile(const Manifest& manifest,
                                         const std::filesystem::path& outputPath,
                                         IChunkProvider& provider) const = 0;

    // Async operations
    virtual std::future<Result<void>> reconstructFileAsync(const Manifest& manifest,
                                                           const std::filesystem::path& outputPath,
                                                           IChunkProvider& provider) const = 0;

    // Validation
    virtual Result<bool> validateManifest(const Manifest& manifest) const = 0;
    virtual Result<bool> verifyFileIntegrity(const Manifest& manifest,
                                             const std::filesystem::path& filePath) const = 0;

    // Statistics
    virtual ManifestStats getStats() const = 0;
};

// Concrete manifest manager implementation
class ManifestManager : public IManifestManager {
public:
    // Configuration options
    struct Config {
        bool enableCompression = true;
        std::string compressionAlgorithm = "zstd";
        bool enableChecksums = true;
        size_t maxChunksPerManifest = 1000000; // 1M chunks
        bool enableCaching = true;
        size_t cacheSize = 1000; // Number of manifests to cache
    };

    explicit ManifestManager(Config config);
    ~ManifestManager();

    // Disable copy, enable move
    ManifestManager(const ManifestManager&) = delete;
    ManifestManager& operator=(const ManifestManager&) = delete;
    ManifestManager(ManifestManager&&) noexcept;
    ManifestManager& operator=(ManifestManager&&) noexcept;

    // Create manifest using ranges
    template <std::ranges::input_range R>
    requires std::same_as<std::ranges::range_value_t<R>, chunking::Chunk>
    Result<Manifest> createManifest(const FileInfo& fileInfo, R&& chunks) {
        Manifest manifest{.fileHash = fileInfo.hash,
                          .fileSize = fileInfo.size,
                          .originalName = fileInfo.originalName,
                          .mimeType = fileInfo.mimeType,
                          .createdAt = fileInfo.createdAt,
                          .modifiedAt = std::chrono::system_clock::now(),
                          .chunks = {},
                          .metadata = {}};

        // Transform chunks to references using ranges
        namespace rv = std::ranges::views;
        auto refs = chunks | rv::transform([](const auto& chunk) {
                        return ChunkRef{.hash = chunk.hash,
                                        .offset = chunk.offset,
                                        .size = static_cast<uint32_t>(chunk.size)};
                    });

        // Verify chunk count doesn't exceed limit
        auto chunkCount = std::ranges::distance(chunks);
        if (chunkCount > config_.maxChunksPerManifest) {
            return Result<Manifest>(ErrorCode::InvalidArgument);
        }

        // Assign chunks and validate
        manifest.chunks.assign(refs.begin(), refs.end());

        // Calculate checksum if enabled
        if (config_.enableChecksums) {
            manifest.checksum = calculateChecksum(manifest);
        }

        // Update statistics (same as in non-template version)
        updateManifestCreationStats();

        return manifest.isValid() ? Result<Manifest>(std::move(manifest))
                                  : Result<Manifest>(ErrorCode::InvalidArgument);
    }

    Result<Manifest> createManifest(const FileInfo& fileInfo,
                                    const std::vector<ChunkRef>& chunks) override;

    // Serialization
    Result<std::vector<std::byte>> serialize(const Manifest& manifest) const override;
    Result<Manifest> deserialize(std::span<const std::byte> data) const override;

    // File reconstruction

    Result<void> reconstructFile(const Manifest& manifest, const std::filesystem::path& outputPath,
                                 IChunkProvider& provider) const override;

    // Async reconstruction

    std::future<Result<void>> reconstructFileAsync(const Manifest& manifest,
                                                   const std::filesystem::path& outputPath,
                                                   IChunkProvider& provider) const override;

    // Validation
    Result<bool> validateManifest(const Manifest& manifest) const override;
    Result<bool> verifyFileIntegrity(const Manifest& manifest,
                                     const std::filesystem::path& filePath) const override;

    // Statistics
    ManifestStats getStats() const override;

    // Utility methods
    static std::string detectMimeType(const std::filesystem::path& path);
    static Result<FileInfo> getFileInfo(const std::filesystem::path& path);

protected:
    // Helper method for template functions to update statistics
    void updateManifestCreationStats();

private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;
    Config config_;

    // Internal helpers
    uint32_t calculateChecksum(const Manifest& manifest) const;
    Result<std::vector<std::byte>> compressData(std::span<const std::byte> data) const;
    Result<std::vector<std::byte>> decompressData(std::span<const std::byte> data) const;
};

// Factory function
std::unique_ptr<IManifestManager> createManifestManager(ManifestManager::Config config = {});

// Utility functions for batch operations
Result<std::vector<Manifest>> deserializeManifestBatch(std::span<const std::byte> data);
Result<std::vector<std::byte>> serializeManifestBatch(std::ranges::input_range auto&& manifests);

// Stream processing for large manifests
template <std::ranges::input_range R>
requires std::same_as<std::ranges::range_value_t<R>, ChunkRef>
class ManifestStream {
public:
    explicit ManifestStream(R&& chunks) : chunks_(std::forward<R>(chunks)) {}

    // Process chunks in batches for memory efficiency
    template <typename F> void processBatches(size_t batchSize, F&& processor) {
        // Manual chunking for C++20 compatibility
        std::vector<typename std::ranges::range_value_t<R>> batch;
        batch.reserve(batchSize);

        for (auto&& chunk : chunks_) {
            batch.push_back(chunk);
            if (batch.size() == batchSize) {
                processor(batch);
                batch.clear();
            }
        }

        // Process remaining batch
        if (!batch.empty()) {
            processor(batch);
        }
    }

private:
    R chunks_;
};

} // namespace yams::manifest
