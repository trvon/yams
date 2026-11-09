#pragma once

#include <yams/chunking/gear_cutter.h>
#include <yams/core/concepts.h>
#include <yams/core/span.h>
#include <yams/core/types.h>
#include <yams/crypto/hasher.h>

#include <array>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <vector>

namespace yams::chunking {

// Chunk information
struct Chunk {
    std::vector<std::byte> data;
    Hash hash;
    size_t offset;
    size_t size;

    // Comparison operators
    bool operator==(const Chunk&) const = default;
    bool operator<(const Chunk& other) const {
        return std::tie(offset, size, hash) < std::tie(other.offset, other.size, other.hash);
    }
};

// Lightweight chunk reference (no data)
struct ChunkRef {
    Hash hash;
    size_t offset;
    size_t size;

    bool operator==(const ChunkRef&) const = default;
    bool operator<(const ChunkRef& other) const {
        return std::tie(offset, size, hash) < std::tie(other.offset, other.size, other.hash);
    }
};

// Chunking configuration. Boundaries are content-defined by a gear rolling hash (see
// GearCutter): no cut before minChunkSize, a forced cut at maxChunkSize, and an expected average
// chunk size close to targetChunkSize (roughly minChunkSize + 1.1 * 2^round(log2(target - min))).
struct ChunkingConfig {
    size_t windowSize = 48; ///< Unused since the gear hash (fixed 64-byte window); kept for API.
    size_t minChunkSize = MIN_CHUNK_SIZE;
    size_t targetChunkSize = DEFAULT_CHUNK_SIZE;
    size_t maxChunkSize = MAX_CHUNK_SIZE;
    uint64_t polynomial = 0x3DA3358B4DC173LL; ///< Unused since the gear hash; kept for API.
    uint64_t chunkMask = 0x1FFF;              ///< Unused: the mask is derived from targetChunkSize.
};

// Concepts for chunking
template <typename T>
concept ChunkableData = requires(T t) {
    { yams::span<const std::byte>{t} } -> std::convertible_to<yams::span<const std::byte>>;
};

template <typename T>
concept ChunkProcessor = requires(T t, const ChunkRef& chunk, yams::span<const std::byte> data) {
    { t(chunk, data) } -> std::same_as<void>;
};

// Interface for content chunkers
class IChunker {
public:
    virtual ~IChunker() = default;

    // Get current configuration
    virtual const ChunkingConfig& getConfig() const = 0;

    // Chunk a file
    virtual std::vector<Chunk> chunkFile(const std::filesystem::path& path) = 0;

    // Chunk data in memory
    virtual std::vector<Chunk> chunkData(yams::span<const std::byte> data) = 0;

    // Lightweight variant: produces chunks with offset/size/hash but empty data vectors.
    // Caller must use the source span to access chunk bytes.
    // Default implementation falls back to chunkData().
    virtual std::vector<Chunk> chunkDataLazy(yams::span<const std::byte> data) {
        return chunkData(data);
    }

    // Async file chunking
    virtual std::future<Result<std::vector<Chunk>>>
    chunkFileAsync(const std::filesystem::path& path) = 0;

    // Progress callback support
    using ProgressCallback = std::function<void(uint64_t, uint64_t)>;
    virtual void setProgressCallback(ProgressCallback callback) = 0;
};

// Content-defined chunker over an in-memory buffer. The name is historical: boundaries come from
// the gear rolling hash in GearCutter, shared with StreamingChunker so both produce identical
// boundaries for the same input and config.
class RabinChunker : public IChunker {
public:
    explicit RabinChunker(ChunkingConfig config = {});
    ~RabinChunker();

    // Disable copy, enable move
    RabinChunker(const RabinChunker&) = delete;
    RabinChunker& operator=(const RabinChunker&) = delete;
    RabinChunker(RabinChunker&&) noexcept;
    RabinChunker& operator=(RabinChunker&&) noexcept;

    const ChunkingConfig& getConfig() const override { return config_; }

    std::vector<Chunk> chunkFile(const std::filesystem::path& path) override;
    std::vector<Chunk> chunkData(yams::span<const std::byte> data) override;

    // Lightweight variant: produces chunks with offset/size/hash but empty data vectors.
    // Caller must use the source span to access chunk bytes: data.subspan(chunk.offset,
    // chunk.size). Avoids per-chunk heap allocation when the source buffer outlives the chunks.
    std::vector<Chunk> chunkDataLazy(yams::span<const std::byte> data) override;

    std::future<Result<std::vector<Chunk>>>
    chunkFileAsync(const std::filesystem::path& path) override;

    void setProgressCallback(ProgressCallback callback) override;

    // Generator-based chunking for memory efficiency
    // Note: std::generator requires C++23 or coroutine TS
    // std::generator<Chunk> chunkStream(yams::span<const std::byte> data);

private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;
    ChunkingConfig config_;
    GearCutter cutter_;

    // Shared implementation for chunkData/chunkDataLazy
    std::vector<Chunk> chunkDataImpl(yams::span<const std::byte> data, bool lazy);
};

// Factory function
std::unique_ptr<IChunker> createRabinChunker(ChunkingConfig config = {});

// Utility to calculate deduplication ratio
struct DeduplicationStats {
    size_t totalSize = 0;
    size_t uniqueSize = 0;
    size_t chunkCount = 0;
    size_t uniqueChunks = 0;
};

DeduplicationStats calculateDeduplication(const std::vector<Chunk>& chunks);

} // namespace yams::chunking
