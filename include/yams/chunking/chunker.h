#pragma once

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

// Chunking configuration
struct ChunkingConfig {
    size_t windowSize = 48;
    size_t minChunkSize = MIN_CHUNK_SIZE;
    size_t targetChunkSize = DEFAULT_CHUNK_SIZE;
    size_t maxChunkSize = MAX_CHUNK_SIZE;
    uint64_t polynomial = 0x3DA3358B4DC173LL;
    uint64_t chunkMask = 0x1FFF; // For 64KB average chunks
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

    // Async file chunking
    virtual std::future<Result<std::vector<Chunk>>>
    chunkFileAsync(const std::filesystem::path& path) = 0;

    // Progress callback support
    using ProgressCallback = std::function<void(uint64_t, uint64_t)>;
    virtual void setProgressCallback(ProgressCallback callback) = 0;
};

// Rabin fingerprinting chunker
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

    std::future<Result<std::vector<Chunk>>>
    chunkFileAsync(const std::filesystem::path& path) override;

    void setProgressCallback(ProgressCallback callback) override;

    // Generator-based chunking for memory efficiency
    // Note: std::generator requires C++23 or coroutine TS
    // std::generator<Chunk> chunkStream(yams::span<const std::byte> data);

    // Process chunks without storing all data
    template <ChunkProcessor F>
    void processChunks(yams::span<const std::byte> data, F&& processor) {
        auto hasher = crypto::createSHA256Hasher();
        RabinWindow window;
        size_t pos = 0;

        while (pos < data.size()) {
            auto [chunkEnd, shouldBreak] = findChunkBoundary(data, pos, window);
            size_t chunkSize = chunkEnd - pos;

            // Create chunk reference
            auto chunkSpan = data.subspan(pos, chunkSize);
            auto hash = hasher->hash(chunkSpan);

            ChunkRef ref{.hash = std::move(hash), .offset = pos, .size = chunkSize};

            // Process chunk
            processor(ref, chunkSpan);

            pos = chunkEnd;
        }
    }

private:
    struct RabinWindow {
        std::array<std::byte, 48> window{};
        size_t pos = 0;
        uint64_t hash = 0;
    };

    struct Impl;
    std::unique_ptr<Impl> pImpl;
    ChunkingConfig config_;

    // Find next chunk boundary
    std::pair<size_t, bool> findChunkBoundary(yams::span<const std::byte> data, size_t start,
                                              RabinWindow& window);

    // Update rolling hash
    void updateHash(RabinWindow& window, std::byte newByte);
};

// Factory function
std::unique_ptr<IChunker> createRabinChunker(ChunkingConfig config = {});

// Utility to calculate deduplication ratio
struct DeduplicationStats {
    size_t totalSize = 0;
    size_t uniqueSize = 0;
    size_t chunkCount = 0;
    size_t uniqueChunks = 0;

    constexpr double getRatio() const {
        return totalSize > 0
                   ? 1.0 - (static_cast<double>(uniqueSize) / static_cast<double>(totalSize))
                   : 0.0;
    }
};

DeduplicationStats calculateDeduplication(const std::vector<Chunk>& chunks);

} // namespace yams::chunking
