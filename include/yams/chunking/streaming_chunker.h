#pragma once

#include <yams/chunking/chunker.h>
#include <yams/core/types.h>
#include <yams/crypto/hasher.h>

#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <span>

namespace yams::chunking {

/**
 * Streaming chunker that processes files without loading them entirely into memory.
 * This implementation reads files in small buffers and maintains only the necessary
 * state for Rabin fingerprinting across buffer boundaries.
 */
class StreamingChunker : public IChunker {
public:
    explicit StreamingChunker(ChunkingConfig config = {});
    ~StreamingChunker();

    // Disable copy, enable move
    StreamingChunker(const StreamingChunker&) = delete;
    StreamingChunker& operator=(const StreamingChunker&) = delete;
    StreamingChunker(StreamingChunker&&) noexcept;
    StreamingChunker& operator=(StreamingChunker&&) noexcept;

    const ChunkingConfig& getConfig() const override { return config_; }

    // File chunking - streaming implementation
    std::vector<Chunk> chunkFile(const std::filesystem::path& path) override;

    // Data chunking - processes in-memory data
    std::vector<Chunk> chunkData(std::span<const std::byte> data) override;

    // Async file chunking
    std::future<Result<std::vector<Chunk>>>
    chunkFileAsync(const std::filesystem::path& path) override;

    void setProgressCallback(ProgressCallback callback) override;

    /**
     * Process file in streaming fashion with custom chunk processor.
     * This allows processing chunks as they are identified without storing all chunks in memory.
     *
     * @param path File path to process
     * @param processor Function called for each chunk found
     * @return Result indicating success or error
     */
    template <ChunkProcessor F>
    Result<void> processFileStream(const std::filesystem::path& path, F&& processor) {
        std::ifstream file(path, std::ios::binary);
        if (!file) {
            return Result<void>(
                Error{ErrorCode::FileNotFound, "Failed to open file: " + path.string()});
        }

        // Get file size for progress reporting
        file.seekg(0, std::ios::end);
        auto pos = file.tellg();
        size_t fileSize = (pos == std::ios::pos_type(-1)) ? 0 : static_cast<size_t>(pos);
        file.seekg(0, std::ios::beg);

        return processStream(file, fileSize, std::forward<F>(processor));
    }

    /**
     * Process stream with custom chunk processor.
     *
     * @param stream Input stream to process
     * @param totalSize Total size for progress reporting (0 if unknown)
     * @param processor Function called for each chunk found
     * @return Result indicating success or error
     */
    template <ChunkProcessor F>
    Result<void> processStream(std::istream& stream, size_t totalSize, F&& processor) {
        StreamingContext ctx;
        ctx.totalSize = totalSize;
        ctx.hasher = crypto::createSHA256Hasher();

        // Buffer for reading data
        constexpr size_t BUFFER_SIZE = 64 * 1024; // 64KB buffer
        std::vector<std::byte> buffer(BUFFER_SIZE);

        while (stream.good()) {
            stream.read(reinterpret_cast<char*>(buffer.data()), BUFFER_SIZE);
            const std::streamsize readCount = stream.gcount();
            size_t bytesRead = readCount > 0 ? static_cast<size_t>(readCount) : 0u;

            if (bytesRead == 0)
                break;

            // Process this buffer
            auto result = processBuffer(std::span(buffer.data(), bytesRead), ctx, processor);

            if (!result.has_value()) {
                return result;
            }

            // Report progress
            if (progressCallback_ && totalSize > 0) {
                progressCallback_(ctx.fileOffset, totalSize);
            }
        }

        // Process any remaining data in accumulator
        if (!ctx.accumulator.empty()) {
            finalizeChunk(ctx, processor);
        }

        return Result<void>();
    }

private:
    struct RabinState {
        std::array<std::byte, 48> window{};
        size_t windowPos = 0;
        uint64_t hash = 0;
        bool initialized = false;
    };

    struct StreamingContext {
        RabinState rabin;
        std::vector<std::byte> accumulator;
        size_t fileOffset = 0;
        size_t currentChunkStart = 0;
        size_t totalSize = 0;
        std::unique_ptr<crypto::IContentHasher> hasher;
    };

    struct Impl;
    std::unique_ptr<Impl> pImpl;
    ChunkingConfig config_;
    ProgressCallback progressCallback_;

    // Process a buffer of data, potentially emitting chunks
    template <ChunkProcessor F>
    Result<void> processBuffer(std::span<const std::byte> buffer, StreamingContext& ctx,
                               F&& processor) {
        size_t bufferPos = 0;

        while (bufferPos < buffer.size()) {
            // Add byte to accumulator
            ctx.accumulator.push_back(buffer[bufferPos]);

            // Update Rabin hash
            updateRabinHash(ctx.rabin, buffer[bufferPos]);

            // Check if we should emit a chunk
            size_t chunkSize = ctx.accumulator.size();
            bool shouldEmit = false;

            if (chunkSize >= config_.minChunkSize) {
                // Check for natural boundary using Rabin hash
                if ((ctx.rabin.hash & config_.chunkMask) == config_.chunkMask) {
                    shouldEmit = true;
                } else if (chunkSize >= config_.maxChunkSize) {
                    // Force emit at max size
                    shouldEmit = true;
                }
            }

            if (shouldEmit) {
                emitChunk(ctx, processor);
            }

            bufferPos++;
            ctx.fileOffset++;
        }

        return Result<void>();
    }

    // Emit accumulated chunk
    template <ChunkProcessor F> void emitChunk(StreamingContext& ctx, F&& processor) {
        if (ctx.accumulator.empty())
            return;

        // Calculate hash
        auto hash = ctx.hasher->hash(ctx.accumulator);

        // Create chunk reference
        ChunkRef ref{.hash = std::move(hash),
                     .offset = ctx.currentChunkStart,
                     .size = ctx.accumulator.size()};

        // Process chunk
        processor(ref, std::span(ctx.accumulator));

        // Reset accumulator for next chunk
        ctx.currentChunkStart = ctx.fileOffset;
        ctx.accumulator.clear();
        ctx.accumulator.reserve(config_.targetChunkSize);
    }

    // Finalize any remaining data as a chunk
    template <ChunkProcessor F> void finalizeChunk(StreamingContext& ctx, F&& processor) {
        if (!ctx.accumulator.empty()) {
            emitChunk(ctx, processor);
        }
    }

    // Update Rabin rolling hash
    void updateRabinHash(RabinState& state, std::byte newByte);
};

/**
 * Factory function to create a streaming chunker
 */
std::unique_ptr<IChunker> createStreamingChunker(ChunkingConfig config = {});

} // namespace yams::chunking