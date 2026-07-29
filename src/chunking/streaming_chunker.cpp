#include <spdlog/spdlog.h>
#include <yams/chunking/streaming_chunker.h>
#include <yams/core/assert.hpp>
#include <yams/crypto/hasher.h>

#include "rabin_fingerprint_table.h"

#include <algorithm>
#include <fstream>

namespace yams::chunking {

struct StreamingChunker::Impl {
    std::unique_ptr<detail::RabinFingerprintTable> tables;

    explicit Impl(uint64_t polynomial)
        : tables(std::make_unique<detail::RabinFingerprintTable>(polynomial)) {}
};

StreamingChunker::StreamingChunker(ChunkingConfig config)
    : pImpl(std::make_unique<Impl>(config.polynomial != 0 ? config.polynomial
                                                          : detail::kDefaultRabinPolynomial)),
      config_(std::move(config)) {
    if (config_.polynomial == 0) {
        spdlog::warn("StreamingChunker polynomial was zero; using default polynomial");
        config_.polynomial = detail::kDefaultRabinPolynomial;
    }
    YAMS_PRECONDITION(config_.windowSize > 0, "StreamingChunker windowSize must be positive");
    spdlog::debug("Created StreamingChunker with target chunk size: {}", config_.targetChunkSize);
}

StreamingChunker::~StreamingChunker() = default;

StreamingChunker::StreamingChunker(StreamingChunker&&) noexcept = default;
StreamingChunker& StreamingChunker::operator=(StreamingChunker&&) noexcept = default;

void StreamingChunker::updateRabinHash(RabinState& state, std::byte newByte) {
    if (!state.initialized) {
        // Initialize window with zeros
        std::fill(state.window.begin(), state.window.end(), std::byte{0});
        state.initialized = true;
    }

    size_t windowSize = config_.windowSize;
    if (windowSize == 0) {
        windowSize = 1;
    } else if (windowSize > state.window.size()) {
        windowSize = state.window.size();
    }
    if (state.windowPos >= windowSize) {
        state.windowPos = 0;
    }

    // Remove oldest byte from window
    std::byte oldByte = state.window[state.windowPos];

    // Update window
    state.window[state.windowPos] = newByte;
    ++state.windowPos;
    if (state.windowPos >= windowSize) {
        state.windowPos = 0;
    }

    // Update hash using precomputed tables
    uint64_t oldHash = pImpl->tables->outTable[static_cast<uint8_t>(oldByte)];
    uint64_t newHash = pImpl->tables->outTable[static_cast<uint8_t>(newByte)];

    state.hash = ((state.hash - oldHash) << 8) ^ newHash;
}

std::vector<Chunk> StreamingChunker::chunkFile(const std::filesystem::path& path) {
    std::vector<Chunk> chunks;

    auto result =
        processFileStream(path, [&chunks](const ChunkRef& ref, std::span<const std::byte> data) {
            Chunk chunk;
            chunk.hash = ref.hash;
            chunk.offset = ref.offset;
            chunk.size = ref.size;
            chunk.data.assign(data.begin(), data.end());
            chunks.push_back(std::move(chunk));
        });

    if (!result.has_value()) {
        throw std::runtime_error("Failed to chunk file: " + result.error().message);
    }

    spdlog::debug("Chunked file {} into {} chunks", path.string(), chunks.size());
    return chunks;
}

std::vector<Chunk> StreamingChunker::chunkData(std::span<const std::byte> data) {
    std::vector<Chunk> chunks;
    StreamingContext ctx;
    ctx.totalSize = data.size();
    ctx.hasher = crypto::createSHA256Hasher();

    // Process data in simulated "buffers" for consistency with streaming approach
    constexpr size_t BUFFER_SIZE = static_cast<size_t>(64) * static_cast<size_t>(1024);
    size_t offset = 0;

    while (offset < data.size()) {
        size_t chunkSize = std::min(BUFFER_SIZE, data.size() - offset);
        auto buffer = data.subspan(offset, chunkSize);

        auto result = processBuffer(
            buffer, ctx, [&chunks](const ChunkRef& ref, std::span<const std::byte> chunkData) {
                Chunk chunk;
                chunk.hash = ref.hash;
                chunk.offset = ref.offset;
                chunk.size = ref.size;
                chunk.data.assign(chunkData.begin(), chunkData.end());
                chunks.push_back(std::move(chunk));
            });

        if (!result.has_value()) {
            throw std::runtime_error("Failed to chunk data: " + result.error().message);
        }

        offset += chunkSize;
    }

    // Process any remaining data
    if (!ctx.accumulator.empty()) {
        finalizeChunk(ctx, [&chunks](const ChunkRef& ref, std::span<const std::byte> chunkData) {
            Chunk chunk;
            chunk.hash = ref.hash;
            chunk.offset = ref.offset;
            chunk.size = ref.size;
            chunk.data.assign(chunkData.begin(), chunkData.end());
            chunks.push_back(std::move(chunk));
        });
    }

    spdlog::debug("Chunked {} bytes into {} chunks", data.size(), chunks.size());
    return chunks;
}

std::future<Result<std::vector<Chunk>>>
StreamingChunker::chunkFileAsync(const std::filesystem::path& path) {
    return std::async(std::launch::async, [this, path]() -> Result<std::vector<Chunk>> {
        try {
            return chunkFile(path);
        } catch (const std::exception& e) {
            spdlog::error("Failed to chunk file {}: {}", path.string(), e.what());
            return Result<std::vector<Chunk>>(
                Error{ErrorCode::FileNotFound, "Failed to chunk file: " + std::string(e.what())});
        }
    });
}

void StreamingChunker::setProgressCallback(ProgressCallback callback) {
    progressCallback_ = std::move(callback);
}

std::unique_ptr<IChunker> createStreamingChunker(ChunkingConfig config) {
    return std::make_unique<StreamingChunker>(std::move(config));
}

} // namespace yams::chunking
