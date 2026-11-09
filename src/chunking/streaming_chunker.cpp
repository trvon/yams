#include <spdlog/spdlog.h>
#include <yams/chunking/streaming_chunker.h>
#include <yams/crypto/hasher.h>

#include <algorithm>
#include <fstream>

namespace yams::chunking {

StreamingChunker::StreamingChunker(ChunkingConfig config)
    : config_(std::move(config)),
      cutter_(config_.minChunkSize, config_.targetChunkSize, config_.maxChunkSize) {
    spdlog::debug("Created StreamingChunker (gear CDC) with target chunk size: {}",
                  config_.targetChunkSize);
}

StreamingChunker::~StreamingChunker() = default;

StreamingChunker::StreamingChunker(StreamingChunker&&) noexcept = default;
StreamingChunker& StreamingChunker::operator=(StreamingChunker&&) noexcept = default;

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
