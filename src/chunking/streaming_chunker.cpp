#include <spdlog/spdlog.h>
#include <yams/chunking/streaming_chunker.h>
#include <yams/crypto/hasher.h>

#include <algorithm>
#include <array>
#include <fstream>

namespace yams::chunking {

// Rabin fingerprinting tables (local to streaming chunker to avoid unity ODR conflicts)
struct StreamingRabinTables {
    std::array<uint64_t, 256> outTable{};
    std::array<std::array<uint64_t, 256>, 64> modTable{};

    explicit StreamingRabinTables(uint64_t polynomial) {
        // Initialize output table
        for (int i = 0; i < 256; ++i) {
            uint64_t hash = 0;
            for (int j = 0; j < 8; ++j) {
                if (i & (1 << j)) {
                    hash ^= polynomial << j;
                }
            }
            outTable[i] = hash;
        }

        // Initialize modulus tables for window operations
        for (int i = 0; i < 64; ++i) {
            for (int j = 0; j < 256; ++j) {
                modTable[i][j] = modPow(j, ExpTag{static_cast<size_t>(i)}, PolyTag{polynomial});
            }
        }
    }

private:
    struct ExpTag { size_t v; };
    struct PolyTag { uint64_t v; };
    static uint64_t modPow(uint64_t base, ExpTag exp, PolyTag poly) {
        uint64_t result = 1;
        base %= poly.v;
        while (exp.v > 0) {
            if (exp.v & 1) {
                result = (result * base) % poly.v;
            }
            exp.v >>= 1;
            base = (base * base) % poly.v;
        }
        return result;
    }
};

struct StreamingChunker::Impl {
    std::unique_ptr<StreamingRabinTables> tables;

    explicit Impl(uint64_t polynomial)
        : tables(std::make_unique<StreamingRabinTables>(polynomial)) {}
};

StreamingChunker::StreamingChunker(ChunkingConfig config)
    : pImpl(std::make_unique<Impl>(config.polynomial)), config_(std::move(config)) {
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

    // Remove oldest byte from window
    std::byte oldByte = state.window[state.windowPos];

    // Update window
    state.window[state.windowPos] = newByte;
    state.windowPos = (state.windowPos + 1) % config_.windowSize;

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
