#include <spdlog/spdlog.h>
#include <yams/chunking/chunker.h>
#include <yams/crypto/hasher.h>

#if defined(YAMS_HAS_STD_FORMAT) && YAMS_HAS_STD_FORMAT
#include <format>
namespace yamsfmt = std;
#else
#include <spdlog/fmt/fmt.h>
namespace yamsfmt = fmt;
#endif

#include <algorithm>
#include <fstream>
#include <limits>
#include <unordered_set>

namespace yams::chunking {

struct RabinChunker::Impl {
    ProgressCallback progressCallback;
};

RabinChunker::RabinChunker(ChunkingConfig config)
    : pImpl(std::make_unique<Impl>()), config_(std::move(config)),
      cutter_(config_.minChunkSize, config_.targetChunkSize, config_.maxChunkSize) {
    spdlog::debug("Created RabinChunker (gear CDC) with target chunk size: {}",
                  config_.targetChunkSize);
}

RabinChunker::~RabinChunker() = default;

RabinChunker::RabinChunker(RabinChunker&&) noexcept = default;
RabinChunker& RabinChunker::operator=(RabinChunker&&) noexcept = default;

std::vector<Chunk> RabinChunker::chunkData(std::span<const std::byte> data) {
    return chunkDataImpl(data, /*lazy=*/false);
}

std::vector<Chunk> RabinChunker::chunkDataLazy(std::span<const std::byte> data) {
    return chunkDataImpl(data, /*lazy=*/true);
}

std::vector<Chunk> RabinChunker::chunkDataImpl(std::span<const std::byte> data, bool lazy) {
    std::vector<Chunk> chunks;
    if (config_.targetChunkSize > 0) {
        chunks.reserve((data.size() / config_.targetChunkSize) + 1);
    }

    size_t pos = 0;
    while (pos < data.size()) {
        const size_t chunkSize = cutter_.nextCut(data.subspan(pos));
        const size_t chunkEnd = pos + chunkSize;

        chunks.emplace_back();
        Chunk& chunk = chunks.back();
        chunk.offset = pos;
        chunk.size = chunkSize;
        auto chunkSpan = data.subspan(pos, chunkSize);
        chunk.hash = crypto::SHA256Hasher::hash(chunkSpan);
        if (!lazy) {
            chunk.data.assign(chunkSpan.begin(), chunkSpan.end());
        }
        pos = chunkEnd;

        // Report progress if callback is set
        if (pImpl->progressCallback) {
            pImpl->progressCallback(pos, data.size());
        }
    }

    spdlog::debug("Chunked {} bytes into {} chunks", data.size(), chunks.size());
    return chunks;
}

std::vector<Chunk> RabinChunker::chunkFile(const std::filesystem::path& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        throw std::runtime_error(yamsfmt::format("Failed to open file: {}", path.string()));
    }

    // Get file size
    file.seekg(0, std::ios::end);
    const auto endPos = file.tellg();
    if (endPos < std::streampos{0}) {
        throw std::runtime_error("Failed to determine file size");
    }
    size_t fileSize = static_cast<size_t>(endPos);
    file.seekg(0, std::ios::beg);

    // Read entire file into memory (for now - streaming version would be better for large files)
    std::vector<std::byte> data(fileSize);
    const auto maxStream = static_cast<size_t>(std::numeric_limits<std::streamsize>::max());
    const auto toRead = static_cast<std::streamsize>(std::min(fileSize, maxStream));
    file.read(reinterpret_cast<char*>(data.data()), toRead);

    if (!file) {
        throw std::runtime_error("Failed to read file");
    }

    return chunkData(data);
}

std::future<Result<std::vector<Chunk>>>
RabinChunker::chunkFileAsync(const std::filesystem::path& path) {
    return std::async(std::launch::async, [this, path]() -> Result<std::vector<Chunk>> {
        try {
            return chunkFile(path);
        } catch (const std::exception& e) {
            spdlog::error("Failed to chunk file {}: {}", path.string(), e.what());
            return Result<std::vector<Chunk>>(ErrorCode::FileNotFound);
        }
    });
}

void RabinChunker::setProgressCallback(ProgressCallback callback) {
    pImpl->progressCallback = std::move(callback);
}

std::unique_ptr<IChunker> createRabinChunker(ChunkingConfig config) {
    return std::make_unique<RabinChunker>(std::move(config));
}

DeduplicationStats calculateDeduplication(const std::vector<Chunk>& chunks) {
    DeduplicationStats stats;
    std::unordered_set<Hash> uniqueHashes;

    for (const auto& chunk : chunks) {
        stats.totalSize += chunk.size;
        stats.chunkCount++;

        if (uniqueHashes.insert(chunk.hash).second) {
            stats.uniqueSize += chunk.size;
            stats.uniqueChunks++;
        }
    }

    return stats;
}

} // namespace yams::chunking
