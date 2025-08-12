#include <yams/chunking/chunker.h>
#include <yams/crypto/hasher.h>
#include <spdlog/spdlog.h>
#if defined(YAMS_HAS_STD_FORMAT) && YAMS_HAS_STD_FORMAT
#include <format>
namespace yamsfmt = std;
#else
#include <spdlog/fmt/fmt.h>
namespace yamsfmt = fmt;
#endif

#include <algorithm>
#include <fstream>
#include <unordered_set>

namespace yams::chunking {

// Precomputed polynomial powers for Rabin fingerprinting
struct RabinTables {
    std::array<uint64_t, 256> outTable{};
    std::array<std::array<uint64_t, 256>, 64> modTable{};
    
    explicit RabinTables(uint64_t polynomial) {
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
        
        // Initialize modulus tables
        for (int i = 0; i < 64; ++i) {
            for (int j = 0; j < 256; ++j) {
                modTable[i][j] = modPow(j, i, polynomial);
            }
        }
    }
    
private:
    static uint64_t modPow(uint64_t base, uint64_t exp, uint64_t poly) {
        uint64_t result = 1;
        base %= poly;
        while (exp > 0) {
            if (exp & 1) {
                result = (result * base) % poly;
            }
            exp >>= 1;
            base = (base * base) % poly;
        }
        return result;
    }
};

struct RabinChunker::Impl {
    std::unique_ptr<RabinTables> tables;
    ProgressCallback progressCallback;
    std::unique_ptr<crypto::IContentHasher> hasher;
    
    explicit Impl(uint64_t polynomial) 
        : tables(std::make_unique<RabinTables>(polynomial)),
          hasher(crypto::createSHA256Hasher()) {}
};

RabinChunker::RabinChunker(ChunkingConfig config) 
    : pImpl(std::make_unique<Impl>(config.polynomial)),
      config_(std::move(config)) {
    spdlog::debug("Created RabinChunker with target chunk size: {}", config_.targetChunkSize);
}

RabinChunker::~RabinChunker() = default;

RabinChunker::RabinChunker(RabinChunker&&) noexcept = default;
RabinChunker& RabinChunker::operator=(RabinChunker&&) noexcept = default;

void RabinChunker::updateHash(RabinWindow& window, std::byte newByte) {
    // Remove oldest byte from window
    std::byte oldByte = window.window[window.pos];
    
    // Update window
    window.window[window.pos] = newByte;
    window.pos = (window.pos + 1) % config_.windowSize;
    
    // Update hash using precomputed tables
    uint64_t oldHash = pImpl->tables->outTable[static_cast<uint8_t>(oldByte)];
    uint64_t newHash = pImpl->tables->outTable[static_cast<uint8_t>(newByte)];
    
    window.hash = ((window.hash - oldHash) << 8) ^ newHash;
}

std::pair<size_t, bool> RabinChunker::findChunkBoundary(
    std::span<const std::byte> data, 
    size_t start,
    RabinWindow& window) {
    
    size_t pos = start;
    size_t chunkStart = start;
    
    // Skip to minimum chunk size
    size_t minBoundary = std::min(chunkStart + config_.minChunkSize, data.size());
    while (pos < minBoundary && pos < data.size()) {
        updateHash(window, data[pos++]);
    }
    
    // Look for chunk boundary
    size_t maxBoundary = std::min(chunkStart + config_.maxChunkSize, data.size());
    while (pos < maxBoundary) {
        updateHash(window, data[pos]);
        
        // Check if we've found a boundary
        if ((window.hash & config_.chunkMask) == config_.chunkMask) {
            return {pos + 1, true};
        }
        pos++;
    }
    
    // Forced boundary at max chunk size or end of data
    return {pos, false};
}

std::vector<Chunk> RabinChunker::chunkData(std::span<const std::byte> data) {
    std::vector<Chunk> chunks;
    RabinWindow window{};
    size_t pos = 0;
    
    while (pos < data.size()) {
        auto [chunkEnd, foundBoundary] = findChunkBoundary(data, pos, window);
        size_t chunkSize = chunkEnd - pos;
        
        // Create chunk
        Chunk chunk;
        chunk.offset = pos;
        chunk.size = chunkSize;
        chunk.data.assign(data.begin() + pos, data.begin() + chunkEnd);
        chunk.hash = pImpl->hasher->hash(chunk.data);
        
        chunks.push_back(std::move(chunk));
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
    size_t fileSize = file.tellg();
    file.seekg(0, std::ios::beg);
    
    // Read entire file into memory (for now - streaming version would be better for large files)
    std::vector<std::byte> data(fileSize);
    file.read(reinterpret_cast<char*>(data.data()), fileSize);
    
    if (!file) {
        throw std::runtime_error("Failed to read file");
    }
    
    return chunkData(data);
}

std::future<Result<std::vector<Chunk>>> RabinChunker::chunkFileAsync(
    const std::filesystem::path& path) {
    
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

// Note: std::generator requires C++23 or coroutine TS
// std::generator<Chunk> RabinChunker::chunkStream(std::span<const std::byte> data) {
//     RabinWindow window{};
//     size_t pos = 0;
//     
//     while (pos < data.size()) {
//         auto [chunkEnd, foundBoundary] = findChunkBoundary(data, pos, window);
//         size_t chunkSize = chunkEnd - pos;
//         
//         // Create and yield chunk
//         Chunk chunk;
//         chunk.offset = pos;
//         chunk.size = chunkSize;
//         chunk.data.assign(data.begin() + pos, data.begin() + chunkEnd);
//         chunk.hash = pImpl->hasher->hash(chunk.data);
//         
//         co_yield chunk;
//         pos = chunkEnd;
//     }
// }

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