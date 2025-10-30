/**
 * @file chunking_test.cpp
 * @brief Comprehensive chunking tests covering RabinChunker and StreamingChunker
 *
 * Migrated from GTest to Catch2 as part of PBI-068.
 * Combines rabin_chunker_test.cpp (5 tests) and streaming_chunker_test.cpp (10 tests).
 *
 * Test Coverage:
 * - RabinChunker: Content-defined chunking with rolling hash
 * - StreamingChunker: Stream-based chunking with file and memory support
 * - Chunk size constraints (min/target/max)
 * - Streaming vs batch equivalence
 * - Progress callbacks
 * - Error handling (empty files, non-existent files)
 * - Stream fragmentation resilience
 * - Async chunking
 */

#include <catch2/catch_test_macros.hpp>
#include <yams/chunking/chunker.h>
#include <yams/chunking/streaming_chunker.h>
#include <yams/crypto/hasher.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <random>
#include <span>
#include <sstream>
#include <streambuf>
#include <vector>

using namespace yams::chunking;

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * @brief Create a byte vector filled with a specific value
 */
static std::vector<std::byte> make_bytes(size_t n, uint8_t value = 0x42) {
    std::vector<std::byte> buf(n);
    std::memset(buf.data(), value, n);
    return buf;
}

/**
 * @brief Create deterministic pseudo-random data for reproducible tests
 */
static std::string makePatternData(size_t n) {
    std::string d;
    d.resize(n);
    for (size_t i = 0; i < n; ++i) {
        d[i] = static_cast<char>((i * 1315423911u + 0x9E3779B9u) & 0xFF);
    }
    return d;
}

/**
 * @brief Streambuf that segments reads into fixed-size chunks
 *
 * Simulates fragmented stream reads to test buffer management.
 */
class SegmentingStringBuf : public std::streambuf {
public:
    SegmentingStringBuf(const std::string& data, size_t segment)
        : data_(data), segment_(segment), pos_(0) {}

protected:
    std::streamsize xsgetn(char* s, std::streamsize count) override {
        if (pos_ >= data_.size())
            return 0;
        std::streamsize n =
            static_cast<std::streamsize>(std::min(segment_, static_cast<size_t>(count)));
        n = std::min<std::streamsize>(n, static_cast<std::streamsize>(data_.size() - pos_));
        std::memcpy(s, data_.data() + pos_, static_cast<size_t>(n));
        pos_ += static_cast<size_t>(n);
        return n;
    }

    int underflow() override { return traits_type::eof(); }

private:
    const std::string data_;
    const size_t segment_;
    size_t pos_;
};

// ============================================================================
// RabinChunker Tests
// ============================================================================

TEST_CASE("RabinChunker - Basic functionality", "[chunking][rabin]") {
    SECTION("Empty input produces no chunks") {
        std::vector<std::byte> empty;
        RabinChunker chunker;
        auto result = chunker.chunkData(empty);
        REQUIRE(result.empty());
    }

    SECTION("Single byte with unit chunk config") {
        auto data = make_bytes(1);
        ChunkingConfig cfg;
        cfg.minChunkSize = 1;
        cfg.targetChunkSize = 1;
        cfg.maxChunkSize = 1;

        RabinChunker chunker(cfg);
        auto result = chunker.chunkData(data);
        REQUIRE(result.size() == 1);
        REQUIRE(result[0].size == 1);
        REQUIRE(result[0].offset == 0);
    }

    SECTION("Fixed-size chunks at boundary") {
        // Exactly 10 bytes, min=max=10 => should produce 1 chunk
        auto data = make_bytes(10);
        ChunkingConfig cfg;
        cfg.minChunkSize = 10;
        cfg.targetChunkSize = 10;
        cfg.maxChunkSize = 10;

        RabinChunker chunker(cfg);
        auto result = chunker.chunkData(data);
        REQUIRE(result.size() == 1);
        REQUIRE(result[0].size == 10);
        REQUIRE(result[0].offset == 0);
    }
}

TEST_CASE("RabinChunker - Chunk invariants", "[chunking][rabin]") {
    auto data = make_bytes(1000);
    ChunkingConfig cfg;
    cfg.minChunkSize = 64;
    cfg.targetChunkSize = 256;
    cfg.maxChunkSize = 512;

    RabinChunker chunker(cfg);
    auto chunks = chunker.chunkData(data);

    REQUIRE_FALSE(chunks.empty());

    SECTION("Chunk sizes respect constraints") {
        for (const auto& c : chunks) {
            // Last chunk might be smaller than minChunkSize
            if (&c != &chunks.back()) {
                REQUIRE(c.size >= cfg.minChunkSize);
            }
            REQUIRE(c.size <= cfg.maxChunkSize);
        }
    }

    SECTION("Total coverage matches input") {
        size_t totalSize = 0;
        for (const auto& c : chunks) {
            totalSize += c.size;
        }
        REQUIRE(totalSize == data.size());
    }

    SECTION("Offsets are sequential and non-overlapping") {
        uint64_t expectedOffset = 0;
        for (const auto& c : chunks) {
            REQUIRE(c.offset == expectedOffset);
            expectedOffset += c.size;
        }
    }
}

TEST_CASE("RabinChunker - Streaming equivalence", "[chunking][rabin][streaming]") {
    // Verify that streaming chunker produces same results as non-streaming
    auto data = make_bytes(500);
    ChunkingConfig cfg;
    cfg.minChunkSize = 32;
    cfg.targetChunkSize = 128;
    cfg.maxChunkSize = 256;

    RabinChunker rabin(cfg);
    StreamingChunker streaming(cfg);

    auto rabinChunks = rabin.chunkData(data);
    auto streamingChunks = streaming.chunkData(data);

    REQUIRE(rabinChunks.size() == streamingChunks.size());

    for (size_t i = 0; i < rabinChunks.size(); ++i) {
        REQUIRE(rabinChunks[i].offset == streamingChunks[i].offset);
        REQUIRE(rabinChunks[i].size == streamingChunks[i].size);
        // Hashes might differ if not computed in RabinChunker, but sizes/offsets must match
    }
}

// ============================================================================
// StreamingChunker Tests
// ============================================================================

TEST_CASE("StreamingChunker - File operations", "[chunking][streaming]") {
    auto testDir = std::filesystem::temp_directory_path() / "yams_chunking_test";
    std::filesystem::create_directories(testDir);

    auto createTestFile = [&](size_t size, const std::string& name = "test.dat") {
        auto path = testDir / name;
        std::ofstream file(path, std::ios::binary);
        std::mt19937 gen(42);
        std::uniform_int_distribution<> dist(0, 255);
        for (size_t i = 0; i < size; ++i) {
            char byte = static_cast<char>(dist(gen));
            file.write(&byte, 1);
        }
        return path;
    };

    SECTION("Small file chunking") {
        auto filePath = createTestFile(1024);

        ChunkingConfig config;
        config.minChunkSize = 256;
        config.targetChunkSize = 512;
        config.maxChunkSize = 1024;

        StreamingChunker chunker(config);
        auto chunks = chunker.chunkFile(filePath);

        REQUIRE_FALSE(chunks.empty());

        size_t totalSize = 0;
        for (const auto& chunk : chunks) {
            totalSize += chunk.size;
            REQUIRE(chunk.size >= config.minChunkSize);
            REQUIRE(chunk.size <= config.maxChunkSize);
        }
        REQUIRE(totalSize == 1024);
    }

    SECTION("Large file chunking") {
        auto filePath = createTestFile(1024 * 1024); // 1MB

        ChunkingConfig config;
        config.minChunkSize = 4 * 1024;
        config.targetChunkSize = 64 * 1024;
        config.maxChunkSize = 128 * 1024;

        StreamingChunker chunker(config);
        auto chunks = chunker.chunkFile(filePath);

        REQUIRE(chunks.size() > 1);

        size_t totalSize = 0;
        for (const auto& chunk : chunks) {
            totalSize += chunk.size;

            if (&chunk != &chunks.back()) {
                REQUIRE(chunk.size >= config.minChunkSize);
            }
            REQUIRE(chunk.size <= config.maxChunkSize);
            REQUIRE_FALSE(chunk.hash.empty());
        }
        REQUIRE(totalSize == 1024 * 1024);
    }

    SECTION("Streaming vs in-memory equivalence") {
        auto filePath = createTestFile(100 * 1024); // 100KB

        ChunkingConfig config;
        config.minChunkSize = 2 * 1024;
        config.targetChunkSize = 8 * 1024;
        config.maxChunkSize = 16 * 1024;

        StreamingChunker chunker(config);
        auto streamingChunks = chunker.chunkFile(filePath);

        // Read file to memory
        std::ifstream file(filePath, std::ios::binary);
        file.seekg(0, std::ios::end);
        size_t fileSize = file.tellg();
        file.seekg(0, std::ios::beg);
        std::vector<std::byte> data(fileSize);
        file.read(reinterpret_cast<char*>(data.data()), fileSize);

        auto memoryChunks = chunker.chunkData(data);

        REQUIRE(streamingChunks.size() == memoryChunks.size());
        for (size_t i = 0; i < streamingChunks.size(); ++i) {
            REQUIRE(streamingChunks[i].hash == memoryChunks[i].hash);
            REQUIRE(streamingChunks[i].size == memoryChunks[i].size);
            REQUIRE(streamingChunks[i].offset == memoryChunks[i].offset);
        }
    }

    SECTION("Empty file produces no chunks") {
        auto filePath = createTestFile(0, "empty.dat");
        StreamingChunker chunker;
        auto chunks = chunker.chunkFile(filePath);
        REQUIRE(chunks.empty());
    }

    SECTION("Non-existent file throws exception") {
        StreamingChunker chunker;
        REQUIRE_THROWS_AS(chunker.chunkFile(testDir / "nonexistent.dat"), std::runtime_error);
    }

    // Cleanup
    std::filesystem::remove_all(testDir);
}

TEST_CASE("StreamingChunker - Stream processing", "[chunking][streaming]") {
    auto testDir = std::filesystem::temp_directory_path() / "yams_chunking_test";
    std::filesystem::create_directories(testDir);

    auto createTestFile = [&](size_t size, const std::string& name = "test.dat") {
        auto path = testDir / name;
        std::ofstream file(path, std::ios::binary);
        std::mt19937 gen(42);
        std::uniform_int_distribution<> dist(0, 255);
        for (size_t i = 0; i < size; ++i) {
            char byte = static_cast<char>(dist(gen));
            file.write(&byte, 1);
        }
        return path;
    };

    SECTION("Process file stream with callback") {
        auto filePath = createTestFile(50 * 1024); // 50KB

        ChunkingConfig config;
        config.minChunkSize = 1024;
        config.targetChunkSize = 4096;
        config.maxChunkSize = 8192;

        StreamingChunker chunker(config);

        std::vector<ChunkRef> refs;
        size_t totalProcessed = 0;

        auto result = chunker.processFileStream(
            filePath,
            [&refs, &totalProcessed](const ChunkRef& ref, std::span<const std::byte> data) {
                refs.push_back(ref);
                totalProcessed += data.size();
                REQUIRE(ref.size == data.size());
            });

        REQUIRE(result.has_value());
        REQUIRE_FALSE(refs.empty());
        REQUIRE(totalProcessed == 50 * 1024);
    }

    SECTION("Progress callback tracking") {
        auto filePath = createTestFile(10 * 1024); // 10KB

        ChunkingConfig config;
        config.minChunkSize = 512;
        config.targetChunkSize = 1024;
        config.maxChunkSize = 2048;

        StreamingChunker chunker(config);

        std::vector<std::pair<uint64_t, uint64_t>> progressUpdates;
        chunker.setProgressCallback([&progressUpdates](uint64_t current, uint64_t total) {
            progressUpdates.push_back({current, total});
        });

        auto chunks = chunker.chunkFile(filePath);

        REQUIRE_FALSE(progressUpdates.empty());

        // Progress should be monotonically increasing
        uint64_t lastProgress = 0;
        for (const auto& [current, total] : progressUpdates) {
            REQUIRE(current >= lastProgress);
            REQUIRE(total == 10 * 1024);
            lastProgress = current;
        }
    }

    // Cleanup
    std::filesystem::remove_all(testDir);
}

TEST_CASE("StreamingChunker - Async operations", "[chunking][streaming][async]") {
    auto testDir = std::filesystem::temp_directory_path() / "yams_chunking_test";
    std::filesystem::create_directories(testDir);

    auto filePath = testDir / "async_test.dat";
    std::ofstream file(filePath, std::ios::binary);
    std::mt19937 gen(42);
    std::uniform_int_distribution<> dist(0, 255);
    for (size_t i = 0; i < 5 * 1024; ++i) {
        char byte = static_cast<char>(dist(gen));
        file.write(&byte, 1);
    }
    file.close();

    StreamingChunker chunker;
    auto future = chunker.chunkFileAsync(filePath);

    auto result = future.get();
    REQUIRE(result.has_value());

    const auto& chunks = result.value();
    REQUIRE_FALSE(chunks.empty());

    size_t totalSize = 0;
    for (const auto& chunk : chunks) {
        totalSize += chunk.size;
    }
    REQUIRE(totalSize == 5 * 1024);

    // Cleanup
    std::filesystem::remove_all(testDir);
}

TEST_CASE("StreamingChunker - Stream fragmentation", "[chunking][streaming][robustness]") {
    SECTION("Multi-write fragmentation equivalence") {
        const size_t N = 256 * 1024 + 777; // Cross buffer boundaries
        auto data = makePatternData(N);

        ChunkingConfig cfg;
        cfg.minChunkSize = 4 * 1024;
        cfg.targetChunkSize = 32 * 1024;
        cfg.maxChunkSize = 64 * 1024;

        auto runWithSeg = [&](size_t seg) {
            StreamingChunker chunker(cfg);
            SegmentingStringBuf sb(data, seg);
            std::istream is(&sb);
            std::vector<ChunkRef> refs;
            auto r = chunker.processStream(
                is, data.size(), [&](const ChunkRef& ref, std::span<const std::byte> bytes) {
                    refs.push_back(ref);
                    REQUIRE(ref.size == bytes.size());
                });
            REQUIRE(r.has_value());
            return refs;
        };

        // Single read (all at once)
        auto refs1 = runWithSeg(data.size());
        // Two segments
        auto refs2 = runWithSeg(std::max<size_t>(1, data.size() / 2));
        // Many small segments
        auto refsN = runWithSeg(7);

        REQUIRE(refs1.size() == refs2.size());
        REQUIRE(refs1.size() == refsN.size());

        for (size_t i = 0; i < refs1.size(); ++i) {
            REQUIRE(refs1[i].offset == refs2[i].offset);
            REQUIRE(refs1[i].size == refs2[i].size);
            REQUIRE(refs1[i].hash == refs2[i].hash);
            REQUIRE(refs1[i].offset == refsN[i].offset);
            REQUIRE(refs1[i].size == refsN[i].size);
            REQUIRE(refs1[i].hash == refsN[i].hash);
        }
    }

    SECTION("Final flush emits once for partial chunk") {
        // Small data that never reaches minChunkSize
        const size_t LEN = 37;
        std::string data(LEN, 'A');

        ChunkingConfig cfg;
        cfg.minChunkSize = 128; // Larger than LEN
        cfg.targetChunkSize = 256;
        cfg.maxChunkSize = 1024;

        StreamingChunker chunker(cfg);
        SegmentingStringBuf sb(data, 5); // Small reads
        std::istream is(&sb);

        std::vector<ChunkRef> refs;
        auto r = chunker.processStream(is, data.size(),
                                       [&](const ChunkRef& ref, std::span<const std::byte> bytes) {
                                           refs.push_back(ref);
                                           REQUIRE(ref.size == bytes.size());
                                       });
        REQUIRE(r.has_value());
        REQUIRE(refs.size() == 1);
        REQUIRE(refs[0].offset == 0);
        REQUIRE(refs[0].size == LEN);
    }
}
