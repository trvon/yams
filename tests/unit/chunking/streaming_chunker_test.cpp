#include <gtest/gtest.h>
#include <yams/chunking/chunker.h>
#include <yams/chunking/streaming_chunker.h>
#include <yams/crypto/hasher.h>

#include <filesystem>
#include <fstream>
#include <random>
#include <streambuf>

using namespace yams::chunking;

class StreamingChunkerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test directory
        testDir = std::filesystem::temp_directory_path() / "kronos_streaming_chunker_test";
        std::filesystem::create_directories(testDir);
    }

    void TearDown() override {
        // Clean up test directory
        std::filesystem::remove_all(testDir);
    }

    std::filesystem::path createTestFile(size_t size, const std::string& name = "test.dat") {
        auto path = testDir / name;
        std::ofstream file(path, std::ios::binary);

        // Generate deterministic test data
        std::mt19937 gen(42);
        std::uniform_int_distribution<> dist(0, 255);

        for (size_t i = 0; i < size; ++i) {
            char byte = static_cast<char>(dist(gen));
            file.write(&byte, 1);
        }

        return path;
    }

    std::filesystem::path testDir;
};

TEST_F(StreamingChunkerTest, ChunkSmallFile) {
    // Create a small test file
    auto filePath = createTestFile(1024);

    ChunkingConfig config;
    config.minChunkSize = 256;
    config.targetChunkSize = 512;
    config.maxChunkSize = 1024;

    StreamingChunker chunker(config);
    auto chunks = chunker.chunkFile(filePath);

    // Should produce at least one chunk
    ASSERT_FALSE(chunks.empty());

    // Total size should match file size
    size_t totalSize = 0;
    for (const auto& chunk : chunks) {
        totalSize += chunk.size;
        EXPECT_GE(chunk.size, config.minChunkSize);
        EXPECT_LE(chunk.size, config.maxChunkSize);
    }
    EXPECT_EQ(totalSize, 1024);
}

TEST_F(StreamingChunkerTest, ChunkLargeFile) {
    // Create a larger test file (1MB)
    auto filePath = createTestFile(1024 * 1024);

    ChunkingConfig config;
    config.minChunkSize = 4 * 1024;     // 4KB min
    config.targetChunkSize = 64 * 1024; // 64KB target
    config.maxChunkSize = 128 * 1024;   // 128KB max

    StreamingChunker chunker(config);
    auto chunks = chunker.chunkFile(filePath);

    // Should produce multiple chunks
    ASSERT_GT(chunks.size(), 1);

    // Verify chunk properties
    size_t totalSize = 0;
    for (const auto& chunk : chunks) {
        totalSize += chunk.size;

        // All chunks except possibly the last should respect size constraints
        if (&chunk != &chunks.back()) {
            EXPECT_GE(chunk.size, config.minChunkSize);
        }
        EXPECT_LE(chunk.size, config.maxChunkSize);

        // Verify hash is not empty
        EXPECT_FALSE(chunk.hash.empty());
    }

    // Total size should match file size
    EXPECT_EQ(totalSize, 1024 * 1024);
}

TEST_F(StreamingChunkerTest, StreamingVsInMemory) {
    // Create test file
    auto filePath = createTestFile(100 * 1024); // 100KB

    ChunkingConfig config;
    config.minChunkSize = 2 * 1024;
    config.targetChunkSize = 8 * 1024;
    config.maxChunkSize = 16 * 1024;

    // Chunk with streaming chunker
    StreamingChunker streamingChunker(config);
    auto streamingChunks = streamingChunker.chunkFile(filePath);

    // Read file to memory and chunk
    std::ifstream file(filePath, std::ios::binary);
    file.seekg(0, std::ios::end);
    size_t fileSize = file.tellg();
    file.seekg(0, std::ios::beg);

    std::vector<std::byte> data(fileSize);
    file.read(reinterpret_cast<char*>(data.data()), fileSize);
    auto memoryChunks = streamingChunker.chunkData(data);

    // Results should be identical
    ASSERT_EQ(streamingChunks.size(), memoryChunks.size());

    for (size_t i = 0; i < streamingChunks.size(); ++i) {
        EXPECT_EQ(streamingChunks[i].hash, memoryChunks[i].hash);
        EXPECT_EQ(streamingChunks[i].size, memoryChunks[i].size);
        EXPECT_EQ(streamingChunks[i].offset, memoryChunks[i].offset);
    }
}

TEST_F(StreamingChunkerTest, ProcessFileStream) {
    auto filePath = createTestFile(50 * 1024); // 50KB

    ChunkingConfig config;
    config.minChunkSize = 1024;
    config.targetChunkSize = 4096;
    config.maxChunkSize = 8192;

    StreamingChunker chunker(config);

    // Process file with custom processor
    std::vector<ChunkRef> refs;
    size_t totalProcessed = 0;

    auto result = chunker.processFileStream(
        filePath, [&refs, &totalProcessed](const ChunkRef& ref, std::span<const std::byte> data) {
            refs.push_back(ref);
            totalProcessed += data.size();

            // Verify data size matches ref
            EXPECT_EQ(ref.size, data.size());
        });

    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(refs.empty());
    EXPECT_EQ(totalProcessed, 50 * 1024);
}

TEST_F(StreamingChunkerTest, ProgressCallback) {
    auto filePath = createTestFile(10 * 1024); // 10KB

    ChunkingConfig config;
    config.minChunkSize = 512;
    config.targetChunkSize = 1024;
    config.maxChunkSize = 2048;

    StreamingChunker chunker(config);

    // Track progress
    std::vector<std::pair<uint64_t, uint64_t>> progressUpdates;
    chunker.setProgressCallback([&progressUpdates](uint64_t current, uint64_t total) {
        progressUpdates.push_back({current, total});
    });

    auto chunks = chunker.chunkFile(filePath);

    // Should have received progress updates
    EXPECT_FALSE(progressUpdates.empty());

    // Progress should be monotonically increasing
    uint64_t lastProgress = 0;
    for (const auto& [current, total] : progressUpdates) {
        EXPECT_GE(current, lastProgress);
        EXPECT_EQ(total, 10 * 1024);
        lastProgress = current;
    }
}

TEST_F(StreamingChunkerTest, EmptyFile) {
    auto filePath = createTestFile(0, "empty.dat");

    StreamingChunker chunker;
    auto chunks = chunker.chunkFile(filePath);

    // Empty file should produce no chunks
    EXPECT_TRUE(chunks.empty());
}

TEST_F(StreamingChunkerTest, NonExistentFile) {
    StreamingChunker chunker;

    // Should throw exception for non-existent file
    EXPECT_THROW(chunker.chunkFile(testDir / "nonexistent.dat"), std::runtime_error);
}

TEST_F(StreamingChunkerTest, AsyncChunking) {
    auto filePath = createTestFile(5 * 1024);

    StreamingChunker chunker;
    auto future = chunker.chunkFileAsync(filePath);

    auto result = future.get();
    ASSERT_TRUE(result.has_value());

    const auto& chunks = result.value();
    EXPECT_FALSE(chunks.empty());

    size_t totalSize = 0;
    for (const auto& chunk : chunks) {
        totalSize += chunk.size;
    }
    EXPECT_EQ(totalSize, 5 * 1024);
}

// Helper streambuf that segments reads into fixed-size chunks to simulate multi-write fragmentation
class SegmentingStringBuf : public std::streambuf {
public:
    SegmentingStringBuf(const std::string& data, size_t segment)
        : data_(data), segment_(segment), pos_(0) {}

protected:
    std::streamsize xsgetn(char* s, std::streamsize count) override {
        if (pos_ >= data_.size())
            return 0;
        std::streamsize n = static_cast<std::streamsize>(
            std::min(segment_, static_cast<size_t>(count))); // cap by segment size
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

static std::string makePatternData(size_t n) {
    std::string d;
    d.resize(n);
    for (size_t i = 0; i < n; ++i) {
        d[i] = static_cast<char>((i * 1315423911u + 0x9E3779B9u) & 0xFF);
    }
    return d;
}

TEST_F(StreamingChunkerTest, MultiWriteFragmentationEquivalence) {
    // Deterministic pseudo-random data
    const size_t N = 256 * 1024 + 777; // cross buffer boundaries
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
        auto r = chunker.processStream(is, data.size(),
                                       [&](const ChunkRef& ref, std::span<const std::byte> bytes) {
                                           refs.push_back(ref);
                                           // Ensure data span matches ref
                                           EXPECT_EQ(ref.size, bytes.size());
                                       });
        EXPECT_TRUE(r.has_value()) << (r ? "" : r.error().message);
        return refs;
    };

    // Single read (all at once)
    auto refs1 = runWithSeg(data.size());
    // Two segments
    auto refs2 = runWithSeg(std::max<size_t>(1, data.size() / 2));
    // Many small segments
    auto refsN = runWithSeg(7);

    ASSERT_EQ(refs1.size(), refs2.size());
    ASSERT_EQ(refs1.size(), refsN.size());
    for (size_t i = 0; i < refs1.size(); ++i) {
        EXPECT_EQ(refs1[i].offset, refs2[i].offset);
        EXPECT_EQ(refs1[i].size, refs2[i].size);
        EXPECT_EQ(refs1[i].hash, refs2[i].hash);
        EXPECT_EQ(refs1[i].offset, refsN[i].offset);
        EXPECT_EQ(refs1[i].size, refsN[i].size);
        EXPECT_EQ(refs1[i].hash, refsN[i].hash);
    }
}

TEST_F(StreamingChunkerTest, FinalFlushEmitsOnceForPartialChunk) {
    // Create small data that will never reach minChunkSize; ensures finalize emits exactly one
    const size_t LEN = 37;
    std::string data(LEN, 'A');

    ChunkingConfig cfg;
    cfg.minChunkSize = 128; // larger than LEN
    cfg.targetChunkSize = 256;
    cfg.maxChunkSize = 1024; // also larger than LEN

    StreamingChunker chunker(cfg);
    SegmentingStringBuf sb(data, 5); // small reads
    std::istream is(&sb);

    std::vector<ChunkRef> refs;
    auto r = chunker.processStream(is, data.size(),
                                   [&](const ChunkRef& ref, std::span<const std::byte> bytes) {
                                       refs.push_back(ref);
                                       EXPECT_EQ(ref.size, bytes.size());
                                   });
    ASSERT_TRUE(r.has_value()) << (r ? "" : r.error().message);

    ASSERT_EQ(refs.size(), 1u);
    EXPECT_EQ(refs[0].offset, 0u);
    EXPECT_EQ(refs[0].size, LEN);
}
