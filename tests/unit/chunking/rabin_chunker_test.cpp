#include <gtest/gtest.h>
#include <yams/chunking/chunker.h>
#include <yams/chunking/streaming_chunker.h>
#include <sstream>

using namespace yams::chunking;

static std::vector<std::byte> make_bytes(size_t n, uint8_t value = 0xAB) {
    std::vector<std::byte> v;
    v.resize(n);
    for (size_t i = 0; i < n; ++i) v[i] = std::byte{value};
    return v;
}

TEST(RabinChunkerTest, EmptyInputProducesNoChunks) {
    RabinChunker chunker;
    auto chunks = chunker.chunkData({});
    EXPECT_TRUE(chunks.empty());
}

TEST(RabinChunkerTest, SingleByteWithUnitChunkConfig) {
    ChunkingConfig cfg{};
    cfg.minChunkSize = 1;
    cfg.targetChunkSize = 1;
    cfg.maxChunkSize = 1;
    RabinChunker chunker(cfg);

    auto data = make_bytes(1, 0x11);
    auto chunks = chunker.chunkData(std::span<const std::byte>(data.data(), data.size()));
    ASSERT_EQ(chunks.size(), 1u);
    EXPECT_EQ(chunks[0].size, 1u);
    EXPECT_EQ(chunks[0].offset, 0u);
}

TEST(RabinChunkerTest, FixedSizeChunksAtBoundary) {
    ChunkingConfig cfg{};
    cfg.minChunkSize = 64;
    cfg.targetChunkSize = 64;
    cfg.maxChunkSize = 64;
    RabinChunker chunker(cfg);

    // Exactly one chunk
    auto data1 = make_bytes(64, 0x22);
    auto chunks1 = chunker.chunkData(std::span<const std::byte>(data1.data(), data1.size()));
    ASSERT_EQ(chunks1.size(), 1u);
    EXPECT_EQ(chunks1[0].size, 64u);

    // Off-by-one above boundary: expect 2 chunks (64 + 1)
    auto data2 = make_bytes(65, 0x33);
    auto chunks2 = chunker.chunkData(std::span<const std::byte>(data2.data(), data2.size()));
    ASSERT_EQ(chunks2.size(), 2u);
    EXPECT_EQ(chunks2[0].size, 64u);
    EXPECT_EQ(chunks2[1].size, 1u);
    EXPECT_EQ(chunks2[0].offset, 0u);
    EXPECT_EQ(chunks2[1].offset, 64u);
}

TEST(RabinChunkerTest, InvariantsLengthAndRange) {
    ChunkingConfig cfg{};
    cfg.minChunkSize = 32;
    cfg.targetChunkSize = 64;
    cfg.maxChunkSize = 96;
    RabinChunker chunker(cfg);

    auto data = make_bytes(1024, 0x44);
    auto chunks = chunker.chunkData(std::span<const std::byte>(data.data(), data.size()));

    size_t sum = 0;
    for (const auto& c : chunks) {
        sum += c.size;
        // All chunks except possibly the last must be within [min,max]
        if (&c != &chunks.back()) {
            EXPECT_GE(c.size, cfg.minChunkSize);
            EXPECT_LE(c.size, cfg.maxChunkSize);
        }
    }
    EXPECT_EQ(sum, data.size());
}

TEST(RabinChunkerTest, StreamingMatchesNonStreamingForFixedSize) {
    ChunkingConfig cfg{};
    cfg.minChunkSize = 64;
    cfg.targetChunkSize = 64;
    cfg.maxChunkSize = 64;

    auto data = make_bytes(256, 0x55);

    RabinChunker chunker(cfg);
    auto nonstream = chunker.chunkData(std::span<const std::byte>(data.data(), data.size()));

    auto streamer = createStreamingChunker(cfg);
    std::vector<ChunkRef> refs;
    std::string s(reinterpret_cast<const char*>(data.data()), data.size());
    std::istringstream iss(s);
    auto res = static_cast<StreamingChunker*>(streamer.get())->processStream(
        iss,
        data.size(),
        [&](const ChunkRef& ref, std::span<const std::byte>) { refs.push_back(ref); });
    ASSERT_TRUE(res.has_value());

    ASSERT_EQ(refs.size(), nonstream.size());
    for (size_t i = 0; i < refs.size(); ++i) {
        EXPECT_EQ(refs[i].size, nonstream[i].size);
        EXPECT_EQ(refs[i].offset, nonstream[i].offset);
    }
}
