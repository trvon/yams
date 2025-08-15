#include "test_helpers.h"
#include <gtest/gtest.h>
#include <yams/core/span.h>
#include <yams/crypto/hasher.h>

using namespace yams;
using namespace yams::crypto;
using namespace yams::test;

class SHA256HasherTest : public YamsTest {
protected:
    std::unique_ptr<SHA256Hasher> hasher;

    void SetUp() override {
        YamsTest::SetUp();
        hasher = std::make_unique<SHA256Hasher>();
    }
};

TEST_F(SHA256HasherTest, EmptyInput) {
    hasher->init();
    auto hash = hasher->finalize();
    EXPECT_EQ(hash, TestVectors::EMPTY_SHA256);
}

TEST_F(SHA256HasherTest, KnownTestVectors) {
    // Test "abc"
    {
        auto data = TestVectors::getABCData();
        hasher->init();
        hasher->update(yams::span{data});
        auto hash = hasher->finalize();
        EXPECT_EQ(hash, TestVectors::ABC_SHA256);
    }

    // Test "Hello World"
    {
        auto data = TestVectors::getHelloWorldData();
        hasher->init();
        hasher->update(yams::span{data});
        auto hash = hasher->finalize();
        EXPECT_EQ(hash, TestVectors::HELLO_WORLD_SHA256);
    }
}

TEST_F(SHA256HasherTest, LargeInput) {
    // Test with 1MB of data
    auto data = generateRandomBytes(1024 * 1024);

    hasher->init();
    hasher->update(yams::span{data});
    auto hash1 = hasher->finalize();

    // Hash again to verify consistency
    hasher->init();
    hasher->update(yams::span{data});
    auto hash2 = hasher->finalize();

    EXPECT_EQ(hash1, hash2);
    EXPECT_EQ(hash1.size(), 64u); // SHA-256 hex string length
}

TEST_F(SHA256HasherTest, StreamingUpdate) {
    // Hash data in chunks
    auto data = generateRandomBytes(1000);

    hasher->init();
    hasher->update(yams::span{data.data(), 100});
    hasher->update(yams::span{data.data() + 100, 400});
    hasher->update(yams::span{data.data() + 500, 500});
    auto hash1 = hasher->finalize();

    // Hash all at once
    hasher->init();
    hasher->update(yams::span{data});
    auto hash2 = hasher->finalize();

    EXPECT_EQ(hash1, hash2);
}

TEST_F(SHA256HasherTest, FileHashing) {
    // Create test file
    auto content = generateRandomBytes(10000);
    auto path = createTestFileWithContent(content, "test.bin");

    // Hash file
    auto fileHash = hasher->hashFile(path);

    // Hash content directly
    hasher->init();
    hasher->update(std::span{content});
    auto contentHash = hasher->finalize();

    EXPECT_EQ(fileHash, contentHash);
}

TEST_F(SHA256HasherTest, AsyncFileHashing) {
    // Create test file
    auto content = generateRandomBytes(50000);
    auto path = createTestFileWithContent(content, "async_test.bin");

    // Hash asynchronously
    auto future = hasher->hashFileAsync(path);
    auto result = future.get();

    ASSERT_TRUE(result.has_value());

    // Verify against sync hash
    auto syncHash = hasher->hashFile(path);
    EXPECT_EQ(result.value(), syncHash);
}

TEST_F(SHA256HasherTest, ProgressCallback) {
    // Create larger test file
    auto content = generateRandomBytes(1024 * 1024); // 1MB
    auto path = createTestFileWithContent(content, "progress_test.bin");

    // Track progress
    std::vector<std::pair<uint64_t, uint64_t>> progressUpdates;
    hasher->setProgressCallback(
        [&](uint64_t current, uint64_t total) { progressUpdates.emplace_back(current, total); });

    hasher->hashFile(path);

    // Verify progress was reported
    EXPECT_GT(progressUpdates.size(), 0u);

    // Verify final progress equals file size
    if (!progressUpdates.empty()) {
        auto [finalCurrent, finalTotal] = progressUpdates.back();
        EXPECT_EQ(finalCurrent, content.size());
        EXPECT_EQ(finalTotal, content.size());
    }
}

TEST_F(SHA256HasherTest, StaticHashMethod) {
    auto data = TestVectors::getABCData();
    auto hash = SHA256Hasher::hash(yams::span{data});
    EXPECT_EQ(hash, TestVectors::ABC_SHA256);
}

TEST_F(SHA256HasherTest, GenericHashMethod) {
    // Test with vector
    std::vector<std::byte> vec = TestVectors::getABCData();
    auto hash1 = hasher->hash(vec);
    EXPECT_EQ(hash1, TestVectors::ABC_SHA256);

    // Test with file path
    auto path = createTestFileWithContent(vec, "generic_test.bin");
    auto hash2 = hasher->hashFile(path);
    EXPECT_EQ(hash2, TestVectors::ABC_SHA256);
}

TEST_F(SHA256HasherTest, ErrorHandling) {
    // Test non-existent file
    auto future = hasher->hashFileAsync("/non/existent/file");
    auto result = future.get();

    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code, ErrorCode::FileNotFound);
}

// Performance test (only in release builds)
#ifdef NDEBUG
TEST_F(SHA256HasherTest, Performance) {
    // Create 100MB test file
    constexpr size_t fileSize = 100 * 1024 * 1024;
    auto content = generateRandomBytes(fileSize);
    auto path = createTestFileWithContent(content, "perf_test.bin");

    auto start = std::chrono::high_resolution_clock::now();
    auto hash = hasher->hashFile(path);
    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    auto throughput = (fileSize / 1024.0 / 1024.0) / (duration.count() / 1000.0);

    std::cout << std::format("Hashed {}MB in {}ms ({:.2f}MB/s)\n", fileSize / 1024 / 1024,
                             duration.count(), throughput);

    // Expect at least 100MB/s
    EXPECT_GT(throughput, 100.0);
}
#endif