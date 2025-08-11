#include <gtest/gtest.h>
#include <yams/manifest/manifest_manager.h>
#include <yams/chunking/chunker.h>
#include <yams/crypto/hasher.h>
#include "test_helpers.h"

#include <algorithm>
#include <random>
#include <thread>

using namespace yams;
using namespace yams::manifest;
using namespace yams::chunking;
using namespace yams::test;

class ManifestManagerTest : public YamsTest {
protected:
    std::unique_ptr<ManifestManager> manager;
    
    void SetUp() override {
        YamsTest::SetUp();
        
        ManifestManager::Config config{
            .enableCompression = false,  // Disable for predictable tests
            .enableChecksums = true,
            .enableCaching = true,
            .cacheSize = 100
        };
        
        manager = std::make_unique<ManifestManager>(std::move(config));
    }
    
    // Helper to create test chunks
    std::vector<Chunk> createTestChunks(size_t count, size_t chunkSize = 1024) {
        std::vector<Chunk> chunks;
        chunks.reserve(count);
        
        uint64_t offset = 0;
        auto hasher = crypto::createSHA256Hasher();
        
        for (size_t i = 0; i < count; ++i) {
            auto data = generateRandomBytes(chunkSize);
            auto hash = hasher->hash(data);
            
            chunks.emplace_back(Chunk{
                .data = data,
                .hash = hash,
                .offset = offset,
                .size = data.size()
            });
            
            offset += data.size();
        }
        
        return chunks;
    }
    
    // Helper to create test file info
    FileInfo createTestFileInfo(const std::vector<Chunk>& chunks) {
        uint64_t totalSize = 0;
        std::vector<std::byte> allData;
        
        for (const auto& chunk : chunks) {
            totalSize += chunk.size;
            allData.insert(allData.end(), chunk.data.begin(), chunk.data.end());
        }
        
        auto hasher = crypto::createSHA256Hasher();
        auto fileHash = hasher->hash(allData);
        
        return FileInfo{
            .hash = fileHash,
            .size = totalSize,
            .mimeType = "text/plain",
            .createdAt = std::chrono::system_clock::now(),
            .originalName = "test.txt"
        };
    }
};

TEST_F(ManifestManagerTest, CreateSimpleManifest) {
    auto chunks = createTestChunks(5, 512);
    auto fileInfo = createTestFileInfo(chunks);
    
    auto result = manager->createManifest(fileInfo, chunks);
    ASSERT_TRUE(result.has_value());
    
    const auto& manifest = result.value();
    EXPECT_EQ(manifest.version, Manifest::CURRENT_VERSION);
    EXPECT_EQ(manifest.fileHash, fileInfo.hash);
    EXPECT_EQ(manifest.fileSize, fileInfo.size);
    EXPECT_EQ(manifest.originalName, fileInfo.originalName);
    EXPECT_EQ(manifest.mimeType, fileInfo.mimeType);
    EXPECT_EQ(manifest.chunks.size(), chunks.size());
    
    // Verify chunk mapping
    for (size_t i = 0; i < chunks.size(); ++i) {
        EXPECT_EQ(manifest.chunks[i].hash, chunks[i].hash);
        EXPECT_EQ(manifest.chunks[i].offset, chunks[i].offset);
        EXPECT_EQ(manifest.chunks[i].size, chunks[i].size);
    }
    
    EXPECT_TRUE(manifest.isValid());
}

TEST_F(ManifestManagerTest, CreateEmptyManifest) {
    std::vector<Chunk> emptyChunks;
    FileInfo fileInfo{
        .hash = "empty",
        .size = 0,
        .mimeType = "text/plain",
        .createdAt = std::chrono::system_clock::now(),
        .originalName = "empty.txt"
    };
    
    auto result = manager->createManifest(fileInfo, emptyChunks);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::InvalidArgument);
}

TEST_F(ManifestManagerTest, CreateLargeManifest) {
    // Test with many chunks
    auto chunks = createTestChunks(1000, 64);
    auto fileInfo = createTestFileInfo(chunks);
    
    auto result = manager->createManifest(fileInfo, chunks);
    ASSERT_TRUE(result.has_value());
    
    const auto& manifest = result.value();
    EXPECT_EQ(manifest.chunks.size(), 1000u);
    EXPECT_TRUE(manifest.isValid());
    
    // Verify total size calculation
    EXPECT_EQ(manifest.calculateTotalSize(), manifest.fileSize);
}

TEST_F(ManifestManagerTest, SerializeDeserializeRoundtrip) {
    auto chunks = createTestChunks(10, 256);
    auto fileInfo = createTestFileInfo(chunks);
    
    // Create manifest
    auto createResult = manager->createManifest(fileInfo, chunks);
    ASSERT_TRUE(createResult.has_value());
    const auto& originalManifest = createResult.value();
    
    // Serialize
    auto serializeResult = manager->serialize(originalManifest);
    ASSERT_TRUE(serializeResult.has_value());
    const auto& serializedData = serializeResult.value();
    
    // Verify serialized size is reasonable
    EXPECT_GT(serializedData.size(), 100u);  // Should have substantial content
    EXPECT_LT(serializedData.size(), originalManifest.fileSize / 10);  // Should be < 10% of file
    
    // Deserialize
    auto deserializeResult = manager->deserialize(serializedData);
    ASSERT_TRUE(deserializeResult.has_value());
    const auto& deserializedManifest = deserializeResult.value();
    
    // Verify roundtrip accuracy
    EXPECT_EQ(deserializedManifest.version, originalManifest.version);
    EXPECT_EQ(deserializedManifest.fileHash, originalManifest.fileHash);
    EXPECT_EQ(deserializedManifest.fileSize, originalManifest.fileSize);
    EXPECT_EQ(deserializedManifest.originalName, originalManifest.originalName);
    EXPECT_EQ(deserializedManifest.mimeType, originalManifest.mimeType);
    EXPECT_EQ(deserializedManifest.chunks.size(), originalManifest.chunks.size());
    
    // Verify all chunks match
    for (size_t i = 0; i < originalManifest.chunks.size(); ++i) {
        EXPECT_EQ(deserializedManifest.chunks[i].hash, originalManifest.chunks[i].hash);
        EXPECT_EQ(deserializedManifest.chunks[i].offset, originalManifest.chunks[i].offset);
        EXPECT_EQ(deserializedManifest.chunks[i].size, originalManifest.chunks[i].size);
    }
}

TEST_F(ManifestManagerTest, SerializeCorruptedData) {
    std::vector<std::byte> corruptedData{
        std::byte{0xFF}, std::byte{0xFE}, std::byte{0xFD}
    };
    
    auto result = manager->deserialize(corruptedData);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::CorruptedData);
}

TEST_F(ManifestManagerTest, ValidateManifest) {
    auto chunks = createTestChunks(5, 512);
    auto fileInfo = createTestFileInfo(chunks);
    
    auto createResult = manager->createManifest(fileInfo, chunks);
    ASSERT_TRUE(createResult.has_value());
    auto manifest = createResult.value();
    
    // Valid manifest
    auto validationResult = manager->validateManifest(manifest);
    ASSERT_TRUE(validationResult.has_value());
    EXPECT_TRUE(validationResult.value());
    
    // Test invalid manifest - wrong file size
    manifest.fileSize = 12345;
    validationResult = manager->validateManifest(manifest);
    ASSERT_TRUE(validationResult.has_value());
    EXPECT_FALSE(validationResult.value());
}

TEST_F(ManifestManagerTest, ValidateManifestWithGaps) {
    auto chunks = createTestChunks(3, 512);
    auto fileInfo = createTestFileInfo(chunks);
    
    auto createResult = manager->createManifest(fileInfo, chunks);
    ASSERT_TRUE(createResult.has_value());
    auto manifest = createResult.value();
    
    // Create gap in chunk offsets
    manifest.chunks[1].offset = manifest.chunks[0].offset + manifest.chunks[0].size + 100;
    
    auto validationResult = manager->validateManifest(manifest);
    ASSERT_TRUE(validationResult.has_value());
    EXPECT_FALSE(validationResult.value());
}

TEST_F(ManifestManagerTest, FileReconstruction) {
    auto chunks = createTestChunks(5, 1024);
    auto fileInfo = createTestFileInfo(chunks);
    
    // Create manifest
    auto createResult = manager->createManifest(fileInfo, chunks);
    ASSERT_TRUE(createResult.has_value());
    const auto& manifest = createResult.value();
    
    // Mock chunk provider
    struct MockChunkProvider : public IChunkProvider {
        const std::vector<Chunk>& chunks;
        
        explicit MockChunkProvider(const std::vector<Chunk>& c) : chunks(c) {}
        
        Result<std::vector<std::byte>> getChunk(const std::string& hash) const override {
            for (const auto& chunk : chunks) {
                if (chunk.hash == hash) {
                    return Result<std::vector<std::byte>>(chunk.data);
                }
            }
            return Result<std::vector<std::byte>>(ErrorCode::ChunkNotFound);
        }
        
        bool hasChunk(const std::string& hash) const {
            return std::ranges::any_of(chunks, [&hash](const auto& chunk) {
                return chunk.hash == hash;
            });
        }
    };
    
    MockChunkProvider provider(chunks);
    auto outputPath = getTempDir() / "reconstructed.txt";
    
    // Reconstruct file
    auto result = manager->reconstructFile(manifest, outputPath, provider);
    ASSERT_TRUE(result.has_value());
    
    // Verify reconstructed file
    ASSERT_TRUE(std::filesystem::exists(outputPath));
    EXPECT_EQ(std::filesystem::file_size(outputPath), manifest.fileSize);
    
    // Verify file content matches original
    std::ifstream file(outputPath, std::ios::binary);
    ASSERT_TRUE(file.is_open());
    
    for (const auto& chunk : chunks) {
        std::vector<char> buffer(chunk.size);
        file.read(buffer.data(), chunk.size);
        ASSERT_TRUE(file.good());
        
        // Compare chunk data
        auto chunkBytes = reinterpret_cast<const std::byte*>(buffer.data());
        EXPECT_TRUE(std::equal(chunk.data.begin(), chunk.data.end(), chunkBytes));
    }
}

TEST_F(ManifestManagerTest, FileReconstructionMissingChunk) {
    auto chunks = createTestChunks(3, 512);
    auto fileInfo = createTestFileInfo(chunks);
    
    auto createResult = manager->createManifest(fileInfo, chunks);
    ASSERT_TRUE(createResult.has_value());
    const auto& manifest = createResult.value();
    
    // Mock provider missing the second chunk
    struct IncompleteChunkProvider : public IChunkProvider {
        const std::vector<Chunk>& chunks;
        
        explicit IncompleteChunkProvider(const std::vector<Chunk>& c) : chunks(c) {}
        
        Result<std::vector<std::byte>> getChunk(const std::string& hash) const override {
            // Skip the second chunk
            for (size_t i = 0; i < chunks.size(); ++i) {
                if (i == 1) continue;  // Skip second chunk
                if (chunks[i].hash == hash) {
                    return Result<std::vector<std::byte>>(chunks[i].data);
                }
            }
            return Result<std::vector<std::byte>>(ErrorCode::ChunkNotFound);
        }
        
        bool hasChunk(const std::string& hash) const {
            for (size_t i = 0; i < chunks.size(); ++i) {
                if (i == 1) continue;
                if (chunks[i].hash == hash) return true;
            }
            return false;
        }
    };
    
    IncompleteChunkProvider provider(chunks);
    auto outputPath = getTempDir() / "failed_reconstruction.txt";
    
    auto result = manager->reconstructFile(manifest, outputPath, provider);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::ChunkNotFound);
}

TEST_F(ManifestManagerTest, AsyncFileReconstruction) {
    auto chunks = createTestChunks(3, 256);
    auto fileInfo = createTestFileInfo(chunks);
    
    auto createResult = manager->createManifest(fileInfo, chunks);
    ASSERT_TRUE(createResult.has_value());
    const auto& manifest = createResult.value();
    
    // Mock chunk provider with delays
    struct SlowChunkProvider : public IChunkProvider {
        const std::vector<Chunk>& chunks;
        
        explicit SlowChunkProvider(const std::vector<Chunk>& c) : chunks(c) {}
        
        Result<std::vector<std::byte>> getChunk(const std::string& hash) const override {
            // Simulate network delay
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            
            for (const auto& chunk : chunks) {
                if (chunk.hash == hash) {
                    return Result<std::vector<std::byte>>(chunk.data);
                }
            }
            return Result<std::vector<std::byte>>(ErrorCode::ChunkNotFound);
        }
        
        bool hasChunk(const std::string& hash) const {
            return std::ranges::any_of(chunks, [&hash](const auto& chunk) {
                return chunk.hash == hash;
            });
        }
    };
    
    SlowChunkProvider provider(chunks);
    auto outputPath = getTempDir() / "async_reconstructed.txt";
    
    // Start async reconstruction
    auto future = manager->reconstructFileAsync(manifest, outputPath, provider);
    
    // Should complete within reasonable time
    auto status = future.wait_for(std::chrono::seconds(5));
    ASSERT_EQ(status, std::future_status::ready);
    
    auto result = future.get();
    ASSERT_TRUE(result.has_value());
    
    // Verify file was created
    ASSERT_TRUE(std::filesystem::exists(outputPath));
    EXPECT_EQ(std::filesystem::file_size(outputPath), manifest.fileSize);
}

TEST_F(ManifestManagerTest, MimeTypeDetection) {
    // Test various file extensions
    EXPECT_EQ(ManifestManager::detectMimeType("test.txt"), "text/plain");
    EXPECT_EQ(ManifestManager::detectMimeType("config.json"), "application/json");
    EXPECT_EQ(ManifestManager::detectMimeType("image.jpg"), "image/jpeg");
    EXPECT_EQ(ManifestManager::detectMimeType("image.JPEG"), "image/jpeg");  // Case insensitive
    EXPECT_EQ(ManifestManager::detectMimeType("document.pdf"), "application/pdf");
    EXPECT_EQ(ManifestManager::detectMimeType("unknown.xyz"), "application/octet-stream");
}

TEST_F(ManifestManagerTest, Statistics) {
    auto chunks = createTestChunks(5, 512);
    auto fileInfo = createTestFileInfo(chunks);
    
    // Create and serialize multiple manifests
    for (int i = 0; i < 3; ++i) {
        auto createResult = manager->createManifest(fileInfo, chunks);
        ASSERT_TRUE(createResult.has_value());
        
        auto serializeResult = manager->serialize(createResult.value());
        ASSERT_TRUE(serializeResult.has_value());
        
        auto deserializeResult = manager->deserialize(serializeResult.value());
        ASSERT_TRUE(deserializeResult.has_value());
    }
    
    auto stats = manager->getStats();
    EXPECT_EQ(stats.totalManifests, 3u);
    EXPECT_GT(stats.avgSerializationTime.count(), 0);
    EXPECT_GT(stats.avgDeserializationTime.count(), 0);
}

TEST_F(ManifestManagerTest, ConcurrentOperations) {
    const int numThreads = 10;
    const int operationsPerThread = 5;
    
    std::vector<std::thread> threads;
    std::atomic<int> successCount{0};
    
    auto chunks = createTestChunks(3, 256);
    auto fileInfo = createTestFileInfo(chunks);
    
    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([=, this, &chunks, &fileInfo, &successCount]() {
            for (int j = 0; j < operationsPerThread; ++j) {
                // Create manifest
                auto createResult = manager->createManifest(fileInfo, chunks);
                if (!createResult.has_value()) continue;
                
                // Serialize
                auto serializeResult = manager->serialize(createResult.value());
                if (!serializeResult.has_value()) continue;
                
                // Deserialize
                auto deserializeResult = manager->deserialize(serializeResult.value());
                if (!deserializeResult.has_value()) continue;
                
                // Validate
                auto validateResult = manager->validateManifest(deserializeResult.value());
                if (!validateResult.has_value() || !validateResult.value()) continue;
                
                successCount++;
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    EXPECT_EQ(successCount.load(), numThreads * operationsPerThread);
}

TEST_F(ManifestManagerTest, PerformanceBenchmark) {
    // Test serialization performance
    auto chunks = createTestChunks(100, 1024);  // 100KB file
    auto fileInfo = createTestFileInfo(chunks);
    
    auto createResult = manager->createManifest(fileInfo, chunks);
    ASSERT_TRUE(createResult.has_value());
    const auto& manifest = createResult.value();
    
    // Benchmark serialization
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < 10; ++i) {
        auto result = manager->serialize(manifest);
        ASSERT_TRUE(result.has_value());
    }
    auto end = std::chrono::steady_clock::now();
    
    auto avgSerializationTime = std::chrono::duration_cast<std::chrono::milliseconds>(
        end - start).count() / 10;
    
    // Should serialize 100KB file manifest in < 50ms
    EXPECT_LT(avgSerializationTime, 50);
    
    // Verify manifest size is < 1% of file size
    auto serializeResult = manager->serialize(manifest);
    ASSERT_TRUE(serializeResult.has_value());
    
    double manifestRatio = static_cast<double>(serializeResult.value().size()) / manifest.fileSize;
    EXPECT_LT(manifestRatio, 0.01);  // < 1%
}