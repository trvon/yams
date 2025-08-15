#include "test_helpers.h"
#include <gtest/gtest.h>
#include <yams/chunking/chunker.h>
#include <yams/crypto/hasher.h>
#include <yams/manifest/manifest_manager.h>
#include <yams/storage/storage_engine.h>

#include <spdlog/spdlog.h>
#include <fstream>
#include <random>

using namespace yams;
using namespace yams::manifest;
using namespace yams::storage;
using namespace yams::chunking;
using namespace yams::test;

class ManifestIntegrationTest : public YamsTest {
protected:
    std::unique_ptr<ManifestManager> manifestManager;
    std::unique_ptr<StorageEngine> storageEngine;
    std::unique_ptr<RabinChunker> chunker;
    std::filesystem::path storagePath;

    void SetUp() override {
        YamsTest::SetUp();

        // Setup storage
        storagePath = getTempDir() / "integration_storage";
        std::filesystem::create_directories(storagePath);

        StorageConfig storageConfig{.basePath = storagePath, .shardDepth = 2, .mutexPoolSize = 64};
        storageEngine = std::make_unique<StorageEngine>(std::move(storageConfig));

        // Setup manifest manager
        ManifestManager::Config manifestConfig{.enableCompression = true,
                                               .enableChecksums = true,
                                               .enableCaching = true,
                                               .cacheSize = 50};
        manifestManager = std::make_unique<ManifestManager>(std::move(manifestConfig));

        // Setup chunker
        ChunkingConfig chunkConfig{
            .minChunkSize = 4096, .targetChunkSize = 16384, .maxChunkSize = 65536};
        chunker = std::make_unique<RabinChunker>(std::move(chunkConfig));
    }

    void TearDown() override {
        manifestManager.reset();
        storageEngine.reset();
        chunker.reset();
        std::filesystem::remove_all(storagePath);
        YamsTest::TearDown();
    }

    // Create a test file with specific content
    std::filesystem::path createTestFile(const std::string& content,
                                         const std::string& filename = "test.txt") {
        auto filePath = getTempDir() / filename;
        std::ofstream file(filePath);
        file << content;
        file.close();
        return filePath;
    }

    // Storage adapter for manifest reconstruction
    struct StorageChunkProvider {
        StorageEngine& storage;

        explicit StorageChunkProvider(StorageEngine& s) : storage(s) {}

        Result<std::vector<std::byte>> getChunk(const std::string& hash) const {
            return storage.retrieve(hash);
        }

        bool hasChunk(const std::string& hash) const {
            auto result = storage.exists(hash);
            return result.has_value() && result.value();
        }
    };
};

TEST_F(ManifestIntegrationTest, EndToEndFileProcessing) {
    // Create test file
    std::string testContent = "This is a test file for end-to-end processing. ";
    for (int i = 0; i < 100; ++i) {
        testContent += "Line " + std::to_string(i) + " with some content. ";
    }

    auto originalFile = createTestFile(testContent, "original.txt");

    // Get file info
    auto fileInfoResult = ManifestManager::getFileInfo(originalFile);
    ASSERT_TRUE(fileInfoResult.has_value());
    const auto& fileInfo = fileInfoResult.value();

    // Chunk the file
    auto chunks = chunker->chunkFile(originalFile);
    ASSERT_FALSE(chunks.empty());

    spdlog::info("Chunked file into {} chunks, total size: {}", chunks.size(), fileInfo.size);

    // Store all chunks in storage engine
    for (const auto& chunk : chunks) {
        auto storeResult = storageEngine->store(chunk.hash, chunk.data);
        ASSERT_TRUE(storeResult.has_value()) << "Failed to store chunk: " << chunk.hash;
    }

    // Create manifest
    auto manifestResult = manifestManager->createManifest(fileInfo, chunks);
    ASSERT_TRUE(manifestResult.has_value());
    const auto& manifest = manifestResult.value();

    // Validate manifest
    auto validateResult = manifestManager->validateManifest(manifest);
    ASSERT_TRUE(validateResult.has_value());
    EXPECT_TRUE(validateResult.value());

    // Serialize manifest
    auto serializeResult = manifestManager->serialize(manifest);
    ASSERT_TRUE(serializeResult.has_value());
    const auto& serializedManifest = serializeResult.value();

    spdlog::info("Manifest serialized to {} bytes ({:.2f}% of original file)",
                 serializedManifest.size(), 100.0 * serializedManifest.size() / fileInfo.size);

    // Store serialized manifest in storage
    auto hasher = crypto::createSHA256Hasher();
    auto manifestHash = hasher->hash(serializedManifest);
    auto storeManifestResult = storageEngine->store(manifestHash, serializedManifest);
    ASSERT_TRUE(storeManifestResult.has_value());

    // Retrieve and deserialize manifest
    auto retrieveManifestResult = storageEngine->retrieve(manifestHash);
    ASSERT_TRUE(retrieveManifestResult.has_value());

    auto deserializeResult = manifestManager->deserialize(retrieveManifestResult.value());
    ASSERT_TRUE(deserializeResult.has_value());
    const auto& retrievedManifest = deserializeResult.value();

    // Verify manifest integrity
    EXPECT_EQ(retrievedManifest.fileHash, manifest.fileHash);
    EXPECT_EQ(retrievedManifest.fileSize, manifest.fileSize);
    EXPECT_EQ(retrievedManifest.chunks.size(), manifest.chunks.size());

    // Reconstruct file using storage provider
    StorageChunkProvider provider(*storageEngine);
    auto reconstructedFile = getTempDir() / "reconstructed.txt";

    auto reconstructResult =
        manifestManager->reconstructFile(retrievedManifest, reconstructedFile, provider);
    ASSERT_TRUE(reconstructResult.has_value()) << "File reconstruction failed";

    // Verify reconstructed file
    ASSERT_TRUE(std::filesystem::exists(reconstructedFile));
    EXPECT_EQ(std::filesystem::file_size(reconstructedFile), fileInfo.size);

    // Verify file content matches exactly
    auto verifyResult = manifestManager->verifyFileIntegrity(retrievedManifest, reconstructedFile);
    ASSERT_TRUE(verifyResult.has_value());
    EXPECT_TRUE(verifyResult.value());

    // Compare files byte by byte
    std::ifstream orig(originalFile, std::ios::binary);
    std::ifstream recon(reconstructedFile, std::ios::binary);

    ASSERT_TRUE(orig.is_open() && recon.is_open());

    std::string origContent((std::istreambuf_iterator<char>(orig)),
                            std::istreambuf_iterator<char>());
    std::string reconContent((std::istreambuf_iterator<char>(recon)),
                             std::istreambuf_iterator<char>());

    EXPECT_EQ(origContent, reconContent);
}

TEST_F(ManifestIntegrationTest, LargeFileProcessing) {
    // Create larger test file (1MB)
    std::string pattern = "0123456789ABCDEF";
    std::string largeContent;
    largeContent.reserve(1024 * 1024);

    for (size_t i = 0; i < 65536; ++i) { // 65536 * 16 = 1MB
        largeContent += pattern;
    }

    auto originalFile = createTestFile(largeContent, "large.txt");

    // Process file
    auto fileInfoResult = ManifestManager::getFileInfo(originalFile);
    ASSERT_TRUE(fileInfoResult.has_value());
    const auto& fileInfo = fileInfoResult.value();

    auto chunks = chunker->chunkFile(originalFile);
    ASSERT_FALSE(chunks.empty());

    spdlog::info("Large file: {} bytes, {} chunks, avg chunk size: {}", fileInfo.size,
                 chunks.size(), fileInfo.size / chunks.size());

    // Store chunks
    size_t duplicateChunks = 0;
    for (const auto& chunk : chunks) {
        auto existsResult = storageEngine->exists(chunk.hash);
        if (existsResult.has_value() && existsResult.value()) {
            duplicateChunks++;
            continue;
        }

        auto storeResult = storageEngine->store(chunk.hash, chunk.data);
        ASSERT_TRUE(storeResult.has_value());
    }

    spdlog::info("Deduplication: {} duplicate chunks out of {}", duplicateChunks, chunks.size());

    // Create and test manifest
    auto manifestResult = manifestManager->createManifest(fileInfo, chunks);
    ASSERT_TRUE(manifestResult.has_value());
    const auto& manifest = manifestResult.value();

    // Test serialization performance
    auto start = std::chrono::steady_clock::now();
    auto serializeResult = manifestManager->serialize(manifest);
    auto end = std::chrono::steady_clock::now();

    ASSERT_TRUE(serializeResult.has_value());
    auto serializationTime = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    spdlog::info("Serialization time: {} ms", serializationTime.count());
    EXPECT_LT(serializationTime.count(), 100); // < 100ms for 1MB file

    // Test manifest size efficiency
    double manifestRatio = static_cast<double>(serializeResult.value().size()) / fileInfo.size;
    spdlog::info("Manifest overhead: {:.4f}%", manifestRatio * 100);
    EXPECT_LT(manifestRatio, 0.01); // < 1% overhead

    // Test reconstruction
    StorageChunkProvider provider(*storageEngine);
    auto reconstructedFile = getTempDir() / "large_reconstructed.txt";

    start = std::chrono::steady_clock::now();
    auto reconstructResult =
        manifestManager->reconstructFile(manifest, reconstructedFile, provider);
    end = std::chrono::steady_clock::now();

    ASSERT_TRUE(reconstructResult.has_value());
    auto reconstructionTime = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    spdlog::info("Reconstruction time: {} ms", reconstructionTime.count());

    // Verify reconstruction
    auto verifyResult = manifestManager->verifyFileIntegrity(manifest, reconstructedFile);
    ASSERT_TRUE(verifyResult.has_value());
    EXPECT_TRUE(verifyResult.value());
}

TEST_F(ManifestIntegrationTest, MultipleFileDeduplication) {
    // Create multiple files with overlapping content
    std::string baseContent = "Common content that should be deduplicated. ";
    for (int i = 0; i < 50; ++i) {
        baseContent += "Line " + std::to_string(i) + " shared across files. ";
    }

    std::vector<std::filesystem::path> files;
    std::vector<Manifest> manifests;

    // Create 5 files with variations
    for (int fileNum = 0; fileNum < 5; ++fileNum) {
        std::string content = baseContent;
        content += "\nFile-specific content for file " + std::to_string(fileNum) + ":\n";

        for (int i = 0; i < 20; ++i) {
            content +=
                "Unique line " + std::to_string(i) + " for file " + std::to_string(fileNum) + ". ";
        }

        auto filePath = createTestFile(content, "file_" + std::to_string(fileNum) + ".txt");
        files.push_back(filePath);

        // Process file
        auto fileInfoResult = ManifestManager::getFileInfo(filePath);
        ASSERT_TRUE(fileInfoResult.has_value());

        auto chunks = chunker->chunkFile(filePath);
        ASSERT_FALSE(chunks.empty());

        // Store chunks (with deduplication)
        for (const auto& chunk : chunks) {
            auto storeResult = storageEngine->store(chunk.hash, chunk.data);
            ASSERT_TRUE(storeResult.has_value());
        }

        // Create manifest
        auto manifestResult = manifestManager->createManifest(fileInfoResult.value(), chunks);
        ASSERT_TRUE(manifestResult.has_value());
        manifests.push_back(manifestResult.value());
    }

    // Verify all files can be reconstructed
    StorageChunkProvider provider(*storageEngine);

    for (size_t i = 0; i < files.size(); ++i) {
        auto reconstructedFile = getTempDir() / ("reconstructed_" + std::to_string(i) + ".txt");

        auto reconstructResult =
            manifestManager->reconstructFile(manifests[i], reconstructedFile, provider);
        ASSERT_TRUE(reconstructResult.has_value()) << "Failed to reconstruct file " << i;

        // Verify integrity
        auto verifyResult = manifestManager->verifyFileIntegrity(manifests[i], reconstructedFile);
        ASSERT_TRUE(verifyResult.has_value());
        EXPECT_TRUE(verifyResult.value()) << "Integrity check failed for file " << i;
    }

    // Check storage efficiency
    auto storageStats = storageEngine->getStats();
    spdlog::info("Storage stats: {} objects, {} bytes", storageStats.totalObjects.load(),
                 storageStats.totalBytes.load());

    // Calculate total original file size
    uint64_t totalOriginalSize = 0;
    for (const auto& manifest : manifests) {
        totalOriginalSize += manifest.fileSize;
    }

    double compressionRatio =
        static_cast<double>(storageStats.totalBytes.load()) / totalOriginalSize;
    spdlog::info("Deduplication ratio: {:.2f}% (stored {} bytes vs {} original)",
                 compressionRatio * 100, storageStats.totalBytes.load(), totalOriginalSize);

    // With overlapping content, we should see significant deduplication
    EXPECT_LT(compressionRatio, 0.8); // < 80% of original size due to deduplication
}

TEST_F(ManifestIntegrationTest, PartialFileReconstruction) {
    // Create test file
    std::string content = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    for (int i = 0; i < 100; ++i) {
        content += content; // Exponential growth for predictable chunks
    }

    auto originalFile = createTestFile(content, "partial_test.txt");

    // Process file
    auto fileInfoResult = ManifestManager::getFileInfo(originalFile);
    ASSERT_TRUE(fileInfoResult.has_value());

    auto chunks = chunker->chunkFile(originalFile);
    ASSERT_GE(chunks.size(), 3u); // Need at least 3 chunks for partial test

    // Store all chunks
    for (const auto& chunk : chunks) {
        auto storeResult = storageEngine->store(chunk.hash, chunk.data);
        ASSERT_TRUE(storeResult.has_value());
    }

    auto manifestResult = manifestManager->createManifest(fileInfoResult.value(), chunks);
    ASSERT_TRUE(manifestResult.has_value());
    const auto& manifest = manifestResult.value();

    // Test partial reconstruction by removing middle chunk from storage
    auto middleChunkHash = chunks[chunks.size() / 2].hash;
    auto removeResult = storageEngine->remove(middleChunkHash);
    ASSERT_TRUE(removeResult.has_value());

    // Attempt reconstruction - should fail
    StorageChunkProvider provider(*storageEngine);
    auto partialFile = getTempDir() / "partial_reconstructed.txt";

    auto reconstructResult = manifestManager->reconstructFile(manifest, partialFile, provider);
    ASSERT_FALSE(reconstructResult.has_value());
    EXPECT_EQ(reconstructResult.error(), ErrorCode::ChunkNotFound);

    // Restore the missing chunk
    auto restoreResult = storageEngine->store(middleChunkHash, chunks[chunks.size() / 2].data);
    ASSERT_TRUE(restoreResult.has_value());

    // Now reconstruction should succeed
    reconstructResult = manifestManager->reconstructFile(manifest, partialFile, provider);
    ASSERT_TRUE(reconstructResult.has_value());

    // Verify integrity
    auto verifyResult = manifestManager->verifyFileIntegrity(manifest, partialFile);
    ASSERT_TRUE(verifyResult.has_value());
    EXPECT_TRUE(verifyResult.value());
}

TEST_F(ManifestIntegrationTest, ConcurrentFileProcessing) {
    const int numFiles = 10;
    const int numThreads = 4;

    // Create test files
    std::vector<std::filesystem::path> files;
    for (int i = 0; i < numFiles; ++i) {
        std::string content = "File " + std::to_string(i) + " content: ";
        for (int j = 0; j < 100; ++j) {
            content += "Line " + std::to_string(j) + " of file " + std::to_string(i) + ". ";
        }

        auto filePath = createTestFile(content, "concurrent_" + std::to_string(i) + ".txt");
        files.push_back(filePath);
    }

    // Process files concurrently
    std::vector<std::thread> threads;
    std::vector<Manifest> manifests(numFiles);
    std::atomic<int> processedCount{0};
    std::atomic<int> errors{0};

    auto processFile = [&](int startIdx, int endIdx) {
        for (int i = startIdx; i < endIdx && i < numFiles; ++i) {
            try {
                // Get file info
                auto fileInfoResult = ManifestManager::getFileInfo(files[i]);
                if (!fileInfoResult.has_value()) {
                    errors++;
                    continue;
                }

                // Chunk file
                auto chunks = chunker->chunkFile(files[i]);
                if (chunks.empty()) {
                    errors++;
                    continue;
                }

                // Store chunks
                for (const auto& chunk : chunks) {
                    auto storeResult = storageEngine->store(chunk.hash, chunk.data);
                    if (!storeResult.has_value()) {
                        errors++;
                        continue;
                    }
                }

                // Create manifest
                auto manifestResult =
                    manifestManager->createManifest(fileInfoResult.value(), chunks);
                if (!manifestResult.has_value()) {
                    errors++;
                    continue;
                }

                manifests[i] = manifestResult.value();
                processedCount++;

            } catch (const std::exception& e) {
                spdlog::error("Error processing file {}: {}", i, e.what());
                errors++;
            }
        }
    };

    // Launch threads
    int filesPerThread = (numFiles + numThreads - 1) / numThreads;
    for (int t = 0; t < numThreads; ++t) {
        int startIdx = t * filesPerThread;
        int endIdx = startIdx + filesPerThread;
        threads.emplace_back(processFile, startIdx, endIdx);
    }

    // Wait for completion
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(errors.load(), 0);
    EXPECT_EQ(processedCount.load(), numFiles);

    // Verify all files can be reconstructed
    StorageChunkProvider provider(*storageEngine);

    for (int i = 0; i < numFiles; ++i) {
        auto reconstructedFile = getTempDir() / ("concurrent_recon_" + std::to_string(i) + ".txt");

        auto reconstructResult =
            manifestManager->reconstructFile(manifests[i], reconstructedFile, provider);
        ASSERT_TRUE(reconstructResult.has_value()) << "Failed to reconstruct file " << i;

        auto verifyResult = manifestManager->verifyFileIntegrity(manifests[i], reconstructedFile);
        ASSERT_TRUE(verifyResult.has_value());
        EXPECT_TRUE(verifyResult.value()) << "Integrity check failed for file " << i;
    }
}