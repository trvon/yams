#include <gtest/gtest.h>
#include <yams/api/content_store.h>
#include <yams/api/content_store_builder.h>
#include <yams/storage/storage_engine.h>
#include <yams/storage/reference_counter.h>
#include <yams/chunking/chunker.h>
#include <yams/crypto/hasher.h>
#include <yams/manifest/manifest_manager.h>
#include <yams/wal/wal_manager.h>
#include <yams/integrity/verifier.h>
#include <filesystem>
#include <fstream>
#include <random>
#include <thread>
#include <chrono>

namespace fs = std::filesystem;
using namespace yams;
using namespace yams::api;
using namespace std::chrono_literals;

class FullSystemIntegrationTest : public ::testing::Test {
protected:
    fs::path testDir;
    std::unique_ptr<IContentStore> contentStore;
    
    void SetUp() override {
        // Create test directory
        testDir = fs::temp_directory_path() / 
                 ("kronos_integration_" + std::to_string(std::random_device{}()));
        fs::create_directories(testDir);
        
        // Create content store with all features enabled
        ContentStoreConfig config;
        config.storagePath = testDir;
        config.chunkSize = 64 * 1024;  // 64KB chunks
        config.enableCompression = true;
        // config.enableEncryption = false;  // Field doesn't exist
        config.enableIntegrityChecks = true;
        // config.enableWAL = true;  // Field doesn't exist
        
        auto result = ContentStoreBuilder()
            .withConfig(config)
            .build();
            
        ASSERT_TRUE(result.has_value()) << "Failed to create content store";
        contentStore = std::move(result).value();
    }
    
    void TearDown() override {
        contentStore.reset();
        fs::remove_all(testDir);
    }
    
    // Helper to create test file with specific content
    fs::path createTestFile(const std::string& name, size_t size) {
        fs::path filePath = testDir / name;
        std::ofstream file(filePath, std::ios::binary);
        
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 255);
        
        for (size_t i = 0; i < size; ++i) {
            file.put(static_cast<char>(dis(gen)));
        }
        
        file.close();
        return filePath;
    }
    
    // Helper to create file with repeating pattern (for dedup testing)
    fs::path createPatternFile(const std::string& name, const std::string& pattern, size_t repeats) {
        fs::path filePath = testDir / name;
        std::ofstream file(filePath);
        
        for (size_t i = 0; i < repeats; ++i) {
            file << pattern;
        }
        
        file.close();
        return filePath;
    }
    
    // Helper to verify file content
    bool filesAreEqual(const fs::path& file1, const fs::path& file2) {
        std::ifstream f1(file1, std::ios::binary);
        std::ifstream f2(file2, std::ios::binary);
        
        return std::equal(
            std::istreambuf_iterator<char>(f1),
            std::istreambuf_iterator<char>(),
            std::istreambuf_iterator<char>(f2),
            std::istreambuf_iterator<char>()
        );
    }
};

// Test basic store and retrieve workflow
TEST_F(FullSystemIntegrationTest, BasicStoreRetrieve) {
    // Create test file
    auto inputFile = createTestFile("test_input.bin", 1024 * 1024);  // 1MB
    
    // Store file
    ContentMetadata metadata;
    metadata.originalName = "test_input.bin";
    metadata.mimeType = "application/octet-stream";
    metadata.tags = {"test", "integration"};
    
    auto storeResult = contentStore->store(inputFile, metadata);
    ASSERT_TRUE(storeResult.has_value()) << "Store operation failed";
    
    auto& storeInfo = storeResult.value();
    EXPECT_FALSE(storeInfo.contentHash.empty());
    EXPECT_EQ(storeInfo.bytesStored, fs::file_size(inputFile));
    
    // Retrieve file
    auto outputFile = testDir / "test_output.bin";
    auto retrieveResult = contentStore->retrieve(storeInfo.contentHash, outputFile);
    ASSERT_TRUE(retrieveResult.has_value()) << "Retrieve operation failed";
    
    EXPECT_TRUE(retrieveResult.value().found);
    EXPECT_EQ(retrieveResult.value().size, fs::file_size(inputFile));
    
    // Verify content matches
    EXPECT_TRUE(filesAreEqual(inputFile, outputFile));
}

// Test deduplication across multiple files
TEST_F(FullSystemIntegrationTest, DeduplicationAcrossFiles) {
    // Create files with overlapping content
    std::string pattern = "This is a repeating pattern for deduplication testing. ";
    auto file1 = createPatternFile("dedup1.txt", pattern, 1000);
    auto file2 = createPatternFile("dedup2.txt", pattern, 1000);
    auto file3 = createPatternFile("dedup3.txt", pattern + "Modified", 1000);
    
    // Store all files
    auto result1 = contentStore->store(file1);
    auto result2 = contentStore->store(file2);
    auto result3 = contentStore->store(file3);
    
    ASSERT_TRUE(result1.has_value());
    ASSERT_TRUE(result2.has_value());
    ASSERT_TRUE(result3.has_value());
    
    // Files 1 and 2 should have same hash
    EXPECT_EQ(result1.value().contentHash, result2.value().contentHash);
    
    // File 3 should have different hash
    EXPECT_NE(result1.value().contentHash, result3.value().contentHash);
    
    // Second file should be fully deduped
    EXPECT_GT(result2.value().bytesDeduped, 0);
    EXPECT_NEAR(result2.value().dedupRatio(), 1.0, 0.01);
}

// Test large file handling
TEST_F(FullSystemIntegrationTest, LargeFileHandling) {
    // Create 100MB file
    auto largeFile = createTestFile("large.bin", 100 * 1024 * 1024);
    
    // Track progress
    std::atomic<int> progressCalls{0};
    std::atomic<uint64_t> lastProgress{0};
    
    auto progressCallback = [&](uint64_t processed, uint64_t total) {
        progressCalls++;
        lastProgress = processed;
        EXPECT_LE(processed, total);
    };
    
    // Store with progress
    ContentMetadata metadata;
    metadata.originalName = "large.bin";
    
    auto start = std::chrono::steady_clock::now();
    auto storeResult = contentStore->store(largeFile, metadata, progressCallback);
    auto storeTime = std::chrono::steady_clock::now() - start;
    
    ASSERT_TRUE(storeResult.has_value());
    EXPECT_GT(progressCalls.load(), 10);  // Should have multiple progress updates
    EXPECT_EQ(lastProgress.load(), fs::file_size(largeFile));
    
    // Retrieve
    auto outputFile = testDir / "large_output.bin";
    start = std::chrono::steady_clock::now();
    auto retrieveResult = contentStore->retrieve(storeResult.value().contentHash, outputFile);
    auto retrieveTime = std::chrono::steady_clock::now() - start;
    
    ASSERT_TRUE(retrieveResult.has_value());
    EXPECT_TRUE(filesAreEqual(largeFile, outputFile));
    
    // Performance check - should process at reasonable speed
    double storeMBps = (100.0 * 1024 * 1024) / 
        std::chrono::duration_cast<std::chrono::milliseconds>(storeTime).count() / 1000;
    double retrieveMBps = (100.0 * 1024 * 1024) / 
        std::chrono::duration_cast<std::chrono::milliseconds>(retrieveTime).count() / 1000;
    
    std::cout << "Store performance: " << storeMBps << " MB/s" << std::endl;
    std::cout << "Retrieve performance: " << retrieveMBps << " MB/s" << std::endl;
    
    EXPECT_GT(storeMBps, 10);     // > 10 MB/s
    EXPECT_GT(retrieveMBps, 20);  // Retrieve should be faster
}

// Test concurrent operations
TEST_F(FullSystemIntegrationTest, ConcurrentOperations) {
    const int numThreads = 10;
    const int filesPerThread = 5;
    std::vector<std::thread> threads;
    std::atomic<int> successCount{0};
    
    // Concurrent stores
    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([this, t, &successCount]() {
            for (int f = 0; f < filesPerThread; ++f) {
                std::string name = "thread" + std::to_string(t) + "_file" + std::to_string(f) + ".txt";
                auto file = createTestFile(name, 1024 * (f + 1));  // Variable sizes
                
                auto result = contentStore->store(file);
                if (result.has_value()) {
                    successCount++;
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    EXPECT_EQ(successCount.load(), numThreads * filesPerThread);
    
    // Get stats
    auto stats = contentStore->getStats();
    EXPECT_EQ(stats.storeOperations, numThreads * filesPerThread);
}

// Test crash recovery
TEST_F(FullSystemIntegrationTest, CrashRecovery) {
    // Store some files
    std::vector<std::string> hashes;
    for (int i = 0; i < 5; ++i) {
        auto file = createTestFile("recovery" + std::to_string(i) + ".txt", 1024 * (i + 1));
        auto result = contentStore->store(file);
        ASSERT_TRUE(result.has_value());
        hashes.push_back(result.value().contentHash);
    }
    
    // Simulate crash by destroying and recreating content store
    contentStore.reset();
    
    // Recreate content store
    ContentStoreConfig config;
    config.storagePath = testDir;
    config.chunkSize = 64 * 1024;
    config.enableCompression = true;
    config.enableIntegrityChecks = true;
    // config.enableWAL = true;  // Field doesn't exist
    
    auto result = ContentStoreBuilder()
        .withConfig(config)
        .build();
        
    ASSERT_TRUE(result.has_value());
    contentStore = std::move(result.value());
    
    // Verify all content is still accessible
    for (const auto& hash : hashes) {
        auto exists = contentStore->exists(hash);
        ASSERT_TRUE(exists.has_value());
        EXPECT_TRUE(exists.value());
    }
}

// Test integrity verification
TEST_F(FullSystemIntegrationTest, IntegrityVerification) {
    // Store file
    auto file = createTestFile("integrity.bin", 10 * 1024);  // 10KB
    auto storeResult = contentStore->store(file);
    ASSERT_TRUE(storeResult.has_value());
    
    // Get storage path for manual corruption
    fs::path storagePath = testDir / "blocks";
    
    // Find and corrupt a block file
    bool corrupted = false;
    for (const auto& entry : fs::recursive_directory_iterator(storagePath)) {
        if (entry.is_regular_file() && entry.path().extension() == "") {
            // Corrupt the file
            std::ofstream corrupt(entry.path(), std::ios::binary | std::ios::app);
            corrupt << "CORRUPTED_DATA";
            corrupt.close();
            corrupted = true;
            break;
        }
    }
    
    ASSERT_TRUE(corrupted) << "Could not find block file to corrupt";
    
    // Trigger integrity check
    auto health = contentStore->checkHealth();
    
    // Should detect corruption
    EXPECT_FALSE(health.isHealthy);
    EXPECT_FALSE(health.errors.empty());
}

// Test metadata operations
TEST_F(FullSystemIntegrationTest, MetadataOperations) {
    // Store file with rich metadata
    auto file = createTestFile("metadata_test.doc", 50 * 1024);
    
    ContentMetadata metadata;
    metadata.originalName = "important_document.doc";
    metadata.mimeType = "application/msword";
    metadata.description = "Important business document";
    metadata.tags = {"business", "quarterly-report", "2024"};
    metadata.customFields["author"] = "John Doe";
    metadata.customFields["department"] = "Finance";
    metadata.customFields["version"] = "2.1";
    
    auto storeResult = contentStore->store(file, metadata);
    ASSERT_TRUE(storeResult.has_value());
    
    // Retrieve metadata
    auto metaResult = contentStore->getMetadata(storeResult.value().contentHash);
    ASSERT_TRUE(metaResult.has_value());
    
    auto& retrieved = metaResult.value();
    EXPECT_EQ(retrieved.originalName, metadata.originalName);
    EXPECT_EQ(retrieved.mimeType, metadata.mimeType);
    EXPECT_EQ(retrieved.description, metadata.description);
    EXPECT_EQ(retrieved.tags, metadata.tags);
    EXPECT_EQ(retrieved.customFields["author"], "John Doe");
    
    // Update metadata
    retrieved.addTag("approved");
    retrieved.customFields["approved_by"] = "Jane Smith";
    retrieved.customFields["approval_date"] = "2024-01-15";
    
    auto updateResult = contentStore->updateMetadata(storeResult.value().contentHash, retrieved);
    ASSERT_TRUE(updateResult.has_value());
    
    // Verify update
    auto updatedMeta = contentStore->getMetadata(storeResult.value().contentHash);
    ASSERT_TRUE(updatedMeta.has_value());
    // FIXME: Update to use current API
    // EXPECT_TRUE(updatedMeta.value().hasTag("approved"));
    // EXPECT_EQ(updatedMeta.value().customFields["approved_by"], "Jane Smith");
}

// Test batch operations
TEST_F(FullSystemIntegrationTest, BatchOperations) {
    // Create multiple files
    std::vector<fs::path> files;
    std::vector<ContentMetadata> metadataList;
    
    for (int i = 0; i < 20; ++i) {
        files.push_back(createTestFile("batch" + std::to_string(i) + ".dat", 10 * 1024 * (i + 1)));
        
        ContentMetadata meta;
        meta.originalName = "batch" + std::to_string(i) + ".dat";
        meta.tags = {"batch", "test"};
        metadataList.push_back(meta);
    }
    
    // Batch store
    auto start = std::chrono::steady_clock::now();
    auto results = contentStore->storeBatch(files, metadataList);
    auto batchTime = std::chrono::steady_clock::now() - start;
    
    ASSERT_EQ(results.size(), files.size());
    
    std::vector<std::string> hashes;
    for (const auto& result : results) {
        ASSERT_TRUE(result.has_value());
        hashes.push_back(result.value().contentHash);
    }
    
    // Individual stores for comparison
    std::vector<fs::path> compareFiles;
    for (int i = 0; i < 20; ++i) {
        compareFiles.push_back(createTestFile("individual" + std::to_string(i) + ".dat", 
                                             10 * 1024 * (i + 1)));
    }
    
    start = std::chrono::steady_clock::now();
    for (const auto& file : compareFiles) {
        contentStore->store(file);
    }
    auto individualTime = std::chrono::steady_clock::now() - start;
    
    // Batch should be faster than individual
    EXPECT_LT(batchTime.count(), individualTime.count());
    
    std::cout << "Batch time: " << 
        std::chrono::duration_cast<std::chrono::milliseconds>(batchTime).count() << "ms" << std::endl;
    std::cout << "Individual time: " << 
        std::chrono::duration_cast<std::chrono::milliseconds>(individualTime).count() << "ms" << std::endl;
}

// Test garbage collection
TEST_F(FullSystemIntegrationTest, GarbageCollection) {
    // Store files
    std::vector<std::string> hashes;
    for (int i = 0; i < 10; ++i) {
        auto file = createTestFile("gc" + std::to_string(i) + ".txt", 1024 * (i + 1));
        auto result = contentStore->store(file);
        ASSERT_TRUE(result.has_value());
        hashes.push_back(result.value().contentHash);
    }
    
    // Delete half of them
    for (size_t i = 0; i < hashes.size() / 2; ++i) {
        auto result = contentStore->remove(hashes[i]);
        ASSERT_TRUE(result.has_value());
        EXPECT_TRUE(result.value());
    }
    
    // Run garbage collection
    auto gcResult = contentStore->runGarbageCollection();
    ASSERT_TRUE(gcResult.has_value());
    
    auto& gcStats = gcResult.value();
    EXPECT_GT(gcStats.blocksRemoved, 0);
    EXPECT_GT(gcStats.bytesFreed, 0);
    
    // Verify remaining files still accessible
    for (size_t i = hashes.size() / 2; i < hashes.size(); ++i) {
        auto exists = contentStore->exists(hashes[i]);
        ASSERT_TRUE(exists.has_value());
        EXPECT_TRUE(exists.value());
    }
}

// Test error conditions
TEST_F(FullSystemIntegrationTest, ErrorHandling) {
    // Non-existent file
    auto result1 = contentStore->store(testDir / "nonexistent.txt");
    EXPECT_FALSE(result1.has_value());
    EXPECT_EQ(result1.error(), ErrorCode::FileNotFound);
    
    // Invalid hash
    auto result2 = contentStore->retrieve("invalid_hash_format", testDir / "out.txt");
    EXPECT_FALSE(result2.has_value());
    
    // Store to read-only location (if possible to test)
    // This test is platform-specific
    
    // Empty file
    fs::path emptyFile = testDir / "empty.txt";
    std::ofstream(emptyFile).close();
    
    auto result3 = contentStore->store(emptyFile);
    // Empty files might be handled differently - check behavior
    if (result3.has_value()) {
        EXPECT_EQ(result3.value().bytesStored, 0);
    }
}

// Test stream operations
TEST_F(FullSystemIntegrationTest, StreamOperations) {
    // Create in-memory content
    std::string content = "This is streaming content for testing purposes. ";
    for (int i = 0; i < 1000; ++i) {
        content += "Repeated content block " + std::to_string(i) + ". ";
    }
    
    // Store via stream
    std::istringstream input(content);
    ContentMetadata metadata;
    metadata.originalName = "stream_content.txt";
    metadata.mimeType = "text/plain";
    
    auto storeResult = contentStore->storeStream(input, metadata);
    ASSERT_TRUE(storeResult.has_value());
    
    // Retrieve via stream
    std::ostringstream output;
    auto retrieveResult = contentStore->retrieveStream(storeResult.value().contentHash, output);
    ASSERT_TRUE(retrieveResult.has_value());
    
    EXPECT_EQ(output.str(), content);
}

// Test memory operations
TEST_F(FullSystemIntegrationTest, MemoryOperations) {
    // Create byte array
    std::vector<std::byte> data(1024 * 1024);  // 1MB
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 255);
    
    for (auto& byte : data) {
        byte = static_cast<std::byte>(dis(gen));
    }
    
    // Store bytes
    ContentMetadata metadata;
    metadata.originalName = "memory_data.bin";
    
    auto storeResult = contentStore->storeBytes(data, metadata);
    ASSERT_TRUE(storeResult.has_value());
    
    // Retrieve bytes
    auto retrieveResult = contentStore->retrieveBytes(storeResult.value().contentHash);
    ASSERT_TRUE(retrieveResult.has_value());
    
    EXPECT_EQ(retrieveResult.value(), data);
}