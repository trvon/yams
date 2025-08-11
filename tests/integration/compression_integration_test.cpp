#include <gtest/gtest.h>
#include <yams/storage/storage_engine.h>
#include <yams/compression/compressor_interface.h>
#include <yams/compression/compression_policy.h>
#include <yams/compression/compression_header.h>
#include <yams/api/content_metadata.h>
#include <yams/core/types.h>
#include <filesystem>
#include <random>
#include <thread>
#include <chrono>

using namespace yams;
using namespace yams::storage;
using namespace yams::compression;
using namespace yams::api;

class CompressionIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create temporary test directory
        testDir_ = std::filesystem::temp_directory_path() / "kronos_comp_integration_test";
        std::filesystem::create_directories(testDir_);
        
        // Initialize storage engine
        StorageEngine::Config config;
        config.dataPath = testDir_.string();
        config.enableCompression = true;
        config.compressionPolicy = std::make_shared<CompressionPolicy>();
        
        storageEngine_ = std::make_unique<StorageEngine>(config);
        auto result = storageEngine_->initialize();
        ASSERT_TRUE(result.has_value()) << "Failed to initialize storage engine";
    }
    
    void TearDown() override {
        storageEngine_.reset();
        std::filesystem::remove_all(testDir_);
    }
    
    std::vector<std::byte> generateTestData(size_t size, const std::string& pattern = "random") {
        std::vector<std::byte> data(size);
        
        if (pattern == "random") {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> dis(0, 255);
            for (auto& b : data) {
                b = std::byte{static_cast<uint8_t>(dis(gen))};
            }
        } else if (pattern == "text") {
            const char* text = "The quick brown fox jumps over the lazy dog. ";
            size_t textLen = std::strlen(text);
            for (size_t i = 0; i < size; ++i) {
                data[i] = std::byte{static_cast<uint8_t>(text[i % textLen])};
            }
        } else if (pattern == "zeros") {
            std::fill(data.begin(), data.end(), std::byte{0});
        }
        
        return data;
    }
    
    ContentMetadata createMetadata(const std::string& name, size_t size, 
                                  const std::string& mimeType = "application/octet-stream") {
        ContentMetadata metadata;
        metadata.id = "test-" + name;
        metadata.name = name;
        metadata.size = size;
        metadata.mimeType = mimeType;
        metadata.createdAt = std::chrono::system_clock::now();
        metadata.modifiedAt = metadata.createdAt;
        metadata.accessedAt = metadata.createdAt;
        return metadata;
    }
    
    std::filesystem::path testDir_;
    std::unique_ptr<StorageEngine> storageEngine_;
};

// Test storing and retrieving compressed data
TEST_F(CompressionIntegrationTest, StoreAndRetrieveCompressed) {
    // Create compressible text data
    auto data = generateTestData(10 * 1024, "text"); // 10KB of text
    auto metadata = createMetadata("document.txt", data.size(), "text/plain");
    
    // Store the data
    auto storeResult = storageEngine_->store(data, metadata);
    ASSERT_TRUE(storeResult.has_value()) << "Failed to store data";
    
    std::string contentId = storeResult.value();
    
    // Retrieve the data
    auto retrieveResult = storageEngine_->retrieve(contentId);
    ASSERT_TRUE(retrieveResult.has_value()) << "Failed to retrieve data";
    
    // Verify data integrity
    EXPECT_EQ(retrieveResult.value(), data) << "Retrieved data doesn't match original";
    
    // Check if data was actually compressed (by checking storage size)
    auto stats = storageEngine_->getStatistics();
    EXPECT_GT(stats.totalBytesStored, 0);
    EXPECT_LT(stats.compressedBytesStored, data.size()) << "Data should be compressed";
}

// Test mixed compressed and uncompressed data
TEST_F(CompressionIntegrationTest, MixedCompressedUncompressed) {
    struct TestCase {
        std::string name;
        size_t size;
        std::string mimeType;
        std::string pattern;
        bool shouldCompress;
    };
    
    std::vector<TestCase> testCases = {
        {"small.txt", 100, "text/plain", "text", false},          // Too small
        {"large.txt", 20 * 1024, "text/plain", "text", true},     // Should compress
        {"image.jpg", 50 * 1024, "image/jpeg", "random", false},  // Already compressed
        {"data.json", 15 * 1024, "application/json", "text", true}, // Compressible
        {"archive.zip", 30 * 1024, "application/zip", "random", false} // Already compressed
    };
    
    std::vector<std::string> contentIds;
    
    // Store all test cases
    for (const auto& tc : testCases) {
        auto data = generateTestData(tc.size, tc.pattern);
        auto metadata = createMetadata(tc.name, tc.size, tc.mimeType);
        
        auto result = storageEngine_->store(data, metadata);
        ASSERT_TRUE(result.has_value()) << "Failed to store " << tc.name;
        contentIds.push_back(result.value());
    }
    
    // Retrieve and verify all
    for (size_t i = 0; i < testCases.size(); ++i) {
        const auto& tc = testCases[i];
        auto data = generateTestData(tc.size, tc.pattern);
        
        auto result = storageEngine_->retrieve(contentIds[i]);
        ASSERT_TRUE(result.has_value()) << "Failed to retrieve " << tc.name;
        EXPECT_EQ(result.value(), data) << "Data mismatch for " << tc.name;
    }
}

// Test concurrent compression operations
TEST_F(CompressionIntegrationTest, ConcurrentCompression) {
    const int numThreads = 5;
    const int filesPerThread = 10;
    std::vector<std::thread> threads;
    std::atomic<int> successCount{0};
    std::atomic<int> failureCount{0};
    
    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&, t]() {
            for (int f = 0; f < filesPerThread; ++f) {
                // Create unique data for each file
                std::string name = "thread" + std::to_string(t) + "_file" + std::to_string(f) + ".txt";
                auto data = generateTestData(5 * 1024 + t * 1000 + f * 100, "text");
                auto metadata = createMetadata(name, data.size(), "text/plain");
                
                // Store
                auto storeResult = storageEngine_->store(data, metadata);
                if (!storeResult.has_value()) {
                    failureCount++;
                    continue;
                }
                
                // Retrieve immediately
                auto retrieveResult = storageEngine_->retrieve(storeResult.value());
                if (retrieveResult.has_value() && retrieveResult.value() == data) {
                    successCount++;
                } else {
                    failureCount++;
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    EXPECT_EQ(successCount, numThreads * filesPerThread);
    EXPECT_EQ(failureCount, 0);
}

// Test compression policy application
TEST_F(CompressionIntegrationTest, CompressionPolicyApplication) {
    // Create custom policy that compresses everything > 1KB
    CompressionPolicy::Rules rules;
    rules.neverCompressBelow = 1024;
    rules.alwaysCompressAbove = 1024;
    rules.compressAfterAge = std::chrono::hours(0); // Compress immediately
    rules.neverCompressBefore = std::chrono::hours(0);
    
    auto customPolicy = std::make_shared<CompressionPolicy>(rules);
    storageEngine_->updateCompressionPolicy(customPolicy);
    
    // Test different sizes
    std::vector<std::pair<size_t, bool>> testCases = {
        {500, false},    // Below threshold
        {1500, true},    // Above threshold
        {5000, true},    // Well above threshold
    };
    
    for (const auto& [size, shouldCompress] : testCases) {
        auto data = generateTestData(size, "text");
        auto metadata = createMetadata("test" + std::to_string(size) + ".txt", size, "text/plain");
        
        auto result = storageEngine_->store(data, metadata);
        ASSERT_TRUE(result.has_value()) << "Failed to store size " << size;
        
        // Verify compression happened based on policy
        auto stats = storageEngine_->getContentStats(result.value());
        if (shouldCompress) {
            EXPECT_TRUE(stats.isCompressed) << "Size " << size << " should be compressed";
            EXPECT_LT(stats.compressedSize, size) << "Compressed size should be smaller";
        } else {
            EXPECT_FALSE(stats.isCompressed) << "Size " << size << " should not be compressed";
        }
    }
}

// Test storage with compression during high load
TEST_F(CompressionIntegrationTest, HighLoadCompression) {
    const int totalFiles = 100;
    const size_t fileSize = 10 * 1024; // 10KB each
    std::vector<std::string> contentIds;
    std::vector<std::vector<std::byte>> originalData;
    
    // Store many files rapidly
    auto startTime = std::chrono::steady_clock::now();
    
    for (int i = 0; i < totalFiles; ++i) {
        auto data = generateTestData(fileSize, i % 2 == 0 ? "text" : "random");
        originalData.push_back(data);
        
        auto metadata = createMetadata("file" + std::to_string(i) + ".dat", fileSize);
        auto result = storageEngine_->store(data, metadata);
        ASSERT_TRUE(result.has_value()) << "Failed to store file " << i;
        contentIds.push_back(result.value());
    }
    
    auto storeTime = std::chrono::steady_clock::now() - startTime;
    
    // Retrieve all files
    startTime = std::chrono::steady_clock::now();
    
    for (int i = 0; i < totalFiles; ++i) {
        auto result = storageEngine_->retrieve(contentIds[i]);
        ASSERT_TRUE(result.has_value()) << "Failed to retrieve file " << i;
        EXPECT_EQ(result.value(), originalData[i]) << "Data mismatch for file " << i;
    }
    
    auto retrieveTime = std::chrono::steady_clock::now() - startTime;
    
    // Log performance metrics
    auto storeMs = std::chrono::duration_cast<std::chrono::milliseconds>(storeTime).count();
    auto retrieveMs = std::chrono::duration_cast<std::chrono::milliseconds>(retrieveTime).count();
    
    std::cout << "High load test - Store time: " << storeMs << "ms, Retrieve time: " 
              << retrieveMs << "ms" << std::endl;
    
    // Check compression statistics
    auto stats = storageEngine_->getStatistics();
    EXPECT_GT(stats.compressionOperations, 0) << "Some files should have been compressed";
    EXPECT_GT(stats.decompressionOperations, 0) << "Some files should have been decompressed";
}

// Test recovery after compression failure
TEST_F(CompressionIntegrationTest, CompressionFailureRecovery) {
    // This test would require injecting failures, which might need mock support
    // For now, test that uncompressible data still works
    
    // Generate highly random (uncompressible) data
    auto data = generateTestData(10 * 1024, "random");
    auto metadata = createMetadata("random.bin", data.size(), "application/octet-stream");
    
    // Even if compression fails or doesn't help, storage should succeed
    auto storeResult = storageEngine_->store(data, metadata);
    ASSERT_TRUE(storeResult.has_value()) << "Storage should succeed even with poor compression";
    
    // Retrieve and verify
    auto retrieveResult = storageEngine_->retrieve(storeResult.value());
    ASSERT_TRUE(retrieveResult.has_value()) << "Retrieval should succeed";
    EXPECT_EQ(retrieveResult.value(), data) << "Data integrity maintained";
}

// Test updating compression policy on live system
TEST_F(CompressionIntegrationTest, LivePolicyUpdate) {
    // Store some data with default policy
    auto data1 = generateTestData(5 * 1024, "text");
    auto metadata1 = createMetadata("before.txt", data1.size(), "text/plain");
    auto id1 = storageEngine_->store(data1, metadata1).value();
    
    // Update policy to be more aggressive
    CompressionPolicy::Rules newRules;
    newRules.neverCompressBelow = 100; // Very low threshold
    newRules.defaultZstdLevel = 9; // High compression
    auto newPolicy = std::make_shared<CompressionPolicy>(newRules);
    storageEngine_->updateCompressionPolicy(newPolicy);
    
    // Store more data with new policy
    auto data2 = generateTestData(5 * 1024, "text");
    auto metadata2 = createMetadata("after.txt", data2.size(), "text/plain");
    auto id2 = storageEngine_->store(data2, metadata2).value();
    
    // Both should still be retrievable
    EXPECT_TRUE(storageEngine_->retrieve(id1).has_value());
    EXPECT_TRUE(storageEngine_->retrieve(id2).has_value());
    
    // New data might have different compression characteristics
    auto stats1 = storageEngine_->getContentStats(id1);
    auto stats2 = storageEngine_->getContentStats(id2);
    
    // Both should be compressed given their size
    EXPECT_TRUE(stats1.isCompressed);
    EXPECT_TRUE(stats2.isCompressed);
}

// Test data integrity with compression
TEST_F(CompressionIntegrationTest, DataIntegrityVerification) {
    // Test with various data patterns to ensure integrity
    std::vector<std::string> patterns = {"zeros", "text", "random"};
    
    for (const auto& pattern : patterns) {
        // Generate data with specific pattern
        auto data = generateTestData(20 * 1024, pattern);
        
        // Calculate checksum before storage
        uint32_t checksumBefore = 0;
        for (const auto& byte : data) {
            checksumBefore = (checksumBefore << 1) ^ static_cast<uint8_t>(byte);
        }
        
        // Store
        auto metadata = createMetadata(pattern + ".dat", data.size());
        auto result = storageEngine_->store(data, metadata);
        ASSERT_TRUE(result.has_value()) << "Failed to store " << pattern;
        
        // Retrieve
        auto retrieved = storageEngine_->retrieve(result.value());
        ASSERT_TRUE(retrieved.has_value()) << "Failed to retrieve " << pattern;
        
        // Calculate checksum after retrieval
        uint32_t checksumAfter = 0;
        for (const auto& byte : retrieved.value()) {
            checksumAfter = (checksumAfter << 1) ^ static_cast<uint8_t>(byte);
        }
        
        EXPECT_EQ(checksumBefore, checksumAfter) << "Checksum mismatch for " << pattern;
        EXPECT_EQ(data, retrieved.value()) << "Data mismatch for " << pattern;
    }
}