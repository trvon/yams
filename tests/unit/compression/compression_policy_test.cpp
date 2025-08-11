#include <gtest/gtest.h>
#include <yams/compression/compression_policy.h>
#include <yams/api/content_metadata.h>
#include <thread>

using namespace yams;
using namespace yams::compression;
using namespace yams::api;

class CompressionPolicyTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create default policy
        policy_ = std::make_unique<CompressionPolicy>();
    }
    
    // Helper to create test metadata
    ContentMetadata createMetadata(
        const std::string& name,
        uint64_t size,
        const std::string& mimeType = "text/plain") {
        
        ContentMetadata metadata;
        metadata.id = "test-" + name;
        metadata.name = name;
        metadata.size = size;
        metadata.mimeType = mimeType;
        metadata.contentHash = std::string("0123456789abcdef");
        metadata.createdAt = std::chrono::system_clock::now();
        metadata.modifiedAt = metadata.createdAt;
        metadata.accessedAt = metadata.createdAt;
        return metadata;
    }
    
    // Helper to create access pattern
    AccessPattern createAccessPattern(
        std::chrono::hours ageHours,
        size_t accessCount = 10) {
        
        auto now = std::chrono::system_clock::now();
        AccessPattern pattern;
        pattern.created = now - ageHours;
        pattern.lastAccessed = now - ageHours;  // Use ageHours for lastAccessed too
        pattern.accessCount = accessCount;
        pattern.readCount = accessCount;
        return pattern;
    }
    
    std::unique_ptr<CompressionPolicy> policy_;
};

TEST_F(CompressionPolicyTest, DefaultRules) {
    auto rules = policy_->rules();
    
    // Check some default values
    EXPECT_EQ(rules.compressAfterAge, std::chrono::hours(24 * 7));
    EXPECT_EQ(rules.archiveAfterAge, std::chrono::hours(24 * 30));
    EXPECT_EQ(rules.neverCompressBelow, 4096);
    EXPECT_EQ(rules.alwaysCompressAbove, 10 * 1024 * 1024);
}

TEST_F(CompressionPolicyTest, TooNewFile) {
    auto metadata = createMetadata("test.txt", 100000);
    auto pattern = createAccessPattern(std::chrono::hours(0));  // Just created
    
    auto decision = policy_->shouldCompress(metadata, pattern);
    EXPECT_FALSE(decision.shouldCompress);
    EXPECT_TRUE(decision.reason.find("File too new") != std::string::npos);
}

TEST_F(CompressionPolicyTest, TooSmallFile) {
    auto metadata = createMetadata("test.txt", 1000);  // < 4KB
    auto pattern = createAccessPattern(std::chrono::hours(48));
    
    auto decision = policy_->shouldCompress(metadata, pattern);
    EXPECT_FALSE(decision.shouldCompress);
    EXPECT_TRUE(decision.reason.find("too small") != std::string::npos);
}

TEST_F(CompressionPolicyTest, LargeFileShouldCompress) {
    auto metadata = createMetadata("large.txt", 20 * 1024 * 1024);  // 20MB
    auto pattern = createAccessPattern(std::chrono::hours(48));
    
    auto decision = policy_->shouldCompress(metadata, pattern);
    EXPECT_TRUE(decision.shouldCompress);
    EXPECT_TRUE(decision.reason.find("Large file") != std::string::npos);
}

TEST_F(CompressionPolicyTest, OldFileShouldUseLZMA) {
    auto metadata = createMetadata("old.txt", 1024 * 1024);  // 1MB
    auto pattern = createAccessPattern(std::chrono::hours(24 * 35));  // 35 days old
    pattern.accessCount = 2;  // Rarely accessed
    
    auto decision = policy_->shouldCompress(metadata, pattern);
    EXPECT_TRUE(decision.shouldCompress);
    EXPECT_EQ(decision.algorithm, CompressionAlgorithm::LZMA);
    EXPECT_TRUE(decision.reason.find("Old file") != std::string::npos);
}

TEST_F(CompressionPolicyTest, ActiveFileShouldUseZstandard) {
    auto metadata = createMetadata("active.txt", 1024 * 1024);  // 1MB
    auto pattern = createAccessPattern(std::chrono::hours(24 * 3));  // 3 days old
    pattern.accessCount = 100;  // Frequently accessed
    
    auto decision = policy_->shouldCompress(metadata, pattern);
    EXPECT_TRUE(decision.shouldCompress);
    EXPECT_EQ(decision.algorithm, CompressionAlgorithm::Zstandard);
}

TEST_F(CompressionPolicyTest, ExcludedMimeTypes) {
    // Test with JPEG image
    auto metadata = createMetadata("photo.jpg", 5 * 1024 * 1024, "image/jpeg");
    auto pattern = createAccessPattern(std::chrono::hours(24 * 10));
    
    auto decision = policy_->shouldCompress(metadata, pattern);
    EXPECT_FALSE(decision.shouldCompress);
    EXPECT_TRUE(decision.reason.find("Content appears to be already compressed") != std::string::npos);
}

TEST_F(CompressionPolicyTest, ExcludedExtensions) {
    // Test with ZIP file
    auto metadata = createMetadata("archive.zip", 5 * 1024 * 1024, "application/octet-stream");
    auto pattern = createAccessPattern(std::chrono::hours(24 * 10));
    
    auto decision = policy_->shouldCompress(metadata, pattern);
    EXPECT_FALSE(decision.shouldCompress);
    EXPECT_TRUE(decision.reason.find("Content appears to be already compressed") != std::string::npos);
}

TEST_F(CompressionPolicyTest, CompressibleTypes) {
    // Test various compressible types
    std::vector<std::pair<std::string, std::string>> testCases = {
        {"document.txt", "text/plain"},
        {"data.json", "application/json"},
        {"page.html", "text/html"},
        {"style.css", "text/css"},
        {"config.xml", "application/xml"},
        {"script.js", "application/javascript"}
    };
    
    for (const auto& [filename, mimeType] : testCases) {
        auto metadata = createMetadata(filename, 100 * 1024, mimeType);
        auto pattern = createAccessPattern(std::chrono::hours(24 * 10));
        
        auto decision = policy_->shouldCompress(metadata, pattern);
        EXPECT_TRUE(decision.shouldCompress) 
            << "File " << filename << " with type " << mimeType << " should be compressible";
    }
}

TEST_F(CompressionPolicyTest, FileTemperatureClassification) {
    auto metadata = createMetadata("test.txt", 1024 * 1024);
    
    // Hot file - many accesses per day
    {
        auto pattern = createAccessPattern(std::chrono::hours(24 * 5));
        pattern.accessCount = 500;  // 100 accesses/day
        
        auto decision = policy_->shouldCompress(metadata, pattern);
        EXPECT_TRUE(decision.shouldCompress);
        EXPECT_EQ(decision.algorithm, CompressionAlgorithm::Zstandard);
        EXPECT_EQ(decision.level, 1);  // Fast compression for hot files
    }
    
    // Cold file - few accesses
    {
        auto pattern = createAccessPattern(std::chrono::hours(24 * 20));
        pattern.accessCount = 2;  // 0.1 accesses/day
        
        auto decision = policy_->shouldCompress(metadata, pattern);
        EXPECT_TRUE(decision.shouldCompress);
        EXPECT_EQ(decision.algorithm, CompressionAlgorithm::LZMA);
    }
}

TEST_F(CompressionPolicyTest, CustomRules) {
    CompressionPolicy::Rules customRules;
    customRules.compressAfterAge = std::chrono::hours(1);
    customRules.neverCompressBelow = 1024;  // 1KB
    customRules.alwaysCompressAbove = 1024 * 1024;  // 1MB
    customRules.defaultZstdLevel = 5;
    customRules.defaultLzmaLevel = 7;
    
    CompressionPolicy customPolicy(customRules);
    
    // Test with custom rules
    auto metadata = createMetadata("test.txt", 2048);  // 2KB
    auto pattern = createAccessPattern(std::chrono::hours(2));
    
    auto decision = customPolicy.shouldCompress(metadata, pattern);
    EXPECT_TRUE(decision.shouldCompress);
}

TEST_F(CompressionPolicyTest, AlgorithmSelection) {
    auto metadata = createMetadata("test.txt", 100 * 1024 * 1024);  // 100MB
    
    // Large file with low access should use LZMA
    {
        auto pattern = createAccessPattern(std::chrono::hours(24 * 100));  // 100 days old
        pattern.accessCount = 1;  // 0.01 accesses/day (truly cold)
        
        auto algo = policy_->selectAlgorithm(metadata, pattern);
        EXPECT_EQ(algo, CompressionAlgorithm::LZMA);
    }
    
    // Large file with high access should use Zstandard
    {
        auto pattern = createAccessPattern(std::chrono::hours(24 * 10));
        pattern.accessCount = 200;  // 20 accesses/day
        
        auto algo = policy_->selectAlgorithm(metadata, pattern);
        EXPECT_EQ(algo, CompressionAlgorithm::Zstandard);
    }
}

TEST_F(CompressionPolicyTest, ThreadSafety) {
    // Test concurrent access to policy
    const int numThreads = 10;
    const int numOperations = 1000;
    std::vector<std::thread> threads;
    std::atomic<int> successCount{0};
    
    auto metadata = createMetadata("test.txt", 1024 * 1024);
    auto pattern = createAccessPattern(std::chrono::hours(24 * 10));
    
    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&]() {
            for (int j = 0; j < numOperations; ++j) {
                // Alternate between reads and writes
                if (j % 10 == 0) {
                    // Update rules occasionally
                    auto rules = policy_->rules();
                    rules.defaultZstdLevel = static_cast<uint8_t>((j % 9) + 1);
                    policy_->updateRules(rules);
                } else {
                    // Read by making decisions
                    auto decision = policy_->shouldCompress(metadata, pattern);
                    if (decision.shouldCompress) {
                        successCount++;
                    }
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // Should have made many successful compression decisions
    EXPECT_GT(successCount, 0);
}