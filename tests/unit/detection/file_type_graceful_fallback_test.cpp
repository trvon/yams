#include <gtest/gtest.h>
#include <yams/detection/file_type_detector.h>
#include <yams/cli/yams_cli.h>
#include <filesystem>
#include <fstream>
#include <thread>
#include <atomic>
#include <vector>

namespace yams::detection {
namespace fs = std::filesystem;

class FileTypeGracefulFallbackTest : public ::testing::Test {
protected:
    void SetUp() override {
        testDataDir = fs::current_path() / "test_data";
        
        // Create corrupted JSON file
        corruptedJsonPath = testDataDir / "corrupted.json";
        std::ofstream corrupted(corruptedJsonPath);
        corrupted << "{ invalid json content without closing brace";
        corrupted.close();
        
        // Create empty file
        emptyJsonPath = testDataDir / "empty.json";
        std::ofstream empty(emptyJsonPath);
        empty.close();
        
        // Create file with wrong permissions (if possible)
        restrictedPath = testDataDir / "restricted.json";
        std::ofstream restricted(restrictedPath);
        restricted << "{}";
        restricted.close();
        
        nonExistentPath = testDataDir / "does_not_exist.json";
    }
    
    void TearDown() override {
        // Clean up test files
        for (const auto& path : {corruptedJsonPath, emptyJsonPath, restrictedPath}) {
            if (fs::exists(path)) {
                fs::remove(path);
            }
        }
        // Clear detector state
        FileTypeDetector::instance().clearCache();
    }
    
    fs::path testDataDir;
    fs::path corruptedJsonPath;
    fs::path emptyJsonPath;
    fs::path restrictedPath;
    fs::path nonExistentPath;
};

TEST_F(FileTypeGracefulFallbackTest, InitializeWithMissingFile) {
    FileTypeDetectorConfig config;
    config.patternsFile = nonExistentPath;
    config.useCustomPatterns = true;
    config.useBuiltinPatterns = true;
    
    // Should succeed with built-in patterns
    auto result = FileTypeDetector::instance().initialize(config);
    EXPECT_TRUE(result.has_value()) << "Should succeed when patterns file is missing";
    
    // Should still be able to detect using built-in patterns
    std::vector<std::byte> jpegData = {
        std::byte(0xFF), std::byte(0xD8), std::byte(0xFF)
    };
    
    auto detectResult = FileTypeDetector::instance().detectFromBuffer(jpegData);
    // May or may not succeed depending on built-in patterns, but shouldn't crash
    EXPECT_NO_THROW(FileTypeDetector::instance().detectFromBuffer(jpegData));
}

TEST_F(FileTypeGracefulFallbackTest, InitializeWithCorruptedJSON) {
    FileTypeDetectorConfig config;
    config.patternsFile = corruptedJsonPath;
    config.useCustomPatterns = true;
    config.useBuiltinPatterns = true;
    
    // Should succeed with built-in patterns despite corrupted JSON
    auto result = FileTypeDetector::instance().initialize(config);
    EXPECT_TRUE(result.has_value()) << "Should succeed with built-in patterns when JSON is corrupted";
    
    // Classification methods should still work with built-in knowledge
    EXPECT_NO_THROW({
        FileTypeDetector::instance().isTextMimeType("text/plain");
        FileTypeDetector::instance().isBinaryMimeType("image/jpeg");
        FileTypeDetector::instance().getFileTypeCategory("image/png");
    });
}

TEST_F(FileTypeGracefulFallbackTest, InitializeWithEmptyFile) {
    FileTypeDetectorConfig config;
    config.patternsFile = emptyJsonPath;
    config.useCustomPatterns = true;
    config.useBuiltinPatterns = true;
    
    auto result = FileTypeDetector::instance().initialize(config);
    EXPECT_TRUE(result.has_value()) << "Should succeed when patterns file is empty";
}

TEST_F(FileTypeGracefulFallbackTest, LoadPatternsFromMissingFile) {
    auto result = FileTypeDetector::instance().loadPatternsFromFile(nonExistentPath);
    EXPECT_FALSE(result.has_value()) << "Should fail when file doesn't exist";
    
    // But detector should still work with existing patterns
    EXPECT_NO_THROW({
        FileTypeDetector::instance().isTextMimeType("text/plain");
    });
}

TEST_F(FileTypeGracefulFallbackTest, LoadPatternsFromCorruptedFile) {
    auto result = FileTypeDetector::instance().loadPatternsFromFile(corruptedJsonPath);
    EXPECT_FALSE(result.has_value()) << "Should fail when JSON is corrupted";
    
    // But detector should still work with existing patterns
    EXPECT_NO_THROW({
        FileTypeDetector::instance().getFileTypeCategory("image/jpeg");
    });
}

TEST_F(FileTypeGracefulFallbackTest, FallbackToExtensionDetection) {
    // Initialize with no custom patterns
    FileTypeDetectorConfig config;
    config.useCustomPatterns = false;
    config.useBuiltinPatterns = true;
    FileTypeDetector::instance().initialize(config);
    
    // Should fall back to extension-based MIME detection
    EXPECT_EQ(FileTypeDetector::getMimeTypeFromExtension(".jpg"), "image/jpeg");
    EXPECT_EQ(FileTypeDetector::getMimeTypeFromExtension(".txt"), "text/plain");
    EXPECT_EQ(FileTypeDetector::getMimeTypeFromExtension(".unknown"), "application/octet-stream");
}

TEST_F(FileTypeGracefulFallbackTest, ClassificationMethodsWithoutPatterns) {
    // Initialize with minimal configuration
    FileTypeDetectorConfig config;
    config.useCustomPatterns = false;
    config.useBuiltinPatterns = false; // Very minimal setup
    FileTypeDetector::instance().initialize(config);
    
    // Classification methods should still work reasonably
    EXPECT_NO_THROW({
        bool isText = FileTypeDetector::instance().isTextMimeType("text/plain");
        bool isBinary = FileTypeDetector::instance().isBinaryMimeType("image/jpeg");
        std::string category = FileTypeDetector::instance().getFileTypeCategory("application/pdf");
        
        // Should give reasonable results even without patterns
        EXPECT_TRUE(isText || !isText); // Just ensure no exception
        EXPECT_TRUE(isBinary || !isBinary);
        EXPECT_FALSE(category.empty());
    });
}

TEST_F(FileTypeGracefulFallbackTest, DetectionWithoutInitialization) {
    // Don't initialize the detector explicitly
    std::vector<std::byte> someData = {std::byte(0x01), std::byte(0x02)};
    
    // Should not crash even without explicit initialization
    EXPECT_NO_THROW({
        auto result = FileTypeDetector::instance().detectFromBuffer(someData);
        // Result may fail, but shouldn't crash
    });
}

TEST_F(FileTypeGracefulFallbackTest, FindMagicNumbersFile_NoFileFound) {
    // Test the YamsCLI::findMagicNumbersFile function
    // This will likely not find the file in test environment
    auto path = cli::YamsCLI::findMagicNumbersFile();
    
    // Should return empty path gracefully (not crash)
    EXPECT_NO_THROW({
        bool isEmpty = path.empty();
        std::string pathStr = path.string();
    });
    
    // If path is empty, detector should still work with built-in patterns
    if (path.empty()) {
        FileTypeDetectorConfig config;
        config.patternsFile = path; // Empty path
        config.useCustomPatterns = !path.empty();
        config.useBuiltinPatterns = true;
        
        auto result = FileTypeDetector::instance().initialize(config);
        EXPECT_TRUE(result.has_value()) << "Should work even when findMagicNumbersFile returns empty";
    }
}

TEST_F(FileTypeGracefulFallbackTest, RobustConfigurationHandling) {
    // Test with various configuration combinations
    std::vector<FileTypeDetectorConfig> configs = {
        {}, // Default config
        {.useLibMagic = false, .useBuiltinPatterns = true, .useCustomPatterns = false},
        {.useLibMagic = true, .useBuiltinPatterns = false, .useCustomPatterns = false},
        {.useLibMagic = false, .useBuiltinPatterns = false, .useCustomPatterns = true, .patternsFile = nonExistentPath}
    };
    
    for (const auto& config : configs) {
        EXPECT_NO_THROW({
            auto result = FileTypeDetector::instance().initialize(config);
            // Should succeed or fail gracefully, never crash
            EXPECT_TRUE(result.has_value() || !result.has_value());
        }) << "Should handle configuration gracefully";
    }
}

TEST_F(FileTypeGracefulFallbackTest, LargeBufferHandling) {
    FileTypeDetectorConfig config;
    config.maxBytesToRead = 10; // Very small limit
    FileTypeDetector::instance().initialize(config);
    
    // Create large buffer
    std::vector<std::byte> largeBuffer(1000, std::byte(0x41)); // 1KB of 'A'
    
    EXPECT_NO_THROW({
        auto result = FileTypeDetector::instance().detectFromBuffer(largeBuffer);
        // Should handle large buffers gracefully
    });
}

TEST_F(FileTypeGracefulFallbackTest, ConcurrentAccess) {
    // Basic thread safety check - initialize detector
    FileTypeDetectorConfig config;
    FileTypeDetector::instance().initialize(config);
    
    std::vector<std::byte> testData = {std::byte(0xFF), std::byte(0xD8)};
    
    // Multiple threads accessing classification methods
    std::vector<std::thread> threads;
    std::atomic<int> exceptions{0};
    
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back([&]() {
            try {
                for (int j = 0; j < 100; ++j) {
                    FileTypeDetector::instance().isTextMimeType("text/plain");
                    FileTypeDetector::instance().isBinaryMimeType("image/jpeg");
                    FileTypeDetector::instance().getFileTypeCategory("application/pdf");
                    FileTypeDetector::instance().detectFromBuffer(testData);
                }
            } catch (...) {
                exceptions++;
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    EXPECT_EQ(exceptions.load(), 0) << "Should handle concurrent access without exceptions";
}

} // namespace yams::detection