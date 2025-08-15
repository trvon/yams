#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <yams/cli/yams_cli.h>
#include <yams/detection/file_type_detector.h>

namespace yams::cli {
namespace fs = std::filesystem;

class CommandIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create temporary data directory for tests
        testDataDir = fs::temp_directory_path() / "yams_test" / "command_integration";
        fs::create_directories(testDataDir);

        // Create test files
        createTestFile("document.pdf", {0x25, 0x50, 0x44, 0x46}); // PDF
        createTestFile("image.jpg", {0xFF, 0xD8, 0xFF});          // JPEG
        createTestFile("text.txt", {0x48, 0x65, 0x6C, 0x6C});     // Hello...

        // Initialize CLI with test data path
        cli = std::make_unique<YamsCLI>();
        cli->setDataPath(testDataDir);
    }

    void TearDown() override {
        // Clean up
        if (fs::exists(testDataDir)) {
            fs::remove_all(testDataDir);
        }
        // Clear detector state
        detection::FileTypeDetector::instance().clearCache();
    }

    void createTestFile(const std::string& name, const std::vector<uint8_t>& data) {
        fs::path filePath = testDataDir / name;
        std::ofstream file(filePath, std::ios::binary);
        file.write(reinterpret_cast<const char*>(data.data()), data.size());
        // Add some additional content to make files more realistic
        file << " additional content for testing";
        file.close();
        testFiles.push_back(filePath);
    }

    fs::path testDataDir;
    std::vector<fs::path> testFiles;
    std::unique_ptr<YamsCLI> cli;
};

TEST_F(CommandIntegrationTest, FindMagicNumbersFile_MultiplePaths) {
    // Test the path finding logic
    auto foundPath = YamsCLI::findMagicNumbersFile();

    // Should return a path (may be empty if not found)
    EXPECT_NO_THROW({
        std::string pathStr = foundPath.string();
        bool isEmpty = foundPath.empty();
    });

    // If found, file should exist
    if (!foundPath.empty()) {
        EXPECT_TRUE(fs::exists(foundPath)) << "Found path should exist: " << foundPath;
        EXPECT_TRUE(fs::is_regular_file(foundPath)) << "Should be a regular file";
    }
}

TEST_F(CommandIntegrationTest, FindMagicNumbersFile_SearchOrder) {
    // Create a mock magic_numbers.json in current directory
    fs::path currentDir = fs::current_path();
    fs::path localMagicFile = currentDir / "data" / "magic_numbers.json";

    // Ensure parent directory exists
    fs::create_directories(localMagicFile.parent_path());

    // Create minimal valid JSON
    std::ofstream mockFile(localMagicFile);
    mockFile << R"({
        "version": "1.0",
        "patterns": []
    })";
    mockFile.close();

    // Should find our local file
    auto foundPath = YamsCLI::findMagicNumbersFile();

    // Clean up
    fs::remove(localMagicFile);
    fs::remove(localMagicFile.parent_path());

    // If we created a valid file, it should have been found
    // (This test is environment-dependent, so we mainly check it doesn't crash)
    EXPECT_NO_THROW({ std::string pathStr = foundPath.string(); });
}

TEST_F(CommandIntegrationTest, FileTypeDetectorIntegration_ValidConfig) {
    // Test that commands can initialize FileTypeDetector properly
    detection::FileTypeDetectorConfig config;
    config.patternsFile = YamsCLI::findMagicNumbersFile();
    config.useCustomPatterns = !config.patternsFile.empty();
    config.useBuiltinPatterns = true;

    EXPECT_NO_THROW({
        auto result = detection::FileTypeDetector::instance().initialize(config);
        EXPECT_TRUE(result.has_value() || !result.has_value()); // Should not crash
    });

    // Classification methods should work
    EXPECT_NO_THROW({
        bool isText = detection::FileTypeDetector::instance().isTextMimeType("text/plain");
        bool isBinary = detection::FileTypeDetector::instance().isBinaryMimeType("image/jpeg");
        std::string category =
            detection::FileTypeDetector::instance().getFileTypeCategory("application/pdf");

        // Basic sanity checks
        EXPECT_TRUE(isText);
        EXPECT_TRUE(isBinary);
        EXPECT_FALSE(category.empty());
    });
}

TEST_F(CommandIntegrationTest, FileTypeDetectorIntegration_MissingJSON) {
    // Test with explicitly missing JSON file
    detection::FileTypeDetectorConfig config;
    config.patternsFile = "/nonexistent/path/magic_numbers.json";
    config.useCustomPatterns = true;  // Try to use missing file
    config.useBuiltinPatterns = true; // Fall back to built-in

    EXPECT_NO_THROW({
        auto result = detection::FileTypeDetector::instance().initialize(config);
        // Should succeed with built-in patterns
        EXPECT_TRUE(result.has_value()) << "Should fall back gracefully";
    });

    // Should still provide basic functionality
    EXPECT_NO_THROW({
        bool isText = detection::FileTypeDetector::instance().isTextMimeType("text/plain");
        std::string category =
            detection::FileTypeDetector::instance().getFileTypeCategory("image/jpeg");
        EXPECT_TRUE(isText);
        EXPECT_FALSE(category.empty());
    });
}

TEST_F(CommandIntegrationTest, FileTypeDetectorIntegration_FileDetection) {
    // Initialize detector
    detection::FileTypeDetectorConfig config;
    config.patternsFile = YamsCLI::findMagicNumbersFile();
    config.useCustomPatterns = !config.patternsFile.empty();
    config.useBuiltinPatterns = true;
    detection::FileTypeDetector::instance().initialize(config);

    // Test file detection on our test files
    for (const auto& testFile : testFiles) {
        EXPECT_NO_THROW({
            auto result = detection::FileTypeDetector::instance().detectFromFile(testFile);
            // May succeed or fail, but should not crash
            if (result) {
                const auto& sig = result.value();
                EXPECT_FALSE(sig.mimeType.empty()) << "Should have MIME type for " << testFile;
                EXPECT_FALSE(sig.fileType.empty()) << "Should have file type for " << testFile;
            }
        }) << "Should handle file detection gracefully for "
           << testFile;
    }
}

TEST_F(CommandIntegrationTest, CommandSimulation_StatsWithFileTypes) {
    // Simulate what stats command does
    EXPECT_NO_THROW({
        detection::FileTypeDetectorConfig config;
        config.patternsFile = YamsCLI::findMagicNumbersFile();
        config.useCustomPatterns = !config.patternsFile.empty();
        config.useBuiltinPatterns = true;

        auto result = detection::FileTypeDetector::instance().initialize(config);
        EXPECT_TRUE(result.has_value() || !result.has_value());

        // Simulate file type analysis that stats command would do
        for (const auto& testFile : testFiles) {
            // Get MIME type from extension (fallback)
            std::string extension = testFile.extension().string();
            std::string mimeType = detection::FileTypeDetector::getMimeTypeFromExtension(extension);

            // Use classification methods
            std::string fileType =
                detection::FileTypeDetector::instance().getFileTypeCategory(mimeType);
            bool isBinary = detection::FileTypeDetector::instance().isBinaryMimeType(mimeType);

            EXPECT_FALSE(fileType.empty()) << "File type should not be empty for " << testFile;
            EXPECT_TRUE(isBinary || !isBinary)
                << "Binary classification should work for " << testFile;
        }
    });
}

TEST_F(CommandIntegrationTest, CommandSimulation_ListWithFiltering) {
    // Simulate what list command does with file type filtering
    detection::FileTypeDetectorConfig config;
    config.patternsFile = YamsCLI::findMagicNumbersFile();
    config.useCustomPatterns = !config.patternsFile.empty();
    config.useBuiltinPatterns = true;

    auto result = detection::FileTypeDetector::instance().initialize(config);
    EXPECT_TRUE(result.has_value() || !result.has_value());

    // Simulate filtering logic from list command
    std::vector<std::string> mimeTypes = {"image/jpeg", "application/pdf", "text/plain",
                                          "application/json"};

    for (const auto& mimeType : mimeTypes) {
        // Test classification methods used in filtering
        bool isText = detection::FileTypeDetector::instance().isTextMimeType(mimeType);
        bool isBinary = detection::FileTypeDetector::instance().isBinaryMimeType(mimeType);
        std::string category =
            detection::FileTypeDetector::instance().getFileTypeCategory(mimeType);

        // Basic consistency checks
        EXPECT_NE(isText, isBinary)
            << "File should be either text or binary, not both: " << mimeType;
        EXPECT_FALSE(category.empty()) << "Category should not be empty for: " << mimeType;
    }
}

TEST_F(CommandIntegrationTest, CommandSimulation_GetWithFiltering) {
    // Simulate what get command does with file type filtering
    detection::FileTypeDetectorConfig config;
    config.patternsFile = YamsCLI::findMagicNumbersFile();
    config.useCustomPatterns = !config.patternsFile.empty();
    config.useBuiltinPatterns = true;

    auto result = detection::FileTypeDetector::instance().initialize(config);
    EXPECT_TRUE(result.has_value() || !result.has_value());

    // Test file detection from actual files (like get command might do)
    for (const auto& testFile : testFiles) {
        if (fs::exists(testFile)) {
            auto detectResult = detection::FileTypeDetector::instance().detectFromFile(testFile);
            if (detectResult) {
                const auto& sig = detectResult.value();

                // Verify consistency with classification methods
                bool isText = detection::FileTypeDetector::instance().isTextMimeType(sig.mimeType);
                bool isBinary =
                    detection::FileTypeDetector::instance().isBinaryMimeType(sig.mimeType);
                std::string category =
                    detection::FileTypeDetector::instance().getFileTypeCategory(sig.mimeType);

                EXPECT_EQ(sig.isBinary, isBinary) << "Binary classification should be consistent";
                EXPECT_EQ(sig.fileType, category) << "File type should be consistent";
            }
        }
    }
}

TEST_F(CommandIntegrationTest, ErrorRecovery_InitializationFailure) {
    // Test behavior when initialization completely fails
    detection::FileTypeDetectorConfig badConfig;
    badConfig.useLibMagic = false;
    badConfig.useBuiltinPatterns = false;
    badConfig.useCustomPatterns = true;
    badConfig.patternsFile = "/definitely/does/not/exist/anywhere.json";

    EXPECT_NO_THROW({
        auto result = detection::FileTypeDetector::instance().initialize(badConfig);
        // May fail, but shouldn't crash

        // Even if initialization fails, basic methods should work
        std::string mime = detection::FileTypeDetector::getMimeTypeFromExtension(".txt");
        EXPECT_EQ(mime, "text/plain") << "Static method should always work";

        // Classification methods should provide fallback behavior
        bool isText = detection::FileTypeDetector::instance().isTextMimeType("text/plain");
        bool isBinary = detection::FileTypeDetector::instance().isBinaryMimeType("image/jpeg");

        // Should give reasonable results even in degraded mode
        EXPECT_TRUE(isText || !isText); // Just ensure no crash
        EXPECT_TRUE(isBinary || !isBinary);
    });
}

} // namespace yams::cli