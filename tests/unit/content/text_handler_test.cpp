#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <yams/content/text_content_handler.h>
#include <yams/detection/file_type_detector.h>

namespace yams::content::test {

namespace fs = std::filesystem;

class TextHandlerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test directory
        testDir_ = fs::temp_directory_path() /
                   ("text_handler_test_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(testDir_);

        // Initialize file type detector
        auto& detector = detection::FileTypeDetector::instance();
        auto result = detector.initializeWithMagicNumbers();
        ASSERT_TRUE(result) << "Failed to initialize FileTypeDetector";

        // Create handler
        handler_ = std::make_unique<TextContentHandler>();
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(testDir_, ec);
    }

    fs::path createTestFile(const std::string& name, const std::string& content) {
        fs::path filePath = testDir_ / name;
        std::ofstream file(filePath);
        file << content;
        file.close();
        return filePath;
    }

    fs::path testDir_;
    std::unique_ptr<TextContentHandler> handler_;
};

// Test handler name and priority
TEST_F(TextHandlerTest, HandlerMetadata) {
    EXPECT_EQ(handler_->name(), "TextContentHandler");
    EXPECT_GT(handler_->priority(), 0);

    auto mimeTypes = handler_->supportedMimeTypes();
    EXPECT_FALSE(mimeTypes.empty());
    EXPECT_NE(std::find(mimeTypes.begin(), mimeTypes.end(), "text/plain"), mimeTypes.end());
}

// Test basic text file processing
TEST_F(TextHandlerTest, BasicTextFile) {
    std::string content = "Hello, World!\nThis is a test file.\n";
    auto filePath = createTestFile("test.txt", content);

    auto result = handler_->process(filePath);
    ASSERT_TRUE(result) << "Failed to process text file: " << result.error().message;

    auto& contentResult = result.value();
    EXPECT_TRUE(contentResult.text.has_value());
    EXPECT_EQ(contentResult.text.value(), content);
    EXPECT_EQ(contentResult.contentType, "text/plain");
    EXPECT_TRUE(contentResult.shouldIndex);
}

// Test UTF-8 encoding
TEST_F(TextHandlerTest, Utf8Encoding) {
    std::string content = u8"Hello, 世界! 🌍\nÜnicode test: ñ, é, ü";
    auto filePath = createTestFile("utf8.txt", content);

    auto result = handler_->process(filePath);
    ASSERT_TRUE(result);

    auto& contentResult = result.value();
    EXPECT_TRUE(contentResult.text.has_value());
    EXPECT_EQ(contentResult.text.value(), content);

    // Check metadata
    auto it = contentResult.metadata.find("encoding");
    if (it != contentResult.metadata.end()) {
        EXPECT_EQ(it->second, "UTF-8");
    }
}

// Test large text file
TEST_F(TextHandlerTest, LargeTextFile) {
    std::string content;
    const size_t lineCount = 10000;
    for (size_t i = 0; i < lineCount; ++i) {
        content += "Line " + std::to_string(i) + ": This is a test line with some content.\n";
    }

    auto filePath = createTestFile("large.txt", content);

    auto start = std::chrono::steady_clock::now();
    auto result = handler_->process(filePath);
    auto elapsed = std::chrono::steady_clock::now() - start;

    ASSERT_TRUE(result);

    auto& contentResult = result.value();
    EXPECT_TRUE(contentResult.text.has_value());
    EXPECT_EQ(contentResult.text.value(), content);

    // Check processing time is reasonable
    auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
    EXPECT_LT(elapsedMs, 1000) << "Large file processing took too long";

    // Processing time should be recorded
    EXPECT_GT(contentResult.processingTime, 0);
}

// Test empty file
TEST_F(TextHandlerTest, EmptyFile) {
    auto filePath = createTestFile("empty.txt", "");

    auto result = handler_->process(filePath);
    ASSERT_TRUE(result);

    auto& contentResult = result.value();
    EXPECT_TRUE(contentResult.text.has_value());
    EXPECT_EQ(contentResult.text.value(), "");
    EXPECT_TRUE(contentResult.shouldIndex); // Empty files can still be indexed
}

// Test file with special characters
TEST_F(TextHandlerTest, SpecialCharacters) {
    std::string content = "Special chars: \t\n\r!@#$%^&*()[]{}|\\:;\"'<>,.?/~`";
    auto filePath = createTestFile("special.txt", content);

    auto result = handler_->process(filePath);
    ASSERT_TRUE(result);

    auto& contentResult = result.value();
    EXPECT_TRUE(contentResult.text.has_value());
    EXPECT_EQ(contentResult.text.value(), content);
}

// Test line ending variations
TEST_F(TextHandlerTest, LineEndings) {
    // Unix line endings (LF)
    std::string unixContent = "Line 1\nLine 2\nLine 3";
    auto unixFile = createTestFile("unix.txt", unixContent);

    auto result = handler_->process(unixFile);
    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().text.value(), unixContent);

    // Windows line endings (CRLF)
    std::string windowsContent = "Line 1\r\nLine 2\r\nLine 3";
    auto windowsFile = createTestFile("windows.txt", windowsContent);

    result = handler_->process(windowsFile);
    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().text.value(), windowsContent);

    // Mac classic line endings (CR)
    std::string macContent = "Line 1\rLine 2\rLine 3";
    auto macFile = createTestFile("mac.txt", macContent);

    result = handler_->process(macFile);
    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().text.value(), macContent);
}

// Test metadata extraction
TEST_F(TextHandlerTest, MetadataExtraction) {
    std::string content = "Test content\nWith multiple lines\n";
    auto filePath = createTestFile("metadata.txt", content);

    auto result = handler_->process(filePath);
    ASSERT_TRUE(result);

    auto& contentResult = result.value();
    auto& metadata = contentResult.metadata;

    // Should have basic metadata
    EXPECT_FALSE(metadata.empty());

    // Check for common metadata fields
    if (metadata.find("lineCount") != metadata.end()) {
        EXPECT_EQ(metadata["lineCount"], "2");
    }

    if (metadata.find("wordCount") != metadata.end()) {
        int wordCount = std::stoi(metadata["wordCount"]);
        EXPECT_GT(wordCount, 0);
    }

    if (metadata.find("charCount") != metadata.end()) {
        int charCount = std::stoi(metadata["charCount"]);
        EXPECT_EQ(charCount, content.length());
    }
}

// Test canHandle with different file signatures
TEST_F(TextHandlerTest, CanHandle) {
    auto& detector = detection::FileTypeDetector::instance();

    // Create various text files
    auto txtFile = createTestFile("test.txt", "plain text");
    auto mdFile = createTestFile("test.md", "# Markdown");
    auto jsonFile = createTestFile("test.json", "{\"key\": \"value\"}");

    // Get signatures
    auto txtSig = detector.detectFileType(txtFile);
    auto mdSig = detector.detectFileType(mdFile);
    auto jsonSig = detector.detectFileType(jsonFile);

    ASSERT_TRUE(txtSig);
    ASSERT_TRUE(mdSig);
    ASSERT_TRUE(jsonSig);

    // Text handler should handle text files
    EXPECT_TRUE(handler_->canHandle(txtSig.value()));
    EXPECT_TRUE(handler_->canHandle(mdSig.value()));
    EXPECT_TRUE(handler_->canHandle(jsonSig.value()));

    // Should not handle binary files (if we had a binary signature)
    detection::FileSignature binarySig;
    binarySig.mimeType = "application/octet-stream";
    binarySig.category = "binary";
    EXPECT_FALSE(handler_->canHandle(binarySig));
}

// Test processing non-existent file
TEST_F(TextHandlerTest, NonExistentFile) {
    fs::path nonExistent = testDir_ / "does_not_exist.txt";

    auto result = handler_->process(nonExistent);
    EXPECT_FALSE(result);
    if (!result) {
        EXPECT_EQ(result.error().code, ErrorCode::NotFound);
    }
}

// Test processing directory instead of file
TEST_F(TextHandlerTest, ProcessDirectory) {
    auto result = handler_->process(testDir_);
    EXPECT_FALSE(result);
    if (!result) {
        EXPECT_NE(result.error().code, ErrorCode::None);
    }
}

// Test very long lines
TEST_F(TextHandlerTest, VeryLongLines) {
    std::string longLine(100000, 'A'); // 100KB single line
    auto filePath = createTestFile("longline.txt", longLine);

    auto result = handler_->process(filePath);
    ASSERT_TRUE(result);

    auto& contentResult = result.value();
    EXPECT_TRUE(contentResult.text.has_value());
    EXPECT_EQ(contentResult.text.value(), longLine);
}

// Test file with null bytes (binary content)
TEST_F(TextHandlerTest, FileWithNullBytes) {
    std::string content = "Text before null";
    content.push_back('\0');
    content += "Text after null";

    auto filePath = testDir_ / "nullbytes.txt";
    std::ofstream file(filePath, std::ios::binary);
    file.write(content.data(), content.size());
    file.close();

    auto result = handler_->process(filePath);
    // Handler might fail or treat as binary
    if (result) {
        auto& contentResult = result.value();
        // Content might be truncated at null or preserved
        EXPECT_TRUE(contentResult.text.has_value());
    }
}

// Test concurrent processing
TEST_F(TextHandlerTest, ConcurrentProcessing) {
    const int numFiles = 10;
    std::vector<fs::path> files;

    // Create test files
    for (int i = 0; i < numFiles; ++i) {
        std::string content = "File " + std::to_string(i) + " content\n";
        files.push_back(createTestFile("concurrent_" + std::to_string(i) + ".txt", content));
    }

    // Process files concurrently
    std::vector<std::thread> threads;
    std::atomic<int> successCount{0};

    for (const auto& file : files) {
        threads.emplace_back([this, file, &successCount]() {
            auto result = handler_->process(file);
            if (result) {
                successCount++;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(successCount, numFiles) << "All files should be processed successfully";
}

} // namespace yams::content::test