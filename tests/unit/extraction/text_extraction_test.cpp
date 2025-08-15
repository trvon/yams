#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <yams/extraction/plain_text_extractor.h>
#include <yams/extraction/text_extractor.h>

using namespace yams;
using namespace yams::extraction;

class TextExtractionTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create temp directory for test files
        testDir_ = std::filesystem::temp_directory_path() / "kronos_extraction_test";
        std::filesystem::create_directories(testDir_);
    }

    void TearDown() override {
        // Clean up test files
        std::filesystem::remove_all(testDir_);
    }

    void createTestFile(const std::string& filename, const std::string& content) {
        std::ofstream file(testDir_ / filename);
        file << content;
        file.close();
    }

    std::filesystem::path testDir_;
};

TEST_F(TextExtractionTest, FactoryRegistration) {
    auto& factory = TextExtractorFactory::instance();

    // Check that plain text extensions are registered
    EXPECT_TRUE(factory.isSupported(".txt"));
    EXPECT_TRUE(factory.isSupported(".md"));
    EXPECT_TRUE(factory.isSupported(".cpp"));
    EXPECT_TRUE(factory.isSupported(".json"));

    // Check case insensitivity
    EXPECT_TRUE(factory.isSupported(".TXT"));
    EXPECT_TRUE(factory.isSupported(".Cpp"));

    // Check unsupported extensions
    EXPECT_FALSE(factory.isSupported(".exe"));
    EXPECT_FALSE(factory.isSupported(".dll"));
}

TEST_F(TextExtractionTest, PlainTextExtraction) {
    const std::string content = "Hello, World!\nThis is a test file.\nWith multiple lines.";
    createTestFile("test.txt", content);

    try {
        std::cout << "Creating extractor..." << std::endl;
        PlainTextExtractor extractor;

        ExtractionConfig config;
        config.preserveFormatting = true; // Preserve newlines

        std::cout << "Extracting from: " << (testDir_ / "test.txt") << std::endl;
        auto result = extractor.extract(testDir_ / "test.txt", config);

        std::cout << "Checking result..." << std::endl;
        ASSERT_TRUE(result.has_value());
        auto& extraction = result.value();

        if (!extraction.isSuccess()) {
            std::cout << "Extraction failed with error: " << extraction.error << std::endl;
        }

        std::cout << "Checking success..." << std::endl;
        EXPECT_TRUE(extraction.isSuccess());

        std::cout << "Checking text content..." << std::endl;
        EXPECT_EQ(extraction.text, content);
        EXPECT_EQ(extraction.extractionMethod, "plain_text");

        std::cout << "Checking language..." << std::endl;
        EXPECT_FALSE(extraction.language.empty());

        // Debug: print all metadata keys
        std::cout << "Metadata keys:" << std::endl;
        for (const auto& [key, value] : extraction.metadata) {
            std::cout << "  " << key << " = " << value << std::endl;
        }

        // Check metadata
        if (extraction.metadata.find("filename") != extraction.metadata.end()) {
            EXPECT_EQ(extraction.metadata.at("filename"), "test.txt");
        }
        if (extraction.metadata.find("extension") != extraction.metadata.end()) {
            EXPECT_EQ(extraction.metadata.at("extension"), ".txt");
        }
        if (extraction.metadata.find("line_count") != extraction.metadata.end()) {
            EXPECT_EQ(extraction.metadata.at("line_count"), "3");
        }
    } catch (const std::exception& e) {
        FAIL() << "Exception caught: " << e.what();
    }
}

TEST_F(TextExtractionTest, MarkdownExtraction) {
    const std::string content = "# Title\n\n## Subtitle\n\nSome **bold** text and *italic* text.";
    createTestFile("test.md", content);

    PlainTextExtractor extractor;
    ExtractionConfig config;
    config.preserveFormatting = true; // Preserve newlines
    auto result = extractor.extract(testDir_ / "test.md", config);

    ASSERT_TRUE(result.has_value());
    auto& extraction = result.value();

    EXPECT_TRUE(extraction.isSuccess());
    EXPECT_EQ(extraction.text, content);
    ASSERT_TRUE(extraction.metadata.find("format") != extraction.metadata.end());
    EXPECT_EQ(extraction.metadata.at("format"), "markdown");
}

TEST_F(TextExtractionTest, SourceCodeExtraction) {
    const std::string content = R"(#include <iostream>

int main() {
    std::cout << "Hello, World!" << std::endl;
    return 0;
})";
    createTestFile("test.cpp", content);

    PlainTextExtractor extractor;
    ExtractionConfig config;
    config.preserveFormatting = true; // Preserve newlines
    auto result = extractor.extract(testDir_ / "test.cpp", config);

    ASSERT_TRUE(result.has_value());
    auto& extraction = result.value();

    EXPECT_TRUE(extraction.isSuccess());
    EXPECT_EQ(extraction.text, content);
    ASSERT_TRUE(extraction.metadata.find("format") != extraction.metadata.end());
    EXPECT_EQ(extraction.metadata.at("format"), "source_code");
    ASSERT_TRUE(extraction.metadata.find("programming_language") != extraction.metadata.end());
    EXPECT_EQ(extraction.metadata.at("programming_language"), "cpp");
}

TEST_F(TextExtractionTest, EmptyFile) {
    createTestFile("empty.txt", "");

    PlainTextExtractor extractor;
    auto result = extractor.extract(testDir_ / "empty.txt");

    ASSERT_TRUE(result.has_value());
    auto& extraction = result.value();

    EXPECT_TRUE(extraction.isSuccess());
    EXPECT_TRUE(extraction.text.empty());
}

TEST_F(TextExtractionTest, NonExistentFile) {
    PlainTextExtractor extractor;
    auto result = extractor.extract(testDir_ / "nonexistent.txt");

    ASSERT_TRUE(result.has_value());
    auto& extraction = result.value();

    EXPECT_FALSE(extraction.isSuccess());
    EXPECT_TRUE(extraction.text.empty());
    EXPECT_FALSE(extraction.error.empty());
}

TEST_F(TextExtractionTest, LargeFile) {
    // Create a file that exceeds default size limit
    std::string largeContent;
    for (int i = 0; i < 1024 * 1024; ++i) { // 1MB of 'a's
        largeContent += 'a';
    }
    createTestFile("large.txt", largeContent);

    PlainTextExtractor extractor;
    ExtractionConfig config;
    config.maxFileSize = 512 * 1024; // 512KB limit

    auto result = extractor.extract(testDir_ / "large.txt", config);

    ASSERT_TRUE(result.has_value());
    auto& extraction = result.value();

    EXPECT_FALSE(extraction.isSuccess());
    EXPECT_TRUE(extraction.error.find("too large") != std::string::npos);
}

TEST_F(TextExtractionTest, BufferExtraction) {
    const std::string content = "Buffer content test";
    std::vector<std::byte> buffer;
    buffer.reserve(content.size());
    for (char c : content) {
        buffer.push_back(static_cast<std::byte>(c));
    }

    PlainTextExtractor extractor;
    auto result = extractor.extractFromBuffer(buffer);

    ASSERT_TRUE(result.has_value());
    auto& extraction = result.value();

    EXPECT_TRUE(extraction.isSuccess());
    EXPECT_EQ(extraction.text, content);
    EXPECT_EQ(extraction.extractionMethod, "plain_text_buffer");
}

TEST_F(TextExtractionTest, EncodingDetection) {
    // Test UTF-8 BOM
    std::vector<std::byte> utf8Bom = {std::byte(0xEF), std::byte(0xBB), std::byte(0xBF),
                                      std::byte('H'), std::byte('i')};

    double confidence = 0.0;
    auto encoding = EncodingDetector::detectEncoding(utf8Bom, &confidence);

    EXPECT_EQ(encoding, "UTF-8");
    EXPECT_EQ(confidence, 1.0);

    // Test plain ASCII
    std::string asciiText = "Plain ASCII text";
    std::vector<std::byte> asciiBuffer;
    asciiBuffer.reserve(asciiText.size());
    for (char c : asciiText) {
        asciiBuffer.push_back(static_cast<std::byte>(c));
    }

    encoding = EncodingDetector::detectEncoding(asciiBuffer, &confidence);
    EXPECT_EQ(encoding, "UTF-8"); // ASCII is valid UTF-8
    EXPECT_GT(confidence, 0.8);
}

TEST_F(TextExtractionTest, LanguageDetection) {
    // Test English
    std::string englishText = "The quick brown fox jumps over the lazy dog. "
                              "This is a test of the language detection system.";
    double confidence = 0.0;
    auto lang = LanguageDetector::detectLanguage(englishText, &confidence);

    EXPECT_EQ(lang, "en");
    EXPECT_GT(confidence, 0.5);

    // Test Spanish
    std::string spanishText = "El rápido zorro marrón salta sobre el perro perezoso. "
                              "Esta es una prueba del sistema de detección de idiomas.";
    lang = LanguageDetector::detectLanguage(spanishText, &confidence);

    EXPECT_EQ(lang, "es");
    EXPECT_GT(confidence, 0.5);
}

TEST_F(TextExtractionTest, FormattingPreservation) {
    const std::string content = "Line 1\n\n  Indented line\n\tTabbed line\n\n\nMultiple blanks";
    createTestFile("formatted.txt", content);

    PlainTextExtractor extractor;
    ExtractionConfig config;
    config.preserveFormatting = true;

    auto result = extractor.extract(testDir_ / "formatted.txt", config);

    ASSERT_TRUE(result.has_value());
    auto& extraction = result.value();

    EXPECT_TRUE(extraction.isSuccess());
    EXPECT_EQ(extraction.text, content);
}

TEST_F(TextExtractionTest, FormattingNormalization) {
    const std::string content =
        "Line 1\n\n  Indented   line\n\tTabbed\t\tline\n\n\nMultiple   spaces";
    createTestFile("formatted.txt", content);

    PlainTextExtractor extractor;
    ExtractionConfig config;
    config.preserveFormatting = false;

    auto result = extractor.extract(testDir_ / "formatted.txt", config);

    ASSERT_TRUE(result.has_value());
    auto& extraction = result.value();

    EXPECT_TRUE(extraction.isSuccess());
    // Normalized: single spaces, no extra whitespace
    EXPECT_TRUE(extraction.text.find("  ") == std::string::npos); // No double spaces
    EXPECT_TRUE(extraction.text.find("\t") == std::string::npos); // No tabs
}

TEST_F(TextExtractionTest, JsonExtraction) {
    const std::string content = R"({
    "name": "test",
    "value": 42,
    "nested": {
        "array": [1, 2, 3]
    }
})";
    createTestFile("test.json", content);

    PlainTextExtractor extractor;
    auto result = extractor.extract(testDir_ / "test.json");

    ASSERT_TRUE(result.has_value());
    auto& extraction = result.value();

    EXPECT_TRUE(extraction.isSuccess());
    ASSERT_TRUE(extraction.metadata.find("format") != extraction.metadata.end());
    EXPECT_EQ(extraction.metadata.at("format"), "json");
}

TEST_F(TextExtractionTest, CsvExtraction) {
    const std::string content = "Name,Age,City\nJohn,30,New York\nJane,25,Los Angeles";
    createTestFile("test.csv", content);

    PlainTextExtractor extractor;
    auto result = extractor.extract(testDir_ / "test.csv");

    ASSERT_TRUE(result.has_value());
    auto& extraction = result.value();

    EXPECT_TRUE(extraction.isSuccess());
    ASSERT_TRUE(extraction.metadata.find("format") != extraction.metadata.end());
    EXPECT_EQ(extraction.metadata.at("format"), "csv");
    ASSERT_TRUE(extraction.metadata.find("line_count") != extraction.metadata.end());
    EXPECT_EQ(extraction.metadata.at("line_count"), "3");
}

TEST_F(TextExtractionTest, BinaryFileDetection) {
    // Create a file with binary content
    std::vector<uint8_t> binaryData = {0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD};
    std::ofstream file(testDir_ / "binary.dat", std::ios::binary);
    file.write(reinterpret_cast<const char*>(binaryData.data()),
               static_cast<std::streamsize>(binaryData.size()));
    file.close();

    // Read as bytes
    std::ifstream inFile(testDir_ / "binary.dat", std::ios::binary);
    std::vector<std::byte> buffer(binaryData.size());
    inFile.read(reinterpret_cast<char*>(buffer.data()),
                static_cast<std::streamsize>(buffer.size()));
    inFile.close();

    PlainTextExtractor extractor;
    auto result = extractor.extractFromBuffer(buffer);

    ASSERT_TRUE(result.has_value());
    auto& extraction = result.value();

    EXPECT_FALSE(extraction.isSuccess());
    EXPECT_TRUE(extraction.error.find("binary") != std::string::npos);
}