#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <yams/extraction/pdf_extractor.h>
#include <yams/core/result.h>
#include "../../common/test_data_generator.h"
#include "../../common/fixture_manager.h"
#include <filesystem>
#include <fstream>

using namespace yams;
using namespace yams::extraction;
using namespace yams::test;
using ::testing::HasSubstr;
using ::testing::Not;
using ::testing::IsEmpty;

class PDFExtractorTest : public ::testing::Test {
protected:
    void SetUp() override {
        extractor_ = std::make_unique<PDFExtractor>();
        fixtureManager_ = std::make_unique<FixtureManager>();
        testDir_ = fixtureManager_->createTempDirectory("pdf_test");
    }
    
    void TearDown() override {
        fixtureManager_->cleanupTempDirectories();
    }
    
    // Helper to create a test PDF file
    FixtureManager::Fixture createTestPDF(const std::string& name, size_t pages) {
        TestDataGenerator generator;
        auto pdfData = generator.generatePDF(pages);
        return fixtureManager_->createBinaryFixture(name, pdfData);
    }
    
    std::unique_ptr<PDFExtractor> extractor_;
    std::unique_ptr<FixtureManager> fixtureManager_;
    std::filesystem::path testDir_;
};

// Basic Functionality Tests

TEST_F(PDFExtractorTest, ExtractTextFromSimplePDF) {
    // Arrange
    auto fixture = FixtureManager::getSimplePDF();
    
    // Act
    auto result = extractor_->extract(fixture.path);
    
    // Assert
    ASSERT_TRUE(result.has_value()) << "Failed to extract from simple PDF";
    EXPECT_FALSE(result->text.empty()) << "Extracted text should not be empty";
    EXPECT_TRUE(result->isSuccess()) << "Extraction should be successful";
    EXPECT_EQ(result->extractionMethod, "pdfium");
}

TEST_F(PDFExtractorTest, ExtractMetadataComplete) {
    // Arrange
    auto fixture = createTestPDF("metadata_test.pdf", 1);
    
    // Act
    auto result = extractor_->extract(fixture.path);
    
    // Assert
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->isSuccess());
    
    // Check for expected metadata fields
    const auto& metadata = result->metadata;
    
    // PDFium should extract these standard fields if present
    std::vector<std::string> expectedFields = {
        "title", "author", "subject", "keywords", 
        "creator", "producer", "page_count"
    };
    
    for (const auto& field : expectedFields) {
        EXPECT_TRUE(metadata.find(field) != metadata.end()) 
            << "Missing metadata field: " << field;
    }
}

TEST_F(PDFExtractorTest, HandleMultiPageDocument) {
    // Arrange
    auto fixture = createTestPDF("multipage.pdf", 10);
    
    // Act
    auto result = extractor_->extract(fixture.path);
    
    // Assert
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->isSuccess());
    
    // Check page count in metadata
    ASSERT_TRUE(result->metadata.find("page_count") != result->metadata.end());
    EXPECT_EQ(result->metadata.at("page_count"), "10");
    
    // Text should contain content from multiple pages
    EXPECT_THAT(result->text, HasSubstr("Page 1"));
    EXPECT_THAT(result->text, HasSubstr("Page 5"));
    EXPECT_THAT(result->text, HasSubstr("Page 10"));
}

// UTF-8 and Encoding Tests

TEST_F(PDFExtractorTest, HandleUTF8Content) {
    // Create a PDF with Unicode content
    // Note: This would need actual PDF creation with Unicode text
    // For now, we test that UTF-8 is properly handled
    
    auto fixture = createTestPDF("unicode.pdf", 1);
    auto result = extractor_->extract(fixture.path);
    
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->isSuccess());
    
    // Check that encoding is properly detected
    ASSERT_TRUE(result->metadata.find("encoding") != result->metadata.end());
    EXPECT_EQ(result->metadata.at("encoding"), "UTF-8");
}

TEST_F(PDFExtractorTest, HandleSpecialCharacters) {
    auto fixture = createTestPDF("special_chars.pdf", 1);
    auto result = extractor_->extract(fixture.path);
    
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->isSuccess());
    
    // Ensure no corruption of text during extraction
    EXPECT_THAT(result->text, Not(HasSubstr("\ufffd"))); // No replacement characters
}

// Error Handling Tests

TEST_F(PDFExtractorTest, HandleCorruptedPDF) {
    // Arrange
    auto fixture = FixtureManager::getCorruptedPDF();
    
    // Act
    auto result = extractor_->extract(fixture.path);
    
    // Assert
    ASSERT_FALSE(result.has_value()) << "Should fail on corrupted PDF";
    EXPECT_EQ(result.error().code, ErrorCode::InvalidFormat);
    EXPECT_THAT(result.error().message, HasSubstr("PDF"));
}

TEST_F(PDFExtractorTest, HandleNonExistentFile) {
    // Arrange
    std::filesystem::path nonExistent = testDir_ / "does_not_exist.pdf";
    
    // Act
    auto result = extractor_->extract(nonExistent);
    
    // Assert
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code, ErrorCode::FileNotFound);
}

TEST_F(PDFExtractorTest, HandleEmptyPDF) {
    // Create an empty file with PDF extension
    std::filesystem::path emptyPdf = testDir_ / "empty.pdf";
    std::ofstream(emptyPdf).close();
    
    auto result = extractor_->extract(emptyPdf);
    
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code, ErrorCode::InvalidFormat);
}

TEST_F(PDFExtractorTest, HandlePasswordProtectedPDF) {
    // Note: Would need actual password-protected PDF
    // For now, we simulate the expected behavior
    
    // Password-protected PDFs should return appropriate error
    // This is a placeholder - real implementation would need actual encrypted PDF
    GTEST_SKIP() << "Password-protected PDF test requires actual encrypted file";
}

TEST_F(PDFExtractorTest, HandleImageOnlyPDF) {
    // PDFs with only images (no text) should return empty text
    // but still be successful
    
    // This would need a real image-only PDF
    // For now, we test the expected behavior pattern
    GTEST_SKIP() << "Image-only PDF test requires actual image PDF";
}

// Memory and Resource Management Tests

TEST_F(PDFExtractorTest, HandleLargePDF) {
    // Create a large PDF (100 pages)
    auto fixture = createTestPDF("large.pdf", 100);
    
    // Measure memory before
    // Note: Actual memory measurement would be platform-specific
    
    auto result = extractor_->extract(fixture.path);
    
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->isSuccess());
    
    // Verify all pages were processed
    ASSERT_TRUE(result->metadata.find("page_count") != result->metadata.end());
    EXPECT_EQ(result->metadata.at("page_count"), "100");
}

TEST_F(PDFExtractorTest, NoMemoryLeaks) {
    // Extract from multiple PDFs in sequence
    for (int i = 0; i < 10; ++i) {
        auto fixture = createTestPDF("test_" + std::to_string(i) + ".pdf", 5);
        auto result = extractor_->extract(fixture.path);
        ASSERT_TRUE(result.has_value());
    }
    
    // Memory leak detection would be done by valgrind/ASAN in test runs
    // This test ensures repeated extractions don't crash
}

// Buffer Extraction Tests

TEST_F(PDFExtractorTest, ExtractFromBuffer) {
    // Generate PDF data
    TestDataGenerator generator;
    auto pdfData = generator.generatePDF(3);
    
    // Convert to byte vector
    std::vector<std::byte> buffer;
    buffer.reserve(pdfData.size());
    for (auto byte : pdfData) {
        buffer.push_back(static_cast<std::byte>(byte));
    }
    
    // Extract from buffer
    auto result = extractor_->extractFromBuffer(buffer);
    
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->isSuccess());
    EXPECT_FALSE(result->text.empty());
    EXPECT_EQ(result->extractionMethod, "pdfium_buffer");
}

TEST_F(PDFExtractorTest, ExtractFromEmptyBuffer) {
    std::vector<std::byte> emptyBuffer;
    
    auto result = extractor_->extractFromBuffer(emptyBuffer);
    
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code, ErrorCode::InvalidInput);
}

// Complex Layout Tests

TEST_F(PDFExtractorTest, HandleComplexLayout) {
    // Test extraction from PDFs with complex layouts
    // (columns, tables, mixed text orientations)
    
    auto fixture = FixtureManager::getComplexPDF();
    auto result = extractor_->extract(fixture.path);
    
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->isSuccess());
    
    // Text should be extracted in reading order
    // Actual validation would depend on specific PDF content
    EXPECT_FALSE(result->text.empty());
}

// Performance Tests

TEST_F(PDFExtractorTest, ExtractionPerformance) {
    auto fixture = createTestPDF("perf_test.pdf", 10);
    
    auto start = std::chrono::high_resolution_clock::now();
    auto result = extractor_->extract(fixture.path);
    auto end = std::chrono::high_resolution_clock::now();
    
    ASSERT_TRUE(result.has_value());
    
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    // 10-page document should extract in under 100ms
    EXPECT_LT(duration.count(), 100) << "Extraction took " << duration.count() << "ms";
}

// Metadata Extraction Tests

TEST_F(PDFExtractorTest, ExtractAllMetadataFields) {
    auto fixture = createTestPDF("full_metadata.pdf", 2);
    auto result = extractor_->extract(fixture.path);
    
    ASSERT_TRUE(result.has_value());
    
    const auto& metadata = result->metadata;
    
    // Check standard PDF metadata fields
    EXPECT_TRUE(metadata.find("title") != metadata.end());
    EXPECT_TRUE(metadata.find("author") != metadata.end());
    EXPECT_TRUE(metadata.find("subject") != metadata.end());
    EXPECT_TRUE(metadata.find("keywords") != metadata.end());
    EXPECT_TRUE(metadata.find("creator") != metadata.end());
    EXPECT_TRUE(metadata.find("producer") != metadata.end());
    EXPECT_TRUE(metadata.find("creation_date") != metadata.end() ||
                metadata.find("created") != metadata.end());
    EXPECT_TRUE(metadata.find("modification_date") != metadata.end() ||
                metadata.find("modified") != metadata.end());
}

// Configuration Tests

TEST_F(PDFExtractorTest, RespectExtractionConfig) {
    auto fixture = createTestPDF("config_test.pdf", 3);
    
    ExtractionConfig config;
    config.preserveFormatting = false;
    config.maxFileSize = 1024 * 1024; // 1MB limit
    
    auto result = extractor_->extract(fixture.path, config);
    
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->isSuccess());
    
    // Verify formatting was not preserved (no multiple spaces/newlines)
    EXPECT_THAT(result->text, Not(HasSubstr("  "))); // No double spaces
}

// Edge Cases

TEST_F(PDFExtractorTest, HandlePDFWithNoText) {
    // Create minimal PDF with no text content
    std::string minimalPdf = "%PDF-1.4\n"
                             "1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
                             "2 0 obj<</Type/Pages/Count 0/Kids[]>>endobj\n"
                             "xref\n0 3\n"
                             "0000000000 65535 f\n"
                             "0000000009 00000 n\n"
                             "0000000056 00000 n\n"
                             "trailer<</Size 3/Root 1 0 R>>\n"
                             "startxref\n103\n%%EOF";
    
    auto pdfPath = testDir_ / "no_text.pdf";
    std::ofstream(pdfPath) << minimalPdf;
    
    auto result = extractor_->extract(pdfPath);
    
    // Should succeed but with empty text
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->isSuccess());
    EXPECT_TRUE(result->text.empty());
}

TEST_F(PDFExtractorTest, HandleMalformedButRecoverablePDF) {
    // Some PDFs are malformed but still readable
    // PDFium should attempt to recover what it can
    
    std::string malformedPdf = "%PDF-1.4\n"
                               "1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
                               "2 0 obj<</Type/Pages/Count 1/Kids[3 0 R]>>endobj\n"
                               "3 0 obj<</Type/Page/Parent 2 0 R>>endobj\n"
                               // Missing xref table and trailer
                               "%%EOF";
    
    auto pdfPath = testDir_ / "malformed.pdf";
    std::ofstream(pdfPath) << malformedPdf;
    
    auto result = extractor_->extract(pdfPath);
    
    // May or may not succeed depending on PDFium's recovery capabilities
    if (result.has_value()) {
        EXPECT_TRUE(result->isSuccess());
    } else {
        EXPECT_EQ(result.error().code, ErrorCode::InvalidFormat);
    }
}

// Integration with Text Extractor Factory

TEST_F(PDFExtractorTest, RegisteredWithFactory) {
    // PDF extractor should be registered for .pdf extension
    auto& factory = TextExtractorFactory::instance();
    
    EXPECT_TRUE(factory.isSupported(".pdf"));
    EXPECT_TRUE(factory.isSupported(".PDF")); // Case insensitive
    
    // Should create PDF extractor for PDF files
    auto extractor = factory.createExtractor(".pdf");
    ASSERT_NE(extractor, nullptr);
    
    // Should be able to extract
    auto fixture = FixtureManager::getSimplePDF();
    auto result = extractor->extract(fixture.path);
    ASSERT_TRUE(result.has_value());
}

// Concurrent Extraction Tests

TEST_F(PDFExtractorTest, ThreadSafeExtraction) {
    // Multiple threads extracting from different PDFs
    const int numThreads = 4;
    std::vector<std::thread> threads;
    std::vector<bool> results(numThreads, false);
    
    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([this, i, &results]() {
            auto fixture = createTestPDF("thread_" + std::to_string(i) + ".pdf", 2);
            PDFExtractor localExtractor;
            auto result = localExtractor.extract(fixture.path);
            results[i] = result.has_value() && result->isSuccess();
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    for (bool success : results) {
        EXPECT_TRUE(success);
    }
}

// Mock PDFium for isolated testing
class MockPDFium {
public:
    MOCK_METHOD(bool, loadDocument, (const std::string& path));
    MOCK_METHOD(int, getPageCount, ());
    MOCK_METHOD(std::string, extractText, (int pageNum));
    MOCK_METHOD(std::map<std::string, std::string>, getMetadata, ());
};

// Test with mocked PDFium would go here for more isolated unit testing