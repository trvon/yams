#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <yams/content/content_handler_registry.h>
#include <yams/content/pdf_content_handler.h>
#include <yams/core/types.h>
#include <yams/detection/file_type_detector.h>

using namespace yams;
using namespace yams::content;
using namespace yams::detection;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Not;

class PdfContentHandlerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create temp directory for test files
        testDir_ = std::filesystem::temp_directory_path() /
                   ("pdf_handler_test_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(testDir_);

        // Initialize file type detector
        auto& detector = FileTypeDetector::instance();
        auto result = detector.initializeWithMagicNumbers();
        ASSERT_TRUE(result) << "Failed to initialize FileTypeDetector: " << result.error().message;

        // Create handler
        handler_ = std::make_unique<PdfContentHandler>();
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(testDir_, ec);
    }

    // Helper to create a minimal PDF file with text
    std::filesystem::path createTestPDF(const std::string& name,
                                        const std::string& textContent = "Sample PDF text") {
        auto pdfPath = testDir_ / name;

        // Create a simple PDF structure with text
        std::string pdfContent = "%PDF-1.4\n"
                                 "1 0 obj\n"
                                 "<<\n"
                                 "/Type /Catalog\n"
                                 "/Pages 2 0 R\n"
                                 ">>\n"
                                 "endobj\n"
                                 "2 0 obj\n"
                                 "<<\n"
                                 "/Type /Pages\n"
                                 "/Kids [3 0 R]\n"
                                 "/Count 1\n"
                                 ">>\n"
                                 "endobj\n"
                                 "3 0 obj\n"
                                 "<<\n"
                                 "/Type /Page\n"
                                 "/Parent 2 0 R\n"
                                 "/MediaBox [0 0 612 792]\n"
                                 "/Contents 4 0 R\n"
                                 "/Resources <<\n"
                                 "/Font << /F1 5 0 R >>\n"
                                 ">>\n"
                                 ">>\n"
                                 "endobj\n"
                                 "4 0 obj\n"
                                 "<<\n"
                                 "/Length " +
                                 std::to_string(textContent.length() + 50) +
                                 "\n"
                                 ">>\n"
                                 "stream\n"
                                 "BT\n"
                                 "/F1 12 Tf\n"
                                 "100 700 Td\n"
                                 "(" +
                                 textContent +
                                 ") Tj\n"
                                 "ET\n"
                                 "endstream\n"
                                 "endobj\n"
                                 "5 0 obj\n"
                                 "<<\n"
                                 "/Type /Font\n"
                                 "/Subtype /Type1\n"
                                 "/BaseFont /Helvetica\n"
                                 ">>\n"
                                 "endobj\n"
                                 "xref\n"
                                 "0 6\n"
                                 "0000000000 65535 f \n"
                                 "0000000009 00000 n \n"
                                 "0000000058 00000 n \n"
                                 "0000000115 00000 n \n"
                                 "0000000268 00000 n \n"
                                 "0000000400 00000 n \n"
                                 "trailer\n"
                                 "<<\n"
                                 "/Size 6\n"
                                 "/Root 1 0 R\n"
                                 ">>\n"
                                 "startxref\n"
                                 "484\n"
                                 "%%EOF\n";

        std::ofstream file(pdfPath, std::ios::binary);
        file.write(pdfContent.c_str(), pdfContent.size());
        file.close();

        return pdfPath;
    }

    // Helper to create a corrupted PDF file
    std::filesystem::path createCorruptedPDF(const std::string& name) {
        auto pdfPath = testDir_ / name;
        std::ofstream file(pdfPath, std::ios::binary);
        file << "This is not a PDF file";
        file.close();
        return pdfPath;
    }

    std::filesystem::path testDir_;
    std::unique_ptr<PdfContentHandler> handler_;
};

// Basic Handler Functionality Tests

TEST_F(PdfContentHandlerTest, HandlerMetadata) {
    EXPECT_EQ(handler_->name(), "PdfContentHandler");
    EXPECT_GT(handler_->priority(), 0);

    auto supportedTypes = handler_->supportedMimeTypes();
    EXPECT_FALSE(supportedTypes.empty());
    EXPECT_TRUE(std::find(supportedTypes.begin(), supportedTypes.end(), "application/pdf") !=
                supportedTypes.end());
}

TEST_F(PdfContentHandlerTest, CanHandlePdfFiles) {
    auto& detector = FileTypeDetector::instance();
    auto testFile = createTestPDF("test.pdf");

    auto signatureResult = detector.detectFromFile(testFile);
    ASSERT_TRUE(signatureResult) << "Failed to detect file signature: "
                                 << signatureResult.error().message;

    EXPECT_TRUE(handler_->canHandle(signatureResult.value()));
}

TEST_F(PdfContentHandlerTest, CannotHandleNonPdfFiles) {
    auto& detector = FileTypeDetector::instance();

    // Create a text file
    auto textFile = testDir_ / "test.txt";
    std::ofstream(textFile) << "This is a text file";

    auto signatureResult = detector.detectFromFile(textFile);
    ASSERT_TRUE(signatureResult);

    EXPECT_FALSE(handler_->canHandle(signatureResult.value()));
}

// Content Processing Tests

TEST_F(PdfContentHandlerTest, ProcessValidPDF) {
    auto testFile = createTestPDF("valid.pdf", "Hello World from PDF");

    auto result = handler_->process(testFile);
    ASSERT_TRUE(result) << "Failed to process PDF: " << result.error().message;

    const auto& contentResult = result.value();
    EXPECT_EQ(contentResult.handlerName, "PdfContentHandler");
    EXPECT_EQ(contentResult.contentType, "application/pdf");
    EXPECT_TRUE(contentResult.shouldIndex);

    // Check extracted text
    ASSERT_TRUE(contentResult.text.has_value());
    EXPECT_THAT(contentResult.text.value(), HasSubstr("Hello World from PDF"));

    // Check metadata
    EXPECT_FALSE(contentResult.metadata.empty());
    EXPECT_TRUE(contentResult.metadata.find("page_count") != contentResult.metadata.end());
}

TEST_F(PdfContentHandlerTest, ExtractMetadata) {
    auto testFile = createTestPDF("metadata_test.pdf");

    auto result = handler_->process(testFile);
    ASSERT_TRUE(result);

    const auto& metadata = result.value().metadata;

    // Should extract page count
    EXPECT_TRUE(metadata.find("page_count") != metadata.end());
    EXPECT_EQ(metadata.at("page_count"), "1");

    // Should have file size
    EXPECT_TRUE(metadata.find("file_size") != metadata.end());

    // Should have processing time
    EXPECT_TRUE(metadata.find("processing_time_ms") != metadata.end());
}

// Error Handling Tests

TEST_F(PdfContentHandlerTest, HandleNonExistentFile) {
    auto nonExistentFile = testDir_ / "does_not_exist.pdf";

    auto result = handler_->process(nonExistentFile);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::FileNotFound);
    EXPECT_THAT(result.error().message, HasSubstr("not found"));
}

TEST_F(PdfContentHandlerTest, HandleCorruptedPDF) {
    auto corruptedFile = createCorruptedPDF("corrupted.pdf");

    auto result = handler_->process(corruptedFile);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::InvalidData);
    EXPECT_THAT(result.error().message, HasSubstr("PDF"));
}

TEST_F(PdfContentHandlerTest, HandleEmptyPDF) {
    auto emptyFile = testDir_ / "empty.pdf";
    std::ofstream(emptyFile).close();

    auto result = handler_->process(emptyFile);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::InvalidData);
}

// Performance Tests

TEST_F(PdfContentHandlerTest, ProcessingPerformance) {
    auto testFile =
        createTestPDF("performance_test.pdf", "Test content for performance measurement");

    auto start = std::chrono::high_resolution_clock::now();
    auto result = handler_->process(testFile);
    auto end = std::chrono::high_resolution_clock::now();

    ASSERT_TRUE(result);

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Simple PDF should process quickly. Allow generous headroom for CI variability (under 1200ms)
    EXPECT_LT(duration.count(), 1200) << "Processing took " << duration.count() << "ms";

    // Check that processing time is recorded in metadata
    const auto& metadata = result.value().metadata;
    EXPECT_TRUE(metadata.find("processing_time_ms") != metadata.end());
}

// Registry Integration Tests

TEST_F(PdfContentHandlerTest, RegistryIntegration) {
    // Clear registry and register PDF handler
    auto& registry = ContentHandlerRegistry::instance();
    registry.clear();
    registry.registerHandler(std::make_unique<PdfContentHandler>());

    // Create PDF file
    auto testFile = createTestPDF("registry_test.pdf");

    // Get file signature
    auto& detector = FileTypeDetector::instance();
    auto signatureResult = detector.detectFromFile(testFile);
    ASSERT_TRUE(signatureResult);

    // Get handler from registry
    auto handler = registry.getHandler(signatureResult.value());
    ASSERT_NE(handler, nullptr);
    EXPECT_EQ(handler->name(), "PdfContentHandler");

    // Process through registry
    auto result = handler->process(testFile);
    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().handlerName, "PdfContentHandler");
}

// Thread Safety Tests

TEST_F(PdfContentHandlerTest, ThreadSafeProcessing) {
    const int numThreads = 4;
    const int filesPerThread = 3;

    std::vector<std::thread> threads;
    std::vector<bool> results(numThreads * filesPerThread, false);

    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([this, t, filesPerThread, &results]() {
            PdfContentHandler localHandler;

            for (int f = 0; f < filesPerThread; ++f) {
                int index = t * filesPerThread + f;
                auto testFile = createTestPDF(
                    "thread_" + std::to_string(t) + "_file_" + std::to_string(f) + ".pdf",
                    "Thread " + std::to_string(t) + " File " + std::to_string(f));

                auto result = localHandler.process(testFile);
                results[index] = result.has_value();
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    for (bool success : results) {
        EXPECT_TRUE(success);
    }
}

// Edge Cases

TEST_F(PdfContentHandlerTest, HandlePDFWithNoText) {
    // Create a minimal PDF with no text content
    auto pdfPath = testDir_ / "no_text.pdf";
    std::string minimalPdf = "%PDF-1.4\n"
                             "1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
                             "2 0 obj<</Type/Pages/Count 1/Kids[3 0 R]>>endobj\n"
                             "3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 612 792]>>endobj\n"
                             "xref\n0 4\n"
                             "0000000000 65535 f \n"
                             "0000000009 00000 n \n"
                             "0000000056 00000 n \n"
                             "0000000108 00000 n \n"
                             "trailer<</Size 4/Root 1 0 R>>\n"
                             "startxref\n168\n%%EOF";

    std::ofstream(pdfPath) << minimalPdf;

    auto result = handler_->process(pdfPath);

    // Should succeed but with empty or minimal text
    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().handlerName, "PdfContentHandler");

    // Text might be empty or just whitespace
    if (result.value().text.has_value()) {
        std::string text = result.value().text.value();
        // Remove whitespace for testing
        text.erase(std::remove_if(text.begin(), text.end(), ::isspace), text.end());
        EXPECT_TRUE(text.empty() || text.length() < 10);
    }
}

// Configuration Tests

TEST_F(PdfContentHandlerTest, RespectsConfiguration) {
    // Test that handler respects any configuration options
    // (This would be expanded based on actual configuration options)

    auto testFile = createTestPDF("config_test.pdf", "Configuration test content");

    // Process with default configuration
    auto result = handler_->process(testFile);
    ASSERT_TRUE(result);

    // Verify basic processing occurred
    EXPECT_EQ(result.value().handlerName, "PdfContentHandler");
    EXPECT_TRUE(result.value().shouldIndex);
}
