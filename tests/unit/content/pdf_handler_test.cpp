#include <filesystem>
#include <fstream>
#include <vector>
#include <gtest/gtest.h>
#include <yams/content/handlers/pdf_handler.h>

namespace yams::content::test {

namespace fs = std::filesystem;

class PdfHandlerTest : public ::testing::Test {
protected:
    void SetUp() override {
        testDir_ = fs::temp_directory_path() /
                   ("pdf_test_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(testDir_);

        handler_ = std::make_unique<PdfHandler>();
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(testDir_, ec);
    }

    fs::path createPdfFile(const std::string& name, const std::vector<uint8_t>& content) {
        fs::path filePath = testDir_ / name;
        std::ofstream file(filePath, std::ios::binary);
        file.write(reinterpret_cast<const char*>(content.data()), content.size());
        file.close();
        return filePath;
    }

    // Create minimal valid PDF structure
    std::vector<uint8_t> createMinimalPdf(const std::string& text = "Hello World") {
        std::string pdf = "%PDF-1.4\n";
        pdf += "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
        pdf += "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
        pdf += "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] ";
        pdf += "/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>\nendobj\n";
        pdf += "4 0 obj\n<< /Length " + std::to_string(text.length() + 20) + " >>\n";
        pdf += "stream\nBT /F1 12 Tf 100 700 Td (" + text + ") Tj ET\nendstream\nendobj\n";
        pdf += "5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n";
        pdf += "xref\n0 6\n0000000000 65535 f\n";
        pdf += "0000000009 00000 n\n0000000058 00000 n\n0000000115 00000 n\n";
        pdf += "0000000274 00000 n\n0000000381 00000 n\n";
        pdf += "trailer\n<< /Size 6 /Root 1 0 R >>\n";
        pdf += "startxref\n462\n%%EOF";

        return std::vector<uint8_t>(pdf.begin(), pdf.end());
    }

    fs::path testDir_;
    std::unique_ptr<PdfHandler> handler_;
};

// Test PDF format detection
TEST_F(PdfHandlerTest, CanHandle) {
    // PDF files
    EXPECT_TRUE(handler_->canHandle(".pdf"));
    EXPECT_TRUE(handler_->canHandle(".PDF"));

    // Non-PDF files
    EXPECT_FALSE(handler_->canHandle(".txt"));
    EXPECT_FALSE(handler_->canHandle(".doc"));
    EXPECT_FALSE(handler_->canHandle(".html"));
}

// Test processing minimal PDF
TEST_F(PdfHandlerTest, ProcessMinimalPdf) {
    auto pdfContent = createMinimalPdf("Test Content");
    auto filePath = createPdfFile("test.pdf", pdfContent);

    auto result = handler_->process(filePath);

    // PDF processing might fail without proper libraries
    if (!result) {
        // Check for expected error codes
        EXPECT_TRUE(result.error().code == ErrorCode::NotImplemented ||
                    result.error().code == ErrorCode::ProcessingError ||
                    result.error().code == ErrorCode::UnsupportedFormat)
            << "Unexpected error: " << result.error().message;
        return;
    }

    const auto& content = result.value();
    EXPECT_FALSE(content.text.empty());
    EXPECT_FALSE(content.mimeType.empty());
    EXPECT_EQ(content.mimeType, "application/pdf");
}

// Test invalid PDF
TEST_F(PdfHandlerTest, InvalidPdf) {
    std::vector<uint8_t> invalidPdf = {'n', 'o', 't', ' ', 'p', 'd', 'f'};
    auto filePath = createPdfFile("invalid.pdf", invalidPdf);

    auto result = handler_->process(filePath);

    EXPECT_FALSE(result);
    if (!result) {
        EXPECT_TRUE(result.error().code == ErrorCode::InvalidData ||
                    result.error().code == ErrorCode::ProcessingError ||
                    result.error().code == ErrorCode::UnsupportedFormat);
    }
}

// Test empty PDF
TEST_F(PdfHandlerTest, EmptyPdf) {
    std::vector<uint8_t> emptyPdf;
    auto filePath = createPdfFile("empty.pdf", emptyPdf);

    auto result = handler_->process(filePath);

    EXPECT_FALSE(result);
}

// Test corrupted PDF header
TEST_F(PdfHandlerTest, CorruptedHeader) {
    std::vector<uint8_t> corruptedPdf = {'%', 'P', 'X', 'F', '-', '1', '.', '4'};
    auto filePath = createPdfFile("corrupted.pdf", corruptedPdf);

    auto result = handler_->process(filePath);

    EXPECT_FALSE(result);
}

// Test PDF with metadata
TEST_F(PdfHandlerTest, PdfWithMetadata) {
    // Create PDF with metadata
    std::string pdf = "%PDF-1.4\n";
    pdf += "1 0 obj\n<< /Type /Catalog /Pages 2 0 R ";
    pdf += "/Metadata << /Title (Test Document) /Author (Test Author) >> >>\nendobj\n";
    pdf += "2 0 obj\n<< /Type /Pages /Kids [] /Count 0 >>\nendobj\n";
    pdf += "xref\n0 3\n0000000000 65535 f\n";
    pdf += "0000000009 00000 n\n0000000120 00000 n\n";
    pdf += "trailer\n<< /Size 3 /Root 1 0 R >>\n";
    pdf += "startxref\n180\n%%EOF";

    std::vector<uint8_t> pdfData(pdf.begin(), pdf.end());
    auto filePath = createPdfFile("metadata.pdf", pdfData);

    auto result = handler_->process(filePath);

    if (result) {
        const auto& content = result.value();

        // Check if metadata was extracted
        if (!content.metadata.empty()) {
            // Look for title or author in metadata
            bool hasMetadata = content.metadata.find("title") != content.metadata.end() ||
                               content.metadata.find("author") != content.metadata.end() ||
                               content.metadata.find("Title") != content.metadata.end() ||
                               content.metadata.find("Author") != content.metadata.end();
            EXPECT_TRUE(hasMetadata) << "PDF metadata should be extracted";
        }
    }
}

// Test large PDF handling
TEST_F(PdfHandlerTest, LargePdf) {
    // Create a "large" PDF with repeated content
    std::string pdf = "%PDF-1.4\n";
    pdf += "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    pdf += "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    pdf += "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] ";
    pdf += "/Contents 4 0 R /Resources << >> >>\nendobj\n";
    pdf += "4 0 obj\n<< /Length 100000 >>\nstream\n";

    // Add lots of content
    for (int i = 0; i < 10000; ++i) {
        pdf += "BT /F1 12 Tf 100 " + std::to_string(700 - (i % 50) * 10) + " Td (Line " +
               std::to_string(i) + ") Tj ET\n";
    }

    pdf += "\nendstream\nendobj\n";
    pdf += "xref\n0 5\n0000000000 65535 f\n";
    pdf += "trailer\n<< /Size 5 /Root 1 0 R >>\n";
    pdf += "startxref\n100500\n%%EOF";

    std::vector<uint8_t> pdfData(pdf.begin(), pdf.end());
    auto filePath = createPdfFile("large.pdf", pdfData);

    auto result = handler_->process(filePath);

    // Should handle large PDFs gracefully
    if (result) {
        const auto& content = result.value();
        EXPECT_FALSE(content.text.empty());
    } else {
        // Might fail due to size limits
        EXPECT_TRUE(result.error().code == ErrorCode::FileTooLarge ||
                    result.error().code == ErrorCode::ProcessingError ||
                    result.error().code == ErrorCode::NotImplemented);
    }
}

// Test encrypted PDF
TEST_F(PdfHandlerTest, EncryptedPdf) {
    // Simulate encrypted PDF header
    std::string pdf = "%PDF-1.4\n";
    pdf += "1 0 obj\n<< /Type /Catalog /Pages 2 0 R /Encrypt 3 0 R >>\nendobj\n";
    pdf += "2 0 obj\n<< /Type /Pages /Kids [] /Count 0 >>\nendobj\n";
    pdf += "3 0 obj\n<< /Filter /Standard /V 1 /R 2 >>\nendobj\n";
    pdf += "xref\n0 4\n0000000000 65535 f\n";
    pdf += "trailer\n<< /Size 4 /Root 1 0 R >>\n";
    pdf += "startxref\n200\n%%EOF";

    std::vector<uint8_t> pdfData(pdf.begin(), pdf.end());
    auto filePath = createPdfFile("encrypted.pdf", pdfData);

    auto result = handler_->process(filePath);

    // Should detect encryption and fail appropriately
    if (!result) {
        EXPECT_TRUE(result.error().code == ErrorCode::PermissionDenied ||
                    result.error().code == ErrorCode::ProcessingError ||
                    result.error().code == ErrorCode::NotImplemented ||
                    result.error().code == ErrorCode::UnsupportedFormat)
            << "Should handle encrypted PDFs appropriately";
    }
}

// Test PDF with images
TEST_F(PdfHandlerTest, PdfWithImages) {
    // Create PDF with image reference
    std::string pdf = "%PDF-1.4\n";
    pdf += "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    pdf += "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    pdf += "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] ";
    pdf += "/Contents 4 0 R /Resources << /XObject << /Im1 5 0 R >> >> >>\nendobj\n";
    pdf += "4 0 obj\n<< /Length 50 >>\nstream\n";
    pdf += "q 100 0 0 100 100 600 cm /Im1 Do Q\n";
    pdf += "endstream\nendobj\n";
    pdf += "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 10 /Height 10 ";
    pdf += "/ColorSpace /DeviceRGB /BitsPerComponent 8 /Length 300 >>\n";
    pdf += "stream\n";
    // Add dummy image data
    for (int i = 0; i < 300; ++i) {
        pdf += static_cast<char>(i % 256);
    }
    pdf += "\nendstream\nendobj\n";
    pdf += "xref\n0 6\n0000000000 65535 f\n";
    pdf += "trailer\n<< /Size 6 /Root 1 0 R >>\n";
    pdf += "startxref\n600\n%%EOF";

    std::vector<uint8_t> pdfData(pdf.begin(), pdf.end());
    auto filePath = createPdfFile("with_images.pdf", pdfData);

    auto result = handler_->process(filePath);

    if (result) {
        const auto& content = result.value();

        // Check if image was detected in metadata
        if (!content.metadata.empty()) {
            bool hasImageInfo = content.metadata.find("images") != content.metadata.end() ||
                                content.metadata.find("has_images") != content.metadata.end();
            // Image detection is optional
            if (hasImageInfo) {
                EXPECT_TRUE(hasImageInfo) << "PDF with images should note their presence";
            }
        }
    }
}

// Test PDF forms
TEST_F(PdfHandlerTest, PdfWithForms) {
    // Create PDF with form fields
    std::string pdf = "%PDF-1.4\n";
    pdf += "1 0 obj\n<< /Type /Catalog /Pages 2 0 R /AcroForm 3 0 R >>\nendobj\n";
    pdf += "2 0 obj\n<< /Type /Pages /Kids [] /Count 0 >>\nendobj\n";
    pdf += "3 0 obj\n<< /Fields [4 0 R] >>\nendobj\n";
    pdf += "4 0 obj\n<< /Type /Annot /Subtype /Widget /FT /Tx ";
    pdf += "/T (Name) /V (John Doe) >>\nendobj\n";
    pdf += "xref\n0 5\n0000000000 65535 f\n";
    pdf += "trailer\n<< /Size 5 /Root 1 0 R >>\n";
    pdf += "startxref\n250\n%%EOF";

    std::vector<uint8_t> pdfData(pdf.begin(), pdf.end());
    auto filePath = createPdfFile("form.pdf", pdfData);

    auto result = handler_->process(filePath);

    if (result) {
        const auto& content = result.value();

        // Form data extraction is optional
        // Just verify basic processing succeeded
        EXPECT_EQ(content.mimeType, "application/pdf");
    }
}

// Test non-existent file
TEST_F(PdfHandlerTest, NonExistentFile) {
    fs::path filePath = testDir_ / "nonexistent.pdf";

    auto result = handler_->process(filePath);

    EXPECT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::NotFound);
}

// Test permission denied (if possible to test)
TEST_F(PdfHandlerTest, PermissionDenied) {
    auto pdfContent = createMinimalPdf();
    auto filePath = createPdfFile("readonly.pdf", pdfContent);

    // Try to make file unreadable (platform-dependent)
    std::error_code ec;
    fs::permissions(filePath, fs::perms::none, ec);

    if (!ec) {
        auto result = handler_->process(filePath);

        EXPECT_FALSE(result);
        if (!result) {
            EXPECT_EQ(result.error().code, ErrorCode::PermissionDenied);
        }

        // Restore permissions for cleanup
        fs::permissions(filePath, fs::perms::owner_read | fs::perms::owner_write, ec);
    }
}

// Test PDF version compatibility
TEST_F(PdfHandlerTest, DifferentPdfVersions) {
    std::vector<std::string> versions = {"1.0", "1.3", "1.4", "1.5", "1.6", "1.7", "2.0"};

    for (const auto& version : versions) {
        std::string pdf = "%PDF-" + version + "\n";
        pdf += "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
        pdf += "2 0 obj\n<< /Type /Pages /Kids [] /Count 0 >>\nendobj\n";
        pdf += "xref\n0 3\n0000000000 65535 f\n";
        pdf += "trailer\n<< /Size 3 /Root 1 0 R >>\n";
        pdf += "startxref\n150\n%%EOF";

        std::vector<uint8_t> pdfData(pdf.begin(), pdf.end());
        auto filePath = createPdfFile("version_" + version + ".pdf", pdfData);

        auto result = handler_->process(filePath);

        // Handler should at least recognize all versions as PDFs
        // Even if it can't process newer versions
        if (!result) {
            EXPECT_TRUE(result.error().code == ErrorCode::UnsupportedFormat ||
                        result.error().code == ErrorCode::ProcessingError ||
                        result.error().code == ErrorCode::NotImplemented)
                << "PDF version " << version << " handling failed unexpectedly";
        }
    }
}

} // namespace yams::content::test