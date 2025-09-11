#pragma once

#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <vector>
#include <yams/extraction/text_extractor.h>

// PDFium headers (types only)
#include <fpdf_doc.h>
#include <fpdf_text.h>
#include <fpdfview.h>

namespace yams::extraction {

/// Backward-compatible alias to align with ITextExtractor
using ExtractedText = ExtractionResult;

/// Minimal section descriptor extracted from PDFs
struct TextSection {
    std::string title;
    int level = 1;
    size_t startOffset = 0;
    std::string content;
};

/**
 * @brief Options for PDF text extraction
 */
struct PdfExtractOptions {
    bool extractAll = true;      // Extract all text
    bool extractMetadata = true; // Extract PDF metadata (title, author, etc.)
    int maxPages = -1;           // Limit pages (-1 for all)
    bool preserveLayout = false; // Try to preserve text layout
    std::string searchText;      // Extract only around this text
    int contextLines = 5;        // Lines of context around search text
};

/**
 * @brief Simple PDF text extractor
 *
 * This implementation provides basic text extraction from PDF files
 * without external dependencies. It works best with text-based PDFs
 * and may not handle complex layouts or encrypted PDFs.
 */
class PdfExtractor : public ITextExtractor {
public:
    PdfExtractor() = default;
    ~PdfExtractor() override = default;

    /**
     * @brief Extract text from a PDF file
     */
    Result<ExtractionResult> extract(const std::filesystem::path& path,
                                     const ExtractionConfig& config = {}) override;

    /**
     * @brief Extract text from PDF data in memory
     */
    Result<ExtractionResult> extractFromBuffer(std::span<const std::byte> data,
                                               const ExtractionConfig& config = {}) override;

    /**
     * @brief Get supported extensions
     */
    std::vector<std::string> supportedExtensions() const override { return {".pdf"}; }
    std::string name() const override { return "PDF"; }

    /**
     * @brief Get supported MIME types
     */
    std::vector<std::string> supportedMimeTypes() const { return {"application/pdf"}; }

    // Advanced extraction methods

    /**
     * @brief Extract text from specific page
     */
    Result<ExtractionResult> extractPage(const std::filesystem::path& path, int page);

    /**
     * @brief Extract text from page range
     */
    Result<ExtractionResult> extractRange(const std::filesystem::path& path, int startPage,
                                          int endPage);

    /**
     * @brief Extract with custom options
     */
    Result<ExtractionResult> extractWithOptions(const std::filesystem::path& path,
                                                const PdfExtractOptions& options);

    /**
     * @brief Extract text around search term with context
     */
    Result<ExtractionResult> extractAroundText(const std::filesystem::path& path,
                                               const std::string& searchText, int contextLines = 5);

private:
    /**
     * @brief PDFium document wrapper for RAII
     */
    struct PdfDocument {
        FPDF_DOCUMENT doc = nullptr;
        ~PdfDocument();

        PdfDocument() = default;
        PdfDocument(const PdfDocument&) = delete;
        PdfDocument& operator=(const PdfDocument&) = delete;
        PdfDocument(PdfDocument&& other) noexcept : doc(other.doc) { other.doc = nullptr; }
        PdfDocument& operator=(PdfDocument&& other) noexcept {
            if (this != &other) {
                if (doc)
                    this->~PdfDocument();
                doc = other.doc;
                other.doc = nullptr;
            }
            return *this;
        }
    };

    /**
     * @brief PDFium page wrapper for RAII
     */
    struct PdfPage {
        FPDF_PAGE page = nullptr;
        FPDF_TEXTPAGE textPage = nullptr;
        ~PdfPage();

        PdfPage() = default;
        PdfPage(const PdfPage&) = delete;
        PdfPage& operator=(const PdfPage&) = delete;
        PdfPage(PdfPage&& other) noexcept : page(other.page), textPage(other.textPage) {
            other.page = nullptr;
            other.textPage = nullptr;
        }
    };

    /**
     * @brief Initialize PDFium library (called once)
     */
    static void initializePdfium();

    /**
     * @brief Cleanup PDFium library (called at exit)
     */
    static void cleanupPdfium();

    /**
     * @brief Check if PDFium is initialized
     */
    static bool isPdfiumInitialized();

    /**
     * @brief Internal extraction implementation
     */
    Result<ExtractionResult> extractInternal(std::span<const std::byte> data,
                                             const PdfExtractOptions& options);

    /**
     * @brief Extract metadata from PDF document
     */
    void extractMetadata(FPDF_DOCUMENT doc, ExtractionResult& result);

    /**
     * @brief Extract text from a single page
     */
    std::string extractPageText(FPDF_PAGE page);

    /**
     * @brief Convert UTF-16 to UTF-8
     */
    static std::string convertUtf16ToUtf8(const std::vector<unsigned short>& utf16);

    /**
     * @brief Clean extracted text
     */
    std::string cleanText(const std::string& rawText);

    /**
     * @brief Find text in context
     */
    std::vector<std::string> findTextInContext(const std::vector<std::string>& lines,
                                               const std::string& searchText, int contextLines);

    /**
     * @brief Extract document sections/structure
     */
    std::vector<TextSection> extractSections(const std::string& text);

    /**
     * @brief Detect language of extracted text
     */
    std::string detectLanguage(const std::string& text);

    // Static initialization flag
    static bool pdfiumInitialized;
    static std::mutex pdfiumMutex;
};

/**
 * @brief Register PDF extractor with factory
 */
void registerPdfExtractor();

} // namespace yams::extraction