#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>
#include <yams/extraction/text_extractor.h>

// Forward declarations for QPDF
class QPDF;
class QPDFPageObjectHelper;

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
     * @brief Internal extraction implementation using QPDF
     */
    Result<ExtractionResult> extractFromDocument(QPDF& pdf, const PdfExtractOptions& options);

    /**
     * @brief Extract metadata from PDF document
     */
    void extractMetadata(QPDF& pdf, ExtractionResult& result);

    /**
     * @brief Extract text from a single page
     */
    std::string extractPageText(QPDFPageObjectHelper page);

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
};

/**
 * @brief Register PDF extractor with factory
 */
void registerPdfExtractor();

} // namespace yams::extraction