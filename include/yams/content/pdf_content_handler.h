#pragma once

#include <memory>
#include <yams/content/content_handler.h>
#include <yams/extraction/pdf_extractor.h>

namespace yams::content {

/**
 * @brief Content handler for PDF files
 *
 * This handler wraps the existing PdfExtractor to provide
 * PDF processing capabilities within the universal content
 * handler system.
 */
class PdfContentHandler : public IContentHandler {
public:
    PdfContentHandler();
    ~PdfContentHandler() override = default;

    // IContentHandler interface
    std::string name() const override { return "PdfContentHandler"; }

    bool canHandle(const detection::FileSignature& signature) const override;

    std::vector<std::string> supportedMimeTypes() const override;

    Result<ContentResult> process(const std::filesystem::path& path,
                                  const ContentConfig& config = {}) override;

    Result<ContentResult> processBuffer(std::span<const std::byte> data,
                                        const std::string& hint = "",
                                        const ContentConfig& config = {}) override;

private:
    /**
     * @brief Convert PdfExtractor result to ContentResult
     */
    ContentResult convertResult(const extraction::ExtractionResult& extractionResult,
                                const std::filesystem::path& path = {});

    /**
     * @brief Get or create PDF extractor instance
     */
    std::unique_ptr<extraction::PdfExtractor> getExtractor();

    /**
     * @brief Ensure PDF extractor is registered with factory
     */
    void ensureExtractorRegistered();
};

} // namespace yams::content