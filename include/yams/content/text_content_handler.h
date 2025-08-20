#pragma once

#include <memory>
#include <yams/content/content_handler.h>
#include <yams/extraction/text_extractor.h>

namespace yams::content {

/**
 * @brief Handler for text files
 *
 * Wraps existing text extraction functionality from ITextExtractor
 * to work with the new content handler system.
 */
class TextContentHandler : public IContentHandler {
public:
    /**
     * @brief Construct text content handler
     */
    TextContentHandler();

    ~TextContentHandler() override = default;

    /**
     * @brief Check if this handler can process the given file
     */
    [[nodiscard]] bool canHandle(const detection::FileSignature& signature) const override;

    /**
     * @brief Get handler priority
     */
    [[nodiscard]] int priority() const override { return 10; }

    /**
     * @brief Get handler name
     */
    [[nodiscard]] std::string name() const override { return "TextContentHandler"; }

    /**
     * @brief Get supported MIME types
     */
    [[nodiscard]] std::vector<std::string> supportedMimeTypes() const override;

    /**
     * @brief Process file and extract content
     */
    Result<ContentResult> process(const std::filesystem::path& path,
                                  const ContentConfig& config = {}) override;

    /**
     * @brief Process from memory buffer
     */
    Result<ContentResult> processBuffer(std::span<const std::byte> data, const std::string& hint,
                                        const ContentConfig& config = {}) override;

private:
    /**
     * @brief Convert legacy extraction result to content result
     */
    ContentResult convertResult(const extraction::ExtractionResult& extractionResult,
                                const std::filesystem::path& path = {});

    /**
     * @brief Get or create text extractor for file
     */
    std::unique_ptr<extraction::ITextExtractor> getExtractor(const std::filesystem::path& path);

    /**
     * @brief Register text extractor if needed
     */
    void ensureExtractorRegistered();
};

} // namespace yams::content