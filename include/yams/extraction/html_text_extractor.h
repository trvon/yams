#pragma once

#include <regex>
#include <string>
#include <yams/extraction/text_extractor.h>

namespace yams::extraction {

/**
 * @brief HTML text extractor for converting HTML to plain text
 *
 * This extractor removes HTML tags and converts HTML content to readable plain text,
 * similar to browser "reader mode" functionality.
 */
class HtmlTextExtractor : public ITextExtractor {
public:
    HtmlTextExtractor() = default;
    ~HtmlTextExtractor() override = default;

    /**
     * @brief Get extractor name
     */
    std::string name() const override { return "HtmlTextExtractor"; }

    /**
     * @brief Extract text from HTML file
     */
    Result<ExtractionResult> extract(const std::filesystem::path& path,
                                     const ExtractionConfig& config = {}) override;

    /**
     * @brief Extract text from HTML buffer
     */
    Result<ExtractionResult> extractFromBuffer(std::span<const std::byte> data,
                                               const ExtractionConfig& config = {}) override;

    /**
     * @brief Get supported extensions
     */
    std::vector<std::string> supportedExtensions() const override;

    /**
     * @brief Check if file type is supported
     */
    bool canHandle(const std::string& mimeType) const;

    /**
     * @brief Extract text from HTML string
     */
    static std::string extractTextFromHtml(const std::string& html);

private:
    /**
     * @brief Remove script and style blocks
     */
    static std::string removeScriptAndStyle(const std::string& html);

    /**
     * @brief Convert block-level tags to newlines
     */
    static std::string convertBlockTagsToNewlines(const std::string& html);

    /**
     * @brief Strip remaining HTML tags
     */
    static std::string stripHtmlTags(const std::string& html);

    /**
     * @brief Decode HTML entities
     */
    static std::string decodeHtmlEntities(const std::string& text);

    /**
     * @brief Clean up excessive whitespace
     */
    static std::string cleanWhitespace(const std::string& text);

    /**
     * @brief Extract title from HTML
     */
    static std::string extractTitle(const std::string& html);

    /**
     * @brief Extract meta description
     */
    static std::string extractMetaDescription(const std::string& html);
};

} // namespace yams::extraction