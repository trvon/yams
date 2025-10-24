#pragma once

#include <fstream>
#include <yams/extraction/text_extractor.h>

namespace yams::extraction {

/**
 * @brief Extractor for plain text files
 *
 * Supports various text formats including:
 * - Plain text (.txt)
 * - Markdown (.md)
 * - Log files (.log)
 * - CSV files (.csv)
 * - JSON files (.json)
 * - XML files (.xml)
 * - Source code files (.cpp, .h, .py, .js, etc.)
 */
class PlainTextExtractor : public ITextExtractor {
public:
    PlainTextExtractor() = default;
    ~PlainTextExtractor() override = default;

    Result<ExtractionResult> extract(const std::filesystem::path& path,
                                     const ExtractionConfig& config = {}) override;

    Result<ExtractionResult> extractFromBuffer(std::span<const std::byte> data,
                                               const ExtractionConfig& config = {}) override;

    std::vector<std::string> supportedExtensions() const override;

    std::string name() const override { return "Plain Text Extractor"; }

private:
    /**
     * @brief Read file with encoding detection
     */
    Result<std::string> readFileWithEncoding(const std::filesystem::path& path,
                                             const ExtractionConfig& config);

    /**
     * @brief Process text based on file type
     */
    void processTextByType(ExtractionResult& result, const std::filesystem::path& path,
                           const ExtractionConfig& config);

    /**
     * @brief Check if file is likely binary
     */
    bool isBinaryFile(std::span<const std::byte> data);

    /**
     * @brief Check if data is parseable text (UTF-8 or ASCII)
     */
    bool isParseableText(std::span<const std::byte> data);

    /**
     * @brief Extract metadata from specific file types
     */
    void extractFileMetadata(ExtractionResult& result, const std::filesystem::path& path);
};

} // namespace yams::extraction