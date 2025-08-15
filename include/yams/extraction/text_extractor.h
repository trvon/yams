#pragma once

#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <yams/core/types.h>

namespace yams::extraction {

/**
 * @brief Result of text extraction from a document
 */
struct ExtractionResult {
    std::string text;                                      // Extracted text content
    std::string language;                                  // Detected language (ISO 639-1)
    std::unordered_map<std::string, std::string> metadata; // Additional metadata
    std::vector<std::string> warnings;                     // Non-fatal warnings
    bool success = true;                                   // Whether extraction succeeded
    std::string error;                                     // Error message if failed
    std::string extractionMethod;                          // Method used for extraction
    size_t pageCount = 0;                                  // Number of pages (if applicable)

    /**
     * @brief Check if extraction was successful
     */
    [[nodiscard]] bool isSuccess() const { return success && error.empty(); }

    /**
     * @brief Get content length
     */
    [[nodiscard]] size_t contentLength() const { return text.length(); }
};

/**
 * @brief Configuration for text extraction
 */
struct ExtractionConfig {
    size_t maxFileSize = 100 * 1024 * 1024;   // 100MB default limit
    bool preserveFormatting = false;          // Preserve line breaks and spacing
    bool extractMetadata = true;              // Extract document metadata
    bool detectLanguage = true;               // Auto-detect language
    std::string preferredEncoding;            // Preferred text encoding (empty = auto)
    std::chrono::milliseconds timeout{30000}; // Extraction timeout (30s default)
};

/**
 * @brief Base interface for text extractors
 */
class ITextExtractor {
public:
    virtual ~ITextExtractor() = default;

    /**
     * @brief Extract text from a file
     * @param path Path to the file to extract
     * @param config Extraction configuration
     * @return Extraction result
     */
    virtual Result<ExtractionResult> extract(const std::filesystem::path& path,
                                             const ExtractionConfig& config = {}) = 0;

    /**
     * @brief Extract text from memory buffer
     * @param data File data in memory
     * @param config Extraction configuration
     * @return Extraction result
     */
    virtual Result<ExtractionResult> extractFromBuffer(std::span<const std::byte> data,
                                                       const ExtractionConfig& config = {}) = 0;

    /**
     * @brief Get supported file extensions
     * @return List of supported extensions (e.g., ".txt", ".pdf")
     */
    virtual std::vector<std::string> supportedExtensions() const = 0;

    /**
     * @brief Get extractor name
     * @return Human-readable name of the extractor
     */
    virtual std::string name() const = 0;

    /**
     * @brief Check if a file is supported
     * @param path File path to check
     * @return True if the file can be processed by this extractor
     */
    virtual bool canExtract(const std::filesystem::path& path) const {
        auto ext = path.extension().string();
        std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);
        auto supported = supportedExtensions();
        return std::find(supported.begin(), supported.end(), ext) != supported.end();
    }
};

/**
 * @brief Factory for creating text extractors
 */
class TextExtractorFactory {
public:
    using ExtractorCreator = std::function<std::unique_ptr<ITextExtractor>()>;

    /**
     * @brief Get singleton instance
     */
    static TextExtractorFactory& instance();

    /**
     * @brief Create an extractor for a given file extension
     * @param extension File extension (e.g., ".pdf")
     * @return Extractor instance or nullptr if not supported
     */
    std::unique_ptr<ITextExtractor> create(const std::string& extension);

    /**
     * @brief Create an extractor for a given file path
     * @param path File path to extract from
     * @return Extractor instance or nullptr if not supported
     */
    std::unique_ptr<ITextExtractor> createForFile(const std::filesystem::path& path);

    /**
     * @brief Register an extractor for specific extensions
     * @param extensions List of supported extensions
     * @param creator Function to create extractor instances
     */
    void registerExtractor(const std::vector<std::string>& extensions, ExtractorCreator creator);

    /**
     * @brief Get all supported extensions
     * @return List of all registered extensions
     */
    std::vector<std::string> supportedExtensions() const;

    /**
     * @brief Check if an extension is supported
     * @param extension File extension to check
     * @return True if an extractor is registered for this extension
     */
    bool isSupported(const std::string& extension) const;

private:
    TextExtractorFactory() = default;
    std::unordered_map<std::string, ExtractorCreator> extractors_;
    mutable std::mutex mutex_;
};

/**
 * @brief RAII helper for registering extractors
 */
class ExtractorRegistrar {
public:
    ExtractorRegistrar(const std::vector<std::string>& extensions,
                       TextExtractorFactory::ExtractorCreator creator) {
        TextExtractorFactory::instance().registerExtractor(extensions, std::move(creator));
    }
};

/**
 * @brief Helper macro for registering extractors
 */
#define REGISTER_EXTRACTOR(ExtractorClass, ...)                                                    \
    static ::yams::extraction::ExtractorRegistrar _reg_##ExtractorClass(                           \
        {__VA_ARGS__}, []() { return std::make_unique<ExtractorClass>(); })

/**
 * @brief Encoding detection utilities
 */
class EncodingDetector {
public:
    /**
     * @brief Detect text encoding from a buffer
     * @param data Data buffer to analyze
     * @param confidence Confidence level (0.0-1.0)
     * @return Detected encoding name (e.g., "UTF-8", "ISO-8859-1")
     */
    static std::string detectEncoding(std::span<const std::byte> data,
                                      double* confidence = nullptr);

    /**
     * @brief Convert text from one encoding to UTF-8
     * @param text Input text
     * @param fromEncoding Source encoding
     * @return UTF-8 encoded text
     */
    static Result<std::string> convertToUtf8(const std::string& text,
                                             const std::string& fromEncoding);
};

/**
 * @brief Language detection utilities
 */
class LanguageDetector {
public:
    /**
     * @brief Detect language from text
     * @param text Text to analyze
     * @param confidence Confidence level (0.0-1.0)
     * @return ISO 639-1 language code (e.g., "en", "es", "fr")
     */
    static std::string detectLanguage(const std::string& text, double* confidence = nullptr);
};

} // namespace yams::extraction