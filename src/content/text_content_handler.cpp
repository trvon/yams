#include <spdlog/spdlog.h>
#include <fstream>
#include <sstream>
#include <yams/content/text_content_handler.h>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/plain_text_extractor.h>
#include <yams/extraction/text_extractor.h>

namespace yams::content {

TextContentHandler::TextContentHandler() {
    // Ensure the PlainTextExtractor is available
    // The extractor self-registers via REGISTER_EXTRACTOR macro
    ensureExtractorRegistered();
}

bool TextContentHandler::canHandle(const detection::FileSignature& signature) const {
    // Use FileTypeDetector to check if this is a text file
    auto& detector = detection::FileTypeDetector::instance();
    return detector.isTextMimeType(signature.mimeType);
}

std::vector<std::string> TextContentHandler::supportedMimeTypes() const {
    // Return common text MIME type patterns
    // These are patterns that the handler supports
    return {
        "text/plain",              // Explicit plain text
        "text/*",                  // All text types
        "application/json",        // JSON
        "application/xml",         // XML
        "application/x-yaml",      // YAML
        "application/yaml",        // YAML alt
        "application/javascript",  // JavaScript
        "application/x-sh",        // Shell scripts
        "application/x-python",    // Python
        "application/x-ruby",      // Ruby
        "application/x-perl",      // Perl
        "application/x-httpd-php", // PHP
        "application/sql",         // SQL
        "application/toml",        // TOML
        "application/x-tex",       // LaTeX
    };
}

Result<ContentResult> TextContentHandler::process(const std::filesystem::path& path,
                                                  const ContentConfig& config) {
    auto start = std::chrono::steady_clock::now();

    // Validate file exists
    std::error_code fsEc;
    if (!std::filesystem::exists(path, fsEc)) {
        return Error{ErrorCode::NotFound, "File not found: " + path.string()};
    }
    // Reject directories to avoid exceptions when reading size/contents
    if (std::filesystem::is_directory(path, fsEc)) {
        return Error{ErrorCode::FileNotFound, "Path is a directory: " + path.string()};
    }

    // Check file size
    auto fileSize = std::filesystem::file_size(path, fsEc);
    if (fsEc) {
        return Error{ErrorCode::IOError,
                     "Failed to stat file: " + path.string() + ": " + fsEc.message()};
    }
    if (fileSize > config.maxFileSize) {
        return Error{ErrorCode::ResourceExhausted,
                     "File exceeds maximum size: " + std::to_string(fileSize)};
    }

    // Get text extractor
    auto extractor = getExtractor(path);
    if (!extractor) {
        return Error{ErrorCode::NotSupported, "No text extractor available for: " + path.string()};
    }

    // Create extraction config from content config
    extraction::ExtractionConfig extractConfig;
    extractConfig.maxFileSize = config.maxFileSize;
    // Preserve original formatting (line breaks, tabs, CR/LF) for text files by default
    // to match expected behavior in content tests. Callers can still override at extractor level
    // later if needed, but for TextContentHandler we keep raw text intact.
    extractConfig.preserveFormatting = true;
    extractConfig.detectLanguage = config.detectLanguage;
    extractConfig.preferredEncoding = config.preferredEncoding;

    // Extract text
    auto extractResult = extractor->extract(path, extractConfig);
    if (!extractResult) {
        return Error{ErrorCode::InternalError,
                     "Text extraction failed: " + extractResult.error().message};
    }

    // Convert to ContentResult
    auto result = convertResult(extractResult.value(), path);

    // Add processing time
    auto end = std::chrono::steady_clock::now();
    result.processingTimeMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    spdlog::debug("TextContentHandler processed {} in {}ms", path.string(),
                  result.processingTimeMs);

    return result;
}

Result<ContentResult> TextContentHandler::processBuffer(std::span<const std::byte> data,
                                                        const std::string& hint,
                                                        const ContentConfig& /*config*/) {
    auto start = std::chrono::steady_clock::now();

    // Convert byte data to string
    std::string text(reinterpret_cast<const char*>(data.data()), data.size());

    ContentResult result;
    result.text = std::move(text);
    result.handlerName = name();
    result.contentType = "text/plain";
    result.isComplete = true;
    result.shouldIndex = true;

    // Basic metadata
    result.metadata["source"] = "buffer";
    result.metadata["size"] = std::to_string(data.size());
    if (!hint.empty()) {
        result.metadata["hint"] = hint;
    }

    // Add processing time
    auto end = std::chrono::steady_clock::now();
    result.processingTimeMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    return result;
}

ContentResult
TextContentHandler::convertResult(const extraction::ExtractionResult& extractionResult,
                                  const std::filesystem::path& path) {
    ContentResult result;

    // Copy text content
    result.text = extractionResult.text;

    // Copy metadata
    result.metadata = extractionResult.metadata;

    // Add extraction-specific metadata
    result.metadata["extraction_method"] = extractionResult.extractionMethod;
    if (!extractionResult.language.empty()) {
        result.metadata["language"] = extractionResult.language;
    }
    if (extractionResult.pageCount > 0) {
        result.metadata["page_count"] = std::to_string(extractionResult.pageCount);
    }
    result.metadata["content_length"] = std::to_string(extractionResult.contentLength());

    // Set handler info
    result.handlerName = name();
    result.contentType = "text/plain";

    // Set flags
    result.isComplete = extractionResult.success;
    result.shouldIndex = true;

    // Copy warnings
    result.warnings = extractionResult.warnings;
    if (!extractionResult.error.empty() && !extractionResult.success) {
        result.warnings.push_back(extractionResult.error);
    }

    // Add file info if path is provided
    if (!path.empty()) {
        result.metadata["file_path"] = path.string();
        result.metadata["file_name"] = path.filename().string();
        if (path.has_extension()) {
            result.metadata["file_extension"] = path.extension().string();
        }
    }

    return result;
}

std::unique_ptr<extraction::ITextExtractor>
TextContentHandler::getExtractor(const std::filesystem::path& path) {
    // Use FileTypeDetector to detect the file type
    auto& detector = detection::FileTypeDetector::instance();
    auto signatureResult = detector.detectFromFile(path);

    if (!signatureResult) {
        spdlog::warn("Failed to detect file type for {}: {}", path.string(),
                     signatureResult.error().message);
        // Fall back to extension-based detection
        return extraction::TextExtractorFactory::instance().createForFile(path);
    }

    const auto& signature = signatureResult.value();

    // Check if it's a text file
    if (!detector.isTextMimeType(signature.mimeType)) {
        spdlog::debug("File {} is not a text file (MIME: {})", path.string(), signature.mimeType);
        return nullptr;
    }

    // Try to create extractor based on extension first (for specialized extractors)
    auto extractor = extraction::TextExtractorFactory::instance().createForFile(path);
    if (extractor) {
        return extractor;
    }

    // Fall back to PlainTextExtractor for any text file
    spdlog::debug("Using PlainTextExtractor for {}", path.string());
    return std::make_unique<extraction::PlainTextExtractor>();
}

void TextContentHandler::ensureExtractorRegistered() {
    // The PlainTextExtractor self-registers via REGISTER_EXTRACTOR macro
    // when the translation unit is loaded. We just need to ensure
    // the translation unit is linked.

    // This is a no-op but ensures the PlainTextExtractor translation unit
    // is not optimized away by the linker
    static bool registered = []() {
        spdlog::debug("TextContentHandler ensuring text extractors are available");
        return true;
    }();
    (void)registered;
}

} // namespace yams::content