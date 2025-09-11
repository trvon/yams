#include <spdlog/spdlog.h>
#include <fstream>
#include "pdf_content_handler.h"
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/text_extractor.h>

namespace yams::content {

PdfContentHandler::PdfContentHandler() {
    ensureExtractorRegistered();
}

bool PdfContentHandler::canHandle(const detection::FileSignature& signature) const {
    // Check if this is a PDF file
    return signature.mimeType == "application/pdf" || signature.fileType == "pdf" ||
           signature.description.find("PDF") != std::string::npos;
}

std::vector<std::string> PdfContentHandler::supportedMimeTypes() const {
    return {"application/pdf", "application/x-pdf", "application/vnd.pdf"};
}

Result<ContentResult> PdfContentHandler::process(const std::filesystem::path& path,
                                                 const ContentConfig& config) {
    auto start = std::chrono::steady_clock::now();

    // Validate file exists
    if (!std::filesystem::exists(path)) {
        return Error{ErrorCode::FileNotFound, "File not found: " + path.string()};
    }

    // Check file size
    auto fileSize = std::filesystem::file_size(path);
    if (fileSize > config.maxFileSize) {
        return Error{ErrorCode::ResourceExhausted,
                     "File exceeds maximum size: " + std::to_string(fileSize)};
    }

    // Get PDF extractor
    auto extractor = getExtractor();
    if (!extractor) {
        return Error{ErrorCode::NotSupported, "PDF extractor not available"};
    }

    // Create extraction config from content config
    extraction::ExtractionConfig extractConfig;
    extractConfig.maxFileSize = config.maxFileSize;
    extractConfig.preserveFormatting = config.preserveFormatting;
    extractConfig.detectLanguage = config.detectLanguage;
    extractConfig.preferredEncoding = config.preferredEncoding;
    extractConfig.extractMetadata = true; // Always extract PDF metadata

    // Extract text and metadata
    auto extractResult = extractor->extract(path, extractConfig);
    if (!extractResult) {
        // Propagate underlying error code (tests expect InvalidData for corrupted/empty)
        return Error{extractResult.error().code,
                     "PDF extraction failed: " + extractResult.error().message};
    }

    // Convert to ContentResult
    auto result = convertResult(extractResult.value(), path);

    // Add processing time
    auto end = std::chrono::steady_clock::now();
    result.processingTimeMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    // Add file size and processing time to metadata for observability
    try {
        // Reuse earlier computed fileSize (avoid shadowing warnings)
        result.metadata["file_size"] = std::to_string(fileSize);
    } catch (...) {
        // ignore file size errors
    }
    result.metadata["processing_time_ms"] = std::to_string(result.processingTimeMs);

    spdlog::debug("PdfContentHandler processed {} in {}ms", path.string(), result.processingTimeMs);

    return result;
}

Result<ContentResult> PdfContentHandler::processBuffer(std::span<const std::byte> data,
                                                       const std::string& hint,
                                                       const ContentConfig& config) {
    auto start = std::chrono::steady_clock::now();

    // Check buffer size
    if (data.size() > config.maxFileSize) {
        return Error{ErrorCode::ResourceExhausted,
                     "Buffer exceeds maximum size: " + std::to_string(data.size())};
    }

    // Get PDF extractor
    auto extractor = getExtractor();
    if (!extractor) {
        return Error{ErrorCode::NotSupported, "PDF extractor not available"};
    }

    // Create extraction config
    extraction::ExtractionConfig extractConfig;
    extractConfig.maxFileSize = config.maxFileSize;
    extractConfig.preserveFormatting = config.preserveFormatting;
    extractConfig.detectLanguage = config.detectLanguage;
    extractConfig.extractMetadata = true;

    // Extract from buffer
    auto extractResult = extractor->extractFromBuffer(data, extractConfig);
    if (!extractResult) {
        return Error{extractResult.error().code,
                     "PDF extraction from buffer failed: " + extractResult.error().message};
    }

    // Convert to ContentResult
    auto result = convertResult(extractResult.value());

    // Add buffer metadata
    result.metadata["source"] = "buffer";
    result.metadata["size"] = std::to_string(data.size());
    if (!hint.empty()) {
        result.metadata["hint"] = hint;
    }

    // Add processing time
    auto end = std::chrono::steady_clock::now();
    result.processingTimeMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    // Add timing to metadata for buffer case as well
    result.metadata["processing_time_ms"] = std::to_string(result.processingTimeMs);
    return result;
}

ContentResult PdfContentHandler::convertResult(const extraction::ExtractionResult& extractionResult,
                                               const std::filesystem::path& path) {
    ContentResult result;

    // Copy text content
    result.text = extractionResult.text;

    // Copy metadata
    result.metadata = extractionResult.metadata;

    // Add extraction-specific metadata
    result.metadata["extraction_method"] =
        extractionResult.extractionMethod.empty() ? "pdfium" : extractionResult.extractionMethod;

    if (!extractionResult.language.empty()) {
        result.metadata["language"] = extractionResult.language;
    }

    if (extractionResult.pageCount > 0) {
        result.metadata["page_count"] = std::to_string(extractionResult.pageCount);
    }

    result.metadata["content_length"] = std::to_string(extractionResult.contentLength());

    // PDF-specific metadata (if available in the extraction metadata)
    // These might be populated by PdfExtractor
    auto checkAndAdd = [&](const std::string& key) {
        if (extractionResult.metadata.find(key) != extractionResult.metadata.end()) {
            result.metadata["pdf_" + key] = extractionResult.metadata.at(key);
        }
    };

    checkAndAdd("title");
    checkAndAdd("author");
    checkAndAdd("subject");
    checkAndAdd("keywords");
    checkAndAdd("creator");
    checkAndAdd("producer");
    checkAndAdd("creation_date");
    checkAndAdd("modification_date");

    // Set handler info
    result.handlerName = name();
    result.contentType = "application/pdf";
    result.fileFormat = "PDF";

    // Set flags
    result.isComplete = extractionResult.success;
    result.shouldIndex = true; // PDFs should be indexed for search

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

std::unique_ptr<extraction::PdfExtractor> PdfContentHandler::getExtractor() {
    // Create a new PdfExtractor instance
    // The PdfExtractor class handles its own thread safety
    return std::make_unique<extraction::PdfExtractor>();
}

void PdfContentHandler::ensureExtractorRegistered() {
    // PdfExtractor typically self-registers via a registration function
    // or static initialization. If needed, we can call registerPdfExtractor()
    // but this is usually handled by the extraction module

    // Note: The PdfExtractor registration is handled by the extraction module
    // We just need to ensure it's available when needed
    spdlog::debug("PdfContentHandler initialized - PDF extraction available");
}

} // namespace yams::content
