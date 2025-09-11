#include <spdlog/spdlog.h>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <yams/extraction/plain_text_extractor.h>

namespace yams::extraction {

// Register the extractor for various text file types
REGISTER_EXTRACTOR(PlainTextExtractor, ".txt", ".md", ".log", ".csv", ".json", ".xml", ".yml",
                   ".yaml", ".cpp", ".cxx", ".cc", ".c", ".h", ".hpp", ".hxx", ".py", ".js", ".ts",
                   ".java", ".go", ".rs", ".rb", ".php", ".sh", ".bash", ".zsh", ".ps1", ".bat",
                   ".cmd", ".css", ".scss", ".less", ".sql", ".conf", ".cfg", ".ini", ".toml",
                   ".tex", ".bib", ".rst", ".adoc", ".org");

Result<ExtractionResult> PlainTextExtractor::extract(const std::filesystem::path& path,
                                                     const ExtractionConfig& config) {
    ExtractionResult result;
    result.extractionMethod = "plain_text";

    try {
        // Check if file exists
        if (!std::filesystem::exists(path)) {
            result.success = false;
            result.error = "File does not exist: " + path.string();
            return result;
        }

        // Check file size
        auto fileSize = std::filesystem::file_size(path);
        if (fileSize > config.maxFileSize) {
            result.success = false;
            result.error = "File too large: " + std::to_string(fileSize) + " bytes";
            return result;
        }

        // Read file with encoding detection
        auto textResult = readFileWithEncoding(path, config);
        if (!textResult) {
            result.success = false;
            result.error = textResult.error().message;
            return result;
        }

        result.text = std::move(textResult).value();

        // Debug: log the extracted text
        spdlog::debug("Extracted text length: {}", result.text.length());
        spdlog::debug("First 100 chars: {}",
                      result.text.substr(0, std::min<size_t>(100, result.text.length())));

        // Process text based on file type
        processTextByType(result, path, config);

        // Extract metadata
        if (config.extractMetadata) {
            extractFileMetadata(result, path);
        }

        // Detect language
        if (config.detectLanguage && !result.text.empty()) {
            double confidence = 0.0;
            result.language = LanguageDetector::detectLanguage(result.text, &confidence);
            result.metadata["language_confidence"] = std::to_string(confidence);
        }
    } catch (const std::exception& e) {
        result.success = false;
        result.error = "Exception during extraction: " + std::string(e.what());
        spdlog::error("Text extraction failed: {}", e.what());
    } catch (...) {
        result.success = false;
        result.error = "Unknown exception during extraction";
        spdlog::error("Text extraction failed with unknown exception");
    }

    return result;
}

Result<ExtractionResult> PlainTextExtractor::extractFromBuffer(std::span<const std::byte> data,
                                                               const ExtractionConfig& config) {
    ExtractionResult result;
    result.extractionMethod = "plain_text_buffer";

    // Check if data looks binary
    if (isBinaryFile(data)) {
        result.success = false;
        result.error = "Buffer appears to contain binary data";
        return result;
    }

    // Detect encoding
    double confidence = 0.0;
    std::string encoding = EncodingDetector::detectEncoding(data, &confidence);
    result.metadata["encoding"] = encoding;
    result.metadata["encoding_confidence"] = std::to_string(confidence);

    // Convert to string
    if (encoding == "UTF-8" || encoding == "ASCII") {
        result.text = std::string(reinterpret_cast<const char*>(data.data()), data.size());
    } else {
        // Need encoding conversion
        std::string rawText(reinterpret_cast<const char*>(data.data()), data.size());
        auto convertResult = EncodingDetector::convertToUtf8(rawText, encoding);
        if (!convertResult) {
            result.warnings.push_back("Failed to convert encoding, using raw text");
            result.text = std::move(rawText);
        } else {
            result.text = std::move(convertResult).value();
        }
    }

    // Detect language
    if (config.detectLanguage && !result.text.empty()) {
        double langConfidence = 0.0;
        result.language = LanguageDetector::detectLanguage(result.text, &langConfidence);
        result.metadata["language_confidence"] = std::to_string(langConfidence);
    }

    return result;
}

std::vector<std::string> PlainTextExtractor::supportedExtensions() const {
    return {".txt",  ".md",  ".log", ".csv",  ".json", ".xml", ".yml",  ".yaml", ".cpp",
            ".cxx",  ".cc",  ".c",   ".h",    ".hpp",  ".hxx", ".py",   ".js",   ".ts",
            ".java", ".go",  ".rs",  ".rb",   ".php",  ".sh",  ".bash", ".zsh",  ".ps1",
            ".bat",  ".cmd", ".css", ".scss", ".less", ".sql", ".conf", ".cfg",  ".ini",
            ".toml", ".tex", ".bib", ".rst",  ".adoc", ".org"};
}

Result<std::string>
PlainTextExtractor::readFileWithEncoding(const std::filesystem::path& path,
                                         [[maybe_unused]] const ExtractionConfig& config) {
    // Read entire file using simpler approach
    std::ifstream file(path, std::ios::in);
    if (!file.is_open()) {
        return Error{ErrorCode::FileNotFound, "Failed to open file: " + path.string()};
    }

    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string content = buffer.str();
    file.close();

    if (content.empty()) {
        return std::string(); // Empty file
    }

    // Simple encoding check - if it's valid UTF-8, use it
    // Otherwise warn but still use the content
    double confidence = 0.0;
    std::vector<std::byte> bytes;
    bytes.reserve(content.size());
    for (char c : content) {
        bytes.push_back(static_cast<std::byte>(c));
    }
    std::string encoding = EncodingDetector::detectEncoding(bytes, &confidence);

    if (encoding != "UTF-8" && encoding != "ASCII") {
        spdlog::warn("File may not be UTF-8 encoded (detected: {}), using raw text", encoding);
    }

    return content;
}

void PlainTextExtractor::processTextByType(ExtractionResult& result,
                                           const std::filesystem::path& path,
                                           const ExtractionConfig& config) {
    auto ext = path.extension().string();
    std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);

    // Add file type to metadata
    result.metadata["file_type"] = ext;

    // Count lines
    size_t lineCount = std::count(result.text.begin(), result.text.end(), '\n');
    if (!result.text.empty() && result.text.back() != '\n') {
        lineCount++; // Last line without newline
    }
    result.metadata["line_count"] = std::to_string(lineCount);

    // Special processing for specific file types
    if (ext == ".md") {
        result.metadata["format"] = "markdown";
        // Could extract headings, links, etc.
    } else if (ext == ".json") {
        result.metadata["format"] = "json";
        // Could validate JSON structure
    } else if (ext == ".xml" || ext == ".html" || ext == ".htm") {
        result.metadata["format"] = "xml";
        // Could extract tags, attributes
    } else if (ext == ".csv") {
        result.metadata["format"] = "csv";
        // Could detect delimiter, column count
    } else if (ext == ".cpp" || ext == ".h" || ext == ".hpp" || ext == ".py" || ext == ".js" ||
               ext == ".java") {
        result.metadata["format"] = "source_code";
        result.metadata["programming_language"] = ext.substr(1); // Remove dot
    }

    // Preserve original formatting for plain .txt by default; otherwise normalize
    // only when preserveFormatting is explicitly disabled.
    if (!config.preserveFormatting && ext != ".txt") {
        // Normalize whitespace
        std::string processed;
        processed.reserve(result.text.size());

        bool inSpace = false;
        for (unsigned char uc : result.text) {
            if (std::isspace(uc)) {
                if (!inSpace) {
                    processed += ' ';
                    inSpace = true;
                }
            } else {
                processed += static_cast<char>(uc);
                inSpace = false;
            }
        }

        // Trim
        if (!processed.empty() && processed.back() == ' ') {
            processed.pop_back();
        }

        result.text = std::move(processed);
    }
}

bool PlainTextExtractor::isBinaryFile(std::span<const std::byte> data) {
    // Check for null bytes or high concentration of non-printable characters
    size_t nonPrintable = 0;
    size_t checkSize = std::min(data.size(), size_t(8192));

    for (size_t i = 0; i < checkSize; ++i) {
        uint8_t byte = static_cast<uint8_t>(data[i]);

        // Null byte is strong indicator of binary
        if (byte == 0) {
            return true;
        }

        // Count non-printable characters (excluding common whitespace)
        if (byte < 32 && byte != '\t' && byte != '\n' && byte != '\r') {
            nonPrintable++;
        }
    }

    // If more than 30% non-printable, consider binary
    return (nonPrintable * 100 / checkSize) > 30;
}

void PlainTextExtractor::extractFileMetadata(ExtractionResult& result,
                                             const std::filesystem::path& path) {
    result.metadata["filename"] = path.filename().string();
    result.metadata["filepath"] = path.string();
    result.metadata["extension"] = path.extension().string();

    // File stats
    try {
        auto fileSize = std::filesystem::file_size(path);
        result.metadata["file_size"] = std::to_string(fileSize);

        // Use actual last write time
        auto ft = std::filesystem::last_write_time(path);
        auto sctp = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
            ft - decltype(ft)::clock::now() + std::chrono::system_clock::now());
        result.metadata["last_modified"] =
            std::to_string(std::chrono::system_clock::to_time_t(sctp));
    } catch (const std::exception& e) {
        result.warnings.push_back("Failed to extract file stats: " + std::string(e.what()));
    }
}

} // namespace yams::extraction
