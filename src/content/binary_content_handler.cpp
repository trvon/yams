#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <fstream>
#include <ranges>
#include <string>
#include <unordered_set>
#include <yams/content/binary_content_handler.h>
#include <yams/detection/file_type_detector.h>

namespace yams::content {

BinaryContentHandler::BinaryContentHandler(BinaryProcessingConfig config)
    : binaryConfig_{std::move(config)} {}

std::vector<std::string> BinaryContentHandler::supportedMimeTypes() const {
    return {"*/*", "application/octet-stream"};
}

Result<void> BinaryContentHandler::validate(const std::filesystem::path& path) const {
    // First call base validation
    if (auto baseResult = IContentHandler::validate(path); !baseResult) {
        return baseResult;
    }

    // Check file size
    try {
        const auto fileSize = std::filesystem::file_size(path);
        if (fileSize > maxAnalysisSize()) {
            return Error{ErrorCode::InvalidArgument,
                         std::format("Binary file too large: {} bytes (max: {})", fileSize,
                                     maxAnalysisSize())};
        }
    } catch (const std::filesystem::filesystem_error& e) {
        return Error{ErrorCode::DatabaseError, std::format("Filesystem error: {}", e.what())};
    }

    return Result<void>{};
}

Result<ContentResult> BinaryContentHandler::process(const std::filesystem::path& path,
                                                    const ContentConfig& config) {
    const auto startTime = std::chrono::steady_clock::now();

    // Validate input
    if (auto validationResult = validate(path); !validationResult) {
        ++failedProcessing_;
        return validationResult.error();
    }

    try {
        // Read file data
        std::ifstream file(path, std::ios::binary | std::ios::ate);
        if (!file) {
            ++failedProcessing_;
            return Error{ErrorCode::FileNotFound,
                         std::format("Cannot open file: {}", path.string())};
        }

        const auto size = file.tellg();
        if (size <= 0) {
            ++failedProcessing_;
            return Error{ErrorCode::InvalidArgument, "File is empty"};
        }

        std::vector<std::byte> buffer(static_cast<size_t>(size));
        file.seekg(0);
        file.read(reinterpret_cast<char*>(buffer.data()), size);

        if (!file) {
            ++failedProcessing_;
            return Error{ErrorCode::DatabaseError, "Failed to read file data"};
        }

        // Process the buffer
        auto result = processBuffer(buffer, path.extension().string(), config);

        // Update statistics
        const auto endTime = std::chrono::steady_clock::now();
        const auto duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        ++processedFiles_;
        totalBytesProcessed_ += buffer.size();
        if (result.has_value()) {
            ++successfulProcessing_;
        } else {
            ++failedProcessing_;
        }

        return result;

    } catch (const std::exception& e) {
        ++failedProcessing_;
        return Error{ErrorCode::DatabaseError, std::format("File processing error: {}", e.what())};
    }
}

Result<ContentResult> BinaryContentHandler::processBuffer(std::span<const std::byte> data,
                                                          const std::string& /*hint*/,
                                                          const ContentConfig& /*config*/) {
    if (data.empty()) {
        return Error{ErrorCode::InvalidArgument, "Empty binary data"};
    }

    // Use FileTypeDetector for format identification
    auto& detector = detection::FileTypeDetector::instance();
    auto signatureResult = detector.detectFromBuffer(data);

    detection::FileSignature signature;
    if (signatureResult) {
        signature = signatureResult.value();
    } else {
        // Create fallback signature
        signature.mimeType = "application/octet-stream";
        signature.fileType = "binary";
        signature.description = "Binary data";
        signature.isBinary = true;
        signature.confidence = 0.5f;
        signature.magicNumber = detection::extractMagicNumber(data);
    }

    // Perform binary analysis
    auto analysis = analyzeBinaryData(data, {});

    // Build and return result
    return buildContentResult(analysis, {});
}

BinaryAnalysisResult
BinaryContentHandler::analyzeBinaryData(std::span<const std::byte> data,
                                        const std::filesystem::path& /*path*/) const {
    BinaryAnalysisResult analysis;

    // Basic file type detection
    auto& detector = detection::FileTypeDetector::instance();
    auto signatureResult = detector.detectFromBuffer(data);

    if (signatureResult) {
        const auto& sig = signatureResult.value();
        analysis.fileType = sig.fileType;
        analysis.detectedMimeType = sig.mimeType;
    } else {
        analysis.fileType = "unknown";
        analysis.detectedMimeType = "application/octet-stream";
    }

    // Calculate hashes if requested
    if (binaryConfig_.calculateHash) {
        analysis.hashes["sha256"] = calculateHash(data);
    }

    // Extract strings if requested
    if (binaryConfig_.extractStrings) {
        analysis.extractedStrings =
            extractStrings(data, binaryConfig_.minStringLength, binaryConfig_.maxStringLength);
    }

    // Detect text possibility
    analysis.isPossiblyText = detectPossibleText(data);

    // Calculate entropy
    analysis.entropy = calculateEntropy(data);

    // Detect file characteristics
    auto [isExecutable, isCompressed, isEncrypted] = detectFileCharacteristics(data);
    analysis.isExecutable = isExecutable;
    analysis.isCompressed = isCompressed;
    analysis.isEncrypted = isEncrypted;

    return analysis;
}

std::string BinaryContentHandler::calculateHash(std::span<const std::byte> data) const {
    // Simple hash calculation - in production use proper crypto library
    std::hash<std::string_view> hasher;
    std::string_view view{reinterpret_cast<const char*>(data.data()), data.size()};
    auto hashValue = hasher(view);
    return std::format("{:016x}", hashValue);
}

std::vector<std::string> BinaryContentHandler::extractStrings(std::span<const std::byte> data,
                                                              size_t minLength,
                                                              size_t maxLength) const {
    std::vector<std::string> strings;
    std::string current;
    current.reserve(maxLength);

    // Limit search size for performance
    auto searchSize = std::min(data.size(), maxStringSearchSize());
    auto searchData = data.subspan(0, searchSize);

    for (auto byte : searchData) {
        char c = static_cast<char>(byte);

        if (isPrintableAscii(byte)) {
            current += c;
            if (current.length() >= maxLength) {
                if (current.length() >= minLength) {
                    strings.push_back(current);
                }
                current.clear();
            }
        } else {
            if (current.length() >= minLength) {
                strings.push_back(current);
            }
            current.clear();
        }
    }

    // Don't forget the last string
    if (current.length() >= minLength) {
        strings.push_back(current);
    }

    // Limit number of strings returned
    if (strings.size() > 1000) {
        strings.resize(1000);
    }

    return strings;
}

bool BinaryContentHandler::detectPossibleText(std::span<const std::byte> data) const noexcept {
    return looksLikeText(data);
}

size_t BinaryContentHandler::calculateEntropy(std::span<const std::byte> data) const noexcept {
    if (data.empty())
        return 0;

    // Count byte frequencies
    std::array<size_t, 256> frequencies{};
    for (auto byte : data) {
        frequencies[static_cast<uint8_t>(byte)]++;
    }

    // Calculate Shannon entropy
    double entropy = 0.0;
    const double dataSize = static_cast<double>(data.size());

    for (size_t freq : frequencies) {
        if (freq > 0) {
            double probability = freq / dataSize;
            entropy -= probability * std::log2(probability);
        }
    }

    // Convert to 0-100 scale
    return static_cast<size_t>((entropy / 8.0) * 100.0);
}

std::tuple<bool, bool, bool>
BinaryContentHandler::detectFileCharacteristics(std::span<const std::byte> data) const noexcept {
    if (data.size() < 4) {
        return {false, false, false};
    }

    bool isExecutable = false;
    bool isCompressed = false;
    bool isEncrypted = false;

    // Check for executable signatures
    if (data.size() >= 2) {
        // PE header
        if (data[0] == std::byte{0x4D} && data[1] == std::byte{0x5A}) {
            isExecutable = true;
        }
        // ELF header
        if (data.size() >= 4 && data[0] == std::byte{0x7F} && data[1] == std::byte{0x45} &&
            data[2] == std::byte{0x4C} && data[3] == std::byte{0x46}) {
            isExecutable = true;
        }
    }

    // Check for compression signatures
    if (data.size() >= 2) {
        // ZIP
        if (data[0] == std::byte{0x50} && data[1] == std::byte{0x4B}) {
            isCompressed = true;
        }
        // GZIP
        if (data[0] == std::byte{0x1F} && data[1] == std::byte{0x8B}) {
            isCompressed = true;
        }
    }

    // Simple encryption detection based on entropy
    size_t entropy = calculateEntropy(data);
    if (entropy > 85) {              // High entropy suggests encryption/compression
        isEncrypted = !isCompressed; // If not compressed, likely encrypted
    }

    return {isExecutable, isCompressed, isEncrypted};
}

std::unordered_map<std::string, std::string>
BinaryContentHandler::extractHeaderInfo(std::span<const std::byte> data,
                                        const detection::FileSignature& signature) const {
    std::unordered_map<std::string, std::string> headerInfo;

    headerInfo["magic_number"] = signature.magicNumber;
    headerInfo["mime_type"] = signature.mimeType;
    headerInfo["file_type"] = signature.fileType;
    headerInfo["confidence"] = std::to_string(signature.confidence);

    if (data.size() >= 16) {
        headerInfo["header_hex"] = detection::FileTypeDetector::bytesToHex(data.subspan(0, 16));
    }

    return headerInfo;
}

ContentResult
BinaryContentHandler::buildContentResult(const BinaryAnalysisResult& analysis,
                                         const std::filesystem::path& /*path*/) const {
    ContentResult result;

    // Set basic content info
    result.handlerName = name();
    result.contentType = analysis.detectedMimeType;
    result.fileFormat = analysis.fileType;
    result.shouldIndex = analysis.hasStrings(); // Index if it has extractable strings
    result.isComplete = true;

    // Build metadata
    result.metadata["file_type"] = analysis.fileType;
    result.metadata["mime_type"] = analysis.detectedMimeType;
    result.metadata["entropy"] = std::to_string(analysis.entropy);
    result.metadata["is_possibly_text"] = analysis.isPossiblyText ? "true" : "false";
    result.metadata["is_executable"] = analysis.isExecutable ? "true" : "false";
    result.metadata["is_compressed"] = analysis.isCompressed ? "true" : "false";
    result.metadata["is_encrypted"] = analysis.isEncrypted ? "true" : "false";

    // Add hashes
    for (const auto& [algorithm, hash] : analysis.hashes) {
        result.metadata["hash_" + algorithm] = hash;
    }

    // Add header info
    for (const auto& [key, value] : analysis.headerInfo) {
        result.metadata["header_" + key] = value;
    }

    // Add strings as text content if requested
    if (!analysis.extractedStrings.empty()) {
        std::string combinedStrings;
        for (const auto& str : analysis.extractedStrings) {
            if (!combinedStrings.empty()) {
                combinedStrings += "\n";
            }
            combinedStrings += str;
        }
        result.text = combinedStrings;
        result.metadata["extracted_strings_count"] =
            std::to_string(analysis.extractedStrings.size());
    }

    return result;
}

void BinaryContentHandler::extractFileMetadata(const std::filesystem::path& path,
                                               ContentResult& result) const {
    try {
        auto fileSize = std::filesystem::file_size(path);
        auto lastWrite = std::filesystem::last_write_time(path);

        result.metadata["file_size"] = std::to_string(fileSize);
        result.metadata["file_name"] = path.filename().string();
        result.metadata["file_extension"] = path.extension().string();

        // Convert file time to system time
        auto sctp = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
            lastWrite - std::filesystem::file_time_type::clock::now() +
            std::chrono::system_clock::now());
        result.metadata["last_modified"] = std::format("{:%Y-%m-%d %H:%M:%S}", sctp);

    } catch (const std::filesystem::filesystem_error& e) {
        spdlog::warn("Failed to extract file metadata for {}: {}", path.string(), e.what());
    }
}

BinaryContentHandler::ProcessingStats BinaryContentHandler::getStats() const noexcept {
    return ProcessingStats{.totalFilesProcessed = processedFiles_.load(),
                           .successfulProcessing = successfulProcessing_.load(),
                           .failedProcessing = failedProcessing_.load(),
                           .totalProcessingTime = totalProcessingTime_.load(),
                           .totalBytesProcessed = totalBytesProcessed_.load()};
}

// Factory function implementation
std::unique_ptr<BinaryContentHandler> createBinaryHandler(BinaryProcessingConfig config) {
    return std::make_unique<BinaryContentHandler>(std::move(config));
}

// Helper function implementations
bool isLikelyBinaryFile(const std::filesystem::path& path) {
    auto& detector = detection::FileTypeDetector::instance();
    auto result = detector.detectFromFile(path);

    if (result) {
        return result.value().isBinary;
    }

    // Fallback to extension-based detection
    return getBinaryFileExtensions().contains(path.extension().string());
}

std::unordered_set<std::string> getBinaryFileExtensions() {
    return {".exe",    ".dll", ".so",  ".dylib", ".bin", ".dat", ".db",
            ".sqlite", ".zip", ".tar", ".gz",    ".bz2", ".xz",  ".7z",
            ".rar",    ".iso", ".img", ".dmg",   ".pkg", ".deb", ".rpm"};
}

} // namespace yams::content