#pragma once

#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>
#include <yams/core/types.h>
#include <yams/detection/file_type_detector.h>

namespace yams::content {

// Forward declarations
struct ContentConfig;
struct ContentResult;
struct ImageMetadata;
struct AudioMetadata;
struct VideoMetadata;
struct DocumentMetadata;

/**
 * @brief Configuration for content processing
 */
struct ContentConfig {
    // General settings
    size_t maxFileSize = 100 * 1024 * 1024;   ///< 100MB default
    std::chrono::milliseconds timeout{30000}; ///< 30s timeout
    bool extractText = true;                  ///< Extract text content
    bool extractMetadata = true;              ///< Extract metadata
    bool generateThumbnail = false;           ///< Generate preview
    bool deepInspection = false;              ///< Thorough analysis

    // Text extraction settings (if applicable)
    bool preserveFormatting = false;
    bool detectLanguage = true;
    std::string preferredEncoding;

    // Media settings (if applicable)
    bool extractFrames = false;        ///< Extract video frames
    bool extractAudioWaveform = false; ///< Generate waveform
    int thumbnailSize = 256;           ///< Thumbnail dimension

    // Performance settings
    bool useCache = true;                      ///< Use cached results
    bool asyncProcessing = false;              ///< Process asynchronously
    size_t maxMemoryUsage = 512 * 1024 * 1024; ///< 512MB limit
};

/**
 * @brief Image-specific metadata
 */
struct ImageMetadata {
    uint32_t width = 0;
    uint32_t height = 0;
    uint32_t bitsPerPixel = 0;
    std::string colorSpace;
    std::string compression;
    std::unordered_map<std::string, std::string> exifData;
    std::vector<std::string> embeddedText; ///< OCR results
    bool hasAlpha = false;
    bool isAnimated = false;
};

/**
 * @brief Audio-specific metadata
 */
struct AudioMetadata {
    double durationSeconds = 0.0;
    uint32_t bitrate = 0;
    uint32_t sampleRate = 0;
    uint8_t channels = 0;
    std::string codec;
    std::string encoder;
    std::unordered_map<std::string, std::string> tags; ///< ID3, Vorbis, etc.
};

/**
 * @brief Video-specific metadata
 */
struct VideoMetadata {
    double durationSeconds = 0.0;
    uint32_t width = 0;
    uint32_t height = 0;
    double frameRate = 0.0;
    std::string videoCodec;
    std::string audioCodec;
    std::string container;
    uint64_t bitrate = 0;
    std::vector<std::string> subtitleLanguages;
    std::vector<std::string> audioLanguages;
};

/**
 * @brief Document-specific metadata (PDFs, Office docs, etc.)
 */
struct DocumentMetadata {
    std::string title;
    std::string author;
    std::string subject;
    std::string keywords;
    std::string creator;
    std::string producer;
    std::chrono::system_clock::time_point creationDate;
    std::chrono::system_clock::time_point modificationDate;
    size_t pageCount = 0;
    size_t wordCount = 0;
    bool isEncrypted = false;
    bool isSearchable = true;
};

/**
 * @brief Archive entry information
 */
struct ArchiveEntry {
    std::string path;
    std::string name;
    size_t compressedSize = 0;
    size_t uncompressedSize = 0;
    std::chrono::system_clock::time_point modificationTime;
    bool isDirectory = false;
    bool isEncrypted = false;
    std::string compressionMethod;
};

/**
 * @brief Archive-specific metadata (ZIP, RAR, TAR, etc.)
 */
struct ArchiveMetadata {
    int format = 0; // Will be cast from ArchiveFormat enum
    size_t totalFiles = 0;
    size_t totalDirectories = 0;
    size_t compressedSize = 0;
    size_t uncompressedSize = 0;
    std::vector<ArchiveEntry> entries;
    std::optional<std::chrono::system_clock::time_point> creationDate;
    std::optional<std::string> comment;
    std::optional<std::string> creator;
    bool isEncrypted = false;
    bool isSolid = false;
    bool hasRecoveryRecord = false;
    std::string compressionMethod;
    std::string formatVersion;
    std::unordered_map<std::string, std::string> customMetadata;
    float confidence = 1.0f;

    // Helper methods
    [[nodiscard]] bool hasBasicInfo() const noexcept { return totalFiles > 0; }

    [[nodiscard]] double getCompressionRatio() const noexcept {
        return uncompressedSize > 0 ? static_cast<double>(compressedSize) / uncompressedSize : 0.0;
    }

    [[nodiscard]] size_t getTotalSpaceSaved() const noexcept {
        return uncompressedSize > compressedSize ? uncompressedSize - compressedSize : 0;
    }

    [[nodiscard]] std::vector<std::string> getFileTypes() const {
        std::unordered_map<std::string, bool> extensionMap;
        for (const auto& entry : entries) {
            if (!entry.isDirectory) {
                auto pos = entry.name.find_last_of('.');
                if (pos != std::string::npos) {
                    extensionMap[entry.name.substr(pos)] = true;
                }
            }
        }
        std::vector<std::string> extensions;
        extensions.reserve(extensionMap.size());
        for (const auto& [ext, _] : extensionMap) {
            extensions.push_back(ext);
        }
        return extensions;
    }
};

/**
 * @brief Content chunk for indexing
 */
struct ContentChunk {
    std::string documentId;
    std::string chunkId;
    size_t chunkIndex = 0;
    size_t startOffset = 0;
    size_t endOffset = 0;
    std::string content;
    std::unordered_map<std::string, std::string> metadata;
};

/**
 * @brief Result from content processing
 */
struct ContentResult {
    // Core content
    std::optional<std::string> text;  ///< Extracted text (if any)
    std::vector<ContentChunk> chunks; ///< Text chunks for indexing

    // Metadata (key-value pairs)
    std::unordered_map<std::string, std::string> metadata;

    // Structured metadata
    std::optional<ImageMetadata> imageData;
    std::optional<AudioMetadata> audioData;
    std::optional<VideoMetadata> videoData;
    std::optional<DocumentMetadata> documentData;
    std::optional<ArchiveMetadata> archiveData;

    // Binary data
    std::vector<std::byte> thumbnail;     ///< Preview image
    std::vector<std::byte> extractedData; ///< Other extracted data

    // Processing info
    std::string handlerName;           ///< Handler that processed
    std::string contentType;           ///< MIME type
    std::string fileFormat;            ///< Specific format
    bool isComplete = true;            ///< Full extraction done
    bool shouldIndex = true;           ///< Create search index
    size_t processingTimeMs = 0;       ///< Processing duration
    std::vector<std::string> warnings; ///< Non-fatal issues

    // Helper methods
    [[nodiscard]] bool hasText() const { return text.has_value() && !text->empty(); }
    [[nodiscard]] bool hasMetadata() const { return !metadata.empty(); }
    [[nodiscard]] size_t getContentSize() const { return text ? text->size() : 0; }
    [[nodiscard]] bool isSuccess() const { return isComplete && warnings.empty(); }
};

/**
 * @brief File content handler interface
 *
 * Base interface for all content handlers. Each handler specializes
 * in processing specific file types and extracting relevant metadata.
 */
class IContentHandler {
public:
    virtual ~IContentHandler() = default;

    /**
     * @brief Check if this handler can process the given file
     * @param signature File type signature from FileTypeDetector
     * @return True if this handler supports the file type
     */
    [[nodiscard]] virtual bool canHandle(const detection::FileSignature& signature) const = 0;

    /**
     * @brief Get handler priority (higher = preferred)
     * @return Priority value, default 0
     */
    [[nodiscard]] virtual int priority() const { return 0; }

    /**
     * @brief Get handler name
     * @return Unique handler name
     */
    [[nodiscard]] virtual std::string name() const = 0;

    /**
     * @brief Get supported MIME types
     * @return Vector of MIME type patterns this handler supports
     */
    [[nodiscard]] virtual std::vector<std::string> supportedMimeTypes() const = 0;

    /**
     * @brief Process file and extract content/metadata
     * @param path File path to process
     * @param config Processing configuration
     * @return Processing result or error
     */
    virtual Result<ContentResult> process(const std::filesystem::path& path,
                                          const ContentConfig& config = {}) = 0;

    /**
     * @brief Process from memory buffer
     * @param data File data in memory
     * @param hint Optional file type hint
     * @param config Processing configuration
     * @return Processing result or error
     */
    virtual Result<ContentResult> processBuffer(std::span<const std::byte> /*data*/,
                                                const std::string& /*hint*/,
                                                const ContentConfig& /*config*/ = {}) {
        return Error{ErrorCode::NotImplemented, "Buffer processing not supported by " + name()};
    }

    /**
     * @brief Validate file before processing
     * @param path File path to validate
     * @return Success or validation error
     */
    virtual Result<void> validate(const std::filesystem::path& path) const {
        if (!std::filesystem::exists(path)) {
            return Error{ErrorCode::FileNotFound, "File not found: " + path.string()};
        }
        return Result<void>();
    }
};

} // namespace yams::content