#pragma once

#include <atomic>
#include <chrono>
#include <concepts>
#include <format>
#include <mutex>
#include <optional>
#include <span>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <yams/content/content_handler.h>
#include <yams/detection/file_type_detector.h>

namespace yams::content {

/**
 * @brief concept for archive extractors
 */
template <typename T>
concept ArchiveExtractor = requires(T extractor, std::span<const std::byte> data) {
    { extractor.extractFileList(data) } -> std::convertible_to<std::vector<std::string>>;
    { extractor.canExtract(data) } -> std::convertible_to<bool>;
    { extractor.archiveFormat() } -> std::convertible_to<std::string>;
};

/**
 * @brief concept for archive analyzers
 */
template <typename T>
concept ArchiveAnalyzer = requires(T analyzer, std::span<const std::byte> data) {
    { analyzer.analyze(data) } -> std::convertible_to<ArchiveMetadata>;
    { analyzer.supportedFormat() } -> std::convertible_to<std::string>;
    { analyzer.maxAnalysisSize() } -> std::convertible_to<size_t>;
};

/**
 * @brief Archive processing configuration
 */
struct ArchiveProcessingConfig {
    bool extractFileList = true;                        ///< Extract file listing
    bool extractMetadata = true;                        ///< Extract archive metadata
    bool analyzeContents = false;                       ///< Analyze contained files
    bool extractFiles = false;                          ///< Extract files to temp directory
    bool calculateHashes = false;                       ///< Calculate file hashes
    bool detectEncryption = true;                       ///< Detect encrypted archives
    size_t maxFileSize = 1ULL * 1024 * 1024 * 1024;     ///< 1GB max
    size_t maxAnalysisSize = 100 * 1024 * 1024;         ///< 100MB for full analysis
    size_t maxFileListSize = 10000;                     ///< Max files to list
    size_t maxExtractionSize = 50 * 1024 * 1024;        ///< Max total extraction size
    std::chrono::milliseconds processingTimeout{60000}; ///< 60s timeout
    std::chrono::milliseconds analysisTimeout{30000};   ///< 30s for deep analysis
    bool useAsyncProcessing = true;                     ///< Use background threads
    uint32_t maxConcurrentAnalysis = 1;                 ///< Max concurrent analysis threads
    std::string tempExtractionDir;                      ///< Temp directory for extraction
};

/**
 * @brief Archive format enumeration
 */
enum class ArchiveFormat {
    Unknown,
    ZIP,
    RAR,
    TAR,
    GZIP,
    BZIP2,
    XZ,
    _7Z,
    LZH,
    ARJ,
    CAB,
    ISO,
    DMG,
    IMG,
    BIN,
    CUE,
    NRG
};

/**
 * @brief Convert archive format to string
 */
[[nodiscard]] constexpr std::string_view formatToString(ArchiveFormat format) noexcept {
    switch (format) {
        case ArchiveFormat::ZIP:
            return "ZIP";
        case ArchiveFormat::RAR:
            return "RAR";
        case ArchiveFormat::TAR:
            return "TAR";
        case ArchiveFormat::GZIP:
            return "GZIP";
        case ArchiveFormat::BZIP2:
            return "BZIP2";
        case ArchiveFormat::XZ:
            return "XZ";
        case ArchiveFormat::_7Z:
            return "7Z";
        case ArchiveFormat::LZH:
            return "LZH";
        case ArchiveFormat::ARJ:
            return "ARJ";
        case ArchiveFormat::CAB:
            return "CAB";
        case ArchiveFormat::ISO:
            return "ISO";
        case ArchiveFormat::DMG:
            return "DMG";
        case ArchiveFormat::IMG:
            return "IMG";
        case ArchiveFormat::BIN:
            return "BIN";
        case ArchiveFormat::CUE:
            return "CUE";
        case ArchiveFormat::NRG:
            return "NRG";
        default:
            return "Unknown";
    }
}

/**
 * @brief Archive analysis result for async processing
 */
struct ArchiveAnalysisResult {
    ArchiveMetadata metadata;
    std::vector<std::string> warnings;
    std::vector<std::string> errors;
    std::chrono::milliseconds processingTime{0};
    bool isComplete = false;

    [[nodiscard]] bool hasErrors() const noexcept { return !errors.empty(); }
    [[nodiscard]] bool hasWarnings() const noexcept { return !warnings.empty(); }
};

/**
 * @brief Content handler for archive files
 */
class ArchiveContentHandler : public IContentHandler {
public:
    ArchiveContentHandler();
    explicit ArchiveContentHandler(ArchiveProcessingConfig config);
    ~ArchiveContentHandler() override;

    // Disable copy and move (due to atomic members)
    ArchiveContentHandler(const ArchiveContentHandler&) = delete;
    ArchiveContentHandler& operator=(const ArchiveContentHandler&) = delete;
    ArchiveContentHandler(ArchiveContentHandler&&) = delete;
    ArchiveContentHandler& operator=(ArchiveContentHandler&&) = delete;

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
    [[nodiscard]] std::string name() const override { return "ArchiveContentHandler"; }

    /**
     * @brief Get supported MIME types
     */
    [[nodiscard]] std::vector<std::string> supportedMimeTypes() const override;

    /**
     * @brief Process archive file and extract content/metadata
     */
    Result<ContentResult> process(const std::filesystem::path& path,
                                  const ContentConfig& config = {}) override;

    /**
     * @brief Process archive from memory buffer
     */
    Result<ContentResult> processBuffer(std::span<const std::byte> data, const std::string& hint,
                                        const ContentConfig& config = {}) override;

    /**
     * @brief Validate archive file before processing
     */
    Result<void> validate(const std::filesystem::path& path) const override;

    /**
     * @brief Get processing configuration
     */
    [[nodiscard]] const ArchiveProcessingConfig& getArchiveConfig() const noexcept {
        return archiveConfig_;
    }

    /**
     * @brief Update processing configuration
     */
    void setArchiveConfig(ArchiveProcessingConfig config) noexcept {
        archiveConfig_ = std::move(config);
    }

    /**
     * @brief Cancel all ongoing processing
     */
    void cancelProcessing() noexcept { stopProcessing_ = true; }

    /**
     * @brief Get processing statistics
     */
    struct ProcessingStats {
        size_t totalFilesProcessed = 0;
        size_t successfulProcessing = 0;
        size_t failedProcessing = 0;
        size_t asyncProcessingCount = 0;
        std::chrono::milliseconds totalProcessingTime{0};
        std::chrono::milliseconds averageProcessingTime{0};
        size_t totalBytesProcessed = 0;
        size_t totalArchiveEntriesProcessed = 0;

        [[nodiscard]] double successRate() const noexcept {
            return totalFilesProcessed > 0
                       ? static_cast<double>(successfulProcessing) / totalFilesProcessed
                       : 0.0;
        }

        [[nodiscard]] double averageProcessingTimeMs() const noexcept {
            return totalFilesProcessed > 0
                       ? static_cast<double>(totalProcessingTime.count()) / totalFilesProcessed
                       : 0.0;
        }

        [[nodiscard]] double averageEntriesPerArchive() const noexcept {
            return totalFilesProcessed > 0
                       ? static_cast<double>(totalArchiveEntriesProcessed) / totalFilesProcessed
                       : 0.0;
        }
    };

    [[nodiscard]] ProcessingStats getProcessingStats() const;
    void resetStats();

private:
    // C++20: consteval for compile-time constants
    static consteval size_t maxArchiveFileSize() noexcept {
        return 5ULL * 1024 * 1024 * 1024;
    }                                                                    // 5GB
    static consteval size_t minArchiveFileSize() noexcept { return 22; } // ZIP minimum
    static consteval std::chrono::milliseconds maxProcessingTime() noexcept {
        return std::chrono::milliseconds{600000}; // 10 minutes
    }

    /**
     * @brief Convert MIME type to ArchiveFormat enum
     */
    [[nodiscard]] ArchiveFormat getFormatFromMimeType(const std::string& mimeType) const noexcept;

    /**
     * @brief Extract archive metadata using async processing
     */
    [[nodiscard]] std::optional<ArchiveAnalysisResult>
    extractArchiveMetadataAsync(std::span<const std::byte> data, ArchiveFormat format,
                                const detection::FileSignature& signature) const;

    /**
     * @brief Extract basic archive properties synchronously
     */
    [[nodiscard]] std::optional<ArchiveMetadata>
    extractBasicMetadata(std::span<const std::byte> data, ArchiveFormat format,
                         const detection::FileSignature& signature) const;

    /**
     * @brief Extract file list using concepts
     */
    template <ArchiveExtractor Extractor>
    [[nodiscard]] std::vector<std::string> extractFileList(std::span<const std::byte> data,
                                                           Extractor&& extractor) const {
        if (!extractor.canExtract(data)) {
            return {};
        }
        return extractor.extractFileList(data);
    }

    /**
     * @brief Build ContentResult from analysis
     */
    [[nodiscard]] ContentResult buildContentResult(const ArchiveMetadata& metadata,
                                                   std::optional<std::string> extractedText,
                                                   const detection::FileSignature& signature,
                                                   const std::filesystem::path& path = {}) const;

    /**
     * @brief Create error message using format
     */
    template <typename... Args>
    [[nodiscard]] std::string formatError(std::string_view operation,
                                          const std::filesystem::path& path,
                                          std::format_string<Args...> fmt, Args&&... args) const {
        auto details = std::format(fmt, std::forward<Args>(args)...);
        return std::format("Archive processing failed: {} for '{}' - {}", operation, path.string(),
                           details);
    }

    /**
     * @brief Check if format supports metadata
     */
    [[nodiscard]] static constexpr bool formatSupportsMetadata(ArchiveFormat format) noexcept {
        return format != ArchiveFormat::Unknown;
    }

    /**
     * @brief Check if format supports encryption
     */
    [[nodiscard]] static constexpr bool formatSupportsEncryption(ArchiveFormat format) noexcept {
        return format == ArchiveFormat::ZIP || format == ArchiveFormat::RAR ||
               format == ArchiveFormat::_7Z;
    }

    /**
     * @brief Update processing statistics atomically
     */
    void updateStats(bool success, std::chrono::milliseconds duration, size_t bytes,
                     size_t entries = 0) noexcept;

    // Member variables
    ArchiveProcessingConfig archiveConfig_;

    // Thread management for async processing
    mutable std::atomic<bool> stopProcessing_{false};
    mutable std::vector<std::thread> processingThreads_;
    mutable std::mutex threadsMutex_;

    // Statistics (atomic for thread safety)
    mutable std::atomic<size_t> processedFiles_{0};
    mutable std::atomic<size_t> successfulProcessing_{0};
    mutable std::atomic<size_t> failedProcessing_{0};
    mutable std::atomic<size_t> asyncProcessingCount_{0};
    mutable std::atomic<std::chrono::milliseconds> totalProcessingTime_{
        std::chrono::milliseconds{0}};
    mutable std::atomic<size_t> totalBytesProcessed_{0};
    mutable std::atomic<size_t> totalArchiveEntriesProcessed_{0};
    mutable std::mutex statsMutex_;
    ProcessingStats stats_;
};

/**
 * @brief Factory function for creating ArchiveContentHandler
 */
[[nodiscard]] std::unique_ptr<ArchiveContentHandler>
createArchiveHandler(ArchiveProcessingConfig config = {});

/**
 * @brief Helper function to check if file is a supported archive format
 */
[[nodiscard]] bool isArchiveFile(const std::filesystem::path& path);

/**
 * @brief Helper function to get archive format from file extension
 */
[[nodiscard]] ArchiveFormat getArchiveFormatFromExtension(std::string_view extension);

/**
 * @brief Helper function to get file extensions for archive format
 */
[[nodiscard]] std::vector<std::string> getExtensionsForFormat(ArchiveFormat format);

/**
 * @brief Helper function to estimate compression ratio from file size
 */
[[nodiscard]] std::optional<double> estimateCompressionRatio(size_t compressedSize,
                                                             size_t uncompressedSize);

/**
 * @brief Helper function to validate archive metadata completeness
 */
[[nodiscard]] bool isMetadataComplete(const ArchiveMetadata& metadata);

/**
 * @brief Helper function to get common archive formats
 */
[[nodiscard]] std::vector<ArchiveFormat> getCommonFormats();

/**
 * @brief Helper function to determine if format is commonly used
 */
[[nodiscard]] bool isCommonFormat(ArchiveFormat format);

} // namespace yams::content
