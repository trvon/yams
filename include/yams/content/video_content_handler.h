#pragma once

#include <atomic>
#include <chrono>
#include <concepts>
#include <mutex>
#include <optional>
#include <span>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <yams/common/format.h>
#include <yams/content/content_handler.h>
#include <yams/detection/file_type_detector.h>

namespace yams::content {

/**
 * @brief C++20 concept for video metadata extractors
 */
template <typename T>
concept VideoMetadataExtractor = requires(T extractor, std::span<const std::byte> data) {
    {
        extractor.extractMetadata(data)
    } -> std::convertible_to<std::unordered_map<std::string, std::string>>;
    { extractor.canExtract(data) } -> std::convertible_to<bool>;
    { extractor.containerFormat() } -> std::convertible_to<std::string>;
};

/**
 * @brief C++20 concept for video analyzers
 */
template <typename T>
concept VideoAnalyzer = requires(T analyzer, std::span<const std::byte> data) {
    { analyzer.analyze(data) } -> std::convertible_to<VideoMetadata>;
    { analyzer.supportedContainer() } -> std::convertible_to<std::string>;
    { analyzer.maxAnalysisSize() } -> std::convertible_to<size_t>;
};

/**
 * @brief Video processing configuration
 */
struct VideoProcessingConfig {
    bool extractMetadata = true;                        ///< Extract video metadata
    bool extractThumbnail = false;                      ///< Generate thumbnail
    bool extractFrames = false;                         ///< Extract key frames
    bool analyzeStreams = true;                         ///< Analyze video/audio streams
    bool extractSubtitles = false;                      ///< Extract subtitle tracks
    bool detectScenes = false;                          ///< Detect scene changes
    size_t maxFileSize = 2ULL * 1024 * 1024 * 1024;     ///< 2GB max
    size_t maxAnalysisSize = 100 * 1024 * 1024;         ///< 100MB for full analysis
    size_t thumbnailTimestamp = 10;                     ///< Thumbnail at 10 seconds
    uint32_t maxFrameExtraction = 5;                    ///< Max frames to extract
    std::chrono::milliseconds processingTimeout{60000}; ///< 60s timeout
    std::chrono::milliseconds analysisTimeout{30000};   ///< 30s for deep analysis
    bool useAsyncProcessing = true;                     ///< Use background threads
    uint32_t maxConcurrentAnalysis = 1;                 ///< Max concurrent analysis threads
};

/**
 * @brief Video container format enumeration
 */
enum class VideoFormat {
    Unknown,
    MP4,
    AVI,
    MKV,
    MOV,
    WMV,
    FLV,
    WEBM,
    OGV,
    M4V,
    ASF,
    RM,
    RMVB,
    _3GP,
    TS,
    MTS,
    M2TS
};

/**
 * @brief Convert video format to string
 */
[[nodiscard]] constexpr std::string_view formatToString(VideoFormat format) noexcept {
    switch (format) {
        case VideoFormat::MP4:
            return "MP4";
        case VideoFormat::AVI:
            return "AVI";
        case VideoFormat::MKV:
            return "MKV";
        case VideoFormat::MOV:
            return "MOV";
        case VideoFormat::WMV:
            return "WMV";
        case VideoFormat::FLV:
            return "FLV";
        case VideoFormat::WEBM:
            return "WebM";
        case VideoFormat::OGV:
            return "OGV";
        case VideoFormat::M4V:
            return "M4V";
        case VideoFormat::ASF:
            return "ASF";
        case VideoFormat::RM:
            return "RealMedia";
        case VideoFormat::RMVB:
            return "RMVB";
        case VideoFormat::_3GP:
            return "3GP";
        case VideoFormat::TS:
            return "MPEG-TS";
        case VideoFormat::MTS:
            return "MTS";
        case VideoFormat::M2TS:
            return "M2TS";
        default:
            return "Unknown";
    }
}

/**
 * @brief Video codec enumeration
 */
enum class VideoCodec {
    Unknown,
    H264,
    H265_HEVC,
    VP8,
    VP9,
    AV1,
    MPEG2,
    MPEG4,
    DivX,
    XviD,
    WMV,
    Theora,
    ProRes,
    DNxHD
};

/**
 * @brief Convert video codec to string
 */
[[nodiscard]] constexpr std::string_view codecToString(VideoCodec codec) noexcept {
    switch (codec) {
        case VideoCodec::H264:
            return "H.264/AVC";
        case VideoCodec::H265_HEVC:
            return "H.265/HEVC";
        case VideoCodec::VP8:
            return "VP8";
        case VideoCodec::VP9:
            return "VP9";
        case VideoCodec::AV1:
            return "AV1";
        case VideoCodec::MPEG2:
            return "MPEG-2";
        case VideoCodec::MPEG4:
            return "MPEG-4";
        case VideoCodec::DivX:
            return "DivX";
        case VideoCodec::XviD:
            return "XviD";
        case VideoCodec::WMV:
            return "WMV";
        case VideoCodec::Theora:
            return "Theora";
        case VideoCodec::ProRes:
            return "ProRes";
        case VideoCodec::DNxHD:
            return "DNxHD";
        default:
            return "Unknown";
    }
}

/**
 * @brief Extended video metadata
 */
struct ExtendedVideoMetadata : public VideoMetadata {
    VideoFormat format = VideoFormat::Unknown;
    VideoCodec videoCodecEnum = VideoCodec::Unknown;
    std::optional<std::chrono::system_clock::time_point> creationDate;
    std::optional<std::string> title;
    std::optional<std::string> description;
    std::optional<std::string> author;
    std::optional<std::string> copyright;
    std::optional<std::string> genre;
    std::optional<std::string> comment;
    std::optional<std::string> encoder;
    std::optional<std::vector<std::byte>> thumbnail;
    std::vector<std::pair<double, std::vector<std::byte>>> keyFrames; ///< Timestamp + frame data
    std::vector<std::string> chapters;                                ///< Chapter titles
    std::unordered_map<std::string, std::string> customMetadata; ///< Container-specific metadata
    bool hasVideo = true;
    bool hasAudio = true;
    bool hasSubtitles = false;
    bool isLiveStream = false;
    bool isVariableFrameRate = false;
    float confidence = 1.0f;

    // C++20: Helper methods with concepts
    template <std::convertible_to<std::string> T>
    void setMetadata(const std::string& key, T&& value) {
        customMetadata[key] = std::forward<T>(value);
    }

    [[nodiscard]] bool hasBasicInfo() const noexcept {
        return width > 0 && height > 0 && durationSeconds > 0;
    }

    [[nodiscard]] bool hasThumbnail() const noexcept {
        return thumbnail.has_value() && !thumbnail->empty();
    }

    [[nodiscard]] std::string getDisplayName() const {
        if (title) {
            return *title;
        }
        if (author && title) {
            return yams::fmt_format("{} - {}", *author, *title);
        }
        return yams::fmt_format("{}x{} {}s", width, height, static_cast<int>(durationSeconds));
    }

    [[nodiscard]] double getAspectRatio() const noexcept {
        return height > 0 ? static_cast<double>(width) / height : 0.0;
    }

    [[nodiscard]] std::string getResolutionCategory() const noexcept {
        if (height >= 2160)
            return "4K";
        if (height >= 1440)
            return "1440p";
        if (height >= 1080)
            return "1080p";
        if (height >= 720)
            return "720p";
        if (height >= 480)
            return "480p";
        return "SD";
    }
};

/**
 * @brief Video analysis result for async processing
 */
struct VideoAnalysisResult {
    ExtendedVideoMetadata metadata;
    std::vector<std::string> warnings;
    std::vector<std::string> errors;
    std::chrono::milliseconds processingTime{0};
    bool isComplete = false;

    [[nodiscard]] bool hasErrors() const noexcept { return !errors.empty(); }
    [[nodiscard]] bool hasWarnings() const noexcept { return !warnings.empty(); }
};

/**
 * @brief Content handler for video files
 */
class VideoContentHandler : public IContentHandler {
public:
    VideoContentHandler();
    explicit VideoContentHandler(VideoProcessingConfig config);
    ~VideoContentHandler() override;

    // Disable copy and move (due to atomic members)
    VideoContentHandler(const VideoContentHandler&) = delete;
    VideoContentHandler& operator=(const VideoContentHandler&) = delete;
    VideoContentHandler(VideoContentHandler&&) = delete;
    VideoContentHandler& operator=(VideoContentHandler&&) = delete;

    /**
     * @brief Check if this handler can process the given file
     */
    [[nodiscard]] bool canHandle(const detection::FileSignature& signature) const override;

    /**
     * @brief Get handler priority
     */
    [[nodiscard]] int priority() const override { return 15; }

    /**
     * @brief Get handler name
     */
    [[nodiscard]] std::string name() const override { return "VideoContentHandler"; }

    /**
     * @brief Get supported MIME types
     */
    [[nodiscard]] std::vector<std::string> supportedMimeTypes() const override;

    /**
     * @brief Process video file and extract content/metadata
     */
    Result<ContentResult> process(const std::filesystem::path& path,
                                  const ContentConfig& config = {}) override;

    /**
     * @brief Process video from memory buffer
     */
    Result<ContentResult> processBuffer(std::span<const std::byte> data, const std::string& hint,
                                        const ContentConfig& config = {}) override;

    /**
     * @brief Validate video file before processing
     */
    Result<void> validate(const std::filesystem::path& path) const override;

    /**
     * @brief Get processing configuration
     */
    [[nodiscard]] const VideoProcessingConfig& getVideoConfig() const noexcept {
        return videoConfig_;
    }

    /**
     * @brief Update processing configuration
     */
    void setVideoConfig(VideoProcessingConfig config) noexcept { videoConfig_ = std::move(config); }

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
    };

    [[nodiscard]] ProcessingStats getProcessingStats() const;
    void resetStats();

private:
    // C++20: consteval for compile-time constants
    static consteval size_t maxVideoFileSize() noexcept {
        return 10ULL * 1024 * 1024 * 1024;
    } // 10GB
    static consteval size_t minVideoFileSize() noexcept { return 1024; } // 1KB
    static consteval std::chrono::milliseconds maxProcessingTime() noexcept {
        return std::chrono::milliseconds{300000}; // 5 minutes
    }

    /**
     * @brief Convert MIME type to VideoFormat enum
     */
    [[nodiscard]] VideoFormat getFormatFromMimeType(const std::string& mimeType) const noexcept;

    /**
     * @brief Extract video metadata using async processing
     */
    [[nodiscard]] std::optional<VideoAnalysisResult>
    extractVideoMetadataAsync(std::span<const std::byte> data, VideoFormat format,
                              const detection::FileSignature& signature) const;

    /**
     * @brief Extract basic video properties synchronously
     */
    [[nodiscard]] std::optional<ExtendedVideoMetadata>
    extractBasicMetadata(std::span<const std::byte> data, VideoFormat format,
                         const detection::FileSignature& signature) const;

    /**
     * @brief Extract metadata using C++20 concepts
     */
    template <VideoMetadataExtractor MetadataExtractor>
    [[nodiscard]] std::unordered_map<std::string, std::string>
    extractMetadata(std::span<const std::byte> data, MetadataExtractor&& extractor) const {
        if (!extractor.canExtract(data)) {
            return {};
        }
        return extractor.extractMetadata(data);
    }

    /**
     * @brief Extract thumbnail from video file
     */
    [[nodiscard]] std::optional<std::vector<std::byte>>
    extractThumbnail(std::span<const std::byte> data, VideoFormat format,
                     uint32_t timestampSeconds = 10) const;

    /**
     * @brief Extract key frames from video file
     */
    [[nodiscard]] std::vector<std::pair<double, std::vector<std::byte>>>
    extractKeyFrames(std::span<const std::byte> data, VideoFormat format,
                     uint32_t maxFrames = 5) const;

    /**
     * @brief Extract subtitle tracks from video file
     */
    [[nodiscard]] std::vector<std::string> extractSubtitles(std::span<const std::byte> data,
                                                            VideoFormat format) const;

    /**
     * @brief Analyze video streams for codec information
     */
    [[nodiscard]] std::pair<VideoCodec, std::string>
    analyzeVideoStream(std::span<const std::byte> data, VideoFormat format) const;

    /**
     * @brief Build ContentResult from analysis
     */
    [[nodiscard]] ContentResult buildContentResult(const ExtendedVideoMetadata& metadata,
                                                   std::optional<std::string> extractedText,
                                                   const detection::FileSignature& signature,
                                                   const std::filesystem::path& path = {}) const;

    /**
     * @brief Create error message using C++20 format
     */
    template <typename... Args>
    [[nodiscard]] std::string formatError(std::string_view operation,
                                          const std::filesystem::path& path,
#if defined(__cpp_lib_format) && (__cpp_lib_format >= 201907)
                                          std::format_string<Args...> fmt,
#else
                                          fmt::format_string<Args...> fmt,
#endif
                                          Args&&... args) const {
        auto details = yams::fmt_format(fmt, std::forward<Args>(args)...);
        return yams::fmt_format("Video processing failed: {} for '{}' - {}", operation,
                                path.string(), details);
    }

    /**
     * @brief Check if format supports metadata
     */
    [[nodiscard]] static constexpr bool formatSupportsMetadata(VideoFormat format) noexcept {
        return format != VideoFormat::Unknown;
    }

    /**
     * @brief Check if format is lossless
     */
    [[nodiscard]] static constexpr bool
    formatIsLossless([[maybe_unused]] VideoFormat format) noexcept {
        return false; // Most video formats are lossy
    }

    /**
     * @brief Update processing statistics atomically
     */
    void updateStats(bool success, std::chrono::milliseconds duration, size_t bytes) noexcept;

    // Member variables
    VideoProcessingConfig videoConfig_;

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
    mutable std::mutex statsMutex_;
    ProcessingStats stats_;
};

/**
 * @brief Factory function for creating VideoContentHandler
 */
[[nodiscard]] std::unique_ptr<VideoContentHandler>
createVideoHandler(VideoProcessingConfig config = {});

/**
 * @brief Helper function to check if file is a supported video format
 */
[[nodiscard]] bool isVideoFile(const std::filesystem::path& path);

/**
 * @brief Helper function to get video format from file extension
 */
[[nodiscard]] VideoFormat getVideoFormatFromExtension(std::string_view extension);

/**
 * @brief Helper function to get file extensions for video format
 */
[[nodiscard]] std::vector<std::string> getExtensionsForFormat(VideoFormat format);

/**
 * @brief Helper function to estimate video bitrate from file size and duration
 */
[[nodiscard]] std::optional<uint64_t> estimateBitrate(size_t fileSize, double durationSeconds);

/**
 * @brief Helper function to validate video metadata completeness
 */
[[nodiscard]] bool isMetadataComplete(const ExtendedVideoMetadata& metadata);

/**
 * @brief Helper function to get common video resolutions
 */
[[nodiscard]] std::vector<std::pair<uint32_t, uint32_t>> getCommonResolutions();

/**
 * @brief Helper function to determine if resolution is standard
 */
[[nodiscard]] bool isStandardResolution(uint32_t width, uint32_t height);

} // namespace yams::content
