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
#include <vector>
#include <yams/content/content_handler.h>
#include <yams/detection/file_type_detector.h>

namespace yams::content {

/**
 * @brief concept for audio tag extractors
 */
template <typename T>
concept AudioTagExtractor = requires(T extractor, std::span<const std::byte> data) {
    {
        extractor.extractTags(data)
    } -> std::convertible_to<std::unordered_map<std::string, std::string>>;
    { extractor.canExtract(data) } -> std::convertible_to<bool>;
    { extractor.tagFormat() } -> std::convertible_to<std::string>;
};

/**
 * @brief concept for audio analyzers
 */
template <typename T>
concept AudioAnalyzer = requires(T analyzer, std::span<const std::byte> data) {
    { analyzer.analyze(data) } -> std::convertible_to<AudioMetadata>;
    { analyzer.supportedFormat() } -> std::convertible_to<std::string>;
    { analyzer.maxAnalysisSize() } -> std::convertible_to<size_t>;
};

/**
 * @brief Audio processing configuration
 */
struct AudioProcessingConfig {
    bool extractTags = true;                            ///< Extract ID3/metadata tags
    bool analyzeAudio = true;                           ///< Analyze audio properties
    bool extractWaveform = false;                       ///< Generate waveform data
    bool detectSilence = false;                         ///< Detect silent sections
    bool extractLyrics = false;                         ///< Extract embedded lyrics
    size_t maxFileSize = 500 * 1024 * 1024;             ///< 500MB max
    size_t maxAnalysisSize = 50 * 1024 * 1024;          ///< 50MB for full analysis
    std::chrono::milliseconds processingTimeout{30000}; ///< 30s timeout
    std::chrono::milliseconds analysisTimeout{10000};   ///< 10s for deep analysis
    bool useAsyncProcessing = true;                     ///< Use background threads
    uint32_t maxConcurrentAnalysis = 2;                 ///< Max concurrent analysis threads
};

/**
 * @brief Audio format enumeration
 */
enum class AudioFormat { Unknown, MP3, WAV, FLAC, OGG, AAC, MP4, M4A, WMA, AIFF, APE, OPUS, AMR };

/**
 * @brief Convert audio format to string
 */
[[nodiscard]] constexpr std::string_view formatToString(AudioFormat format) noexcept {
    switch (format) {
        case AudioFormat::MP3:
            return "MP3";
        case AudioFormat::WAV:
            return "WAV";
        case AudioFormat::FLAC:
            return "FLAC";
        case AudioFormat::OGG:
            return "OGG";
        case AudioFormat::AAC:
            return "AAC";
        case AudioFormat::MP4:
            return "MP4";
        case AudioFormat::M4A:
            return "M4A";
        case AudioFormat::WMA:
            return "WMA";
        case AudioFormat::AIFF:
            return "AIFF";
        case AudioFormat::APE:
            return "APE";
        case AudioFormat::OPUS:
            return "OPUS";
        case AudioFormat::AMR:
            return "AMR";
        default:
            return "Unknown";
    }
}

/**
 * @brief Extended audio metadata
 */
struct ExtendedAudioMetadata : public AudioMetadata {
    AudioFormat format = AudioFormat::Unknown;
    std::optional<std::chrono::system_clock::time_point> recordingDate;
    std::optional<std::string> album;
    std::optional<std::string> artist;
    std::optional<std::string> title;
    std::optional<std::string> genre;
    std::optional<std::string> year;
    std::optional<std::string> track;
    std::optional<std::string> albumArtist;
    std::optional<std::string> composer;
    std::optional<std::string> publisher;
    std::optional<std::string> copyright;
    std::optional<std::string> lyrics;
    std::optional<std::string> comment;
    std::optional<double> replayGain;
    std::optional<std::vector<std::byte>> albumArt;
    std::vector<std::pair<double, double>> silentSections; ///< Start/end times in seconds
    bool isLossless = false;
    bool hasVariableBitrate = false;
    float confidence = 1.0f;

    // C++20: Helper methods with concepts
    template <std::convertible_to<std::string> T> void setTag(const std::string& key, T&& value) {
        tags[key] = std::forward<T>(value);
    }

    [[nodiscard]] bool hasBasicInfo() const noexcept {
        return artist.has_value() && title.has_value();
    }

    [[nodiscard]] bool hasAlbumArt() const noexcept {
        return albumArt.has_value() && !albumArt->empty();
    }

    [[nodiscard]] std::string getDisplayName() const {
        if (artist && title) {
            return std::format("{} - {}", *artist, *title);
        }
        if (title) {
            return *title;
        }
        return "Unknown Track";
    }
};

/**
 * @brief Audio analysis result for async processing
 */
struct AudioAnalysisResult {
    ExtendedAudioMetadata metadata;
    std::vector<std::string> warnings;
    std::vector<std::string> errors;
    std::chrono::milliseconds processingTime{0};
    bool isComplete = false;

    [[nodiscard]] bool hasErrors() const noexcept { return !errors.empty(); }

    [[nodiscard]] bool hasWarnings() const noexcept { return !warnings.empty(); }
};

/**
 * @brief Content handler for audio files
 */
class AudioContentHandler : public IContentHandler {
public:
    AudioContentHandler();
    explicit AudioContentHandler(AudioProcessingConfig config);
    ~AudioContentHandler() override;

    // Disable copy and move (due to atomic members)
    AudioContentHandler(const AudioContentHandler&) = delete;
    AudioContentHandler& operator=(const AudioContentHandler&) = delete;
    AudioContentHandler(AudioContentHandler&&) = delete;
    AudioContentHandler& operator=(AudioContentHandler&&) = delete;

    /**
     * @brief Check if this handler can process the given file
     */
    [[nodiscard]] bool canHandle(const detection::FileSignature& signature) const override;

    /**
     * @brief Get handler priority
     */
    [[nodiscard]] int priority() const override { return 12; }

    /**
     * @brief Get handler name
     */
    [[nodiscard]] std::string name() const override { return "AudioContentHandler"; }

    /**
     * @brief Get supported MIME types
     */
    [[nodiscard]] std::vector<std::string> supportedMimeTypes() const override;

    /**
     * @brief Process audio file and extract content/metadata
     */
    Result<ContentResult> process(const std::filesystem::path& path,
                                  const ContentConfig& config = {}) override;

    /**
     * @brief Process audio from memory buffer
     */
    Result<ContentResult> processBuffer(std::span<const std::byte> data, const std::string& hint,
                                        const ContentConfig& config = {}) override;

    /**
     * @brief Validate audio file before processing
     */
    Result<void> validate(const std::filesystem::path& path) const override;

    /**
     * @brief Get processing configuration
     */
    [[nodiscard]] const AudioProcessingConfig& getAudioConfig() const noexcept {
        return audioConfig_;
    }

    /**
     * @brief Update processing configuration
     */
    void setAudioConfig(AudioProcessingConfig config) noexcept { audioConfig_ = std::move(config); }

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

    [[nodiscard]] ProcessingStats getStats() const noexcept;

private:
    // C++20: consteval for compile-time constants
    static consteval size_t maxAudioFileSize() noexcept { return 1024 * 1024 * 1024; } // 1GB
    static consteval size_t minAudioFileSize() noexcept { return 1024; }               // 1KB
    static consteval std::chrono::milliseconds maxProcessingTime() noexcept {
        return std::chrono::milliseconds{60000};
    }

    /**
     * @brief Convert MIME type to AudioFormat enum
     */
    [[nodiscard]] AudioFormat getFormatFromMimeType(const std::string& mimeType) const noexcept;

    /**
     * @brief Extract audio metadata using async processing
     */
    [[nodiscard]] std::optional<AudioAnalysisResult>
    extractAudioMetadataAsync(std::span<const std::byte> data, AudioFormat format,
                              const detection::FileSignature& signature) const;

    /**
     * @brief Extract basic audio properties synchronously
     */
    [[nodiscard]] std::optional<ExtendedAudioMetadata>
    extractBasicMetadata(std::span<const std::byte> data, AudioFormat format,
                         const detection::FileSignature& signature) const;

    /**
     * @brief Extract ID3/metadata tags using concepts
     */
    template <AudioTagExtractor TagExtractor>
    [[nodiscard]] std::unordered_map<std::string, std::string>
    extractTags(std::span<const std::byte> data, TagExtractor&& extractor) const {
        if (!extractor.canExtract(data)) {
            return {};
        }
        return extractor.extractTags(data);
    }

    /**
     * @brief Extract album art from audio file
     */
    [[nodiscard]] std::optional<std::vector<std::byte>>
    extractAlbumArt(std::span<const std::byte> data, AudioFormat format) const;

    /**
     * @brief Extract lyrics from audio file
     */
    [[nodiscard]] std::optional<std::string> extractLyrics(std::span<const std::byte> data,
                                                           AudioFormat format) const;

    /**
     * @brief Analyze audio waveform for silence detection
     */
    [[nodiscard]] std::vector<std::pair<double, double>>
    detectSilentSections(std::span<const std::byte> data, AudioFormat format) const;

    /**
     * @brief Build ContentResult from analysis
     */
    [[nodiscard]] ContentResult buildContentResult(const ExtendedAudioMetadata& metadata,
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
        return std::format("Audio processing failed: {} for '{}' - {}", operation, path.string(),
                           details);
    }

    /**
     * @brief Check if format supports metadata tags
     */
    [[nodiscard]] static constexpr bool formatSupportsMetadata(AudioFormat format) noexcept {
        return format != AudioFormat::WAV && format != AudioFormat::AIFF;
    }

    /**
     * @brief Check if format is lossless
     */
    [[nodiscard]] static constexpr bool formatIsLossless(AudioFormat format) noexcept {
        return format == AudioFormat::FLAC || format == AudioFormat::WAV ||
               format == AudioFormat::AIFF || format == AudioFormat::APE;
    }

    /**
     * @brief Update processing statistics atomically
     */
    void updateStats(bool success, std::chrono::milliseconds duration, size_t bytes) noexcept;

    // Member variables
    AudioProcessingConfig audioConfig_;

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
    mutable ProcessingStats stats_;
};

/**
 * @brief Factory function for creating AudioContentHandler
 */
[[nodiscard]] std::unique_ptr<AudioContentHandler>
createAudioHandler(AudioProcessingConfig config = {});

/**
 * @brief Helper function to check if file is a supported audio format
 */
[[nodiscard]] bool isAudioFile(const std::filesystem::path& path);

/**
 * @brief Helper function to get audio format from file extension
 */
[[nodiscard]] AudioFormat getAudioFormatFromExtension(std::string_view extension);

/**
 * @brief Helper function to get file extensions for audio format
 */
[[nodiscard]] std::vector<std::string> getExtensionsForFormat(AudioFormat format);

/**
 * @brief Helper function to estimate audio file duration from size and bitrate
 */
[[nodiscard]] std::optional<double> estimateDuration(size_t fileSize, uint32_t bitrate);

/**
 * @brief Helper function to validate audio metadata completeness
 */
[[nodiscard]] bool isMetadataComplete(const ExtendedAudioMetadata& metadata);

} // namespace yams::content