#if __has_include(<spdlog/spdlog.h>)
#include <spdlog/spdlog.h>
#elif __has_include("spdlog/spdlog.h")
#include "spdlog/spdlog.h"
#else
namespace spdlog {
template <typename... Args> inline void debug(const char*, Args&&...) {}
template <typename... Args> inline void info(const char*, Args&&...) {}
template <typename... Args> inline void warn(const char*, Args&&...) {}
template <typename... Args> inline void error(const char*, Args&&...) {}
} // namespace spdlog
#endif
#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstring>
#include <fstream>
#include <random>
#include <regex>
#include <string>
#ifdef _MSC_VER
#include <cstdlib>
#endif
#if __has_include(<yams/content/video_content_handler.h>)
#include <yams/content/video_content_handler.h>
#elif __has_include("yams/content/video_content_handler.h")
#include "yams/content/video_content_handler.h"
#else
#include "../../include/yams/content/video_content_handler.h"
#endif
// Ensure formatting uses unified wrapper
#include <yams/common/format.h>

#include "buffer_processing_helpers.hpp"

namespace yams::content {

namespace {

// C++20: consteval lookup tables for video-specific mappings
consteval auto createVideoMimeTypes() {
    return std::array{"video/mp4",        "video/quicktime", "video/x-msvideo", "video/avi",
                      "video/x-ms-wmv",   "video/webm",      "video/ogg",       "video/x-flv",
                      "video/x-matroska", "video/3gpp",      "video/mp2t",      "video/x-ms-asf"};
}

consteval auto createVideoExtensions() {
    return std::array{".mp4", ".avi", ".mkv", ".mov",  ".wmv", ".flv", ".webm", ".ogv",
                      ".m4v", ".asf", ".rm",  ".rmvb", ".3gp", ".ts",  ".mts",  ".m2ts"};
}

constexpr auto videoMimeTypes = createVideoMimeTypes();
constexpr auto videoExtensions = createVideoExtensions();

constexpr uint32_t byteswap32(uint32_t value) noexcept {
    return ((value & 0x000000FFu) << 24) | ((value & 0x0000FF00u) << 8) |
           ((value & 0x00FF0000u) >> 8) | ((value & 0xFF000000u) >> 24);
}

static_assert(byteswap32(0x12345678u) == 0x78563412u,
              "byteswap32 must remain constexpr across toolchains");

// Basic MP4 box header parser
struct MP4BoxHeader {
    uint32_t size;
    char type[4];
};

// Basic AVI RIFF header parser
struct AVIHeader {
    char riff[4]; // "RIFF"
    uint32_t fileSize;
    char avi[4]; // "AVI "
};

// Extract basic MP4 metadata
std::optional<ExtendedVideoMetadata> analyzeMP4Header(const std::filesystem::path& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file)
        return std::nullopt;

    MP4BoxHeader header;
    bool foundFtyp = false;

    // Read file size for bitrate estimation
    const auto fileSize = std::filesystem::file_size(path);

    // Look for 'ftyp' box to confirm MP4 format
    while (file.read(reinterpret_cast<char*>(&header), sizeof(header))) {
        // Convert from big-endian
        header.size = byteswap32(header.size);

        if (std::strncmp(header.type, "ftyp", 4) == 0) {
            foundFtyp = true;
            break;
        }

        // Skip to next box
        if (header.size > sizeof(header)) {
            file.seekg(header.size - sizeof(header), std::ios::cur);
        } else {
            break;
        }
    }

    if (!foundFtyp) {
        return std::nullopt;
    }

    ExtendedVideoMetadata metadata;
    metadata.format = VideoFormat::MP4;
    metadata.container = "MP4";
    metadata.confidence = 0.8f;

    // Set basic file info
    if (fileSize > 0) {
        metadata.customMetadata["file_size"] = std::to_string(fileSize);
    }

    return metadata;
}

// Extract basic AVI metadata
std::optional<ExtendedVideoMetadata> analyzeAVIHeader(const std::filesystem::path& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file)
        return std::nullopt;

    AVIHeader header;
    file.read(reinterpret_cast<char*>(&header), sizeof(header));

    if (std::strncmp(header.riff, "RIFF", 4) != 0 || std::strncmp(header.avi, "AVI ", 4) != 0) {
        return std::nullopt;
    }

    ExtendedVideoMetadata metadata;
    metadata.format = VideoFormat::AVI;
    metadata.container = "AVI";
    metadata.confidence = 0.8f;

    // Set basic file info
    const auto fileSize = std::filesystem::file_size(path);
    if (fileSize > 0) {
        metadata.customMetadata["file_size"] = std::to_string(fileSize);
    }

    return metadata;
}

// Command-line fallback using system tools
std::optional<ExtendedVideoMetadata> extractUsingFFProbe(const std::filesystem::path& path) {
#ifdef YAMS_HAVE_FFPROBE
    std::string cmd = yams::fmt_format(
        "ffprobe -v quiet -show_format -show_streams -of json \"{}\"", path.string());

    if (FILE* pipe = popen(cmd.c_str(), "r")) {
        char buffer[4096];
        std::string result;
        while (fgets(buffer, sizeof(buffer), pipe)) {
            result += buffer;
        }
        pclose(pipe);

        // Parse ffprobe JSON output (simplified parsing)
        ExtendedVideoMetadata metadata;
        std::regex durationRegex(R"(\"duration\":\s*\"([0-9.]+)\")");
        std::regex widthRegex(R"(\"width\":\s*([0-9]+))");
        std::regex heightRegex(R"(\"height\":\s*([0-9]+))");
        std::regex codecRegex(R"(\"codec_name\":\s*\"([^\"]+)\")");
        std::regex bitrateRegex(R"(\"bit_rate\":\s*\"([0-9]+)\")");
        std::regex frameRateRegex(R"(\"r_frame_rate\":\s*\"([0-9]+)/([0-9]+)\")");

        std::smatch match;
        if (std::regex_search(result, match, durationRegex)) {
            metadata.durationSeconds = std::stod(match[1].str());
        }
        if (std::regex_search(result, match, widthRegex)) {
            metadata.width = std::stoul(match[1].str());
        }
        if (std::regex_search(result, match, heightRegex)) {
            metadata.height = std::stoul(match[1].str());
        }
        if (std::regex_search(result, match, codecRegex)) {
            metadata.videoCodec = match[1].str();
        }
        if (std::regex_search(result, match, bitrateRegex)) {
            metadata.bitrate = std::stoull(match[1].str());
        }
        if (std::regex_search(result, match, frameRateRegex)) {
            if (auto denominator = std::stoul(match[2].str()); denominator > 0) {
                metadata.frameRate = static_cast<double>(std::stoul(match[1].str())) / denominator;
            }
        }

        return metadata;
    }
#endif

#ifdef YAMS_HAVE_MEDIAINFO
    std::string mi_cmd = yams::fmt_format("mediainfo --Output=JSON \"{}\"", path.string());

    if (FILE* pipe = popen(mi_cmd.c_str(), "r")) {
        char buffer[4096];
        std::string result;
        while (fgets(buffer, sizeof(buffer), pipe)) {
            result += buffer;
        }
        pclose(pipe);

        // Parse MediaInfo JSON output (simplified)
        ExtendedVideoMetadata metadata;
        std::regex durationRegex(R"(\"Duration\":\s*\"([0-9.]+)\")");
        std::regex widthRegex(R"(\"Width\":\s*\"([0-9]+)\")");
        std::regex heightRegex(R"(\"Height\":\s*\"([0-9]+)\")");
        std::regex formatRegex(R"(\"Format\":\s*\"([^\"]+)\")");

        std::smatch match;
        if (std::regex_search(result, match, durationRegex)) {
            metadata.durationSeconds = std::stod(match[1].str()) / 1000.0; // MediaInfo in ms
        }
        if (std::regex_search(result, match, widthRegex)) {
            metadata.width = std::stoul(match[1].str());
        }
        if (std::regex_search(result, match, heightRegex)) {
            metadata.height = std::stoul(match[1].str());
        }
        if (std::regex_search(result, match, formatRegex)) {
            metadata.container = match[1].str();
        }

        return metadata;
    }
#endif

    return std::nullopt;
}

std::string extensionForVideoMime(std::string_view mimeType) {
    if (mimeType == "video/mp4")
        return ".mp4";
    if (mimeType == "video/quicktime")
        return ".mov";
    if (mimeType == "video/x-msvideo" || mimeType == "video/avi")
        return ".avi";
    if (mimeType == "video/x-ms-wmv")
        return ".wmv";
    if (mimeType == "video/webm")
        return ".webm";
    if (mimeType == "video/ogg")
        return ".ogv";
    if (mimeType == "video/x-flv")
        return ".flv";
    if (mimeType == "video/x-matroska")
        return ".mkv";
    if (mimeType == "video/3gpp")
        return ".3gp";
    if (mimeType == "video/mp2t")
        return ".ts";
    if (mimeType == "video/x-ms-asf")
        return ".asf";
    return {};
}

bool isGenericVideoDetection(const detection::FileSignature& signature) {
    return signature.fileType == "binary" ||
           signature.fileType.find("multimedia") != std::string::npos ||
           signature.mimeType == "application/octet-stream" ||
           signature.mimeType.find("various") != std::string::npos;
}

bool hasConfirmedVideoMetadata(const ContentResult& result) {
    if (!result.videoData) {
        return false;
    }
    const auto& metadata = *result.videoData;
    return metadata.durationSeconds > 0.0 || metadata.width > 0 || metadata.height > 0 ||
           metadata.frameRate > 0.0 || !metadata.videoCodec.empty() ||
           !metadata.audioCodec.empty() || !metadata.container.empty() || metadata.bitrate > 0;
}

bool hasVideoContainerMagic(std::span<const std::byte> data) {
    const auto* bytes = reinterpret_cast<const unsigned char*>(data.data());
    return (data.size() >= 12 && std::memcmp(bytes + 4, "ftyp", 4) == 0) ||
           (data.size() >= 12 && std::memcmp(bytes, "RIFF", 4) == 0 &&
            std::memcmp(bytes + 8, "AVI ", 4) == 0) ||
           (data.size() >= 4 && std::memcmp(bytes, "\x1A\x45\xDF\xA3", 4) == 0);
}

std::string chooseVideoTempExtension(std::span<const std::byte> data, std::string_view hint) {
    return detail::chooseTempExtension(data, hint, ".mp4", videoExtensions, extensionForVideoMime);
}

} // anonymous namespace

// VideoContentHandler Implementation
VideoContentHandler::VideoContentHandler() : VideoContentHandler(VideoProcessingConfig{}) {}

VideoContentHandler::VideoContentHandler(VideoProcessingConfig config)
    : videoConfig_(std::move(config)) {
    spdlog::debug("VideoContentHandler initialized with config");
}

VideoContentHandler::~VideoContentHandler() {
    detail::cancelAndJoinProcessing([this] { cancelProcessing(); }, threadsMutex_,
                                    processingThreads_);
}

std::vector<std::string> VideoContentHandler::supportedMimeTypes() const {
    return std::vector<std::string>(videoMimeTypes.begin(), videoMimeTypes.end());
}

bool VideoContentHandler::canHandle(const detection::FileSignature& signature) const {
    return detail::canHandleMediaSignature(signature, supportedMimeTypes(), "video");
}

Result<ContentResult> VideoContentHandler::process(const std::filesystem::path& path,
                                                   const ContentConfig& config) {
    (void)config;
    const auto startTime = std::chrono::steady_clock::now();

    try {
        // Validate file
        if (auto validationResult = validate(path); !validationResult) {
            return validationResult.error();
        }

        ContentResult result;
        result.handlerName = name();

        // Extract video metadata
        ExtendedVideoMetadata videoMeta{};

        // Try external tools first (most comprehensive)
        if (auto metadata = extractUsingFFProbe(path)) {
            videoMeta = *metadata;
        } else {
            // Fallback to native parsers
            const auto ext = path.extension().string();
            if (ext == ".mp4" || ext == ".m4v") {
                if (auto mp4Meta = analyzeMP4Header(path)) {
                    videoMeta = *mp4Meta;
                }
            } else if (ext == ".avi") {
                if (auto aviMeta = analyzeAVIHeader(path)) {
                    videoMeta = *aviMeta;
                }
            }
        }

        // Set basic metadata
        const auto fileSize = std::filesystem::file_size(path);
        result.metadata["file_size"] = std::to_string(fileSize);
        result.metadata["file_type"] = "video";
        result.metadata["duration"] = std::to_string(videoMeta.durationSeconds);
        result.metadata["width"] = std::to_string(videoMeta.width);
        result.metadata["height"] = std::to_string(videoMeta.height);
        result.metadata["frame_rate"] = std::to_string(videoMeta.frameRate);
        result.metadata["bitrate"] = std::to_string(videoMeta.bitrate);
        result.metadata["video_codec"] = videoMeta.videoCodec;
        result.metadata["audio_codec"] = videoMeta.audioCodec;
        result.metadata["container"] = videoMeta.container;

        // Add custom metadata
        for (const auto& [key, value] : videoMeta.customMetadata) {
            result.metadata[key] = value;
        }

        result.videoData = videoMeta;
        result.shouldIndex = videoMeta.hasBasicInfo(); // Index if we found basic metadata

        // Processing time
        const auto processingTime = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - startTime);
        result.processingTimeMs = static_cast<size_t>(processingTime.count());

        // Update statistics
        updateStats(true, processingTime, fileSize);

        spdlog::debug("Processed video file: {} ({}ms)", path.filename().string(),
                      processingTime.count());

        return result;

    } catch (const std::exception& e) {
        const auto processingTime = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - startTime);
        updateStats(false, processingTime, 0);

        return Error{ErrorCode::InternalError,
                     yams::fmt_format("Video processing failed: {}", e.what())};
    }
}

Result<ContentResult> VideoContentHandler::processBuffer(std::span<const std::byte> data,
                                                         const std::string& hint,
                                                         const ContentConfig& config) {
    return detail::processBufferWithTempFile(
        data, hint, config, "video",
        [this](const detection::FileSignature& signature) { return canHandle(signature); },
        isGenericVideoDetection, chooseVideoTempExtension,
        [this](const std::filesystem::path& path, const ContentConfig& processConfig) {
            return process(path, processConfig);
        },
        hasConfirmedVideoMetadata, hasVideoContainerMagic);
}

Result<void> VideoContentHandler::validate(const std::filesystem::path& path) const {
    return detail::validateMediaFile(path, videoConfig_.maxFileSize, 1, "Video file not found",
                                     "Empty video file");
}

VideoFormat VideoContentHandler::getFormatFromMimeType(const std::string& mimeType) const noexcept {
    if (mimeType == "video/mp4")
        return VideoFormat::MP4;
    if (mimeType == "video/quicktime")
        return VideoFormat::MOV;
    if (mimeType == "video/x-msvideo" || mimeType == "video/avi")
        return VideoFormat::AVI;
    if (mimeType == "video/x-ms-wmv")
        return VideoFormat::WMV;
    if (mimeType == "video/webm")
        return VideoFormat::WEBM;
    if (mimeType == "video/ogg")
        return VideoFormat::OGV;
    if (mimeType == "video/x-flv")
        return VideoFormat::FLV;
    if (mimeType == "video/x-matroska")
        return VideoFormat::MKV;
    if (mimeType == "video/3gpp")
        return VideoFormat::_3GP;
    if (mimeType == "video/mp2t")
        return VideoFormat::TS;
    return VideoFormat::Unknown;
}

VideoContentHandler::ProcessingStats VideoContentHandler::getProcessingStats() const {
    std::lock_guard lock(statsMutex_);
    return stats_;
}

void VideoContentHandler::resetStats() {
    std::lock_guard lock(statsMutex_);
    stats_ = ProcessingStats{};
}

void VideoContentHandler::updateStats(bool success, std::chrono::milliseconds duration,
                                      size_t bytes) noexcept {
    detail::updateProcessingStats(statsMutex_, stats_, success, duration, bytes);
}

// Helper implementations
std::unique_ptr<VideoContentHandler> createVideoHandler(VideoProcessingConfig config) {
    return std::make_unique<VideoContentHandler>(std::move(config));
}

bool isVideoFile(const std::filesystem::path& path) {
    const std::string ext = path.extension().string();
    return std::ranges::find(videoExtensions, ext) != videoExtensions.end();
}

VideoFormat getVideoFormatFromExtension(std::string_view extension) {
    if (extension == ".mp4" || extension == ".m4v")
        return VideoFormat::MP4;
    if (extension == ".avi")
        return VideoFormat::AVI;
    if (extension == ".mkv")
        return VideoFormat::MKV;
    if (extension == ".mov")
        return VideoFormat::MOV;
    if (extension == ".wmv")
        return VideoFormat::WMV;
    if (extension == ".flv")
        return VideoFormat::FLV;
    if (extension == ".webm")
        return VideoFormat::WEBM;
    if (extension == ".ogv")
        return VideoFormat::OGV;
    if (extension == ".asf")
        return VideoFormat::ASF;
    if (extension == ".rm")
        return VideoFormat::RM;
    if (extension == ".rmvb")
        return VideoFormat::RMVB;
    if (extension == ".3gp")
        return VideoFormat::_3GP;
    if (extension == ".ts")
        return VideoFormat::TS;
    if (extension == ".mts")
        return VideoFormat::MTS;
    if (extension == ".m2ts")
        return VideoFormat::M2TS;
    return VideoFormat::Unknown;
}

std::vector<std::string> getExtensionsForFormat(VideoFormat format) {
    switch (format) {
        case VideoFormat::MP4:
            return {".mp4", ".m4v"};
        case VideoFormat::AVI:
            return {".avi"};
        case VideoFormat::MKV:
            return {".mkv"};
        case VideoFormat::MOV:
            return {".mov"};
        case VideoFormat::WMV:
            return {".wmv"};
        case VideoFormat::FLV:
            return {".flv"};
        case VideoFormat::WEBM:
            return {".webm"};
        case VideoFormat::OGV:
            return {".ogv"};
        case VideoFormat::ASF:
            return {".asf"};
        case VideoFormat::RM:
            return {".rm"};
        case VideoFormat::RMVB:
            return {".rmvb"};
        case VideoFormat::_3GP:
            return {".3gp"};
        case VideoFormat::TS:
            return {".ts"};
        case VideoFormat::MTS:
            return {".mts"};
        case VideoFormat::M2TS:
            return {".m2ts"};
        default:
            return {};
    }
}

std::optional<uint64_t> estimateBitrate(size_t fileSize, double durationSeconds) {
    if (durationSeconds <= 0)
        return std::nullopt;
    return static_cast<uint64_t>((fileSize * 8) / durationSeconds);
}

bool isMetadataComplete(const ExtendedVideoMetadata& metadata) {
    return metadata.width > 0 && metadata.height > 0 && metadata.durationSeconds > 0 &&
           !metadata.container.empty();
}

std::vector<std::pair<uint32_t, uint32_t>> getCommonResolutions() {
    return {
        {720, 480},   // SD
        {1280, 720},  // 720p
        {1920, 1080}, // 1080p
        {2560, 1440}, // 1440p
        {3840, 2160}  // 4K
    };
}

bool isStandardResolution(uint32_t width, uint32_t height) {
    const auto resolutions = getCommonResolutions();
    return std::ranges::any_of(resolutions, [width, height](const auto& res) {
        return res.first == width && res.second == height;
    });
}

} // namespace yams::content
