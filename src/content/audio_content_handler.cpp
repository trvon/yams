#include <spdlog/spdlog.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <fstream>
#include <ranges>
#include <regex>
#include <string>
#include <unordered_set>
#include <yams/common/format.h>
#include <yams/content/audio_content_handler.h>

#ifdef YAMS_HAVE_TAGLIB
#include <taglib/attachedpictureframe.h>
#include <taglib/audioproperties.h>
#include <taglib/fileref.h>
#include <taglib/id3v2tag.h>
#include <taglib/tag.h>
#endif

namespace yams::content {

namespace {

// C++20: consteval lookup tables for audio-specific mappings
consteval auto createAudioMimeTypes() {
    return std::array{"audio/mpeg", "audio/mp3",    "audio/wav",      "audio/wave",   "audio/x-wav",
                      "audio/flac", "audio/x-flac", "audio/ogg",      "audio/vorbis", "audio/aac",
                      "audio/mp4",  "audio/m4a",    "audio/x-ms-wma", "audio/wma"};
}

consteval auto createAudioExtensions() {
    return std::array{".mp3", ".wav", ".flac", ".ogg", ".aac", ".m4a", ".wma"};
}

constexpr auto audioMimeTypes = createAudioMimeTypes();
constexpr auto audioExtensions = createAudioExtensions();

// Native ID3v1 parser for fallback
struct ID3v1Tag {
    char title[30];
    char artist[30];
    char album[30];
    char year[4];
    char comment[30];
    uint8_t genre;
};

// Native ID3v2 header parser
struct ID3v2Header {
    char identifier[3]; // "ID3"
    uint8_t version;
    uint8_t revision;
    uint8_t flags;
    uint32_t size;
};

// Extract ID3v1 tag from file end
std::optional<std::unordered_map<std::string, std::string>>
extractID3v1(const std::filesystem::path& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file)
        return std::nullopt;

    // Seek to 128 bytes before end
    file.seekg(-128, std::ios::end);

    ID3v1Tag tag{};
    file.read(reinterpret_cast<char*>(&tag), sizeof(tag));

    if (std::strncmp(tag.title, "TAG", 3) != 0) {
        return std::nullopt; // No ID3v1 tag
    }

    std::unordered_map<std::string, std::string> metadata;

    auto cleanString = [](const char* str, size_t maxLen) -> std::string {
        std::string result(str, std::min(strlen(str), maxLen));
        // Trim whitespace
        result.erase(result.find_last_not_of(" \t\r\n") + 1);
        return result;
    };

    if (auto title = cleanString(tag.title, 30); !title.empty()) {
        metadata["title"] = title;
    }
    if (auto artist = cleanString(tag.artist, 30); !artist.empty()) {
        metadata["artist"] = artist;
    }
    if (auto album = cleanString(tag.album, 30); !album.empty()) {
        metadata["album"] = album;
    }
    if (auto year = cleanString(tag.year, 4); !year.empty()) {
        metadata["year"] = year;
    }

    return metadata;
}

// Basic WAV header analysis for duration/properties
std::optional<AudioMetadata> analyzeWAVHeader(const std::filesystem::path& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file)
        return std::nullopt;

    char riff[4], wave[4];
    uint32_t fileSize, fmtSize;
    uint16_t audioFormat = 0, channels = 0, bitsPerSample = 0;
    uint32_t sampleRate = 0, byteRate = 0;

    file.read(riff, 4);
    file.read(reinterpret_cast<char*>(&fileSize), 4);
    file.read(wave, 4);

    if (std::strncmp(riff, "RIFF", 4) != 0 || std::strncmp(wave, "WAVE", 4) != 0) {
        return std::nullopt;
    }

    // Find fmt chunk
    char chunk[4];
    while (file.read(chunk, 4)) {
        file.read(reinterpret_cast<char*>(&fmtSize), 4);

        if (std::strncmp(chunk, "fmt ", 4) == 0) {
            file.read(reinterpret_cast<char*>(&audioFormat), 2);
            file.read(reinterpret_cast<char*>(&channels), 2);
            file.read(reinterpret_cast<char*>(&sampleRate), 4);
            file.read(reinterpret_cast<char*>(&byteRate), 4);
            file.seekg(2, std::ios::cur); // skip block align
            file.read(reinterpret_cast<char*>(&bitsPerSample), 2);
            break;
        } else {
            file.seekg(fmtSize, std::ios::cur);
        }
    }

    AudioMetadata metadata;
    metadata.sampleRate = sampleRate;
    metadata.channels = static_cast<uint8_t>(channels);
    metadata.codec = "PCM";

    // Calculate approximate duration
    if (sampleRate > 0 && channels > 0 && bitsPerSample > 0) {
        const auto dataSize = std::filesystem::file_size(path) - 44; // Approximate
        metadata.durationSeconds =
            static_cast<double>(dataSize) / (sampleRate * channels * bitsPerSample / 8);
        metadata.bitrate = static_cast<uint32_t>((dataSize * 8) / metadata.durationSeconds);
    }

    return metadata;
}

// Command-line fallback using system tools
std::optional<AudioMetadata> extractUsingFFProbe(const std::filesystem::path& path) {
#ifdef YAMS_HAVE_FFPROBE
    std::string cmd = yams::format("ffprobe -v quiet -show_format -show_streams -of csv=p=0 \"{}\"",
                                   path.string());

    if (FILE* pipe = popen(cmd.c_str(), "r")) {
        char buffer[4096];
        std::string result;
        while (fgets(buffer, sizeof(buffer), pipe)) {
            result += buffer;
        }
        pclose(pipe);

        // Parse ffprobe CSV output
        AudioMetadata metadata;
        std::regex durationRegex(R"(duration=([0-9.]+))");
        std::regex bitrateRegex(R"(bit_rate=([0-9]+))");
        std::regex sampleRateRegex(R"(sample_rate=([0-9]+))");
        std::regex channelsRegex(R"(channels=([0-9]+))");

        std::smatch match;
        if (std::regex_search(result, match, durationRegex)) {
            metadata.durationSeconds = std::stod(match[1].str());
        }
        if (std::regex_search(result, match, bitrateRegex)) {
            metadata.bitrate = std::stoul(match[1].str());
        }
        if (std::regex_search(result, match, sampleRateRegex)) {
            metadata.sampleRate = std::stoul(match[1].str());
        }
        if (std::regex_search(result, match, channelsRegex)) {
            metadata.channels = static_cast<uint8_t>(std::stoul(match[1].str()));
        }

        return metadata;
    }
#else
    (void)path; // suppress unused parameter warning when ffprobe is unavailable
#endif
    return std::nullopt;
}

} // anonymous namespace

// AudioContentHandler Implementation
AudioContentHandler::AudioContentHandler() : AudioContentHandler(AudioProcessingConfig{}) {}

AudioContentHandler::AudioContentHandler(AudioProcessingConfig config)
    : audioConfig_(std::move(config)) {
    spdlog::debug("AudioContentHandler initialized with config");
}

AudioContentHandler::~AudioContentHandler() {
    cancelProcessing();

    // Wait for processing threads to complete
    std::lock_guard lock(threadsMutex_);
    for (auto& thread : processingThreads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

std::vector<std::string> AudioContentHandler::supportedMimeTypes() const {
    return std::vector<std::string>(audioMimeTypes.begin(), audioMimeTypes.end());
}

bool AudioContentHandler::canHandle(const detection::FileSignature& signature) const {
    // Check MIME type
    const auto& mimeTypes = supportedMimeTypes();
    if (std::ranges::find(mimeTypes, signature.mimeType) != mimeTypes.end()) {
        return true;
    }

    // Check if it's an audio file type
    return signature.fileType == "audio";
}

Result<ContentResult> AudioContentHandler::process(const std::filesystem::path& path,
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

        // Extract audio metadata
        AudioMetadata audioMeta{};

#ifdef YAMS_HAVE_TAGLIB
        // Use TagLib if available (most comprehensive)
        TagLib::FileRef file(path.c_str());
        if (!file.isNull() && file.tag() && file.audioProperties()) {
            const auto* tag = file.tag();
            const auto* props = file.audioProperties();

            // Audio properties
            audioMeta.durationSeconds = props->lengthInSeconds();
            audioMeta.bitrate = props->bitrate() * 1000; // TagLib returns kbps
            audioMeta.sampleRate = props->sampleRate();
            audioMeta.channels = static_cast<uint8_t>(props->channels());

            // Tags
            if (!tag->title().isEmpty()) {
                audioMeta.tags["title"] = tag->title().to8Bit(true);
            }
            if (!tag->artist().isEmpty()) {
                audioMeta.tags["artist"] = tag->artist().to8Bit(true);
            }
            if (!tag->album().isEmpty()) {
                audioMeta.tags["album"] = tag->album().to8Bit(true);
            }
            if (tag->year() > 0) {
                audioMeta.tags["year"] = std::to_string(tag->year());
            }
            if (!tag->genre().isEmpty()) {
                audioMeta.tags["genre"] = tag->genre().to8Bit(true);
            }
            if (!tag->comment().isEmpty()) {
                audioMeta.tags["comment"] = tag->comment().to8Bit(true);
            }
        }
#else
        // Fallback implementations
        if (auto metadata = extractUsingFFProbe(path)) {
            audioMeta = *metadata;
        } else if (path.extension() == ".wav") {
            if (auto wavMeta = analyzeWAVHeader(path)) {
                audioMeta = *wavMeta;
            }
        }

        // Try ID3v1 for MP3 files
        if (path.extension() == ".mp3") {
            if (auto id3v1 = extractID3v1(path)) {
                audioMeta.tags = *id3v1;
            }
        }
#endif

        // Set basic metadata
        const auto fileSize = std::filesystem::file_size(path);
        result.metadata["file_size"] = std::to_string(fileSize);
        result.metadata["file_type"] = "audio";
        result.metadata["duration"] = std::to_string(audioMeta.durationSeconds);
        result.metadata["bitrate"] = std::to_string(audioMeta.bitrate);
        result.metadata["sample_rate"] = std::to_string(audioMeta.sampleRate);
        result.metadata["channels"] = std::to_string(audioMeta.channels);
        result.metadata["codec"] = audioMeta.codec;

        // Add tag metadata
        for (const auto& [key, value] : audioMeta.tags) {
            result.metadata[key] = value;
        }

        result.audioData = audioMeta;
        result.shouldIndex = !audioMeta.tags.empty(); // Index if we found metadata

        // Processing time
        const auto processingTime = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - startTime);
        result.processingTimeMs = static_cast<size_t>(processingTime.count());

        // Update statistics
        updateStats(true, processingTime, fileSize);

        spdlog::debug("Processed audio file: {} ({}ms)", path.filename().string(),
                      processingTime.count());

        return result;

    } catch (const std::exception& e) {
        const auto processingTime = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - startTime);
        updateStats(false, processingTime, 0);

        return Error{ErrorCode::InternalError,
                     yams::fmt_format("Audio processing failed: {}", e.what())};
    }
}

Result<ContentResult> AudioContentHandler::processBuffer(std::span<const std::byte> data,
                                                         const std::string& hint,
                                                         const ContentConfig& config) {
    (void)data;
    (void)hint;
    (void)config;
    // For buffer processing, we'd need to write to temp file or use memory-based parsers
    // This is a more complex implementation - for now return not implemented
    return Error{ErrorCode::NotImplemented, "Buffer processing not yet implemented for audio"};
}

Result<void> AudioContentHandler::validate(const std::filesystem::path& path) const {
    if (!std::filesystem::exists(path)) {
        return Error{ErrorCode::FileNotFound, "Audio file not found"};
    }

    if (!std::filesystem::is_regular_file(path)) {
        return Error{ErrorCode::InvalidData, "Not a regular file"};
    }

    const auto fileSize = std::filesystem::file_size(path);
    if (fileSize > audioConfig_.maxFileSize) {
        return Error{
            ErrorCode::ResourceExhausted,
            yams::fmt_format("File too large: {} > {}", fileSize, audioConfig_.maxFileSize)};
    }

    if (fileSize == 0) {
        return Error{ErrorCode::InvalidData, "Empty audio file"};
    }

    return {};
}

AudioContentHandler::ProcessingStats AudioContentHandler::getStats() const noexcept {
    std::lock_guard lock(statsMutex_);
    return stats_;
}

void AudioContentHandler::updateStats(bool success, std::chrono::milliseconds duration,
                                      size_t bytes) noexcept {
    std::lock_guard lock(statsMutex_);
    stats_.totalFilesProcessed++;
    if (success) {
        stats_.successfulProcessing++;
    } else {
        stats_.failedProcessing++;
    }
    stats_.totalProcessingTime += duration;
    stats_.totalBytesProcessed += bytes;

    if (stats_.totalFilesProcessed > 0) {
        stats_.averageProcessingTime = stats_.totalProcessingTime / stats_.totalFilesProcessed;
    }
}

// Helper implementations
std::unique_ptr<AudioContentHandler> createAudioHandler(AudioProcessingConfig config) {
    return std::make_unique<AudioContentHandler>(std::move(config));
}

bool isAudioFile(const std::filesystem::path& path) {
    const std::string ext = path.extension().string();
    return std::ranges::find(audioExtensions, ext) != audioExtensions.end();
}

AudioFormat getAudioFormatFromExtension(std::string_view extension) {
    if (extension == ".mp3")
        return AudioFormat::MP3;
    if (extension == ".wav")
        return AudioFormat::WAV;
    if (extension == ".flac")
        return AudioFormat::FLAC;
    if (extension == ".ogg")
        return AudioFormat::OGG;
    if (extension == ".aac")
        return AudioFormat::AAC;
    if (extension == ".mp4")
        return AudioFormat::MP4;
    if (extension == ".m4a")
        return AudioFormat::M4A;
    if (extension == ".wma")
        return AudioFormat::WMA;
    if (extension == ".aiff")
        return AudioFormat::AIFF;
    if (extension == ".ape")
        return AudioFormat::APE;
    if (extension == ".opus")
        return AudioFormat::OPUS;
    if (extension == ".amr")
        return AudioFormat::AMR;
    return AudioFormat::Unknown;
}

std::vector<std::string> getExtensionsForFormat(AudioFormat format) {
    switch (format) {
        case AudioFormat::MP3:
            return {".mp3"};
        case AudioFormat::WAV:
            return {".wav"};
        case AudioFormat::FLAC:
            return {".flac"};
        case AudioFormat::OGG:
            return {".ogg"};
        case AudioFormat::AAC:
            return {".aac"};
        case AudioFormat::MP4:
            return {".mp4"};
        case AudioFormat::M4A:
            return {".m4a"};
        case AudioFormat::WMA:
            return {".wma"};
        case AudioFormat::AIFF:
            return {".aiff"};
        case AudioFormat::APE:
            return {".ape"};
        case AudioFormat::OPUS:
            return {".opus"};
        case AudioFormat::AMR:
            return {".amr"};
        default:
            return {};
    }
}

std::optional<double> estimateDuration(size_t fileSize, uint32_t bitrate) {
    if (bitrate == 0)
        return std::nullopt;
    return static_cast<double>(fileSize * 8) / bitrate;
}

bool isMetadataComplete(const ExtendedAudioMetadata& metadata) {
    return metadata.hasBasicInfo() && metadata.durationSeconds > 0;
}

} // namespace yams::content
