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
#include <yams/content/archive_content_handler.h>

#ifdef YAMS_HAVE_LIBARCHIVE
#include <archive.h>
#include <archive_entry.h>
#endif

namespace yams::content {

namespace {

// C++20: consteval lookup tables for archive-specific mappings
consteval auto createArchiveMimeTypes() {
    return std::array{"application/zip",
                      "application/x-rar-compressed",
                      "application/x-tar",
                      "application/gzip",
                      "application/x-bzip2",
                      "application/x-xz",
                      "application/x-7z-compressed",
                      "application/x-lzh-compressed",
                      "application/x-arj",
                      "application/vnd.ms-cab-compressed",
                      "application/x-iso9660-image",
                      "application/x-apple-diskimage"};
}

consteval auto createArchiveExtensions() {
    return std::array{".zip", ".rar", ".tar", ".gz",   ".bz2", ".xz",  ".7z",
                      ".lzh", ".arj", ".cab", ".iso",  ".dmg", ".img", ".bin",
                      ".cue", ".nrg", ".tgz", ".tbz2", ".txz"};
}

constexpr auto archiveMimeTypes = createArchiveMimeTypes();
constexpr auto archiveExtensions = createArchiveExtensions();

// Basic ZIP header parser
struct ZIPLocalHeader {
    uint32_t signature; // 0x04034b50
    uint16_t version;
    uint16_t flags;
    uint16_t compression;
    uint16_t modTime;
    uint16_t modDate;
    uint32_t crc32;
    uint32_t compressedSize;
    uint32_t uncompressedSize;
    uint16_t filenameLength;
    uint16_t extraLength;
};

// Basic RAR header parser
struct RARHeader {
    char signature[7]; // "Rar!\x1a\x07\x00"
    uint8_t version;
};

// Basic TAR header parser (POSIX format)
struct TARHeader {
    char name[100];
    char mode[8];
    char uid[8];
    char gid[8];
    char size[12];
    char mtime[12];
    char checksum[8];
    char typeflag;
    char linkname[100];
    char magic[6]; // "ustar\0"
    char version[2];
    char uname[32];
    char gname[32];
    char devmajor[8];
    char devminor[8];
    char prefix[155];
};

// Extract basic ZIP metadata
std::optional<ArchiveMetadata> analyzeZIPHeader(const std::filesystem::path& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file)
        return std::nullopt;

    ZIPLocalHeader header;
    file.read(reinterpret_cast<char*>(&header), sizeof(header));

    if (header.signature != 0x04034b50) { // ZIP local file header signature
        return std::nullopt;
    }

    ArchiveMetadata metadata;
    metadata.format = static_cast<int>(ArchiveFormat::ZIP);
    metadata.compressionMethod = "Deflate";
    metadata.confidence = 0.9f;

    // Set basic file info
    const auto fileSize = std::filesystem::file_size(path);
    if (fileSize > 0) {
        metadata.compressedSize = fileSize;
        metadata.customMetadata["file_size"] = std::to_string(fileSize);
    }

    // Check if encrypted
    metadata.isEncrypted = (header.flags & 0x01) != 0;

    return metadata;
}

// Extract basic RAR metadata
std::optional<ArchiveMetadata> analyzeRARHeader(const std::filesystem::path& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file)
        return std::nullopt;

    RARHeader header;
    file.read(reinterpret_cast<char*>(&header), sizeof(header));

    if (std::strncmp(header.signature, "Rar!", 4) != 0) {
        return std::nullopt;
    }

    ArchiveMetadata metadata;
    metadata.format = static_cast<int>(ArchiveFormat::RAR);
    metadata.formatVersion = std::to_string(header.version);
    metadata.compressionMethod = "RAR";
    metadata.confidence = 0.9f;

    // Set basic file info
    const auto fileSize = std::filesystem::file_size(path);
    if (fileSize > 0) {
        metadata.compressedSize = fileSize;
        metadata.customMetadata["file_size"] = std::to_string(fileSize);
    }

    return metadata;
}

// Extract basic TAR metadata
std::optional<ArchiveMetadata> analyzeTARHeader(const std::filesystem::path& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file)
        return std::nullopt;

    TARHeader header;
    file.read(reinterpret_cast<char*>(&header), sizeof(header));

    if (std::strncmp(header.magic, "ustar", 5) != 0) {
        return std::nullopt;
    }

    ArchiveMetadata metadata;
    metadata.format = static_cast<int>(ArchiveFormat::TAR);
    metadata.compressionMethod = "None";
    metadata.confidence = 0.9f;

    // Set basic file info
    const auto fileSize = std::filesystem::file_size(path);
    if (fileSize > 0) {
        metadata.compressedSize = fileSize;
        metadata.uncompressedSize = fileSize; // TAR is uncompressed
        metadata.customMetadata["file_size"] = std::to_string(fileSize);
    }

    return metadata;
}

// Command-line fallback using system tools
std::optional<ArchiveMetadata> extractUsingLibArchive(const std::filesystem::path& path) {
    (void)path; // Suppress unused parameter warning when libarchive not available
#ifdef YAMS_HAVE_LIBARCHIVE
    struct archive* a;
    struct archive_entry* entry;
    int r;

    a = archive_read_new();
    archive_read_support_filter_all(a);
    archive_read_support_format_all(a);

    r = archive_read_open_filename(a, path.c_str(), 10240);
    if (r != ARCHIVE_OK) {
        archive_read_free(a);
        return std::nullopt;
    }

    ArchiveMetadata metadata;
    metadata.confidence = 0.8f;

    // Read entries
    while (archive_read_next_header(a, &entry) == ARCHIVE_OK) {
        ArchiveEntry archiveEntry;

        const char* pathname = archive_entry_pathname(entry);
        if (pathname) {
            archiveEntry.path = pathname;
            archiveEntry.name = std::filesystem::path(pathname).filename().string();
        }

        archiveEntry.compressedSize = archive_entry_size(entry);
        archiveEntry.uncompressedSize = archive_entry_size(entry);
        archiveEntry.isDirectory = archive_entry_filetype(entry) == AE_IFDIR;

        // Get modification time
        time_t mtime = archive_entry_mtime(entry);
        archiveEntry.modificationTime = std::chrono::system_clock::from_time_t(mtime);

        metadata.entries.push_back(std::move(archiveEntry));

        if (archiveEntry.isDirectory) {
            metadata.totalDirectories++;
        } else {
            metadata.totalFiles++;
        }

        metadata.compressedSize += archiveEntry.compressedSize;
        metadata.uncompressedSize += archiveEntry.uncompressedSize;

        archive_read_data_skip(a);
    }

    // Determine format
    const char* format_name = archive_format_name(a);
    if (format_name) {
        std::string formatStr(format_name);
        if (formatStr.find("ZIP") != std::string::npos) {
            metadata.format = static_cast<int>(ArchiveFormat::ZIP);
        } else if (formatStr.find("RAR") != std::string::npos) {
            metadata.format = static_cast<int>(ArchiveFormat::RAR);
        } else if (formatStr.find("TAR") != std::string::npos) {
            metadata.format = static_cast<int>(ArchiveFormat::TAR);
        } else if (formatStr.find("7-Zip") != std::string::npos) {
            metadata.format = static_cast<int>(ArchiveFormat::_7Z);
        }
    }

    archive_read_close(a);
    archive_read_free(a);

    return metadata;
#endif
    return std::nullopt;
}

// Simple command-line fallback
std::optional<ArchiveMetadata> extractUsingUnzip(const std::filesystem::path& path) {
    std::string cmd = yams::fmt_format("unzip -l \"{}\" 2>/dev/null", path.string());

    if (FILE* pipe = popen(cmd.c_str(), "r")) {
        char buffer[4096];
        std::string result;
        while (fgets(buffer, sizeof(buffer), pipe)) {
            result += buffer;
        }
        int status = pclose(pipe);

        if (status == 0) {
            ArchiveMetadata metadata;
            metadata.format = static_cast<int>(ArchiveFormat::ZIP);
            metadata.confidence = 0.6f;

            // Parse unzip -l output (simple line counting)
            std::regex fileRegex(R"(\\s*\\d+\\s+\\d{2}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}\\s+(.+))");
            std::sregex_iterator iter(result.begin(), result.end(), fileRegex);
            std::sregex_iterator end;

            for (; iter != end; ++iter) {
                const std::smatch& match = *iter;
                if (match.size() > 1) {
                    ArchiveEntry entry;
                    entry.name = match[1].str();
                    entry.path = entry.name;
                    entry.isDirectory = entry.name.back() == '/';

                    metadata.entries.push_back(std::move(entry));

                    if (entry.isDirectory) {
                        metadata.totalDirectories++;
                    } else {
                        metadata.totalFiles++;
                    }
                }
            }

            return metadata;
        }
    }

    return std::nullopt;
}

} // anonymous namespace

// ArchiveContentHandler Implementation
ArchiveContentHandler::ArchiveContentHandler() : ArchiveContentHandler(ArchiveProcessingConfig{}) {}

ArchiveContentHandler::ArchiveContentHandler(ArchiveProcessingConfig config)
    : archiveConfig_(std::move(config)) {
    spdlog::debug("ArchiveContentHandler initialized with config");
}

ArchiveContentHandler::~ArchiveContentHandler() {
    cancelProcessing();

    // Wait for processing threads to complete
    std::lock_guard lock(threadsMutex_);
    for (auto& thread : processingThreads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

std::vector<std::string> ArchiveContentHandler::supportedMimeTypes() const {
    return std::vector<std::string>(archiveMimeTypes.begin(), archiveMimeTypes.end());
}

bool ArchiveContentHandler::canHandle(const detection::FileSignature& signature) const {
    // Check MIME type
    const auto& mimeTypes = supportedMimeTypes();
    if (std::ranges::find(mimeTypes, signature.mimeType) != mimeTypes.end()) {
        return true;
    }

    // Check if it's an archive file type
    return signature.fileType == "archive";
}

Result<ContentResult> ArchiveContentHandler::process(const std::filesystem::path& path,
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

        // Extract archive metadata
        ArchiveMetadata archiveMeta{};

        // Try libarchive first (most comprehensive)
        if (auto metadata = extractUsingLibArchive(path)) {
            archiveMeta = *metadata;
        } else {
            // Fallback to native parsers
            const auto ext = path.extension().string();
            if (ext == ".zip") {
                if (auto zipMeta = analyzeZIPHeader(path)) {
                    archiveMeta = *zipMeta;
                }
            } else if (ext == ".rar") {
                if (auto rarMeta = analyzeRARHeader(path)) {
                    archiveMeta = *rarMeta;
                }
            } else if (ext == ".tar") {
                if (auto tarMeta = analyzeTARHeader(path)) {
                    archiveMeta = *tarMeta;
                }
            }

            // Try simple command-line tools as last resort
            if (archiveMeta.format == static_cast<int>(ArchiveFormat::Unknown) && ext == ".zip") {
                if (auto unzipMeta = extractUsingUnzip(path)) {
                    archiveMeta = *unzipMeta;
                }
            }
        }

        // Set basic metadata
        const auto fileSize = std::filesystem::file_size(path);
        result.metadata["file_size"] = std::to_string(fileSize);
        result.metadata["file_type"] = "archive";
        result.metadata["archive_format"] =
            std::string(formatToString(static_cast<ArchiveFormat>(archiveMeta.format)));
        result.metadata["total_files"] = std::to_string(archiveMeta.totalFiles);
        result.metadata["total_directories"] = std::to_string(archiveMeta.totalDirectories);
        result.metadata["compressed_size"] = std::to_string(archiveMeta.compressedSize);
        result.metadata["uncompressed_size"] = std::to_string(archiveMeta.uncompressedSize);
        result.metadata["compression_method"] = archiveMeta.compressionMethod;
        result.metadata["is_encrypted"] = archiveMeta.isEncrypted ? "true" : "false";

        if (archiveMeta.uncompressedSize > 0) {
            result.metadata["compression_ratio"] =
                std::to_string(archiveMeta.getCompressionRatio());
        }

        // Add custom metadata
        for (const auto& [key, value] : archiveMeta.customMetadata) {
            result.metadata[key] = value;
        }

        // Generate file list as text content if requested
        if (archiveConfig_.extractFileList && !archiveMeta.entries.empty()) {
            std::string fileListText =
                yams::fmt_format("Archive: {} ({})\n", path.filename().string(),
                                 formatToString(static_cast<ArchiveFormat>(archiveMeta.format)));
            fileListText += yams::fmt_format("Files: {}, Directories: {}\n\n",
                                             archiveMeta.totalFiles, archiveMeta.totalDirectories);

            for (const auto& entry : archiveMeta.entries) {
                fileListText +=
                    yams::fmt_format("{}{}\n", entry.path, entry.isDirectory ? "/" : "");
            }

            result.text = fileListText;
        }

        result.archiveData = archiveMeta;
        result.shouldIndex = archiveMeta.hasBasicInfo(); // Index if we found basic metadata

        // Processing time
        const auto processingTime = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - startTime);
        result.processingTimeMs = static_cast<size_t>(processingTime.count());

        // Update statistics
        updateStats(true, processingTime, fileSize, archiveMeta.entries.size());

        spdlog::debug("Processed archive file: {} ({} entries, {}ms)", path.filename().string(),
                      archiveMeta.entries.size(), processingTime.count());

        return result;

    } catch (const std::exception& e) {
        const auto processingTime = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - startTime);
        updateStats(false, processingTime, 0);

        return Error{ErrorCode::InternalError,
                     yams::fmt_format("Archive processing failed: {}", e.what())};
    }
}

Result<ContentResult> ArchiveContentHandler::processBuffer(std::span<const std::byte> data,
                                                           const std::string& hint,
                                                           const ContentConfig& config) {
    (void)data;
    (void)hint;
    (void)config;
    // For buffer processing, we'd need to use libarchive memory functions
    // This is a more complex implementation - for now return not implemented
    return Error{ErrorCode::NotImplemented, "Buffer processing not yet implemented for archives"};
}

Result<void> ArchiveContentHandler::validate(const std::filesystem::path& path) const {
    if (!std::filesystem::exists(path)) {
        return Error{ErrorCode::FileNotFound, "Archive file not found"};
    }

    if (!std::filesystem::is_regular_file(path)) {
        return Error{ErrorCode::InvalidData, "Not a regular file"};
    }

    const auto fileSize = std::filesystem::file_size(path);
    if (fileSize > archiveConfig_.maxFileSize) {
        return Error{
            ErrorCode::ResourceExhausted,
            yams::fmt_format("File too large: {} > {}", fileSize, archiveConfig_.maxFileSize)};
    }

    if (fileSize < minArchiveFileSize()) {
        return Error{ErrorCode::InvalidData, "File too small to be a valid archive"};
    }

    return {};
}

ArchiveFormat
ArchiveContentHandler::getFormatFromMimeType(const std::string& mimeType) const noexcept {
    if (mimeType == "application/zip")
        return ArchiveFormat::ZIP;
    if (mimeType == "application/x-rar-compressed")
        return ArchiveFormat::RAR;
    if (mimeType == "application/x-tar")
        return ArchiveFormat::TAR;
    if (mimeType == "application/gzip")
        return ArchiveFormat::GZIP;
    if (mimeType == "application/x-bzip2")
        return ArchiveFormat::BZIP2;
    if (mimeType == "application/x-xz")
        return ArchiveFormat::XZ;
    if (mimeType == "application/x-7z-compressed")
        return ArchiveFormat::_7Z;
    if (mimeType == "application/x-lzh-compressed")
        return ArchiveFormat::LZH;
    if (mimeType == "application/x-arj")
        return ArchiveFormat::ARJ;
    if (mimeType == "application/vnd.ms-cab-compressed")
        return ArchiveFormat::CAB;
    if (mimeType == "application/x-iso9660-image")
        return ArchiveFormat::ISO;
    if (mimeType == "application/x-apple-diskimage")
        return ArchiveFormat::DMG;
    return ArchiveFormat::Unknown;
}

ArchiveContentHandler::ProcessingStats ArchiveContentHandler::getProcessingStats() const {
    std::lock_guard lock(statsMutex_);
    return stats_;
}

void ArchiveContentHandler::resetStats() {
    std::lock_guard lock(statsMutex_);
    stats_ = ProcessingStats{};
}

void ArchiveContentHandler::updateStats(bool success, std::chrono::milliseconds duration,
                                        size_t bytes, size_t entries) noexcept {
    std::lock_guard lock(statsMutex_);
    stats_.totalFilesProcessed++;
    if (success) {
        stats_.successfulProcessing++;
    } else {
        stats_.failedProcessing++;
    }
    stats_.totalProcessingTime += duration;
    stats_.totalBytesProcessed += bytes;
    stats_.totalArchiveEntriesProcessed += entries;

    if (stats_.totalFilesProcessed > 0) {
        stats_.averageProcessingTime = stats_.totalProcessingTime / stats_.totalFilesProcessed;
    }
}

// Helper implementations
std::unique_ptr<ArchiveContentHandler> createArchiveHandler(ArchiveProcessingConfig config) {
    return std::make_unique<ArchiveContentHandler>(std::move(config));
}

bool isArchiveFile(const std::filesystem::path& path) {
    const std::string ext = path.extension().string();
    return std::ranges::find(archiveExtensions, ext) != archiveExtensions.end();
}

ArchiveFormat getArchiveFormatFromExtension(std::string_view extension) {
    if (extension == ".zip")
        return ArchiveFormat::ZIP;
    if (extension == ".rar")
        return ArchiveFormat::RAR;
    if (extension == ".tar")
        return ArchiveFormat::TAR;
    if (extension == ".gz" || extension == ".tgz")
        return ArchiveFormat::GZIP;
    if (extension == ".bz2" || extension == ".tbz2")
        return ArchiveFormat::BZIP2;
    if (extension == ".xz" || extension == ".txz")
        return ArchiveFormat::XZ;
    if (extension == ".7z")
        return ArchiveFormat::_7Z;
    if (extension == ".lzh")
        return ArchiveFormat::LZH;
    if (extension == ".arj")
        return ArchiveFormat::ARJ;
    if (extension == ".cab")
        return ArchiveFormat::CAB;
    if (extension == ".iso")
        return ArchiveFormat::ISO;
    if (extension == ".dmg")
        return ArchiveFormat::DMG;
    if (extension == ".img")
        return ArchiveFormat::IMG;
    if (extension == ".bin")
        return ArchiveFormat::BIN;
    if (extension == ".cue")
        return ArchiveFormat::CUE;
    if (extension == ".nrg")
        return ArchiveFormat::NRG;
    return ArchiveFormat::Unknown;
}

std::vector<std::string> getExtensionsForFormat(ArchiveFormat format) {
    switch (format) {
        case ArchiveFormat::ZIP:
            return {".zip"};
        case ArchiveFormat::RAR:
            return {".rar"};
        case ArchiveFormat::TAR:
            return {".tar"};
        case ArchiveFormat::GZIP:
            return {".gz", ".tgz"};
        case ArchiveFormat::BZIP2:
            return {".bz2", ".tbz2"};
        case ArchiveFormat::XZ:
            return {".xz", ".txz"};
        case ArchiveFormat::_7Z:
            return {".7z"};
        case ArchiveFormat::LZH:
            return {".lzh"};
        case ArchiveFormat::ARJ:
            return {".arj"};
        case ArchiveFormat::CAB:
            return {".cab"};
        case ArchiveFormat::ISO:
            return {".iso"};
        case ArchiveFormat::DMG:
            return {".dmg"};
        case ArchiveFormat::IMG:
            return {".img"};
        case ArchiveFormat::BIN:
            return {".bin"};
        case ArchiveFormat::CUE:
            return {".cue"};
        case ArchiveFormat::NRG:
            return {".nrg"};
        default:
            return {};
    }
}

std::optional<double> estimateCompressionRatio(size_t compressedSize, size_t uncompressedSize) {
    if (uncompressedSize == 0)
        return std::nullopt;
    return static_cast<double>(compressedSize) / uncompressedSize;
}

bool isMetadataComplete(const ArchiveMetadata& metadata) {
    return metadata.totalFiles > 0 && metadata.format != static_cast<int>(ArchiveFormat::Unknown);
}

std::vector<ArchiveFormat> getCommonFormats() {
    return {ArchiveFormat::ZIP, ArchiveFormat::RAR, ArchiveFormat::TAR, ArchiveFormat::GZIP,
            ArchiveFormat::_7Z};
}

bool isCommonFormat(ArchiveFormat format) {
    const auto commonFormats = getCommonFormats();
    return std::ranges::find(commonFormats, format) != commonFormats.end();
}

} // namespace yams::content
