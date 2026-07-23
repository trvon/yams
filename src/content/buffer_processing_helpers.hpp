#pragma once

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <yams/common/format.h>
#include <yams/content/content_handler.h>
#include <yams/core/types.h>
#include <yams/detection/file_type_detector.h>

namespace yams::content::detail {

template <typename CancelProcessing, typename ThreadRange>
void cancelAndJoinProcessing(CancelProcessing cancelProcessing, std::mutex& threadsMutex,
                             ThreadRange& processingThreads) {
    cancelProcessing();
    std::lock_guard lock(threadsMutex);
    for (auto& thread : processingThreads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

inline bool canHandleMediaSignature(const detection::FileSignature& signature,
                                    const std::vector<std::string>& mimeTypes,
                                    std::string_view fileType) {
    return std::ranges::find(mimeTypes, signature.mimeType) != mimeTypes.end() ||
           signature.fileType == fileType;
}

inline Result<void> validateMediaFile(const std::filesystem::path& path, size_t maxFileSize,
                                      size_t minFileSize, std::string_view missingMessage,
                                      std::string_view tooSmallMessage) {
    if (!std::filesystem::exists(path)) {
        return Error{ErrorCode::FileNotFound, std::string(missingMessage)};
    }

    if (!std::filesystem::is_regular_file(path)) {
        return Error{ErrorCode::InvalidData, "Not a regular file"};
    }

    const auto fileSize = std::filesystem::file_size(path);
    if (fileSize > maxFileSize) {
        return Error{ErrorCode::ResourceExhausted,
                     yams::fmt_format("File too large: {} > {}", fileSize, maxFileSize)};
    }

    if (fileSize < minFileSize) {
        return Error{ErrorCode::InvalidData, std::string(tooSmallMessage)};
    }

    return {};
}

template <typename Stats, typename ExtraUpdate>
void updateProcessingStats(std::mutex& statsMutex, Stats& stats, bool success,
                           std::chrono::milliseconds duration, size_t bytes,
                           ExtraUpdate extraUpdate) noexcept {
    std::lock_guard lock(statsMutex);
    stats.totalFilesProcessed++;
    if (success) {
        stats.successfulProcessing++;
    } else {
        stats.failedProcessing++;
    }
    stats.totalProcessingTime += duration;
    stats.totalBytesProcessed += bytes;
    extraUpdate(stats);

    if (stats.totalFilesProcessed > 0) {
        stats.averageProcessingTime = stats.totalProcessingTime / stats.totalFilesProcessed;
    }
}

template <typename Stats>
void updateProcessingStats(std::mutex& statsMutex, Stats& stats, bool success,
                           std::chrono::milliseconds duration, size_t bytes) noexcept {
    updateProcessingStats(statsMutex, stats, success, duration, bytes, [](Stats&) noexcept {});
}

template <typename ExtensionRange>
std::string normalizeExtensionHint(std::string_view hint,
                                   const ExtensionRange& supportedExtensions) {
    if (hint.empty()) {
        return {};
    }

    std::filesystem::path hintPath{std::string(hint)};
    std::string extension = hintPath.extension().string();
    if (extension.empty() && hint.front() == '.') {
        extension = std::string(hint);
    }

    std::ranges::transform(extension, extension.begin(),
                           [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return std::ranges::find(supportedExtensions, extension) == supportedExtensions.end()
               ? std::string{}
               : extension;
}

template <typename ExtensionRange, typename ExtensionForMime>
std::string chooseTempExtension(std::span<const std::byte> data, std::string_view hint,
                                std::string_view fallbackExtension,
                                const ExtensionRange& supportedExtensions,
                                ExtensionForMime extensionForMime) {
    if (auto extension = normalizeExtensionHint(hint, supportedExtensions); !extension.empty()) {
        return extension;
    }

    auto signatureResult = detection::FileTypeDetector::instance().detectFromBuffer(data);
    if (signatureResult) {
        if (auto extension = extensionForMime(signatureResult.value().mimeType);
            !extension.empty()) {
            return extension;
        }
    }

    return std::string(fallbackExtension);
}

inline Result<std::filesystem::path> writeBufferToTempFile(std::span<const std::byte> data,
                                                           std::string_view extension,
                                                           std::string_view mediaKind) {
    if (data.empty()) {
        return Error{ErrorCode::InvalidArgument, yams::fmt_format("Empty {} data", mediaKind)};
    }

    static thread_local std::random_device rd;
    static thread_local std::mt19937_64 gen(rd());

    std::filesystem::path tempDir;
    std::error_code ec;
    bool created = false;
    for (int attempt = 0; attempt < 16; ++attempt) {
        tempDir = std::filesystem::temp_directory_path() /
                  yams::fmt_format("yams-{}-buffer-{:016x}", mediaKind, gen());
        if (std::filesystem::create_directory(tempDir, ec)) {
            created = true;
            std::filesystem::permissions(tempDir, std::filesystem::perms::owner_all,
                                         std::filesystem::perm_options::replace, ec);
            if (ec) {
                std::filesystem::remove_all(tempDir);
                return Error{ErrorCode::IOError,
                             yams::fmt_format("Failed to secure temp {} directory: {}", mediaKind,
                                              ec.message())};
            }
            break;
        }
        if (ec) {
            return Error{ErrorCode::IOError,
                         yams::fmt_format("Failed to create temp {} directory: {}", mediaKind,
                                          ec.message())};
        }
    }
    if (!created) {
        return Error{ErrorCode::IOError,
                     yams::fmt_format("Failed to allocate temp {} directory", mediaKind)};
    }

    const auto tempPath = tempDir / yams::fmt_format("buffer{}", extension);

    std::ofstream file(tempPath, std::ios::binary | std::ios::trunc);
    if (!file) {
        return Error{ErrorCode::IOError, yams::fmt_format("Failed to create temp {} file: {}",
                                                          mediaKind, tempPath.string())};
    }

    file.write(reinterpret_cast<const char*>(data.data()),
               static_cast<std::streamsize>(data.size()));
    if (!file) {
        std::error_code cleanupEc;
        std::filesystem::remove_all(tempPath.parent_path(), cleanupEc);
        return Error{ErrorCode::IOError, yams::fmt_format("Failed to write temp {} file: {}",
                                                          mediaKind, tempPath.string())};
    }

    return tempPath;
}

class TempFileCleanup {
public:
    explicit TempFileCleanup(std::filesystem::path tempPath) : tempPath_(std::move(tempPath)) {}

    TempFileCleanup(const TempFileCleanup&) = delete;
    TempFileCleanup& operator=(const TempFileCleanup&) = delete;

    ~TempFileCleanup() {
        std::error_code ec;
        std::filesystem::remove_all(tempPath_.parent_path(), ec);
    }

private:
    std::filesystem::path tempPath_;
};

template <typename CanHandle, typename IsGenericDetection, typename ChooseTempExtension,
          typename ProcessPath, typename HasConfirmedMetadata, typename HasContainerMagic>
Result<ContentResult> processBufferWithTempFile(
    std::span<const std::byte> data, const std::string& hint, const ContentConfig& config,
    std::string_view mediaKind, CanHandle canHandle, IsGenericDetection isGenericDetection,
    ChooseTempExtension chooseTempExtension, ProcessPath processPath,
    HasConfirmedMetadata hasConfirmedMetadata, HasContainerMagic hasContainerMagic) {
    auto signatureResult = detection::FileTypeDetector::instance().detectFromBuffer(data);
    bool requiresParserConfirmation = false;
    if (signatureResult && !canHandle(signatureResult.value())) {
        const auto& signature = signatureResult.value();
        requiresParserConfirmation = isGenericDetection(signature);
        if (!requiresParserConfirmation) {
            return Error{ErrorCode::NotSupported,
                         yams::fmt_format("Not a {} file: detected as {} ({})", mediaKind,
                                          signature.mimeType, signature.fileType)};
        }
    }

    auto tempPath = writeBufferToTempFile(data, chooseTempExtension(data, hint), mediaKind);
    if (!tempPath) {
        return tempPath.error();
    }
    TempFileCleanup cleanup{tempPath.value()};

    auto result = processPath(tempPath.value(), config);
    if (result && requiresParserConfirmation && !hasConfirmedMetadata(result.value()) &&
        !hasContainerMagic(data)) {
        return Error{ErrorCode::NotSupported,
                     yams::fmt_format("Buffer did not contain parseable {} metadata", mediaKind)};
    }
    if (result) {
        result.value().metadata["source"] = "buffer";
        if (!hint.empty()) {
            result.value().metadata["hint"] = hint;
        }
    }
    return result;
}

} // namespace yams::content::detail
