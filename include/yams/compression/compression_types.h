#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace yams::compression {

/**
 * @brief Common buffer type for compression operations
 */
using Buffer = std::vector<std::byte>;
using BufferView = std::span<const std::byte>;
using MutableBufferView = std::span<std::byte>;

/**
 * @brief Dictionary for compression
 */
struct CompressionDictionary {
    std::vector<std::byte> data;
    uint64_t id{0};
    std::string name;
    CompressionAlgorithm algorithm{CompressionAlgorithm::None};

    /**
     * @brief Check if dictionary is valid
     * @return True if dictionary has data
     */
    [[nodiscard]] bool isValid() const noexcept { return !data.empty() && id != 0; }
};

/**
 * @brief Chunk for streaming compression
 */
struct CompressionChunk {
    size_t uncompressedOffset{0}; ///< Offset in original stream
    size_t uncompressedSize{0};   ///< Size of uncompressed chunk
    size_t compressedSize{0};     ///< Size after compression
    std::vector<std::byte> data;  ///< Compressed data
};

/**
 * @brief Options for compression operations
 */
struct CompressionOptions {
    uint8_t level{0};                                ///< Compression level (0=default)
    std::optional<CompressionDictionary> dictionary; ///< Optional dictionary
    bool enableChecksum{true};                       ///< Calculate checksums
    size_t chunkSize{0};                             ///< Chunk size for streaming (0=no chunking)
    size_t maxOutputSize{0};                         ///< Max output size (0=unlimited)
};

/**
 * @brief Progress callback for long operations
 */
using ProgressCallback = std::function<void(size_t current, size_t total)>;

/**
 * @brief Error codes specific to compression
 */
enum class CompressionError {
    None = 0,
    InvalidInput,
    InvalidOutput,
    CompressionFailed,
    DecompressionFailed,
    UnsupportedAlgorithm,
    UnsupportedLevel,
    DictionaryError,
    ChecksumMismatch,
    OutputTooLarge,
    CorruptedData,
    InsufficientMemory,
    SystemResourceError
};

/**
 * @brief Convert compression error to string
 * @param error Error code
 * @return Error description
 */
[[nodiscard]] constexpr const char* compressionErrorToString(CompressionError error) {
    switch (error) {
        case CompressionError::None:
            return "No error";
        case CompressionError::InvalidInput:
            return "Invalid input data";
        case CompressionError::InvalidOutput:
            return "Invalid output buffer";
        case CompressionError::CompressionFailed:
            return "Compression failed";
        case CompressionError::DecompressionFailed:
            return "Decompression failed";
        case CompressionError::UnsupportedAlgorithm:
            return "Unsupported algorithm";
        case CompressionError::UnsupportedLevel:
            return "Unsupported compression level";
        case CompressionError::DictionaryError:
            return "Dictionary error";
        case CompressionError::ChecksumMismatch:
            return "Checksum mismatch";
        case CompressionError::OutputTooLarge:
            return "Output too large";
        case CompressionError::CorruptedData:
            return "Corrupted compressed data";
        case CompressionError::InsufficientMemory:
            return "Insufficient memory";
        case CompressionError::SystemResourceError:
            return "System resource error";
        default:
            return "Unknown compression error";
    }
}

/**
 * @brief Utility functions for compression
 */
namespace util {

/**
 * @brief Calculate CRC32 checksum
 * @param data Data to checksum
 * @return CRC32 value
 */
[[nodiscard]] uint32_t calculateCRC32(BufferView data);

/**
 * @brief Estimate compressed size
 * @param algo Algorithm to use
 * @param inputSize Input data size
 * @param level Compression level
 * @return Estimated compressed size
 */
[[nodiscard]] size_t estimateCompressedSize(CompressionAlgorithm algo, size_t inputSize,
                                            uint8_t level = 0);

/**
 * @brief Check if data appears to be compressed
 * @param data Data to check
 * @return True if data looks compressed
 */
[[nodiscard]] bool isCompressed(BufferView data);

/**
 * @brief Get recommended chunk size for streaming
 * @param algo Algorithm being used
 * @param totalSize Total data size
 * @return Recommended chunk size
 */
[[nodiscard]] size_t recommendedChunkSize(CompressionAlgorithm algo, size_t totalSize);

} // namespace util

} // namespace yams::compression