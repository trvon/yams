#pragma once

#include <cstdint>
#include <cstring>
#include <span>
#include <utility>
#include <vector>
#include <yams/compression/compressor_interface.h>
#include <yams/core/types.h>

namespace yams::compression {

/**
 * @brief Compression flags for future extensions
 */
enum class CompressionFlags : uint8_t {
    None = 0x00,
    HasDictionary = 0x01, ///< Uses compression dictionary
    IsChunked = 0x02,     ///< Data is chunked for streaming
    HasMetadata = 0x04,   ///< Additional metadata follows header
    IsEncrypted = 0x08,   ///< Data is encrypted after compression
};

/**
 * @brief Header for compressed data blocks
 *
 * This header is prepended to all compressed data to enable proper
 * decompression and validation. The format is designed to be:
 * - Forward compatible (reserved space for future fields)
 * - Self-validating (magic number and CRCs)
 * - Informative (includes sizes and algorithm info)
 */
#pragma pack(push, 1) // Ensure no padding
struct CompressionHeader {
    static constexpr uint32_t MAGIC = 0x4B524E43; ///< "KRNC" in hex
    static constexpr uint8_t VERSION = 1;         ///< Current header version
    static constexpr size_t SIZE = 64;            ///< Total header size

    // Header fields (64 bytes total, packed to avoid padding)
    uint32_t magic;             ///< Magic number for validation (4 bytes)
    uint8_t version;            ///< Header version (1 byte)
    uint8_t algorithm;          ///< CompressionAlgorithm enum value (1 byte)
    uint8_t level;              ///< Compression level used (1 byte)
    uint8_t flags;              ///< CompressionFlags bitmap (1 byte)
    uint32_t reserved1;         ///< Reserved for alignment (4 bytes)
    uint64_t uncompressedSize;  ///< Original data size (8 bytes)
    uint64_t compressedSize;    ///< Compressed data size (8 bytes)
    uint32_t uncompressedCRC32; ///< CRC32 of original data (4 bytes)
    uint32_t compressedCRC32;   ///< CRC32 of compressed data (4 bytes)
    uint64_t timestamp;         ///< Unix timestamp of compression (8 bytes)
    uint64_t dictionaryId;      ///< Dictionary ID if HasDictionary (8 bytes)
    uint8_t reserved2[12];      ///< Reserved for future use (12 bytes to make 64 total)

    /**
     * @brief Default constructor initializes header with defaults
     */
    CompressionHeader() noexcept;

    /**
     * @brief Parse header from raw bytes
     * @param data Raw byte data (must be at least SIZE bytes)
     * @return Parsed header or error
     */
    [[nodiscard]] static Result<CompressionHeader> parse(std::span<const std::byte> data);

    /**
     * @brief Serialize header to bytes
     * @return Serialized header (exactly SIZE bytes)
     */
    [[nodiscard]] std::vector<std::byte> serialize() const;

    /**
     * @brief Validate header consistency
     * @return True if header is valid
     */
    [[nodiscard]] bool validate() const noexcept;

    /**
     * @brief Check if a specific flag is set
     * @param flag Flag to check
     * @return True if the flag is set
     */
    [[nodiscard]] bool hasFlag(CompressionFlags flag) const noexcept {
        return (flags & static_cast<uint8_t>(flag)) != 0;
    }

    /**
     * @brief Set a compression flag
     * @param flag Flag to set
     */
    void setFlag(CompressionFlags flag) noexcept { flags |= static_cast<uint8_t>(flag); }

    /**
     * @brief Get compression ratio
     * @return Ratio of original to compressed size
     */
    [[nodiscard]] double compressionRatio() const noexcept {
        return compressedSize > 0
                   ? static_cast<double>(uncompressedSize) / static_cast<double>(compressedSize)
                   : 0.0;
    }

    /**
     * @brief Create header from compression result
     * @param result Compression result
     * @param uncompressedCRC CRC32 of original data
     * @param compressedCRC CRC32 of compressed data
     * @return Initialized header
     */
    [[nodiscard]] static CompressionHeader
    fromResult(const CompressionResult& result, std::pair<uint32_t, uint32_t> crcs);
};
#pragma pack(pop) // Restore default packing

// Ensure struct is exactly 64 bytes
static_assert(sizeof(CompressionHeader) == CompressionHeader::SIZE,
              "CompressionHeader must be exactly 64 bytes");

/**
 * @brief Extended header for additional metadata
 *
 * When HasMetadata flag is set, this structure follows the main header
 */
struct ExtendedCompressionHeader {
    uint32_t metadataSize;                 ///< Size of metadata section
    uint32_t chunkCount;                   ///< Number of chunks (if chunked)
    uint64_t totalChunks;                  ///< Total chunks in stream (if streaming)
    uint64_t chunkIndex;                   ///< Current chunk index (if streaming)
    std::vector<std::byte> customMetadata; ///< Application-specific metadata

    /**
     * @brief Serialize extended header
     * @return Serialized data
     */
    [[nodiscard]] std::vector<std::byte> serialize() const;

    /**
     * @brief Parse extended header
     * @param data Raw byte data
     * @return Parsed header or error
     */
    [[nodiscard]] static Result<ExtendedCompressionHeader> parse(std::span<const std::byte> data);
};

/**
 * @brief Utility class for working with compressed data blocks
 */
class CompressedBlock {
public:
    /**
     * @brief Construct from raw compressed data with header
     * @param data Complete compressed block (header + data)
     */
    explicit CompressedBlock(std::vector<std::byte> data);

    /**
     * @brief Construct from header and compressed data
     * @param header Compression header
     * @param compressedData Compressed data (without header)
     */
    CompressedBlock(CompressionHeader header, std::vector<std::byte> compressedData);

    /**
     * @brief Get the header
     * @return Compression header
     */
    [[nodiscard]] const CompressionHeader& header() const noexcept { return header_; }

    /**
     * @brief Get compressed data (without header)
     * @return Compressed data span
     */
    [[nodiscard]] std::span<const std::byte> compressedData() const noexcept;

    /**
     * @brief Get complete block (header + data)
     * @return Complete block data
     */
    [[nodiscard]] std::span<const std::byte> fullData() const noexcept { return data_; }

    /**
     * @brief Validate block integrity
     * @return True if block is valid
     */
    [[nodiscard]] bool validate() const;

    /**
     * @brief Check if block contains compressed data
     * @param data Data to check
     * @return True if data appears to be a compressed block
     */
    [[nodiscard]] static bool isCompressedBlock(std::span<const std::byte> data);

private:
    CompressionHeader header_;
    std::vector<std::byte> data_; ///< Complete block (header + compressed data)
};

} // namespace yams::compression
