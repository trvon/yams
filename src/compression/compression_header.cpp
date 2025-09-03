#include <chrono>
#include <cstring>
#include <yams/compression/compression_header.h>
#include <yams/compression/compression_utils.h>

namespace yams::compression {

//-----------------------------------------------------------------------------
// CompressionHeader
//-----------------------------------------------------------------------------

CompressionHeader::CompressionHeader() noexcept
    : magic(MAGIC), version(VERSION), algorithm(static_cast<uint8_t>(CompressionAlgorithm::None)),
      level(0), flags(0), reserved1(0), uncompressedSize(0), compressedSize(0),
      uncompressedCRC32(0), compressedCRC32(0), timestamp(0), dictionaryId(0), reserved2{} {}

Result<CompressionHeader> CompressionHeader::parse(std::span<const std::byte> data) {
    if (data.size() < SIZE) {
        return Error{ErrorCode::InvalidArgument,
                     "Insufficient data for header: " + std::to_string(data.size()) +
                         " bytes, need " + std::to_string(SIZE)};
    }

    CompressionHeader header;
    std::memcpy(&header, data.data(), SIZE);

    // Convert from little-endian if necessary
    // (Assuming little-endian storage format)

    if (!header.validate()) {
        return Error{ErrorCode::InvalidData, "Invalid compression header"};
    }

    return header;
}

std::vector<std::byte> CompressionHeader::serialize() const {
    std::vector<std::byte> data(SIZE);

    // Copy header to buffer (assuming little-endian storage)
    std::memcpy(data.data(), this, SIZE);

    return data;
}

bool CompressionHeader::validate() const noexcept {
    // Check magic number
    if (magic != MAGIC) {
        return false;
    }

    // Check version
    if (version > VERSION) {
        return false;
    }

    // Check algorithm is valid
    if (algorithm > static_cast<uint8_t>(CompressionAlgorithm::LZMA)) {
        return false;
    }

    // Check sizes make sense
    if (algorithm != static_cast<uint8_t>(CompressionAlgorithm::None)) {
        if (compressedSize == 0 || uncompressedSize == 0) {
            return false;
        }
        if (compressedSize > uncompressedSize * 2) {
            // Compression should not make data larger than 2x
            return false;
        }
    }

    // Check compression level is reasonable
    if (level > 22) { // Max zstd level
        return false;
    }

    return true;
}

CompressionHeader CompressionHeader::fromResult(
    const CompressionResult& result, std::pair<uint32_t, uint32_t> crcs) {
    CompressionHeader header;
    header.magic = MAGIC;
    header.version = VERSION;
    header.algorithm = static_cast<uint8_t>(result.algorithm);
    header.level = result.level;
    header.flags = 0;
    header.reserved1 = 0;
    header.uncompressedSize = result.originalSize;
    header.compressedSize = result.compressedSize;
    header.uncompressedCRC32 = crcs.first;
    header.compressedCRC32 = crcs.second;
    header.timestamp = std::chrono::duration_cast<std::chrono::seconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    header.dictionaryId = 0;
    std::memset(header.reserved2, 0, sizeof(header.reserved2));

    return header;
}

//-----------------------------------------------------------------------------
// ExtendedCompressionHeader
//-----------------------------------------------------------------------------

std::vector<std::byte> ExtendedCompressionHeader::serialize() const {
    const size_t totalSize = sizeof(uint32_t) * 2 + sizeof(uint64_t) * 2 + customMetadata.size();
    std::vector<std::byte> data(totalSize);

    size_t offset = 0;

    // Write fixed fields
    std::memcpy(data.data() + offset, &metadataSize, sizeof(metadataSize));
    offset += sizeof(metadataSize);

    std::memcpy(data.data() + offset, &chunkCount, sizeof(chunkCount));
    offset += sizeof(chunkCount);

    std::memcpy(data.data() + offset, &totalChunks, sizeof(totalChunks));
    offset += sizeof(totalChunks);

    std::memcpy(data.data() + offset, &chunkIndex, sizeof(chunkIndex));
    offset += sizeof(chunkIndex);

    // Write custom metadata
    if (!customMetadata.empty()) {
        std::memcpy(data.data() + offset, customMetadata.data(), customMetadata.size());
    }

    return data;
}

Result<ExtendedCompressionHeader>
ExtendedCompressionHeader::parse(std::span<const std::byte> data) {
    const size_t minSize = sizeof(uint32_t) * 2 + sizeof(uint64_t) * 2;
    if (data.size() < minSize) {
        return Error{ErrorCode::InvalidArgument,
                     "Insufficient data for extended header: " + std::to_string(data.size()) +
                         " bytes, need at least " + std::to_string(minSize)};
    }

    ExtendedCompressionHeader header;
    size_t offset = 0;

    // Read fixed fields
    std::memcpy(&header.metadataSize, data.data() + offset, sizeof(header.metadataSize));
    offset += sizeof(header.metadataSize);

    std::memcpy(&header.chunkCount, data.data() + offset, sizeof(header.chunkCount));
    offset += sizeof(header.chunkCount);

    std::memcpy(&header.totalChunks, data.data() + offset, sizeof(header.totalChunks));
    offset += sizeof(header.totalChunks);

    std::memcpy(&header.chunkIndex, data.data() + offset, sizeof(header.chunkIndex));
    offset += sizeof(header.chunkIndex);

    // Read custom metadata
    if (header.metadataSize > 0) {
        if (data.size() < offset + header.metadataSize) {
            return Error{ErrorCode::InvalidData,
                         "Extended header specifies more metadata than available"};
        }

        header.customMetadata.resize(header.metadataSize);
        std::memcpy(header.customMetadata.data(), data.data() + offset, header.metadataSize);
    }

    return header;
}

//-----------------------------------------------------------------------------
// CompressedBlock
//-----------------------------------------------------------------------------

CompressedBlock::CompressedBlock(std::vector<std::byte> data) : data_(std::move(data)) {
    if (data_.size() >= CompressionHeader::SIZE) {
        auto result = CompressionHeader::parse(data_);
        if (result) {
            header_ = result.value();
        }
    }
}

CompressedBlock::CompressedBlock(CompressionHeader header, std::vector<std::byte> compressedData)
    : header_(header) {
    // Combine header and data
    auto serializedHeader = header.serialize();
    data_.reserve(serializedHeader.size() + compressedData.size());
    data_.insert(data_.end(), serializedHeader.begin(), serializedHeader.end());
    data_.insert(data_.end(), compressedData.begin(), compressedData.end());
}

std::span<const std::byte> CompressedBlock::compressedData() const noexcept {
    if (data_.size() <= CompressionHeader::SIZE) {
        return {};
    }
    return std::span<const std::byte>(data_.data() + CompressionHeader::SIZE,
                                      data_.size() - CompressionHeader::SIZE);
}

bool CompressedBlock::validate() const {
    if (data_.size() < CompressionHeader::SIZE) {
        return false;
    }

    if (!header_.validate()) {
        return false;
    }

    // Check data size matches header
    const size_t expectedSize = CompressionHeader::SIZE + header_.compressedSize;
    if (data_.size() != expectedSize) {
        return false;
    }

    // Verify CRC32
    const auto compressed = compressedData();
    const uint32_t calculatedCRC = calculateCRC32(compressed);

    return calculatedCRC == header_.compressedCRC32;
}

bool CompressedBlock::isCompressedBlock(std::span<const std::byte> data) {
    if (data.size() < CompressionHeader::SIZE) {
        return false;
    }

    // Check magic number at the beginning
    uint32_t magic;
    std::memcpy(&magic, data.data(), sizeof(magic));

    return magic == CompressionHeader::MAGIC;
}

} // namespace yams::compression