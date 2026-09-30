#include <yams/compression/framed_payload.h>

#include <chrono>
#include <cstring>
#include <string>

#include <yams/compression/compression_header.h>
#include <yams/compression/compression_utils.h>
#include <yams/compression/compressor_interface.h>

namespace yams::compression {

Result<std::vector<std::byte>> encodeFramedPayload(std::span<const std::byte> data) {
    auto compressor =
        CompressionRegistry::instance().createCompressor(CompressionAlgorithm::Zstandard);
    if (!compressor) {
        return Error{ErrorCode::InvalidState, "Zstandard compressor unavailable"};
    }
    constexpr uint8_t kLevel = 3;
    auto compressed = compressor->compress(data, kLevel);
    if (!compressed) {
        return compressed.error();
    }
    const auto& body = compressed.value();

    CompressionHeader header{};
    header.magic = CompressionHeader::MAGIC;
    header.version = CompressionHeader::VERSION;
    header.algorithm = static_cast<uint8_t>(body.algorithm);
    header.level = body.level;
    header.uncompressedSize = static_cast<uint64_t>(body.originalSize);
    header.compressedSize = static_cast<uint64_t>(body.data.size());
    header.uncompressedCRC32 = calculateCRC32(data);
    header.compressedCRC32 = calculateCRC32(body.data);
    const auto nowNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    header.timestamp = nowNs < 0 ? 0ULL : static_cast<uint64_t>(nowNs);
    header.flags = 0;
    header.reserved1 = 0;
    std::memset(header.reserved2, 0, sizeof(header.reserved2));

    static_assert(sizeof(CompressionHeader) == CompressionHeader::SIZE);
    std::vector<std::byte> framed(CompressionHeader::SIZE);
    std::memcpy(framed.data(), &header, CompressionHeader::SIZE);
    framed.insert(framed.end(), body.data.begin(), body.data.end());
    return framed;
}

Result<std::vector<std::byte>> decodeFramedPayload(std::span<const std::byte> framed) {
    if (framed.size() < CompressionHeader::SIZE) {
        return Error{ErrorCode::InvalidData, "framed payload is shorter than its header"};
    }
    auto parsed = CompressionHeader::parse(framed.first(CompressionHeader::SIZE));
    if (!parsed) {
        return parsed.error();
    }
    const auto& header = parsed.value();
    if (!header.validate()) {
        return Error{ErrorCode::InvalidData, "framed payload header is invalid"};
    }

    const auto body = framed.subspan(CompressionHeader::SIZE);
    if (body.size() != header.compressedSize) {
        return Error{ErrorCode::CorruptedData,
                     "framed payload body is " + std::to_string(body.size()) +
                         " bytes; header says " + std::to_string(header.compressedSize)};
    }
    if (calculateCRC32(body) != header.compressedCRC32) {
        return Error{ErrorCode::CorruptedData, "framed payload body CRC mismatch"};
    }

    const auto algorithm = static_cast<CompressionAlgorithm>(header.algorithm);
    std::vector<std::byte> out;
    if (algorithm == CompressionAlgorithm::None) {
        out.assign(body.begin(), body.end());
    } else {
        auto compressor = CompressionRegistry::instance().createCompressor(algorithm);
        if (!compressor) {
            return Error{ErrorCode::NotSupported,
                         std::string("no decoder for compression algorithm ") +
                             algorithmName(algorithm)};
        }
        auto decompressed =
            compressor->decompress(body, static_cast<std::size_t>(header.uncompressedSize));
        if (!decompressed) {
            return decompressed.error();
        }
        out = std::move(decompressed.value());
    }

    if (out.size() != header.uncompressedSize) {
        return Error{ErrorCode::CorruptedData, "framed payload decoded to the wrong size"};
    }
    if (calculateCRC32(out) != header.uncompressedCRC32) {
        return Error{ErrorCode::CorruptedData, "framed payload content CRC mismatch"};
    }
    return out;
}

} // namespace yams::compression
