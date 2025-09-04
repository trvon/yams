#include <spdlog/fmt/fmt.h>
#include <array>
#include <cstring>
#include <openssl/evp.h>
#include <yams/compression/compression_utils.h>

namespace yams::compression {

namespace {
// CRC32 polynomial used by zlib/gzip
constexpr uint32_t CRC32_POLY = 0xEDB88320U;

// Precomputed CRC32 table
struct CRC32Table {
    std::array<uint32_t, 256> table;

    constexpr CRC32Table() : table{} {
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t crc = i;
            for (int j = 0; j < 8; ++j) {
                crc = (crc >> 1) ^ ((crc & 1) ? CRC32_POLY : 0);
            }
            table[i] = crc;
        }
    }
};

constexpr CRC32Table crc32Table;
} // namespace

uint32_t calculateCRC32(std::span<const std::byte> data) {
    uint32_t crc = 0xFFFFFFFFU;

    for (const auto& byte : data) {
        const uint8_t index = static_cast<uint8_t>(crc ^ static_cast<uint8_t>(byte));
        crc = (crc >> 8) ^ crc32Table.table[index];
    }

    return crc ^ 0xFFFFFFFFU;
}

uint32_t updateCRC32(uint32_t crc, std::span<const std::byte> data) {
    // Undo final XOR from previous calculation
    crc ^= 0xFFFFFFFFU;

    for (const auto& byte : data) {
        const uint8_t index = static_cast<uint8_t>(crc ^ static_cast<uint8_t>(byte));
        crc = (crc >> 8) ^ crc32Table.table[index];
    }

    return crc ^ 0xFFFFFFFFU;
}

bool isLikelyCompressed(std::span<const std::byte> data) {
    if (data.size() < 64) {
        // Too small to make a determination
        return false;
    }

    // Check for common compressed file signatures
    const auto* bytes = reinterpret_cast<const uint8_t*>(data.data());

    // gzip
    if (data.size() >= 2 && bytes[0] == 0x1F && bytes[1] == 0x8B) {
        return true;
    }

    // zlib
    if (data.size() >= 2) {
        const uint16_t header = (bytes[0] << 8) | bytes[1];
        if ((header & 0x0F00) == 0x0800 && header % 31 == 0) {
            return true;
        }
    }

    // zip/pk
    if (data.size() >= 4 && bytes[0] == 0x50 && bytes[1] == 0x4B &&
        (bytes[2] == 0x03 || bytes[2] == 0x05 || bytes[2] == 0x07) &&
        (bytes[3] == 0x04 || bytes[3] == 0x06 || bytes[3] == 0x08)) {
        return true;
    }

    // 7z
    if (data.size() >= 6 && std::memcmp(bytes, "7z\xBC\xAF\x27\x1C", 6) == 0) {
        return true;
    }

    // xz
    if (data.size() >= 6 && std::memcmp(bytes,
                                        "\xFD"
                                        "7zXZ\x00",
                                        6) == 0) {
        return true;
    }

    // bzip2
    if (data.size() >= 3 && bytes[0] == 'B' && bytes[1] == 'Z' &&
        (bytes[2] == 'h' || bytes[2] == '0')) {
        return true;
    }

    // zstd
    if (data.size() >= 4) {
        const uint32_t magic = *reinterpret_cast<const uint32_t*>(bytes);
        if (magic == 0xFD2FB528U || magic == 0x28B52FFDU) { // Little/big endian
            return true;
        }
    }

    // Entropy analysis - compressed data has high entropy
    std::array<size_t, 256> frequency{};
    const size_t sampleSize = std::min(data.size(), size_t(4096));

    for (size_t i = 0; i < sampleSize; ++i) {
        frequency[static_cast<uint8_t>(data[i])]++;
    }

    // Calculate Shannon entropy
    double entropy = 0.0;
    for (const auto& count : frequency) {
        if (count > 0) {
            const double p = static_cast<double>(count) / static_cast<double>(sampleSize);
            entropy -= p * std::log2(p);
        }
    }

    // High entropy (> 7.5 bits per byte) suggests compression
    return entropy > 7.5;
}

size_t estimateCompressionRatio(std::span<const std::byte> data, CompressionAlgorithm algorithm) {
    // Sample the first 64KB for estimation
    const size_t sampleSize = std::min(data.size(), size_t(65536));
    const auto sample = data.subspan(0, sampleSize);

    // Count unique bytes for a rough estimate
    std::array<bool, 256> seen{};
    size_t uniqueBytes = 0;

    for (const auto& byte : sample) {
        const uint8_t b = static_cast<uint8_t>(byte);
        if (!seen[b]) {
            seen[b] = true;
            uniqueBytes++;
        }
    }

    // Estimate based on byte diversity and algorithm
    const double diversity = static_cast<double>(uniqueBytes) / 256.0;

    switch (algorithm) {
        case CompressionAlgorithm::Zstandard:
            // Zstandard typical ratios
            if (diversity < 0.1)
                return 10; // Very repetitive
            if (diversity < 0.3)
                return 5; // Somewhat repetitive
            if (diversity < 0.6)
                return 3; // Normal text/code
            if (diversity < 0.9)
                return 2; // Binary data
            return 1;     // Random/encrypted

        case CompressionAlgorithm::LZMA:
            // LZMA typically achieves better ratios
            if (diversity < 0.1)
                return 20;
            if (diversity < 0.3)
                return 8;
            if (diversity < 0.6)
                return 4;
            if (diversity < 0.9)
                return 2;
            return 1;

        case CompressionAlgorithm::None:
        default:
            return 1;
    }
}

Result<std::string> getCompressionStats(const CompressionResult& result) {
    const double ratio = result.originalSize > 0 ? static_cast<double>(result.originalSize) /
                                                       static_cast<double>(result.compressedSize)
                                                 : 0.0;

    const double throughputMBps =
        result.duration.count() > 0 ? (static_cast<double>(result.originalSize) / (1024 * 1024)) /
                                          (static_cast<double>(result.duration.count()) / 1000000.0)
                                    : 0.0;

    std::string algorithmName;
    switch (result.algorithm) {
        case CompressionAlgorithm::None:
            algorithmName = "None";
            break;
        case CompressionAlgorithm::Zstandard:
            algorithmName = "Zstandard";
            break;
        case CompressionAlgorithm::LZMA:
            algorithmName = "LZMA";
            break;
        default:
            algorithmName = "Unknown";
    }

    return fmt::format("Algorithm: {} (level {})\n"
                       "Original size: {} bytes\n"
                       "Compressed size: {} bytes\n"
                       "Compression ratio: {:.2f}x ({:.1f}% reduction)\n"
                       "Compression time: {}μs\n"
                       "Throughput: {:.2f} MB/s",
                       algorithmName, result.level, result.originalSize, result.compressedSize,
                       ratio, (1.0 - 1.0 / ratio) * 100, result.duration.count(), throughputMBps);
}

const char* algorithmName(CompressionAlgorithm algorithm) noexcept {
    switch (algorithm) {
        case CompressionAlgorithm::None:
            return "None";
        case CompressionAlgorithm::Zstandard:
            return "Zstandard";
        case CompressionAlgorithm::LZMA:
            return "LZMA";
        default:
            return "Unknown";
    }
}

} // namespace yams::compression