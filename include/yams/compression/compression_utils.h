#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <yams/compression/compressor_interface.h>
#include <yams/core/types.h>

namespace yams::compression {

/**
 * @brief Calculate CRC32 checksum
 * @param data Data to checksum
 * @return CRC32 value
 */
[[nodiscard]] uint32_t calculateCRC32(std::span<const std::byte> data);

/**
 * @brief Update CRC32 checksum with additional data
 * @param crc Current CRC32 value
 * @param data Additional data
 * @return Updated CRC32 value
 */
[[nodiscard]] uint32_t updateCRC32(uint32_t crc, std::span<const std::byte> data);

/**
 * @brief Check if data is likely already compressed
 * @param data Data to check
 * @return True if data appears compressed
 */
[[nodiscard]] bool isLikelyCompressed(std::span<const std::byte> data);

/**
 * @brief Estimate compression ratio for given data and algorithm
 * @param data Data to analyze
 * @param algorithm Compression algorithm
 * @return Estimated compression ratio (e.g., 3 means 3:1)
 */
[[nodiscard]] size_t estimateCompressionRatio(std::span<const std::byte> data,
                                              CompressionAlgorithm algorithm);

/**
 * @brief Format compression statistics for display
 * @param result Compression result
 * @return Formatted statistics string
 */
[[nodiscard]] Result<std::string> getCompressionStats(const CompressionResult& result);

/**
 * @brief Get algorithm name as string
 * @param algorithm Compression algorithm
 * @return Algorithm name
 */
[[nodiscard]] const char* algorithmName(CompressionAlgorithm algorithm) noexcept;

} // namespace yams::compression