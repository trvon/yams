#pragma once

#include <yams/core/types.h>
#include <span>
#include <vector>
#include <chrono>
#include <memory>

namespace yams::compression {

/**
 * @brief Supported compression algorithms
 */
enum class CompressionAlgorithm : uint8_t {
    None = 0,      ///< No compression
    Zstandard = 1, ///< Zstandard (fast, good ratio)
    LZMA = 2,      ///< LZMA (slow, best ratio)
    // Future algorithms can be added here
    // LZ4 = 3,    ///< LZ4 (fastest, lower ratio)
    // Brotli = 4, ///< Brotli (web-optimized)
};

/**
 * @brief Result of a compression operation
 */
struct CompressionResult {
    std::vector<std::byte> data;          ///< Compressed data
    size_t originalSize;                  ///< Size before compression
    size_t compressedSize;                ///< Size after compression
    CompressionAlgorithm algorithm;       ///< Algorithm used
    uint8_t level;                        ///< Compression level used
    std::chrono::milliseconds duration;   ///< Time taken
    
    /**
     * @brief Calculate compression ratio
     * @return Ratio of original to compressed size
     */
    [[nodiscard]] double ratio() const noexcept {
        return compressedSize > 0 
            ? static_cast<double>(originalSize) / compressedSize 
            : 0.0;
    }
    
    /**
     * @brief Calculate space savings percentage
     * @return Percentage of space saved
     */
    [[nodiscard]] double spaceSaved() const noexcept {
        return originalSize > 0 
            ? (1.0 - static_cast<double>(compressedSize) / originalSize) * 100.0 
            : 0.0;
    }
};

/**
 * @brief Abstract interface for compression algorithms
 */
class ICompressor {
public:
    virtual ~ICompressor() = default;
    
    /**
     * @brief Compress data
     * @param data Input data to compress
     * @param level Compression level (0 = default for algorithm)
     * @return Compression result or error
     */
    [[nodiscard]] virtual Result<CompressionResult> compress(
        std::span<const std::byte> data,
        uint8_t level = 0) = 0;
        
    /**
     * @brief Decompress data
     * @param data Compressed data
     * @param expectedSize Expected uncompressed size (hint for allocation)
     * @return Decompressed data or error
     */
    [[nodiscard]] virtual Result<std::vector<std::byte>> decompress(
        std::span<const std::byte> data,
        size_t expectedSize = 0) = 0;
        
    /**
     * @brief Get the compression algorithm type
     * @return Algorithm identifier
     */
    [[nodiscard]] virtual CompressionAlgorithm algorithm() const = 0;
    
    /**
     * @brief Get supported compression levels
     * @return Pair of (min_level, max_level)
     */
    [[nodiscard]] virtual std::pair<uint8_t, uint8_t> supportedLevels() const = 0;
    
    /**
     * @brief Calculate maximum compressed size for given input
     * @param inputSize Size of input data
     * @return Maximum possible compressed size
     */
    [[nodiscard]] virtual size_t maxCompressedSize(size_t inputSize) const = 0;
    
    /**
     * @brief Check if the algorithm supports streaming compression
     * @return True if streaming is supported
     */
    [[nodiscard]] virtual bool supportsStreaming() const { return false; }
    
    /**
     * @brief Check if the algorithm supports dictionary compression
     * @return True if dictionaries are supported
     */
    [[nodiscard]] virtual bool supportsDictionary() const { return false; }
};

/**
 * @brief Result of a streaming compression/decompression operation
 */
struct StreamingResult {
    size_t bytesConsumed;     ///< Bytes consumed from input
    size_t bytesProduced;     ///< Bytes written to output
    bool hasMoreOutput;       ///< More output available without more input
};

/**
 * @brief Streaming compression interface
 */
class IStreamingCompressor {
public:
    virtual ~IStreamingCompressor() = default;
    
    /**
     * @brief Initialize compression stream
     * @param level Compression level
     * @return Success or error
     */
    [[nodiscard]] virtual Result<void> init(uint8_t level = 0) = 0;
    
    /**
     * @brief Feed data to compression stream
     * @param input Input data chunk
     * @param output Output buffer for compressed data
     * @return Number of bytes written to output
     */
    [[nodiscard]] virtual Result<size_t> compress(
        std::span<const std::byte> input,
        std::span<std::byte> output) = 0;
        
    /**
     * @brief Finish compression and flush remaining data
     * @param output Output buffer for remaining compressed data
     * @return Number of bytes written to output
     */
    [[nodiscard]] virtual Result<size_t> finish(std::span<std::byte> output) = 0;
    
    /**
     * @brief Reset the stream for reuse
     */
    virtual void reset() = 0;
};

/**
 * @brief Streaming decompression interface
 */
class IStreamingDecompressor {
public:
    virtual ~IStreamingDecompressor() = default;
    
    /**
     * @brief Initialize decompression stream
     * @return Success or error
     */
    [[nodiscard]] virtual Result<void> init() = 0;
    
    /**
     * @brief Feed compressed data to decompression stream
     * @param input Compressed data chunk
     * @param output Output buffer for decompressed data
     * @return Number of bytes written to output
     */
    [[nodiscard]] virtual Result<size_t> decompress(
        std::span<const std::byte> input,
        std::span<std::byte> output) = 0;
        
    /**
     * @brief Check if decompression is complete
     * @return True if all data has been decompressed
     */
    [[nodiscard]] virtual bool isFinished() const = 0;
    
    /**
     * @brief Reset the stream for reuse
     */
    virtual void reset() = 0;
};

/**
 * @brief Factory function type for creating compressors
 */
using CompressorFactory = std::function<std::unique_ptr<ICompressor>()>;

/**
 * @brief Registry for compression algorithms
 */
class CompressionRegistry {
public:
    /**
     * @brief Get the singleton instance
     */
    static CompressionRegistry& instance();
    
    /**
     * @brief Register a compressor factory
     * @param algorithm Algorithm identifier
     * @param factory Factory function
     */
    void registerCompressor(CompressionAlgorithm algorithm, CompressorFactory factory);
    
    /**
     * @brief Create a compressor instance
     * @param algorithm Algorithm to use
     * @return Compressor instance or nullptr if not registered
     */
    [[nodiscard]] std::unique_ptr<ICompressor> createCompressor(
        CompressionAlgorithm algorithm) const;
        
    /**
     * @brief Check if an algorithm is available
     * @param algorithm Algorithm to check
     * @return True if the algorithm is registered
     */
    [[nodiscard]] bool isAvailable(CompressionAlgorithm algorithm) const;
    
    /**
     * @brief Get list of available algorithms
     * @return Vector of available algorithm identifiers
     */
    [[nodiscard]] std::vector<CompressionAlgorithm> availableAlgorithms() const;
    
private:
    CompressionRegistry() = default;
    std::unordered_map<CompressionAlgorithm, CompressorFactory> factories_;
    mutable std::mutex mutex_;
};

} // namespace yams::compression