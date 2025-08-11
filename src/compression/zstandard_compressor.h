#pragma once

#include <yams/compression/compressor_interface.h>
#include <zstd.h>
#include <memory>
#include <mutex>

namespace yams::compression {

/**
 * @brief Zstandard compression implementation
 * 
 * Provides fast compression with good compression ratios.
 * Thread-safe implementation with context reuse for performance.
 */
class ZstandardCompressor final : public ICompressor {
public:
    ZstandardCompressor();
    ~ZstandardCompressor() override;
    
    // ICompressor implementation
    [[nodiscard]] Result<CompressionResult> compress(
        std::span<const std::byte> data,
        uint8_t level = 0) override;
        
    [[nodiscard]] Result<std::vector<std::byte>> decompress(
        std::span<const std::byte> data,
        size_t expectedSize = 0) override;
        
    [[nodiscard]] CompressionAlgorithm algorithm() const override { 
        return CompressionAlgorithm::Zstandard; 
    }
    
    [[nodiscard]] std::pair<uint8_t, uint8_t> supportedLevels() const override {
        return {1, 22};
    }
    
    [[nodiscard]] size_t maxCompressedSize(size_t inputSize) const override {
        return ZSTD_compressBound(inputSize);
    }
    
    [[nodiscard]] bool supportsStreaming() const override { return true; }
    [[nodiscard]] bool supportsDictionary() const override { return true; }
    
    // Streaming support
    [[nodiscard]] std::unique_ptr<IStreamingCompressor> createStreamCompressor(
        uint8_t level = 0);
        
    [[nodiscard]] std::unique_ptr<IStreamingDecompressor> createStreamDecompressor();
    
private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * @brief Streaming compression implementation for Zstandard
 */
class ZstandardStreamCompressor final : public IStreamingCompressor {
public:
    explicit ZstandardStreamCompressor(uint8_t level = 0);
    ~ZstandardStreamCompressor() override;
    
    [[nodiscard]] Result<void> init(uint8_t level = 0) override;
    
    [[nodiscard]] Result<size_t> compress(
        std::span<const std::byte> input,
        std::span<std::byte> output) override;
        
    [[nodiscard]] Result<size_t> finish(std::span<std::byte> output) override;
        
    void reset() override;
    
    [[nodiscard]] CompressionAlgorithm algorithm() const {
        return CompressionAlgorithm::Zstandard;
    }
    
    [[nodiscard]] size_t recommendedOutputSize(size_t inputSize) const;
    
private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * @brief Streaming decompression implementation for Zstandard
 */
class ZstandardStreamDecompressor final : public IStreamingDecompressor {
public:
    ZstandardStreamDecompressor();
    ~ZstandardStreamDecompressor() override;
    
    [[nodiscard]] Result<void> init() override;
    
    [[nodiscard]] Result<size_t> decompress(
        std::span<const std::byte> input,
        std::span<std::byte> output) override;
        
    void reset() override;
    
    [[nodiscard]] CompressionAlgorithm algorithm() const {
        return CompressionAlgorithm::Zstandard;
    }
    
    [[nodiscard]] bool isFinished() const override;
    
    [[nodiscard]] size_t recommendedOutputSize(size_t inputSize) const;
    
private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

} // namespace yams::compression