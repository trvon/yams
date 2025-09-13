#pragma once

#include <memory>
#include <yams/compression/compressor_interface.h>

namespace yams::compression {

/**
 * @brief LZMA compression implementation
 *
 * Provides high compression ratios, suitable for archival storage.
 * Supports both LZMA and LZMA2 algorithms.
 */
class LZMACompressor final : public ICompressor {
public:
    /**
     * @brief LZMA algorithm variants
     */
    enum class Variant {
        LZMA = 0, ///< Original LZMA algorithm
        LZMA2 = 1 ///< Improved LZMA2 with better streaming
    };

    /**
     * @brief Construct with default settings (LZMA2)
     */
    LZMACompressor();

    /**
     * @brief Construct with specific variant
     * @param variant LZMA or LZMA2
     */
    explicit LZMACompressor(Variant variant);

    ~LZMACompressor() override;

    /**
     * @brief Set algorithm variant
     * @param variant LZMA or LZMA2
     */
    void setVariant(Variant variant);

    /**
     * @brief Get current variant
     * @return Current algorithm variant
     */
    [[nodiscard]] Variant variant() const;

    // ICompressor implementation
    [[nodiscard]] Result<CompressionResult> compress(std::span<const std::byte> data,
                                                     uint8_t level = 0) override;

    [[nodiscard]] Result<std::vector<std::byte>> decompress(std::span<const std::byte> data,
                                                            size_t expectedSize = 0) override;

    [[nodiscard]] CompressionAlgorithm algorithm() const override {
        return CompressionAlgorithm::LZMA;
    }

    [[nodiscard]] std::pair<uint8_t, uint8_t> supportedLevels() const override { return {0, 9}; }

    [[nodiscard]] size_t maxCompressedSize(size_t inputSize) const override;

    [[nodiscard]] bool supportsStreaming() const override { return true; }
    [[nodiscard]] bool supportsDictionary() const override { return true; }

    // LZMA doesn't have built-in streaming like Zstandard
    // These return nullptr for now
    [[nodiscard]] std::unique_ptr<IStreamingCompressor>
    createStreamCompressor([[maybe_unused]] uint8_t level = 0) {
        return nullptr;
    }

    [[nodiscard]] std::unique_ptr<IStreamingDecompressor> createStreamDecompressor() {
        return nullptr;
    }

    /**
     * @brief Get dictionary size for compression level
     * @param level Compression level (0-9)
     * @return Dictionary size in bytes
     */
    [[nodiscard]] static uint32_t getDictionarySize(uint8_t level);

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

} // namespace yams::compression
