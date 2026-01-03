#pragma once

#include <memory>
#include <yams/compression/compression_policy.h>
#include <yams/compression/compression_stats.h>
#include <yams/compression/compressor_interface.h>
#include <yams/storage/storage_engine.h>

namespace yams::storage {

/**
 * @brief Storage engine wrapper that provides transparent compression
 *
 * This class wraps an existing storage engine and adds automatic compression
 * and decompression based on a configurable policy. It maintains statistics
 * and ensures backward compatibility with uncompressed data.
 */
class CompressedStorageEngine : public StorageEngine {
public:
    /**
     * @brief Configuration for compressed storage
     */
    struct Config {
        bool enableCompression{true};                      ///< Enable compression for new data
        bool compressExisting{false};                      ///< Compress existing uncompressed data
        compression::CompressionPolicy::Rules policyRules; ///< Compression policy rules
        size_t compressionThreshold{4096};                 ///< Minimum size to consider compression
        bool asyncCompression{true};                       ///< Use background compression
        size_t maxAsyncQueue{1000};                        ///< Maximum async compression queue size
        std::chrono::seconds metadataCacheTTL{300};        ///< Metadata cache time-to-live
    };

    /**
     * @brief Construct with underlying storage and configuration
     * @param underlying The storage engine to wrap
     * @param config Configuration parameters
     */
    CompressedStorageEngine(std::shared_ptr<StorageEngine> underlying, Config config);

    ~CompressedStorageEngine() override;

    // StorageEngine implementation
    [[nodiscard]] Result<void> store(std::string_view hash,
                                     std::span<const std::byte> data) override;

    [[nodiscard]] Result<std::vector<std::byte>> retrieve(std::string_view hash) const override;
    [[nodiscard]] Result<IStorageEngine::RawObject>
    retrieveRaw(std::string_view hash) const override;

    [[nodiscard]] Result<bool> exists(std::string_view hash) const noexcept override;

    [[nodiscard]] Result<void> remove(std::string_view hash) override;
    [[nodiscard]] Result<uint64_t> getBlockSize(std::string_view hash) const override;

    // Additional compressed storage functionality
    [[nodiscard]] Result<uint64_t> getCompressedSize(std::string_view key) const;

    [[nodiscard]] Result<uint64_t> getUncompressedSize(std::string_view key) const;

    // Async operations
    std::future<Result<void>> storeAsync(std::string_view hash,
                                         std::span<const std::byte> data) override;
    std::future<Result<std::vector<std::byte>>> retrieveAsync(std::string_view hash) const override;
    std::future<Result<IStorageEngine::RawObject>>
    retrieveRawAsync(std::string_view hash) const override;

    // Batch operations
    std::vector<Result<void>>
    storeBatch(const std::vector<std::pair<std::string, std::vector<std::byte>>>& items) override;

    // Statistics and maintenance
    StorageStats getStats() const noexcept override;
    Result<uint64_t> getStorageSize() const override;

    // Compression-specific methods

    /**
     * @brief Get compression statistics
     * @return Current compression statistics
     */
    [[nodiscard]] compression::CompressionStats getCompressionStats() const;

    /**
     * @brief Compress an existing uncompressed entry
     * @param hash Hash of the entry to compress
     * @param force Force compression even if policy says no
     * @return Success or error
     */
    [[nodiscard]] Result<void> compressExisting(std::string_view hash, bool force = false);

    /**
     * @brief Decompress an existing compressed entry
     * @param hash Hash of the entry to decompress
     * @return Success or error
     */
    [[nodiscard]] Result<void> decompressExisting(std::string_view hash);

    /**
     * @brief Update compression policy rules
     * @param rules New policy rules
     */
    void setPolicyRules(compression::CompressionPolicy::Rules rules);

    /**
     * @brief Get current policy rules
     * @return Current policy rules
     */
    [[nodiscard]] compression::CompressionPolicy::Rules getPolicyRules() const;

    /**
     * @brief Enable or disable compression
     * @param enable True to enable compression
     */
    void setCompressionEnabled(bool enable);

    /**
     * @brief Check if compression is enabled
     * @return True if compression is enabled
     */
    [[nodiscard]] bool isCompressionEnabled() const;

    /**
     * @brief Trigger background compression scan
     * @return Number of items queued for compression
     */
    [[nodiscard]] Result<size_t> triggerCompressionScan();

    /**
     * @brief Wait for all async operations to complete
     * @param timeout Maximum time to wait
     * @return True if all operations completed
     */
    [[nodiscard]] bool
    waitForAsyncOperations(std::chrono::milliseconds timeout = std::chrono::milliseconds::max());

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

} // namespace yams::storage
