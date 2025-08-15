#pragma once

#include <functional>
#include <memory>
#include <yams/api/content_metadata.h>
#include <yams/compression/compression_policy.h>
#include <yams/compression/compression_stats.h>
#include <yams/compression/compressor_interface.h>
#include <yams/core/types.h>
#include <yams/storage/storage_engine.h>

namespace yams::compression {

/**
 * @brief Cache entry for decompressed data
 */
struct DecompressionCacheEntry {
    std::vector<std::byte> data;
    std::chrono::steady_clock::time_point lastAccess;
    size_t accessCount{1};
    size_t compressedSize{0};

    /**
     * @brief Update access time and count
     */
    void touch() {
        lastAccess = std::chrono::steady_clock::now();
        ++accessCount;
    }
};

/**
 * @brief Configuration for compression layer
 */
struct CompressionLayerConfig {
    CompressionPolicy policy;                              ///< Compression policy rules
    size_t decompressionCacheSize{100};                    ///< Max cached decompressed items
    size_t decompressionCacheSizeBytes{512 * 1024 * 1024}; ///< Max cache size (512MB)
    size_t workerThreads{4};                               ///< Background compression threads
    bool enableBackgroundCompression{true};                ///< Enable background compression
    bool enableAdaptiveCompression{true};                  ///< Adapt to system load
    std::chrono::seconds metricsInterval{60};              ///< Metrics collection interval
};

/**
 * @brief Transparent compression layer for storage operations
 *
 * This class provides transparent compression and decompression for the storage
 * layer. It intercepts store/retrieve operations and applies compression based
 * on the configured policy.
 */
class CompressionLayer {
public:
    /**
     * @brief Callback for compression events
     */
    using CompressionCallback = std::function<void(
        const std::string& hash, const CompressionResult& result, const std::string& reason)>;

    /**
     * @brief Construct compression layer
     * @param storage Underlying storage engine
     * @param config Layer configuration
     */
    CompressionLayer(std::shared_ptr<storage::StorageEngine> storage,
                     CompressionLayerConfig config = {});

    ~CompressionLayer();

    /**
     * @brief Store data with automatic compression
     * @param hash Content hash
     * @param data Raw data to store
     * @param metadata Content metadata
     * @return Success or error
     */
    [[nodiscard]] Result<void> store(const std::string& hash, std::span<const std::byte> data,
                                     const api::ContentMetadata& metadata);

    /**
     * @brief Retrieve data with automatic decompression
     * @param hash Content hash
     * @return Decompressed data or error
     */
    [[nodiscard]] Result<std::vector<std::byte>> retrieve(const std::string& hash);

    /**
     * @brief Check if content exists
     * @param hash Content hash
     * @return True if exists
     */
    [[nodiscard]] Result<bool> exists(const std::string& hash) const;

    /**
     * @brief Remove content
     * @param hash Content hash
     * @return Success or error
     */
    [[nodiscard]] Result<bool> remove(const std::string& hash);

    /**
     * @brief Get storage statistics including compression
     * @return Current statistics
     */
    [[nodiscard]] Result<CompressionStats> getStats() const;

    /**
     * @brief Manually compress existing content
     * @param hash Content hash
     * @param algorithm Algorithm to use (or None for policy decision)
     * @param level Compression level (0 for default)
     * @return Compression result or error
     */
    [[nodiscard]] Result<CompressionResult>
    compressExisting(const std::string& hash,
                     CompressionAlgorithm algorithm = CompressionAlgorithm::None,
                     uint8_t level = 0);

    /**
     * @brief Manually decompress existing content
     * @param hash Content hash
     * @return Success or error
     */
    [[nodiscard]] Result<void> decompressExisting(const std::string& hash);

    /**
     * @brief Start background compression based on policy
     * @return Success or error
     */
    [[nodiscard]] Result<void> startBackgroundCompression();

    /**
     * @brief Stop background compression
     */
    void stopBackgroundCompression();

    /**
     * @brief Check if background compression is running
     * @return True if running
     */
    [[nodiscard]] bool isBackgroundCompressionRunning() const;

    /**
     * @brief Register callback for compression events
     * @param callback Function to call on compression
     */
    void onCompression(CompressionCallback callback);

    /**
     * @brief Clear decompression cache
     */
    void clearCache();

    /**
     * @brief Get cache statistics
     * @return Cache hit rate and size info
     */
    struct CacheStats {
        size_t entries{0};
        size_t totalBytes{0};
        size_t hits{0};
        size_t misses{0};
        double hitRate{0.0};
    };
    [[nodiscard]] CacheStats getCacheStats() const;

    /**
     * @brief Update compression policy
     * @param policy New policy rules
     */
    void updatePolicy(CompressionPolicy policy);

    /**
     * @brief Force compression of all uncompressed content
     * @param progressCallback Optional progress callback
     * @return Number of items compressed or error
     */
    [[nodiscard]] Result<size_t>
    compressAll(std::function<void(size_t current, size_t total)> progressCallback = nullptr);

    /**
     * @brief Analyze potential compression savings
     * @return Analysis results
     */
    struct CompressionAnalysis {
        size_t uncompressedFiles{0};
        size_t uncompressedBytes{0};
        size_t estimatedSavings{0};
        std::unordered_map<std::string, size_t> savingsByType;
    };
    [[nodiscard]] Result<CompressionAnalysis> analyzeCompressionPotential() const;

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * @brief RAII guard for temporary decompression
 *
 * Ensures decompressed data is cleaned up after use
 */
class DecompressionGuard {
public:
    /**
     * @brief Construct guard with decompressed data
     * @param data Decompressed data
     * @param layer Compression layer (for cache management)
     * @param hash Content hash
     */
    DecompressionGuard(std::vector<std::byte> data, CompressionLayer* layer,
                       const std::string& hash);

    ~DecompressionGuard();

    // Move-only
    DecompressionGuard(DecompressionGuard&&) = default;
    DecompressionGuard& operator=(DecompressionGuard&&) = default;
    DecompressionGuard(const DecompressionGuard&) = delete;
    DecompressionGuard& operator=(const DecompressionGuard&) = delete;

    /**
     * @brief Get decompressed data
     * @return Data span
     */
    [[nodiscard]] std::span<const std::byte> data() const noexcept { return data_; }

    /**
     * @brief Get data size
     * @return Size in bytes
     */
    [[nodiscard]] size_t size() const noexcept { return data_.size(); }

    /**
     * @brief Release data ownership
     * @return Data vector
     */
    [[nodiscard]] std::vector<std::byte> release() {
        released_ = true;
        return std::move(data_);
    }

private:
    std::vector<std::byte> data_;
    CompressionLayer* layer_;
    std::string hash_;
    bool released_{false};
};

} // namespace yams::compression