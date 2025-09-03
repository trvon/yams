#pragma once

#include <chrono>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <yams/api/content_metadata.h>
#include <yams/compression/compressor_interface.h>
#include <yams/core/types.h>

namespace yams::compression {

/**
 * @brief Access pattern information for content
 */
struct AccessPattern {
    std::chrono::system_clock::time_point lastAccessed;
    std::chrono::system_clock::time_point created;
    size_t accessCount{0};
    size_t readCount{0};
    size_t writeCount{0};
    std::chrono::milliseconds avgAccessDuration{0};

    /**
     * @brief Calculate age since last access
     * @return Duration since last access
     */
    [[nodiscard]] std::chrono::hours ageSinceAccess() const {
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::hours>(now - lastAccessed);
    }

    /**
     * @brief Calculate total age
     * @return Duration since creation
     */
    [[nodiscard]] std::chrono::hours totalAge() const {
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::hours>(now - created);
    }

    /**
     * @brief Calculate access frequency
     * @return Accesses per day
     */
    [[nodiscard]] double accessFrequency() const {
        auto age = totalAge();
        if (age.count() == 0)
            return static_cast<double>(accessCount);
        return static_cast<double>(accessCount) / (static_cast<double>(age.count()) / 24.0);
    }
};

/**
 * @brief Decision about whether and how to compress
 */
struct CompressionDecision {
    bool shouldCompress{false};
    CompressionAlgorithm algorithm{CompressionAlgorithm::None};
    uint8_t level{0};
    std::string reason;

    /**
     * @brief Create a "don't compress" decision
     * @param reason Why compression was skipped
     * @return Decision object
     */
    [[nodiscard]] static CompressionDecision dontCompress(std::string reason) {
        return {false, CompressionAlgorithm::None, 0, std::move(reason)};
    }

    /**
     * @brief Create a "compress" decision
     * @param algo Algorithm to use
     * @param level Compression level
     * @param reason Why this decision was made
     * @return Decision object
     */
    [[nodiscard]] static CompressionDecision compress(CompressionAlgorithm algo, uint8_t level,
                                                      std::string reason) {
        return {true, algo, level, std::move(reason)};
    }
};

/**
 * @brief Policy engine for compression decisions
 */
class CompressionPolicy {
public:
    /**
     * @brief Configuration rules for compression policy
     */
    struct Rules {
        // Age-based rules
        std::chrono::hours compressAfterAge{std::chrono::hours{24LL} * 7LL}; ///< Compress after 1 week
        std::chrono::hours archiveAfterAge{std::chrono::hours{24LL} * 30LL}; ///< Use LZMA after 30 days
        std::chrono::hours neverCompressBefore{1};   ///< Don't compress very new files

        // Size-based rules
        size_t alwaysCompressAbove{static_cast<size_t>(10) * static_cast<size_t>(1024) * static_cast<size_t>(1024)}; ///< Always compress >10MB
        size_t neverCompressBelow{4096};              ///< Never compress <4KB
        size_t preferZstdBelow{static_cast<size_t>(50) * static_cast<size_t>(1024) * static_cast<size_t>(1024)};     ///< Use Zstd for <50MB

        // Type-based rules
        std::unordered_set<std::string> compressibleTypes{"text/plain",
                                                          "text/html",
                                                          "text/css",
                                                          "text/javascript",
                                                          "application/json",
                                                          "application/xml",
                                                          "application/javascript",
                                                          "application/x-yaml",
                                                          "text/markdown",
                                                          "text/csv"};

        std::unordered_set<std::string> excludedTypes{"image/jpeg",
                                                      "image/png",
                                                      "image/webp",
                                                      "image/gif",
                                                      "video/mp4",
                                                      "video/webm",
                                                      "video/x-matroska",
                                                      "audio/mpeg",
                                                      "audio/ogg",
                                                      "audio/wav",
                                                      "application/zip",
                                                      "application/x-7z-compressed",
                                                      "application/x-rar-compressed"};

        // Extension-based rules (fallback when MIME type unavailable)
        std::unordered_set<std::string> compressibleExtensions{
            ".txt",  ".log", ".json", ".xml", ".html", ".css", ".js", ".md",
            ".yaml", ".yml", ".csv",  ".sql", ".py",   ".cpp", ".h"};

        std::unordered_set<std::string> excludedExtensions{".jpg", ".jpeg", ".png", ".gif", ".mp4",
                                                           ".mp3", ".zip",  ".gz",  ".bz2", ".7z",
                                                           ".rar", ".webm", ".mkv", ".avi"};

        // Performance rules
        double maxCpuUsage{0.5};                      ///< Max 50% CPU for background
        size_t maxConcurrentCompressions{4};          ///< Max parallel compressions
        size_t minFreeSpaceBytes{static_cast<size_t>(1024) * static_cast<size_t>(1024) * static_cast<size_t>(1024)}; ///< Need 1GB free space

        // Algorithm preferences
        uint8_t defaultZstdLevel{3}; ///< Fast compression
        uint8_t archiveZstdLevel{9}; ///< Balanced for archives
        uint8_t defaultLzmaLevel{6}; ///< LZMA default

        // Access pattern rules
        double hotFileAccessesPerDay{10.0}; ///< "Hot" file threshold
        double coldFileAccessesPerDay{0.1}; ///< "Cold" file threshold
    };

    /**
     * @brief Construct with default rules
     */
    CompressionPolicy() = default;

    /**
     * @brief Construct with custom rules
     * @param rules Custom rule set
     */
    explicit CompressionPolicy(Rules rules);

    /**
     * @brief Determine if content should be compressed
     * @param metadata Content metadata
     * @param pattern Access pattern information
     * @return Compression decision
     * @note Thread-safe - uses shared lock for reading rules
     */
    [[nodiscard]] CompressionDecision shouldCompress(const api::ContentMetadata& metadata,
                                                     const AccessPattern& pattern) const;

    /**
     * @brief Select algorithm for content
     * @param metadata Content metadata
     * @param pattern Access pattern information
     * @return Selected algorithm
     */
    [[nodiscard]] CompressionAlgorithm selectAlgorithm(const api::ContentMetadata& metadata,
                                                       const AccessPattern& pattern) const;

    /**
     * @brief Select compression level
     * @param algo Selected algorithm
     * @param metadata Content metadata
     * @param pattern Access pattern information
     * @return Compression level
     */
    [[nodiscard]] uint8_t selectLevel(CompressionAlgorithm algo,
                                      const api::ContentMetadata& metadata,
                                      const AccessPattern& pattern) const;

    /**
     * @brief Check if type is compressible
     * @param mimeType MIME type to check
     * @param filename Filename (for extension check)
     * @return True if type should be compressed
     */
    [[nodiscard]] bool isCompressibleType(const std::string& mimeType,
                                          std::string_view filename) const;

    /**
     * @brief Get current rules
     * @return Copy of current rule set
     * @note Returns a copy for thread safety
     */
    [[nodiscard]] Rules rules() const {
        std::lock_guard lock(rulesMutex_);
        return rules_;
    }

    /**
     * @brief Update rules
     * @param rules New rule set
     * @note Thread-safe - uses unique lock for writing rules
     */
    void updateRules(Rules rules) {
        std::unique_lock lock(rulesMutex_);
        rules_ = std::move(rules);
    }

    /**
     * @brief Check system resources
     * @return True if system has resources for compression
     */
    [[nodiscard]] bool hasSystemResources() const;

private:
    Rules rules_;
    mutable std::mutex rulesMutex_; ///< Protects rules_ for thread safety

    /**
     * @brief Get file extension from filename
     * @param filename File name
     * @return Extension (lowercase) or empty string
     */
    [[nodiscard]] static std::string getExtension(const std::string& filename);

    /**
     * @brief Check if content is already compressed
     * @param metadata Content metadata
     * @return True if content appears to be compressed
     */
    [[nodiscard]] bool isAlreadyCompressed(const api::ContentMetadata& metadata) const;

    /**
     * @brief Classify file temperature (hot/warm/cold)
     * @param pattern Access pattern
     * @return Temperature classification
     */
    enum class FileTemperature { Hot, Warm, Cold };
    [[nodiscard]] FileTemperature classifyTemperature(const AccessPattern& pattern) const;
};

/**
 * @brief Policy-based compression scheduler
 *
 * Schedules background compression based on policy rules
 */
class CompressionScheduler {
public:
    /**
     * @brief Configuration for scheduler
     */
    struct Config {
        std::shared_ptr<CompressionPolicy> policy;
        std::chrono::minutes scanInterval{60};     ///< How often to scan
        size_t batchSize{100};                     ///< Files per batch
        std::chrono::milliseconds batchDelay{100}; ///< Delay between batches
    };

    explicit CompressionScheduler(Config config);
    ~CompressionScheduler();

    /**
     * @brief Start background scheduling
     * @return Success or error
     */
    [[nodiscard]] Result<void> start();

    /**
     * @brief Stop background scheduling
     */
    void stop();

    /**
     * @brief Check if scheduler is running
     * @return True if running
     */
    [[nodiscard]] bool isRunning() const noexcept;

    /**
     * @brief Force immediate scan
     * @return Number of files scheduled for compression
     */
    [[nodiscard]] Result<size_t> scanNow();

    /**
     * @brief Register callback for compression events
     * @param callback Function to call when files are compressed
     */
    void onCompression(std::function<void(const std::string&, CompressionResult)> callback);

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

} // namespace yams::compression