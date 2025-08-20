#pragma once

#include <atomic>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <string>
#include <unordered_map>
#include <yams/compression/compressor_interface.h>

namespace yams::compression {

/**
 * @brief Statistics for a specific compression algorithm
 */
struct AlgorithmStats {
    // Default constructor
    AlgorithmStats() = default;

    // Copy constructor
    AlgorithmStats(const AlgorithmStats& other) {
        filesCompressed.store(other.filesCompressed.load());
        filesDecompressed.store(other.filesDecompressed.load());
        bytesInput.store(other.bytesInput.load());
        bytesOutput.store(other.bytesOutput.load());
        compressionTimeMs.store(other.compressionTimeMs.load());
        decompressionTimeMs.store(other.decompressionTimeMs.load());
        compressionErrors.store(other.compressionErrors.load());
        decompressionErrors.store(other.decompressionErrors.load());
    }

    // Copy assignment
    AlgorithmStats& operator=(const AlgorithmStats& other) {
        if (this != &other) {
            filesCompressed.store(other.filesCompressed.load());
            filesDecompressed.store(other.filesDecompressed.load());
            bytesInput.store(other.bytesInput.load());
            bytesOutput.store(other.bytesOutput.load());
            compressionTimeMs.store(other.compressionTimeMs.load());
            decompressionTimeMs.store(other.decompressionTimeMs.load());
            compressionErrors.store(other.compressionErrors.load());
            decompressionErrors.store(other.decompressionErrors.load());
        }
        return *this;
    }

    std::atomic<uint64_t> filesCompressed{0};     ///< Number of files compressed
    std::atomic<uint64_t> filesDecompressed{0};   ///< Number of files decompressed
    std::atomic<uint64_t> bytesInput{0};          ///< Total input bytes
    std::atomic<uint64_t> bytesOutput{0};         ///< Total output bytes
    std::atomic<uint64_t> compressionTimeMs{0};   ///< Total compression time
    std::atomic<uint64_t> decompressionTimeMs{0}; ///< Total decompression time
    std::atomic<uint64_t> compressionErrors{0};   ///< Compression failures
    std::atomic<uint64_t> decompressionErrors{0}; ///< Decompression failures

    /**
     * @brief Calculate average compression ratio
     * @return Average ratio
     */
    [[nodiscard]] double averageRatio() const noexcept {
        uint64_t input = bytesInput.load();
        uint64_t output = bytesOutput.load();
        return output > 0 ? static_cast<double>(input) / output : 0.0;
    }

    /**
     * @brief Calculate average compression speed
     * @return MB/s throughput
     */
    [[nodiscard]] double compressionThroughputMBps() const noexcept {
        uint64_t bytes = bytesInput.load();
        uint64_t ms = compressionTimeMs.load();
        if (ms == 0)
            return 0.0;
        return (bytes / 1024.0 / 1024.0) / (ms / 1000.0);
    }

    /**
     * @brief Calculate average decompression speed
     * @return MB/s throughput
     */
    [[nodiscard]] double decompressionThroughputMBps() const noexcept {
        uint64_t bytes = bytesOutput.load();
        uint64_t ms = decompressionTimeMs.load();
        if (ms == 0)
            return 0.0;
        return (bytes / 1024.0 / 1024.0) / (ms / 1000.0);
    }

    /**
     * @brief Update with compression result
     * @param result Compression result
     */
    void recordCompression(const CompressionResult& result) {
        filesCompressed++;
        bytesInput += result.originalSize;
        bytesOutput += result.compressedSize;
        compressionTimeMs +=
            static_cast<uint64_t>((result.duration.count() < 0) ? 0L : result.duration.count());
    }

    /**
     * @brief Update with decompression timing
     * @param inputSize Compressed size
     * @param outputSize Decompressed size
     * @param duration Time taken
     */
    void recordDecompression(size_t /*inputSize*/, size_t /*outputSize*/,
                             std::chrono::milliseconds duration) {
        filesDecompressed++;
        decompressionTimeMs +=
            static_cast<uint64_t>((duration.count() < 0) ? 0L : duration.count());
    }
};

/**
 * @brief Comprehensive compression statistics
 */
struct CompressionStats {
    // Default constructor
    CompressionStats() = default;

    // Copy constructor (deep copy atomics)
    CompressionStats(const CompressionStats& other) { copyFrom(other); }

    // Copy assignment
    CompressionStats& operator=(const CompressionStats& other) {
        if (this != &other) {
            copyFrom(other);
        }
        return *this;
    }

    // Move constructor
    CompressionStats(CompressionStats&& other) noexcept { moveFrom(std::move(other)); }

    // Move assignment
    CompressionStats& operator=(CompressionStats&& other) noexcept {
        if (this != &other) {
            moveFrom(std::move(other));
        }
        return *this;
    }

    // Overall statistics
    std::atomic<uint64_t> totalCompressedFiles{0};
    std::atomic<uint64_t> totalUncompressedFiles{0};
    std::atomic<uint64_t> totalCompressedBytes{0};
    std::atomic<uint64_t> totalUncompressedBytes{0};
    std::atomic<uint64_t> totalSpaceSaved{0};

    // Per-algorithm statistics
    std::unordered_map<CompressionAlgorithm, AlgorithmStats> algorithmStats;

    // Performance metrics
    std::atomic<uint64_t> cacheHits{0};
    std::atomic<uint64_t> cacheMisses{0};
    std::atomic<uint64_t> cacheEvictions{0};
    std::atomic<uint64_t> backgroundCompressions{0};
    std::atomic<uint64_t> onDemandCompressions{0};

    // Resource usage
    std::atomic<uint64_t> peakMemoryUsageBytes{0};
    std::atomic<uint64_t> currentWorkerThreads{0};
    std::atomic<uint64_t> queuedCompressions{0};

    // Timing
    std::chrono::system_clock::time_point startTime{std::chrono::system_clock::now()};
    std::chrono::system_clock::time_point lastResetTime{std::chrono::system_clock::now()};

    /**
     * @brief Calculate overall compression ratio
     * @return Average compression ratio across all algorithms
     */
    [[nodiscard]] double overallCompressionRatio() const noexcept {
        uint64_t saved = totalSpaceSaved.load();
        uint64_t original = totalUncompressedBytes.load() + saved;
        return original > 0 ? static_cast<double>(original) / totalCompressedBytes.load() : 0.0;
    }

    /**
     * @brief Calculate cache hit rate
     * @return Hit rate as percentage
     */
    [[nodiscard]] double cacheHitRate() const noexcept {
        uint64_t hits = cacheHits.load();
        uint64_t total = hits + cacheMisses.load();
        return total > 0 ? (static_cast<double>(hits) / total) * 100.0 : 0.0;
    }

    /**
     * @brief Format statistics as human-readable report
     * @return Formatted report string
     */
    [[nodiscard]] std::string formatReport() const {
        std::ostringstream oss;
        oss << "=== Compression Statistics ===\n\n";

        // Overall stats
        oss << "Overall Statistics:\n";
        oss << "  Compressed files:    " << totalCompressedFiles.load() << "\n";
        oss << "  Uncompressed files:  " << totalUncompressedFiles.load() << "\n";
        oss << "  Space saved:         " << formatBytes(totalSpaceSaved.load()) << "\n";
        oss << "  Compression ratio:   " << std::fixed << std::setprecision(2)
            << overallCompressionRatio() << ":1\n\n";

        // Per-algorithm stats
        oss << "Algorithm Performance:\n";
        for (const auto& [algo, stats] : algorithmStats) {
            oss << "  " << algorithmName(algo) << ":\n";
            oss << "    Files compressed:  " << stats.filesCompressed.load() << "\n";
            oss << "    Average ratio:     " << std::fixed << std::setprecision(2)
                << stats.averageRatio() << ":1\n";
            oss << "    Compression speed: " << std::fixed << std::setprecision(1)
                << stats.compressionThroughputMBps() << " MB/s\n";
            oss << "    Decompression:     " << std::fixed << std::setprecision(1)
                << stats.decompressionThroughputMBps() << " MB/s\n";
        }
        oss << "\n";

        // Cache stats
        oss << "Cache Performance:\n";
        oss << "  Hit rate:     " << std::fixed << std::setprecision(1) << cacheHitRate() << "%\n";
        oss << "  Total hits:   " << cacheHits.load() << "\n";
        oss << "  Total misses: " << cacheMisses.load() << "\n";
        oss << "  Evictions:    " << cacheEvictions.load() << "\n\n";

        // Resource usage
        oss << "Resource Usage:\n";
        oss << "  Peak memory:     " << formatBytes(peakMemoryUsageBytes.load()) << "\n";
        oss << "  Worker threads:  " << currentWorkerThreads.load() << "\n";
        oss << "  Queued jobs:     " << queuedCompressions.load() << "\n";

        auto uptime = std::chrono::system_clock::now() - startTime;
        auto hours = std::chrono::duration_cast<std::chrono::hours>(uptime).count();
        oss << "  Uptime:          " << hours << " hours\n";

        return oss.str();
    }

    /**
     * @brief Export statistics in Prometheus format
     * @return Prometheus-formatted metrics
     */
    [[nodiscard]] std::string exportPrometheus() const {
        std::ostringstream oss;

        // Overall metrics
        oss << "# HELP yams_compression_files_total Total compressed files\n";
        oss << "# TYPE yams_compression_files_total counter\n";
        oss << "yams_compression_files_total " << totalCompressedFiles.load() << "\n\n";

        oss << "# HELP yams_compression_bytes_saved_total Total bytes saved\n";
        oss << "# TYPE yams_compression_bytes_saved_total counter\n";
        oss << "yams_compression_bytes_saved_total " << totalSpaceSaved.load() << "\n\n";

        oss << "# HELP yams_compression_ratio Overall compression ratio\n";
        oss << "# TYPE yams_compression_ratio gauge\n";
        oss << "yams_compression_ratio " << overallCompressionRatio() << "\n\n";

        // Per-algorithm metrics
        oss << "# HELP yams_compression_algorithm_operations Operations by algorithm\n";
        oss << "# TYPE yams_compression_algorithm_operations counter\n";
        for (const auto& [algo, stats] : algorithmStats) {
            std::string name = algorithmName(algo);
            oss << "yams_compression_algorithm_operations{algorithm=\"" << name
                << "\",operation=\"compress\"} " << stats.filesCompressed.load() << "\n";
            oss << "yams_compression_algorithm_operations{algorithm=\"" << name
                << "\",operation=\"decompress\"} " << stats.filesDecompressed.load() << "\n";
        }
        oss << "\n";

        // Cache metrics
        oss << "# HELP yams_compression_cache_hits_total Cache hit count\n";
        oss << "# TYPE yams_compression_cache_hits_total counter\n";
        oss << "yams_compression_cache_hits_total " << cacheHits.load() << "\n\n";

        oss << "# HELP yams_compression_cache_hit_rate Cache hit rate\n";
        oss << "# TYPE yams_compression_cache_hit_rate gauge\n";
        oss << "yams_compression_cache_hit_rate " << cacheHitRate() / 100.0 << "\n\n";

        return oss.str();
    }

    /**
     * @brief Reset all statistics
     */
    void reset() {
        totalCompressedFiles = 0;
        totalUncompressedFiles = 0;
        totalCompressedBytes = 0;
        totalUncompressedBytes = 0;
        totalSpaceSaved = 0;

        algorithmStats.clear();

        cacheHits = 0;
        cacheMisses = 0;
        cacheEvictions = 0;
        backgroundCompressions = 0;
        onDemandCompressions = 0;

        peakMemoryUsageBytes = 0;
        queuedCompressions = 0;

        lastResetTime = std::chrono::system_clock::now();
    }

private:
    /**
     * @brief Copy from another CompressionStats
     * @param other Source stats
     */
    void copyFrom(const CompressionStats& other) {
        totalCompressedFiles.store(other.totalCompressedFiles.load());
        totalUncompressedFiles.store(other.totalUncompressedFiles.load());
        totalCompressedBytes.store(other.totalCompressedBytes.load());
        totalUncompressedBytes.store(other.totalUncompressedBytes.load());
        totalSpaceSaved.store(other.totalSpaceSaved.load());

        algorithmStats = other.algorithmStats;

        cacheHits.store(other.cacheHits.load());
        cacheMisses.store(other.cacheMisses.load());
        cacheEvictions.store(other.cacheEvictions.load());
        backgroundCompressions.store(other.backgroundCompressions.load());
        onDemandCompressions.store(other.onDemandCompressions.load());

        peakMemoryUsageBytes.store(other.peakMemoryUsageBytes.load());
        currentWorkerThreads.store(other.currentWorkerThreads.load());
        queuedCompressions.store(other.queuedCompressions.load());

        startTime = other.startTime;
        lastResetTime = other.lastResetTime;
    }

    /**
     * @brief Move from another CompressionStats
     * @param other Source stats
     */
    void moveFrom(CompressionStats&& other) {
        totalCompressedFiles.store(other.totalCompressedFiles.exchange(0));
        totalUncompressedFiles.store(other.totalUncompressedFiles.exchange(0));
        totalCompressedBytes.store(other.totalCompressedBytes.exchange(0));
        totalUncompressedBytes.store(other.totalUncompressedBytes.exchange(0));
        totalSpaceSaved.store(other.totalSpaceSaved.exchange(0));

        algorithmStats = std::move(other.algorithmStats);

        cacheHits.store(other.cacheHits.exchange(0));
        cacheMisses.store(other.cacheMisses.exchange(0));
        cacheEvictions.store(other.cacheEvictions.exchange(0));
        backgroundCompressions.store(other.backgroundCompressions.exchange(0));
        onDemandCompressions.store(other.onDemandCompressions.exchange(0));

        peakMemoryUsageBytes.store(other.peakMemoryUsageBytes.exchange(0));
        currentWorkerThreads.store(other.currentWorkerThreads.exchange(0));
        queuedCompressions.store(other.queuedCompressions.exchange(0));

        startTime = other.startTime;
        lastResetTime = other.lastResetTime;
    }

    /**
     * @brief Format bytes as human-readable string
     * @param bytes Number of bytes
     * @return Formatted string
     */
    [[nodiscard]] static std::string formatBytes(uint64_t bytes) {
        const char* units[] = {"B", "KB", "MB", "GB", "TB"};
        int unit = 0;
        double size = static_cast<double>(bytes);

        while (size >= 1024.0 && unit < 4) {
            size /= 1024.0;
            unit++;
        }

        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2) << size << " " << units[unit];
        return oss.str();
    }

    /**
     * @brief Get algorithm name as string
     * @param algo Algorithm enum
     * @return Algorithm name
     */
    [[nodiscard]] static std::string algorithmName(CompressionAlgorithm algo) {
        switch (algo) {
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
};

} // namespace yams::compression