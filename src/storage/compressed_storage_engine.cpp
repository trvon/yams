#include <spdlog/spdlog.h>
#include <condition_variable>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <zstd.h>
#include <yams/compression/compression_header.h>
#include <yams/compression/compression_stats.h>
#include <yams/compression/compressor_interface.h>
#include <yams/storage/compressed_storage_engine.h>

namespace yams::storage {

// Internal utilities
namespace {

bool isCompressedData(std::span<const std::byte> data) {
    if (data.size() < compression::CompressionHeader::SIZE) {
        return false;
    }

    compression::CompressionHeader header;
    std::memcpy(&header, data.data(), sizeof(header));
    return header.magic == compression::CompressionHeader::MAGIC;
}

uint32_t calculateCRC32(std::span<const std::byte> data) {
    // Simple CRC32 implementation - in production use a proper CRC32 library
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < data.size(); ++i) {
        crc ^= static_cast<uint8_t>(data[i]);
        for (int j = 0; j < 8; ++j) {
            crc = (crc >> 1) ^ ((crc & 1) ? 0xEDB88320 : 0);
        }
    }
    return ~crc;
}

} // anonymous namespace

/**
 * @brief Implementation class for CompressedStorageEngine
 */
class CompressedStorageEngine::Impl {
public:
    Impl(std::shared_ptr<StorageEngine> underlying, Config config)
        : underlying_(std::move(underlying)), config_(std::move(config)),
          policy_(config_.policyRules), compressionEnabled_(config_.enableCompression),
          shutdownFlag_(false) {
        if (config_.asyncCompression) {
            startAsyncWorkers();
        }
    }

    ~Impl() { shutdown(); }

    Result<void> store(std::string_view hash, std::span<const std::byte> data) {
        if (!compressionEnabled_ || data.size() < config_.compressionThreshold) {
            return underlying_->store(hash, data);
        }

        // Simplified policy decision - compress if above threshold
        struct SimpleDecision {
            bool shouldCompress = true;
            compression::CompressionAlgorithm algorithm =
                compression::CompressionAlgorithm::Zstandard;
            uint8_t level = 3;
        };

        SimpleDecision decision;
        if (data.size() < config_.compressionThreshold) {
            decision.shouldCompress = false;
        }

        if (!decision.shouldCompress) {
            updateStats([&](compression::CompressionStats& stats) {
                stats.totalUncompressedFiles++;
                stats.totalUncompressedBytes += data.size();
            });
            return underlying_->store(hash, data);
        }

        // Compress data
        auto compressedResult = compressData(data, decision);
        if (!compressedResult) {
            spdlog::warn("Compression failed for hash {}, storing uncompressed: {}", hash,
                         compressedResult.error().message);
            return underlying_->store(hash, data);
        }

        // Store compressed data
        std::span<const std::byte> compressedSpan{
            reinterpret_cast<const std::byte*>(compressedResult.value().data()),
            compressedResult.value().size()};
        return underlying_->store(hash, compressedSpan);
    }

    Result<std::vector<std::byte>> retrieve(std::string_view hash) const {
        auto result = underlying_->retrieve(hash);
        if (!result) {
            return result;
        }

        std::span<const std::byte> data{result.value().data(), result.value().size()};

        // Check if data is compressed
        if (!isCompressedData(data)) {
            return result;
        }

        // Decompress data
        return decompressData(data);
    }

    Result<bool> exists(std::string_view hash) const noexcept { return underlying_->exists(hash); }

    Result<void> remove(std::string_view hash) { return underlying_->remove(hash); }

    Result<uint64_t> size(std::string_view hash) {
        auto dataResult = underlying_->retrieve(hash);
        if (!dataResult) {
            if (dataResult.error().code == ErrorCode::NotFound) {
                return Error(ErrorCode::NotFound);
            }
            return dataResult.error();
        }

        std::span<const std::byte> data{dataResult.value().data(), dataResult.value().size()};

        // If compressed, return original size
        if (isCompressedData(data)) {
            compression::CompressionHeader header;
            std::memcpy(&header, data.data(), sizeof(header));
            return header.uncompressedSize;
        }

        return data.size();
    }

    // Note: list(), stats(), and compact() methods removed as they're not part of the StorageEngine
    // interface

    StorageStats getStats() const noexcept { return underlying_->getStats(); }

    Result<uint64_t> getStorageSize() const { return underlying_->getStorageSize(); }

    // Async operations - delegate to underlying storage
    std::future<Result<void>> storeAsync(std::string_view hash, std::span<const std::byte> data) {
        // For simplicity, run synchronously in thread - could be optimized
        return std::async(std::launch::async,
                          [this, hash = std::string(hash),
                           data = std::vector<std::byte>(data.begin(), data.end())] {
                              return this->store(hash, std::span<const std::byte>(data));
                          });
    }

    std::future<Result<std::vector<std::byte>>> retrieveAsync(std::string_view hash) const {
        return std::async(std::launch::async,
                          [this, hash = std::string(hash)] { return this->retrieve(hash); });
    }

    // Batch operations
    std::vector<Result<void>>
    storeBatch(const std::vector<std::pair<std::string, std::vector<std::byte>>>& items) {
        std::vector<Result<void>> results;
        results.reserve(items.size());
        for (const auto& [hash, data] : items) {
            results.push_back(store(hash, std::span<const std::byte>(data)));
        }
        return results;
    }

    compression::CompressionStats getCompressionStats() const {
        std::lock_guard lock(statsMutex_);
        return compressionStats_;
    }

    Result<void> compressExisting(std::string_view hash, bool force) {
        auto dataResult = underlying_->retrieve(hash);
        if (!dataResult) {
            return dataResult.error();
        }

        std::span<const std::byte> data{dataResult.value().data(), dataResult.value().size()};

        // Check if already compressed
        if (isCompressedData(data)) {
            return Error(ErrorCode::InvalidState, "Data is already compressed");
        }

        // Create metadata for policy
        api::ContentMetadata metadata{.id = std::string(hash),
                                      .name = "",
                                      .size = data.size(),
                                      .mimeType = "application/octet-stream",
                                      .contentHash = std::string(hash),
                                      .createdAt = std::chrono::system_clock::now(),
                                      .modifiedAt = std::chrono::system_clock::now(),
                                      .accessedAt = std::chrono::system_clock::now(),
                                      .tags = {}};

        compression::AccessPattern pattern{.lastAccessed = metadata.createdAt,
                                           .created = metadata.createdAt,
                                           .accessCount = 1,
                                           .readCount = 0,
                                           .writeCount = 0};

        // Check policy or force
        compression::CompressionDecision decision;
        if (force) {
            decision.shouldCompress = true;
            decision.algorithm = compression::CompressionAlgorithm::Zstandard;
            decision.level = 3;
        } else {
            decision = policy_.shouldCompress(metadata, pattern);
            if (!decision.shouldCompress) {
                return Error(ErrorCode::InvalidState, "Policy rejects compression");
            }
        }

        // Compress
        auto compressedResult = compressData(data, decision);
        if (!compressedResult) {
            return compressedResult.error();
        }

        // Replace with compressed version
        std::span<const std::byte> compressedSpan{
            reinterpret_cast<const std::byte*>(compressedResult.value().data()),
            compressedResult.value().size()};
        return underlying_->store(hash, compressedSpan);
    }

    Result<void> decompressExisting(std::string_view hash) {
        auto dataResult = underlying_->retrieve(hash);
        if (!dataResult) {
            return dataResult.error();
        }

        std::span<const std::byte> data{dataResult.value().data(), dataResult.value().size()};

        // Check if compressed
        if (!isCompressedData(data)) {
            return Error(ErrorCode::InvalidState, "Data is not compressed");
        }

        // Decompress
        auto decompressedResult = decompressData(data);
        if (!decompressedResult) {
            return decompressedResult.error();
        }

        // Replace with decompressed version
        std::span<const std::byte> decompressedSpan{
            reinterpret_cast<const std::byte*>(decompressedResult.value().data()),
            decompressedResult.value().size()};
        return underlying_->store(hash, decompressedSpan);
    }

    void setPolicyRules(compression::CompressionPolicy::Rules rules) {
        policy_.updateRules(std::move(rules));
    }

    compression::CompressionPolicy::Rules getPolicyRules() const {
        // getRules method not exposed, return default rules
        return compression::CompressionPolicy::Rules{};
    }

    void setCompressionEnabled(bool enable) { compressionEnabled_ = enable; }

    bool isCompressionEnabled() const { return compressionEnabled_; }

    Result<size_t> triggerCompressionScan() {
        if (!config_.asyncCompression) {
            return Error(ErrorCode::InvalidState, "Async compression is disabled");
        }

        // Note: list() method not available in StorageEngine interface
        // This functionality would require additional interface methods
        return Error(ErrorCode::NotImplemented,
                     "Background compression scan requires list() functionality");
    }

    bool waitForAsyncOperations(std::chrono::milliseconds timeout) {
        std::unique_lock lock(asyncMutex_);
        return asyncCV_.wait_for(lock, timeout,
                                 [this] { return asyncQueue_.empty() && activeJobs_ == 0; });
    }

private:
    struct AsyncJob {
        enum class Type { Compress, Decompress };
        Type type;
        std::string key;
    };

    std::shared_ptr<StorageEngine> underlying_;
    Config config_;
    compression::CompressionPolicy policy_;
    std::atomic<bool> compressionEnabled_;

    // Statistics
    mutable compression::CompressionStats compressionStats_;
    mutable std::mutex statsMutex_;

    // Async compression
    std::vector<std::thread> workers_;
    std::queue<AsyncJob> asyncQueue_;
    std::mutex asyncMutex_;
    std::condition_variable asyncCV_;
    std::atomic<bool> shutdownFlag_;
    std::atomic<size_t> activeJobs_{0};

    // Metadata cache
    struct CachedMetadata {
        bool isCompressed;
        size_t originalSize;
        compression::CompressionAlgorithm algorithm;
        std::chrono::steady_clock::time_point cachedAt;
    };
    mutable std::unordered_map<std::string, CachedMetadata> metadataCache_;
    mutable std::mutex cacheMutex_;

    void updateStats(std::function<void(compression::CompressionStats&)> updater) const {
        std::unique_lock lock(statsMutex_);
        updater(compressionStats_);
    }

    template <typename DecisionType>
    Result<std::vector<std::byte>> compressData(std::span<const std::byte> data,
                                                const DecisionType& decision) {
        auto& registry = compression::CompressionRegistry::instance();
        auto compressor = registry.createCompressor(decision.algorithm);
        if (!compressor) {
            return Result<std::vector<std::byte>>(
                Error{ErrorCode::InvalidArgument, "Compressor not available"});
        }

        // auto start = std::chrono::steady_clock::now(); // unused
        auto result = compressor->compress(data, decision.level);
        if (!result) {
            updateStats([](compression::CompressionStats& stats) {
                stats.algorithmStats[compression::CompressionAlgorithm::Zstandard]
                    .compressionErrors++;
            });
            return result.error();
        }

        // Create header
        compression::CompressionHeader header;
        header.magic = compression::CompressionHeader::MAGIC;
        header.version = compression::CompressionHeader::VERSION;
        header.algorithm = static_cast<uint8_t>(decision.algorithm);
        header.level = decision.level;
        header.uncompressedSize = data.size();
        header.compressedSize = result.value().compressedSize;
        header.uncompressedCRC32 = calculateCRC32(data);
        header.compressedCRC32 = calculateCRC32(std::span<const std::byte>{
            reinterpret_cast<const std::byte*>(result.value().data.data()),
            result.value().data.size()});
        const auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count();
        header.timestamp = now_ns < 0 ? 0ULL : static_cast<uint64_t>(now_ns);
        header.flags = 0;
        header.reserved1 = 0;
        std::memset(header.reserved2, 0, sizeof(header.reserved2));
        // padding field doesn't exist in the actual header structure

        // Combine header and compressed data
        std::vector<std::byte> output;
        output.resize(sizeof(header) + result.value().data.size());
        std::memcpy(output.data(), &header, sizeof(header));
        std::memcpy(output.data() + sizeof(header), result.value().data.data(),
                    result.value().data.size());

        // Update statistics
        updateStats([&](compression::CompressionStats& stats) {
            stats.totalCompressedFiles++;
            stats.totalCompressedBytes += output.size();
            stats.totalSpaceSaved += data.size() - output.size();
            stats.algorithmStats[decision.algorithm].recordCompression(result.value());
            stats.onDemandCompressions++;
        });

        return output;
    }

    Result<std::vector<std::byte>> decompressData(std::span<const std::byte> data) const {
        if (data.size() < sizeof(compression::CompressionHeader)) {
            return Error(ErrorCode::CorruptedData, "Data too small for compressed format");
        }

        // Parse header
        compression::CompressionHeader header;
        std::memcpy(&header, data.data(), sizeof(header));

        if (header.magic != compression::CompressionHeader::MAGIC) {
            return Error(ErrorCode::CorruptedData, "Invalid compression header magic");
        }

        if (header.version != compression::CompressionHeader::VERSION) {
            return Error(ErrorCode::InvalidArgument,
                         "Unsupported compression version: " + std::to_string(header.version));
        }

        // Extract compressed data
        std::span<const std::byte> compressedData{data.data() + sizeof(header),
                                                  data.size() - sizeof(header)};

        // Optional compressed CRC check (can be disabled with YAMS_SKIP_DECOMPRESS_CRC)
        uint32_t actualCRC = 0;
        if (std::getenv("YAMS_SKIP_DECOMPRESS_CRC") == nullptr) {
            actualCRC = calculateCRC32(compressedData);
            if (actualCRC != header.compressedCRC32) {
                updateStats([](compression::CompressionStats& stats) {
                    stats.cacheEvictions++; // Using as error counter
                });
                return Error(ErrorCode::HashMismatch, "Compressed data CRC mismatch");
            }
        }

        // Get decompressor
        auto algorithm = static_cast<compression::CompressionAlgorithm>(header.algorithm);
        auto& registry = compression::CompressionRegistry::instance();
        auto decompressor = registry.createCompressor(algorithm);
        if (!decompressor) {
            return Result<std::vector<std::byte>>(
                Error{ErrorCode::InvalidArgument, "Decompressor not available"});
        }

        // Decompress
        auto start = std::chrono::steady_clock::now();
        auto result = decompressor->decompress(compressedData, header.uncompressedSize);
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);

        if (!result) {
            updateStats([&](compression::CompressionStats& stats) {
                stats.algorithmStats[algorithm].decompressionErrors++;
            });
            return result.error();
        }

        // Optional uncompressed CRC verification (can be disabled with YAMS_SKIP_DECOMPRESS_CRC)
        if (std::getenv("YAMS_SKIP_DECOMPRESS_CRC") == nullptr) {
            actualCRC = calculateCRC32(std::span<const std::byte>{
                reinterpret_cast<const std::byte*>(result.value().data()), result.value().size()});
            if (actualCRC != header.uncompressedCRC32) {
                return Error(ErrorCode::HashMismatch, "Decompressed data CRC mismatch");
            }
        }

        // Update statistics
        updateStats([&](compression::CompressionStats& stats) {
            stats.algorithmStats[algorithm].recordDecompression(compressedData.size(),
                                                                result.value().size(), duration);
        });

        return result;
    }

    void startAsyncWorkers() {
        // Default to half the system cores; allow env override; clamp to [1,64]
        size_t numWorkers = std::max<size_t>(1, std::thread::hardware_concurrency() / 2);
        if (const char* s = std::getenv("YAMS_COMPRESSION_WORKERS")) {
            try {
                long v = std::strtol(s, nullptr, 10);
                if (v > 0)
                    numWorkers = static_cast<size_t>(v);
            } catch (...) {
            }
        }
        if (numWorkers > 64)
            numWorkers = 64;

        for (size_t i = 0; i < numWorkers; ++i) {
            workers_.emplace_back([this] { asyncWorker(); });
        }

        updateStats([numWorkers](compression::CompressionStats& stats) {
            stats.currentWorkerThreads = numWorkers;
        });
    }

    void asyncWorker() {
        while (!shutdownFlag_) {
            AsyncJob job;

            {
                std::unique_lock lock(asyncMutex_);
                asyncCV_.wait(lock, [this] { return !asyncQueue_.empty() || shutdownFlag_; });

                if (shutdownFlag_) {
                    break;
                }

                job = asyncQueue_.front();
                asyncQueue_.pop();
                activeJobs_++;

                updateStats([this](compression::CompressionStats& stats) {
                    stats.queuedCompressions = asyncQueue_.size();
                });
            }

            // Process job
            if (job.type == AsyncJob::Type::Compress) {
                auto result = compressExisting(job.key, false);
                if (result) {
                    updateStats([](compression::CompressionStats& stats) {
                        stats.backgroundCompressions++;
                    });
                }
            }

            activeJobs_--;
            asyncCV_.notify_all();
        }
    }

    void shutdown() {
        shutdownFlag_ = true;
        asyncCV_.notify_all();

        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }
};

// Public interface implementation

CompressedStorageEngine::CompressedStorageEngine(std::shared_ptr<StorageEngine> underlying,
                                                 Config config)
    : StorageEngine(
          {.basePath = underlying ? underlying->getBasePath() : std::filesystem::path{"/tmp"}}),
      pImpl(std::make_unique<Impl>(std::move(underlying), std::move(config))) {}

CompressedStorageEngine::~CompressedStorageEngine() = default;

Result<void> CompressedStorageEngine::store(std::string_view hash,
                                            std::span<const std::byte> data) {
    return pImpl->store(hash, data);
}

Result<std::vector<std::byte>> CompressedStorageEngine::retrieve(std::string_view hash) const {
    return pImpl->retrieve(hash);
}

Result<bool> CompressedStorageEngine::exists(std::string_view hash) const noexcept {
    return pImpl->exists(hash);
}

Result<void> CompressedStorageEngine::remove(std::string_view hash) {
    return pImpl->remove(hash);
}

// Required StorageEngine interface methods
StorageStats CompressedStorageEngine::getStats() const noexcept {
    return pImpl->getStats();
}

Result<uint64_t> CompressedStorageEngine::getStorageSize() const {
    return pImpl->getStorageSize();
}

// Async operations
std::future<Result<void>> CompressedStorageEngine::storeAsync(std::string_view hash,
                                                              std::span<const std::byte> data) {
    return pImpl->storeAsync(hash, data);
}

std::future<Result<std::vector<std::byte>>>
CompressedStorageEngine::retrieveAsync(std::string_view hash) const {
    return pImpl->retrieveAsync(hash);
}

// Batch operations
std::vector<Result<void>> CompressedStorageEngine::storeBatch(
    const std::vector<std::pair<std::string, std::vector<std::byte>>>& items) {
    return pImpl->storeBatch(items);
}

compression::CompressionStats CompressedStorageEngine::getCompressionStats() const {
    return pImpl->getCompressionStats();
}

Result<void> CompressedStorageEngine::compressExisting(std::string_view hash, bool force) {
    return pImpl->compressExisting(hash, force);
}

Result<void> CompressedStorageEngine::decompressExisting(std::string_view hash) {
    return pImpl->decompressExisting(hash);
}

void CompressedStorageEngine::setPolicyRules(compression::CompressionPolicy::Rules rules) {
    pImpl->setPolicyRules(std::move(rules));
}

compression::CompressionPolicy::Rules CompressedStorageEngine::getPolicyRules() const {
    return pImpl->getPolicyRules();
}

void CompressedStorageEngine::setCompressionEnabled(bool enable) {
    pImpl->setCompressionEnabled(enable);
}

bool CompressedStorageEngine::isCompressionEnabled() const {
    return pImpl->isCompressionEnabled();
}

Result<size_t> CompressedStorageEngine::triggerCompressionScan() {
    return pImpl->triggerCompressionScan();
}

bool CompressedStorageEngine::waitForAsyncOperations(std::chrono::milliseconds timeout) {
    return pImpl->waitForAsyncOperations(timeout);
}

} // namespace yams::storage
