#include <yams/compression/compression_header.h>
#include <yams/profiling.h>
#include <yams/storage/reference_counter.h>
#include <yams/storage/storage_engine.h>

#include "storage_key.h"

#include <spdlog/spdlog.h>
#include <array>
#include <cstddef>
#include <fstream>
#include <span>
#if defined(YAMS_HAS_STD_FORMAT) && YAMS_HAS_STD_FORMAT
#include <format>
namespace yamsfmt = std;
#else
#include <spdlog/fmt/fmt.h>
namespace yamsfmt = fmt;
#endif
#include <atomic>
#include <condition_variable>
#include <thread>

namespace yams::storage {
namespace {

void notifyCollectionProgress(const GCOptions& options, const std::string& blockHash,
                              const GCStats& stats) {
    if (options.progressCallback) {
        options.progressCallback(blockHash, stats.blocksDeleted);
    }
}

void recordCollectionStatistics(ReferenceCounter& refCounter, const GCOptions& options,
                                const GCStats& stats) {
    if (options.dryRun || stats.blocksDeleted == 0) {
        return;
    }

    refCounter.updateStatistics("gc_runs", 1);
    refCounter.updateStatistics("gc_blocks_collected", static_cast<int64_t>(stats.blocksDeleted));
    refCounter.updateStatistics("gc_bytes_reclaimed", static_cast<int64_t>(stats.bytesReclaimed));
}

} // namespace

// Implementation details
struct GarbageCollector::Impl {
    ReferenceCounter& refCounter;
    StorageEngine& storageEngine;

    // Scheduling state
    std::thread schedulerThread;
    std::condition_variable schedulerCv;
    std::mutex schedulerMutex;
    std::atomic<bool> stopScheduler{false};
    std::atomic<bool> isCollecting{false};

    // Last collection stats
    mutable std::mutex statsMutex;
    GCStats lastStats;

    Impl(ReferenceCounter& rc, StorageEngine& se) : refCounter(rc), storageEngine(se) {}

    ~Impl() { stopScheduledCollection(); }

    void stopScheduledCollection() {
        {
            std::lock_guard lock(schedulerMutex);
            stopScheduler = true;
        }
        schedulerCv.notify_all();
        if (schedulerThread.joinable()) {
            schedulerThread.join();
        }
    }
};

namespace {

void processCollectibleBlock(ReferenceCounter& refCounter, StorageEngine& storageEngine,
                             IReferenceCounter::ITransaction* txn, const GCOptions& options,
                             const std::string& blockHash, GCStats& stats) {
    auto refCountResult = refCounter.getRefCount(blockHash);
    if (!refCountResult || refCountResult.value() > 0) {
        return;
    }

    const auto blockSizeResult = storageEngine.getBlockSize(blockHash);
    if (options.dryRun) {
        notifyCollectionProgress(options, blockHash, stats);
        return;
    }

    auto deleteResult = storageEngine.remove(blockHash);
    if (!deleteResult) {
        if (deleteResult.error().code == ErrorCode::ChunkNotFound) {
            // File already absent on disk; still prune the zero-ref row to prevent GC starvation,
            // while recording the missing-block error without counting it as deleted/reclaimed.
            if (txn) {
                txn->pruneReference(blockHash);
            }
        }
        stats.errors.push_back(yamsfmt::format("Failed to delete block {}: {}", blockHash,
                                               deleteResult.error().message));
        return;
    }
    if (txn) {
        txn->pruneReference(blockHash);
    }
    if (blockSizeResult) {
        stats.bytesReclaimed += blockSizeResult.value();
    }
    ++stats.blocksDeleted;
    notifyCollectionProgress(options, blockHash, stats);
}

} // namespace

// Constructor
GarbageCollector::GarbageCollector(ReferenceCounter& refCounter, StorageEngine& storageEngine)
    : pImpl(std::make_unique<Impl>(refCounter, storageEngine)) {}

// Destructor

// Move constructor
GarbageCollector::GarbageCollector(GarbageCollector&& other) noexcept {
    if (other.pImpl) {
        other.pImpl->stopScheduledCollection();
    }
    pImpl = std::move(other.pImpl);
}

// Move assignment
GarbageCollector& GarbageCollector::operator=(GarbageCollector&& other) noexcept {
    if (this != &other) {
        if (pImpl) {
            pImpl->stopScheduledCollection();
        }
        if (other.pImpl) {
            other.pImpl->stopScheduledCollection();
        }
        pImpl = std::move(other.pImpl);
    }
    return *this;
}

// Run garbage collection
Result<GCStats> GarbageCollector::collect(const GCOptions& options) {
    YAMS_ZONE_SCOPED_N("GarbageCollector::collect");

    // Check if already collecting
    bool expected = false;
    if (!pImpl->isCollecting.compare_exchange_strong(expected, true)) {
        spdlog::debug("Garbage collection already in progress");
        return Result<GCStats>(ErrorCode::OperationInProgress);
    }

    // RAII to ensure flag is reset
    struct CollectingGuard {
        std::atomic<bool>& flag;
        ~CollectingGuard() { flag = false; }
    } guard{pImpl->isCollecting};

    auto startTime = std::chrono::steady_clock::now();
    GCStats stats{};

    spdlog::debug("Starting garbage collection (dry_run: {}, max_blocks: {}, min_age: {}s)",
                  options.dryRun, options.maxBlocksPerRun, options.minAgeSeconds);

    bool commitSucceeded = true;
    try {
        // Get unreferenced blocks
        YAMS_ZONE_SCOPED_N("Get unreferenced blocks");
        auto blocksResult = pImpl->refCounter.getUnreferencedBlocks(
            options.maxBlocksPerRun, std::chrono::seconds(options.minAgeSeconds));

        if (!blocksResult) {
            return Result<GCStats>(blocksResult.error());
        }

        const auto& blocks = blocksResult.value();
        stats.blocksScanned = blocks.size();

        // Only open a write transaction when not in dry-run mode.
        std::unique_ptr<IReferenceCounter::ITransaction> txn;
        if (!options.dryRun) {
            txn = pImpl->refCounter.beginTransaction();
            if (!txn) {
                return Result<GCStats>(ErrorCode::TransactionFailed);
            }
        }

        // Process each block
        {
            YAMS_ZONE_SCOPED_N("Process blocks");
            for (const auto& blockHash : blocks) {
                processCollectibleBlock(pImpl->refCounter, pImpl->storageEngine, txn.get(), options,
                                        blockHash, stats);
            }
        }

        // Commit transaction
        if (!options.dryRun && txn) {
            auto commitResult = txn->commit();
            if (!commitResult) {
                commitSucceeded = false;
                stats.errors.push_back("Failed to commit transaction");
            }
        }

    } catch (const std::exception& e) {
        commitSucceeded = false;
        stats.errors.push_back(yamsfmt::format("Exception during collection: {}", e.what()));
        spdlog::error("Garbage collection failed: {}", e.what());
    }

    // Calculate duration
    auto endTime = std::chrono::steady_clock::now();
    stats.duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    {
        std::lock_guard lock(pImpl->statsMutex);
        pImpl->lastStats = stats;
    }
    if (commitSucceeded) {
        recordCollectionStatistics(pImpl->refCounter, options, stats);
    }

    spdlog::debug(
        "Garbage collection completed: {} blocks scanned, {} deleted, {} bytes reclaimed in {}ms",
        stats.blocksScanned, stats.blocksDeleted, stats.bytesReclaimed, stats.duration.count());

    if (!commitSucceeded) {
        return Result<GCStats>(ErrorCode::TransactionFailed);
    }
    return stats;
}

// Async garbage collection
std::future<Result<GCStats>> GarbageCollector::collectAsync(const GCOptions& options) {
    return std::async(std::launch::async,
                      [this, options = options]() -> Result<GCStats> { return collect(options); });
}

// Schedule periodic collection
void GarbageCollector::scheduleCollection(std::chrono::seconds interval, const GCOptions& options) {
    // Stop any existing scheduler
    stopScheduledCollection();

    pImpl->stopScheduler = false;

    // Start scheduler thread
    pImpl->schedulerThread = std::thread([this, interval, options]() {
        spdlog::debug("Started scheduled garbage collection (interval: {}s)", interval.count());

        while (!pImpl->stopScheduler) {
            // Wait for interval or stop signal
            std::unique_lock lock(pImpl->schedulerMutex);
            if (pImpl->schedulerCv.wait_for(lock, interval,
                                            [this] { return pImpl->stopScheduler.load(); })) {
                break; // Stop requested
            }

            // Run collection
            if (!pImpl->stopScheduler) {
                spdlog::debug("Running scheduled garbage collection");
                auto result = collect(options);
                if (!result) {
                    spdlog::error("Scheduled garbage collection failed");
                }
            }
        }

        spdlog::debug("Stopped scheduled garbage collection");
    });
}

// Stop scheduled collection
void GarbageCollector::stopScheduledCollection() {
    pImpl->stopScheduledCollection();
}

// Check if collection is running
bool GarbageCollector::isCollecting() const {
    return pImpl->isCollecting.load();
}

// Get last collection stats
GCStats GarbageCollector::getLastStats() const {
    std::lock_guard lock(pImpl->statsMutex);
    return pImpl->lastStats;
}

// Factory function
std::unique_ptr<GarbageCollector> createGarbageCollector(ReferenceCounter& refCounter,
                                                         StorageEngine& storageEngine) {
    return std::make_unique<GarbageCollector>(refCounter, storageEngine);
}

// Utility function to rebuild reference database
Result<void> rebuildReferenceDatabase(const std::filesystem::path& dbPath,
                                      const std::filesystem::path& storagePath) {
    spdlog::debug("Starting reference database rebuild from storage at {}", storagePath.string());

    try {
        // Rebuild into a temporary database before atomically swapping into dbPath.
        auto tempDbPath = dbPath;
        tempDbPath += ".rebuild.tmp";
        std::error_code rmEc;
        std::filesystem::remove(tempDbPath, rmEc);
        auto tempWal = tempDbPath;
        tempWal += "-wal";
        auto tempShm = tempDbPath;
        tempShm += "-shm";
        std::filesystem::remove(tempWal, rmEc);
        std::filesystem::remove(tempShm, rmEc);

        size_t blocksFound = 0;
        size_t totalSize = 0;
        {
            ReferenceCounter::Config config{
                .databasePath = tempDbPath, .enableWAL = false, .enableStatistics = true};
            auto refCounter = createReferenceCounter(config);

            auto txn = refCounter->beginTransaction();
            if (!txn) {
                return Result<void>(ErrorCode::TransactionFailed);
            }

            // Iterate through storage shards
            for (const auto& shardEntry : std::filesystem::directory_iterator(storagePath)) {
                if (!shardEntry.is_directory()) {
                    continue;
                }

                // Iterate through blocks in shard
                for (const auto& blockEntry : std::filesystem::directory_iterator(shardEntry)) {
                    if (!blockEntry.is_regular_file()) {
                        continue;
                    }

                    const std::string blockHash =
                        storageHashFromObjectPath(storagePath, blockEntry.path());
                    if (!isValidStorageKey(blockHash) || isManifestKey(blockHash)) {
                        continue;
                    }

                    const size_t compressedSize = blockEntry.file_size();
                    if (compressedSize == 0) {
                        continue;
                    }
                    size_t uncompressedSize = compressedSize; // Default: assume not compressed

                    // Try to read compression header to get uncompressed size
                    if (compressedSize >= compression::CompressionHeader::SIZE) {
                        std::ifstream file(blockEntry.path(), std::ios::binary);
                        if (file) {
                            std::array<std::byte, compression::CompressionHeader::SIZE> headerBytes;
                            file.read(reinterpret_cast<char*>(headerBytes.data()),
                                      headerBytes.size());
                            if (file.gcount() == static_cast<std::streamsize>(headerBytes.size())) {
                                auto headerResult =
                                    compression::CompressionHeader::parse(headerBytes);
                                if (headerResult && headerResult.value().validate() &&
                                    headerResult.value().uncompressedSize >= compressedSize) {
                                    uncompressedSize = headerResult.value().uncompressedSize;
                                }
                            }
                        }
                    }

                    // Add to reference database with both sizes
                    txn->increment(blockHash, compressedSize, uncompressedSize);

                    blocksFound++;
                    totalSize += compressedSize;

                    // Progress logging
                    if (blocksFound % 1000 == 0) {
                        spdlog::info("Processed {} blocks ({} bytes)", blocksFound, totalSize);
                    }
                }
            }

            // Commit transaction
            auto commitResult = txn->commit();
            if (!commitResult) {
                return Result<void>(ErrorCode::TransactionFailed);
            }
        }

        // Remove stale sidecars at destination before swapping rebuilt DB into place.
        auto destWal = dbPath;
        destWal += "-wal";
        auto destShm = dbPath;
        destShm += "-shm";
        std::filesystem::remove(destWal, rmEc);
        std::filesystem::remove(destShm, rmEc);
        std::filesystem::rename(tempDbPath, dbPath);

        spdlog::info("Reference database rebuild completed: {} blocks, {} bytes total", blocksFound,
                     totalSize);

        return {};

    } catch (const std::exception& e) {
        spdlog::error("Failed to rebuild reference database: {}", e.what());
        return Result<void>(ErrorCode::Unknown);
    }
}

GarbageCollector::~GarbageCollector() = default;

} // namespace yams::storage
