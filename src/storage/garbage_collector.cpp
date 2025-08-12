#include <yams/storage/reference_counter.h>
#include <yams/storage/storage_engine.h>

#include <spdlog/spdlog.h>
#if defined(YAMS_HAS_STD_FORMAT) && YAMS_HAS_STD_FORMAT
#include <format>
namespace yamsfmt = std;
#else
#include <spdlog/fmt/fmt.h>
namespace yamsfmt = fmt;
#endif
#include <thread>
#include <condition_variable>
#include <atomic>

namespace yams::storage {

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
    
    Impl(ReferenceCounter& rc, StorageEngine& se) 
        : refCounter(rc), storageEngine(se) {}
    
    ~Impl() {
        stopScheduledCollection();
    }
    
    void stopScheduledCollection() {
        stopScheduler = true;
        schedulerCv.notify_all();
        if (schedulerThread.joinable()) {
            schedulerThread.join();
        }
    }
};

// Constructor
GarbageCollector::GarbageCollector(ReferenceCounter& refCounter, 
                                 StorageEngine& storageEngine)
    : pImpl(std::make_unique<Impl>(refCounter, storageEngine)) {
}

// Destructor
GarbageCollector::~GarbageCollector() = default;

// Move constructor
GarbageCollector::GarbageCollector(GarbageCollector&&) noexcept = default;

// Move assignment
GarbageCollector& GarbageCollector::operator=(GarbageCollector&&) noexcept = default;

// Run garbage collection
Result<GCStats> GarbageCollector::collect(const GCOptions& options) {
    // Check if already collecting
    bool expected = false;
    if (!pImpl->isCollecting.compare_exchange_strong(expected, true)) {
        spdlog::warn("Garbage collection already in progress");
        return Result<GCStats>(ErrorCode::OperationInProgress);
    }
    
    // RAII to ensure flag is reset
    struct CollectingGuard {
        std::atomic<bool>& flag;
        ~CollectingGuard() { flag = false; }
    } guard{pImpl->isCollecting};
    
    auto startTime = std::chrono::steady_clock::now();
    GCStats stats{};
    
    spdlog::info("Starting garbage collection (dry_run: {}, max_blocks: {}, min_age: {}s)",
                 options.dryRun, options.maxBlocksPerRun, options.minAgeSeconds);
    
    try {
        // Get unreferenced blocks
        auto blocksResult = pImpl->refCounter.getUnreferencedBlocks(
            options.maxBlocksPerRun, 
            std::chrono::seconds(options.minAgeSeconds));
        
        if (!blocksResult) {
            stats.errors.push_back("Failed to get unreferenced blocks");
            return stats;
        }
        
        const auto& blocks = blocksResult.value();
        stats.blocksScanned = blocks.size();
        
        // Begin transaction for atomic updates
        auto txn = pImpl->refCounter.beginTransaction();
        if (!txn) {
            stats.errors.push_back("Failed to begin transaction");
            return stats;
        }
        
        // Process each block
        for (const auto& blockHash : blocks) {
            // Double-check reference count (race condition protection)
            auto refCountResult = pImpl->refCounter.getRefCount(blockHash);
            if (!refCountResult || refCountResult.value() > 0) {
                continue;  // Skip if error or references exist
            }
            
            if (!options.dryRun) {
                // Delete from storage
                auto deleteResult = pImpl->storageEngine.remove(blockHash);
                if (!deleteResult) {
                    stats.errors.push_back(
                        yamsfmt::format("Failed to delete block {}: {}", 
                                  blockHash, deleteResult.error().message));
                    continue;
                }
                
                // Get block size for statistics
                // Note: In a real implementation, we'd get this from the reference DB
                stats.bytesReclaimed += 4096;  // Placeholder size
                
                // Remove from reference database
                // This would be done through a specific method in ReferenceCounter
                // For now, we'll just count it
                stats.blocksDeleted++;
            } else {
                // Dry run - just count
                stats.blocksDeleted++;
                stats.bytesReclaimed += 4096;  // Placeholder size
            }
            
            // Progress callback
            if (options.progressCallback) {
                options.progressCallback(blockHash, stats.blocksDeleted);
            }
        }
        
        // Commit transaction
        if (!options.dryRun) {
            auto commitResult = txn->commit();
            if (!commitResult) {
                stats.errors.push_back("Failed to commit transaction");
            }
        }
        
    } catch (const std::exception& e) {
        stats.errors.push_back(yamsfmt::format("Exception during collection: {}", e.what()));
        spdlog::error("Garbage collection failed: {}", e.what());
    }
    
    // Calculate duration
    auto endTime = std::chrono::steady_clock::now();
    stats.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        endTime - startTime);
    
    // Update last stats
    {
        std::lock_guard lock(pImpl->statsMutex);
        pImpl->lastStats = stats;
    }
    
    // Update database statistics
    if (!options.dryRun && stats.blocksDeleted > 0) {
        pImpl->refCounter.updateStatistics("gc_runs", 1);
        pImpl->refCounter.updateStatistics("gc_blocks_collected", static_cast<int64_t>(stats.blocksDeleted));
        pImpl->refCounter.updateStatistics("gc_bytes_reclaimed", static_cast<int64_t>(stats.bytesReclaimed));
    }
    
    spdlog::info("Garbage collection completed: {} blocks scanned, {} deleted, {} bytes reclaimed in {}ms",
                 stats.blocksScanned, stats.blocksDeleted, stats.bytesReclaimed, 
                 stats.duration.count());
    
    return stats;
}

// Async garbage collection
std::future<Result<GCStats>> GarbageCollector::collectAsync(const GCOptions& options) {
    return std::async(std::launch::async, [this, options]() {
        return collect(options);
    });
}

// Schedule periodic collection
void GarbageCollector::scheduleCollection(std::chrono::seconds interval,
                                        const GCOptions& options) {
    // Stop any existing scheduler
    stopScheduledCollection();
    
    pImpl->stopScheduler = false;
    
    // Start scheduler thread
    pImpl->schedulerThread = std::thread([this, interval, options]() {
        spdlog::info("Started scheduled garbage collection (interval: {}s)", interval.count());
        
        while (!pImpl->stopScheduler) {
            // Wait for interval or stop signal
            std::unique_lock lock(pImpl->schedulerMutex);
            if (pImpl->schedulerCv.wait_for(lock, interval, 
                [this] { return pImpl->stopScheduler.load(); })) {
                break;  // Stop requested
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
        
        spdlog::info("Stopped scheduled garbage collection");
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
std::unique_ptr<GarbageCollector> createGarbageCollector(
    ReferenceCounter& refCounter,
    StorageEngine& storageEngine) {
    return std::make_unique<GarbageCollector>(refCounter, storageEngine);
}

// Utility function to rebuild reference database
Result<void> rebuildReferenceDatabase(
    const std::filesystem::path& dbPath,
    const std::filesystem::path& storagePath) {
    
    spdlog::info("Starting reference database rebuild from storage at {}", 
                 storagePath.string());
    
    try {
        // Create new reference counter
        ReferenceCounter::Config config{
            .databasePath = dbPath,
            .enableWAL = true,
            .enableStatistics = true
        };
        auto refCounter = createReferenceCounter(config);
        
        // Scan storage directory
        size_t blocksFound = 0;
        size_t totalSize = 0;
        
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
                
                std::string blockHash = blockEntry.path().filename().string();
                size_t blockSize = blockEntry.file_size();
                
                // Add to reference database with count of 1
                txn->increment(blockHash, blockSize);
                
                blocksFound++;
                totalSize += blockSize;
                
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
        
        spdlog::info("Reference database rebuild completed: {} blocks, {} bytes total",
                     blocksFound, totalSize);
        
        return {};
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to rebuild reference database: {}", e.what());
        return Result<void>(ErrorCode::Unknown);
    }
}

} // namespace yams::storage