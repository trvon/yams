#pragma once

#include <yams/core/concepts.h>
#include <yams/core/types.h>

#include <atomic>
#include <chrono>
#include <coroutine>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <ranges>
#include <shared_mutex>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace yams::storage {

// Forward declarations
class Database;
class GarbageCollector;
class StorageEngine;

// Reference counting statistics
struct RefCountStats {
    uint64_t totalBlocks;        // Total number of tracked blocks
    uint64_t totalReferences;    // Sum of all reference counts
    uint64_t totalBytes;         // Total size of all blocks
    uint64_t unreferencedBlocks; // Blocks with ref_count = 0
    uint64_t unreferencedBytes;  // Size of unreferenced blocks
    uint64_t transactions;       // Total transactions processed
    uint64_t rollbacks;          // Total rolled back transactions
};

// Options for garbage collection
struct GCOptions {
    size_t maxBlocksPerRun = 1000; // Maximum blocks to collect per run
    size_t minAgeSeconds = 3600;   // Minimum age before collection (1 hour)
    bool dryRun = false;           // If true, only report what would be collected
    std::function<void(const std::string&, size_t)> progressCallback;
};

// Results from garbage collection
struct GCStats {
    size_t blocksScanned = 0;
    size_t blocksDeleted = 0;
    uint64_t bytesReclaimed = 0;
    std::chrono::milliseconds duration{0};
    std::vector<std::string> errors;
};

// Concepts for batch operations
template <typename T>
concept BlockHashRange =
    std::ranges::input_range<T> && std::convertible_to<std::ranges::range_value_t<T>, std::string>;

template <typename T>
concept BlockInfoRange = std::ranges::input_range<T> && requires(std::ranges::range_value_t<T> v) {
    { v.hash } -> std::convertible_to<std::string>;
    { v.size } -> std::convertible_to<size_t>;
};

// Interface for reference counting operations
class IReferenceCounter {
public:
    virtual ~IReferenceCounter() = default;

    // Single operations
    virtual Result<void> increment(std::string_view blockHash, size_t blockSize) = 0;
    virtual Result<void> decrement(std::string_view blockHash) = 0;

    // Queries
    virtual Result<uint64_t> getRefCount(std::string_view blockHash) const = 0;
    virtual Result<bool> hasReferences(std::string_view blockHash) const = 0;
    virtual Result<RefCountStats> getStats() const = 0;

    // Transaction support
    class ITransaction {
    public:
        virtual ~ITransaction() = default;

        virtual void increment(std::string_view blockHash, size_t blockSize = 0) = 0;
        virtual void decrement(std::string_view blockHash) = 0;
        virtual Result<void> commit() = 0;
        virtual void rollback() = 0;
        virtual bool isActive() const = 0;
    };

    virtual std::unique_ptr<ITransaction> beginTransaction() = 0;
};

// Main reference counter implementation
class ReferenceCounter : public IReferenceCounter {
public:
    // Configuration options
    struct Config {
        std::filesystem::path databasePath;
        bool enableWAL = true;        // Write-Ahead Logging
        bool enableStatistics = true; // Track detailed statistics
        size_t cacheSize = 10000;     // SQLite page cache size
        size_t busyTimeout = 5000;    // Busy timeout in milliseconds
        bool enableAuditLog = false;  // Enable audit logging
    };

    explicit ReferenceCounter(Config config);
    ~ReferenceCounter();

    // Delete copy, enable move
    ReferenceCounter(const ReferenceCounter&) = delete;
    ReferenceCounter& operator=(const ReferenceCounter&) = delete;
    ReferenceCounter(ReferenceCounter&&) noexcept;
    ReferenceCounter& operator=(ReferenceCounter&&) noexcept;

    // Single operations
    Result<void> increment(std::string_view blockHash, size_t blockSize) override;
    Result<void> decrement(std::string_view blockHash) override;

    // Batch operations with C++20 ranges
    template <BlockHashRange R> Result<void> incrementBatch(R&& hashes, size_t blockSize = 0) {
        auto txn = beginTransaction();
        if (!txn) {
            return Result<void>(ErrorCode::TransactionFailed);
        }

        for (const auto& hash : hashes) {
            txn->increment(hash, blockSize);
        }

        return txn->commit();
    }

    template <BlockInfoRange R> Result<void> incrementBatchWithSizes(R&& items) {
        auto txn = beginTransaction();
        if (!txn) {
            return Result<void>(ErrorCode::TransactionFailed);
        }

        for (const auto& item : items) {
            txn->increment(item.hash, item.size);
        }

        return txn->commit();
    }

    template <BlockHashRange R> Result<void> decrementBatch(R&& hashes) {
        auto txn = beginTransaction();
        if (!txn) {
            return Result<void>(ErrorCode::TransactionFailed);
        }

        for (const auto& hash : hashes) {
            txn->decrement(hash);
        }

        return txn->commit();
    }

    // Async batch operations with coroutines
    template <BlockHashRange R>
    std::future<Result<void>> incrementBatchAsync(R&& hashes, size_t blockSize = 0) {
        return std::async(std::launch::async,
                          [this, hashes = std::forward<R>(hashes), blockSize]() {
                              return incrementBatch(hashes, blockSize);
                          });
    }

    // Queries
    Result<uint64_t> getRefCount(std::string_view blockHash) const override;
    Result<bool> hasReferences(std::string_view blockHash) const override;
    Result<RefCountStats> getStats() const override;

    // Get unreferenced blocks for garbage collection
    Result<std::vector<std::string>>
    getUnreferencedBlocks(size_t limit = 1000,
                          std::chrono::seconds minAge = std::chrono::seconds(3600)) const;

    // Transaction implementation with RAII
    class Transaction : public ITransaction {
        friend class ReferenceCounter;

    public:
        ~Transaction();

        // Delete copy, enable move
        Transaction(const Transaction&) = delete;
        Transaction& operator=(const Transaction&) = delete;
        Transaction(Transaction&&) noexcept;
        Transaction& operator=(Transaction&&) noexcept;

        // Operations
        void increment(std::string_view blockHash, size_t blockSize = 0) override;
        void decrement(std::string_view blockHash) override;

        // Batch operations for efficiency
        template <BlockHashRange R> void incrementBatch(R&& hashes, size_t blockSize = 0) {
            for (const auto& hash : hashes) {
                increment(hash, blockSize);
            }
        }

        template <BlockHashRange R> void decrementBatch(R&& hashes) {
            for (const auto& hash : hashes) {
                decrement(hash);
            }
        }

        // Transaction control
        Result<void> commit() override;
        void rollback() override;
        bool isActive() const override { return active_; }

        // Get transaction ID
        int64_t getId() const { return transactionId_; }

    private:
        Transaction(ReferenceCounter* counter, int64_t id);

        ReferenceCounter* counter_;
        int64_t transactionId_;
        bool active_;
        bool committed_;

        // Track operations for potential rollback
        struct Operation {
            enum class Type { Increment, Decrement };
            Type type;
            std::string blockHash;
            size_t blockSize;
            int delta;
        };
        std::vector<Operation> operations_;
    };

    std::unique_ptr<ITransaction> beginTransaction() override;

    // Maintenance operations
    Result<void> vacuum();     // Optimize database
    Result<void> checkpoint(); // Checkpoint WAL
    Result<void> analyze();    // Update statistics

    // Database backup
    Result<void> backup(const std::filesystem::path& destPath);
    Result<void> restore(const std::filesystem::path& srcPath);

    // Integrity check
    Result<bool> verifyIntegrity() const;

public:
    // Statistics update (for garbage collector)
    Result<void> updateStatistics(const std::string& statName, int64_t delta);

private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;

    // Internal methods
    Result<void> initializeDatabase();
    Result<void> executeSchemaMigrations();
};

// Garbage collector for unreferenced blocks
class GarbageCollector {
public:
    GarbageCollector(ReferenceCounter& refCounter, StorageEngine& storageEngine);
    ~GarbageCollector();

    // Delete copy, enable move
    GarbageCollector(const GarbageCollector&) = delete;
    GarbageCollector& operator=(const GarbageCollector&) = delete;
    GarbageCollector(GarbageCollector&&) noexcept;
    GarbageCollector& operator=(GarbageCollector&&) noexcept;

    // Run garbage collection
    Result<GCStats> collect(const GCOptions& options = {});

    // Async garbage collection
    std::future<Result<GCStats>> collectAsync(const GCOptions& options = {});

    // Schedule periodic collection
    void scheduleCollection(std::chrono::seconds interval, const GCOptions& options = {});

    // Stop scheduled collection
    void stopScheduledCollection();

    // Check if collection is running
    bool isCollecting() const;

    // Get last collection stats
    GCStats getLastStats() const;

private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;
};

// Factory functions
std::unique_ptr<IReferenceCounter> createReferenceCounter(ReferenceCounter::Config config);

std::unique_ptr<GarbageCollector> createGarbageCollector(ReferenceCounter& refCounter,
                                                         StorageEngine& storageEngine);

// Utility functions
Result<void> rebuildReferenceDatabase(const std::filesystem::path& dbPath,
                                      const std::filesystem::path& storagePath);

} // namespace yams::storage