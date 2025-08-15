#pragma once

#include <yams/core/types.h>
#include <yams/wal/wal_entry.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace yams::wal {

// Forward declarations
class WALFile;
class Transaction;

/**
 * WAL Manager - Manages write-ahead logging for crash recovery
 *
 * Features:
 * - Atomic transactions with commit/rollback
 * - Automatic log rotation based on size
 * - Crash recovery with replay capability
 * - Checkpointing for faster recovery
 * - Configurable sync policies
 */
class WALManager {
public:
    // Configuration for WAL behavior
    struct Config {
        std::filesystem::path walDirectory = "./wal";
        size_t maxLogSize = 100 * 1024 * 1024;      // 100MB per log file
        size_t syncInterval = 1000;                 // Sync every N entries
        std::chrono::milliseconds syncTimeout{100}; // Max time between syncs
        bool compressOldLogs = true;                // Compress rotated logs
        size_t maxOpenFiles = 10;                   // Max concurrent log files
        bool enableGroupCommit = true;              // Batch commits for performance
    };

    // Recovery statistics
    struct RecoveryStats {
        size_t entriesProcessed = 0;
        size_t transactionsRecovered = 0;
        size_t transactionsRolledBack = 0;
        size_t errorsEncountered = 0;
        size_t bytesProcessed = 0;
        std::chrono::milliseconds duration{0};
    };

    // Constructor
    WALManager();
    explicit WALManager(Config config);
    ~WALManager();

    // Disable copy, enable move
    WALManager(const WALManager&) = delete;
    WALManager& operator=(const WALManager&) = delete;
    WALManager(WALManager&&) noexcept;
    WALManager& operator=(WALManager&&) noexcept;

    // Initialize WAL (create directories, open files)
    [[nodiscard]] Result<void> initialize();

    // Shutdown WAL (sync and close all files)
    [[nodiscard]] Result<void> shutdown();

    // Transaction management
    [[nodiscard]] std::unique_ptr<Transaction> beginTransaction();

    // Direct entry writing (non-transactional)
    [[nodiscard]] Result<uint64_t> writeEntry(const WALEntry& entry);

    // Recovery process
    using ApplyFunction = std::function<Result<void>(const WALEntry&)>;
    [[nodiscard]] Result<RecoveryStats> recover(ApplyFunction applyEntry);

    // Maintenance operations
    [[nodiscard]] Result<void> checkpoint();
    [[nodiscard]] Result<void> rotateLogs();
    [[nodiscard]] Result<void> pruneLogs(std::chrono::hours retention);

    // Force sync to disk
    [[nodiscard]] Result<void> sync();

    // Get current state
    [[nodiscard]] uint64_t getCurrentSequence() const;
    [[nodiscard]] size_t getPendingEntries() const;
    [[nodiscard]] std::filesystem::path getCurrentLogPath() const;

    // Statistics
    struct Stats {
        uint64_t totalEntries = 0;
        uint64_t totalBytes = 0;
        size_t activeTransactions = 0;
        size_t logFileCount = 0;
        std::chrono::steady_clock::time_point lastSync;
        std::chrono::steady_clock::time_point lastRotation;
    };
    [[nodiscard]] Stats getStats() const;

private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * Transaction - Represents an atomic group of operations
 *
 * Operations within a transaction are either all applied or all rolled back.
 * Transactions provide ACID guarantees for storage operations.
 */
class Transaction {
public:
    // Transaction state
    enum class State { Active, Committed, RolledBack, Failed };

    ~Transaction();

    // Disable copy
    Transaction(const Transaction&) = delete;
    Transaction& operator=(const Transaction&) = delete;

    // Enable move
    Transaction(Transaction&&) noexcept;
    Transaction& operator=(Transaction&&) noexcept;

    // Add operations to transaction
    [[nodiscard]] Result<void> storeBlock(const std::string& hash, uint32_t size,
                                          uint32_t refCount = 1);

    [[nodiscard]] Result<void> deleteBlock(const std::string& hash);

    [[nodiscard]] Result<void> updateReference(const std::string& hash, int32_t delta);

    [[nodiscard]] Result<void> updateMetadata(const std::string& hash, const std::string& key,
                                              const std::string& value);

    // Generic operation
    [[nodiscard]] Result<void> addOperation(WALEntry::OpType op, std::span<const std::byte> data);

    // Transaction control
    [[nodiscard]] Result<void> commit();
    [[nodiscard]] Result<void> rollback();

    // Transaction info
    [[nodiscard]] uint64_t getId() const noexcept;
    [[nodiscard]] State getState() const noexcept;
    [[nodiscard]] size_t getOperationCount() const noexcept;

private:
    friend class WALManager;

    // Private constructor - only WALManager can create transactions
    Transaction(WALManager* manager, uint64_t id);

    struct Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * WALFile - Represents a single WAL log file
 *
 * Handles low-level file operations with memory-mapped I/O for performance.
 * Provides iteration support for reading entries during recovery.
 */
class WALFile {
public:
    // Open modes
    enum class Mode {
        Read,     // Read-only for recovery
        Write,    // Write-only for logging
        ReadWrite // Both (used during rotation)
    };

    // Constructor
    WALFile(const std::filesystem::path& path, Mode mode);
    ~WALFile();

    // Disable copy
    WALFile(const WALFile&) = delete;
    WALFile& operator=(const WALFile&) = delete;

    // Enable move
    WALFile(WALFile&&) noexcept;
    WALFile& operator=(WALFile&&) noexcept;

    // File operations
    [[nodiscard]] Result<void> open();
    [[nodiscard]] Result<void> close();
    [[nodiscard]] bool isOpen() const;

    // Writing
    [[nodiscard]] Result<size_t> append(const WALEntry& entry);
    [[nodiscard]] Result<void> sync();

    // Reading
    class Iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = WALEntry;
        using difference_type = std::ptrdiff_t;
        using pointer = const WALEntry*;
        using reference = const WALEntry&;

        Iterator() = default;

        [[nodiscard]] bool operator==(const Iterator& other) const noexcept;
        [[nodiscard]] bool operator!=(const Iterator& other) const noexcept;

        Iterator& operator++();
        Iterator operator++(int);

        [[nodiscard]] std::optional<WALEntry> operator*() const;

    private:
        friend class WALFile;
        Iterator(WALFile* file, size_t position);

        struct Impl;
        std::shared_ptr<Impl> pImpl;
    };

    [[nodiscard]] Iterator begin();
    [[nodiscard]] Iterator end();

    // File info
    [[nodiscard]] size_t getSize() const;
    [[nodiscard]] std::filesystem::path getPath() const;
    [[nodiscard]] bool canWrite() const;

private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;
};

} // namespace yams::wal