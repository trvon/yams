#include <yams/wal/wal_manager.h>

#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <fstream>
#include <map>
#include <optional>
#include <ranges>
#include <shared_mutex>
#include <sstream>
#include <string_view>
#include <vector>

#include <yams/compression/compressor_interface.h>
#include <yams/core/atomic_utils.h>

namespace {

bool isCompressedWalLog(const std::filesystem::path& path) {
    return path.extension() == ".zst" && path.stem().extension() == ".log";
}

bool isWalLogFile(const std::filesystem::path& path) {
    return path.extension() == ".log" || isCompressedWalLog(path);
}

std::optional<yams::compression::CompressionAlgorithm>
compressionAlgorithmForPath(const std::filesystem::path& path) {
    if (isCompressedWalLog(path)) {
        return yams::compression::CompressionAlgorithm::Zstandard;
    }
    return std::nullopt;
}

} // namespace

namespace yams::wal {

// Transaction implementation
struct Transaction::Impl {
    WALManager* manager;
    uint64_t id;
    State state = State::Active;
    std::vector<WALEntry> entries;
    mutable std::mutex mutex;

    explicit Impl(WALManager* mgr, uint64_t txnId) : manager(mgr), id(txnId) {}
};

Transaction::Transaction(WALManager* manager, uint64_t id)
    : pImpl(std::make_unique<Impl>(manager, id)) {}

Transaction::~Transaction() {
    if (pImpl && pImpl->state == State::Active) {
        // Auto-rollback if not committed (ignore result in destructor)
        [[maybe_unused]] auto result = rollback();
    }
}

Transaction::Transaction(Transaction&&) noexcept = default;
Transaction& Transaction::operator=(Transaction&&) noexcept = default;

Result<void> Transaction::storeBlock(const std::string& hash, uint32_t size, uint32_t refCount) {
    auto data = WALEntry::StoreBlockData::encode(hash, size, refCount);
    return addOperation(WALEntry::OpType::StoreBlock, data);
}

Result<void> Transaction::deleteBlock(const std::string& hash) {
    auto data = WALEntry::DeleteBlockData::encode(hash);
    return addOperation(WALEntry::OpType::DeleteBlock, data);
}

Result<void> Transaction::updateReference(const std::string& hash, int32_t delta) {
    auto data = WALEntry::UpdateReferenceData::encode(hash, delta);
    return addOperation(WALEntry::OpType::UpdateReference, data);
}

Result<void> Transaction::updateMetadata(const std::string& hash, const std::string& key,
                                         const std::string& value) {
    auto data = WALEntry::UpdateMetadataData::encode(hash, key, value);
    return addOperation(WALEntry::OpType::UpdateMetadata, data);
}

Result<void> Transaction::addOperation(WALEntry::OpType op, std::span<const std::byte> data) {
    std::lock_guard lock(pImpl->mutex);

    if (pImpl->state != State::Active) {
        return Result<void>(ErrorCode::InvalidOperation);
    }

    // Create entry (sequence number will be assigned on write)
    WALEntry entry(op, 0, pImpl->id, data);
    pImpl->entries.push_back(std::move(entry));

    return Result<void>();
}

Result<void> Transaction::commit() {
    std::lock_guard lock(pImpl->mutex);

    if (pImpl->state != State::Active) {
        return Result<void>(ErrorCode::InvalidOperation);
    }

    try {
        // Write begin transaction
        auto beginData = WALEntry::TransactionData::encode(
            pImpl->id, static_cast<uint32_t>(pImpl->entries.size()));
        WALEntry beginEntry(WALEntry::OpType::BeginTransaction, 0, pImpl->id, beginData);

        auto beginResult = pImpl->manager->writeEntry(beginEntry);
        if (!beginResult) {
            pImpl->state = State::Failed;
            return Result<void>(beginResult.error());
        }

        // Write all operations
        for (auto& entry : pImpl->entries) {
            auto result = pImpl->manager->writeEntry(entry);
            if (!result) {
                pImpl->state = State::Failed;
                return Result<void>(result.error());
            }
        }

        // Write commit
        auto commitData = WALEntry::TransactionData::encode(pImpl->id, 0);
        WALEntry commitEntry(WALEntry::OpType::CommitTransaction, 0, pImpl->id, commitData);

        auto commitResult = pImpl->manager->writeEntry(commitEntry);
        if (!commitResult) {
            pImpl->state = State::Failed;
            return Result<void>(commitResult.error());
        }

        pImpl->state = State::Committed;
        // Update WAL manager active transaction count
        if (pImpl->manager) {
            pImpl->manager->notifyTransactionClosed();
        }
        return Result<void>();

    } catch (const std::exception& e) {
        spdlog::error("Transaction commit failed: {}", e.what());
        pImpl->state = State::Failed;
        return Result<void>(ErrorCode::Unknown);
    }
}

Result<void> Transaction::rollback() {
    std::lock_guard lock(pImpl->mutex);

    if (pImpl->state != State::Active) {
        return Result<void>(ErrorCode::InvalidOperation);
    }

    try {
        // Write rollback entry
        auto rollbackData = WALEntry::TransactionData::encode(pImpl->id, 0);
        WALEntry rollbackEntry(WALEntry::OpType::Rollback, 0, pImpl->id, rollbackData);

        auto result = pImpl->manager->writeEntry(rollbackEntry);
        if (!result) {
            pImpl->state = State::Failed;
            return Result<void>(result.error());
        }

        pImpl->state = State::RolledBack;
        if (pImpl->manager) {
            pImpl->manager->notifyTransactionClosed();
        }
        return Result<void>();

    } catch (const std::exception& e) {
        spdlog::error("Transaction rollback failed: {}", e.what());
        pImpl->state = State::Failed;
        return Result<void>(ErrorCode::Unknown);
    }
}

uint64_t Transaction::getId() const noexcept {
    return pImpl->id;
}

Transaction::State Transaction::getState() const noexcept {
    std::lock_guard lock(pImpl->mutex);
    return pImpl->state;
}

size_t Transaction::getOperationCount() const noexcept {
    std::lock_guard lock(pImpl->mutex);
    return pImpl->entries.size();
}

// WAL Manager implementation
struct WALManager::Impl {
    Config config;
    std::atomic<uint64_t> sequenceNumber{0};
    std::atomic<uint64_t> transactionIdCounter{0};

    std::unique_ptr<WALFile> currentLog;
    std::filesystem::path currentLogPath;

    mutable std::shared_mutex stateMutex;
    std::deque<WALEntry> pendingEntries;
    std::atomic<uint64_t> maxExclusiveLockWaitNs{0};
    std::atomic<uint64_t> maxSharedLockWaitNs{0};

    static constexpr std::chrono::nanoseconds kExclusiveWarnThreshold{
        std::chrono::milliseconds(10)};

    static bool updateMaxAtomic(std::atomic<uint64_t>& target, uint64_t candidate) {
        auto current = target.load(std::memory_order_relaxed);
        while (candidate > current) {
            if (target.compare_exchange_weak(current, candidate, std::memory_order_relaxed)) {
                return true;
            }
        }
        return false;
    }

    void recordExclusiveLockWait(std::chrono::nanoseconds waitedNs, std::string_view context) {
        if (updateMaxAtomic(maxExclusiveLockWaitNs, static_cast<uint64_t>(waitedNs.count())) &&
            waitedNs >= kExclusiveWarnThreshold) {
            const auto waitedMs =
                std::chrono::duration_cast<std::chrono::milliseconds>(waitedNs).count();
            spdlog::warn("WAL {} exclusive lock wait {} ms (pending={}, active={})", context,
                         waitedMs, pendingEntries.size(), activeTransactions.load());
        }
    }

    void recordSharedLockWait(std::chrono::nanoseconds waitedNs) {
        updateMaxAtomic(maxSharedLockWaitNs, static_cast<uint64_t>(waitedNs.count()));
    }

    // Background sync thread
    std::thread syncThread;
    std::atomic<bool> running{false};
    std::condition_variable_any syncCv;

    // Statistics
    std::atomic<uint64_t> totalEntries{0};
    std::atomic<uint64_t> totalBytes{0};
    std::atomic<size_t> activeTransactions{0};
    std::chrono::steady_clock::time_point lastSync;
    std::chrono::steady_clock::time_point lastRotation;

    explicit Impl(Config cfg) : config(std::move(cfg)) {}

    ~Impl() {
        if (running) {
            running = false;
            syncCv.notify_all();
            if (syncThread.joinable()) {
                syncThread.join();
            }
        }
    }

    Result<void> openNewLog() {
        auto timestamp = std::chrono::system_clock::now();
        auto timeT = std::chrono::system_clock::to_time_t(timestamp);

        std::stringstream filename;
        filename << "wal_" << std::put_time(std::localtime(&timeT), "%Y%m%d_%H%M%S") << "_"
                 << sequenceNumber.load() << ".log";

        currentLogPath = config.walDirectory / filename.str();
        currentLog = std::make_unique<WALFile>(currentLogPath, WALFile::Mode::Write);

        auto result = currentLog->open();
        if (!result) {
            spdlog::error("Failed to open WAL file {}: {}", currentLogPath.string(),
                          result.error().message);
            currentLog.reset();
            return result;
        }

        lastRotation = std::chrono::steady_clock::now();
        spdlog::info("Opened new WAL file: {}", currentLogPath.string());

        return Result<void>();
    }

    void backgroundSync() {
        while (running) {
            auto lockStart = std::chrono::steady_clock::now();
            std::unique_lock lock(stateMutex);
            recordExclusiveLockWait(std::chrono::steady_clock::now() - lockStart, "backgroundSync");

            // Wait for entries or timeout
            syncCv.wait_for(lock, config.syncTimeout,
                            [this] { return !pendingEntries.empty() || !running; });

            if (!running)
                break;

            // Sync if we have entries or timeout reached
            if (!pendingEntries.empty() ||
                (std::chrono::steady_clock::now() - lastSync) > config.syncTimeout) {
                if (currentLog && currentLog->isOpen()) {
                    auto result = currentLog->sync();
                    if (result) {
                        lastSync = std::chrono::steady_clock::now();
                        pendingEntries.clear();
                    } else {
                        spdlog::error("WAL sync failed: {}", result.error().message);
                    }
                }
            }
        }
    }

    Result<uint64_t> writeEntryInternal(const WALEntry& entry) {
        // Check if log is available
        if (!currentLog || !currentLog->isOpen()) {
            return Result<uint64_t>(ErrorCode::InvalidOperation);
        }

        // Assign sequence number
        auto seqNum = sequenceNumber.fetch_add(1) + 1;

        // Create entry with sequence number
        WALEntry entryWithSeq = entry;
        entryWithSeq.header.sequenceNum = seqNum;
        entryWithSeq.updateChecksum();

        // Check if rotation needed
        if (currentLog && currentLog->getSize() > config.maxLogSize) {
            auto rotateResult = rotateLogs();
            if (!rotateResult) {
                return Result<uint64_t>(rotateResult.error());
            }
        }

        // Write to log
        auto writeResult = currentLog->append(entryWithSeq);
        if (!writeResult) {
            return Result<uint64_t>(writeResult.error());
        }

        // Update stats
        totalEntries.fetch_add(1);
        totalBytes.fetch_add(writeResult.value());

        // Add to pending for sync
        pendingEntries.push_back(entryWithSeq);

        // Notify sync thread if interval reached
        if (pendingEntries.size() >= config.syncInterval) {
            syncCv.notify_one();
        }

        return Result<uint64_t>(seqNum);
    }

    Result<std::filesystem::path> compressLogFile(const std::filesystem::path& logPath) {
        using namespace yams::compression;

        auto compressor =
            CompressionRegistry::instance().createCompressor(CompressionAlgorithm::Zstandard);
        if (!compressor) {
            return Result<std::filesystem::path>(
                Error{ErrorCode::NotSupported, "Zstandard compression is not available"});
        }

        std::ifstream input(logPath, std::ios::binary | std::ios::ate);
        if (!input) {
            return Result<std::filesystem::path>(
                Error{ErrorCode::FileNotFound,
                      fmt::format("Unable to open WAL log for compression: {}", logPath.string())});
        }

        const auto fileSize = input.tellg();
        if (fileSize < 0) {
            return Result<std::filesystem::path>(
                Error{ErrorCode::IOError,
                      fmt::format("Failed to determine size of {}", logPath.string())});
        }
        input.seekg(0, std::ios::beg);

        std::vector<std::byte> buffer(static_cast<size_t>(fileSize));
        if (!buffer.empty()) {
            input.read(reinterpret_cast<char*>(buffer.data()), fileSize);
            if (!input) {
                return Result<std::filesystem::path>(Error{
                    ErrorCode::IOError,
                    fmt::format("Failed to read WAL log {} for compression", logPath.string())});
            }
        }

        auto compressionResult =
            compressor->compress(std::span<const std::byte>(buffer.data(), buffer.size()), 0);
        if (!compressionResult) {
            return Result<std::filesystem::path>(compressionResult.error());
        }

        auto compressedPath = logPath;
        compressedPath += ".zst";
        auto tempPath = compressedPath;
        tempPath += ".tmp";

        std::ofstream output(tempPath, std::ios::binary | std::ios::trunc);
        if (!output) {
            return Result<std::filesystem::path>(
                Error{ErrorCode::WriteError,
                      fmt::format("Unable to create temporary compressed WAL log {}",
                                  tempPath.string())});
        }

        const auto& compressedData = compressionResult.value().data;
        if (!compressedData.empty()) {
            output.write(reinterpret_cast<const char*>(compressedData.data()),
                         static_cast<std::streamsize>(compressedData.size()));
            if (!output) {
                return Result<std::filesystem::path>(
                    Error{ErrorCode::WriteError,
                          fmt::format("Failed to write compressed WAL log {}", tempPath.string())});
            }
        }

        output.close();

        std::error_code ec;
        std::filesystem::rename(tempPath, compressedPath, ec);
        if (ec) {
            std::filesystem::remove(tempPath);
            return Result<std::filesystem::path>(Error{
                ErrorCode::IOError, fmt::format("Failed to finalize compressed WAL log {}: {}",
                                                compressedPath.string(), ec.message())});
        }

        std::filesystem::remove(logPath, ec);
        if (ec) {
            spdlog::warn("Unable to remove uncompressed WAL log {} after compression: {}",
                         logPath.string(), ec.message());
        }

        spdlog::info("Compressed WAL log {} -> {} ({} bytes -> {} bytes)", logPath.string(),
                     compressedPath.string(), buffer.size(), compressedData.size());

        return Result<std::filesystem::path>(compressedPath);
    }

    Result<void> rotateLogs() {
        // Close current log
        if (currentLog) {
            auto syncResult = currentLog->sync();
            if (!syncResult) {
                return syncResult;
            }

            auto closeResult = currentLog->close();
            if (!closeResult) {
                return closeResult;
            }

            // Optionally compress old log
            if (config.compressOldLogs) {
                auto compressResult = compressLogFile(currentLogPath);
                if (!compressResult) {
                    spdlog::warn("Failed to compress WAL log {}: {}", currentLogPath.string(),
                                 compressResult.error().message);
                }
            }
        }

        // Open new log
        return openNewLog();
    }
};

WALManager::WALManager() : pImpl(std::make_unique<Impl>(Config{})) {}

WALManager::WALManager(Config config) : pImpl(std::make_unique<Impl>(std::move(config))) {}

WALManager::~WALManager() = default;
WALManager::WALManager(WALManager&&) noexcept = default;
WALManager& WALManager::operator=(WALManager&&) noexcept = default;

Result<void> WALManager::initialize() {
    // Create WAL directory if needed
    if (!std::filesystem::exists(pImpl->config.walDirectory)) {
        std::error_code ec;
        std::filesystem::create_directories(pImpl->config.walDirectory, ec);
        if (ec) {
            return Result<void>(ErrorCode::PermissionDenied);
        }
    }

    // Find the latest sequence number from existing logs
    uint64_t maxSeq = 0;
    for (const auto& entry : std::filesystem::directory_iterator(pImpl->config.walDirectory)) {
        if (!isWalLogFile(entry.path())) {
            continue;
        }

        auto stemPath = entry.path().stem();
        if (isCompressedWalLog(entry.path())) {
            stemPath = stemPath.stem();
        }

        auto stem = stemPath.string();
        auto lastUnderscore = stem.find_last_of('_');
        if (lastUnderscore == std::string::npos) {
            continue;
        }

        try {
            uint64_t seq = std::stoull(stem.substr(lastUnderscore + 1));
            maxSeq = std::max(maxSeq, seq);
        } catch (...) {
            // Ignore parsing errors for unexpected filenames
        }
    }

    pImpl->sequenceNumber = maxSeq;

    // Open new log file
    auto result = pImpl->openNewLog();
    if (!result) {
        return result;
    }

    // Start background sync thread
    pImpl->running = true;
    pImpl->syncThread = std::thread(&Impl::backgroundSync, pImpl.get());

    spdlog::info("WAL manager initialized with sequence {}", maxSeq);
    return Result<void>();
}

Result<void> WALManager::shutdown() {
    // Stop background thread
    pImpl->running = false;
    pImpl->syncCv.notify_all();

    if (pImpl->syncThread.joinable()) {
        pImpl->syncThread.join();
    }

    // Final sync
    auto lockStart = std::chrono::steady_clock::now();
    std::unique_lock lock(pImpl->stateMutex);
    pImpl->recordExclusiveLockWait(std::chrono::steady_clock::now() - lockStart, "shutdown");
    if (pImpl->currentLog && pImpl->currentLog->isOpen()) {
        auto syncResult = pImpl->currentLog->sync();
        if (!syncResult) {
            spdlog::error("Failed to sync during shutdown: {}", syncResult.error().message);
            return syncResult;
        }

        auto closeResult = pImpl->currentLog->close();
        if (!closeResult) {
            spdlog::error("Failed to close log during shutdown: {}", closeResult.error().message);
            return closeResult;
        }
    }

    spdlog::info("WAL manager shutdown complete");
    return Result<void>();
}

std::unique_ptr<Transaction> WALManager::beginTransaction() {
    auto txnId = pImpl->transactionIdCounter.fetch_add(1) + 1;
    pImpl->activeTransactions.fetch_add(1);

    return std::unique_ptr<Transaction>(new Transaction(this, txnId));
}

Result<uint64_t> WALManager::writeEntry(const WALEntry& entry) {
    auto lockStart = std::chrono::steady_clock::now();
    std::unique_lock lock(pImpl->stateMutex);
    pImpl->recordExclusiveLockWait(std::chrono::steady_clock::now() - lockStart, "writeEntry");
    return pImpl->writeEntryInternal(entry);
}

Result<WALManager::RecoveryStats> WALManager::recover(ApplyFunction applyEntry) {
    RecoveryStats stats;
    auto startTime = std::chrono::steady_clock::now();

    // Find all log files (compressed and uncompressed)
    std::vector<std::filesystem::path> logFiles;
    if (std::filesystem::exists(pImpl->config.walDirectory)) {
        for (const auto& entry : std::filesystem::directory_iterator(pImpl->config.walDirectory)) {
            if (isWalLogFile(entry.path())) {
                logFiles.push_back(entry.path());
            }
        }
    }

    // Sort by modification time to maintain chronological replay order
    std::ranges::sort(logFiles, [](const auto& a, const auto& b) {
        return std::filesystem::last_write_time(a) < std::filesystem::last_write_time(b);
    });

    // Track active transactions
    std::map<uint64_t, std::vector<WALEntry>> activeTransactions;

    auto applyOperation = [&](const WALEntry& opEntry) {
        auto result = applyEntry(opEntry);
        if (!result) {
            spdlog::error("Failed to apply WAL entry (txn {}, op {}): {}",
                          opEntry.header.transactionId, static_cast<int>(opEntry.header.operation),
                          result.error().message);
            stats.errorsEncountered++;
        }
    };

    auto processEntry = [&](const WALEntry& entry) {
        stats.entriesProcessed++;
        stats.bytesProcessed += entry.totalSize();

        pImpl->sequenceNumber = std::max(pImpl->sequenceNumber.load(), entry.header.sequenceNum);

        switch (entry.header.operation) {
            case WALEntry::OpType::BeginTransaction: {
                activeTransactions[entry.header.transactionId] = {};
                break;
            }

            case WALEntry::OpType::CommitTransaction: {
                auto txnIt = activeTransactions.find(entry.header.transactionId);
                if (txnIt != activeTransactions.end()) {
                    for (const auto& txnEntry : txnIt->second) {
                        applyOperation(txnEntry);
                    }
                    activeTransactions.erase(txnIt);
                    stats.transactionsRecovered++;
                }
                break;
            }

            case WALEntry::OpType::Rollback: {
                auto txnIt = activeTransactions.find(entry.header.transactionId);
                if (txnIt != activeTransactions.end()) {
                    activeTransactions.erase(txnIt);
                    stats.transactionsRolledBack++;
                }
                break;
            }

            default: {
                if (entry.header.transactionId > 0) {
                    auto txnIt = activeTransactions.find(entry.header.transactionId);
                    if (txnIt != activeTransactions.end()) {
                        txnIt->second.push_back(entry);
                    }
                } else {
                    applyOperation(entry);
                }
                break;
            }
        }
    };

    // Process each log file
    for (const auto& logPath : logFiles) {
        spdlog::info("Recovering from log: {}", logPath.string());

        auto algorithm = compressionAlgorithmForPath(logPath);

        if (!algorithm) {
            WALFile logFile(logPath, WALFile::Mode::Read);
            auto openResult = logFile.open();
            if (!openResult) {
                spdlog::error("Failed to open log file: {}", logPath.string());
                stats.errorsEncountered++;
                continue;
            }

            for (auto it = logFile.begin(); it != logFile.end(); ++it) {
                auto entryOpt = *it;
                if (!entryOpt) {
                    stats.errorsEncountered++;
                    continue;
                }

                processEntry(entryOpt.value());
            }
        } else {
            using namespace yams::compression;

            auto compressor = CompressionRegistry::instance().createCompressor(algorithm.value());
            if (!compressor) {
                spdlog::error("No compressor available to decompress WAL log {}", logPath.string());
                stats.errorsEncountered++;
                continue;
            }

            std::ifstream input(logPath, std::ios::binary | std::ios::ate);
            if (!input) {
                spdlog::error("Failed to open compressed WAL log {}", logPath.string());
                stats.errorsEncountered++;
                continue;
            }

            const auto compressedSize = input.tellg();
            if (compressedSize < 0) {
                spdlog::error("Failed to determine size of compressed WAL log {}",
                              logPath.string());
                stats.errorsEncountered++;
                continue;
            }
            input.seekg(0, std::ios::beg);

            std::vector<std::byte> compressed(static_cast<size_t>(compressedSize));
            if (!compressed.empty()) {
                input.read(reinterpret_cast<char*>(compressed.data()), compressedSize);
                if (!input) {
                    spdlog::error("Failed to read compressed WAL log {}", logPath.string());
                    stats.errorsEncountered++;
                    continue;
                }
            }

            auto decompressedResult = compressor->decompress(
                std::span<const std::byte>(compressed.data(), compressed.size()), 0);
            if (!decompressedResult) {
                spdlog::error("Failed to decompress WAL log {}: {}", logPath.string(),
                              decompressedResult.error().message);
                stats.errorsEncountered++;
                continue;
            }

            const auto& decompressed = decompressedResult.value();
            size_t offset = 0;
            while (offset + sizeof(WALEntry::Header) <= decompressed.size()) {
                WALEntry::Header header;
                std::memcpy(&header, decompressed.data() + offset, sizeof(WALEntry::Header));

                if (!header.isValid()) {
                    spdlog::error("Invalid WAL entry header encountered in {}", logPath.string());
                    stats.errorsEncountered++;
                    break;
                }

                const size_t entrySize = sizeof(WALEntry::Header) + header.dataSize;
                if (offset + entrySize > decompressed.size()) {
                    spdlog::error("Incomplete WAL entry detected in {}", logPath.string());
                    stats.errorsEncountered++;
                    break;
                }

                auto entryOpt = WALEntry::deserialize(
                    std::span<const std::byte>(decompressed.data() + offset, entrySize));
                if (!entryOpt) {
                    spdlog::error("Failed to deserialize WAL entry in {}", logPath.string());
                    stats.errorsEncountered++;
                    break;
                }

                processEntry(entryOpt.value());
                offset += entrySize;
            }

            if (offset != decompressed.size()) {
                spdlog::warn("Compressed WAL log {} had {} trailing bytes after recovery",
                             logPath.string(), decompressed.size() - offset);
            }
        }
    }

    // Rollback any incomplete transactions
    for (const auto& [txnId, entries] : activeTransactions) {
        spdlog::warn("Rolling back incomplete transaction: {}", txnId);
        stats.transactionsRolledBack++;
    }

    stats.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - startTime);

    spdlog::info("Recovery complete: {} entries, {} transactions recovered, "
                 "{} rolled back, {} errors in {}ms",
                 stats.entriesProcessed, stats.transactionsRecovered, stats.transactionsRolledBack,
                 stats.errorsEncountered, stats.duration.count());

    return Result<RecoveryStats>(stats);
}

Result<void> WALManager::checkpoint() {
    auto lockStart = std::chrono::steady_clock::now();
    std::unique_lock lock(pImpl->stateMutex);
    pImpl->recordExclusiveLockWait(std::chrono::steady_clock::now() - lockStart, "checkpoint");

    // Create checkpoint entry
    auto checkpointData = WALEntry::CheckpointData::encode(
        pImpl->sequenceNumber.load(),
        static_cast<uint64_t>(std::chrono::system_clock::now().time_since_epoch().count()));

    WALEntry checkpointEntry(WALEntry::OpType::Checkpoint, 0, 0, checkpointData);

    auto result = pImpl->writeEntryInternal(checkpointEntry);
    if (!result) {
        return Result<void>(result.error());
    }

    // Force sync
    return pImpl->currentLog->sync();
}

Result<void> WALManager::rotateLogs() {
    auto lockStart = std::chrono::steady_clock::now();
    std::unique_lock lock(pImpl->stateMutex);
    pImpl->recordExclusiveLockWait(std::chrono::steady_clock::now() - lockStart, "rotateLogs");
    return pImpl->rotateLogs();
}

Result<void> WALManager::pruneLogs(std::chrono::hours retention) {
    auto cutoffTime = std::chrono::system_clock::now() - retention;
    size_t prunedCount = 0;

    std::filesystem::path activeLogPath;
    {
        auto lockStart = std::chrono::steady_clock::now();
        std::shared_lock lock(pImpl->stateMutex);
        pImpl->recordSharedLockWait(std::chrono::steady_clock::now() - lockStart);
        activeLogPath = pImpl->currentLogPath;
    }

    for (const auto& entry : std::filesystem::directory_iterator(pImpl->config.walDirectory)) {
        if (isWalLogFile(entry.path()) && entry.path() != activeLogPath) {
            auto lastWrite = std::filesystem::last_write_time(entry);
            auto lastWriteTime = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
                lastWrite - std::filesystem::file_time_type::clock::now() +
                std::chrono::system_clock::now());

            if (lastWriteTime < cutoffTime) {
                std::error_code ec;
                std::filesystem::remove(entry.path(), ec);
                if (!ec) {
                    prunedCount++;
                } else {
                    spdlog::error("Failed to prune log {}: {}", entry.path().string(),
                                  ec.message());
                }
            }
        }
    }

    spdlog::info("Pruned {} old log files", prunedCount);
    return Result<void>();
}

Result<void> WALManager::sync() {
    auto lockStart = std::chrono::steady_clock::now();
    std::unique_lock lock(pImpl->stateMutex);
    pImpl->recordExclusiveLockWait(std::chrono::steady_clock::now() - lockStart, "sync");

    if (pImpl->currentLog && pImpl->currentLog->isOpen()) {
        return pImpl->currentLog->sync();
    }

    return Result<void>();
}

uint64_t WALManager::getCurrentSequence() const {
    return pImpl->sequenceNumber.load();
}

size_t WALManager::getPendingEntries() const {
    auto lockStart = std::chrono::steady_clock::now();
    std::shared_lock lock(pImpl->stateMutex);
    pImpl->recordSharedLockWait(std::chrono::steady_clock::now() - lockStart);
    return pImpl->pendingEntries.size();
}

std::filesystem::path WALManager::getCurrentLogPath() const {
    auto lockStart = std::chrono::steady_clock::now();
    std::shared_lock lock(pImpl->stateMutex);
    pImpl->recordSharedLockWait(std::chrono::steady_clock::now() - lockStart);
    return pImpl->currentLogPath;
}

WALManager::Stats WALManager::getStats() const {
    Stats stats;
    stats.totalEntries = pImpl->totalEntries.load();
    stats.totalBytes = pImpl->totalBytes.load();
    stats.activeTransactions = pImpl->activeTransactions.load();
    {
        auto lockStart = std::chrono::steady_clock::now();
        std::shared_lock lock(pImpl->stateMutex);
        pImpl->recordSharedLockWait(std::chrono::steady_clock::now() - lockStart);
        stats.pendingEntriesCount = pImpl->pendingEntries.size();
    }

    // Count log files
    size_t logCount = 0;
    if (std::filesystem::exists(pImpl->config.walDirectory)) {
        for (const auto& entry : std::filesystem::directory_iterator(pImpl->config.walDirectory)) {
            if (isWalLogFile(entry.path())) {
                logCount++;
            }
        }
    }
    stats.logFileCount = logCount;

    stats.lastSync = pImpl->lastSync;
    stats.lastRotation = pImpl->lastRotation;
    stats.maxExclusiveLockWait =
        std::chrono::nanoseconds(pImpl->maxExclusiveLockWaitNs.load(std::memory_order_relaxed));
    stats.maxSharedLockWait =
        std::chrono::nanoseconds(pImpl->maxSharedLockWaitNs.load(std::memory_order_relaxed));

    return stats;
}

void WALManager::notifyTransactionClosed() {
    // Decrement active transactions atomically, avoiding underflow
    core::decrement_if_positive(pImpl->activeTransactions);
}

} // namespace yams::wal
