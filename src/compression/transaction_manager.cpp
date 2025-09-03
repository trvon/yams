#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <optional>
#include <shared_mutex>
#include <sstream>
#include <thread>
#include <yams/compression/compression_utils.h>
#include <yams/compression/compressor_interface.h>
#include <yams/compression/transaction_manager.h>

namespace yams::compression {

//-----------------------------------------------------------------------------
// Transaction utilities
//-----------------------------------------------------------------------------

namespace transaction_utils {

bool requiresWriteLock(TransactionOperation operation) noexcept {
    switch (operation) {
        case TransactionOperation::Compress:
        case TransactionOperation::CompressStream:
            return true;
        case TransactionOperation::Decompress:
        case TransactionOperation::DecompressStream:
        case TransactionOperation::Validate:
            return false;
    }
    return false;
}

const char* operationName(TransactionOperation operation) noexcept {
    switch (operation) {
        case TransactionOperation::Compress:
            return "Compress";
        case TransactionOperation::Decompress:
            return "Decompress";
        case TransactionOperation::CompressStream:
            return "CompressStream";
        case TransactionOperation::DecompressStream:
            return "DecompressStream";
        case TransactionOperation::Validate:
            return "Validate";
        default:
            return "Unknown";
    }
}

const char* stateName(TransactionState state) noexcept {
    switch (state) {
        case TransactionState::Pending:
            return "Pending";
        case TransactionState::Active:
            return "Active";
        case TransactionState::Committed:
            return "Committed";
        case TransactionState::Aborted:
            return "Aborted";
        case TransactionState::RolledBack:
            return "RolledBack";
        default:
            return "Unknown";
    }
}

const char* isolationLevelName(IsolationLevel level) noexcept {
    switch (level) {
        case IsolationLevel::ReadUncommitted:
            return "ReadUncommitted";
        case IsolationLevel::ReadCommitted:
            return "ReadCommitted";
        case IsolationLevel::RepeatableRead:
            return "RepeatableRead";
        case IsolationLevel::Serializable:
            return "Serializable";
        default:
            return "Unknown";
    }
}

} // namespace transaction_utils

//-----------------------------------------------------------------------------
// TransactionManager::Impl
//-----------------------------------------------------------------------------

class TransactionManager::Impl {
public:
    Impl(TransactionConfig cfg, std::shared_ptr<CompressionErrorHandler> errorHandler)
        : config_(std::move(cfg)), errorHandler_(std::move(errorHandler)), nextTransactionId_(1) {}

    ~Impl() { stop(); }

    Result<void> start() {
        if (running_.load()) {
            return Error{ErrorCode::InvalidState, "Transaction manager already running"};
        }

        running_.store(true);

        // Start cleanup thread if transaction log is enabled
        if (config_.enableTransactionLog) {
            cleanupThread_ = std::thread([this] { cleanupLoop(); });
        }

        spdlog::info("Transaction manager started");
        return {};
    }

    void stop() {
        if (!running_.load()) {
            return;
        }

        running_.store(false);
        cv_.notify_all();

        if (cleanupThread_.joinable()) {
            cleanupThread_.join();
        }

        // Abort all active transactions
        abortAllActive();

        spdlog::info("Transaction manager stopped");
    }

    Result<TransactionId> beginTransaction(TransactionOperation operation,
                                           IsolationLevel isolation) {
        std::unique_lock lock(txMutex_);

        // Check transaction limit
        size_t activeCount = static_cast<size_t>(
            std::count_if(transactions_.begin(), transactions_.end(), [](const auto& pair) {
                return pair.second.state == TransactionState::Active;
            }));

        if (activeCount >= config_.maxConcurrentTransactions) {
            return Error{ErrorCode::ResourceExhausted, "Maximum concurrent transactions reached"};
        }

        // Create new transaction
        TransactionId txId = nextTransactionId_++;

        TransactionRecord record;
        record.id = txId;
        record.operation = operation;
        record.state = TransactionState::Active;
        record.startTime = std::chrono::steady_clock::now();
        record.algorithm = CompressionAlgorithm::None;
        record.dataSize = 0;

        transactions_[txId] = record;
        transactionLog_.push_back(record);

        // Update stats
        stats_[TransactionState::Active]++;

        lock.unlock();

        // Notify callbacks
        notifyCallbacks(record);

        spdlog::debug("Transaction {} started: {} with isolation {}", txId,
                      transaction_utils::operationName(operation),
                      transaction_utils::isolationLevelName(isolation));

        return txId;
    }

    Result<void> commitTransaction(TransactionId txId) {
        std::unique_lock lock(txMutex_);

        auto it = transactions_.find(txId);
        if (it == transactions_.end()) {
            return Error{ErrorCode::NotFound, "Transaction not found"};
        }

        if (it->second.state != TransactionState::Active) {
            return Error{ErrorCode::InvalidState,
                         fmt::format("Cannot commit transaction in {} state",
                                     transaction_utils::stateName(it->second.state))};
        }

        // Update transaction state
        it->second.state = TransactionState::Committed;
        it->second.endTime = std::chrono::steady_clock::now();

        // Update stats
        stats_[TransactionState::Active]--;
        stats_[TransactionState::Committed]++;

        auto record = it->second;

        // Add to log
        transactionLog_.push_back(record);

        lock.unlock();

        // Notify callbacks
        notifyCallbacks(record);

        spdlog::debug("Transaction {} committed", txId);

        return {};
    }

    Result<void> abortTransaction(TransactionId txId, const std::string& reason) {
        std::unique_lock lock(txMutex_);

        auto it = transactions_.find(txId);
        if (it == transactions_.end()) {
            return Error{ErrorCode::NotFound, "Transaction not found"};
        }

        if (it->second.state != TransactionState::Active) {
            return Error{ErrorCode::InvalidState,
                         fmt::format("Cannot abort transaction in {} state",
                                     transaction_utils::stateName(it->second.state))};
        }

        // Update transaction state
        it->second.state = TransactionState::Aborted;
        it->second.endTime = std::chrono::steady_clock::now();
        it->second.error = Error{ErrorCode::TransactionAborted, reason};

        // Update stats
        stats_[TransactionState::Active]--;
        stats_[TransactionState::Aborted]++;

        auto record = it->second;

        // Add to log
        transactionLog_.push_back(record);

        lock.unlock();

        // Notify callbacks
        notifyCallbacks(record);

        spdlog::debug("Transaction {} aborted: {}", txId, reason);

        return {};
    }

    Result<void> rollbackTransaction(TransactionId txId) {
        std::unique_lock lock(txMutex_);

        auto it = transactions_.find(txId);
        if (it == transactions_.end()) {
            return Error{ErrorCode::NotFound, "Transaction not found"};
        }

        if (it->second.state != TransactionState::Active) {
            return Error{ErrorCode::InvalidState,
                         fmt::format("Cannot rollback transaction in {} state",
                                     transaction_utils::stateName(it->second.state))};
        }

        // Update transaction state
        it->second.state = TransactionState::RolledBack;
        it->second.endTime = std::chrono::steady_clock::now();

        // Update stats
        stats_[TransactionState::Active]--;
        stats_[TransactionState::RolledBack]++;

        auto record = it->second;

        // Add to log
        transactionLog_.push_back(record);

        lock.unlock();

        // Perform rollback operations
        performRollback(record);

        // Notify callbacks
        notifyCallbacks(record);

        spdlog::debug("Transaction {} rolled back", txId);

        return {};
    }

    Result<CompressionResult> compressInTransaction(std::span<const std::byte> data,
                                                    CompressionAlgorithm algorithm,
                                                    std::optional<TransactionId> txId,
                                                    uint8_t level) {
        // Create transaction if not provided
        TransactionId actualTxId;
        bool autoCommit = false;

        if (txId.has_value()) {
            actualTxId = txId.value();
        } else {
            auto txResult =
                beginTransaction(TransactionOperation::Compress, IsolationLevel::ReadCommitted);
            if (!txResult.has_value()) {
                return Error{txResult.error().code, fmt::format("Failed to begin transaction: {}",
                                                                txResult.error().message)};
            }
            actualTxId = txResult.value();
            autoCommit = true;
        }

        // Get compressor
        auto& registry = CompressionRegistry::instance();
        auto compressor = registry.createCompressor(algorithm);
        if (!compressor) {
            if (autoCommit) {
                abortTransaction(actualTxId, "Failed to create compressor");
            }
            return Error{ErrorCode::InternalError, "Failed to create compressor"};
        }

        // Perform compression
        auto result = compressor->compress(data, level);

        // Update transaction record
        {
            std::lock_guard lock(txMutex_);
            auto it = transactions_.find(actualTxId);
            if (it != transactions_.end()) {
                it->second.algorithm = algorithm;
                it->second.dataSize = data.size();

                if (!result.has_value()) {
                    it->second.error = result.error();
                }
            }
        }

        // Handle result
        if (result.has_value()) {
            if (autoCommit) {
                commitTransaction(actualTxId);
            }
            return result;
        } else {
            if (autoCommit) {
                abortTransaction(actualTxId, result.error().message);
            }
            return result;
        }
    }

    Result<std::vector<std::byte>> decompressInTransaction(std::span<const std::byte> data,
                                                           CompressionAlgorithm algorithm,
                                                           size_t expectedSize,
                                                           std::optional<TransactionId> txId) {
        // Create transaction if not provided
        TransactionId actualTxId;
        bool autoCommit = false;

        if (txId.has_value()) {
            actualTxId = txId.value();
        } else {
            auto txResult =
                beginTransaction(TransactionOperation::Decompress, IsolationLevel::ReadCommitted);
            if (!txResult.has_value()) {
                return Error{txResult.error().code, fmt::format("Failed to begin transaction: {}",
                                                                txResult.error().message)};
            }
            actualTxId = txResult.value();
            autoCommit = true;
        }

        // Get compressor
        auto& registry = CompressionRegistry::instance();
        auto compressor = registry.createCompressor(algorithm);
        if (!compressor) {
            if (autoCommit) {
                abortTransaction(actualTxId, "Failed to create decompressor");
            }
            return Error{ErrorCode::InternalError, "Failed to create decompressor"};
        }

        // Perform decompression
        auto result = compressor->decompress(data, expectedSize);

        // Update transaction record
        {
            std::lock_guard lock(txMutex_);
            auto it = transactions_.find(actualTxId);
            if (it != transactions_.end()) {
                it->second.algorithm = algorithm;
                it->second.dataSize = data.size();

                if (!result.has_value()) {
                    it->second.error = result.error();
                }
            }
        }

        // Handle result
        if (result.has_value()) {
            if (autoCommit) {
                commitTransaction(actualTxId);
            }
            return result;
        } else {
            if (autoCommit) {
                abortTransaction(actualTxId, result.error().message);
            }
            return result;
        }
    }

    TransactionState getTransactionState(TransactionId txId) const {
        std::shared_lock lock(txMutex_);

        auto it = transactions_.find(txId);
        if (it == transactions_.end()) {
            return TransactionState::Pending; // Default for not found
        }

        return it->second.state;
    }

    std::optional<TransactionRecord> getTransaction(TransactionId txId) const {
        std::shared_lock lock(txMutex_);

        auto it = transactions_.find(txId);
        if (it == transactions_.end()) {
            return std::nullopt;
        }

        return it->second;
    }

    std::vector<TransactionId> getActiveTransactions() const {
        std::shared_lock lock(txMutex_);

        std::vector<TransactionId> active;
        for (const auto& [txId, record] : transactions_) {
            if (record.state == TransactionState::Active) {
                active.push_back(txId);
            }
        }

        return active;
    }

    void registerCallback(TransactionCallback callback) {
        std::lock_guard lock(callbacksMutex_);
        callbacks_.push_back(std::move(callback));
    }

    void updateConfig(const TransactionConfig& config) {
        std::lock_guard lock(configMutex_);
        config_ = config;
    }

    const TransactionConfig& config() const {
        std::lock_guard lock(configMutex_);
        return config_;
    }

    bool isRunning() const { return running_.load(); }

    std::unordered_map<TransactionState, size_t> getStats() const {
        std::shared_lock lock(txMutex_);
        return stats_;
    }

    void resetStats() {
        std::unique_lock lock(txMutex_);
        stats_.clear();
    }

    std::string
    exportTransactionLog([[maybe_unused]] std::pair<std::chrono::system_clock::time_point, std::chrono::system_clock::time_point> range) const {
        std::shared_lock lock(txMutex_);

        std::ostringstream oss;
        oss << "{\n  \"transactions\": [\n";

        bool first = true;
        for (const auto& record : transactionLog_) {
            if (!first)
                oss << ",\n";
            first = false;

            oss << "    {\n";
            oss << "      \"id\": " << record.id << ",\n";
            oss << "      \"operation\": \"" << transaction_utils::operationName(record.operation)
                << "\",\n";
            oss << "      \"state\": \"" << transaction_utils::stateName(record.state) << "\",\n";
            oss << "      \"algorithm\": \"" << algorithmName(record.algorithm) << "\",\n";
            oss << "      \"dataSize\": " << record.dataSize << ",\n";
            oss << "      \"duration\": " << record.duration().count() << ",\n";

            if (record.error.has_value()) {
                oss << "      \"error\": \"" << record.error.value().message << "\",\n";
            }

            oss << "      \"metadata\": \"" << record.metadata << "\"\n";
            oss << "    }";
        }

        oss << "\n  ]\n}";

        return oss.str();
    }

    std::string getDiagnostics() const {
        std::ostringstream oss;
        oss << "=== Transaction Manager Diagnostics ===\n\n";

        oss << "Status: " << (running_.load() ? "Running" : "Stopped") << "\n";

        auto stats = getStats();
        oss << "\nTransaction Statistics:\n";
        for (const auto& [state, count] : stats) {
            oss << fmt::format("  {}: {}\n", transaction_utils::stateName(state), count);
        }

        auto active = getActiveTransactions();
        oss << "\nActive Transactions: " << active.size() << "\n";
        for (auto txId : active) {
            auto tx = getTransaction(txId);
            if (tx.has_value()) {
                oss << fmt::format("  - Transaction {}: {} ({}ms)\n", txId,
                                   transaction_utils::operationName(tx->operation),
                                   tx->duration().count());
            }
        }

        oss << "\nConfiguration:\n";
        oss << fmt::format("  Default Isolation: {}\n",
                           transaction_utils::isolationLevelName(config_.defaultIsolation));
        oss << fmt::format("  Transaction Timeout: {}s\n", config_.transactionTimeout.count());
        oss << fmt::format("  Max Concurrent: {}\n", config_.maxConcurrentTransactions);
        oss << fmt::format("  Auto Rollback: {}\n", config_.enableAutoRollback);

        return oss.str();
    }

private:
    // Configuration
    mutable std::mutex configMutex_;
    TransactionConfig config_;

    // Error handler
    std::shared_ptr<CompressionErrorHandler> errorHandler_;

    // Running state
    std::atomic<bool> running_{false};

    // Transactions
    mutable std::shared_mutex txMutex_;
    std::unordered_map<TransactionId, TransactionRecord> transactions_;
    std::deque<TransactionRecord> transactionLog_;
    std::atomic<TransactionId> nextTransactionId_;

    // Statistics
    std::unordered_map<TransactionState, size_t> stats_;

    // Callbacks
    mutable std::mutex callbacksMutex_;
    std::vector<TransactionCallback> callbacks_;

    // Cleanup thread
    std::thread cleanupThread_;
    std::condition_variable cv_;
    std::mutex cleanupMutex_;

    void cleanupLoop() {
        while (running_.load()) {
            std::unique_lock<std::mutex> lock(cleanupMutex_);
            cv_.wait_for(lock, std::chrono::hours(1), [this] { return !running_.load(); });

            if (!running_.load()) {
                break;
            }

            // Clean old transaction log entries
            auto cutoffTime = std::chrono::steady_clock::now() -
                              std::chrono::hours(24 * config_.logRetentionDays);

            transactionLog_.erase(std::remove_if(transactionLog_.begin(), transactionLog_.end(),
                                                 [cutoffTime](const TransactionRecord& record) {
                                                     return record.isComplete() &&
                                                            record.endTime < cutoffTime;
                                                 }),
                                  transactionLog_.end());

            // Check for timed out transactions
            auto now = std::chrono::steady_clock::now();
            for (auto& [txId, record] : transactions_) {
                if (record.state == TransactionState::Active) {
                    auto elapsed = now - record.startTime;
                    if (elapsed > config_.transactionTimeout) {
                        spdlog::warn(
                            "Transaction {} timed out after {}s", txId,
                            std::chrono::duration_cast<std::chrono::seconds>(elapsed).count());

                        // Abort timed out transaction
                        record.state = TransactionState::Aborted;
                        record.endTime = now;
                        record.error = Error{ErrorCode::Timeout, "Transaction timed out"};

                        stats_[TransactionState::Active]--;
                        stats_[TransactionState::Aborted]++;

                        lock.unlock();
                        notifyCallbacks(record);
                        lock.lock();
                    }
                }
            }
        }
    }

    void notifyCallbacks(const TransactionRecord& record) {
        std::lock_guard lock(callbacksMutex_);
        for (const auto& callback : callbacks_) {
            try {
                callback(record);
            } catch (const std::exception& ex) {
                spdlog::error("Transaction callback failed: {}", ex.what());
            }
        }
    }

    void performRollback(const TransactionRecord& record) {
        // In a real implementation, this would undo the transaction's effects
        // For compression operations, this might involve:
        // - Removing compressed data from cache
        // - Restoring previous state
        // - Releasing resources

        spdlog::debug("Performing rollback for transaction {}", record.id);

        // For now, just log the rollback
        if (errorHandler_ && config_.enableAutoRollback) {
            CompressionError error;
            error.code = ErrorCode::TransactionAborted;
            error.severity = ErrorSeverity::Warning;
            error.recommendedStrategy = RecoveryStrategy::None;
            error.algorithm = record.algorithm;
            error.operation = "Transaction rollback";
            error.details = fmt::format("Transaction {} rolled back", record.id);
            error.dataSize = record.dataSize;
            error.compressionLevel = 0;
            error.timestamp = std::chrono::system_clock::now();
            error.attemptNumber = 1;

            // Create a dummy retry function
            auto retryFunc = []() -> Result<CompressionResult> {
                return Error{ErrorCode::InvalidOperation, "Rollback retry not supported"};
            };

            [[maybe_unused]] auto result = errorHandler_->handleError(error, retryFunc);
        }
    }

    void abortAllActive() {
        std::unique_lock lock(txMutex_);

        for (auto& [txId, record] : transactions_) {
            if (record.state == TransactionState::Active) {
                record.state = TransactionState::Aborted;
                record.endTime = std::chrono::steady_clock::now();
                record.error =
                    Error{ErrorCode::SystemShutdown, "Transaction aborted due to system shutdown"};

                stats_[TransactionState::Active]--;
                stats_[TransactionState::Aborted]++;
            }
        }
    }
};

//-----------------------------------------------------------------------------
// TransactionManager
//-----------------------------------------------------------------------------

TransactionManager::TransactionManager(TransactionConfig config,
                                       std::shared_ptr<CompressionErrorHandler> errorHandler)
    : pImpl(std::make_unique<Impl>(std::move(config), std::move(errorHandler))) {}

TransactionManager::~TransactionManager() = default;

TransactionManager::TransactionManager(TransactionManager&&) noexcept = default;
TransactionManager& TransactionManager::operator=(TransactionManager&&) noexcept = default;

Result<void> TransactionManager::start() {
    return pImpl->start();
}

void TransactionManager::stop() {
    pImpl->stop();
}

Result<TransactionId> TransactionManager::beginTransaction(TransactionOperation operation,
                                                           IsolationLevel isolation) {
    return pImpl->beginTransaction(operation, isolation);
}

Result<void> TransactionManager::commitTransaction(TransactionId txId) {
    return pImpl->commitTransaction(txId);
}

Result<void> TransactionManager::abortTransaction(TransactionId txId, const std::string& reason) {
    return pImpl->abortTransaction(txId, reason);
}

Result<void> TransactionManager::rollbackTransaction(TransactionId txId) {
    return pImpl->rollbackTransaction(txId);
}

Result<CompressionResult>
TransactionManager::compressInTransaction(std::span<const std::byte> data,
                                          CompressionAlgorithm algorithm,
                                          std::optional<TransactionId> txId, uint8_t level) {
    return pImpl->compressInTransaction(data, algorithm, txId, level);
}

Result<std::vector<std::byte>>
TransactionManager::decompressInTransaction(std::span<const std::byte> data,
                                            CompressionAlgorithm algorithm, size_t expectedSize,
                                            std::optional<TransactionId> txId) {
    return pImpl->decompressInTransaction(data, algorithm, expectedSize, txId);
}

TransactionState TransactionManager::getTransactionState(TransactionId txId) const {
    return pImpl->getTransactionState(txId);
}

std::optional<TransactionRecord> TransactionManager::getTransaction(TransactionId txId) const {
    return pImpl->getTransaction(txId);
}

std::vector<TransactionId> TransactionManager::getActiveTransactions() const {
    return pImpl->getActiveTransactions();
}

void TransactionManager::registerCallback(TransactionCallback callback) {
    pImpl->registerCallback(std::move(callback));
}

void TransactionManager::updateConfig(const TransactionConfig& config) {
    pImpl->updateConfig(config);
}

const TransactionConfig& TransactionManager::config() const noexcept {
    return pImpl->config();
}

bool TransactionManager::isRunning() const noexcept {
    return pImpl->isRunning();
}

std::unordered_map<TransactionState, size_t> TransactionManager::getStats() const {
    return pImpl->getStats();
}

void TransactionManager::resetStats() {
    pImpl->resetStats();
}

std::string
TransactionManager::exportTransactionLog(std::pair<std::chrono::system_clock::time_point, std::chrono::system_clock::time_point> range) const {
    return pImpl->exportTransactionLog(range);
}

std::string TransactionManager::getDiagnostics() const {
    return pImpl->getDiagnostics();
}

//-----------------------------------------------------------------------------
// TransactionScope
//-----------------------------------------------------------------------------

TransactionScope::TransactionScope(TransactionManager& manager, TransactionOperation operation,
                                   IsolationLevel isolation)
    : manager_(manager) {
    auto result = manager_.beginTransaction(operation, isolation);
    if (result.has_value()) {
        txId_ = result.value();
        valid_ = true;
    } else {
        spdlog::error("Failed to begin transaction: {}", result.error().message);
    }
}

TransactionScope::~TransactionScope() noexcept {
    if (!valid_) {
        return;
    }

    if (!committed_) {
        try {
            if (shouldRollback_) {
                [[maybe_unused]] auto result = manager_.rollbackTransaction(txId_);
            } else {
                [[maybe_unused]] auto result = manager_.commitTransaction(txId_);
            }
        } catch (const std::exception& e) {
            spdlog::error("Transaction scope exception: {}", e.what());
        } catch (...) {
            spdlog::error("Transaction scope exception: unknown exception");
        }
    }
}

Result<void> TransactionScope::commit() {
    if (!valid_) {
        return Error{ErrorCode::InvalidState, "Transaction scope is invalid"};
    }

    if (committed_) {
        return Error{ErrorCode::InvalidState, "Transaction already committed"};
    }

    auto result = manager_.commitTransaction(txId_);
    if (result.has_value()) {
        committed_ = true;
    }

    return result;
}

Result<void> TransactionScope::abort(const std::string& reason) {
    if (!valid_) {
        return Error{ErrorCode::InvalidState, "Transaction scope is invalid"};
    }

    if (committed_) {
        return Error{ErrorCode::InvalidState, "Transaction already committed"};
    }

    auto result = manager_.abortTransaction(txId_, reason);
    if (result.has_value()) {
        committed_ = true; // Prevent double abort/commit
    }

    return result;
}

} // namespace yams::compression
