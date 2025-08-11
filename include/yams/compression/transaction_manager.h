#pragma once

#include <yams/core/types.h>
#include <yams/compression/compressor_interface.h>
#include <yams/compression/error_handler.h>
#include <atomic>
#include <memory>
#include <string>
#include <functional>
#include <mutex>
#include <vector>
#include <unordered_map>

namespace yams::compression {

/**
 * @brief Transaction operation type
 */
enum class TransactionOperation : uint8_t {
    Compress,           ///< Compression operation
    Decompress,         ///< Decompression operation
    CompressStream,     ///< Streaming compression
    DecompressStream,   ///< Streaming decompression
    Validate            ///< Validation operation
};

/**
 * @brief Transaction state
 */
enum class TransactionState : uint8_t {
    Pending,            ///< Transaction not yet started
    Active,             ///< Transaction in progress
    Committed,          ///< Transaction successfully committed
    Aborted,            ///< Transaction aborted
    RolledBack          ///< Transaction rolled back
};

/**
 * @brief Transaction isolation level
 */
enum class IsolationLevel : uint8_t {
    ReadUncommitted,    ///< Dirty reads allowed
    ReadCommitted,      ///< Only committed data visible
    RepeatableRead,     ///< Consistent reads within transaction
    Serializable        ///< Full serialization
};

/**
 * @brief Transaction ID type
 */
using TransactionId = uint64_t;

/**
 * @brief Transaction record
 */
struct TransactionRecord {
    TransactionId id;                     ///< Unique transaction ID
    TransactionOperation operation;       ///< Operation type
    TransactionState state;               ///< Current state
    std::chrono::steady_clock::time_point startTime;    ///< Start timestamp
    std::chrono::steady_clock::time_point endTime;      ///< End timestamp (if finished)
    CompressionAlgorithm algorithm;       ///< Algorithm used
    size_t dataSize;                      ///< Size of data processed
    std::string metadata;                 ///< Additional metadata
    std::optional<Error> error;           ///< Error if failed
    
    /**
     * @brief Get duration
     */
    [[nodiscard]] std::chrono::milliseconds duration() const {
        auto end = (state == TransactionState::Active) ? 
                   std::chrono::steady_clock::now() : endTime;
        return std::chrono::duration_cast<std::chrono::milliseconds>(end - startTime);
    }
    
    /**
     * @brief Check if transaction is complete
     */
    [[nodiscard]] bool isComplete() const noexcept {
        return state == TransactionState::Committed || 
               state == TransactionState::Aborted ||
               state == TransactionState::RolledBack;
    }
};

/**
 * @brief Transaction configuration
 */
struct TransactionConfig {
    IsolationLevel defaultIsolation = IsolationLevel::ReadCommitted;
    std::chrono::seconds transactionTimeout{300};      ///< 5 minute default timeout
    size_t maxConcurrentTransactions = 100;             ///< Max concurrent transactions
    bool enableAutoRollback = true;                     ///< Auto rollback on failure
    bool enableTransactionLog = true;                   ///< Log all transactions
    size_t logRetentionDays = 30;                      ///< Transaction log retention
};

/**
 * @brief Transaction callback
 */
using TransactionCallback = std::function<void(const TransactionRecord&)>;

/**
 * @brief Transaction-safe compression manager
 * 
 * Provides ACID guarantees for compression operations with
 * proper isolation, rollback support, and crash recovery.
 */
class TransactionManager {
public:
    /**
     * @brief Constructor
     * @param config Transaction configuration
     * @param errorHandler Error handler instance
     */
    explicit TransactionManager(
        TransactionConfig config = {},
        std::shared_ptr<CompressionErrorHandler> errorHandler = nullptr);
    
    /**
     * @brief Destructor
     */
    ~TransactionManager();
    
    // Non-copyable, movable
    TransactionManager(const TransactionManager&) = delete;
    TransactionManager& operator=(const TransactionManager&) = delete;
    TransactionManager(TransactionManager&&) noexcept;
    TransactionManager& operator=(TransactionManager&&) noexcept;
    
    /**
     * @brief Start transaction manager
     * @return Success or error
     */
    [[nodiscard]] Result<void> start();
    
    /**
     * @brief Stop transaction manager
     */
    void stop();
    
    /**
     * @brief Begin new transaction
     * @param operation Operation type
     * @param isolation Isolation level (optional)
     * @return Transaction ID or error
     */
    [[nodiscard]] Result<TransactionId> beginTransaction(
        TransactionOperation operation,
        IsolationLevel isolation = IsolationLevel::ReadCommitted);
    
    /**
     * @brief Commit transaction
     * @param txId Transaction ID
     * @return Success or error
     */
    [[nodiscard]] Result<void> commitTransaction(TransactionId txId);
    
    /**
     * @brief Abort transaction
     * @param txId Transaction ID
     * @param reason Abort reason
     * @return Success or error
     */
    [[nodiscard]] Result<void> abortTransaction(
        TransactionId txId, 
        const std::string& reason = "");
    
    /**
     * @brief Rollback transaction
     * @param txId Transaction ID
     * @return Success or error
     */
    [[nodiscard]] Result<void> rollbackTransaction(TransactionId txId);
    
    /**
     * @brief Execute compression in transaction
     * @param data Data to compress
     * @param algorithm Algorithm to use
     * @param level Compression level
     * @param txId Transaction ID (optional, creates new if not provided)
     * @return Compression result or error
     */
    [[nodiscard]] Result<CompressionResult> compressInTransaction(
        std::span<const std::byte> data,
        CompressionAlgorithm algorithm,
        uint8_t level = 0,
        std::optional<TransactionId> txId = std::nullopt);
    
    /**
     * @brief Execute decompression in transaction
     * @param data Data to decompress
     * @param algorithm Algorithm to use
     * @param expectedSize Expected size (optional)
     * @param txId Transaction ID (optional, creates new if not provided)
     * @return Decompression result or error
     */
    [[nodiscard]] Result<std::vector<std::byte>> decompressInTransaction(
        std::span<const std::byte> data,
        CompressionAlgorithm algorithm,
        size_t expectedSize = 0,
        std::optional<TransactionId> txId = std::nullopt);
    
    /**
     * @brief Get transaction state
     * @param txId Transaction ID
     * @return Transaction state
     */
    [[nodiscard]] TransactionState getTransactionState(TransactionId txId) const;
    
    /**
     * @brief Get transaction record
     * @param txId Transaction ID
     * @return Transaction record or nullopt if not found
     */
    [[nodiscard]] std::optional<TransactionRecord> getTransaction(
        TransactionId txId) const;
    
    /**
     * @brief Get active transactions
     * @return List of active transaction IDs
     */
    [[nodiscard]] std::vector<TransactionId> getActiveTransactions() const;
    
    /**
     * @brief Register transaction callback
     * @param callback Function to call on transaction events
     */
    void registerCallback(TransactionCallback callback);
    
    /**
     * @brief Update configuration
     * @param config New configuration
     */
    void updateConfig(const TransactionConfig& config);
    
    /**
     * @brief Get current configuration
     * @return Current configuration
     */
    [[nodiscard]] const TransactionConfig& config() const noexcept;
    
    /**
     * @brief Check if transaction manager is running
     * @return True if running
     */
    [[nodiscard]] bool isRunning() const noexcept;
    
    /**
     * @brief Get transaction statistics
     * @return Statistics map
     */
    [[nodiscard]] std::unordered_map<TransactionState, size_t> getStats() const;
    
    /**
     * @brief Reset transaction statistics
     */
    void resetStats();
    
    /**
     * @brief Export transaction log
     * @param startTime Start time filter
     * @param endTime End time filter
     * @return Transaction log as JSON
     */
    [[nodiscard]] std::string exportTransactionLog(
        std::chrono::system_clock::time_point startTime = {},
        std::chrono::system_clock::time_point endTime = {}) const;
    
    /**
     * @brief Get diagnostic information
     * @return Diagnostic report
     */
    [[nodiscard]] std::string getDiagnostics() const;

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * @brief RAII transaction scope
 * 
 * Automatically commits or rolls back transaction on scope exit
 */
class TransactionScope {
public:
    /**
     * @brief Constructor - begins transaction
     * @param manager Transaction manager
     * @param operation Operation type
     * @param isolation Isolation level
     */
    TransactionScope(
        TransactionManager& manager,
        TransactionOperation operation,
        IsolationLevel isolation = IsolationLevel::ReadCommitted);
    
    /**
     * @brief Destructor - commits or rolls back
     */
    ~TransactionScope() noexcept;
    
    // Non-copyable, non-movable
    TransactionScope(const TransactionScope&) = delete;
    TransactionScope& operator=(const TransactionScope&) = delete;
    TransactionScope(TransactionScope&&) = delete;
    TransactionScope& operator=(TransactionScope&&) = delete;
    
    /**
     * @brief Get transaction ID
     * @return Transaction ID
     */
    [[nodiscard]] TransactionId id() const noexcept { return txId_; }
    
    /**
     * @brief Check if transaction is valid
     * @return True if valid
     */
    [[nodiscard]] bool isValid() const noexcept { return valid_; }
    
    /**
     * @brief Commit transaction
     * @return Success or error
     */
    [[nodiscard]] Result<void> commit();
    
    /**
     * @brief Abort transaction
     * @param reason Abort reason
     * @return Success or error
     */
    [[nodiscard]] Result<void> abort(const std::string& reason = "");
    
    /**
     * @brief Mark transaction for rollback
     */
    void markForRollback() noexcept { shouldRollback_ = true; }

private:
    TransactionManager& manager_;
    TransactionId txId_;
    bool valid_ = false;
    bool committed_ = false;
    bool shouldRollback_ = false;
};

/**
 * @brief Transaction utilities
 */
namespace transaction_utils {
    /**
     * @brief Check if operation requires write lock
     * @param operation Operation type
     * @return True if write lock needed
     */
    [[nodiscard]] bool requiresWriteLock(TransactionOperation operation) noexcept;
    
    /**
     * @brief Get operation name
     * @param operation Operation type
     * @return Operation name string
     */
    [[nodiscard]] const char* operationName(TransactionOperation operation) noexcept;
    
    /**
     * @brief Get state name
     * @param state Transaction state
     * @return State name string
     */
    [[nodiscard]] const char* stateName(TransactionState state) noexcept;
    
    /**
     * @brief Get isolation level name
     * @param level Isolation level
     * @return Level name string
     */
    [[nodiscard]] const char* isolationLevelName(IsolationLevel level) noexcept;
}

} // namespace yams::compression