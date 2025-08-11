#pragma once

#include <yams/core/types.h>
#include <yams/compression/compressor_interface.h>
#include <yams/compression/error_handler.h>
#include <yams/compression/integrity_validator.h>
#include <chrono>
#include <string>
#include <functional>
#include <memory>
#include <optional>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <future>
#include <unordered_map>

namespace yams::compression {

/**
 * @brief Recovery operation types
 */
enum class RecoveryOperation : uint8_t {
    CompressFallback,      ///< Try alternative compression algorithm
    CompressRetry,         ///< Retry with same algorithm
    CompressUncompressed,  ///< Store uncompressed
    DecompressFallback,    ///< Try alternative decompression
    DecompressRetry,       ///< Retry decompression
    DecompressRecover,     ///< Attempt to recover partial data
    ValidateAndRepair      ///< Validate and repair corrupted data
};

/**
 * @brief Recovery operation status
 */
enum class RecoveryStatus : uint8_t {
    Pending,      ///< Operation is queued
    InProgress,   ///< Operation is being executed
    Success,      ///< Operation completed successfully
    Failed,       ///< Operation failed
    Cancelled     ///< Operation was cancelled
};

/**
 * @brief Recovery operation request
 */
struct RecoveryRequest {
    RecoveryOperation operation;              ///< Type of recovery operation
    CompressionAlgorithm originalAlgorithm;  ///< Algorithm that failed
    CompressionAlgorithm fallbackAlgorithm;  ///< Alternative algorithm to try
    std::vector<std::byte> data;             ///< Data to process
    uint8_t compressionLevel;                ///< Compression level to use
    size_t maxRetries;                       ///< Maximum retry attempts
    std::chrono::milliseconds timeout;       ///< Operation timeout
    std::string context;                     ///< Additional context information
    
    /**
     * @brief Create compression recovery request
     */
    static RecoveryRequest createCompressionRecovery(
        std::span<const std::byte> data,
        CompressionAlgorithm failedAlgorithm,
        uint8_t level = 0);
    
    /**
     * @brief Create decompression recovery request
     */
    static RecoveryRequest createDecompressionRecovery(
        std::span<const std::byte> data,
        CompressionAlgorithm algorithm,
        size_t expectedSize = 0);
};

/**
 * @brief Recovery operation result
 */
struct RecoveryOperationResult {
    RecoveryStatus status;                    ///< Final status of recovery
    RecoveryOperation operationPerformed;     ///< Operation that was performed
    std::optional<CompressionResult> compressionResult;  ///< Compression result if applicable
    std::optional<std::vector<std::byte>> decompressionResult;  ///< Decompression result
    size_t attemptsUsed;                      ///< Number of attempts made
    std::chrono::milliseconds duration;       ///< Total time taken
    std::string message;                      ///< Result message
    std::vector<std::string> diagnostics;     ///< Diagnostic information
    
    /**
     * @brief Check if recovery was successful
     */
    [[nodiscard]] bool isSuccessful() const noexcept {
        return status == RecoveryStatus::Success;
    }
    
    /**
     * @brief Format result as string
     */
    [[nodiscard]] std::string format() const;
};

/**
 * @brief Configuration for recovery manager
 */
struct RecoveryConfig {
    size_t maxConcurrentOperations = 4;       ///< Maximum concurrent recovery operations
    size_t maxQueueSize = 100;                ///< Maximum recovery queue size
    std::chrono::seconds operationTimeout{30}; ///< Default operation timeout
    bool enablePartialRecovery = true;         ///< Allow partial data recovery
    bool enableAggressiveRecovery = false;     ///< Try more aggressive recovery methods
    double corruptionThreshold = 0.3;          ///< Max acceptable corruption ratio
    size_t threadPoolSize = 2;                 ///< Recovery thread pool size
};

/**
 * @brief Callback for recovery events
 */
using RecoveryCallback = std::function<void(const RecoveryOperationResult&)>;

/**
 * @brief Comprehensive recovery manager for compression operations
 * 
 * Provides advanced recovery capabilities including fallback algorithms,
 * partial data recovery, and intelligent retry strategies.
 */
class RecoveryManager {
public:
    /**
     * @brief Constructor with configuration
     * @param config Recovery configuration
     * @param errorHandler Error handler instance
     * @param validator Integrity validator instance
     */
    RecoveryManager(
        RecoveryConfig config,
        std::shared_ptr<CompressionErrorHandler> errorHandler,
        std::shared_ptr<IntegrityValidator> validator);
    
    /**
     * @brief Destructor
     */
    ~RecoveryManager();
    
    // Non-copyable, movable
    RecoveryManager(const RecoveryManager&) = delete;
    RecoveryManager& operator=(const RecoveryManager&) = delete;
    RecoveryManager(RecoveryManager&&) noexcept;
    RecoveryManager& operator=(RecoveryManager&&) noexcept;
    
    /**
     * @brief Start recovery manager
     * @return Success or error
     */
    [[nodiscard]] Result<void> start();
    
    /**
     * @brief Stop recovery manager
     */
    void stop();
    
    /**
     * @brief Submit recovery request
     * @param request Recovery request
     * @return Recovery result future
     */
    [[nodiscard]] std::future<RecoveryOperationResult> submitRecovery(RecoveryRequest request);
    
    /**
     * @brief Perform synchronous recovery
     * @param request Recovery request
     * @return Recovery result
     */
    [[nodiscard]] RecoveryOperationResult performRecovery(const RecoveryRequest& request);
    
    /**
     * @brief Attempt to recover corrupted compressed data
     * @param corruptedData Corrupted compressed data
     * @param algorithm Algorithm used for compression
     * @param expectedSize Expected decompressed size
     * @return Recovered data or error
     */
    [[nodiscard]] Result<std::vector<std::byte>> recoverCorruptedData(
        std::span<const std::byte> corruptedData,
        CompressionAlgorithm algorithm,
        size_t expectedSize = 0);
    
    /**
     * @brief Try alternative compression strategy
     * @param data Data to compress
     * @param failedAlgorithm Algorithm that failed
     * @param level Compression level
     * @return Compression result using alternative method
     */
    [[nodiscard]] Result<CompressionResult> tryAlternativeCompression(
        std::span<const std::byte> data,
        CompressionAlgorithm failedAlgorithm,
        uint8_t level = 0);
    
    /**
     * @brief Register recovery callback
     * @param callback Function to call for recovery events
     */
    void registerRecoveryCallback(RecoveryCallback callback);
    
    /**
     * @brief Update configuration
     * @param config New configuration
     */
    void updateConfig(const RecoveryConfig& config);
    
    /**
     * @brief Get current configuration
     * @return Current configuration
     */
    [[nodiscard]] const RecoveryConfig& config() const noexcept;
    
    /**
     * @brief Get recovery statistics
     * @return Statistics map
     */
    [[nodiscard]] std::unordered_map<RecoveryOperation, size_t> getRecoveryStats() const;
    
    /**
     * @brief Get recovery success rate
     * @return Success rate as percentage
     */
    [[nodiscard]] double getSuccessRate() const;
    
    /**
     * @brief Get queue size
     * @return Number of pending recovery operations
     */
    [[nodiscard]] size_t getQueueSize() const;
    
    /**
     * @brief Check if recovery manager is running
     * @return True if running
     */
    [[nodiscard]] bool isRunning() const noexcept;
    
    /**
     * @brief Reset recovery statistics
     */
    void resetStats();
    
    /**
     * @brief Cancel all pending recovery operations
     */
    void cancelAllPending();
    
    /**
     * @brief Set algorithm preference order
     * @param algorithms Preferred algorithm order for fallback
     */
    void setAlgorithmPreference(std::vector<CompressionAlgorithm> algorithms);
    
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
 * @brief RAII helper for recovery operations
 */
class RecoveryScope {
public:
    /**
     * @brief Constructor
     * @param manager Recovery manager
     * @param operation Recovery operation type
     */
    RecoveryScope(RecoveryManager& manager, RecoveryOperation operation);
    
    /**
     * @brief Destructor - completes recovery if needed
     */
    ~RecoveryScope() noexcept;
    
    /**
     * @brief Set data for recovery
     * @param data Data to recover
     */
    void setData(std::span<const std::byte> data);
    
    /**
     * @brief Set algorithm information
     * @param original Original algorithm
     * @param fallback Fallback algorithm
     */
    void setAlgorithms(CompressionAlgorithm original, 
                      CompressionAlgorithm fallback = CompressionAlgorithm::None);
    
    /**
     * @brief Execute recovery operation
     * @return Recovery result
     */
    [[nodiscard]] RecoveryOperationResult execute();
    
    /**
     * @brief Get recovery result
     * @return Recovery result (valid after execute or destructor)
     */
    [[nodiscard]] const RecoveryOperationResult& result() const noexcept;

private:
    RecoveryManager& manager_;
    RecoveryOperation operation_;
    RecoveryRequest request_;
    RecoveryOperationResult result_;
    bool executed_ = false;
};

/**
 * @brief Recovery utility functions
 */
namespace recovery_utils {
    /**
     * @brief Determine best fallback algorithm
     * @param failedAlgorithm Algorithm that failed
     * @param dataSize Size of data
     * @return Recommended fallback algorithm
     */
    [[nodiscard]] CompressionAlgorithm selectFallbackAlgorithm(
        CompressionAlgorithm failedAlgorithm,
        size_t dataSize);
    
    /**
     * @brief Estimate recovery success probability
     * @param operation Recovery operation
     * @param previousFailures Number of previous failures
     * @return Success probability (0.0 to 1.0)
     */
    [[nodiscard]] double estimateRecoveryProbability(
        RecoveryOperation operation,
        size_t previousFailures);
    
    /**
     * @brief Analyze failure pattern
     * @param error Error that occurred
     * @return Recommended recovery operation
     */
    [[nodiscard]] RecoveryOperation analyzeFailurePattern(
        const CompressionError& error);
    
    /**
     * @brief Attempt partial data recovery
     * @param corruptedData Corrupted data
     * @param algorithm Compression algorithm used
     * @return Recovered portions of data
     */
    [[nodiscard]] std::vector<std::pair<size_t, std::vector<std::byte>>> 
    recoverPartialData(
        std::span<const std::byte> corruptedData,
        CompressionAlgorithm algorithm);
    
    /**
     * @brief Repair compressed data headers
     * @param data Compressed data with damaged headers
     * @param algorithm Algorithm used
     * @return Repaired data or empty if repair failed
     */
    [[nodiscard]] std::optional<std::vector<std::byte>> repairCompressedHeaders(
        std::span<const std::byte> data,
        CompressionAlgorithm algorithm);
}

} // namespace yams::compression