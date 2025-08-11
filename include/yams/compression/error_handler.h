#pragma once

#include <yams/core/types.h>
#include <yams/compression/compressor_interface.h>
#include <chrono>
#include <string>
#include <functional>
#include <memory>
#include <unordered_map>

namespace yams::compression {

/**
 * @brief Error severity levels for compression operations
 */
enum class ErrorSeverity : uint8_t {
    Info = 0,        ///< Informational - no action required
    Warning = 1,     ///< Warning - potential issue but operation continues
    Error = 2,       ///< Error - operation failed but recoverable
    Critical = 3     ///< Critical - system integrity compromised
};

/**
 * @brief Recovery strategies for handling compression errors
 */
enum class RecoveryStrategy : uint8_t {
    None = 0,           ///< No recovery - fail immediately
    Retry = 1,          ///< Retry the operation with same parameters
    Fallback = 2,       ///< Try alternative compression algorithm
    Uncompressed = 3,   ///< Fall back to uncompressed storage
    Alternative = 4     ///< Use alternative strategy (e.g., different level)
};

/**
 * @brief Detailed error information for compression operations
 */
struct CompressionError {
    ErrorCode code;                                    ///< Base error code
    ErrorSeverity severity;                           ///< Error severity level
    RecoveryStrategy recommendedStrategy;             ///< Suggested recovery strategy
    CompressionAlgorithm algorithm;                   ///< Algorithm that failed
    std::string operation;                            ///< Operation that failed
    std::string details;                              ///< Detailed error description
    size_t dataSize;                                  ///< Size of data being processed
    uint8_t compressionLevel;                         ///< Compression level used
    std::chrono::system_clock::time_point timestamp;  ///< When error occurred
    size_t attemptNumber;                             ///< Retry attempt number
    
    /**
     * @brief Create error from basic information
     */
    static CompressionError create(
        ErrorCode code,
        ErrorSeverity severity,
        const std::string& operation,
        const std::string& details = ""
    );
    
    /**
     * @brief Format error as human-readable string
     */
    [[nodiscard]] std::string format() const;
    
    /**
     * @brief Check if error is recoverable
     */
    [[nodiscard]] bool isRecoverable() const noexcept;
    
    /**
     * @brief Check if retry is recommended
     */
    [[nodiscard]] bool shouldRetry() const noexcept;
};

/**
 * @brief Configuration for error handling behavior
 */
struct ErrorHandlingConfig {
    size_t maxRetryAttempts = 3;                      ///< Maximum retry attempts
    std::chrono::milliseconds retryDelay{100};       ///< Delay between retries
    double retryBackoffMultiplier = 2.0;             ///< Exponential backoff multiplier
    std::chrono::milliseconds maxRetryDelay{5000};   ///< Maximum retry delay
    bool enableFallbackAlgorithms = true;            ///< Allow fallback to other algorithms
    bool enableUncompressedFallback = true;          ///< Allow fallback to uncompressed
    bool enableIntegrityValidation = true;           ///< Enable data integrity checks
    double corruptionThreshold = 0.01;               ///< Corruption rate threshold for alerts
    size_t validationSampleSize = 1024;              ///< Sample size for validation
};

/**
 * @brief Callback for error notifications
 */
using ErrorCallback = std::function<void(const CompressionError&)>;

/**
 * @brief Recovery action result
 */
struct RecoveryResult {
    bool success;                    ///< Whether recovery was successful
    RecoveryStrategy strategyUsed;   ///< Recovery strategy that was used
    std::string message;             ///< Result message
    size_t attemptsUsed;            ///< Number of attempts used
    std::chrono::milliseconds duration; ///< Time taken for recovery
};

/**
 * @brief Central error handler for compression operations
 * 
 * Provides comprehensive error handling, recovery, and monitoring
 * capabilities for compression operations.
 */
class CompressionErrorHandler {
public:
    /**
     * @brief Constructor with configuration
     * @param config Error handling configuration
     */
    explicit CompressionErrorHandler(ErrorHandlingConfig config = {});
    
    /**
     * @brief Destructor
     */
    ~CompressionErrorHandler();
    
    // Non-copyable, movable
    CompressionErrorHandler(const CompressionErrorHandler&) = delete;
    CompressionErrorHandler& operator=(const CompressionErrorHandler&) = delete;
    CompressionErrorHandler(CompressionErrorHandler&&) noexcept;
    CompressionErrorHandler& operator=(CompressionErrorHandler&&) noexcept;
    
    /**
     * @brief Handle compression error with automatic recovery
     * @param error Error information
     * @param retryFunction Function to retry the operation
     * @return Recovery result
     */
    [[nodiscard]] RecoveryResult handleError(
        const CompressionError& error,
        std::function<Result<CompressionResult>()> retryFunction);
    
    /**
     * @brief Handle decompression error with automatic recovery
     * @param error Error information  
     * @param retryFunction Function to retry the operation
     * @return Recovery result
     */
    [[nodiscard]] RecoveryResult handleDecompressionError(
        const CompressionError& error,
        std::function<Result<std::vector<std::byte>>()> retryFunction);
    
    /**
     * @brief Register error callback for notifications
     * @param callback Function to call when errors occur
     */
    void registerErrorCallback(ErrorCallback callback);
    
    /**
     * @brief Update configuration
     * @param config New configuration
     */
    void updateConfig(const ErrorHandlingConfig& config);
    
    /**
     * @brief Get current configuration
     * @return Current configuration
     */
    [[nodiscard]] const ErrorHandlingConfig& config() const noexcept;
    
    /**
     * @brief Get error statistics
     * @return Error statistics map
     */
    [[nodiscard]] std::unordered_map<ErrorCode, size_t> getErrorStats() const;
    
    /**
     * @brief Get recovery statistics
     * @return Recovery statistics map
     */
    [[nodiscard]] std::unordered_map<RecoveryStrategy, size_t> getRecoveryStats() const;
    
    /**
     * @brief Reset all statistics
     */
    void resetStats();
    
    /**
     * @brief Check if system is in degraded mode
     * @return True if operating with reduced functionality
     */
    [[nodiscard]] bool isInDegradedMode() const noexcept;
    
    /**
     * @brief Set degraded mode state
     * @param degraded Whether to enable degraded mode
     */
    void setDegradedMode(bool degraded) noexcept;
    
    /**
     * @brief Create compression error from exception
     * @param ex Exception that occurred
     * @param algorithm Algorithm that was being used
     * @param operation Operation that failed
     * @return Compression error
     */
    static CompressionError fromException(
        const std::exception& ex,
        CompressionAlgorithm algorithm,
        const std::string& operation);
    
    /**
     * @brief Create compression error from result
     * @param result Failed result
     * @param algorithm Algorithm that was being used
     * @param operation Operation that failed
     * @return Compression error
     */
    template<typename T>
    static CompressionError fromResult(
        const Result<T>& result,
        CompressionAlgorithm algorithm,
        const std::string& operation) {
        
        if (result.has_value()) {
            return CompressionError::create(
                ErrorCode::InvalidState,
                ErrorSeverity::Error,
                operation,
                "fromResult called with successful result"
            );
        }
        
        CompressionError error = CompressionError::create(
            result.error().code,
            mapErrorCodeToSeverity(result.error().code),
            operation,
            result.error().message
        );
        
        error.algorithm = algorithm;
        error.recommendedStrategy = mapErrorCodeToStrategy(result.error().code);
        
        return error;
    }
    
    /**
     * @brief Map error code to severity level
     */
    static ErrorSeverity mapErrorCodeToSeverity(ErrorCode code) noexcept;
    
    /**
     * @brief Map error code to recovery strategy
     */
    static RecoveryStrategy mapErrorCodeToStrategy(ErrorCode code) noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * @brief RAII helper for compression error handling
 */
class CompressionErrorScope {
public:
    /**
     * @brief Constructor
     * @param handler Error handler to use
     * @param operation Operation being performed
     * @param algorithm Algorithm being used
     */
    CompressionErrorScope(
        CompressionErrorHandler& handler,
        const std::string& operation,
        CompressionAlgorithm algorithm);
    
    /**
     * @brief Destructor - handles any unhandled exceptions
     */
    ~CompressionErrorScope() noexcept;
    
    /**
     * @brief Handle error and attempt recovery
     * @param error Error that occurred
     * @param retryFunction Function to retry operation
     * @return Recovery result
     */
    template<typename T>
    [[nodiscard]] RecoveryResult handleError(
        const CompressionError& error,
        std::function<Result<T>()> retryFunction) {
        
        if constexpr (std::is_same_v<T, CompressionResult>) {
            return handler_.handleError(error, retryFunction);
        } else {
            return handler_.handleDecompressionError(error, retryFunction);
        }
    }
    
    /**
     * @brief Create error from current context
     * @param code Error code
     * @param details Error details
     * @return Compression error
     */
    [[nodiscard]] CompressionError createError(
        ErrorCode code,
        const std::string& details = "") const;

private:
    CompressionErrorHandler& handler_;
    std::string operation_;
    CompressionAlgorithm algorithm_;
    std::chrono::steady_clock::time_point startTime_;
};

} // namespace yams::compression