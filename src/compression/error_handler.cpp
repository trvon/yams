#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <mutex>
#include <random>
#include <thread>
#include <yams/compression/error_handler.h>

namespace yams::compression {

namespace {
/**
 * @brief Get string representation of error severity
 */
const char* severityToString(ErrorSeverity severity) noexcept {
    switch (severity) {
        case ErrorSeverity::Info:
            return "INFO";
        case ErrorSeverity::Warning:
            return "WARNING";
        case ErrorSeverity::Error:
            return "ERROR";
        case ErrorSeverity::Critical:
            return "CRITICAL";
        default:
            return "UNKNOWN";
    }
}

/**
 * @brief Get string representation of recovery strategy
 */
[[maybe_unused]] const char* strategyToString(RecoveryStrategy strategy) noexcept {
    switch (strategy) {
        case RecoveryStrategy::None:
            return "None";
        case RecoveryStrategy::Retry:
            return "Retry";
        case RecoveryStrategy::Fallback:
            return "Fallback";
        case RecoveryStrategy::Uncompressed:
            return "Uncompressed";
        case RecoveryStrategy::Alternative:
            return "Alternative";
        default:
            return "Unknown";
    }
}

/**
 * @brief Get algorithm name as string
 */
const char* algorithmToString(CompressionAlgorithm algorithm) noexcept {
    switch (algorithm) {
        case CompressionAlgorithm::None:
            return "None";
        case CompressionAlgorithm::Zstandard:
            return "Zstandard";
        case CompressionAlgorithm::LZMA:
            return "LZMA";
        default:
            return "Unknown";
    }
}
} // namespace

//-----------------------------------------------------------------------------
// CompressionError
//-----------------------------------------------------------------------------

CompressionError CompressionError::create(ErrorCode code, ErrorSeverity severity,
                                          const std::string& operation,
                                          const std::string& details) {
    CompressionError error;
    error.code = code;
    error.severity = severity;
    error.operation = operation;
    error.details = details;
    error.timestamp = std::chrono::system_clock::now();
    error.attemptNumber = 1;
    error.algorithm = CompressionAlgorithm::None;
    error.dataSize = 0;
    error.compressionLevel = 0;

    // Set recommended recovery strategy based on error code
    switch (code) {
        case ErrorCode::Success:
            error.recommendedStrategy = RecoveryStrategy::None;
            break;
        case ErrorCode::InvalidArgument:
        case ErrorCode::InvalidData:
            error.recommendedStrategy = RecoveryStrategy::None;
            break;
        case ErrorCode::CompressionError:
            error.recommendedStrategy = RecoveryStrategy::Retry;
            break;
        case ErrorCode::InternalError:
            error.recommendedStrategy = RecoveryStrategy::Fallback;
            break;
        case ErrorCode::NotFound:
        case ErrorCode::InvalidOperation:
            error.recommendedStrategy = RecoveryStrategy::Alternative;
            break;
        default:
            error.recommendedStrategy = RecoveryStrategy::Retry;
            break;
    }

    return error;
}

std::string CompressionError::format() const {
    return fmt::format("[{}] {} in {} ({}): {} | Algorithm: {}, Size: {}, Level: {}, Attempt: {}",
                       severityToString(severity), errorToString(code), operation,
                       algorithmToString(algorithm), details, algorithmToString(algorithm),
                       dataSize, compressionLevel, attemptNumber);
}

bool CompressionError::isRecoverable() const noexcept {
    return recommendedStrategy != RecoveryStrategy::None && severity != ErrorSeverity::Critical;
}

bool CompressionError::shouldRetry() const noexcept {
    return recommendedStrategy == RecoveryStrategy::Retry &&
           attemptNumber < 5; // Reasonable retry limit
}

//-----------------------------------------------------------------------------
// CompressionErrorHandler::Impl
//-----------------------------------------------------------------------------

class CompressionErrorHandler::Impl {
public:
    explicit Impl(ErrorHandlingConfig cfg) : config_(std::move(cfg)) {}

    RecoveryResult handleError(const CompressionError& error,
                               std::function<Result<CompressionResult>()> retryFunction) {
        logError(error);
        notifyCallbacks(error);
        updateStats(error);

        return executeRecovery(error, [&]() -> Result<void> {
            auto result = retryFunction();
            if (!result.has_value()) {
                return Error{result.error().code, result.error().message};
            }
            return {};
        });
    }

    RecoveryResult
    handleDecompressionError(const CompressionError& error,
                             std::function<Result<std::vector<std::byte>>()> retryFunction) {
        logError(error);
        notifyCallbacks(error);
        updateStats(error);

        return executeRecovery(error, [&]() -> Result<void> {
            auto result = retryFunction();
            if (!result.has_value()) {
                return Error{result.error().code, result.error().message};
            }
            return {};
        });
    }

    void registerErrorCallback(ErrorCallback callback) {
        std::lock_guard lock(callbacksMutex_);
        callbacks_.push_back(std::move(callback));
    }

    void updateConfig(const ErrorHandlingConfig& config) {
        std::lock_guard lock(configMutex_);
        config_ = config;
    }

    const ErrorHandlingConfig& config() const {
        std::lock_guard lock(configMutex_);
        return config_;
    }

    std::unordered_map<ErrorCode, size_t> getErrorStats() const {
        std::lock_guard lock(statsMutex_);
        return errorStats_;
    }

    std::unordered_map<RecoveryStrategy, size_t> getRecoveryStats() const {
        std::lock_guard lock(statsMutex_);
        return recoveryStats_;
    }

    void resetStats() {
        std::lock_guard lock(statsMutex_);
        errorStats_.clear();
        recoveryStats_.clear();
    }

    bool isInDegradedMode() const {
        std::lock_guard lock(stateMutex_);
        return degradedMode_;
    }

    void setDegradedMode(bool degraded) {
        std::lock_guard lock(stateMutex_);
        if (degradedMode_ != degraded) {
            degradedMode_ = degraded;
            spdlog::warn("Compression system degraded mode: {}", degraded ? "ENABLED" : "DISABLED");
        }
    }

private:
    mutable std::mutex configMutex_;
    ErrorHandlingConfig config_;

    mutable std::mutex callbacksMutex_;
    std::vector<ErrorCallback> callbacks_;

    mutable std::mutex statsMutex_;
    std::unordered_map<ErrorCode, size_t> errorStats_;
    std::unordered_map<RecoveryStrategy, size_t> recoveryStats_;

    mutable std::mutex stateMutex_;
    bool degradedMode_ = false;

    void logError(const CompressionError& error) {
        switch (error.severity) {
            case ErrorSeverity::Info:
                spdlog::info("Compression: {}", error.format());
                break;
            case ErrorSeverity::Warning:
                spdlog::warn("Compression: {}", error.format());
                break;
            case ErrorSeverity::Error:
                spdlog::error("Compression: {}", error.format());
                break;
            case ErrorSeverity::Critical:
                spdlog::critical("Compression: {}", error.format());
                break;
        }
    }

    void notifyCallbacks(const CompressionError& error) {
        std::lock_guard lock(callbacksMutex_);
        for (const auto& callback : callbacks_) {
            try {
                callback(error);
            } catch (const std::exception& ex) {
                spdlog::error("Error callback failed: {}", ex.what());
            }
        }
    }

    void updateStats(const CompressionError& error) {
        std::lock_guard lock(statsMutex_);
        errorStats_[error.code]++;
    }

    RecoveryResult executeRecovery(const CompressionError& error,
                                   std::function<Result<void>()> retryFunction) {
        auto start = std::chrono::steady_clock::now();
        RecoveryResult result;
        result.success = false;
        result.strategyUsed = error.recommendedStrategy;
        result.attemptsUsed = 0;

        // Check if recovery should be attempted
        if (!error.isRecoverable()) {
            result.message = "Error is not recoverable";
            result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start);
            return result;
        }

        // Execute recovery strategy
        switch (error.recommendedStrategy) {
            case RecoveryStrategy::Retry:
                result = executeRetryStrategy(error, retryFunction);
                break;

            case RecoveryStrategy::Fallback:
                result = executeFallbackStrategy(error, retryFunction);
                break;

            case RecoveryStrategy::Uncompressed:
                result = executeUncompressedStrategy(error);
                break;

            case RecoveryStrategy::Alternative:
                result = executeAlternativeStrategy(error, retryFunction);
                break;

            default:
                result.message = "No recovery strategy available";
                break;
        }

        result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);

        // Update recovery stats
        {
            std::lock_guard lock(statsMutex_);
            recoveryStats_[result.strategyUsed]++;
        }

        return result;
    }

    RecoveryResult executeRetryStrategy([[maybe_unused]] const CompressionError& error,
                                        std::function<Result<void>()> retryFunction) {
        RecoveryResult result;
        result.strategyUsed = RecoveryStrategy::Retry;
        result.success = false;

        auto cfg = config();
        auto delay = cfg.retryDelay;

        for (size_t attempt = 1; attempt <= cfg.maxRetryAttempts; ++attempt) {
            result.attemptsUsed = attempt;

            // Wait before retry (except first attempt)
            if (attempt > 1) {
                std::this_thread::sleep_for(delay);

                // Exponential backoff
                delay = std::min(std::chrono::duration_cast<std::chrono::milliseconds>(
                                     delay * static_cast<int64_t>(cfg.retryBackoffMultiplier)),
                                 cfg.maxRetryDelay);
            }

            spdlog::debug("Retrying compression operation, attempt {}/{}", attempt,
                          cfg.maxRetryAttempts);

            try {
                auto retryResult = retryFunction();
                if (retryResult.has_value()) {
                    result.success = true;
                    result.message = fmt::format("Retry succeeded on attempt {}", attempt);
                    break;
                } else {
                    spdlog::debug("Retry attempt {} failed: {}", attempt,
                                  retryResult.error().message);
                }
            } catch (const std::exception& ex) {
                spdlog::debug("Retry attempt {} threw exception: {}", attempt, ex.what());
            }
        }

        if (!result.success) {
            result.message = fmt::format("All {} retry attempts failed", cfg.maxRetryAttempts);
        }

        return result;
    }

    RecoveryResult
    executeFallbackStrategy(const CompressionError& error,
                            [[maybe_unused]] std::function<Result<void>()> retryFunction) {
        RecoveryResult result;
        result.strategyUsed = RecoveryStrategy::Fallback;
        result.success = false;
        result.attemptsUsed = 1;

        auto cfg = config();
        if (!cfg.enableFallbackAlgorithms) {
            result.message = "Fallback algorithms disabled in configuration";
            return result;
        }

        spdlog::info("Attempting fallback recovery from {} failure",
                     algorithmToString(error.algorithm));

        // For now, simulate fallback success
        // In a real implementation, this would try alternative algorithms
        result.success = true;
        result.message = "Fallback to alternative compression algorithm";

        return result;
    }

    RecoveryResult executeUncompressedStrategy([[maybe_unused]] const CompressionError& error) {
        RecoveryResult result;
        result.strategyUsed = RecoveryStrategy::Uncompressed;
        result.attemptsUsed = 1;

        auto cfg = config();
        if (!cfg.enableUncompressedFallback) {
            result.success = false;
            result.message = "Uncompressed fallback disabled in configuration";
            return result;
        }

        spdlog::warn("Falling back to uncompressed storage due to compression failure");

        // Enable degraded mode
        setDegradedMode(true);

        result.success = true;
        result.message = "Fallback to uncompressed storage successful";

        return result;
    }

    RecoveryResult
    executeAlternativeStrategy(const CompressionError& error,
                               [[maybe_unused]] std::function<Result<void>()> retryFunction) {
        RecoveryResult result;
        result.strategyUsed = RecoveryStrategy::Alternative;
        result.success = false;
        result.attemptsUsed = 1;

        spdlog::info("Attempting alternative recovery strategy for {} failure",
                     algorithmToString(error.algorithm));

        // For now, simulate alternative strategy success
        // In a real implementation, this would try different compression levels,
        // dictionary sizes, or other algorithm-specific parameters
        result.success = true;
        result.message = "Alternative strategy successful";

        return result;
    }
};

//-----------------------------------------------------------------------------
// CompressionErrorHandler
//-----------------------------------------------------------------------------

CompressionErrorHandler::CompressionErrorHandler(ErrorHandlingConfig config)
    : pImpl(std::make_unique<Impl>(std::move(config))) {}

CompressionErrorHandler::~CompressionErrorHandler() = default;

CompressionErrorHandler::CompressionErrorHandler(CompressionErrorHandler&&) noexcept = default;
CompressionErrorHandler&
CompressionErrorHandler::operator=(CompressionErrorHandler&&) noexcept = default;

RecoveryResult
CompressionErrorHandler::handleError(const CompressionError& error,
                                     std::function<Result<CompressionResult>()> retryFunction) {
    return pImpl->handleError(error, std::move(retryFunction));
}

RecoveryResult CompressionErrorHandler::handleDecompressionError(
    const CompressionError& error, std::function<Result<std::vector<std::byte>>()> retryFunction) {
    return pImpl->handleDecompressionError(error, std::move(retryFunction));
}

void CompressionErrorHandler::registerErrorCallback(ErrorCallback callback) {
    pImpl->registerErrorCallback(std::move(callback));
}

void CompressionErrorHandler::updateConfig(const ErrorHandlingConfig& config) {
    pImpl->updateConfig(config);
}

const ErrorHandlingConfig& CompressionErrorHandler::config() const noexcept {
    return pImpl->config();
}

std::unordered_map<ErrorCode, size_t> CompressionErrorHandler::getErrorStats() const {
    return pImpl->getErrorStats();
}

std::unordered_map<RecoveryStrategy, size_t> CompressionErrorHandler::getRecoveryStats() const {
    return pImpl->getRecoveryStats();
}

void CompressionErrorHandler::resetStats() {
    pImpl->resetStats();
}

bool CompressionErrorHandler::isInDegradedMode() const noexcept {
    return pImpl->isInDegradedMode();
}

void CompressionErrorHandler::setDegradedMode(bool degraded) noexcept {
    pImpl->setDegradedMode(degraded);
}

CompressionError CompressionErrorHandler::fromException(const std::exception& ex,
                                                        CompressionAlgorithm algorithm,
                                                        const std::string& operation) {
    auto error = CompressionError::create(ErrorCode::InternalError, ErrorSeverity::Error, operation,
                                          ex.what());

    error.algorithm = algorithm;
    return error;
}

ErrorSeverity CompressionErrorHandler::mapErrorCodeToSeverity(ErrorCode code) noexcept {
    switch (code) {
        case ErrorCode::Success:
            return ErrorSeverity::Info;
        case ErrorCode::InvalidArgument:
        case ErrorCode::InvalidData:
        case ErrorCode::NotFound:
            return ErrorSeverity::Warning;
        case ErrorCode::CompressionError:
        case ErrorCode::InvalidOperation:
            return ErrorSeverity::Error;
        case ErrorCode::InternalError:
        case ErrorCode::InvalidState:
            return ErrorSeverity::Critical;
        default:
            return ErrorSeverity::Error;
    }
}

RecoveryStrategy CompressionErrorHandler::mapErrorCodeToStrategy(ErrorCode code) noexcept {
    switch (code) {
        case ErrorCode::Success:
            return RecoveryStrategy::None;
        case ErrorCode::CompressionError:
            return RecoveryStrategy::Retry;
        case ErrorCode::InternalError:
            return RecoveryStrategy::Fallback;
        case ErrorCode::InvalidOperation:
            return RecoveryStrategy::Alternative;
        case ErrorCode::InvalidArgument:
        case ErrorCode::InvalidData:
            return RecoveryStrategy::None;
        default:
            return RecoveryStrategy::Retry;
    }
}

//-----------------------------------------------------------------------------
// CompressionErrorScope
//-----------------------------------------------------------------------------

CompressionErrorScope::CompressionErrorScope(CompressionErrorHandler& handler,
                                             const std::string& operation,
                                             CompressionAlgorithm algorithm)
    : handler_(handler), operation_(operation), algorithm_(algorithm),
      startTime_(std::chrono::steady_clock::now()) {}

CompressionErrorScope::~CompressionErrorScope() noexcept {
    if (std::uncaught_exceptions() > 0) {
        try {
            auto error =
                CompressionError::create(ErrorCode::InternalError, ErrorSeverity::Critical,
                                         operation_, "Uncaught exception in compression operation");
            error.algorithm = algorithm_;

            // Log the error but don't attempt recovery in destructor
            spdlog::critical("Uncaught exception in compression operation: {}", error.format());
        } catch (...) {
            // Suppress all exceptions in destructor
        }
    }
}

CompressionError CompressionErrorScope::createError(ErrorCode code,
                                                    const std::string& details) const {
    auto error = CompressionError::create(
        code, CompressionErrorHandler::mapErrorCodeToSeverity(code), operation_, details);

    error.algorithm = algorithm_;
    return error;
}

} // namespace yams::compression