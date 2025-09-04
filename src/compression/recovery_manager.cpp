#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <future>
#include <mutex>
#include <optional>
#include <queue>
#include <random>
#include <span>
#include <sstream>
#include <string>
#include <thread>
#include <typeinfo>
#include <vector>
#include <yams/compression/compression_utils.h>
#include <yams/compression/recovery_manager.h>

namespace yams::compression {

namespace {
/**
 * @brief Get string representation of recovery operation
 */
const char* operationToString(RecoveryOperation op) noexcept {
    switch (op) {
        case RecoveryOperation::CompressFallback:
            return "CompressFallback";
        case RecoveryOperation::CompressRetry:
            return "CompressRetry";
        case RecoveryOperation::CompressUncompressed:
            return "CompressUncompressed";
        case RecoveryOperation::DecompressFallback:
            return "DecompressFallback";
        case RecoveryOperation::DecompressRetry:
            return "DecompressRetry";
        case RecoveryOperation::DecompressRecover:
            return "DecompressRecover";
        case RecoveryOperation::ValidateAndRepair:
            return "ValidateAndRepair";
        default:
            return "Unknown";
    }
}

/**
 * @brief Get string representation of recovery status
 */
const char* statusToString(RecoveryStatus status) noexcept {
    switch (status) {
        case RecoveryStatus::Pending:
            return "Pending";
        case RecoveryStatus::InProgress:
            return "InProgress";
        case RecoveryStatus::Success:
            return "Success";
        case RecoveryStatus::Failed:
            return "Failed";
        case RecoveryStatus::Cancelled:
            return "Cancelled";
        default:
            return "Unknown";
    }
}
} // namespace

//-----------------------------------------------------------------------------
// RecoveryRequest
//-----------------------------------------------------------------------------

RecoveryRequest RecoveryRequest::createCompressionRecovery(std::span<const std::byte> data,
                                                           CompressionAlgorithm failedAlgorithm,
                                                           uint8_t level) {
    RecoveryRequest request;
    request.operation = RecoveryOperation::CompressFallback;
    request.originalAlgorithm = failedAlgorithm;
    request.fallbackAlgorithm =
        recovery_utils::selectFallbackAlgorithm(failedAlgorithm, data.size());
    request.data.assign(data.begin(), data.end());
    request.compressionLevel = level;
    request.maxRetries = 3;
    request.timeout = std::chrono::milliseconds{5000};
    request.context =
        fmt::format("Compression recovery for {} algorithm", algorithmName(failedAlgorithm));

    return request;
}

RecoveryRequest RecoveryRequest::createDecompressionRecovery(std::span<const std::byte> data,
                                                             CompressionAlgorithm algorithm,
                                                             size_t expectedSize) {
    RecoveryRequest request;
    request.operation = RecoveryOperation::DecompressRecover;
    request.originalAlgorithm = algorithm;
    request.fallbackAlgorithm = CompressionAlgorithm::None;
    request.data.assign(data.begin(), data.end());
    request.compressionLevel = 0;
    request.maxRetries = 3;
    request.timeout = std::chrono::milliseconds{10000};
    request.context = fmt::format("Decompression recovery for {} algorithm, expected size: {}",
                                  algorithmName(algorithm), expectedSize);

    return request;
}

//-----------------------------------------------------------------------------
// RecoveryOperationResult
//-----------------------------------------------------------------------------

std::string RecoveryOperationResult::format() const {
    std::ostringstream oss;
    oss << fmt::format("Recovery {}: {} operation completed in {}ms with {} attempts",
                       statusToString(status), operationToString(operationPerformed),
                       duration.count(), attemptsUsed);

    if (!message.empty()) {
        oss << " - " << message;
    }

    if (!diagnostics.empty()) {
        oss << "\nDiagnostics:";
        for (const auto& diag : diagnostics) {
            oss << "\n  - " << diag;
        }
    }

    return oss.str();
}

//-----------------------------------------------------------------------------
// RecoveryManager::Impl
//-----------------------------------------------------------------------------

class RecoveryManager::Impl {
public:
    Impl(RecoveryConfig cfg, std::shared_ptr<CompressionErrorHandler> errorHandler,
         std::shared_ptr<IntegrityValidator> validator)
        : config_(std::move(cfg)), errorHandler_(std::move(errorHandler)),
          validator_(std::move(validator)) {}

    ~Impl() { stop(); }

    Result<void> start() {
        if (running_.load()) {
            return Error{ErrorCode::InvalidState, "Recovery manager already running"};
        }

        running_.store(true);

        // Start worker threads
        for (size_t i = 0; i < config_.threadPoolSize; ++i) {
            workers_.emplace_back([this] { workerLoop(); });
        }

        spdlog::info("Recovery manager started with {} worker threads", config_.threadPoolSize);

        return {};
    }

    void stop() {
        if (!running_.load()) {
            return;
        }

        running_.store(false);
        cv_.notify_all();

        // Wait for workers to finish
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        workers_.clear();

        // Cancel pending operations
        cancelAllPending();

        spdlog::info("Recovery manager stopped");
    }

    std::future<RecoveryOperationResult> submitRecovery(RecoveryRequest request) {
        auto promise = std::make_shared<std::promise<RecoveryOperationResult>>();
        auto future = promise->get_future();

        {
            std::lock_guard lock(queueMutex_);

            if (recoveryQueue_.size() >= config_.maxQueueSize) {
                RecoveryOperationResult result;
                result.status = RecoveryStatus::Failed;
                result.message = "Recovery queue is full";
                promise->set_value(result);
                return future;
            }

            recoveryQueue_.emplace(std::move(request), std::move(promise));
        }

        cv_.notify_one();
        return future;
    }

    RecoveryOperationResult performRecovery(const RecoveryRequest& request) {
        auto start = std::chrono::steady_clock::now();

        RecoveryOperationResult result;
        result.operationPerformed = request.operation;
        result.attemptsUsed = 0;

        try {
            switch (request.operation) {
                case RecoveryOperation::CompressFallback:
                    result = performCompressionFallback(request);
                    break;

                case RecoveryOperation::CompressRetry:
                    result = performCompressionRetry(request);
                    break;

                case RecoveryOperation::CompressUncompressed:
                    result = performUncompressedStorage(request);
                    break;

                case RecoveryOperation::DecompressFallback:
                    result = performDecompressionFallback(request);
                    break;

                case RecoveryOperation::DecompressRetry:
                    result = performDecompressionRetry(request);
                    break;

                case RecoveryOperation::DecompressRecover:
                    result = performDecompressionRecovery(request);
                    break;

                case RecoveryOperation::ValidateAndRepair:
                    result = performValidateAndRepair(request);
                    break;

                default:
                    result.status = RecoveryStatus::Failed;
                    result.message = "Unknown recovery operation";
                    break;
            }

        } catch (const std::exception& ex) {
            result.status = RecoveryStatus::Failed;
            result.message = fmt::format("Recovery exception: {}", ex.what());
            result.diagnostics.push_back(fmt::format("Exception type: {}", typeid(ex).name()));
        }

        auto end = std::chrono::steady_clock::now();
        result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        // Update statistics
        updateStats(result);

        // Notify callbacks
        notifyCallbacks(result);

        return result;
    }

    Result<std::vector<std::byte>> recoverCorruptedData(std::span<const std::byte> corruptedData,
                                                        CompressionAlgorithm algorithm,
                                                        size_t expectedSize) {
        spdlog::info("Attempting to recover {} bytes of corrupted {} data", corruptedData.size(),
                     algorithmName(algorithm));

        // First try to repair headers
        auto repaired = recovery_utils::repairCompressedHeaders(corruptedData, algorithm);
        if (repaired.has_value()) {
            spdlog::info("Successfully repaired compressed headers");

            // Try to decompress repaired data
            auto& registry = CompressionRegistry::instance();
            auto compressor = registry.createCompressor(algorithm);
            if (compressor) {
                auto result = compressor->decompress(repaired.value(), expectedSize);
                if (result.has_value()) {
                    return result;
                }
            }
        }

        // Try partial recovery
        auto partialData = recovery_utils::recoverPartialData(corruptedData, algorithm);
        if (!partialData.empty()) {
            spdlog::info("Recovered {} partial data segments", partialData.size());

            // Combine partial recoveries
            std::vector<std::byte> recovered;
            for (const auto& [offset, data] : partialData) {
                recovered.insert(recovered.end(), data.begin(), data.end());
            }

            if (!recovered.empty()) {
                return recovered;
            }
        }

        return Error{ErrorCode::CorruptedData, "Unable to recover corrupted data"};
    }

    Result<CompressionResult> tryAlternativeCompression(std::span<const std::byte> data,
                                                        CompressionAlgorithm failedAlgorithm,
                                                        uint8_t level) {
        auto fallbackAlgo = recovery_utils::selectFallbackAlgorithm(failedAlgorithm, data.size());

        if (fallbackAlgo == CompressionAlgorithm::None) {
            // Store uncompressed
            CompressionResult result;
            result.data.assign(data.begin(), data.end());
            result.algorithm = CompressionAlgorithm::None;
            result.level = 0;
            result.originalSize = data.size();
            result.compressedSize = data.size();
            result.duration = std::chrono::milliseconds{0};
            return result;
        }

        auto& registry = CompressionRegistry::instance();
        auto compressor = registry.createCompressor(fallbackAlgo);
        if (!compressor) {
            return Error{ErrorCode::InternalError, fmt::format("Failed to create {} compressor",
                                                               algorithmName(fallbackAlgo))};
        }

        return compressor->compress(data, level);
    }

    void registerRecoveryCallback(RecoveryCallback callback) {
        std::lock_guard lock(callbacksMutex_);
        callbacks_.push_back(std::move(callback));
    }

    void updateConfig(const RecoveryConfig& config) {
        std::lock_guard lock(configMutex_);
        config_ = config;
    }

    const RecoveryConfig& config() const {
        std::lock_guard lock(configMutex_);
        return config_;
    }

    std::unordered_map<RecoveryOperation, size_t> getRecoveryStats() const {
        std::lock_guard lock(statsMutex_);
        return operationStats_;
    }

    double getSuccessRate() const {
        std::lock_guard lock(statsMutex_);
        if (totalOperations_ == 0) {
            return 0.0;
        }
        return static_cast<double>(successfulOperations_) / static_cast<double>(totalOperations_);
    }

    size_t getQueueSize() const {
        std::lock_guard lock(queueMutex_);
        return recoveryQueue_.size();
    }

    bool isRunning() const { return running_.load(); }

    void resetStats() {
        std::lock_guard lock(statsMutex_);
        operationStats_.clear();
        totalOperations_ = 0;
        successfulOperations_ = 0;
    }

    void cancelAllPending() {
        std::lock_guard lock(queueMutex_);

        while (!recoveryQueue_.empty()) {
            auto& [request, promise] = recoveryQueue_.front();

            RecoveryOperationResult result;
            result.status = RecoveryStatus::Cancelled;
            result.operationPerformed = request.operation;
            result.message = "Operation cancelled";

            promise->set_value(result);
            recoveryQueue_.pop();
        }
    }

    void setAlgorithmPreference(std::vector<CompressionAlgorithm> algorithms) {
        std::lock_guard lock(configMutex_);
        algorithmPreference_ = std::move(algorithms);
    }

    std::string getDiagnostics() const {
        std::ostringstream oss;
        oss << "=== Recovery Manager Diagnostics ===\n\n";

        oss << "Status: " << (running_.load() ? "Running" : "Stopped") << "\n";
        oss << "Queue size: " << getQueueSize() << "\n";
        oss << "Success rate: " << fmt::format("{:.1f}%", getSuccessRate() * 100) << "\n";
        oss << "Total operations: " << totalOperations_ << "\n";
        oss << "Successful operations: " << successfulOperations_ << "\n\n";

        oss << "Operation Statistics:\n";
        auto stats = getRecoveryStats();
        for (const auto& [op, count] : stats) {
            oss << fmt::format("  {}: {}\n", operationToString(op), count);
        }

        return oss.str();
    }

private:
    // Configuration
    mutable std::mutex configMutex_;
    RecoveryConfig config_;
    std::vector<CompressionAlgorithm> algorithmPreference_;

    // Dependencies
    std::shared_ptr<CompressionErrorHandler> errorHandler_;
    std::shared_ptr<IntegrityValidator> validator_;

    // Worker threads
    std::atomic<bool> running_{false};
    std::vector<std::thread> workers_;

    // Recovery queue
    using RecoveryTask =
        std::pair<RecoveryRequest, std::shared_ptr<std::promise<RecoveryOperationResult>>>;
    mutable std::mutex queueMutex_;
    std::condition_variable cv_;
    std::queue<RecoveryTask> recoveryQueue_;

    // Callbacks
    mutable std::mutex callbacksMutex_;
    std::vector<RecoveryCallback> callbacks_;

    // Statistics
    mutable std::mutex statsMutex_;
    std::unordered_map<RecoveryOperation, size_t> operationStats_;
    size_t totalOperations_ = 0;
    size_t successfulOperations_ = 0;

    void workerLoop() {
        while (running_.load()) {
            RecoveryTask task;

            {
                std::unique_lock lock(queueMutex_);
                cv_.wait(lock, [this] { return !recoveryQueue_.empty() || !running_.load(); });

                if (!running_.load()) {
                    break;
                }

                if (recoveryQueue_.empty()) {
                    continue;
                }

                task = std::move(recoveryQueue_.front());
                recoveryQueue_.pop();
            }

            // Perform recovery
            auto result = performRecovery(task.first);
            task.second->set_value(result);
        }
    }

    void updateStats(const RecoveryOperationResult& result) {
        std::lock_guard lock(statsMutex_);
        operationStats_[result.operationPerformed]++;
        totalOperations_++;

        if (result.status == RecoveryStatus::Success) {
            successfulOperations_++;
        }
    }

    void notifyCallbacks(const RecoveryOperationResult& result) {
        std::lock_guard lock(callbacksMutex_);
        for (const auto& callback : callbacks_) {
            try {
                callback(result);
            } catch (const std::exception& ex) {
                spdlog::error("Recovery callback failed: {}", ex.what());
            }
        }
    }

    RecoveryOperationResult performCompressionFallback(const RecoveryRequest& request) {
        RecoveryOperationResult result;
        result.operationPerformed = RecoveryOperation::CompressFallback;

        auto compressionResult = tryAlternativeCompression(request.data, request.originalAlgorithm,
                                                           request.compressionLevel);

        if (compressionResult.has_value()) {
            result.status = RecoveryStatus::Success;
            result.compressionResult = compressionResult.value();
            result.message = fmt::format("Fallback to {} successful",
                                         algorithmName(compressionResult.value().algorithm));
            result.attemptsUsed = 1;
        } else {
            result.status = RecoveryStatus::Failed;
            result.message =
                fmt::format("Fallback compression failed: {}", compressionResult.error().message);
            result.attemptsUsed = 1;
        }

        return result;
    }

    RecoveryOperationResult performCompressionRetry(const RecoveryRequest& request) {
        RecoveryOperationResult result;
        result.operationPerformed = RecoveryOperation::CompressRetry;

        auto& registry = CompressionRegistry::instance();
        auto compressor = registry.createCompressor(request.originalAlgorithm);

        if (!compressor) {
            result.status = RecoveryStatus::Failed;
            result.message = "Failed to create compressor";
            result.attemptsUsed = 0;
            return result;
        }

        size_t attempts = 0;
        for (size_t i = 0; i < request.maxRetries; ++i) {
            attempts++;

            // Add delay between retries
            if (i > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100 * i));
            }

            auto compressionResult = compressor->compress(request.data, request.compressionLevel);

            if (compressionResult.has_value()) {
                result.status = RecoveryStatus::Success;
                result.compressionResult = compressionResult.value();
                result.message = fmt::format("Retry successful on attempt {}", attempts);
                result.attemptsUsed = attempts;
                return result;
            }

            result.diagnostics.push_back(
                fmt::format("Attempt {} failed: {}", attempts, compressionResult.error().message));
        }

        result.status = RecoveryStatus::Failed;
        result.message = fmt::format("All {} retry attempts failed", attempts);
        result.attemptsUsed = attempts;

        return result;
    }

    RecoveryOperationResult performUncompressedStorage(const RecoveryRequest& request) {
        RecoveryOperationResult result;
        result.operationPerformed = RecoveryOperation::CompressUncompressed;

        // Create uncompressed result
        CompressionResult uncompressed;
        uncompressed.data = request.data;
        uncompressed.algorithm = CompressionAlgorithm::None;
        uncompressed.level = 0;
        uncompressed.originalSize = request.data.size();
        uncompressed.compressedSize = request.data.size();
        uncompressed.duration = std::chrono::milliseconds{0};

        result.status = RecoveryStatus::Success;
        result.compressionResult = uncompressed;
        result.message = "Stored as uncompressed data";
        result.attemptsUsed = 1;

        // Enable degraded mode in error handler
        if (errorHandler_) {
            errorHandler_->setDegradedMode(true);
        }

        return result;
    }

    RecoveryOperationResult performDecompressionFallback(const RecoveryRequest& request) {
        RecoveryOperationResult result;
        result.operationPerformed = RecoveryOperation::DecompressFallback;

        // Try different algorithms
        std::vector<CompressionAlgorithm> algorithms = {CompressionAlgorithm::Zstandard,
                                                        CompressionAlgorithm::LZMA};

        // Remove the original algorithm
        algorithms.erase(
            std::remove(algorithms.begin(), algorithms.end(), request.originalAlgorithm),
            algorithms.end());

        auto& registry = CompressionRegistry::instance();

        for (auto algo : algorithms) {
            auto compressor = registry.createCompressor(algo);
            if (!compressor) {
                continue;
            }

            auto decompressResult = compressor->decompress(request.data);
            if (decompressResult.has_value()) {
                result.status = RecoveryStatus::Success;
                result.decompressionResult = decompressResult.value();
                result.message =
                    fmt::format("Decompressed using {} algorithm", algorithmName(algo));
                result.attemptsUsed = 1;
                return result;
            }

            result.diagnostics.push_back(
                fmt::format("{} decompression failed", algorithmName(algo)));
        }

        result.status = RecoveryStatus::Failed;
        result.message = "All fallback decompression attempts failed";
        result.attemptsUsed = algorithms.size();

        return result;
    }

    RecoveryOperationResult performDecompressionRetry(const RecoveryRequest& request) {
        // Similar to compression retry
        RecoveryOperationResult result;
        result.operationPerformed = RecoveryOperation::DecompressRetry;

        auto& registry = CompressionRegistry::instance();
        auto compressor = registry.createCompressor(request.originalAlgorithm);

        if (!compressor) {
            result.status = RecoveryStatus::Failed;
            result.message = "Failed to create decompressor";
            result.attemptsUsed = 0;
            return result;
        }

        size_t attempts = 0;
        for (size_t i = 0; i < request.maxRetries; ++i) {
            attempts++;

            if (i > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100 * i));
            }

            auto decompressResult = compressor->decompress(request.data);

            if (decompressResult.has_value()) {
                result.status = RecoveryStatus::Success;
                result.decompressionResult = decompressResult.value();
                result.message = fmt::format("Retry successful on attempt {}", attempts);
                result.attemptsUsed = attempts;
                return result;
            }
        }

        result.status = RecoveryStatus::Failed;
        result.message = fmt::format("All {} retry attempts failed", attempts);
        result.attemptsUsed = attempts;

        return result;
    }

    RecoveryOperationResult performDecompressionRecovery(const RecoveryRequest& request) {
        RecoveryOperationResult result;
        result.operationPerformed = RecoveryOperation::DecompressRecover;

        auto recoveredData = recoverCorruptedData(request.data, request.originalAlgorithm, 0);

        if (recoveredData.has_value()) {
            result.status = RecoveryStatus::Success;
            result.decompressionResult = recoveredData.value();
            result.message =
                fmt::format("Recovered {} bytes of data", recoveredData.value().size());
            result.attemptsUsed = 1;
        } else {
            result.status = RecoveryStatus::Failed;
            result.message = recoveredData.error().message;
            result.attemptsUsed = 1;
        }

        return result;
    }

    RecoveryOperationResult performValidateAndRepair(const RecoveryRequest& request) {
        RecoveryOperationResult result;
        result.operationPerformed = RecoveryOperation::ValidateAndRepair;

        // Validate data integrity
        auto validationResult =
            validator_->performDeepAnalysis(request.data, request.originalAlgorithm);

        if (validationResult.isValid) {
            result.status = RecoveryStatus::Success;
            result.message = "Data validation passed";
            result.attemptsUsed = 1;
            return result;
        }

        // Attempt repair
        auto repaired =
            recovery_utils::repairCompressedHeaders(request.data, request.originalAlgorithm);

        if (repaired.has_value()) {
            // Validate repaired data
            auto revalidation =
                validator_->performDeepAnalysis(repaired.value(), request.originalAlgorithm);

            if (revalidation.isValid) {
                result.status = RecoveryStatus::Success;
                result.message = "Data repaired and validated successfully";
                result.attemptsUsed = 1;

                // Create compression result with repaired data
                CompressionResult compressionResult;
                compressionResult.data = repaired.value();
                compressionResult.algorithm = request.originalAlgorithm;
                compressionResult.level = 3; // Default level per performance report recommendation
                compressionResult.originalSize = 0; // Unknown
                compressionResult.compressedSize = repaired.value().size();
                result.compressionResult = compressionResult;
            } else {
                result.status = RecoveryStatus::Failed;
                result.message = "Repair attempted but validation still fails";
                result.attemptsUsed = 1;
            }
        } else {
            result.status = RecoveryStatus::Failed;
            result.message = "Unable to repair compressed data";
            result.attemptsUsed = 1;
        }

        return result;
    }
};

//-----------------------------------------------------------------------------
// RecoveryManager
//-----------------------------------------------------------------------------

RecoveryManager::RecoveryManager(RecoveryConfig config,
                                 std::shared_ptr<CompressionErrorHandler> errorHandler,
                                 std::shared_ptr<IntegrityValidator> validator)
    : pImpl(std::make_unique<Impl>(std::move(config), std::move(errorHandler),
                                   std::move(validator))) {}

RecoveryManager::~RecoveryManager() = default;

RecoveryManager::RecoveryManager(RecoveryManager&&) noexcept = default;
RecoveryManager& RecoveryManager::operator=(RecoveryManager&&) noexcept = default;

Result<void> RecoveryManager::start() {
    return pImpl->start();
}

void RecoveryManager::stop() {
    pImpl->stop();
}

std::future<RecoveryOperationResult> RecoveryManager::submitRecovery(RecoveryRequest request) {
    return pImpl->submitRecovery(std::move(request));
}

RecoveryOperationResult RecoveryManager::performRecovery(const RecoveryRequest& request) {
    return pImpl->performRecovery(request);
}

Result<std::vector<std::byte>>
RecoveryManager::recoverCorruptedData(std::span<const std::byte> corruptedData,
                                      CompressionAlgorithm algorithm, size_t expectedSize) {
    return pImpl->recoverCorruptedData(corruptedData, algorithm, expectedSize);
}

Result<CompressionResult>
RecoveryManager::tryAlternativeCompression(std::span<const std::byte> data,
                                           CompressionAlgorithm failedAlgorithm, uint8_t level) {
    return pImpl->tryAlternativeCompression(data, failedAlgorithm, level);
}

void RecoveryManager::registerRecoveryCallback(RecoveryCallback callback) {
    pImpl->registerRecoveryCallback(std::move(callback));
}

void RecoveryManager::updateConfig(const RecoveryConfig& config) {
    pImpl->updateConfig(config);
}

const RecoveryConfig& RecoveryManager::config() const noexcept {
    return pImpl->config();
}

std::unordered_map<RecoveryOperation, size_t> RecoveryManager::getRecoveryStats() const {
    return pImpl->getRecoveryStats();
}

double RecoveryManager::getSuccessRate() const {
    return pImpl->getSuccessRate();
}

size_t RecoveryManager::getQueueSize() const {
    return pImpl->getQueueSize();
}

bool RecoveryManager::isRunning() const noexcept {
    return pImpl->isRunning();
}

void RecoveryManager::resetStats() {
    pImpl->resetStats();
}

void RecoveryManager::cancelAllPending() {
    pImpl->cancelAllPending();
}

void RecoveryManager::setAlgorithmPreference(std::vector<CompressionAlgorithm> algorithms) {
    pImpl->setAlgorithmPreference(std::move(algorithms));
}

std::string RecoveryManager::getDiagnostics() const {
    return pImpl->getDiagnostics();
}

//-----------------------------------------------------------------------------
// RecoveryScope
//-----------------------------------------------------------------------------

RecoveryScope::RecoveryScope(RecoveryManager& manager, RecoveryOperation operation)
    : manager_(manager), operation_(operation) {
    request_.operation = operation;
    result_.status = RecoveryStatus::InProgress;
    result_.operationPerformed = operation;
    result_.attemptsUsed = 0;
    result_.duration = std::chrono::milliseconds{0};
}

RecoveryScope::~RecoveryScope() noexcept {
    if (!executed_ && !request_.data.empty()) {
        try {
            result_ = manager_.performRecovery(request_);
        } catch (const std::exception& e) {
            spdlog::error("Recovery scope exception: {}", e.what());
        } catch (...) {
            spdlog::error("Recovery scope exception: unknown exception");
        }
    }
}

void RecoveryScope::setData(std::span<const std::byte> data) {
    request_.data.assign(data.begin(), data.end());
}

void RecoveryScope::setAlgorithms(CompressionAlgorithm original,
                                  std::optional<CompressionAlgorithm> fallback) {
    request_.originalAlgorithm = original;
    request_.fallbackAlgorithm = fallback.value_or(CompressionAlgorithm::None);
}

RecoveryOperationResult RecoveryScope::execute() {
    if (!executed_) {
        result_ = manager_.performRecovery(request_);
        executed_ = true;
    }
    return result_;
}

const RecoveryOperationResult& RecoveryScope::result() const noexcept {
    return result_;
}

//-----------------------------------------------------------------------------
// Recovery utility functions
//-----------------------------------------------------------------------------

namespace recovery_utils {

CompressionAlgorithm selectFallbackAlgorithm(CompressionAlgorithm failedAlgorithm,
                                             size_t dataSize) {
    // Simple heuristic for fallback selection
    if (failedAlgorithm == CompressionAlgorithm::LZMA) {
        // LZMA failed, try faster Zstandard
        return CompressionAlgorithm::Zstandard;
    } else if (failedAlgorithm == CompressionAlgorithm::Zstandard) {
        // Zstandard failed, try LZMA for better compression
        if (dataSize < 1024ULL * 1024ULL) {    // Small data
            return CompressionAlgorithm::None; // Store uncompressed
        }
        return CompressionAlgorithm::LZMA;
    }

    // Default to no compression
    return CompressionAlgorithm::None;
}

double estimateRecoveryProbability(RecoveryOperation operation, size_t previousFailures) {
    // Base probabilities for each operation
    double baseProbability = 0.5;

    switch (operation) {
        case RecoveryOperation::CompressRetry:
        case RecoveryOperation::DecompressRetry:
            baseProbability = 0.7; // Retries often succeed
            break;

        case RecoveryOperation::CompressFallback:
        case RecoveryOperation::DecompressFallback:
            baseProbability = 0.8; // Fallback algorithms usually work
            break;

        case RecoveryOperation::CompressUncompressed:
            baseProbability = 1.0; // Always succeeds
            break;

        case RecoveryOperation::DecompressRecover:
            baseProbability = 0.3; // Recovery is difficult
            break;

        case RecoveryOperation::ValidateAndRepair:
            baseProbability = 0.4; // Repair success varies
            break;
    }

    // Reduce probability with each failure
    double failurePenalty = 0.1 * static_cast<double>(previousFailures);
    return std::max(0.0, baseProbability - failurePenalty);
}

RecoveryOperation analyzeFailurePattern(const CompressionError& error) {
    // Analyze error to determine best recovery strategy
    switch (error.code) {
        case ErrorCode::CompressionError:
            if (error.attemptNumber < 3) {
                return RecoveryOperation::CompressRetry;
            } else {
                return RecoveryOperation::CompressFallback;
            }

        case ErrorCode::CorruptedData:
            return RecoveryOperation::ValidateAndRepair;

        case ErrorCode::InternalError:
            return RecoveryOperation::CompressFallback;

        case ErrorCode::InvalidData:
            return RecoveryOperation::DecompressRecover;

        default:
            return RecoveryOperation::CompressRetry;
    }
}

std::vector<std::pair<size_t, std::vector<std::byte>>>
recoverPartialData(std::span<const std::byte> corruptedData, CompressionAlgorithm algorithm) {
    std::vector<std::pair<size_t, std::vector<std::byte>>> recovered;

    // Algorithm-specific partial recovery
    if (algorithm == CompressionAlgorithm::Zstandard) {
        // Zstandard frame-based recovery
        size_t offset = 0;
        while (offset + 4 < corruptedData.size()) {
            // Look for Zstandard magic number
            if (corruptedData[offset] == std::byte{0x28} &&
                corruptedData[offset + 1] == std::byte{0xB5} &&
                corruptedData[offset + 2] == std::byte{0x2F} &&
                corruptedData[offset + 3] == std::byte{0xFD}) {
                // Found potential frame start
                auto& registry = CompressionRegistry::instance();
                auto compressor = registry.createCompressor(CompressionAlgorithm::Zstandard);

                if (compressor) {
                    // Try to decompress from this offset
                    for (size_t len = 100; offset + len <= corruptedData.size(); len += 100) {
                        auto result = compressor->decompress(corruptedData.subspan(offset, len));

                        if (result.has_value()) {
                            recovered.emplace_back(offset, result.value());
                            offset += len;
                            break;
                        }
                    }
                }
            }
            offset++;
        }
    }

    return recovered;
}

std::optional<std::vector<std::byte>> repairCompressedHeaders(std::span<const std::byte> data,
                                                              CompressionAlgorithm algorithm) {
    if (data.empty()) {
        return std::nullopt;
    }

    std::vector<std::byte> repaired(data.begin(), data.end());

    if (algorithm == CompressionAlgorithm::Zstandard) {
        // Repair Zstandard header
        if (data.size() >= 4) {
            // Fix magic number if corrupted
            if (data[0] != std::byte{0x28} || data[1] != std::byte{0xB5} ||
                data[2] != std::byte{0x2F} || data[3] != std::byte{0xFD}) {
                repaired[0] = std::byte{0x28};
                repaired[1] = std::byte{0xB5};
                repaired[2] = std::byte{0x2F};
                repaired[3] = std::byte{0xFD};

                spdlog::debug("Repaired Zstandard magic number");
                return repaired;
            }
        }
    } else if (algorithm == CompressionAlgorithm::LZMA) {
        // Repair LZMA header
        if (data.size() >= 5) {
            // Check and fix properties byte
            uint8_t props = static_cast<uint8_t>(data[0]);
            if (props > 225) { // Invalid properties
                // Set to default properties
                repaired[0] = std::byte{93}; // Default LZMA properties

                spdlog::debug("Repaired LZMA properties byte");
                return repaired;
            }
        }
    }

    return std::nullopt;
}

} // namespace recovery_utils

} // namespace yams::compression
