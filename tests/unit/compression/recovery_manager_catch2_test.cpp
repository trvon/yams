// Catch2 tests for Recovery Manager
// Migrated from GTest: recovery_manager_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <future>
#include <random>
#include <yams/compression/compressor_interface.h>
#include <yams/compression/error_handler.h>
#include <yams/compression/integrity_validator.h>
#include <yams/compression/recovery_manager.h>

using namespace yams;
using namespace yams::compression;

namespace {
struct RecoveryManagerFixture {
    RecoveryManagerFixture() {
        config_ = RecoveryConfig{};
        config_.maxConcurrentOperations = 4;
        config_.maxQueueSize = 100;
        config_.operationTimeout = std::chrono::seconds{30};
        config_.enablePartialRecovery = true;
        config_.threadPoolSize = 2;

        errorHandler_ = std::make_shared<CompressionErrorHandler>(ErrorHandlingConfig{});
        validator_ = std::make_shared<IntegrityValidator>(ValidationConfig{});

        manager_ = std::make_unique<RecoveryManager>(config_, errorHandler_, validator_);
        auto result = manager_->start();
        if (!result.has_value()) {
            manager_.reset();
        }
    }

    ~RecoveryManagerFixture() {
        if (manager_) {
            manager_->stop();
        }
    }

    std::vector<std::byte> generateTestData(size_t size) {
        std::vector<std::byte> data(size);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 255);

        for (size_t i = 0; i < size; ++i) {
            data[i] = static_cast<std::byte>(dis(gen));
        }

        return data;
    }

    RecoveryConfig config_;
    std::shared_ptr<CompressionErrorHandler> errorHandler_;
    std::shared_ptr<IntegrityValidator> validator_;
    std::unique_ptr<RecoveryManager> manager_;
};
} // namespace

TEST_CASE_METHOD(RecoveryManagerFixture, "RecoveryManager - BasicRecoverySubmission",
                 "[compression][recovery][catch2]") {
    REQUIRE(manager_ != nullptr);

    auto data = generateTestData(1024);

    auto request =
        RecoveryRequest::createCompressionRecovery(data, CompressionAlgorithm::Zstandard, 5);

    auto future = manager_->submitRecovery(request);
    auto result = future.get();

    CHECK(result.status != RecoveryStatus::Pending);
    CHECK(result.attemptsUsed > 0);
    CHECK_FALSE(result.message.empty());
}

TEST_CASE_METHOD(RecoveryManagerFixture, "RecoveryManager - CompressionFallbackRecovery",
                 "[compression][recovery][catch2]") {
    REQUIRE(manager_ != nullptr);

    auto data = generateTestData(2048);

    RecoveryRequest request;
    request.operation = RecoveryOperation::CompressFallback;
    request.originalAlgorithm = CompressionAlgorithm::LZMA;
    request.fallbackAlgorithm = CompressionAlgorithm::Zstandard;
    request.data = data;
    request.compressionLevel = 3;
    request.maxRetries = 1;
    request.timeout = std::chrono::milliseconds{5000};

    auto result = manager_->performRecovery(request);

    // Should either succeed with fallback or fail gracefully
    CHECK((result.status == RecoveryStatus::Success || result.status == RecoveryStatus::Failed));
    CHECK(result.operationPerformed == RecoveryOperation::CompressFallback);
}

TEST_CASE_METHOD(RecoveryManagerFixture, "RecoveryManager - DecompressionRecovery",
                 "[compression][recovery][catch2]") {
    REQUIRE(manager_ != nullptr);

    auto data = generateTestData(1024);

    auto request =
        RecoveryRequest::createDecompressionRecovery(data, CompressionAlgorithm::Zstandard, 2048);

    auto result = manager_->performRecovery(request);

    CHECK(result.operationPerformed == RecoveryOperation::DecompressRecover);
    // Recovery of random data should fail
    CHECK(result.status == RecoveryStatus::Failed);
}

TEST_CASE_METHOD(RecoveryManagerFixture, "RecoveryManager - TryAlternativeCompression",
                 "[compression][recovery][catch2]") {
    REQUIRE(manager_ != nullptr);

    auto data = generateTestData(4096);

    auto result = manager_->tryAlternativeCompression(data, CompressionAlgorithm::LZMA, 5);

    // Should return either compressed data or uncompressed as fallback
    REQUIRE(result.has_value());
    CHECK(result.value().originalSize == data.size());
}

TEST_CASE_METHOD(RecoveryManagerFixture, "RecoveryManager - RecoveryCallback",
                 "[compression][recovery][catch2]") {
    REQUIRE(manager_ != nullptr);

    bool callbackInvoked = false;
    RecoveryStatus capturedStatus = RecoveryStatus::Pending;

    manager_->registerRecoveryCallback([&](const RecoveryOperationResult& result) {
        callbackInvoked = true;
        capturedStatus = result.status;
    });

    auto data = generateTestData(512);
    RecoveryRequest request;
    request.operation = RecoveryOperation::CompressUncompressed;
    request.originalAlgorithm = CompressionAlgorithm::None;
    request.data = data;

    (void)manager_->performRecovery(request);

    CHECK(callbackInvoked);
    CHECK(capturedStatus == RecoveryStatus::Success);
}

TEST_CASE_METHOD(RecoveryManagerFixture, "RecoveryManager - ConcurrentRecoveryOperations",
                 "[compression][recovery][concurrent][catch2][.]") {
    REQUIRE(manager_ != nullptr);

    const int numOperations = 10;
    std::vector<std::future<RecoveryOperationResult>> futures;

    for (int i = 0; i < numOperations; ++i) {
        auto data = generateTestData(512 + i * 100);
        auto request =
            RecoveryRequest::createCompressionRecovery(data, CompressionAlgorithm::Zstandard, 3);

        futures.push_back(manager_->submitRecovery(request));
    }

    // Wait for all operations
    for (auto& future : futures) {
        auto result = future.get();
        CHECK(result.status != RecoveryStatus::Pending);
    }
}

TEST_CASE_METHOD(RecoveryManagerFixture, "RecoveryManager - RecoveryStatistics",
                 "[compression][recovery][catch2]") {
    REQUIRE(manager_ != nullptr);

    // Perform several recovery operations
    for (int i = 0; i < 5; ++i) {
        auto data = generateTestData(1024);
        RecoveryRequest request;
        request.operation = RecoveryOperation::CompressUncompressed;
        request.originalAlgorithm = CompressionAlgorithm::None;
        request.data = data;

        (void)manager_->performRecovery(request);
    }

    auto stats = manager_->getRecoveryStats();
    CHECK(stats[RecoveryOperation::CompressUncompressed] == 5);

    auto successRate = manager_->getSuccessRate();
    CHECK(successRate > 0.0);
    CHECK(successRate <= 1.0);
}

TEST_CASE_METHOD(RecoveryManagerFixture, "RecoveryManager - QueueSizeLimit",
                 "[compression][recovery][catch2]") {
    REQUIRE(manager_ != nullptr);

    // Stop manager to prevent processing
    manager_->stop();

    // Create new manager with small queue
    RecoveryConfig smallQueueConfig = config_;
    smallQueueConfig.maxQueueSize = 5;

    auto smallManager =
        std::make_unique<RecoveryManager>(smallQueueConfig, errorHandler_, validator_);

    // Don't start it to keep queue from being processed

    // Try to submit more than queue size
    std::vector<std::future<RecoveryOperationResult>> futures;
    for (int i = 0; i < 10; ++i) {
        auto data = generateTestData(100);
        auto request =
            RecoveryRequest::createCompressionRecovery(data, CompressionAlgorithm::Zstandard);

        auto future = smallManager->submitRecovery(request);
        futures.push_back(std::move(future));
    }

    // Some should fail due to queue full
    int failedCount = 0;
    for (auto& future : futures) {
        auto result = future.get();
        if (result.status == RecoveryStatus::Failed &&
            result.message.find("queue is full") != std::string::npos) {
            failedCount++;
        }
    }

    CHECK(failedCount > 0);
}

TEST_CASE_METHOD(RecoveryManagerFixture, "RecoveryManager - RecoveryScope",
                 "[compression][recovery][catch2]") {
    REQUIRE(manager_ != nullptr);

    auto data = generateTestData(1024);
    RecoveryOperationResult capturedResult;

    {
        RecoveryScope scope(*manager_, RecoveryOperation::CompressUncompressed);
        scope.setData(data);
        scope.setAlgorithms(CompressionAlgorithm::Zstandard, CompressionAlgorithm::None);

        // Execute explicitly
        capturedResult = scope.execute();
    }

    CHECK(capturedResult.status == RecoveryStatus::Success);
    CHECK(capturedResult.operationPerformed == RecoveryOperation::CompressUncompressed);
}

TEST_CASE_METHOD(RecoveryManagerFixture, "RecoveryManager - AlgorithmPreference",
                 "[compression][recovery][catch2]") {
    REQUIRE(manager_ != nullptr);

    std::vector<CompressionAlgorithm> preference = {
        CompressionAlgorithm::Zstandard, CompressionAlgorithm::LZMA, CompressionAlgorithm::None};

    manager_->setAlgorithmPreference(preference);

    // This should influence fallback selection
    auto data = generateTestData(2048);
    auto result = manager_->tryAlternativeCompression(data, CompressionAlgorithm::LZMA, 5);

    CHECK(result.has_value());
}

TEST_CASE_METHOD(RecoveryManagerFixture, "RecoveryManager - DiagnosticsOutput",
                 "[compression][recovery][catch2]") {
    REQUIRE(manager_ != nullptr);

    // Perform some operations
    auto data = generateTestData(512);
    RecoveryRequest request;
    request.operation = RecoveryOperation::CompressUncompressed;
    request.originalAlgorithm = CompressionAlgorithm::None;
    request.data = data;

    (void)manager_->performRecovery(request);

    auto diagnostics = manager_->getDiagnostics();
    CHECK_FALSE(diagnostics.empty());
    CHECK(diagnostics.find("Recovery Manager Diagnostics") != std::string::npos);
    CHECK(diagnostics.find("Status: Running") != std::string::npos);
}

TEST_CASE_METHOD(RecoveryManagerFixture, "RecoveryManager - RecoveryUtilities",
                 "[compression][recovery][catch2]") {
    // Test fallback algorithm selection
    auto fallback =
        recovery_utils::selectFallbackAlgorithm(CompressionAlgorithm::LZMA, 1024 * 1024);
    CHECK(fallback == CompressionAlgorithm::Zstandard);

    fallback = recovery_utils::selectFallbackAlgorithm(CompressionAlgorithm::Zstandard, 512);
    CHECK(fallback == CompressionAlgorithm::None);

    // Test recovery probability estimation
    auto probability =
        recovery_utils::estimateRecoveryProbability(RecoveryOperation::CompressRetry, 0);
    CHECK(probability > 0.5);

    probability =
        recovery_utils::estimateRecoveryProbability(RecoveryOperation::CompressUncompressed, 0);
    CHECK(probability == 1.0);

    // Test failure pattern analysis
    CompressionError error;
    error.code = ErrorCode::CompressionError;
    error.attemptNumber = 1;

    auto operation = recovery_utils::analyzeFailurePattern(error);
    CHECK(operation == RecoveryOperation::CompressRetry);

    error.attemptNumber = 5;
    operation = recovery_utils::analyzeFailurePattern(error);
    CHECK(operation == RecoveryOperation::CompressFallback);
}
