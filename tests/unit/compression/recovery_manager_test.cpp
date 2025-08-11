#include <gtest/gtest.h>
#include <yams/compression/recovery_manager.h>
#include <yams/compression/compressor_interface.h>
#include <yams/compression/error_handler.h>
#include <yams/compression/integrity_validator.h>
#include <future>
#include <random>

using namespace yams;
using namespace yams::compression;

class RecoveryManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        config_ = RecoveryConfig{};
        config_.maxConcurrentOperations = 4;
        config_.maxQueueSize = 100;
        config_.operationTimeout = std::chrono::seconds{30};
        config_.enablePartialRecovery = true;
        config_.threadPoolSize = 2;
        
        errorHandler_ = std::make_shared<CompressionErrorHandler>(ErrorHandlingConfig{});
        validator_ = std::make_shared<IntegrityValidator>(ValidationConfig{});
        
        manager_ = std::make_unique<RecoveryManager>(config_, errorHandler_, validator_);
        ASSERT_TRUE(manager_->start().has_value());
    }
    
    void TearDown() override {
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

TEST_F(RecoveryManagerTest, BasicRecoverySubmission) {
    auto data = generateTestData(1024);
    
    auto request = RecoveryRequest::createCompressionRecovery(
        data, CompressionAlgorithm::Zstandard, 5);
    
    auto future = manager_->submitRecovery(request);
    auto result = future.get();
    
    EXPECT_NE(result.status, RecoveryStatus::Pending);
    EXPECT_GT(result.attemptsUsed, 0);
    EXPECT_FALSE(result.message.empty());
}

TEST_F(RecoveryManagerTest, CompressionFallbackRecovery) {
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
    EXPECT_TRUE(result.status == RecoveryStatus::Success || 
                result.status == RecoveryStatus::Failed);
    EXPECT_EQ(result.operationPerformed, RecoveryOperation::CompressFallback);
}

TEST_F(RecoveryManagerTest, DecompressionRecovery) {
    auto data = generateTestData(1024);
    
    auto request = RecoveryRequest::createDecompressionRecovery(
        data, CompressionAlgorithm::Zstandard, 2048);
    
    auto result = manager_->performRecovery(request);
    
    EXPECT_EQ(result.operationPerformed, RecoveryOperation::DecompressRecover);
    // Recovery of random data should fail
    EXPECT_EQ(result.status, RecoveryStatus::Failed);
}

TEST_F(RecoveryManagerTest, TryAlternativeCompression) {
    auto data = generateTestData(4096);
    
    auto result = manager_->tryAlternativeCompression(
        data, CompressionAlgorithm::LZMA, 5);
    
    // Should return either compressed data or uncompressed as fallback
    EXPECT_TRUE(result.has_value());
    if (result.has_value()) {
        EXPECT_EQ(result.value().originalSize, data.size());
    }
}

TEST_F(RecoveryManagerTest, RecoveryCallback) {
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
    
    manager_->performRecovery(request);
    
    EXPECT_TRUE(callbackInvoked);
    EXPECT_EQ(capturedStatus, RecoveryStatus::Success);
}

TEST_F(RecoveryManagerTest, ConcurrentRecoveryOperations) {
    const int numOperations = 10;
    std::vector<std::future<RecoveryOperationResult>> futures;
    
    for (int i = 0; i < numOperations; ++i) {
        auto data = generateTestData(512 + i * 100);
        auto request = RecoveryRequest::createCompressionRecovery(
            data, CompressionAlgorithm::Zstandard, 3);
        
        futures.push_back(manager_->submitRecovery(request));
    }
    
    // Wait for all operations
    for (auto& future : futures) {
        auto result = future.get();
        EXPECT_NE(result.status, RecoveryStatus::Pending);
    }
}

TEST_F(RecoveryManagerTest, RecoveryStatistics) {
    // Perform several recovery operations
    for (int i = 0; i < 5; ++i) {
        auto data = generateTestData(1024);
        RecoveryRequest request;
        request.operation = RecoveryOperation::CompressUncompressed;
        request.originalAlgorithm = CompressionAlgorithm::None;
        request.data = data;
        
        manager_->performRecovery(request);
    }
    
    auto stats = manager_->getRecoveryStats();
    EXPECT_EQ(stats[RecoveryOperation::CompressUncompressed], 5);
    
    auto successRate = manager_->getSuccessRate();
    EXPECT_GT(successRate, 0.0);
    EXPECT_LE(successRate, 1.0);
}

TEST_F(RecoveryManagerTest, QueueSizeLimit) {
    // Stop manager to prevent processing
    manager_->stop();
    
    // Create new manager with small queue
    RecoveryConfig smallQueueConfig = config_;
    smallQueueConfig.maxQueueSize = 5;
    
    auto smallManager = std::make_unique<RecoveryManager>(
        smallQueueConfig, errorHandler_, validator_);
    
    // Don't start it to keep queue from being processed
    
    // Try to submit more than queue size
    std::vector<std::future<RecoveryOperationResult>> futures;
    for (int i = 0; i < 10; ++i) {
        auto data = generateTestData(100);
        auto request = RecoveryRequest::createCompressionRecovery(
            data, CompressionAlgorithm::Zstandard);
        
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
    
    EXPECT_GT(failedCount, 0);
}

TEST_F(RecoveryManagerTest, RecoveryScope) {
    auto data = generateTestData(1024);
    RecoveryOperationResult capturedResult;
    
    {
        RecoveryScope scope(*manager_, RecoveryOperation::CompressUncompressed);
        scope.setData(data);
        scope.setAlgorithms(CompressionAlgorithm::Zstandard, CompressionAlgorithm::None);
        
        // Execute explicitly
        capturedResult = scope.execute();
    }
    
    EXPECT_EQ(capturedResult.status, RecoveryStatus::Success);
    EXPECT_EQ(capturedResult.operationPerformed, RecoveryOperation::CompressUncompressed);
}

TEST_F(RecoveryManagerTest, AlgorithmPreference) {
    std::vector<CompressionAlgorithm> preference = {
        CompressionAlgorithm::Zstandard,
        CompressionAlgorithm::LZMA,
        CompressionAlgorithm::None
    };
    
    manager_->setAlgorithmPreference(preference);
    
    // This should influence fallback selection
    auto data = generateTestData(2048);
    auto result = manager_->tryAlternativeCompression(
        data, CompressionAlgorithm::LZMA, 5);
    
    EXPECT_TRUE(result.has_value());
}

TEST_F(RecoveryManagerTest, DiagnosticsOutput) {
    // Perform some operations
    auto data = generateTestData(512);
    RecoveryRequest request;
    request.operation = RecoveryOperation::CompressUncompressed;
    request.originalAlgorithm = CompressionAlgorithm::None;
    request.data = data;
    
    manager_->performRecovery(request);
    
    auto diagnostics = manager_->getDiagnostics();
    EXPECT_FALSE(diagnostics.empty());
    EXPECT_NE(diagnostics.find("Recovery Manager Diagnostics"), std::string::npos);
    EXPECT_NE(diagnostics.find("Status: Running"), std::string::npos);
}

TEST_F(RecoveryManagerTest, RecoveryUtilities) {
    // Test fallback algorithm selection
    auto fallback = recovery_utils::selectFallbackAlgorithm(
        CompressionAlgorithm::LZMA, 1024 * 1024);
    EXPECT_EQ(fallback, CompressionAlgorithm::Zstandard);
    
    fallback = recovery_utils::selectFallbackAlgorithm(
        CompressionAlgorithm::Zstandard, 512);
    EXPECT_EQ(fallback, CompressionAlgorithm::None);
    
    // Test recovery probability estimation
    auto probability = recovery_utils::estimateRecoveryProbability(
        RecoveryOperation::CompressRetry, 0);
    EXPECT_GT(probability, 0.5);
    
    probability = recovery_utils::estimateRecoveryProbability(
        RecoveryOperation::CompressUncompressed, 0);
    EXPECT_EQ(probability, 1.0);
    
    // Test failure pattern analysis
    CompressionError error;
    error.code = ErrorCode::CompressionError;
    error.attemptNumber = 1;
    
    auto operation = recovery_utils::analyzeFailurePattern(error);
    EXPECT_EQ(operation, RecoveryOperation::CompressRetry);
    
    error.attemptNumber = 5;
    operation = recovery_utils::analyzeFailurePattern(error);
    EXPECT_EQ(operation, RecoveryOperation::CompressFallback);
}