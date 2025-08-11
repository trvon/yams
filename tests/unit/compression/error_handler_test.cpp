#include <gtest/gtest.h>
#include <yams/compression/error_handler.h>
#include <yams/compression/compressor_interface.h>
#include <thread>
#include <chrono>

using namespace yams;
using namespace yams::compression;

class ErrorHandlerTest : public ::testing::Test {
protected:
    void SetUp() override {
        config_ = ErrorHandlingConfig{};
        config_.maxRetryAttempts = 3;
        config_.retryDelay = std::chrono::milliseconds{100};
        config_.maxRetryDelay = std::chrono::milliseconds{5000};
        config_.enableIntegrityValidation = true;
        
        handler_ = std::make_unique<CompressionErrorHandler>(config_);
    }
    
    void TearDown() override {
        handler_.reset();
    }
    
    ErrorHandlingConfig config_;
    std::unique_ptr<CompressionErrorHandler> handler_;
};

TEST_F(ErrorHandlerTest, BasicErrorHandling) {
    CompressionError error;
    error.code = ErrorCode::CompressionError;
    error.severity = ErrorSeverity::Error;
    error.recommendedStrategy = RecoveryStrategy::Retry;
    error.algorithm = CompressionAlgorithm::Zstandard;
    error.operation = "compress";
    error.details = "Test compression error";
    error.dataSize = 1024;
    error.compressionLevel = 5;
    error.timestamp = std::chrono::system_clock::now();
    error.attemptNumber = 1;
    
    int retryCount = 0;
    auto retryFunc = [&retryCount]() -> Result<CompressionResult> {
        retryCount++;
        if (retryCount < 3) {
            return Error{ErrorCode::CompressionError, "Still failing"};
        }
        
        CompressionResult result;
        result.algorithm = CompressionAlgorithm::Zstandard;
        result.level = 5;
        result.originalSize = 1024;
        result.compressedSize = 512;
        result.duration = std::chrono::microseconds{1000};
        return result;
    };
    
    auto result = handler_->handleError(error, retryFunc);
    ASSERT_TRUE(result.isSuccessful());
    EXPECT_EQ(retryCount, 3);
}

TEST_F(ErrorHandlerTest, MaxRetriesExhausted) {
    CompressionError error;
    error.code = ErrorCode::CompressionError;
    error.severity = ErrorSeverity::Error;
    error.recommendedStrategy = RecoveryStrategy::Retry;
    error.algorithm = CompressionAlgorithm::LZMA;
    error.attemptNumber = 1;
    
    auto retryFunc = []() -> Result<CompressionResult> {
        return Error{ErrorCode::CompressionError, "Always fails"};
    };
    
    auto result = handler_->handleError(error, retryFunc);
    ASSERT_FALSE(result.isSuccessful());
    EXPECT_EQ(result.strategy, RecoveryStrategy::Fallback);
}

TEST_F(ErrorHandlerTest, CriticalErrorHandling) {
    CompressionError error;
    error.code = ErrorCode::InternalError;
    error.severity = ErrorSeverity::Critical;
    error.recommendedStrategy = RecoveryStrategy::None;
    error.algorithm = CompressionAlgorithm::Zstandard;
    
    auto retryFunc = []() -> Result<CompressionResult> {
        return Error{ErrorCode::InternalError, "Critical error"};
    };
    
    auto result = handler_->handleError(error, retryFunc);
    ASSERT_FALSE(result.isSuccessful());
    EXPECT_EQ(result.strategy, RecoveryStrategy::None);
}

TEST_F(ErrorHandlerTest, DegradedModeActivation) {
    EXPECT_FALSE(handler_->isInDegradedMode());
    
    handler_->setDegradedMode(true);
    EXPECT_TRUE(handler_->isInDegradedMode());
    
    handler_->setDegradedMode(false);
    EXPECT_FALSE(handler_->isInDegradedMode());
}

TEST_F(ErrorHandlerTest, ErrorStatistics) {
    // Record some errors
    for (int i = 0; i < 5; i++) {
        CompressionError error;
        error.code = ErrorCode::CompressionError;
        error.severity = ErrorSeverity::Error;
        error.recommendedStrategy = RecoveryStrategy::Retry;
        error.algorithm = CompressionAlgorithm::Zstandard;
        error.attemptNumber = 1;
        
        auto retryFunc = []() -> Result<CompressionResult> {
            CompressionResult result;
            result.algorithm = CompressionAlgorithm::Zstandard;
            return result;
        };
        
        handler_->handleError(error, retryFunc);
    }
    
    auto stats = handler_->getErrorStats();
    EXPECT_EQ(stats.totalErrors, 5);
    EXPECT_EQ(stats.errorsByCode[ErrorCode::CompressionError], 5);
    EXPECT_EQ(stats.errorsBySeverity[ErrorSeverity::Error], 5);
}

TEST_F(ErrorHandlerTest, ErrorCallbacks) {
    bool callbackInvoked = false;
    ErrorCode capturedCode = ErrorCode::Success;
    
    handler_->registerErrorCallback([&](const CompressionError& error) {
        callbackInvoked = true;
        capturedCode = error.code;
    });
    
    CompressionError error;
    error.code = ErrorCode::CompressionError;
    error.severity = ErrorSeverity::Error;
    error.recommendedStrategy = RecoveryStrategy::None;
    
    auto retryFunc = []() -> Result<CompressionResult> {
        return Error{ErrorCode::CompressionError, "Test error"};
    };
    
    handler_->handleError(error, retryFunc);
    
    EXPECT_TRUE(callbackInvoked);
    EXPECT_EQ(capturedCode, ErrorCode::CompressionError);
}

TEST_F(ErrorHandlerTest, RecoveryStrategySelection) {
    CompressionError error;
    error.code = ErrorCode::CompressionError;
    error.severity = ErrorSeverity::Error;
    error.algorithm = CompressionAlgorithm::Zstandard;
    
    // Test different attempt numbers
    error.attemptNumber = 1;
    error.recommendedStrategy = RecoveryStrategy::Retry;
    auto strategy = handler_->selectRecoveryStrategy(error);
    EXPECT_EQ(strategy, RecoveryStrategy::Retry);
    
    error.attemptNumber = 4;
    strategy = handler_->selectRecoveryStrategy(error);
    EXPECT_EQ(strategy, RecoveryStrategy::Fallback);
    
    error.attemptNumber = 6;
    strategy = handler_->selectRecoveryStrategy(error);
    EXPECT_EQ(strategy, RecoveryStrategy::Uncompressed);
}

TEST_F(ErrorHandlerTest, ResetStatistics) {
    // Record an error
    CompressionError error;
    error.code = ErrorCode::CompressionError;
    error.severity = ErrorSeverity::Error;
    
    auto retryFunc = []() -> Result<CompressionResult> {
        return Error{ErrorCode::CompressionError, "Test"};
    };
    
    handler_->handleError(error, retryFunc);
    
    auto stats = handler_->getErrorStats();
    EXPECT_GT(stats.totalErrors, 0);
    
    handler_->resetStats();
    stats = handler_->getErrorStats();
    EXPECT_EQ(stats.totalErrors, 0);
}

TEST_F(ErrorHandlerTest, ConfigurationUpdate) {
    ErrorHandlingConfig newConfig;
    newConfig.maxRetryAttempts = 5;
    newConfig.retryDelay = std::chrono::milliseconds{200};
    newConfig.maxRetryDelay = std::chrono::milliseconds{10000};
    newConfig.enableIntegrityValidation = false;
    
    handler_->updateConfig(newConfig);
    
    const auto& currentConfig = handler_->config();
    EXPECT_EQ(currentConfig.maxRetryAttempts, 5);
    EXPECT_EQ(currentConfig.retryDelay.count(), 200);
    EXPECT_FALSE(currentConfig.enableIntegrityValidation);
}

TEST_F(ErrorHandlerTest, DiagnosticsOutput) {
    // Record some errors
    CompressionError error;
    error.code = ErrorCode::CompressionError;
    error.severity = ErrorSeverity::Error;
    error.algorithm = CompressionAlgorithm::Zstandard;
    
    auto retryFunc = []() -> Result<CompressionResult> {
        return Error{ErrorCode::CompressionError, "Test"};
    };
    
    handler_->handleError(error, retryFunc);
    
    auto diagnostics = handler_->getDiagnostics();
    EXPECT_FALSE(diagnostics.empty());
    EXPECT_NE(diagnostics.find("Error Handler Diagnostics"), std::string::npos);
    EXPECT_NE(diagnostics.find("Total errors: 1"), std::string::npos);
}

// Test for ErrorScope RAII helper
TEST_F(ErrorHandlerTest, ErrorScopeRAII) {
    bool errorHandled = false;
    
    handler_->registerErrorCallback([&](const CompressionError& error) {
        errorHandled = true;
        EXPECT_EQ(error.code, ErrorCode::CompressionError);
    });
    
    {
        ErrorScope scope(*handler_, CompressionAlgorithm::Zstandard);
        scope.setError(ErrorCode::CompressionError, "Test error in scope");
        // ErrorScope destructor should handle the error
    }
    
    EXPECT_TRUE(errorHandled);
}