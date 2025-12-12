// Catch2 tests for Compression Error Handler
// Migrated from GTest: error_handler_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <thread>
#include <yams/compression/compressor_interface.h>
#include <yams/compression/error_handler.h>

using namespace yams;
using namespace yams::compression;

namespace {
struct ErrorHandlerFixture {
    ErrorHandlerFixture() {
        config_ = ErrorHandlingConfig{};
        config_.maxRetryAttempts = 3;
        config_.retryDelay = std::chrono::milliseconds{100};
        config_.maxRetryDelay = std::chrono::milliseconds{5000};
        config_.enableIntegrityValidation = true;

        handler_ = std::make_unique<CompressionErrorHandler>(config_);
    }

    ~ErrorHandlerFixture() { handler_.reset(); }

    ErrorHandlingConfig config_;
    std::unique_ptr<CompressionErrorHandler> handler_;
};
} // namespace

TEST_CASE_METHOD(ErrorHandlerFixture, "CompressionErrorHandler - BasicErrorHandling",
                 "[compression][error_handler][catch2]") {
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
    CHECK(result.isSuccessful());
    CHECK(retryCount == 3);
}

TEST_CASE_METHOD(ErrorHandlerFixture, "CompressionErrorHandler - MaxRetriesExhausted",
                 "[compression][error_handler][catch2]") {
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
    CHECK_FALSE(result.isSuccessful());
    CHECK(result.strategy == RecoveryStrategy::Fallback);
}

TEST_CASE_METHOD(ErrorHandlerFixture, "CompressionErrorHandler - CriticalErrorHandling",
                 "[compression][error_handler][catch2]") {
    CompressionError error;
    error.code = ErrorCode::InternalError;
    error.severity = ErrorSeverity::Critical;
    error.recommendedStrategy = RecoveryStrategy::None;
    error.algorithm = CompressionAlgorithm::Zstandard;

    auto retryFunc = []() -> Result<CompressionResult> {
        return Error{ErrorCode::InternalError, "Critical error"};
    };

    auto result = handler_->handleError(error, retryFunc);
    CHECK_FALSE(result.isSuccessful());
    CHECK(result.strategy == RecoveryStrategy::None);
}

TEST_CASE_METHOD(ErrorHandlerFixture, "CompressionErrorHandler - DegradedModeActivation",
                 "[compression][error_handler][catch2]") {
    CHECK_FALSE(handler_->isInDegradedMode());

    handler_->setDegradedMode(true);
    CHECK(handler_->isInDegradedMode());

    handler_->setDegradedMode(false);
    CHECK_FALSE(handler_->isInDegradedMode());
}

TEST_CASE_METHOD(ErrorHandlerFixture, "CompressionErrorHandler - ErrorStatistics",
                 "[compression][error_handler][catch2]") {
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
    CHECK(stats.totalErrors == 5);
    CHECK(stats.errorsByCode[ErrorCode::CompressionError] == 5);
    CHECK(stats.errorsBySeverity[ErrorSeverity::Error] == 5);
}

TEST_CASE_METHOD(ErrorHandlerFixture, "CompressionErrorHandler - ErrorCallbacks",
                 "[compression][error_handler][catch2]") {
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

    CHECK(callbackInvoked);
    CHECK(capturedCode == ErrorCode::CompressionError);
}

TEST_CASE_METHOD(ErrorHandlerFixture, "CompressionErrorHandler - RecoveryStrategySelection",
                 "[compression][error_handler][catch2]") {
    CompressionError error;
    error.code = ErrorCode::CompressionError;
    error.severity = ErrorSeverity::Error;
    error.algorithm = CompressionAlgorithm::Zstandard;

    SECTION("Early attempts should retry") {
        error.attemptNumber = 1;
        error.recommendedStrategy = RecoveryStrategy::Retry;
        auto strategy = handler_->selectRecoveryStrategy(error);
        CHECK(strategy == RecoveryStrategy::Retry);
    }

    SECTION("Middle attempts should fallback") {
        error.attemptNumber = 4;
        auto strategy = handler_->selectRecoveryStrategy(error);
        CHECK(strategy == RecoveryStrategy::Fallback);
    }

    SECTION("Late attempts should use uncompressed") {
        error.attemptNumber = 6;
        auto strategy = handler_->selectRecoveryStrategy(error);
        CHECK(strategy == RecoveryStrategy::Uncompressed);
    }
}

TEST_CASE_METHOD(ErrorHandlerFixture, "CompressionErrorHandler - ResetStatistics",
                 "[compression][error_handler][catch2]") {
    // Record an error
    CompressionError error;
    error.code = ErrorCode::CompressionError;
    error.severity = ErrorSeverity::Error;

    auto retryFunc = []() -> Result<CompressionResult> {
        return Error{ErrorCode::CompressionError, "Test"};
    };

    handler_->handleError(error, retryFunc);

    auto stats = handler_->getErrorStats();
    CHECK(stats.totalErrors > 0);

    handler_->resetStats();
    stats = handler_->getErrorStats();
    CHECK(stats.totalErrors == 0);
}

TEST_CASE_METHOD(ErrorHandlerFixture, "CompressionErrorHandler - ConfigurationUpdate",
                 "[compression][error_handler][catch2]") {
    ErrorHandlingConfig newConfig;
    newConfig.maxRetryAttempts = 5;
    newConfig.retryDelay = std::chrono::milliseconds{200};
    newConfig.maxRetryDelay = std::chrono::milliseconds{10000};
    newConfig.enableIntegrityValidation = false;

    handler_->updateConfig(newConfig);

    const auto& currentConfig = handler_->config();
    CHECK(currentConfig.maxRetryAttempts == 5);
    CHECK(currentConfig.retryDelay.count() == 200);
    CHECK_FALSE(currentConfig.enableIntegrityValidation);
}

TEST_CASE_METHOD(ErrorHandlerFixture, "CompressionErrorHandler - DiagnosticsOutput",
                 "[compression][error_handler][catch2]") {
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
    CHECK_FALSE(diagnostics.empty());
    CHECK(diagnostics.find("Error Handler Diagnostics") != std::string::npos);
    CHECK(diagnostics.find("Total errors: 1") != std::string::npos);
}

TEST_CASE_METHOD(ErrorHandlerFixture, "CompressionErrorHandler - ErrorScopeRAII",
                 "[compression][error_handler][catch2]") {
    bool errorHandled = false;

    handler_->registerErrorCallback([&](const CompressionError& error) {
        errorHandled = true;
        CHECK(error.code == ErrorCode::CompressionError);
    });

    {
        ErrorScope scope(*handler_, CompressionAlgorithm::Zstandard);
        scope.setError(ErrorCode::CompressionError, "Test error in scope");
        // ErrorScope destructor should handle the error
    }

    CHECK(errorHandled);
}
