#include <gtest/gtest.h>
#include <yams/compression/error_handler.h>

using namespace yams;
using namespace yams::compression;

class ErrorHandlerTest : public ::testing::Test {
protected:
    void SetUp() override {
        config_ = ErrorHandlingConfig{};
        handler_ = std::make_unique<CompressionErrorHandler>(config_);
    }

    void TearDown() override { handler_.reset(); }

    ErrorHandlingConfig config_;
    std::unique_ptr<CompressionErrorHandler> handler_;
};

TEST_F(ErrorHandlerTest, BasicConstruction) {
    EXPECT_TRUE(handler_ != nullptr);
    EXPECT_FALSE(handler_->isInDegradedMode());
}

TEST_F(ErrorHandlerTest, DegradedModeToggle) {
    EXPECT_FALSE(handler_->isInDegradedMode());

    handler_->setDegradedMode(true);
    EXPECT_TRUE(handler_->isInDegradedMode());

    handler_->setDegradedMode(false);
    EXPECT_FALSE(handler_->isInDegradedMode());
}

TEST_F(ErrorHandlerTest, ConfigurationAccess) {
    const auto& currentConfig = handler_->config();
    EXPECT_GT(currentConfig.maxRetryAttempts, 0);
    EXPECT_GT(currentConfig.retryDelay.count(), 0);
}

TEST_F(ErrorHandlerTest, ErrorStatistics) {
    auto stats = handler_->getErrorStats();
    EXPECT_EQ(stats.totalErrors, 0);

    handler_->resetStats();
    stats = handler_->getErrorStats();
    EXPECT_EQ(stats.totalErrors, 0);
}

TEST_F(ErrorHandlerTest, DiagnosticsOutput) {
    auto diagnostics = handler_->getDiagnostics();
    EXPECT_FALSE(diagnostics.empty());
    EXPECT_NE(diagnostics.find("Error Handler"), std::string::npos);
}