#include <gtest/gtest.h>
#include <yams/compression/error_handler.h>

using namespace yams;
using namespace yams::compression;

TEST(ErrorHandlerBasicTest, Construction) {
    ErrorHandlingConfig config{};
    CompressionErrorHandler handler(config);
    
    EXPECT_FALSE(handler.isInDegradedMode());
}

TEST(ErrorHandlerBasicTest, DegradedMode) {
    ErrorHandlingConfig config{};
    CompressionErrorHandler handler(config);
    
    handler.setDegradedMode(true);
    EXPECT_TRUE(handler.isInDegradedMode());
    
    handler.setDegradedMode(false);
    EXPECT_FALSE(handler.isInDegradedMode());
}

TEST(ErrorHandlerBasicTest, Configuration) {
    ErrorHandlingConfig config{};
    config.maxRetryAttempts = 5;
    
    CompressionErrorHandler handler(config);
    const auto& currentConfig = handler.config();
    
    EXPECT_EQ(currentConfig.maxRetryAttempts, 5);
}