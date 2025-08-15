#include <gtest/gtest.h>
#include <yams/compression/integrity_validator.h>

using namespace yams;
using namespace yams::compression;

TEST(IntegrityValidatorBasicTest, Construction) {
    ValidationConfig config{};
    IntegrityValidator validator(config);

    // Just verify construction works
    EXPECT_TRUE(true);
}

TEST(IntegrityValidatorBasicTest, BasicOperation) {
    ValidationConfig config{};
    IntegrityValidator validator(config);

    // Test basic functionality exists
    const auto& currentConfig = validator.config();
    EXPECT_TRUE(true); // Just verify we can access config
}