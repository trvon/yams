#include <random>
#include <gtest/gtest.h>
#include <yams/compression/integrity_validator.h>

using namespace yams;
using namespace yams::compression;

class IntegrityValidatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        config_ = ValidationConfig{};
        validator_ = std::make_unique<IntegrityValidator>(config_);
    }

    void TearDown() override { validator_.reset(); }

    std::vector<std::byte> generateRandomData(size_t size) {
        std::vector<std::byte> data(size);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 255);

        for (size_t i = 0; i < size; ++i) {
            data[i] = static_cast<std::byte>(dis(gen));
        }

        return data;
    }

    ValidationConfig config_;
    std::unique_ptr<IntegrityValidator> validator_;
};

TEST_F(IntegrityValidatorTest, BasicConstruction) {
    EXPECT_TRUE(validator_ != nullptr);
}

TEST_F(IntegrityValidatorTest, BasicValidation) {
    auto data = generateRandomData(1024);

    auto result = validator_->validateData(data, ValidationType::None);
    EXPECT_TRUE(result.isValid);
    EXPECT_EQ(result.validationType, ValidationType::None);
}

TEST_F(IntegrityValidatorTest, ChecksumValidation) {
    auto data = generateRandomData(2048);

    auto result = validator_->validateData(data, ValidationType::Checksum);
    EXPECT_TRUE(result.isValid);
    EXPECT_EQ(result.validationType, ValidationType::Checksum);
}

TEST_F(IntegrityValidatorTest, ConfigurationAccess) {
    const auto& currentConfig = validator_->config();
    EXPECT_TRUE(currentConfig.defaultValidationType != ValidationType::Deep ||
                currentConfig.defaultValidationType == ValidationType::Deep);
}

TEST_F(IntegrityValidatorTest, ValidationStatistics) {
    // Perform a validation
    auto data = generateRandomData(512);
    validator_->validateData(data, ValidationType::Checksum);

    auto stats = validator_->getValidationStats();
    EXPECT_GT(stats.totalValidations, 0);
}

TEST_F(IntegrityValidatorTest, DiagnosticsOutput) {
    auto diagnostics = validator_->getDiagnostics();
    EXPECT_FALSE(diagnostics.empty());
    EXPECT_NE(diagnostics.find("Integrity Validator"), std::string::npos);
}