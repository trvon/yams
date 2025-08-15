#include <random>
#include <gtest/gtest.h>
#include <yams/compression/compression_header.h>
#include <yams/compression/compression_utils.h>
#include <yams/compression/compressor_interface.h>
#include <yams/compression/integrity_validator.h>

using namespace yams;
using namespace yams::compression;

class IntegrityValidatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        config_ = ValidationConfig{};
        config_.defaultValidationType = ValidationType::Checksum;
        config_.enableAsyncValidation = false;
        config_.deepAnalysisThreshold = 1024 * 1024; // 1MB

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

    std::vector<std::byte> generateCompressibleData(size_t size) {
        std::vector<std::byte> data(size);

        // Generate compressible pattern
        for (size_t i = 0; i < size; ++i) {
            data[i] = static_cast<std::byte>(i % 16);
        }

        return data;
    }

    ValidationConfig config_;
    std::unique_ptr<IntegrityValidator> validator_;
};

TEST_F(IntegrityValidatorTest, BasicValidation) {
    auto data = generateRandomData(1024);

    auto result = validator_->validateData(data, ValidationType::None);
    EXPECT_TRUE(result.isValid);
    EXPECT_EQ(result.validationType, ValidationType::None);
    EXPECT_FALSE(result.checksum.has_value());
}

TEST_F(IntegrityValidatorTest, ChecksumValidation) {
    auto data = generateRandomData(2048);

    auto result = validator_->validateData(data, ValidationType::Checksum);
    EXPECT_TRUE(result.isValid);
    EXPECT_EQ(result.validationType, ValidationType::Checksum);
    EXPECT_TRUE(result.checksum.has_value());

    // Verify checksum is correct
    auto expectedChecksum = calculateCRC32(data);
    EXPECT_EQ(result.checksum.value(), expectedChecksum);
}

TEST_F(IntegrityValidatorTest, CorruptedDataDetection) {
    auto originalData = generateCompressibleData(1024);

    // Simulate compression result
    CompressionResult compressionResult;
    compressionResult.data = originalData;
    compressionResult.algorithm = CompressionAlgorithm::Zstandard;
    compressionResult.level = 3;
    compressionResult.originalSize = originalData.size();
    compressionResult.compressedSize = originalData.size();

    // Validate original
    auto result1 = validator_->validateCompression(compressionResult, originalData);
    EXPECT_TRUE(result1.isValid);

    // Corrupt the data
    compressionResult.data[100] = std::byte{0xFF};
    compressionResult.data[101] = std::byte{0xFF};

    // Validate corrupted
    auto result2 = validator_->validateCompression(compressionResult, originalData);
    EXPECT_FALSE(result2.isValid);
    EXPECT_TRUE(result2.corruptionDetected);
}

TEST_F(IntegrityValidatorTest, RoundTripValidation) {
    auto originalData = generateCompressibleData(4096);

    // Create mock compressed data
    std::vector<std::byte> compressedData(originalData.size() / 2);
    std::copy(originalData.begin(), originalData.begin() + compressedData.size(),
              compressedData.begin());

    auto result = validator_->validateRoundTrip(originalData, compressedData,
                                                originalData, // Simulate perfect decompression
                                                CompressionAlgorithm::LZMA);

    EXPECT_TRUE(result.isValid);
    EXPECT_EQ(result.validationType, ValidationType::RoundTrip);
}

TEST_F(IntegrityValidatorTest, RoundTripValidationFailure) {
    auto originalData = generateCompressibleData(4096);
    auto compressedData = generateRandomData(2048);

    // Create different decompressed data
    auto decompressedData = originalData;
    decompressedData[100] = std::byte{0xFF};

    auto result = validator_->validateRoundTrip(originalData, compressedData, decompressedData,
                                                CompressionAlgorithm::Zstandard);

    EXPECT_FALSE(result.isValid);
    EXPECT_TRUE(result.corruptionDetected);
    EXPECT_NE(result.errorDetails.find("Round-trip validation failed"), std::string::npos);
}

TEST_F(IntegrityValidatorTest, DeepAnalysis) {
    auto data = generateCompressibleData(2 * 1024 * 1024); // 2MB

    auto result = validator_->performDeepAnalysis(data, CompressionAlgorithm::Zstandard);

    EXPECT_EQ(result.validationType, ValidationType::Deep);
    EXPECT_TRUE(result.checksum.has_value());
    EXPECT_GT(result.entropyScore, 0.0);
    EXPECT_LE(result.entropyScore, 8.0); // Max entropy is 8 bits per byte
}

TEST_F(IntegrityValidatorTest, ValidateHeader) {
    CompressionHeader header;
    header.magic = CompressionHeader::MAGIC;
    header.version = CompressionHeader::VERSION;
    header.algorithm = static_cast<uint8_t>(CompressionAlgorithm::Zstandard);
    header.compressionLevel = 5;
    header.originalSize = 1024;
    header.compressedSize = 512;
    header.checksum = 0x12345678;
    header.flags = 0;
    header.timestamp = std::chrono::duration_cast<std::chrono::seconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();

    auto result = validator_->validateHeader(header);
    EXPECT_TRUE(result.isValid);
}

TEST_F(IntegrityValidatorTest, ValidateInvalidHeader) {
    CompressionHeader header;
    header.magic = 0xDEADBEEF; // Wrong magic
    header.version = CompressionHeader::VERSION;
    header.algorithm = static_cast<uint8_t>(CompressionAlgorithm::LZMA);
    header.originalSize = 1024;
    header.compressedSize = 512;

    auto result = validator_->validateHeader(header);
    EXPECT_FALSE(result.isValid);
    EXPECT_TRUE(result.corruptionDetected);
}

TEST_F(IntegrityValidatorTest, RegisterValidationCallback) {
    bool callbackInvoked = false;
    bool validationPassed = false;

    validator_->registerValidationCallback([&](const ValidationResult& result) {
        callbackInvoked = true;
        validationPassed = result.isValid;
    });

    auto data = generateRandomData(1024);
    validator_->validateData(data, ValidationType::Checksum);

    EXPECT_TRUE(callbackInvoked);
    EXPECT_TRUE(validationPassed);
}

TEST_F(IntegrityValidatorTest, ValidationStatistics) {
    // Perform several validations
    for (int i = 0; i < 5; ++i) {
        auto data = generateRandomData(512);
        validator_->validateData(data, ValidationType::Checksum);
    }

    auto stats = validator_->getValidationStats();
    EXPECT_EQ(stats.totalValidations, 5);
    EXPECT_EQ(stats.successfulValidations, 5);
    EXPECT_EQ(stats.failedValidations, 0);
    EXPECT_EQ(stats.validationsByType[ValidationType::Checksum], 5);
}

TEST_F(IntegrityValidatorTest, IsLikelyCorrupted) {
    // Test with random data (high entropy, not corrupted)
    auto randomData = generateRandomData(1024);
    EXPECT_FALSE(validator_->isLikelyCorrupted(randomData, CompressionAlgorithm::None));

    // Test with pattern data (low entropy, not corrupted)
    auto patternData = std::vector<std::byte>(1024, std::byte{0xAA});
    EXPECT_FALSE(validator_->isLikelyCorrupted(patternData, CompressionAlgorithm::None));

    // Test with mostly zeros (might indicate corruption)
    auto zeroData = std::vector<std::byte>(1024, std::byte{0x00});
    auto result = validator_->isLikelyCorrupted(zeroData, CompressionAlgorithm::Zstandard);
    // This might be true for compressed data algorithms
}

TEST_F(IntegrityValidatorTest, ValidationScope) {
    bool validationPerformed = false;

    validator_->registerValidationCallback([&](const ValidationResult& result) {
        validationPerformed = true;
        EXPECT_TRUE(result.isValid);
    });

    {
        ValidationScope scope(*validator_, ValidationType::Checksum);
        auto data = generateRandomData(256);
        scope.setData(data);
        // Scope destructor should perform validation
    }

    EXPECT_TRUE(validationPerformed);
}

TEST_F(IntegrityValidatorTest, ConfigurationUpdate) {
    ValidationConfig newConfig;
    newConfig.defaultValidationType = ValidationType::RoundTrip;
    newConfig.checksumAlgorithm = ChecksumAlgorithm::SHA256;
    newConfig.enableAsyncValidation = true;

    validator_->updateConfig(newConfig);

    const auto& currentConfig = validator_->config();
    EXPECT_EQ(currentConfig.defaultValidationType, ValidationType::RoundTrip);
    EXPECT_EQ(currentConfig.checksumAlgorithm, ChecksumAlgorithm::SHA256);
    EXPECT_TRUE(currentConfig.enableAsyncValidation);
}

TEST_F(IntegrityValidatorTest, DiagnosticsOutput) {
    // Perform some validations
    auto data = generateRandomData(1024);
    validator_->validateData(data, ValidationType::Checksum);
    validator_->validateData(data, ValidationType::None);

    auto diagnostics = validator_->getDiagnostics();
    EXPECT_FALSE(diagnostics.empty());
    EXPECT_NE(diagnostics.find("Integrity Validator Diagnostics"), std::string::npos);
    EXPECT_NE(diagnostics.find("Total validations: 2"), std::string::npos);
}