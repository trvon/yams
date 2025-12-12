// Catch2 tests for Integrity Validator
// Migrated from GTest: integrity_validator_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <random>
#include <yams/compression/compression_header.h>
#include <yams/compression/compression_utils.h>
#include <yams/compression/compressor_interface.h>
#include <yams/compression/integrity_validator.h>

using namespace yams;
using namespace yams::compression;

namespace {
struct IntegrityValidatorFixture {
    IntegrityValidatorFixture() {
        config_ = ValidationConfig{};
        config_.defaultValidationType = ValidationType::Checksum;
        config_.enableAsyncValidation = false;
        config_.deepAnalysisThreshold = 1024 * 1024; // 1MB

        validator_ = std::make_unique<IntegrityValidator>(config_);
    }

    ~IntegrityValidatorFixture() { validator_.reset(); }

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
} // namespace

TEST_CASE_METHOD(IntegrityValidatorFixture, "IntegrityValidator - BasicValidation",
                 "[compression][integrity][catch2]") {
    auto data = generateRandomData(1024);

    auto result = validator_->validateData(data, ValidationType::None);
    CHECK(result.isValid);
    CHECK(result.validationType == ValidationType::None);
    CHECK_FALSE(result.checksum.has_value());
}

TEST_CASE_METHOD(IntegrityValidatorFixture, "IntegrityValidator - ChecksumValidation",
                 "[compression][integrity][catch2]") {
    auto data = generateRandomData(2048);

    auto result = validator_->validateData(data, ValidationType::Checksum);
    CHECK(result.isValid);
    CHECK(result.validationType == ValidationType::Checksum);
    REQUIRE(result.checksum.has_value());

    // Verify checksum is correct
    auto expectedChecksum = calculateCRC32(data);
    CHECK(result.checksum.value() == expectedChecksum);
}

TEST_CASE_METHOD(IntegrityValidatorFixture, "IntegrityValidator - CorruptedDataDetection",
                 "[compression][integrity][catch2]") {
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
    CHECK(result1.isValid);

    // Corrupt the data
    compressionResult.data[100] = std::byte{0xFF};
    compressionResult.data[101] = std::byte{0xFF};

    // Validate corrupted
    auto result2 = validator_->validateCompression(compressionResult, originalData);
    CHECK_FALSE(result2.isValid);
    CHECK(result2.corruptionDetected);
}

TEST_CASE_METHOD(IntegrityValidatorFixture, "IntegrityValidator - RoundTripValidation",
                 "[compression][integrity][catch2]") {
    auto originalData = generateCompressibleData(4096);

    // Create mock compressed data
    std::vector<std::byte> compressedData(originalData.size() / 2);
    std::copy_n(originalData.begin(), compressedData.size(), compressedData.begin());

    auto result = validator_->validateRoundTrip(originalData, compressedData,
                                                originalData, // Simulate perfect decompression
                                                CompressionAlgorithm::LZMA);

    CHECK(result.isValid);
    CHECK(result.validationType == ValidationType::RoundTrip);
}

TEST_CASE_METHOD(IntegrityValidatorFixture, "IntegrityValidator - RoundTripValidationFailure",
                 "[compression][integrity][catch2]") {
    auto originalData = generateCompressibleData(4096);
    auto compressedData = generateRandomData(2048);

    // Create different decompressed data
    auto decompressedData = originalData;
    decompressedData[100] = std::byte{0xFF};

    auto result = validator_->validateRoundTrip(originalData, compressedData, decompressedData,
                                                CompressionAlgorithm::Zstandard);

    CHECK_FALSE(result.isValid);
    CHECK(result.corruptionDetected);
    CHECK(result.errorDetails.find("Round-trip validation failed") != std::string::npos);
}

TEST_CASE_METHOD(IntegrityValidatorFixture, "IntegrityValidator - DeepAnalysis",
                 "[compression][integrity][catch2]") {
    auto data = generateCompressibleData(2 * 1024 * 1024); // 2MB

    auto result = validator_->performDeepAnalysis(data, CompressionAlgorithm::Zstandard);

    CHECK(result.validationType == ValidationType::Deep);
    REQUIRE(result.checksum.has_value());
    CHECK(result.entropyScore > 0.0);
    CHECK(result.entropyScore <= 8.0); // Max entropy is 8 bits per byte
}

TEST_CASE_METHOD(IntegrityValidatorFixture, "IntegrityValidator - ValidateHeader",
                 "[compression][integrity][catch2]") {
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
    CHECK(result.isValid);
}

TEST_CASE_METHOD(IntegrityValidatorFixture, "IntegrityValidator - ValidateInvalidHeader",
                 "[compression][integrity][catch2]") {
    CompressionHeader header;
    header.magic = 0xDEADBEEF; // Wrong magic
    header.version = CompressionHeader::VERSION;
    header.algorithm = static_cast<uint8_t>(CompressionAlgorithm::LZMA);
    header.originalSize = 1024;
    header.compressedSize = 512;

    auto result = validator_->validateHeader(header);
    CHECK_FALSE(result.isValid);
    CHECK(result.corruptionDetected);
}

TEST_CASE_METHOD(IntegrityValidatorFixture, "IntegrityValidator - RegisterValidationCallback",
                 "[compression][integrity][catch2]") {
    bool callbackInvoked = false;
    bool validationPassed = false;

    validator_->registerValidationCallback([&](const ValidationResult& result) {
        callbackInvoked = true;
        validationPassed = result.isValid;
    });

    auto data = generateRandomData(1024);
    validator_->validateData(data, ValidationType::Checksum);

    CHECK(callbackInvoked);
    CHECK(validationPassed);
}

TEST_CASE_METHOD(IntegrityValidatorFixture, "IntegrityValidator - ValidationStatistics",
                 "[compression][integrity][catch2]") {
    // Perform several validations
    for (int i = 0; i < 5; ++i) {
        auto data = generateRandomData(512);
        validator_->validateData(data, ValidationType::Checksum);
    }

    auto stats = validator_->getValidationStats();
    CHECK(stats.totalValidations == 5);
    CHECK(stats.successfulValidations == 5);
    CHECK(stats.failedValidations == 0);
    CHECK(stats.validationsByType[ValidationType::Checksum] == 5);
}

TEST_CASE_METHOD(IntegrityValidatorFixture, "IntegrityValidator - IsLikelyCorrupted",
                 "[compression][integrity][catch2]") {
    // Test with random data (high entropy, not corrupted)
    auto randomData = generateRandomData(1024);
    CHECK_FALSE(validator_->isLikelyCorrupted(randomData, CompressionAlgorithm::None));

    // Test with pattern data (low entropy, not corrupted)
    auto patternData = std::vector<std::byte>(1024, std::byte{0xAA});
    CHECK_FALSE(validator_->isLikelyCorrupted(patternData, CompressionAlgorithm::None));
}

TEST_CASE_METHOD(IntegrityValidatorFixture, "IntegrityValidator - ValidationScope",
                 "[compression][integrity][catch2]") {
    bool validationPerformed = false;

    validator_->registerValidationCallback([&](const ValidationResult& result) {
        validationPerformed = true;
        CHECK(result.isValid);
    });

    {
        ValidationScope scope(*validator_, ValidationType::Checksum);
        auto data = generateRandomData(256);
        scope.setData(data);
        // Scope destructor should perform validation
    }

    CHECK(validationPerformed);
}

TEST_CASE_METHOD(IntegrityValidatorFixture, "IntegrityValidator - ConfigurationUpdate",
                 "[compression][integrity][catch2]") {
    ValidationConfig newConfig;
    newConfig.defaultValidationType = ValidationType::RoundTrip;
    newConfig.checksumAlgorithm = ChecksumAlgorithm::SHA256;
    newConfig.enableAsyncValidation = true;

    validator_->updateConfig(newConfig);

    const auto& currentConfig = validator_->config();
    CHECK(currentConfig.defaultValidationType == ValidationType::RoundTrip);
    CHECK(currentConfig.checksumAlgorithm == ChecksumAlgorithm::SHA256);
    CHECK(currentConfig.enableAsyncValidation);
}

TEST_CASE_METHOD(IntegrityValidatorFixture, "IntegrityValidator - DiagnosticsOutput",
                 "[compression][integrity][catch2]") {
    // Perform some validations
    auto data = generateRandomData(1024);
    validator_->validateData(data, ValidationType::Checksum);
    validator_->validateData(data, ValidationType::None);

    auto diagnostics = validator_->getDiagnostics();
    CHECK_FALSE(diagnostics.empty());
    CHECK(diagnostics.find("Integrity Validator Diagnostics") != std::string::npos);
    CHECK(diagnostics.find("Total validations: 2") != std::string::npos);
}
