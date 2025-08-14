#pragma once

#include <yams/core/types.h>
#include <yams/compression/compressor_interface.h>
#include <yams/core/span.h>
#include <vector>
#include <string>
#include <functional>
#include <memory>
#include <chrono>

namespace yams::compression {

/**
 * @brief Types of integrity validation
 */
enum class ValidationType : uint8_t {
    None = 0,           ///< No validation
    Checksum = 1,       ///< Basic checksum validation
    RoundTrip = 2,      ///< Compress->decompress->compare validation
    Deep = 3            ///< Comprehensive validation with multiple checks
};

/**
 * @brief Validation result information
 */
struct ValidationResult {
    bool isValid;                                     ///< Whether data is valid
    ValidationType type;                              ///< Type of validation performed
    std::string errorMessage;                         ///< Error message if invalid
    std::chrono::microseconds validationTime;        ///< Time taken for validation
    size_t originalSize;                              ///< Size of original data
    size_t compressedSize;                           ///< Size of compressed data
    uint32_t originalChecksum;                        ///< Checksum of original data
    uint32_t validationChecksum;                      ///< Checksum after validation
    double compressionRatio;                          ///< Achieved compression ratio
    
    /**
     * @brief Create successful validation result
     */
    static ValidationResult success(ValidationType type, std::chrono::microseconds time);
    
    /**
     * @brief Create failed validation result
     */
    static ValidationResult failure(ValidationType type, const std::string& error);
    
    /**
     * @brief Format result as string
     */
    [[nodiscard]] std::string format() const;
};

/**
 * @brief Configuration for integrity validation
 */
struct ValidationConfig {
    ValidationType defaultType = ValidationType::Checksum;  ///< Default validation type
    bool enableContinuousValidation = true;                 ///< Enable ongoing validation
    double samplingRate = 0.1;                             ///< Fraction of operations to validate
    size_t maxValidationSize = 10 * 1024 * 1024;          ///< Max size for full validation
    std::chrono::milliseconds validationTimeout{5000};     ///< Timeout for validation
    bool enableChecksumCache = true;                        ///< Cache checksums for performance
    size_t checksumCacheSize = 1000;                       ///< Maximum cached checksums
    uint32_t polynomialSeed = 0xEDB88320;                  ///< CRC32 polynomial
};

/**
 * @brief Callback for validation events
 */
using ValidationCallback = std::function<void(const ValidationResult&)>;

/**
 * @brief Comprehensive integrity validator for compression operations
 * 
 * Provides various levels of validation to ensure data integrity
 * during compression and decompression operations.
 */
class IntegrityValidator {
public:
    /**
     * @brief Constructor with configuration
     * @param config Validation configuration
     */
    explicit IntegrityValidator(ValidationConfig config = {});
    
    /**
     * @brief Destructor
     */
    ~IntegrityValidator();
    
    // Non-copyable, movable
    IntegrityValidator(const IntegrityValidator&) = delete;
    IntegrityValidator& operator=(const IntegrityValidator&) = delete;
    IntegrityValidator(IntegrityValidator&&) noexcept;
    IntegrityValidator& operator=(IntegrityValidator&&) noexcept;
    
    /**
     * @brief Validate compression result
     * @param original Original data
     * @param result Compression result
     * @param type Type of validation to perform
     * @return Validation result
     */
    [[nodiscard]] ValidationResult validateCompression(
        yams::span<const std::byte> original,
        const CompressionResult& result,
        ValidationType type = ValidationType::Checksum);
    
    /**
     * @brief Validate decompression result
     * @param compressed Compressed data
     * @param decompressed Decompressed data
     * @param originalChecksum Expected checksum of original data
     * @param type Type of validation to perform
     * @return Validation result
     */
    [[nodiscard]] ValidationResult validateDecompression(
        yams::span<const std::byte> compressed,
        yams::span<const std::byte> decompressed,
        uint32_t originalChecksum = 0,
        ValidationType type = ValidationType::Checksum);
    
    /**
     * @brief Perform round-trip validation
     * @param original Original data
     * @param compressor Compressor to use
     * @param level Compression level
     * @return Validation result
     */
    [[nodiscard]] ValidationResult validateRoundTrip(
        yams::span<const std::byte> original,
        ICompressor& compressor,
        uint8_t level = 0);
    
    /**
     * @brief Calculate checksum of data
     * @param data Data to checksum
     * @return CRC32 checksum
     */
    [[nodiscard]] uint32_t calculateChecksum(yams::span<const std::byte> data) const;
    
    /**
     * @brief Verify data integrity using checksum
     * @param data Data to verify
     * @param expectedChecksum Expected checksum
     * @return True if checksums match
     */
    [[nodiscard]] bool verifyChecksum(
        yams::span<const std::byte> data,
        uint32_t expectedChecksum) const;
    
    /**
     * @brief Detect potential data corruption
     * @param data Data to analyze
     * @return Corruption probability (0.0 = no corruption, 1.0 = certain corruption)
     */
    [[nodiscard]] double detectCorruption(yams::span<const std::byte> data) const;
    
    /**
     * @brief Register validation callback
     * @param callback Function to call for validation events
     */
    void registerValidationCallback(ValidationCallback callback);
    
    /**
     * @brief Update configuration
     * @param config New configuration
     */
    void updateConfig(const ValidationConfig& config);
    
    /**
     * @brief Get current configuration
     * @return Current configuration
     */
    [[nodiscard]] const ValidationConfig& config() const noexcept;
    
    /**
     * @brief Get validation statistics
     * @return Statistics map
     */
    [[nodiscard]] std::unordered_map<ValidationType, size_t> getValidationStats() const;
    
    /**
     * @brief Get validation failure rate
     * @return Failure rate as percentage
     */
    [[nodiscard]] double getFailureRate() const;
    
    /**
     * @brief Reset validation statistics
     */
    void resetStats();
    
    /**
     * @brief Check if validation should be performed based on sampling rate
     * @return True if validation should be performed
     */
    [[nodiscard]] bool shouldValidate() const;
    
    /**
     * @brief Enable or disable validation
     * @param enabled Whether to enable validation
     */
    void setValidationEnabled(bool enabled) noexcept;
    
    /**
     * @brief Check if validation is enabled
     * @return True if validation is enabled
     */
    [[nodiscard]] bool isValidationEnabled() const noexcept;
    
    /**
     * @brief Perform deep analysis of compressed data
     * @param compressed Compressed data
     * @param algorithm Algorithm used for compression
     * @return Analysis result with detailed information
     */
    [[nodiscard]] ValidationResult performDeepAnalysis(
        yams::span<const std::byte> compressed,
        CompressionAlgorithm algorithm) const;

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * @brief RAII helper for automatic validation
 */
class ValidationScope {
public:
    /**
     * @brief Constructor
     * @param validator Validator to use
     * @param type Type of validation to perform
     */
    ValidationScope(IntegrityValidator& validator, ValidationType type);
    
    /**
     * @brief Destructor - performs validation if enabled
     */
    ~ValidationScope() noexcept;
    
    /**
     * @brief Set original data for validation
     * @param data Original data
     */
    void setOriginalData(yams::span<const std::byte> data);
    
    /**
     * @brief Set compression result for validation
     * @param result Compression result
     */
    void setCompressionResult(const CompressionResult& result);
    
    /**
     * @brief Set decompressed data for validation
     * @param data Decompressed data
     */
    void setDecompressedData(yams::span<const std::byte> data);
    
    /**
     * @brief Get validation result
     * @return Validation result (only valid after operation completes)
     */
    [[nodiscard]] const ValidationResult& result() const noexcept;
    
    /**
     * @brief Check if validation passed
     * @return True if validation was successful
     */
    [[nodiscard]] bool isValid() const noexcept;

private:
    IntegrityValidator& validator_;
    ValidationType type_;
    std::vector<std::byte> originalData_;
    std::unique_ptr<CompressionResult> compressionResult_;
    std::vector<std::byte> decompressedData_;
    ValidationResult result_;
    bool hasOriginal_ = false;
    bool hasCompressed_ = false;
    bool hasDecompressed_ = false;
};

/**
 * @brief Utility functions for data integrity
 */
namespace integrity_utils {
    /**
     * @brief Calculate CRC32 checksum
     * @param data Data to checksum
     * @param polynomial CRC32 polynomial (default: IEEE 802.3)
     * @return CRC32 checksum
     */
    [[nodiscard]] uint32_t crc32(
        yams::span<const std::byte> data,
        uint32_t polynomial = 0xEDB88320);
    
    /**
     * @brief Calculate entropy of data (measure of randomness)
     * @param data Data to analyze
     * @return Entropy value (0.0 to 8.0 bits per byte)
     */
    [[nodiscard]] double calculateEntropy(yams::span<const std::byte> data);
    
    /**
     * @brief Detect patterns that might indicate corruption
     * @param data Data to analyze
     * @return Pattern anomaly score (0.0 = normal, 1.0 = highly anomalous)
     */
    [[nodiscard]] double detectPatternAnomalies(yams::span<const std::byte> data);
    
    /**
     * @brief Check for common compression artifacts
     * @param compressed Compressed data
     * @param algorithm Algorithm used
     * @return True if artifacts detected
     */
    [[nodiscard]] bool hasCompressionArtifacts(
        yams::span<const std::byte> compressed,
        CompressionAlgorithm algorithm);
    
    /**
     * @brief Validate compressed data format
     * @param compressed Compressed data
     * @param algorithm Expected algorithm
     * @return True if format is valid
     */
    [[nodiscard]] bool validateCompressionFormat(
        yams::span<const std::byte> compressed,
        CompressionAlgorithm algorithm);
}

} // namespace yams::compression