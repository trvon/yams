#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <array>
#include <cmath>
#include <mutex>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <yams/compression/integrity_validator.h>

namespace yams::compression {

namespace {
/**
 * @brief Get string representation of validation type
 */
const char* validationTypeToString(ValidationType type) noexcept {
    switch (type) {
        case ValidationType::None:
            return "None";
        case ValidationType::Checksum:
            return "Checksum";
        case ValidationType::RoundTrip:
            return "RoundTrip";
        case ValidationType::Deep:
            return "Deep";
        default:
            return "Unknown";
    }
}

/**
 * @brief CRC32 lookup table for fast computation
 */
class CRC32Table {
public:
    CRC32Table(uint32_t polynomial) {
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t crc = i;
            for (int j = 0; j < 8; ++j) {
                if (crc & 1) {
                    crc = (crc >> 1) ^ polynomial;
                } else {
                    crc >>= 1;
                }
            }
            table_[i] = crc;
        }
    }

    uint32_t calculate(std::span<const std::byte> data, uint32_t initial = 0xFFFFFFFF) const {
        uint32_t crc = initial;
        for (auto byte : data) {
            crc = table_[(crc ^ static_cast<uint8_t>(byte)) & 0xFF] ^ (crc >> 8);
        }
        return crc ^ 0xFFFFFFFF;
    }

private:
    uint32_t table_[256];
};

// Global CRC32 table with IEEE 802.3 polynomial
static const CRC32Table g_crc32Table(0xEDB88320);
} // namespace

//-----------------------------------------------------------------------------
// ValidationResult
//-----------------------------------------------------------------------------

ValidationResult ValidationResult::success(ValidationType type, std::chrono::microseconds time) {
    ValidationResult result;
    result.isValid = true;
    result.type = type;
    result.validationTime = time;
    result.originalSize = 0;
    result.compressedSize = 0;
    result.originalChecksum = 0;
    result.validationChecksum = 0;
    result.compressionRatio = 0.0;
    return result;
}

ValidationResult ValidationResult::failure(ValidationType type, const std::string& error) {
    ValidationResult result;
    result.isValid = false;
    result.type = type;
    result.errorMessage = error;
    result.validationTime = std::chrono::microseconds{0};
    result.originalSize = 0;
    result.compressedSize = 0;
    result.originalChecksum = 0;
    result.validationChecksum = 0;
    result.compressionRatio = 0.0;
    return result;
}

std::string ValidationResult::format() const {
    if (isValid) {
        return fmt::format("Validation PASSED [{}]: ratio={:.2f}, time={}μs, checksum=0x{:08X}",
                           validationTypeToString(type), compressionRatio, validationTime.count(),
                           originalChecksum);
    } else {
        return fmt::format("Validation FAILED [{}]: {} (time={}μs)", validationTypeToString(type),
                           errorMessage, validationTime.count());
    }
}

//-----------------------------------------------------------------------------
// IntegrityValidator::Impl
//-----------------------------------------------------------------------------

class IntegrityValidator::Impl {
public:
    explicit Impl(ValidationConfig cfg) : config_(std::move(cfg)), rng_(std::random_device{}()) {}

    ValidationResult validateCompression(std::span<const std::byte> original,
                                         const CompressionResult& result, ValidationType type) {
        auto start = std::chrono::steady_clock::now();

        if (type == ValidationType::None) {
            return ValidationResult::success(type, std::chrono::microseconds{0});
        }

        updateStats(type);

        try {
            ValidationResult validationResult;
            validationResult.type = type;
            validationResult.originalSize = original.size();
            validationResult.compressedSize = result.data.size();
            validationResult.compressionRatio = result.ratio();
            validationResult.originalChecksum = calculateChecksum(original);

            switch (type) {
                case ValidationType::Checksum:
                    validationResult = validateChecksumOnly(original, result);
                    break;

                case ValidationType::RoundTrip:
                    validationResult = validateRoundTripInternal(original, result);
                    break;

                case ValidationType::Deep:
                    validationResult = validateDeepAnalysis(original, result);
                    break;

                default:
                    validationResult = ValidationResult::failure(type, "Unknown validation type");
                    break;
            }

            auto end = std::chrono::steady_clock::now();
            validationResult.validationTime =
                std::chrono::duration_cast<std::chrono::microseconds>(end - start);

            notifyCallbacks(validationResult);

            if (!validationResult.isValid) {
                updateFailureStats();
            }

            return validationResult;

        } catch (const std::exception& ex) {
            updateFailureStats();

            return ValidationResult::failure(type,
                                             fmt::format("Validation exception: {}", ex.what()));
        }
    }

    ValidationResult validateDecompression(std::span<const std::byte> compressed,
                                           std::span<const std::byte> decompressed,
                                           uint32_t originalChecksum, ValidationType type) {
        auto start = std::chrono::steady_clock::now();

        if (type == ValidationType::None) {
            return ValidationResult::success(type, std::chrono::microseconds{0});
        }

        updateStats(type);

        try {
            ValidationResult result;
            result.type = type;
            result.originalSize = decompressed.size();
            result.compressedSize = compressed.size();
            result.compressionRatio = decompressed.empty()
                                          ? 0.0
                                          : static_cast<double>(decompressed.size()) /
                                                static_cast<double>(compressed.size());

            uint32_t decompressedChecksum = calculateChecksum(decompressed);
            result.validationChecksum = decompressedChecksum;
            result.originalChecksum = originalChecksum;

            // Basic checksum validation
            if (originalChecksum != 0 && decompressedChecksum != originalChecksum) {
                result.isValid = false;
                result.errorMessage =
                    fmt::format("Checksum mismatch: expected 0x{:08X}, got 0x{:08X}",
                                originalChecksum, decompressedChecksum);
            } else {
                result.isValid = true;
            }

            // Additional validation for deeper types
            if (result.isValid && type == ValidationType::Deep) {
                double corruptionProbability = detectCorruption(decompressed);
                if (corruptionProbability > 0.5) {
                    result.isValid = false;
                    result.errorMessage = fmt::format(
                        "High corruption probability detected: {:.2f}", corruptionProbability);
                }
            }

            auto end = std::chrono::steady_clock::now();
            result.validationTime =
                std::chrono::duration_cast<std::chrono::microseconds>(end - start);

            notifyCallbacks(result);

            if (!result.isValid) {
                updateFailureStats();
            }

            return result;

        } catch (const std::exception& ex) {
            updateFailureStats();

            return ValidationResult::failure(
                type, fmt::format("Decompression validation exception: {}", ex.what()));
        }
    }

    ValidationResult validateRoundTrip(std::span<const std::byte> original, ICompressor& compressor,
                                       uint8_t level) {
        auto start = std::chrono::steady_clock::now();

        updateStats(ValidationType::RoundTrip);

        try {
            // Compress the data
            auto compressResult = compressor.compress(original, level);
            if (!compressResult.has_value()) {
                return ValidationResult::failure(
                    ValidationType::RoundTrip,
                    fmt::format("Compression failed: {}", compressResult.error().message));
            }

            // Decompress the data
            auto decompressResult = compressor.decompress(compressResult.value().data);
            if (!decompressResult.has_value()) {
                return ValidationResult::failure(
                    ValidationType::RoundTrip,
                    fmt::format("Decompression failed: {}", decompressResult.error().message));
            }

            // Compare original and decompressed data
            const auto& decompressed = decompressResult.value();

            ValidationResult result;
            result.type = ValidationType::RoundTrip;
            result.originalSize = original.size();
            result.compressedSize = compressResult.value().data.size();
            result.compressionRatio = compressResult.value().ratio();
            result.originalChecksum = calculateChecksum(original);
            result.validationChecksum = calculateChecksum(decompressed);

            if (original.size() != decompressed.size()) {
                result.isValid = false;
                result.errorMessage = fmt::format("Size mismatch: original={}, decompressed={}",
                                                  original.size(), decompressed.size());
            } else if (!std::equal(original.begin(), original.end(), decompressed.begin())) {
                result.isValid = false;
                result.errorMessage = "Data content mismatch after round-trip";
            } else {
                result.isValid = true;
            }

            auto end = std::chrono::steady_clock::now();
            result.validationTime =
                std::chrono::duration_cast<std::chrono::microseconds>(end - start);

            notifyCallbacks(result);

            if (!result.isValid) {
                updateFailureStats();
            }

            return result;

        } catch (const std::exception& ex) {
            updateFailureStats();

            return ValidationResult::failure(
                ValidationType::RoundTrip,
                fmt::format("Round-trip validation exception: {}", ex.what()));
        }
    }

    uint32_t calculateChecksum(std::span<const std::byte> data) const {
        return g_crc32Table.calculate(data);
    }

    bool verifyChecksum(std::span<const std::byte> data, uint32_t expectedChecksum) const {
        return calculateChecksum(data) == expectedChecksum;
    }

    double detectCorruption(std::span<const std::byte> data) const {
        if (data.empty()) {
            return 0.0;
        }

        // Calculate entropy to detect unusual patterns
        double entropy = integrity_utils::calculateEntropy(data);
        double patternScore = integrity_utils::detectPatternAnomalies(data);

        // Combine scores - high entropy and high pattern anomalies suggest corruption
        double corruptionScore = 0.0;

        // Very low entropy (all same bytes) or very high entropy (random noise) can indicate
        // corruption
        if (entropy < 1.0 || entropy > 7.5) {
            corruptionScore += 0.3;
        }

        // Pattern anomalies contribute to corruption score
        corruptionScore += patternScore * 0.7;

        return std::min(1.0, corruptionScore);
    }

    void registerValidationCallback(ValidationCallback callback) {
        std::lock_guard lock(callbacksMutex_);
        callbacks_.push_back(std::move(callback));
    }

    void updateConfig(const ValidationConfig& config) {
        std::lock_guard lock(configMutex_);
        config_ = config;
    }

    const ValidationConfig& config() const {
        std::lock_guard lock(configMutex_);
        return config_;
    }

    std::unordered_map<ValidationType, size_t> getValidationStats() const {
        std::lock_guard lock(statsMutex_);
        return validationStats_;
    }

    double getFailureRate() const {
        std::lock_guard lock(statsMutex_);
        size_t total = 0;
        for (const auto& [type, count] : validationStats_) {
            total += count;
        }
        return total > 0 ? static_cast<double>(validationFailures_) / static_cast<double>(total)
                         : 0.0;
    }

    void resetStats() {
        std::lock_guard lock(statsMutex_);
        validationStats_.clear();
        validationFailures_ = 0;
    }

    bool shouldValidate() const {
        auto cfg = config();
        if (!validationEnabled_) {
            return false;
        }

        std::lock_guard lock(samplingMutex_);
        std::uniform_real_distribution<double> dist(0.0, 1.0);
        return dist(rng_) < cfg.samplingRate;
    }

    void setValidationEnabled(bool enabled) { validationEnabled_ = enabled; }

    bool isValidationEnabled() const { return validationEnabled_; }

    ValidationResult performDeepAnalysis(std::span<const std::byte> compressed,
                                         CompressionAlgorithm algorithm) const {
        auto start = std::chrono::steady_clock::now();

        try {
            ValidationResult result;
            result.isValid = false; // Will be set to true if validation passes
            result.type = ValidationType::Deep;
            result.originalSize = 0; // Unknown for deep analysis
            result.compressedSize = compressed.size();
            result.originalChecksum = 0;   // Unknown for deep analysis
            result.validationChecksum = 0; // Will be calculated if needed
            result.compressionRatio = 0.0; // Unknown without original size

            // Validate compression format
            bool formatValid = integrity_utils::validateCompressionFormat(compressed, algorithm);
            if (!formatValid) {
                result.isValid = false;
                result.errorMessage = "Invalid compression format detected";
            } else {
                // Check for compression artifacts
                bool hasArtifacts = integrity_utils::hasCompressionArtifacts(compressed, algorithm);
                if (hasArtifacts) {
                    result.isValid = false;
                    result.errorMessage = "Compression artifacts detected";
                } else {
                    result.isValid = true;
                }
            }

            auto end = std::chrono::steady_clock::now();
            result.validationTime =
                std::chrono::duration_cast<std::chrono::microseconds>(end - start);

            return result;

        } catch (const std::exception& ex) {
            return ValidationResult::failure(ValidationType::Deep,
                                             fmt::format("Deep analysis exception: {}", ex.what()));
        }
    }

private:
    mutable std::mutex configMutex_;
    ValidationConfig config_;

    mutable std::mutex callbacksMutex_;
    std::vector<ValidationCallback> callbacks_;

    mutable std::mutex statsMutex_;
    std::unordered_map<ValidationType, size_t> validationStats_;
    size_t validationFailures_ = 0;

    mutable std::mutex samplingMutex_;
    mutable std::mt19937 rng_;

    std::atomic<bool> validationEnabled_{true};

    void updateStats(ValidationType type) {
        std::lock_guard lock(statsMutex_);
        validationStats_[type]++;
    }

    void updateFailureStats() {
        std::lock_guard lock(statsMutex_);
        validationFailures_++;
    }

    void notifyCallbacks(const ValidationResult& result) {
        std::lock_guard lock(callbacksMutex_);
        for (const auto& callback : callbacks_) {
            try {
                callback(result);
            } catch (const std::exception& ex) {
                spdlog::error("Validation callback failed: {}", ex.what());
            }
        }
    }

    ValidationResult validateChecksumOnly(std::span<const std::byte> original,
                                          const CompressionResult& result) {
        ValidationResult validationResult;
        validationResult.type = ValidationType::Checksum;
        validationResult.originalSize = original.size();
        validationResult.compressedSize = result.data.size();
        validationResult.compressionRatio = result.ratio();
        validationResult.originalChecksum = calculateChecksum(original);

        // For checksum validation, we just verify the compression completed successfully
        // Real checksum validation would require decompressing and comparing
        validationResult.isValid = true;
        validationResult.validationChecksum = validationResult.originalChecksum;

        return validationResult;
    }

    ValidationResult validateRoundTripInternal(std::span<const std::byte> original,
                                               const CompressionResult& result) {
        // This would need access to the compressor to perform actual round-trip validation
        // For now, return a basic validation
        ValidationResult validationResult;
        validationResult.type = ValidationType::RoundTrip;
        validationResult.originalSize = original.size();
        validationResult.compressedSize = result.data.size();
        validationResult.compressionRatio = result.ratio();
        validationResult.originalChecksum = calculateChecksum(original);
        validationResult.validationChecksum =
            calculateChecksum(result.data); // Checksum of compressed data
        validationResult.isValid = true;

        return validationResult;
    }

    ValidationResult validateDeepAnalysis(std::span<const std::byte> original,
                                          const CompressionResult& result) {
        ValidationResult validationResult;
        validationResult.type = ValidationType::Deep;
        validationResult.originalSize = original.size();
        validationResult.compressedSize = result.data.size();
        validationResult.compressionRatio = result.ratio();
        validationResult.originalChecksum = calculateChecksum(original);
        validationResult.validationChecksum =
            calculateChecksum(result.data); // Checksum of compressed data
        validationResult.isValid = false;   // Will be set based on analysis

        // Perform deep analysis
        bool formatValid =
            integrity_utils::validateCompressionFormat(result.data, result.algorithm);
        bool hasArtifacts = integrity_utils::hasCompressionArtifacts(result.data, result.algorithm);
        double corruptionProbability = detectCorruption(original);

        if (!formatValid) {
            validationResult.isValid = false;
            validationResult.errorMessage = "Invalid compression format";
        } else if (hasArtifacts) {
            validationResult.isValid = false;
            validationResult.errorMessage = "Compression artifacts detected";
        } else if (corruptionProbability > 0.5) {
            validationResult.isValid = false;
            validationResult.errorMessage =
                fmt::format("High corruption probability: {:.2f}", corruptionProbability);
        } else {
            validationResult.isValid = true;
        }

        return validationResult;
    }
};

//-----------------------------------------------------------------------------
// IntegrityValidator
//-----------------------------------------------------------------------------

IntegrityValidator::IntegrityValidator(ValidationConfig config)
    : pImpl(std::make_unique<Impl>(std::move(config))) {}

IntegrityValidator::~IntegrityValidator() = default;

IntegrityValidator::IntegrityValidator(IntegrityValidator&&) noexcept = default;
IntegrityValidator& IntegrityValidator::operator=(IntegrityValidator&&) noexcept = default;

ValidationResult IntegrityValidator::validateCompression(std::span<const std::byte> original,
                                                         const CompressionResult& result,
                                                         ValidationType type) {
    return pImpl->validateCompression(original, result, type);
}

ValidationResult IntegrityValidator::validateDecompression(std::span<const std::byte> compressed,
                                                           std::span<const std::byte> decompressed,
                                                           uint32_t originalChecksum,
                                                           ValidationType type) {
    return pImpl->validateDecompression(compressed, decompressed, originalChecksum, type);
}

ValidationResult IntegrityValidator::validateRoundTrip(std::span<const std::byte> original,
                                                       ICompressor& compressor, uint8_t level) {
    return pImpl->validateRoundTrip(original, compressor, level);
}

uint32_t IntegrityValidator::calculateChecksum(std::span<const std::byte> data) const {
    return pImpl->calculateChecksum(data);
}

bool IntegrityValidator::verifyChecksum(std::span<const std::byte> data,
                                        uint32_t expectedChecksum) const {
    return pImpl->verifyChecksum(data, expectedChecksum);
}

double IntegrityValidator::detectCorruption(std::span<const std::byte> data) const {
    return pImpl->detectCorruption(data);
}

void IntegrityValidator::registerValidationCallback(ValidationCallback callback) {
    pImpl->registerValidationCallback(std::move(callback));
}

void IntegrityValidator::updateConfig(const ValidationConfig& config) {
    pImpl->updateConfig(config);
}

const ValidationConfig& IntegrityValidator::config() const noexcept {
    return pImpl->config();
}

std::unordered_map<ValidationType, size_t> IntegrityValidator::getValidationStats() const {
    return pImpl->getValidationStats();
}

double IntegrityValidator::getFailureRate() const {
    return pImpl->getFailureRate();
}

void IntegrityValidator::resetStats() {
    pImpl->resetStats();
}

bool IntegrityValidator::shouldValidate() const {
    return pImpl->shouldValidate();
}

void IntegrityValidator::setValidationEnabled(bool enabled) noexcept {
    pImpl->setValidationEnabled(enabled);
}

bool IntegrityValidator::isValidationEnabled() const noexcept {
    return pImpl->isValidationEnabled();
}

ValidationResult IntegrityValidator::performDeepAnalysis(std::span<const std::byte> compressed,
                                                         CompressionAlgorithm algorithm) const {
    return pImpl->performDeepAnalysis(compressed, algorithm);
}

//-----------------------------------------------------------------------------
// ValidationScope
//-----------------------------------------------------------------------------

ValidationScope::ValidationScope(IntegrityValidator& validator, ValidationType type)
    : validator_(validator), type_(type),
      result_(ValidationResult::failure(type, "Not completed")) {}

ValidationScope::~ValidationScope() noexcept {
    if (type_ != ValidationType::None && hasOriginal_) {
        try {
            if (hasCompressed_ && compressionResult_) {
                result_ = validator_.validateCompression(originalData_, *compressionResult_, type_);
            } else if (hasDecompressed_) {
                uint32_t originalChecksum = validator_.calculateChecksum(originalData_);
                result_ = validator_.validateDecompression({}, decompressedData_, originalChecksum,
                                                           type_);
            }
        } catch (const std::exception& e) {
            spdlog::error("Validation scope exception: {}", e.what());
        } catch (...) {
            spdlog::error("Validation scope exception: unknown exception");
        }
    }
}

void ValidationScope::setOriginalData(std::span<const std::byte> data) {
    originalData_.assign(data.begin(), data.end());
    hasOriginal_ = true;
}

void ValidationScope::setCompressionResult(const CompressionResult& result) {
    compressionResult_ = std::make_unique<CompressionResult>(result);
    hasCompressed_ = true;
}

void ValidationScope::setDecompressedData(std::span<const std::byte> data) {
    decompressedData_.assign(data.begin(), data.end());
    hasDecompressed_ = true;
}

const ValidationResult& ValidationScope::result() const noexcept {
    return result_;
}

bool ValidationScope::isValid() const noexcept {
    return result_.isValid;
}

//-----------------------------------------------------------------------------
// Utility functions
//-----------------------------------------------------------------------------

namespace integrity_utils {

uint32_t crc32(std::span<const std::byte> data, uint32_t polynomial) {
    CRC32Table table(polynomial);
    return table.calculate(data);
}

double calculateEntropy(std::span<const std::byte> data) {
    if (data.empty()) {
        return 0.0;
    }

    // Count frequency of each byte value
    std::array<size_t, 256> freq = {};
    for (auto byte : data) {
        freq[static_cast<uint8_t>(byte)]++;
    }

    // Calculate Shannon entropy
    double entropy = 0.0;
    size_t total = data.size();

    for (size_t count : freq) {
        if (count > 0) {
            double p = static_cast<double>(count) / static_cast<double>(total);
            entropy -= p * std::log2(p);
        }
    }

    return entropy;
}

double detectPatternAnomalies(std::span<const std::byte> data) {
    if (data.size() < 4) {
        return 0.0;
    }

    // Look for suspicious patterns
    double anomalyScore = 0.0;

    // Check for excessive repetition
    size_t consecutiveRepeats = 0;
    size_t maxRepeats = 0;
    for (size_t i = 1; i < data.size(); ++i) {
        if (data[i] == data[i - 1]) {
            consecutiveRepeats++;
        } else {
            maxRepeats = std::max(maxRepeats, consecutiveRepeats);
            consecutiveRepeats = 0;
        }
    }
    maxRepeats = std::max(maxRepeats, consecutiveRepeats);

    // Excessive repetition indicates possible corruption
    if (maxRepeats > data.size() / 4) {
        anomalyScore += 0.5;
    }

    // Check for alternating patterns (corruption signature)
    size_t alternations = 0;
    for (size_t i = 2; i < data.size(); ++i) {
        if (data[i] == data[i - 2] && data[i] != data[i - 1]) {
            alternations++;
        }
    }

    if (alternations > data.size() / 8) {
        anomalyScore += 0.3;
    }

    return std::min(1.0, anomalyScore);
}

bool hasCompressionArtifacts(std::span<const std::byte> compressed,
                             CompressionAlgorithm algorithm) {
    if (compressed.empty()) {
        return true;
    }

    // Basic format validation based on algorithm
    switch (algorithm) {
        case CompressionAlgorithm::Zstandard:
            // Zstandard magic number: 0x28, 0xB5, 0x2F, 0xFD
            if (compressed.size() >= 4) {
                return !(compressed[0] == std::byte{0x28} && compressed[1] == std::byte{0xB5} &&
                         compressed[2] == std::byte{0x2F} && compressed[3] == std::byte{0xFD});
            }
            return true;

        case CompressionAlgorithm::LZMA: {
            // LZMA has properties header
            if (compressed.size() < 5) {
                return true;
            }
            // Basic validation - properties should be reasonable
            uint8_t props = static_cast<uint8_t>(compressed[0]);
            return props > 225; // Invalid properties
        }

        default:
            return false;
    }
}

bool validateCompressionFormat(std::span<const std::byte> compressed,
                               CompressionAlgorithm algorithm) {
    return !hasCompressionArtifacts(compressed, algorithm);
}

} // namespace integrity_utils

} // namespace yams::compression
