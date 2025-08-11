#pragma once

#include <yams/core/types.h>
#include <yams/chunking/chunker.h>
#include <yams/crypto/hasher.h>
#include <yams/manifest/manifest_manager.h>

#include <memory>
#include <vector>
#include <functional>
#include <chrono>

namespace yams::integrity {

/**
 * Configuration for chunk validation
 */
struct ChunkValidationConfig {
    bool enableValidation = true;              // Enable/disable validation
    bool failOnFirstError = false;             // Stop on first validation error
    bool logErrors = true;                     // Log validation errors
    size_t maxParallelValidations = 4;         // Max concurrent validations
    std::chrono::milliseconds timeout{5000};   // Validation timeout per chunk
};

/**
 * Result of validating a single chunk
 */
struct ChunkValidationResult {
    std::string chunkHash;
    bool isValid;
    std::string errorMessage;
    size_t chunkOffset;
    size_t chunkSize;
    std::chrono::milliseconds validationTime;
    
    bool operator==(const ChunkValidationResult& other) const = default;
};

/**
 * Overall validation report for a file/manifest
 */
struct ValidationReport {
    size_t totalChunks = 0;
    size_t validChunks = 0;
    size_t invalidChunks = 0;
    std::vector<ChunkValidationResult> failures;
    std::chrono::milliseconds totalTime{0};
    bool overallSuccess = false;
    
    double getSuccessRate() const {
        return totalChunks > 0 ? 
            static_cast<double>(validChunks) / totalChunks : 0.0;
    }
};

/**
 * Interface for chunk validation
 */
class IChunkValidator {
public:
    virtual ~IChunkValidator() = default;
    
    /**
     * Validate a single chunk
     * @param chunkData The chunk data to validate
     * @param expectedHash The expected hash of the chunk
     * @return Validation result
     */
    virtual ChunkValidationResult validateChunk(
        std::span<const std::byte> chunkData,
        const std::string& expectedHash) = 0;
    
    /**
     * Validate all chunks in a manifest
     * @param manifest The manifest containing chunk references
     * @param chunkProvider Provider to retrieve chunk data
     * @return Validation report
     */
    virtual ValidationReport validateManifest(
        const manifest::Manifest& manifest,
        const std::function<Result<std::vector<std::byte>>(const std::string&)>& chunkProvider) = 0;
    
    /**
     * Get current configuration
     */
    virtual const ChunkValidationConfig& getConfig() const = 0;
    
    /**
     * Update configuration
     */
    virtual void setConfig(const ChunkValidationConfig& config) = 0;
};

/**
 * Concrete implementation of chunk validator
 */
class ChunkValidator : public IChunkValidator {
public:
    explicit ChunkValidator(ChunkValidationConfig config = {});
    ~ChunkValidator();
    
    // Disable copy, enable move
    ChunkValidator(const ChunkValidator&) = delete;
    ChunkValidator& operator=(const ChunkValidator&) = delete;
    ChunkValidator(ChunkValidator&&) noexcept;
    ChunkValidator& operator=(ChunkValidator&&) noexcept;
    
    /**
     * Validate a single chunk
     */
    ChunkValidationResult validateChunk(
        std::span<const std::byte> chunkData,
        const std::string& expectedHash) override;
    
    /**
     * Validate all chunks in a manifest
     */
    ValidationReport validateManifest(
        const manifest::Manifest& manifest,
        const std::function<Result<std::vector<std::byte>>(const std::string&)>& chunkProvider) override;
    
    /**
     * Validate chunks during reconstruction
     * This can be called during file reconstruction to validate chunks on-the-fly
     */
    Result<void> validateDuringReconstruction(
        const manifest::ChunkRef& chunkRef,
        std::span<const std::byte> chunkData);
    
    /**
     * Batch validate multiple chunks
     */
    std::vector<ChunkValidationResult> validateChunks(
        const std::vector<std::pair<std::span<const std::byte>, std::string>>& chunks);
    
    /**
     * Asynchronous validation
     */
    std::future<ValidationReport> validateManifestAsync(
        const manifest::Manifest& manifest,
        const std::function<Result<std::vector<std::byte>>(const std::string&)>& chunkProvider);
    
    const ChunkValidationConfig& getConfig() const override { return config_; }
    void setConfig(const ChunkValidationConfig& config) override { config_ = config; }
    
    /**
     * Set progress callback for long-running validations
     */
    using ProgressCallback = std::function<void(size_t current, size_t total)>;
    void setProgressCallback(ProgressCallback callback) { progressCallback_ = callback; }
    
    /**
     * Get statistics about validations performed
     */
    struct Statistics {
        uint64_t totalValidations = 0;
        uint64_t successfulValidations = 0;
        uint64_t failedValidations = 0;
        std::chrono::milliseconds totalValidationTime{0};
        
        double getSuccessRate() const {
            return totalValidations > 0 ?
                static_cast<double>(successfulValidations) / totalValidations : 0.0;
        }
    };
    
    const Statistics& getStatistics() const { return stats_; }
    void resetStatistics() { stats_ = Statistics{}; }
    
private:
    ChunkValidationConfig config_;
    std::unique_ptr<crypto::IContentHasher> hasher_;
    ProgressCallback progressCallback_;
    Statistics stats_;
    
    // Helper to calculate hash of chunk data
    std::string calculateHash(std::span<const std::byte> data);
    
    // Helper to validate a single chunk with timing
    ChunkValidationResult validateChunkInternal(
        std::span<const std::byte> chunkData,
        const std::string& expectedHash,
        size_t offset,
        size_t size);
};

/**
 * Factory function to create a chunk validator
 */
std::unique_ptr<IChunkValidator> createChunkValidator(
    ChunkValidationConfig config = {});

} // namespace yams::integrity