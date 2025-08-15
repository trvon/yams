#include <spdlog/spdlog.h>
#include <yams/crypto/hasher.h>
#include <yams/integrity/chunk_validator.h>
#if defined(YAMS_HAS_STD_FORMAT) && YAMS_HAS_STD_FORMAT
#include <format>
namespace yamsfmt = std;
#else
#include <spdlog/fmt/fmt.h>
namespace yamsfmt = fmt;
#endif

#include <algorithm>
#include <execution>
#include <future>

namespace yams::integrity {

ChunkValidator::ChunkValidator(ChunkValidationConfig config)
    : config_(std::move(config)), hasher_(crypto::createSHA256Hasher()) {
    spdlog::debug("ChunkValidator initialized with validation={}", config_.enableValidation);
}

ChunkValidator::~ChunkValidator() = default;

ChunkValidator::ChunkValidator(ChunkValidator&&) noexcept = default;
ChunkValidator& ChunkValidator::operator=(ChunkValidator&&) noexcept = default;

ChunkValidationResult ChunkValidator::validateChunk(std::span<const std::byte> chunkData,
                                                    const std::string& expectedHash) {
    return validateChunkInternal(chunkData, expectedHash, 0, chunkData.size());
}

ValidationReport ChunkValidator::validateManifest(
    const manifest::Manifest& manifest,
    const std::function<Result<std::vector<std::byte>>(const std::string&)>& chunkProvider) {
    auto startTime = std::chrono::high_resolution_clock::now();

    ValidationReport report;
    report.totalChunks = manifest.chunks.size();

    if (!config_.enableValidation) {
        spdlog::debug("Chunk validation is disabled, skipping manifest validation");
        report.overallSuccess = true;
        report.validChunks = report.totalChunks;
        return report;
    }

    spdlog::info("Starting validation of {} chunks for file {}", manifest.chunks.size(),
                 manifest.fileHash.substr(0, 8));

    size_t currentChunk = 0;
    for (const auto& chunkRef : manifest.chunks) {
        currentChunk++;

        // Report progress
        if (progressCallback_) {
            progressCallback_(currentChunk, manifest.chunks.size());
        }

        // Retrieve chunk data
        auto chunkResult = chunkProvider(chunkRef.hash);
        if (!chunkResult.has_value()) {
            ChunkValidationResult result{.chunkHash = chunkRef.hash,
                                         .isValid = false,
                                         .errorMessage = "Failed to retrieve chunk: " +
                                                         chunkResult.error().message,
                                         .chunkOffset = chunkRef.offset,
                                         .chunkSize = chunkRef.size,
                                         .validationTime = std::chrono::milliseconds(0)};

            report.invalidChunks++;
            report.failures.push_back(result);

            if (config_.logErrors) {
                spdlog::error("Failed to retrieve chunk {} at offset {}: {}",
                              chunkRef.hash.substr(0, 8), chunkRef.offset,
                              chunkResult.error().message);
            }

            if (config_.failOnFirstError) {
                break;
            }
            continue;
        }

        const auto& chunkData = chunkResult.value();

        // Validate chunk size
        if (chunkData.size() != chunkRef.size) {
            ChunkValidationResult result{.chunkHash = chunkRef.hash,
                                         .isValid = false,
                                         .errorMessage =
                                             yamsfmt::format("Size mismatch: expected {}, got {}",
                                                             chunkRef.size, chunkData.size()),
                                         .chunkOffset = chunkRef.offset,
                                         .chunkSize = chunkRef.size,
                                         .validationTime = std::chrono::milliseconds(0)};

            report.invalidChunks++;
            report.failures.push_back(result);

            if (config_.logErrors) {
                spdlog::error("Chunk {} size mismatch at offset {}: expected {}, got {}",
                              chunkRef.hash.substr(0, 8), chunkRef.offset, chunkRef.size,
                              chunkData.size());
            }

            if (config_.failOnFirstError) {
                break;
            }
            continue;
        }

        // Validate chunk hash
        auto validationResult =
            validateChunkInternal(chunkData, chunkRef.hash, chunkRef.offset, chunkRef.size);

        if (validationResult.isValid) {
            report.validChunks++;
        } else {
            report.invalidChunks++;
            report.failures.push_back(validationResult);

            if (config_.logErrors) {
                spdlog::error("Chunk {} validation failed at offset {}: {}",
                              chunkRef.hash.substr(0, 8), chunkRef.offset,
                              validationResult.errorMessage);
            }

            if (config_.failOnFirstError) {
                break;
            }
        }
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    report.totalTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    report.overallSuccess = (report.invalidChunks == 0);

    // Update statistics
    stats_.totalValidations += report.totalChunks;
    stats_.successfulValidations += report.validChunks;
    stats_.failedValidations += report.invalidChunks;
    stats_.totalValidationTime += report.totalTime;

    spdlog::info("Manifest validation completed: {}/{} chunks valid, took {}ms", report.validChunks,
                 report.totalChunks, report.totalTime.count());

    return report;
}

Result<void> ChunkValidator::validateDuringReconstruction(const manifest::ChunkRef& chunkRef,
                                                          std::span<const std::byte> chunkData) {
    if (!config_.enableValidation) {
        return Result<void>();
    }

    auto result = validateChunkInternal(chunkData, chunkRef.hash, chunkRef.offset, chunkRef.size);

    if (!result.isValid) {
        return Result<void>(Error{ErrorCode::ValidationError,
                                  yamsfmt::format("Chunk validation failed at offset {}: {}",
                                                  chunkRef.offset, result.errorMessage)});
    }

    return Result<void>();
}

std::vector<ChunkValidationResult> ChunkValidator::validateChunks(
    const std::vector<std::pair<std::span<const std::byte>, std::string>>& chunks) {
    std::vector<ChunkValidationResult> results;
    results.reserve(chunks.size());

    // Use parallel execution if configured and beneficial
    // Note: std::execution::par_unseq not available in current implementation
    // TODO: Implement parallel validation using thread pool when needed
    if (false && config_.maxParallelValidations > 1 && chunks.size() > 10) {
        std::mutex resultsMutex;

        // Parallel execution would go here when std::execution is available
        // std::for_each(std::execution::par_unseq,
        //              chunks.begin(), chunks.end(),
        //              [this, &results, &resultsMutex](const auto& chunk) {
        //     auto result = validateChunk(chunk.first, chunk.second);
        //
        //     std::lock_guard lock(resultsMutex);
        //     results.push_back(result);
        // });
    } else {
        // Sequential validation
        for (const auto& [data, hash] : chunks) {
            results.push_back(validateChunk(data, hash));
        }
    }

    return results;
}

std::future<ValidationReport> ChunkValidator::validateManifestAsync(
    const manifest::Manifest& manifest,
    const std::function<Result<std::vector<std::byte>>(const std::string&)>& chunkProvider) {
    return std::async(std::launch::async, [this, manifest, chunkProvider]() {
        return validateManifest(manifest, chunkProvider);
    });
}

std::string ChunkValidator::calculateHash(std::span<const std::byte> data) {
    return hasher_->hash(data);
}

ChunkValidationResult ChunkValidator::validateChunkInternal(std::span<const std::byte> chunkData,
                                                            const std::string& expectedHash,
                                                            size_t offset, size_t size) {
    auto startTime = std::chrono::high_resolution_clock::now();

    ChunkValidationResult result{.chunkHash = expectedHash,
                                 .isValid = false,
                                 .errorMessage = "",
                                 .chunkOffset = offset,
                                 .chunkSize = size,
                                 .validationTime = std::chrono::milliseconds(0)};

    try {
        // Calculate actual hash
        std::string actualHash = calculateHash(chunkData);

        // Compare hashes
        if (actualHash == expectedHash) {
            result.isValid = true;
        } else {
            result.errorMessage =
                yamsfmt::format("Hash mismatch: expected {}, got {}", expectedHash.substr(0, 8),
                                actualHash.substr(0, 8));
        }
    } catch (const std::exception& e) {
        result.errorMessage = yamsfmt::format("Hash calculation failed: {}", e.what());
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    result.validationTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    return result;
}

std::unique_ptr<IChunkValidator> createChunkValidator(ChunkValidationConfig config) {
    return std::make_unique<ChunkValidator>(std::move(config));
}

} // namespace yams::integrity