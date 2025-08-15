#pragma once

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <yams/api/content_store.h>
#include <yams/core/types.h>
#include <yams/metadata/metadata_repository.h>

// Forward declarations
namespace yams::cli {
class YamsCLI;
}

namespace yams::vector {

/**
 * @brief Simple embedding service that reuses the proven repair logic
 *
 * This service provides a simplified interface for generating embeddings
 * by reusing the robust embedding generation code from the repair command.
 */
class EmbeddingService {
public:
    /**
     * @brief Create an embedding service instance
     * @param cli CLI instance for accessing storage and repositories
     * @return Service instance or nullptr if initialization failed
     */
    static std::unique_ptr<EmbeddingService> create(cli::YamsCLI* cli);

    EmbeddingService(std::shared_ptr<api::IContentStore> store,
                     std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
                     std::filesystem::path dataPath);

    /**
     * @brief Generate embedding for a single document (synchronous)
     * @param documentHash SHA256 hash of the document
     * @return Success or error result
     */
    Result<void> generateEmbeddingForDocument(const std::string& documentHash);

    /**
     * @brief Check if embeddings are missing and trigger repair if needed
     *
     * Checks system health and spawns a single detached repair thread
     * only if embeddings are missing and no repair is in progress.
     */
    void triggerRepairIfNeeded();

    /**
     * @brief Generate embeddings for multiple documents (synchronous)
     * @param documentHashes Vector of SHA256 hashes
     * @return Success or error result
     */
    Result<void> generateEmbeddingsForDocuments(const std::vector<std::string>& documentHashes);

    /**
     * @brief Check if embedding generation is available
     * @return True if models are available and service is ready
     */
    bool isAvailable() const;

private:
    std::shared_ptr<api::IContentStore> store_;
    std::shared_ptr<metadata::IMetadataRepository> metadataRepo_;
    std::filesystem::path dataPath_;

    // Simple static flag to prevent multiple repair threads
    static std::atomic<bool> repairInProgress_;

    // Internal helper methods (extracted from repair command)
    std::vector<std::string> getAvailableModels() const;
    Result<void> generateEmbeddingsInternal(const std::vector<std::string>& documentHashes,
                                            bool showProgress = false);

    // Static repair function that runs in detached thread
    static void runRepair(std::shared_ptr<api::IContentStore> store,
                          std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
                          std::filesystem::path dataPath);
};

} // namespace yams::vector