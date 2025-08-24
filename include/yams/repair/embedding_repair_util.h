#pragma once

#include <yams/api/content_store.h>
#include <yams/core/types.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>

#include <atomic>
#include <filesystem>
#include <functional>
#include <memory>
#include <vector>

namespace yams::repair {

// Progress callback for repair operations
using EmbeddingRepairProgressCallback =
    std::function<void(size_t current, size_t total, const std::string& details)>;

struct EmbeddingRepairStats {
    size_t documentsProcessed = 0;
    size_t embeddingsGenerated = 0;
    size_t embeddingsSkipped = 0;
    size_t failedOperations = 0;
};

struct EmbeddingRepairConfig {
    size_t batchSize = 32;
    bool skipExisting = true;
    std::string preferredModel; // Empty = auto-detect
    std::filesystem::path dataPath;
    bool verbose = false;
};

/**
 * Repair missing embeddings for all documents or specific document hashes.
 * This is a shared utility function that can be used by both CLI and daemon.
 *
 * @param contentStore The content store to retrieve document content
 * @param metadataRepo The metadata repository to query documents
 * @param embeddingGenerator The embedding generator (must be initialized)
 * @param config Configuration for the repair operation
 * @param documentHashes Optional list of specific document hashes to repair (empty = all)
 * @param progressCallback Optional callback for progress updates
 * @return Result containing repair statistics or error
 */
Result<EmbeddingRepairStats>
repairMissingEmbeddings(std::shared_ptr<api::IContentStore> contentStore,
                        std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
                        std::shared_ptr<vector::EmbeddingGenerator> embeddingGenerator,
                        const EmbeddingRepairConfig& config,
                        const std::vector<std::string>& documentHashes = {},
                        EmbeddingRepairProgressCallback progressCallback = nullptr);

/**
 * Check if a document has embeddings in the vector database.
 *
 * @param documentHash The SHA256 hash of the document
 * @param dataPath Path to the data directory containing vectors.db
 * @return true if embeddings exist, false otherwise
 */
bool hasEmbedding(const std::string& documentHash, const std::filesystem::path& dataPath);

/**
 * Get list of documents missing embeddings.
 *
 * @param metadataRepo The metadata repository
 * @param dataPath Path to the data directory containing vectors.db
 * @param limit Maximum number of documents to return (0 = all)
 * @return List of document hashes missing embeddings
 */
Result<std::vector<std::string>>
getDocumentsMissingEmbeddings(std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
                              const std::filesystem::path& dataPath, size_t limit = 0);

} // namespace yams::repair