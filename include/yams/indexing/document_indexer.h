#pragma once

#include <yams/core/types.h>
#include <yams/metadata/document_metadata.h>
#include <yams/extraction/text_extractor.h>
#include <filesystem>
#include <functional>
#include <memory>
#include <vector>
#include <optional>
#include <chrono>

namespace yams::indexing {

/**
 * @brief Configuration for document indexing
 */
struct IndexingConfig {
    size_t chunkSize = 4096;              // Size of text chunks for large documents
    size_t overlapSize = 256;             // Overlap between chunks
    size_t maxDocumentSize = 100 * 1024 * 1024; // 100MB max document size
    size_t batchSize = 100;               // Number of documents per batch
    bool extractMetadata = true;          // Extract document metadata
    bool detectLanguage = true;           // Detect document language
    bool updateExisting = true;           // Update existing indexed documents
    std::chrono::milliseconds timeout{60000}; // Indexing timeout per document
};

/**
 * @brief Status of indexing operation
 */
enum class IndexingStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Skipped
};

/**
 * @brief Result of indexing a single document
 */
struct IndexingResult {
    std::filesystem::path path;
    IndexingStatus status;
    std::string documentId;
    size_t chunksCreated = 0;
    std::chrono::milliseconds duration{0};
    std::optional<std::string> error;
    
    bool isSuccess() const { return status == IndexingStatus::Completed; }
};

/**
 * @brief Progress callback for batch indexing
 */
using ProgressCallback = std::function<void(size_t current, size_t total, const IndexingResult&)>;

/**
 * @brief Interface for document indexer
 */
class IDocumentIndexer {
public:
    virtual ~IDocumentIndexer() = default;
    
    /**
     * @brief Index a single document
     * @param path Path to the document
     * @param config Indexing configuration
     * @return Result of indexing operation
     */
    virtual Result<IndexingResult> indexDocument(
        const std::filesystem::path& path,
        const IndexingConfig& config = {}) = 0;
    
    /**
     * @brief Index multiple documents
     * @param paths Paths to documents
     * @param config Indexing configuration
     * @param progress Progress callback (optional)
     * @return Vector of indexing results
     */
    virtual Result<std::vector<IndexingResult>> indexDocuments(
        const std::vector<std::filesystem::path>& paths,
        const IndexingConfig& config = {},
        ProgressCallback progress = nullptr) = 0;
    
    /**
     * @brief Update index for a modified document
     * @param path Path to the document
     * @param config Indexing configuration
     * @return Result of update operation
     */
    virtual Result<IndexingResult> updateDocument(
        const std::filesystem::path& path,
        const IndexingConfig& config = {}) = 0;
    
    /**
     * @brief Remove document from index
     * @param documentId Document identifier
     * @return Success or error
     */
    virtual Result<void> removeDocument(const std::string& documentId) = 0;
    
    /**
     * @brief Check if document needs re-indexing
     * @param path Path to the document
     * @return True if document is new or modified
     */
    virtual Result<bool> needsIndexing(const std::filesystem::path& path) = 0;
    
    /**
     * @brief Get indexing statistics
     * @return Map of statistic name to value
     */
    virtual std::unordered_map<std::string, int64_t> getStatistics() const = 0;
};

/**
 * @brief Document content chunk for indexing
 */
struct ContentChunk {
    std::string documentId;
    size_t chunkIndex;
    size_t startOffset;
    size_t endOffset;
    std::string content;
    std::string language;
    std::unordered_map<std::string, std::string> metadata;
};

/**
 * @brief Interface for content processing
 */
class IContentProcessor {
public:
    virtual ~IContentProcessor() = default;
    
    /**
     * @brief Split content into chunks
     * @param content Full document content
     * @param config Indexing configuration
     * @return Vector of content chunks
     */
    virtual std::vector<ContentChunk> chunkContent(
        const std::string& content,
        const std::string& documentId,
        const IndexingConfig& config) = 0;
    
    /**
     * @brief Preprocess text for indexing
     * @param text Raw text
     * @return Processed text
     */
    virtual std::string preprocessText(const std::string& text) = 0;
    
    /**
     * @brief Extract key terms from content
     * @param content Document content
     * @param maxTerms Maximum number of terms to extract
     * @return Vector of key terms with weights
     */
    virtual std::vector<std::pair<std::string, double>> extractKeyTerms(
        const std::string& content,
        size_t maxTerms = 20) = 0;
};

} // namespace yams::indexing

// Forward declaration in the correct namespace
namespace yams::metadata {
    class MetadataRepository;
}

namespace yams::indexing {

/**
 * @brief Factory function to create DocumentIndexer
 * @param metadataRepo Metadata repository for document storage
 * @return Unique pointer to IDocumentIndexer implementation
 */
std::unique_ptr<IDocumentIndexer> createDocumentIndexer(
    std::shared_ptr<metadata::MetadataRepository> metadataRepo);

} // namespace yams::indexing