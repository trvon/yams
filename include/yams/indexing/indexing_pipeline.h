#pragma once

#include <yams/indexing/document_indexer.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/extraction/text_extractor.h>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace yams::indexing {

/**
 * @brief Pipeline stage for document processing
 */
enum class PipelineStage {
    Queued,
    Extracting,
    Processing,
    Indexing,
    Completed
};

/**
 * @brief Document processing task
 */
struct IndexingTask {
    std::filesystem::path path;
    IndexingConfig config;
    PipelineStage stage = PipelineStage::Queued;
    std::chrono::system_clock::time_point queuedTime;
    std::optional<extraction::ExtractionResult> extractionResult;
    std::optional<metadata::DocumentInfo> documentInfo;
};

/**
 * @brief Indexing pipeline for document processing
 */
class IndexingPipeline {
public:
    /**
     * @brief Constructor
     * @param metadataRepo Metadata repository for document storage
     * @param maxConcurrency Maximum concurrent processing threads
     */
    IndexingPipeline(
        std::shared_ptr<metadata::MetadataRepository> metadataRepo,
        size_t maxConcurrency = std::thread::hardware_concurrency());
    
    ~IndexingPipeline();
    
    /**
     * @brief Start the pipeline processing threads
     */
    void start();
    
    /**
     * @brief Stop the pipeline and wait for completion
     */
    void stop();
    
    /**
     * @brief Add document to indexing queue
     * @param path Path to document
     * @param config Indexing configuration
     * @return Task ID for tracking
     */
    std::string queueDocument(
        const std::filesystem::path& path,
        const IndexingConfig& config = {});
    
    /**
     * @brief Queue multiple documents for indexing
     * @param paths Vector of document paths
     * @param config Indexing configuration
     * @return Vector of task IDs
     */
    std::vector<std::string> queueDocuments(
        const std::vector<std::filesystem::path>& paths,
        const IndexingConfig& config = {});
    
    /**
     * @brief Get pipeline statistics
     * @return Map of statistic name to value
     */
    std::unordered_map<std::string, int64_t> getStatistics() const;
    
    /**
     * @brief Get current queue size
     * @return Number of documents in queue
     */
    size_t getQueueSize() const;
    
    /**
     * @brief Check if pipeline is running
     * @return True if pipeline is active
     */
    bool isRunning() const { return running_.load(); }
    
    /**
     * @brief Set progress callback
     * @param callback Progress callback function
     */
    void setProgressCallback(ProgressCallback callback);
    
private:
    /**
     * @brief Worker thread function
     */
    void workerThread();
    
    /**
     * @brief Process a single indexing task
     * @param task Task to process
     * @return Indexing result
     */
    IndexingResult processTask(IndexingTask& task);
    
    /**
     * @brief Extract text from document
     * @param task Task containing document path
     * @return True if extraction successful
     */
    bool extractDocument(IndexingTask& task);
    
    /**
     * @brief Process extracted content
     * @param task Task with extraction result
     * @return True if processing successful
     */
    bool processContent(IndexingTask& task);
    
    /**
     * @brief Index processed content
     * @param task Task with processed content
     * @return True if indexing successful
     */
    bool indexContent(IndexingTask& task);
    
    /**
     * @brief Generate unique task ID
     * @return Unique identifier
     */
    std::string generateTaskId();
    
private:
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
    std::unique_ptr<IContentProcessor> contentProcessor_;
    
    // Thread management
    std::vector<std::thread> workers_;
    size_t maxConcurrency_;
    std::atomic<bool> running_{false};
    
    // Task queue
    std::queue<std::unique_ptr<IndexingTask>> taskQueue_;
    mutable std::mutex queueMutex_;
    std::condition_variable queueCondition_;
    
    // Statistics
    std::atomic<int64_t> documentsProcessed_{0};
    std::atomic<int64_t> documentsFailed_{0};
    std::atomic<int64_t> totalChunks_{0};
    std::atomic<int64_t> totalBytes_{0};
    
    // Progress tracking
    ProgressCallback progressCallback_;
    mutable std::mutex progressMutex_;
    
    // Task tracking
    std::unordered_map<std::string, std::unique_ptr<IndexingTask>> activeTasks_;
    mutable std::mutex taskMutex_;
    std::atomic<uint64_t> taskCounter_{0};
};

} // namespace yams::indexing