#include <yams/indexing/indexing_pipeline.h>
#include <yams/indexing/document_indexer.h>
#include <spdlog/spdlog.h>
#include <chrono>
#include <sstream>
#include <iomanip>

namespace yams::indexing {

// Forward declaration of factory function
extern std::unique_ptr<IDocumentIndexer> createDocumentIndexer(
    std::shared_ptr<metadata::MetadataRepository> metadataRepo);

// Content processor implementation from document_indexer.cpp
class ContentProcessor : public IContentProcessor {
public:
    std::vector<ContentChunk> chunkContent(
        const std::string& content,
        const std::string& documentId,
        const IndexingConfig& config) override;
    
    std::string preprocessText(const std::string& text) override;
    
    std::vector<std::pair<std::string, double>> extractKeyTerms(
        const std::string& content,
        size_t maxTerms) override;
};

IndexingPipeline::IndexingPipeline(
    std::shared_ptr<metadata::MetadataRepository> metadataRepo,
    size_t maxConcurrency)
    : metadataRepo_(std::move(metadataRepo)),
      contentProcessor_(std::make_unique<ContentProcessor>()),
      maxConcurrency_(maxConcurrency) {
    
    spdlog::info("Initializing indexing pipeline with {} worker threads", 
                maxConcurrency_);
}

IndexingPipeline::~IndexingPipeline() {
    if (running_) {
        stop();
    }
}

void IndexingPipeline::start() {
    if (running_.exchange(true)) {
        spdlog::warn("Indexing pipeline already running");
        return;
    }
    
    spdlog::info("Starting indexing pipeline");
    
    // Start worker threads
    workers_.reserve(maxConcurrency_);
    for (size_t i = 0; i < maxConcurrency_; ++i) {
        workers_.emplace_back(&IndexingPipeline::workerThread, this);
    }
}

void IndexingPipeline::stop() {
    if (!running_.exchange(false)) {
        spdlog::warn("Indexing pipeline not running");
        return;
    }
    
    spdlog::info("Stopping indexing pipeline");
    
    // Wake up all worker threads
    queueCondition_.notify_all();
    
    // Wait for all workers to finish
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    
    workers_.clear();
    
    spdlog::info("Indexing pipeline stopped. Documents processed: {}, Failed: {}",
                documentsProcessed_.load(), documentsFailed_.load());
}

std::string IndexingPipeline::queueDocument(
    const std::filesystem::path& path,
    const IndexingConfig& config) {
    
    auto task = std::make_unique<IndexingTask>();
    task->path = path;
    task->config = config;
    task->stage = PipelineStage::Queued;
    task->queuedTime = std::chrono::system_clock::now();
    
    std::string taskId = generateTaskId();
    
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        taskQueue_.push(std::move(task));
    }
    
    queueCondition_.notify_one();
    
    spdlog::debug("Queued document for indexing: {} (Task ID: {})", 
                 path.string(), taskId);
    
    return taskId;
}

std::vector<std::string> IndexingPipeline::queueDocuments(
    const std::vector<std::filesystem::path>& paths,
    const IndexingConfig& config) {
    
    std::vector<std::string> taskIds;
    taskIds.reserve(paths.size());
    
    for (const auto& path : paths) {
        taskIds.push_back(queueDocument(path, config));
    }
    
    spdlog::info("Queued {} documents for indexing", paths.size());
    
    return taskIds;
}

std::unordered_map<std::string, int64_t> IndexingPipeline::getStatistics() const {
    return {
        {"documents_processed", documentsProcessed_.load()},
        {"documents_failed", documentsFailed_.load()},
        {"total_chunks", totalChunks_.load()},
        {"total_bytes", totalBytes_.load()},
        {"queue_size", static_cast<int64_t>(getQueueSize())},
        {"active_workers", static_cast<int64_t>(workers_.size())}
    };
}

size_t IndexingPipeline::getQueueSize() const {
    std::lock_guard<std::mutex> lock(queueMutex_);
    return taskQueue_.size();
}

void IndexingPipeline::setProgressCallback(ProgressCallback callback) {
    std::lock_guard<std::mutex> lock(progressMutex_);
    progressCallback_ = std::move(callback);
}

void IndexingPipeline::workerThread() {
    std::stringstream ss;
    ss << std::this_thread::get_id();
    spdlog::debug("Worker thread started: {}", ss.str());
    
    while (running_) {
        std::unique_ptr<IndexingTask> task;
        
        // Get task from queue
        {
            std::unique_lock<std::mutex> lock(queueMutex_);
            queueCondition_.wait(lock, [this] {
                return !taskQueue_.empty() || !running_;
            });
            
            if (!running_) {
                break;
            }
            
            if (taskQueue_.empty()) {
                continue;
            }
            
            task = std::move(taskQueue_.front());
            taskQueue_.pop();
        }
        
        // Process task
        auto result = processTask(*task);
        
        // Update statistics
        if (result.isSuccess()) {
            documentsProcessed_++;
            totalChunks_ += result.chunksCreated;
        } else {
            documentsFailed_++;
        }
        
        // Call progress callback
        {
            std::lock_guard<std::mutex> lock(progressMutex_);
            if (progressCallback_) {
                progressCallback_(
                    documentsProcessed_ + documentsFailed_,
                    documentsProcessed_ + documentsFailed_ + getQueueSize(),
                    result
                );
            }
        }
    }
    
    std::stringstream ss2;
    ss2 << std::this_thread::get_id();
    spdlog::debug("Worker thread stopped: {}", ss2.str());
}

IndexingResult IndexingPipeline::processTask(IndexingTask& task) {
    auto startTime = std::chrono::steady_clock::now();
    
    IndexingResult result;
    result.path = task.path;
    result.status = IndexingStatus::InProgress;
    
    try {
        // Stage 1: Extract document
        task.stage = PipelineStage::Extracting;
        if (!extractDocument(task)) {
            result.status = IndexingStatus::Failed;
            result.error = "Text extraction failed";
            return result;
        }
        
        // Stage 2: Process content
        task.stage = PipelineStage::Processing;
        if (!processContent(task)) {
            result.status = IndexingStatus::Failed;
            result.error = "Content processing failed";
            return result;
        }
        
        // Stage 3: Index content
        task.stage = PipelineStage::Indexing;
        if (!indexContent(task)) {
            result.status = IndexingStatus::Failed;
            result.error = "Content indexing failed";
            return result;
        }
        
        task.stage = PipelineStage::Completed;
        result.status = IndexingStatus::Completed;
        
        if (task.documentInfo) {
            result.documentId = std::to_string(task.documentInfo->id);
        }
        
    } catch (const std::exception& e) {
        result.status = IndexingStatus::Failed;
        result.error = "Exception: " + std::string(e.what());
        spdlog::error("Task processing failed for {}: {}", 
                     task.path.string(), e.what());
    }
    
    auto endTime = std::chrono::steady_clock::now();
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        endTime - startTime);
    
    return result;
}

bool IndexingPipeline::extractDocument(IndexingTask& task) {
    try {
        // Get appropriate extractor
        auto& factory = extraction::TextExtractorFactory::instance();
        auto extractor = factory.create(task.path.extension().string());
        
        if (!extractor) {
            spdlog::warn("No extractor available for extension: {}", 
                        task.path.extension().string());
            return false;
        }
        
        // Configure extraction
        extraction::ExtractionConfig config;
        config.maxFileSize = task.config.maxDocumentSize;
        config.extractMetadata = task.config.extractMetadata;
        config.detectLanguage = task.config.detectLanguage;
        config.preserveFormatting = true;
        config.timeout = task.config.timeout;
        
        // Extract text
        auto result = extractor->extract(task.path, config);
        if (!result) {
            spdlog::error("Extraction failed for {}: {}", 
                         task.path.string(), result.error().message);
            return false;
        }
        
        task.extractionResult = std::move(result).value();
        
        if (!task.extractionResult->isSuccess()) {
            spdlog::error("Extraction unsuccessful for {}: {}", 
                         task.path.string(), task.extractionResult->error);
            return false;
        }
        
        totalBytes_ += std::filesystem::file_size(task.path);
        
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Exception during extraction of {}: {}", 
                     task.path.string(), e.what());
        return false;
    }
}

bool IndexingPipeline::processContent(IndexingTask& task) {
    if (!task.extractionResult) {
        return false;
    }
    
    try {
        // Create document info
        metadata::DocumentInfo docInfo;
        docInfo.filePath = task.path.string();
        docInfo.fileName = task.path.filename().string();
        docInfo.fileExtension = task.path.extension().string();
        docInfo.fileSize = static_cast<int64_t>(std::filesystem::file_size(task.path));
        // Convert filesystem time to system_clock time
        auto fsTime = std::filesystem::last_write_time(task.path);
        auto scTime = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
            fsTime - std::filesystem::file_time_type::clock::now() + 
            std::chrono::system_clock::now()
        );
        docInfo.modifiedTime = scTime;
        docInfo.indexedTime = std::chrono::system_clock::now();
        docInfo.contentExtracted = true;
        docInfo.extractionStatus = metadata::ExtractionStatus::Success;
        
        // Set MIME type from extraction metadata if available
        auto& metadata = task.extractionResult->metadata;
        if (metadata.find("mime_type") != metadata.end()) {
            docInfo.mimeType = metadata["mime_type"];
        }
        
        task.documentInfo = std::move(docInfo);
        
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Exception during content processing of {}: {}", 
                     task.path.string(), e.what());
        return false;
    }
}

bool IndexingPipeline::indexContent(IndexingTask& task) {
    if (!task.extractionResult || !task.documentInfo) {
        return false;
    }
    
    try {
        // Add document to metadata repository
        auto result = metadataRepo_->insertDocument(task.documentInfo.value());
        if (!result) {
            spdlog::error("Failed to insert document to metadata repository: {}", 
                         result.error().message);
            return false;
        }
        
        task.documentInfo->id = result.value();
        
        // Chunk and index content
        auto chunks = contentProcessor_->chunkContent(
            task.extractionResult->text,
            std::to_string(task.documentInfo->id),
            task.config
        );
        
        totalChunks_ += chunks.size();
        
        // TODO: Index chunks into FTS5
        
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Exception during content indexing of {}: {}", 
                     task.path.string(), e.what());
        return false;
    }
}

std::string IndexingPipeline::generateTaskId() {
    uint64_t id = taskCounter_.fetch_add(1);
    
    // Create timestamp-based ID
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    
    std::stringstream ss;
    ss << std::hex << timestamp << "-" << std::setfill('0') 
       << std::setw(8) << id;
    
    return ss.str();
}

} // namespace yams::indexing