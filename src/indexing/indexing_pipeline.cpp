#include <spdlog/spdlog.h>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <sstream>
#include <yams/crypto/hasher.h>
#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/indexing/document_indexer.h>
#include <yams/indexing/indexing_pipeline.h>
#include <yams/metadata/path_utils.h>
#include <yams/profiling.h>

namespace yams::indexing {

// Forward declaration of factory function
extern std::unique_ptr<IDocumentIndexer>
createDocumentIndexer(std::shared_ptr<metadata::MetadataRepository> metadataRepo);

// Content processor implementation from document_indexer.cpp
class ContentProcessor : public IContentProcessor {
public:
    std::vector<ContentChunk> chunkContent(const std::string& content,
                                           const std::string& documentId,
                                           const IndexingConfig& config) override;

    std::string preprocessText(const std::string& text) override;

    std::vector<std::pair<std::string, double>> extractKeyTerms(const std::string& content,
                                                                size_t maxTerms) override;
};

IndexingPipeline::IndexingPipeline(std::shared_ptr<metadata::MetadataRepository> metadataRepo,
                                   size_t maxConcurrency)
    : metadataRepo_(std::move(metadataRepo)),
      contentProcessor_(std::make_unique<ContentProcessor>()), maxConcurrency_(maxConcurrency) {
    spdlog::info("Initializing indexing pipeline with {} worker threads", maxConcurrency_);
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

std::string IndexingPipeline::queueDocument(const std::filesystem::path& path,
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

    spdlog::debug("Queued document for indexing: {} (Task ID: {})", path.string(), taskId);

    return taskId;
}

std::vector<std::string>
IndexingPipeline::queueDocuments(const std::vector<std::filesystem::path>& paths,
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
    return {{"documents_processed", documentsProcessed_.load()},
            {"documents_failed", documentsFailed_.load()},
            {"total_chunks", totalChunks_.load()},
            {"total_bytes", totalBytes_.load()},
            {"queue_size", static_cast<int64_t>(getQueueSize())},
            {"active_workers", static_cast<int64_t>(workers_.size())}};
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
            queueCondition_.wait(lock, [this] { return !taskQueue_.empty() || !running_; });

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
                progressCallback_(documentsProcessed_ + documentsFailed_,
                                  documentsProcessed_ + documentsFailed_ + getQueueSize(), result);
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

        // Best-effort: delegate entity/symbol extraction to daemon service
        try {
            namespace ydaemon = ::yams::daemon;
            extern ydaemon::ServiceManager* yams_get_global_service_manager();
            if (auto* sm = yams_get_global_service_manager()) {
                auto gc = sm->getGraphComponent();
                if (gc && task.extractionResult) {
                    ydaemon::GraphComponent::EntityExtractionJob j;
                    if (task.documentInfo)
                        j.documentHash = task.documentInfo->sha256Hash;
                    j.filePath = task.path.string();
                    j.contentUtf8 = task.extractionResult->text;
                    j.language = task.extractionResult->language;
                    (void)gc->submitEntityExtraction(std::move(j));
                }
            }
        } catch (...) {
        }

        task.stage = PipelineStage::Completed;
        result.status = IndexingStatus::Completed;

        if (task.documentInfo) {
            result.documentId = std::to_string(task.documentInfo->id);
        }

    } catch (const std::exception& e) {
        result.status = IndexingStatus::Failed;
        result.error = "Exception: " + std::string(e.what());
        spdlog::error("Task processing failed for {}: {}", task.path.string(), e.what());
    }

    auto endTime = std::chrono::steady_clock::now();
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

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
            spdlog::error("Extraction failed for {}: {}", task.path.string(),
                          result.error().message);
            return false;
        }

        task.extractionResult = std::move(result).value();

        if (!task.extractionResult->isSuccess()) {
            spdlog::error("Extraction unsuccessful for {}: {}", task.path.string(),
                          task.extractionResult->error);
            return false;
        }

        totalBytes_ += std::filesystem::file_size(task.path);

        return true;

    } catch (const std::exception& e) {
        spdlog::error("Exception during extraction of {}: {}", task.path.string(), e.what());
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

        // Calculate SHA256 hash of the file
        try {
            crypto::SHA256Hasher hasher;
            docInfo.sha256Hash = hasher.hashFile(task.path);
        } catch (const std::exception& e) {
            spdlog::error("Failed to calculate SHA256 hash for {}: {}", task.path.string(),
                          e.what());
            return false;
        }

        // Convert filesystem time to system_clock time
        auto fsTime = std::filesystem::last_write_time(task.path);
        auto scTime = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
            fsTime - std::filesystem::file_time_type::clock::now() +
            std::chrono::system_clock::now());
        using std::chrono::floor;
        using namespace std::chrono;
        docInfo.modifiedTime = floor<seconds>(scTime);
        docInfo.indexedTime = floor<seconds>(std::chrono::system_clock::now());
        docInfo.contentExtracted = true;
        docInfo.extractionStatus = metadata::ExtractionStatus::Success;

        auto derived = metadata::computePathDerivedValues(docInfo.filePath);
        docInfo.filePath = derived.normalizedPath;
        docInfo.pathPrefix = derived.pathPrefix;
        docInfo.reversePath = derived.reversePath;
        docInfo.pathHash = derived.pathHash;
        docInfo.parentHash = derived.parentHash;
        docInfo.pathDepth = derived.pathDepth;

        // Set MIME type from extraction metadata if available
        auto& metadata = task.extractionResult->metadata;
        if (metadata.find("mime_type") != metadata.end()) {
            docInfo.mimeType = metadata["mime_type"];
        }

        task.documentInfo = std::move(docInfo);

        return true;

    } catch (const std::exception& e) {
        spdlog::error("Exception during content processing of {}: {}", task.path.string(),
                      e.what());
        return false;
    }
}

bool IndexingPipeline::indexContent(IndexingTask& task) {
    YAMS_ZONE_SCOPED_N("IndexingPipeline::indexContent");

    if (!task.extractionResult || !task.documentInfo) {
        return false;
    }

    try {
        // Check if document with same hash already exists
        auto existingDoc = metadataRepo_->getDocumentByHash(task.documentInfo->sha256Hash);
        if (!existingDoc) {
            spdlog::error("Failed to check for existing document: {}", existingDoc.error().message);
            return false;
        }

        int64_t documentId = -1;
        bool isNewDocument = true;

        if (existingDoc.value().has_value()) {
            // Document with same hash exists
            auto& existing = existingDoc.value().value();
            isNewDocument = false;
            documentId = existing.id;

            auto derivedExisting = metadata::computePathDerivedValues(existing.filePath);
            existing.pathPrefix = derivedExisting.pathPrefix;
            existing.reversePath = derivedExisting.reversePath;
            existing.pathHash = derivedExisting.pathHash;
            existing.parentHash = derivedExisting.parentHash;
            existing.pathDepth = derivedExisting.pathDepth;

            spdlog::debug("Found existing document with hash {} at path {}",
                          task.documentInfo->sha256Hash, existing.filePath);

            if (existing.filePath != task.documentInfo->filePath) {
                // Same content, different location - track this relationship
                spdlog::info("Duplicate content detected: {} has same content as {}",
                             task.path.string(), existing.filePath);

                // Add metadata to track alternate location
                auto metaResult = metadataRepo_->setMetadata(
                    existing.id,
                    "alternate_location_" +
                        std::to_string(std::chrono::system_clock::now().time_since_epoch().count()),
                    metadata::MetadataValue(task.path.string()));

                if (!metaResult) {
                    spdlog::warn("Failed to add alternate location metadata: {}",
                                 metaResult.error().message);
                }
            }

            // Update the existing document's indexed time (make a copy since existing is const)
            auto updatedDoc = existing;
            updatedDoc.indexedTime = task.documentInfo->indexedTime;
            auto updateResult = metadataRepo_->updateDocument(updatedDoc);
            if (!updateResult) {
                spdlog::warn("Failed to update document indexed time: {}",
                             updateResult.error().message);
            }

            task.documentInfo->id = documentId;
        } else {
            // New document - insert it
            auto result = metadataRepo_->insertDocument(task.documentInfo.value());
            if (!result) {
                spdlog::error("Failed to insert document to metadata repository: {}",
                              result.error().message);
                return false;
            }

            documentId = result.value();
            task.documentInfo->id = documentId;
        }

        // Always create a snapshot on every ingestion for version tracking
        auto timestamp = std::chrono::system_clock::now();
        auto snapshotId = "auto_" + std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
                                                       timestamp.time_since_epoch())
                                                       .count());

        metadataRepo_->setMetadata(documentId, "snapshot_id", metadata::MetadataValue(snapshotId));
        metadataRepo_->setMetadata(
            documentId, "snapshot_time",
            metadata::MetadataValue(static_cast<int64_t>(timestamp.time_since_epoch().count())));

        spdlog::debug("Created automatic snapshot {} for document {}", snapshotId,
                      isNewDocument ? "(new)" : "(existing)");

        // Chunk and index content (only for new documents)
        if (isNewDocument) {
            auto chunks = contentProcessor_->chunkContent(task.extractionResult->text,
                                                          std::to_string(documentId), task.config);

            totalChunks_ += chunks.size();
        }

        // TODO: Index chunks into FTS5

        return true;

    } catch (const std::exception& e) {
        spdlog::error("Exception during content indexing of {}: {}", task.path.string(), e.what());
        return false;
    }
}

std::string IndexingPipeline::generateTaskId() {
    uint64_t id = taskCounter_.fetch_add(1);

    // Create timestamp-based ID
    auto now = std::chrono::system_clock::now();
    auto timestamp =
        std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

    std::stringstream ss;
    ss << std::hex << timestamp << "-" << std::setfill('0') << std::setw(8) << id;

    return ss.str();
}

} // namespace yams::indexing
