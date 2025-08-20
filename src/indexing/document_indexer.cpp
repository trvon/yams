#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <sstream>
#include <unordered_set>
#include <yams/content/content_handler_registry.h>
#include <yams/crypto/hasher.h>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/plain_text_extractor.h>
#include <yams/extraction/text_extractor.h>
#include <yams/indexing/document_indexer.h>
#include <yams/metadata/metadata_repository.h>

namespace yams::indexing {

/**
 * @brief Implementation of document indexer
 */
class DocumentIndexer : public IDocumentIndexer {
public:
    DocumentIndexer(std::shared_ptr<metadata::MetadataRepository> metadataRepo,
                    std::shared_ptr<IContentProcessor> contentProcessor)
        : metadataRepo_(std::move(metadataRepo)), contentProcessor_(std::move(contentProcessor)) {
        // Initialize content handler registry with default handlers
        auto& registry = content::ContentHandlerRegistry::instance();
        registry.initializeDefaultHandlers();

        // Keep text extractor for backward compatibility
        auto& factory = extraction::TextExtractorFactory::instance();
        textExtractor_ = factory.create(".txt"); // Default to plain text
    }

    Result<IndexingResult> indexDocument(const std::filesystem::path& path,
                                         const IndexingConfig& config) override {
        auto startTime = std::chrono::steady_clock::now();
        IndexingResult result;
        result.path = path;
        result.status = IndexingStatus::InProgress;

        try {
            // Check if file exists
            if (!std::filesystem::exists(path)) {
                result.status = IndexingStatus::Failed;
                result.error = "File does not exist: " + path.string();
                return result;
            }

            // Check file size
            auto fileSize = std::filesystem::file_size(path);
            if (fileSize > config.maxDocumentSize) {
                result.status = IndexingStatus::Skipped;
                result.error = "File too large: " + std::to_string(fileSize) + " bytes";
                return result;
            }

            // Use ContentHandlerRegistry to process the file
            auto& registry = content::ContentHandlerRegistry::instance();
            auto& detector = detection::FileTypeDetector::instance();

            // Detect file type
            auto signatureResult = detector.detectFromFile(path);
            if (!signatureResult) {
                result.status = IndexingStatus::Failed;
                result.error = "Failed to detect file type: " + signatureResult.error().message;
                return result;
            }

            // Get appropriate handler
            auto handler = registry.getHandler(signatureResult.value());

            std::string extractedText;
            std::unordered_map<std::string, std::string> metadata;

            if (handler) {
                // Use new content handler system
                content::ContentConfig contentConfig;
                contentConfig.maxFileSize = config.maxDocumentSize;
                contentConfig.extractMetadata = config.extractMetadata;
                contentConfig.detectLanguage = config.detectLanguage;
                contentConfig.preserveFormatting = true;

                auto processResult = handler->process(path, contentConfig);
                if (!processResult) {
                    result.status = IndexingStatus::Failed;
                    result.error = processResult.error().message;
                    return result;
                }

                auto& contentResult = processResult.value();
                extractedText = contentResult.text.value_or("");
                metadata = contentResult.metadata;

                // Skip indexing if handler says not to
                if (!contentResult.shouldIndex) {
                    result.status = IndexingStatus::Skipped;
                    result.error = "Content handler indicated file should not be indexed";
                    return result;
                }
            } else {
                // Fall back to legacy text extractor if available
                extraction::ExtractionConfig extractConfig;
                extractConfig.maxFileSize = config.maxDocumentSize;
                extractConfig.extractMetadata = config.extractMetadata;
                extractConfig.detectLanguage = config.detectLanguage;
                extractConfig.preserveFormatting = true;

                // Check if text extractor is available
                if (!textExtractor_) {
                    // Try to get extractor for this file type
                    auto& factory = extraction::TextExtractorFactory::instance();
                    textExtractor_ = factory.createForFile(path);

                    if (!textExtractor_) {
                        result.status = IndexingStatus::Failed;
                        result.error = "No handler or text extractor available for file type: " +
                                       path.extension().string();
                        return result;
                    }
                }

                auto extractResult = textExtractor_->extract(path, extractConfig);
                if (!extractResult) {
                    result.status = IndexingStatus::Failed;
                    result.error = extractResult.error().message;
                    return result;
                }

                auto& extraction = extractResult.value();
                if (!extraction.isSuccess()) {
                    result.status = IndexingStatus::Failed;
                    result.error = extraction.error;
                    return result;
                }

                extractedText = extraction.text;
                metadata = extraction.metadata;
            }

            // Create or update document info
            metadata::DocumentInfo docInfo;
            docInfo.filePath = path.string();
            docInfo.fileName = path.filename().string();
            docInfo.fileExtension = path.extension().string();
            docInfo.fileSize = static_cast<int64_t>(fileSize);
            // Convert filesystem time to system_clock time
            auto fsTime = std::filesystem::last_write_time(path);
            auto scTime = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
                fsTime - std::filesystem::file_time_type::clock::now() +
                std::chrono::system_clock::now());
            docInfo.modifiedTime = scTime;
            docInfo.indexedTime = std::chrono::system_clock::now();
            docInfo.contentExtracted = !extractedText.empty();
            docInfo.extractionStatus = metadata::ExtractionStatus::Success;

            // Store additional metadata from content handler
            // Note: This would require updating DocumentInfo to support arbitrary metadata
            // For now, we can at least log it
            if (!metadata.empty()) {
                spdlog::debug("Document {} has {} metadata items", path.string(), metadata.size());
                for (const auto& [key, value] : metadata) {
                    spdlog::trace("  {}: {}", key, value);
                }
            }

            // Calculate SHA256 hash
            auto hashResult = calculateFileHash(path);
            if (hashResult) {
                docInfo.sha256Hash = hashResult.value();
                spdlog::debug("indexDocument: Storing document {} with hash {}", path.string(),
                              docInfo.sha256Hash);
            } else {
                spdlog::error("indexDocument: Failed to calculate hash for {}", path.string());
            }

            // Check if document with same hash already exists (duplicate content)
            auto existingDoc = metadataRepo_->getDocumentByHash(docInfo.sha256Hash);
            int64_t documentId = -1;
            bool isNewDocument = true;

            if (existingDoc && existingDoc.value().has_value()) {
                // Document with same hash exists
                auto& existing = existingDoc.value().value();

                if (existing.filePath != path.string()) {
                    // Same content, different location
                    spdlog::info("Duplicate content detected: {} has same content as {}",
                                 path.string(), existing.filePath);

                    // Track alternate location
                    auto metaResult = metadataRepo_->setMetadata(
                        existing.id,
                        "alternate_location_" +
                            std::to_string(
                                std::chrono::system_clock::now().time_since_epoch().count()),
                        metadata::MetadataValue(path.string()));

                    // Create automatic snapshot if significant time has passed
                    auto timeDiff = docInfo.indexedTime - existing.indexedTime;
                    auto hoursSinceLastIndex =
                        std::chrono::duration_cast<std::chrono::hours>(timeDiff).count();

                    if (hoursSinceLastIndex > 24) {
                        auto timestamp = std::chrono::system_clock::now();
                        auto snapshotId =
                            "auto_" +
                            std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
                                               timestamp.time_since_epoch())
                                               .count());

                        metadataRepo_->setMetadata(existing.id, "snapshot_id",
                                                   metadata::MetadataValue(snapshotId));

                        spdlog::info("Created automatic snapshot {} for duplicate content",
                                     snapshotId);
                    }

                    // Update indexed time (make a copy since existing is const)
                    auto updatedDoc = existing;
                    updatedDoc.indexedTime = docInfo.indexedTime;
                    metadataRepo_->updateDocument(updatedDoc);

                    documentId = existing.id;
                    isNewDocument = false;
                } else {
                    // Same file, same content - shouldn't happen as needsIndexing() would return
                    // false
                    spdlog::warn("Unexpected: indexDocument called for unchanged file {}",
                                 path.string());
                    documentId = existing.id;
                    isNewDocument = false;
                }
            } else {
                // New document - insert it
                auto insertResult = metadataRepo_->insertDocument(docInfo);
                if (!insertResult) {
                    result.status = IndexingStatus::Failed;
                    result.error = insertResult.error().message;
                    return result;
                }

                documentId = insertResult.value();
            }

            result.documentId = std::to_string(documentId);

            // Store additional metadata from content handler
            if (!metadata.empty()) {
                for (const auto& [key, value] : metadata) {
                    auto metadataResult =
                        metadataRepo_->setMetadata(documentId, key, metadata::MetadataValue(value));
                    if (!metadataResult) {
                        spdlog::warn("Failed to store metadata {}={} for document {}: {}", key,
                                     value, documentId, metadataResult.error().message);
                    }
                }
                spdlog::debug("Stored {} metadata items for document {}", metadata.size(),
                              documentId);
            }

            // Process and chunk content (only for new documents)
            if (isNewDocument) {
                auto chunks =
                    contentProcessor_->chunkContent(extractedText, result.documentId, config);

                // Index chunks into FTS5
                for (const auto& chunk : chunks) {
                    auto indexResult = indexChunk(chunk);
                    if (!indexResult) {
                        spdlog::warn("Failed to index chunk {} for document {}: {}",
                                     chunk.chunkIndex, result.documentId,
                                     indexResult.error().message);
                    }
                }

                result.chunksCreated = chunks.size();
                chunksCreated_ += chunks.size();
            } else {
                result.chunksCreated = 0; // No new chunks for duplicate content
            }
            result.status = IndexingStatus::Completed;

            // Update statistics
            documentsIndexed_++;
            bytesProcessed_ += fileSize;

        } catch (const std::exception& e) {
            result.status = IndexingStatus::Failed;
            result.error = "Exception during indexing: " + std::string(e.what());
            spdlog::error("Document indexing failed: {}", e.what());
        }

        auto endTime = std::chrono::steady_clock::now();
        result.duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

        return result;
    }

    Result<std::vector<IndexingResult>>
    indexDocuments(const std::vector<std::filesystem::path>& paths, const IndexingConfig& config,
                   ProgressCallback progress) override {
        std::vector<IndexingResult> results;
        results.reserve(paths.size());

        size_t current = 0;
        for (const auto& path : paths) {
            auto result = indexDocument(path, config);
            if (result) {
                results.push_back(std::move(result).value());

                if (progress) {
                    progress(++current, paths.size(), results.back());
                }
            } else {
                IndexingResult failedResult;
                failedResult.path = path;
                failedResult.status = IndexingStatus::Failed;
                failedResult.error = result.error().message;
                results.push_back(failedResult);

                if (progress) {
                    progress(++current, paths.size(), results.back());
                }
            }
        }

        return results;
    }

    Result<IndexingResult> updateDocument(const std::filesystem::path& path,
                                          const IndexingConfig& config) override {
        // Check if document exists in index
        auto needsUpdate = needsIndexing(path);
        if (!needsUpdate) {
            return Error{ErrorCode::InternalError, needsUpdate.error().message};
        }

        if (!needsUpdate.value()) {
            IndexingResult result;
            result.path = path;
            result.status = IndexingStatus::Skipped;
            result.error = "Document is up to date";
            return result;
        }

        // Re-index the document
        return indexDocument(path, config);
    }

    Result<void> removeDocument(const std::string& documentId) override {
        // Remove from metadata repository
        int64_t docId = std::stoll(documentId);
        auto deleteResult = metadataRepo_->deleteDocument(docId);
        if (!deleteResult) {
            return deleteResult;
        }

        // Remove from FTS5 index
        auto ftsResult = removeFromFTS(documentId);
        if (!ftsResult) {
            return ftsResult;
        }

        documentsRemoved_++;
        return {};
    }

    Result<bool> needsIndexing(const std::filesystem::path& path) override {
        if (!std::filesystem::exists(path)) {
            return Error{ErrorCode::FileNotFound, "File does not exist: " + path.string()};
        }

        // Calculate current file hash
        auto hashResult = calculateFileHash(path);
        if (!hashResult) {
            return Error{ErrorCode::InternalError, "Failed to calculate file hash"};
        }

        spdlog::debug("needsIndexing: Calculated hash for {}: {}", path.string(),
                      hashResult.value());

        // Check if document exists in index
        auto docResult = metadataRepo_->getDocumentByHash(hashResult.value());
        if (!docResult) {
            return Error{ErrorCode::InternalError, docResult.error().message};
        }

        if (!docResult.value().has_value()) {
            // Document not in index, needs indexing
            spdlog::debug("needsIndexing: Document with hash {} not found in index",
                          hashResult.value());
            return true;
        }

        // Document with this hash exists - check if it's the same file
        auto& existingDoc = docResult.value().value();

        if (existingDoc.filePath == path.string()) {
            // Same file, same content - no re-indexing needed
            spdlog::debug("needsIndexing: Document {} already indexed with same hash",
                          path.string());
            return false;
        } else {
            // Same content, different file - this needs special handling
            // We'll track this as an alternate location but won't re-index the content
            spdlog::debug("needsIndexing: Found duplicate content - {} has same hash as {}",
                          path.string(), existingDoc.filePath);
            // Return true so indexDocument() can handle the relationship tracking
            return true;
        }
    }

    std::unordered_map<std::string, int64_t> getStatistics() const override {
        return {{"documents_indexed", documentsIndexed_.load()},
                {"documents_removed", documentsRemoved_.load()},
                {"chunks_created", chunksCreated_.load()},
                {"bytes_processed", bytesProcessed_.load()}};
    }

private:
    Result<std::string> calculateFileHash(const std::filesystem::path& path) {
        try {
            crypto::SHA256Hasher hasher;
            return hasher.hashFile(path);
        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError,
                         "Failed to calculate SHA256 hash: " + std::string(e.what())};
        }
    }

    Result<void> indexChunk(const ContentChunk& chunk) {
        // TODO: Implement FTS5 indexing
        // This will insert the chunk into the FTS5 table
        spdlog::debug("Indexing chunk {} for document {}", chunk.chunkIndex, chunk.documentId);
        return {};
    }

    Result<void> removeFromFTS(const std::string& documentId) {
        // TODO: Implement FTS5 removal
        spdlog::debug("Removing document {} from FTS index", documentId);
        return {};
    }

private:
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<IContentProcessor> contentProcessor_;
    std::shared_ptr<extraction::ITextExtractor> textExtractor_;

    // Statistics
    std::atomic<int64_t> documentsIndexed_{0};
    std::atomic<int64_t> documentsRemoved_{0};
    std::atomic<int64_t> chunksCreated_{0};
    std::atomic<int64_t> bytesProcessed_{0};
};

/**
 * @brief Implementation of content processor
 */
class ContentProcessor : public IContentProcessor {
public:
    std::vector<ContentChunk> chunkContent(const std::string& content,
                                           const std::string& documentId,
                                           const IndexingConfig& config) override {
        std::vector<ContentChunk> chunks;

        if (content.size() <= config.chunkSize) {
            // Small document, single chunk
            ContentChunk chunk;
            chunk.documentId = documentId;
            chunk.chunkIndex = 0;
            chunk.startOffset = 0;
            chunk.endOffset = content.size();
            chunk.content = preprocessText(content);
            chunks.push_back(std::move(chunk));
            return chunks;
        }

        // Large document, split into chunks with overlap
        size_t offset = 0;
        size_t chunkIndex = 0;

        while (offset < content.size()) {
            ContentChunk chunk;
            chunk.documentId = documentId;
            chunk.chunkIndex = chunkIndex++;
            chunk.startOffset = offset;

            // Calculate chunk end with overlap
            size_t chunkEnd = std::min(offset + config.chunkSize, content.size());

            // Try to break at word boundary
            if (chunkEnd < content.size()) {
                size_t lastSpace = content.find_last_of(" \n\t", chunkEnd);
                if (lastSpace != std::string::npos && lastSpace > offset + config.chunkSize / 2) {
                    chunkEnd = lastSpace + 1;
                }
            }

            chunk.endOffset = chunkEnd;
            chunk.content = preprocessText(content.substr(offset, chunkEnd - offset));
            chunks.push_back(std::move(chunk));

            // Move offset with overlap
            if (chunkEnd >= content.size()) {
                break;
            }
            offset = chunkEnd - config.overlapSize;
        }

        return chunks;
    }

    std::string preprocessText(const std::string& text) override {
        // Basic text preprocessing
        std::string processed = text;

        // Normalize whitespace
        bool inSpace = false;
        size_t writePos = 0;
        for (size_t readPos = 0; readPos < processed.size(); ++readPos) {
            char c = processed[readPos];
            if (std::isspace(c)) {
                if (!inSpace) {
                    processed[writePos++] = ' ';
                    inSpace = true;
                }
            } else {
                processed[writePos++] = c;
                inSpace = false;
            }
        }
        processed.resize(writePos);

        // Trim leading/trailing whitespace
        size_t start = processed.find_first_not_of(" \t\n\r");
        size_t end = processed.find_last_not_of(" \t\n\r");

        if (start == std::string::npos) {
            return "";
        }

        return processed.substr(start, end - start + 1);
    }

    std::vector<std::pair<std::string, double>> extractKeyTerms(const std::string& content,
                                                                size_t maxTerms) override {
        // Simple term frequency extraction
        std::unordered_map<std::string, int> termFrequency;

        // Tokenize content
        std::istringstream stream(content);
        std::string word;
        while (stream >> word) {
            // Convert to lowercase
            std::transform(word.begin(), word.end(), word.begin(), ::tolower);

            // Remove punctuation
            word.erase(
                std::remove_if(word.begin(), word.end(), [](char c) { return !std::isalnum(c); }),
                word.end());

            if (word.length() > 2 && !isStopWord(word)) {
                termFrequency[word]++;
            }
        }

        // Sort by frequency
        std::vector<std::pair<std::string, double>> terms;
        for (const auto& [term, freq] : termFrequency) {
            terms.emplace_back(term, static_cast<double>(freq));
        }

        std::sort(terms.begin(), terms.end(),
                  [](const auto& a, const auto& b) { return a.second > b.second; });

        // Return top terms
        if (terms.size() > maxTerms) {
            terms.resize(maxTerms);
        }

        return terms;
    }

private:
    bool isStopWord(const std::string& word) {
        static const std::unordered_set<std::string> stopWords = {
            "the",    "is",    "at",    "which", "on",      "and",    "a",    "an",      "as",
            "are",    "was",   "were",  "been",  "be",      "have",   "has",  "had",     "do",
            "does",   "did",   "will",  "would", "could",   "should", "may",  "might",   "must",
            "shall",  "can",   "need",  "dare",  "ought",   "used",   "to",   "of",      "in",
            "for",    "with",  "by",    "from",  "up",      "about",  "into", "through", "during",
            "before", "after", "above", "below", "between", "under",  "over"};

        return stopWords.find(word) != stopWords.end();
    }
};

// Factory function
std::unique_ptr<IDocumentIndexer>
createDocumentIndexer(std::shared_ptr<metadata::MetadataRepository> metadataRepo) {
    auto contentProcessor = std::make_shared<ContentProcessor>();
    return std::make_unique<DocumentIndexer>(metadataRepo, contentProcessor);
}

} // namespace yams::indexing