#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <span>
#include <sstream>
#include <string_view>
#include <unordered_set>
#include <vector>
#include <yams/content/content_handler_registry.h>
#include <yams/crypto/hasher.h>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/plain_text_extractor.h>
#include <yams/extraction/text_extractor.h>
#include <yams/indexing/document_indexer.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/metadata/query_helpers.h>

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

    [[nodiscard]] Result<IndexingResult> indexDocument(const std::filesystem::path& path,
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
            {
                auto derived = metadata::computePathDerivedValues(docInfo.filePath);
                docInfo.filePath = derived.normalizedPath;
                docInfo.pathPrefix = derived.pathPrefix;
                docInfo.reversePath = derived.reversePath;
                docInfo.pathHash = derived.pathHash;
                docInfo.parentHash = derived.parentHash;
                docInfo.pathDepth = derived.pathDepth;
            }
            // Convert filesystem time to system_clock time
            auto fsTime = std::filesystem::last_write_time(path);
            auto scTime = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
                fsTime - std::filesystem::file_time_type::clock::now() +
                std::chrono::system_clock::now());
            using std::chrono::floor;
            using namespace std::chrono;
            docInfo.modifiedTime = floor<seconds>(scTime);
            docInfo.indexedTime = floor<seconds>(std::chrono::system_clock::now());
            docInfo.contentExtracted = !extractedText.empty();
            docInfo.extractionStatus = metadata::ExtractionStatus::Success;

            // Calculate SHA256 hash
            auto hashResult = calculateFileHash(path);
            if (hashResult) {
                docInfo.sha256Hash = hashResult.value();
                spdlog::debug("indexDocument: Storing document {} with hash {}", path.string(),
                              docInfo.sha256Hash);
            } else {
                spdlog::error("indexDocument: Failed to calculate hash for {}", path.string());
            }

            auto existingDocByPath = metadataRepo_->findDocumentByExactPath(path.string());
            if (!existingDocByPath) {
                return Error{ErrorCode::InternalError,
                             "Failed to query by path: " + existingDocByPath.error().message};
            }

            int64_t documentId = -1;
            bool contentChanged = true;
            bool isNewDocument = false;

            if (existingDocByPath.value().has_value()) {
                // Update existing document
                isNewDocument = false;
                auto& existingDoc = existingDocByPath.value().value();
                documentId = existingDoc.id;
                docInfo.id = documentId;

                if (existingDoc.sha256Hash == docInfo.sha256Hash) {
                    contentChanged = false;
                }

                auto updateResult = metadataRepo_->updateDocument(docInfo);
                if (!updateResult) {
                    result.status = IndexingStatus::Failed;
                    result.error = updateResult.error().message;
                    return result;
                }
            } else {
                // Insert new document
                isNewDocument = true;
                auto insertResult = metadataRepo_->insertDocument(docInfo);
                if (!insertResult) {
                    result.status = IndexingStatus::Failed;
                    result.error = insertResult.error().message;
                    return result;
                }
                documentId = insertResult.value();

                // Feature-flagged versioning (Phase 1: path-series only)
                auto isVersioningEnabled = []() {
                    if (const char* env = std::getenv("YAMS_ENABLE_VERSIONING")) {
                        std::string_view v(env);
                        return !(v == "0" || v == "false" || v == "FALSE");
                    }
                    // default ON
                    return true;
                }();

                if (isVersioningEnabled) {
                    try {
                        const auto seriesKey = path.string();
                        // Find all docs with the exact same path (LIKE without wildcards acts as
                        // exact)
                        auto prevListRes =
                            metadata::queryDocumentsByPattern(*metadataRepo_, seriesKey);
                        if (prevListRes) {
                            const auto& prevList = prevListRes.value();
                            std::optional<metadata::DocumentInfo> prevLatest;
                            int64_t maxVersion = 0;
                            for (const auto& d : prevList) {
                                if (d.id == documentId)
                                    continue; // skip the newly inserted
                                auto isLatestRes = metadataRepo_->getMetadata(d.id, "is_latest");
                                if (isLatestRes && isLatestRes.value().has_value() &&
                                    isLatestRes.value().value().asBoolean()) {
                                    prevLatest = d;
                                    break;
                                }
                                // Fallback: remember highest version if present
                                auto verRes = metadataRepo_->getMetadata(d.id, "version");
                                if (verRes && verRes.value().has_value()) {
                                    int64_t v = 0;
                                    try {
                                        v = verRes.value().value().asInteger();
                                    } catch (...) {
                                        v = 0;
                                    }
                                    if (v > maxVersion) {
                                        maxVersion = v;
                                        prevLatest = d;
                                    }
                                }
                            }

                            // Prepare version metadata
                            int64_t newVersion = 1;
                            if (prevLatest.has_value()) {
                                // Flip previous latest off
                                auto _ = metadataRepo_->setMetadata(prevLatest->id, "is_latest",
                                                                    metadata::MetadataValue(false));
                                // Link lineage parent->child
                                metadata::DocumentRelationship rel;
                                rel.parentId = prevLatest->id;
                                rel.childId = documentId;
                                rel.relationshipType = metadata::RelationshipType::VersionOf;
                                rel.createdTime = std::chrono::floor<std::chrono::seconds>(
                                    std::chrono::system_clock::now());
                                (void)metadataRepo_->insertRelationship(rel);

                                // Compute incremented version
                                auto pv = metadataRepo_->getMetadata(prevLatest->id, "version");
                                if (pv && pv.value().has_value()) {
                                    try {
                                        newVersion = pv.value().value().asInteger() + 1;
                                    } catch (...) {
                                        newVersion = maxVersion > 0 ? maxVersion + 1 : 2;
                                    }
                                } else {
                                    newVersion = maxVersion > 0 ? maxVersion + 1 : 2;
                                }
                            } else {
                                newVersion = 1;
                            }

                            // Set new doc flags
                            (void)metadataRepo_->setMetadata(documentId, "version",
                                                             metadata::MetadataValue(newVersion));
                            (void)metadataRepo_->setMetadata(documentId, "is_latest",
                                                             metadata::MetadataValue(true));
                            (void)metadataRepo_->setMetadata(documentId, "series_key",
                                                             metadata::MetadataValue(seriesKey));

                            spdlog::info(
                                "Versioning: path='{}' new_id={} version={} prev_latest={}",
                                seriesKey, documentId, newVersion,
                                prevLatest.has_value() ? prevLatest->id : 0);
                        } else {
                            spdlog::warn("Versioning: queryDocumentsByPattern failed for '{}': {}",
                                         path.string(), prevListRes.error().message);
                        }
                    } catch (const std::exception& ex) {
                        spdlog::warn("Versioning: exception while updating lineage for '{}': {}",
                                     path.string(), ex.what());
                    }
                }
            }

            if (metadataRepo_) {
                auto treeRes = metadataRepo_->upsertPathTreeForDocument(
                    docInfo, documentId, isNewDocument, std::span<const float>());
                if (!treeRes) {
                    spdlog::warn("Failed to update path tree for {}: {}", docInfo.filePath,
                                 treeRes.error().message);
                }
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

            // Process and chunk content (only for new documents or if content changed)
            if (isNewDocument || contentChanged) {
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
                result.chunksCreated = 0; // No new chunks for metadata-only update
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

    [[nodiscard]] Result<std::vector<IndexingResult>>
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

    [[nodiscard]] Result<IndexingResult> updateDocument(const std::filesystem::path& path,
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

    [[nodiscard]] Result<bool> needsIndexing(const std::filesystem::path& path) override {
        if (!std::filesystem::exists(path)) {
            return Error{ErrorCode::FileNotFound, "File does not exist: " + path.string()};
        }

        // Query by path to get existing document info
        auto docResult = metadataRepo_->findDocumentByExactPath(path.string());
        if (!docResult) {
            return Error{ErrorCode::InternalError,
                         "Failed to query by path: " + docResult.error().message};
        }

        if (!docResult.value().has_value()) {
            // Not in DB, needs indexing
            return true;
        }

        auto& existingDoc = docResult.value().value();

        // Compare metadata (mtime and size) for a fast check
        std::error_code ec;
        auto current_mtime_fs = std::filesystem::last_write_time(path, ec);
        if (ec) {
            return Error{ErrorCode::IOError,
                         "Failed to get modification time for " + path.string()};
        }
        auto current_size = std::filesystem::file_size(path, ec);
        if (ec) {
            return Error{ErrorCode::IOError, "Failed to get file size for " + path.string()};
        }

        auto current_mtime_sc = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
            current_mtime_fs - std::filesystem::file_time_type::clock::now() +
            std::chrono::system_clock::now());

        // Compare seconds since epoch to avoid sub-second precision issues
        if (std::chrono::duration_cast<std::chrono::seconds>(
                existingDoc.modifiedTime.time_since_epoch())
                    .count() == std::chrono::duration_cast<std::chrono::seconds>(
                                    current_mtime_sc.time_since_epoch())
                                    .count() &&
            existingDoc.fileSize == static_cast<int64_t>(current_size)) {
            return false;
        }

        // For robustness, if metadata differs, check hash to be sure.
        auto newHashResult = calculateFileHash(path);
        if (!newHashResult) {
            return newHashResult.error();
        }

        if (existingDoc.sha256Hash == newHashResult.value()) {
            // Hashes match, but mtime/size was different.
            // This means only metadata changed. We should update it.
            // Returning true will trigger indexDocument, which should handle the update.
            return true;
        }

        // Hashes differ, needs indexing.
        return true;
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
