#include <yams/extraction/extraction_util.h>
#include <yams/extraction/text_extractor.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/vector/vector_database.h>

#include <spdlog/spdlog.h>
#include <atomic>
#include <ctime>
#include <fcntl.h>
#include <filesystem>
#include <thread>
#include <unistd.h>

namespace yams::repair {

namespace {
// File lock for cross-process safety
class VectorDbLock {
public:
    explicit VectorDbLock(const std::filesystem::path& lockPath) : path_(lockPath), fd_(-1) {
        fd_ = ::open(path_.c_str(), O_CREAT | O_RDWR, 0644);
        if (fd_ >= 0) {
            struct flock fl{};
            fl.l_type = F_WRLCK;
            fl.l_whence = SEEK_SET;
            fl.l_start = 0;
            fl.l_len = 0;
            if (fcntl(fd_, F_SETLK, &fl) == -1) {
                ::close(fd_);
                fd_ = -1;
            } else {
                // Stamp PID and timestamp for diagnostics
                try {
                    (void)ftruncate(fd_, 0);
                    std::string stamp = std::to_string(static_cast<long long>(::getpid())) + " " +
                                        std::to_string(static_cast<long long>(::time(nullptr))) +
                                        "\n";
                    (void)::write(fd_, stamp.data(), stamp.size());
                    (void)lseek(fd_, 0, SEEK_SET);
                } catch (...) {
                }
            }
        }
    }

    bool isLocked() const { return fd_ >= 0; }

    ~VectorDbLock() {
        if (fd_ >= 0) {
            struct flock fl{};
            fl.l_type = F_UNLCK;
            fl.l_whence = SEEK_SET;
            fl.l_start = 0;
            fl.l_len = 0;
            (void)fcntl(fd_, F_SETLK, &fl);
            ::close(fd_);
        }
    }

private:
    std::filesystem::path path_;
    int fd_;
};
} // namespace

Result<EmbeddingRepairStats>
repairMissingEmbeddings(std::shared_ptr<api::IContentStore> contentStore,
                        std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
                        std::shared_ptr<vector::EmbeddingGenerator> embeddingGenerator,
                        const EmbeddingRepairConfig& config,
                        const std::vector<std::string>& documentHashes,
                        EmbeddingRepairProgressCallback progressCallback,
                        const yams::extraction::ContentExtractorList& extractors) {
    EmbeddingRepairStats stats;

    if (!contentStore || !metadataRepo || !embeddingGenerator) {
        return Error{ErrorCode::InvalidArgument, "Missing required components"};
    }

    // Ensure embedding generator is initialized
    if (!embeddingGenerator->isInitialized()) {
        if (!embeddingGenerator->initialize()) {
            return Error{ErrorCode::InternalError, "Failed to initialize embedding generator"};
        }
    }

    // Initialize vector database. If missing, create with the generator's dimension.
    vector::VectorDatabaseConfig vdbConfig;
    vdbConfig.database_path = (config.dataPath / "vectors.db").string();
    vdbConfig.embedding_dim = embeddingGenerator->getEmbeddingDimension();
    // Allow creation on first repair run so CLI can bootstrap the DB without a daemon race.
    vdbConfig.create_if_missing = true;

    // Note: We no longer hold a long-lived exclusive DB lock across the entire repair.
    // Writes are serialized per-batch below; reads and compute proceed concurrently.

    auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
    if (!vectorDb->initialize()) {
        return Error{ErrorCode::DatabaseError,
                     "Vector database initialization failed: " + vectorDb->getLastError()};
    }

    // Guard: if an existing DB has a fixed dimension that does not match the embedding generator,
    // abort early with a clear diagnostic instead of failing during batch insert.
    try {
        size_t existing = vectorDb->getConfig().embedding_dim;
        size_t genDim = embeddingGenerator->getEmbeddingDimension();
        if (existing > 0 && genDim > 0 && existing != genDim) {
            std::string msg = "Embedding dimension mismatch: vector DB expects " +
                              std::to_string(existing) + ", but generator produces " +
                              std::to_string(genDim) +
                              ". Install/select a model with dim=" + std::to_string(existing) +
                              " (e.g., all-MiniLM-L6-v2 for 384) or recreate the vector DB.";
            spdlog::error("[repair] {}", msg);
            return Error{ErrorCode::InvalidState, msg};
        }
    } catch (...) {
    }

    // Get documents to process
    std::vector<metadata::DocumentInfo> documents;

    if (documentHashes.empty()) {
        // Process all documents
        auto allDocs = metadataRepo->findDocumentsByPath("%");
        if (!allDocs) {
            return Error{allDocs.error()};
        }
        documents = std::move(allDocs.value());
    } else {
        // Process specific documents
        for (const auto& hash : documentHashes) {
            auto docResult = metadataRepo->getDocumentByHash(hash);
            if (docResult && docResult.value()) {
                documents.push_back(*docResult.value());
            }
        }
    }

    if (documents.empty()) {
        return stats; // Nothing to process
    }

    spdlog::info("Processing {} documents for embedding repair", documents.size());

    // Process documents in batches
    for (size_t i = 0; i < documents.size(); i += config.batchSize) {
        size_t end = std::min(i + config.batchSize, documents.size());
        std::vector<std::string> texts;
        std::vector<metadata::DocumentInfo> batchDocs;

        // Collect texts for this batch (extract text; avoid raw bytes)
        for (size_t j = i; j < end; ++j) {
            const auto& doc = documents[j];
            stats.documentsProcessed++;

            // Check if embedding already exists
            if (config.skipExisting && vectorDb->hasEmbedding(doc.sha256Hash)) {
                stats.embeddingsSkipped++;
                continue;
            }

            // Extract text using util (plugins + built-ins)
            std::string ext = doc.fileExtension;
            if (!ext.empty() && ext[0] == '.')
                ext.erase(0, 1);
            auto extractedOpt = yams::extraction::util::extractDocumentText(
                contentStore, doc.sha256Hash, doc.mimeType, ext, extractors);
            if (!extractedOpt || extractedOpt->empty()) {
                stats.failedOperations++;
                continue;
            }

            std::string text = std::move(*extractedOpt);
            if (doc.id > 0) {
                metadata::DocumentContent contentRow;
                contentRow.documentId = doc.id;
                contentRow.contentText = text;
                contentRow.contentLength = static_cast<int64_t>(contentRow.contentText.size());
                contentRow.extractionMethod = "repair";
                double langConfidence = 0.0;
                contentRow.language = yams::extraction::LanguageDetector::detectLanguage(
                    contentRow.contentText, &langConfidence);
                auto contentUpsert = metadataRepo->insertContent(contentRow);
                if (!contentUpsert) {
                    spdlog::warn("[repair] Failed to upsert content for {}: {}", doc.sha256Hash,
                                 contentUpsert.error().message);
                } else {
                    auto docRow = metadataRepo->getDocument(doc.id);
                    if (docRow && docRow.value().has_value()) {
                        auto updated = docRow.value().value();
                        updated.contentExtracted = true;
                        updated.extractionStatus = metadata::ExtractionStatus::Success;
                        (void)metadataRepo->updateDocument(updated);
                    }
                }
            }
            // Guard overly large text
            if (text.size() > 1000000) { // 1MB limit
                text.resize(1000000);
            }
            texts.push_back(std::move(text));
            batchDocs.push_back(doc);
        }

        if (!texts.empty()) {
            // Generate embeddings for batch
            auto embeddings = embeddingGenerator->generateEmbeddings(texts);
            if (embeddings.empty()) {
                stats.failedOperations += texts.size();
                spdlog::debug("Failed to generate embeddings for batch of {} texts", texts.size());
                continue;
            }

            // Store embeddings in vector database (acquire short-lived lock per batch)
            std::vector<vector::VectorRecord> records;
            records.reserve(embeddings.size());

            for (size_t k = 0; k < embeddings.size() && k < batchDocs.size(); ++k) {
                vector::VectorRecord record;
                record.document_hash = batchDocs[k].sha256Hash;
                record.chunk_id = vector::utils::generateChunkId(batchDocs[k].sha256Hash, 0);
                record.embedding = embeddings[k];
                record.content = texts[k].substr(0, 1000); // Store snippet
                record.metadata["name"] = batchDocs[k].fileName;
                record.metadata["mime_type"] = batchDocs[k].mimeType;
                record.metadata["path"] = batchDocs[k].filePath;
                records.push_back(std::move(record));
            }

            if (!records.empty()) {
                // Bounded wait/backoff for the DB lock
                const auto lockPath = config.dataPath / "vectors.db.lock";
                auto now = std::chrono::steady_clock::now();
                uint64_t timeout_ms = 10 * 60 * 1000ULL; // default 10 minutes
                if (const char* env_ms = std::getenv("YAMS_REPAIR_LOCK_TIMEOUT_MS")) {
                    try {
                        timeout_ms = std::stoull(std::string(env_ms));
                    } catch (...) {
                    }
                }
                const auto deadline = now + std::chrono::milliseconds(timeout_ms);
                uint64_t sleep_ms = 50;
                bool locked = false;
                while (!locked) {
                    VectorDbLock vlock(lockPath);
                    if (vlock.isLocked()) {
                        // Insert while holding the lock, then release at scope end
                        if (vectorDb->insertVectorsBatch(records)) {
                            locked = true; // success path exits loop
                            // vlock destructs here after insert
                        } else {
                            // Insert failed while holding the lock; treat as batch failure
                            stats.failedOperations += records.size();
                            spdlog::debug("Failed to insert {} embeddings into vector database",
                                          records.size());
                            break;
                        }
                    } else {
                        spdlog::debug("Another process is updating the vector database");
                        if (progressCallback) {
                            progressCallback(0, 0, "Waiting for vector DB lock...");
                        }
                        if (std::chrono::steady_clock::now() >= deadline) {
                            spdlog::warn("Vector database lock timeout; skipping current batch of "
                                         "{} records",
                                         records.size());
                            stats.failedOperations += records.size();
                            break;
                        }
                        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
                        sleep_ms = std::min<uint64_t>(sleep_ms * 2, 2000);
                    }
                }

                if (locked) {
                    stats.embeddingsGenerated += records.size();

                    // Update metadata repository to track embedding status
                    for (const auto& record : records) {
                        auto updateResult = metadataRepo->updateDocumentEmbeddingStatusByHash(
                            record.document_hash, true, embeddingGenerator->getConfig().model_name);
                        if (!updateResult) {
                            spdlog::warn("Failed to update embedding status for {}: {}",
                                         record.document_hash, updateResult.error().message);
                        }
                    }
                } else {
                    try {
                        spdlog::error("[vectordb] batch insert failed: {}",
                                      vectorDb->getLastError());
                    } catch (...) {
                    }
                }
            }
        }

        // Report progress
        if (progressCallback) {
            progressCallback(std::min(i + config.batchSize, documents.size()), documents.size(),
                             "Generated " + std::to_string(stats.embeddingsGenerated) +
                                 " embeddings");
        }
    }

    spdlog::info("Embedding repair complete: {} generated, {} skipped, {} failed",
                 stats.embeddingsGenerated, stats.embeddingsSkipped, stats.failedOperations);

    return stats;
}

bool hasEmbedding(const std::string& documentHash, const std::filesystem::path& dataPath) {
    try {
        vector::VectorDatabaseConfig vdbConfig;
        vdbConfig.database_path = (dataPath / "vectors.db").string();
        vdbConfig.embedding_dim = 384; // Default
                                       // actual value does not matter for existence check
        vdbConfig.create_if_missing = false;
        vdbConfig.create_if_missing = false; // Never create from util

        auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
        if (!vectorDb->initialize()) {
            return false;
        }

        return vectorDb->hasEmbedding(documentHash);
    } catch (...) {
        return false;
    }
}

Result<std::vector<std::string>>
getDocumentsMissingEmbeddings(std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
                              const std::filesystem::path& dataPath, size_t limit) {
    std::vector<std::string> missingEmbeddings;

    // Get all documents
    auto allDocs = metadataRepo->findDocumentsByPath("%");
    if (!allDocs) {
        return Error{allDocs.error()};
    }

    // Initialize vector database for checking
    vector::VectorDatabaseConfig vdbConfig;
    vdbConfig.database_path = (dataPath / "vectors.db").string();
    vdbConfig.embedding_dim = 384; // Default
    vdbConfig.create_if_missing = false;

    auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
    if (!vectorDb->initialize()) {
        return Error{ErrorCode::DatabaseError, "Failed to initialize vector database"};
    }

    size_t count = 0;
    for (const auto& doc : allDocs.value()) {
        if (!vectorDb->hasEmbedding(doc.sha256Hash)) {
            missingEmbeddings.push_back(doc.sha256Hash);
            count++;
            if (limit > 0 && count >= limit) {
                break;
            }
        }
    }

    return missingEmbeddings;
}

} // namespace yams::repair
