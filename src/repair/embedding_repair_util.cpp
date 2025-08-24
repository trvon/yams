#include <yams/repair/embedding_repair_util.h>
#include <yams/vector/vector_database.h>

#include <spdlog/spdlog.h>
#include <atomic>
#include <fcntl.h>
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
                        EmbeddingRepairProgressCallback progressCallback) {
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

    // Initialize vector database
    vector::VectorDatabaseConfig vdbConfig;
    vdbConfig.database_path = (config.dataPath / "vectors.db").string();
    vdbConfig.embedding_dim = embeddingGenerator->getEmbeddingDimension();

    // Acquire lock for vector database access
    VectorDbLock vlock(config.dataPath / "vectors.db.lock");
    if (!vlock.isLocked()) {
        spdlog::debug("Another process is updating the vector database");
        return Error{ErrorCode::InvalidState, "Vector database is locked by another process"};
    }

    auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
    if (!vectorDb->initialize()) {
        return Error{ErrorCode::DatabaseError,
                     "Vector database initialization failed: " + vectorDb->getLastError()};
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

        // Collect texts for this batch
        for (size_t j = i; j < end; ++j) {
            const auto& doc = documents[j];
            stats.documentsProcessed++;

            // Check if embedding already exists
            if (config.skipExisting && vectorDb->hasEmbedding(doc.sha256Hash)) {
                stats.embeddingsSkipped++;
                continue;
            }

            // Get document content
            auto content = contentStore->retrieveBytes(doc.sha256Hash);
            if (!content) {
                stats.failedOperations++;
                spdlog::debug("Failed to retrieve content for {}: {}", doc.sha256Hash.substr(0, 12),
                              content.error().message);
                continue;
            }

            std::string text(reinterpret_cast<const char*>(content.value().data()),
                             content.value().size());

            // Skip empty or very large documents
            if (text.empty() || text.size() > 1000000) { // 1MB limit
                stats.failedOperations++;
                continue;
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

            // Store embeddings in vector database
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
                if (vectorDb->insertVectorsBatch(records)) {
                    stats.embeddingsGenerated += records.size();
                } else {
                    stats.failedOperations += records.size();
                    spdlog::debug("Failed to insert {} embeddings into vector database",
                                  records.size());
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
        vdbConfig.embedding_dim = 384; // Default, actual value doesn't matter for existence check

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