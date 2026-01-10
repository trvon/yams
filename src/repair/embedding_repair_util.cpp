#include <yams/daemon/resource/model_provider.h>
#include <yams/extraction/extraction_util.h>
#include <yams/extraction/text_extractor.h>
#include <yams/ingest/ingest_helpers.h>
#include <yams/metadata/query_helpers.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/vector/document_chunker.h>
#include <yams/vector/vector_database.h>

#include <spdlog/spdlog.h>
#include <atomic>
#include <ctime>
#include <fcntl.h>
#include <filesystem>
#include <thread>
#ifdef _WIN32
#include <io.h>
#include <windows.h>
#include <sys/stat.h>
#else
#include <unistd.h>
#endif

namespace yams::repair {

namespace {
// File lock for cross-process safety
class VectorDbLock {
public:
    explicit VectorDbLock(const std::filesystem::path& lockPath) : path_(lockPath), fd_(-1) {
#ifdef _WIN32
        int err = _sopen_s(&fd_, path_.string().c_str(), _O_RDWR | _O_CREAT | _O_BINARY, _SH_DENYNO,
                           _S_IREAD | _S_IWRITE);
        if (err == 0 && fd_ >= 0) {
            HANDLE hFile = (HANDLE)_get_osfhandle(fd_);
            if (hFile != INVALID_HANDLE_VALUE) {
                OVERLAPPED overlapped = {0};
                // Lock the first byte exclusively
                if (!LockFileEx(hFile, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0,
                                &overlapped)) {
                    _close(fd_);
                    fd_ = -1;
                } else {
                    // Stamp PID and timestamp for diagnostics
                    try {
                        _chsize_s(fd_, 0);
                        std::string stamp = std::to_string(_getpid()) + " " +
                                            std::to_string(std::time(nullptr)) + "\n";
                        _write(fd_, stamp.data(), static_cast<unsigned int>(stamp.size()));
                        _lseek(fd_, 0, SEEK_SET);
                    } catch (...) {
                    }
                }
            } else {
                _close(fd_);
                fd_ = -1;
            }
        } else {
            fd_ = -1;
        }
#else
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
#endif
    }

    bool isLocked() const { return fd_ >= 0; }

    ~VectorDbLock() {
        if (fd_ >= 0) {
#ifdef _WIN32
            HANDLE hFile = (HANDLE)_get_osfhandle(fd_);
            if (hFile != INVALID_HANDLE_VALUE) {
                OVERLAPPED overlapped = {0};
                UnlockFileEx(hFile, 0, 1, 0, &overlapped);
            }
            _close(fd_);
#else
            struct flock fl{};
            fl.l_type = F_UNLCK;
            fl.l_whence = SEEK_SET;
            fl.l_start = 0;
            fl.l_len = 0;
            (void)fcntl(fd_, F_SETLK, &fl);
            ::close(fd_);
#endif
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
                        std::shared_ptr<daemon::IModelProvider> modelProvider,
                        const std::string& modelName, const EmbeddingRepairConfig& config,
                        const std::vector<std::string>& documentHashes,
                        EmbeddingRepairProgressCallback progressCallback,
                        const yams::extraction::ContentExtractorList& extractors) {
    EmbeddingRepairStats stats;

    if (!contentStore || !metadataRepo || !modelProvider) {
        return Error{ErrorCode::InvalidArgument, "Missing required components"};
    }

    if (modelName.empty()) {
        return Error{ErrorCode::InvalidArgument, "Model name is required"};
    }

    if (!modelProvider->isAvailable()) {
        return Error{ErrorCode::InternalError, "Model provider not available"};
    }

    // Get embedding dimension from model provider
    size_t embeddingDim = modelProvider->getEmbeddingDim(modelName);
    if (embeddingDim == 0) {
        return Error{ErrorCode::InternalError,
                     "Could not determine embedding dimension for model: " + modelName};
    }

    // Initialize vector database. If missing, create with the model's dimension.
    vector::VectorDatabaseConfig vdbConfig;
    vdbConfig.database_path = (config.dataPath / "vectors.db").string();
    vdbConfig.embedding_dim = embeddingDim;
    // Allow creation on first repair run so CLI can bootstrap the DB without a daemon race.
    vdbConfig.create_if_missing = true;

    // Note: We no longer hold a long-lived exclusive DB lock across the entire repair.
    // Writes are serialized per-batch below; reads and compute proceed concurrently.

    auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
    if (!vectorDb->initialize()) {
        return Error{ErrorCode::DatabaseError,
                     "Vector database initialization failed: " + vectorDb->getLastError()};
    }

    // Guard: if an existing DB has a fixed dimension that does not match the model provider,
    // abort early with a clear diagnostic instead of failing during batch insert.
    try {
        size_t existing = vectorDb->getConfig().embedding_dim;
        if (existing > 0 && embeddingDim > 0 && existing != embeddingDim) {
            std::string msg = "Embedding dimension mismatch: vector DB expects " +
                              std::to_string(existing) + ", but model '" + modelName +
                              "' produces " + std::to_string(embeddingDim) +
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
        auto allDocs = metadata::queryDocumentsByPattern(*metadataRepo, "%");
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
            // Insert per document with a bounded advisory lock; helper handles chunking/embeds.
            for (size_t k = 0; k < batchDocs.size() && k < texts.size(); ++k) {
                const auto& doc = batchDocs[k];
                const auto& text = texts[k];
                if (text.empty()) {
                    stats.failedOperations += 1;
                    continue;
                }

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
                bool done = false;
                while (!done) {
                    VectorDbLock vlock(lockPath);
                    if (vlock.isLocked()) {
                        yams::vector::ChunkingConfig ccfg{};
                        auto r = yams::ingest::embed_and_insert_document(
                            *modelProvider, modelName, *vectorDb, *metadataRepo, doc.sha256Hash,
                            text, doc.fileName, doc.filePath, doc.mimeType, ccfg);
                        if (r) {
                            stats.embeddingsGenerated += r.value();
                            done = true;
                        } else {
                            stats.failedOperations += 1;
                            spdlog::debug("[repair] embed/insert failed for {}: {}", doc.sha256Hash,
                                          r.error().message);
                            break;
                        }
                    } else {
                        if (progressCallback)
                            progressCallback(0, 0, "Waiting for vector DB lock...");
                        if (std::chrono::steady_clock::now() >= deadline) {
                            spdlog::warn("Vector DB lock timeout; skipping doc {}", doc.sha256Hash);
                            stats.failedOperations += 1;
                            break;
                        }
                        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
                        sleep_ms = std::min<uint64_t>(sleep_ms * 2, 2000);
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

// CLI overload - wraps the daemon version but extracts model info from EmbeddingGenerator
Result<EmbeddingRepairStats>
repairMissingEmbeddings(std::shared_ptr<api::IContentStore> contentStore,
                        std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
                        std::shared_ptr<vector::EmbeddingGenerator> embeddingGenerator,
                        const EmbeddingRepairConfig& config,
                        const std::vector<std::string>& documentHashes,
                        EmbeddingRepairProgressCallback progressCallback,
                        const yams::extraction::ContentExtractorList& extractors) {
    if (!embeddingGenerator) {
        return Error{ErrorCode::InvalidArgument, "EmbeddingGenerator is required"};
    }

    // For CLI usage, EmbeddingGenerator talks to the daemon via IPC.
    // We need to wrap it as an IModelProvider for the shared implementation.
    // Create a simple adapter that forwards to the generator.
    class GeneratorModelProvider : public daemon::IModelProvider {
        std::shared_ptr<vector::EmbeddingGenerator> gen_;

    public:
        explicit GeneratorModelProvider(std::shared_ptr<vector::EmbeddingGenerator> g)
            : gen_(std::move(g)) {}

        bool isAvailable() const override { return gen_ && gen_->isInitialized(); }

        std::string getProviderName() const override { return "cli-generator"; }

        // Single embedding
        Result<std::vector<float>> generateEmbedding(const std::string& text) override {
            try {
                auto result = gen_->generateEmbeddings({text});
                if (result.empty()) {
                    return Error{ErrorCode::InternalError, "No embedding generated"};
                }
                return result[0];
            } catch (const std::exception& e) {
                return Error{ErrorCode::InternalError,
                             std::string("Failed to generate embedding: ") + e.what()};
            }
        }

        // Batch embeddings
        Result<std::vector<std::vector<float>>>
        generateBatchEmbeddings(const std::vector<std::string>& texts) override {
            try {
                auto result = gen_->generateEmbeddings(texts);
                return result;
            } catch (const std::exception& e) {
                return Error{ErrorCode::InternalError,
                             std::string("Failed to generate embeddings: ") + e.what()};
            }
        }

        // Named model versions
        Result<std::vector<float>> generateEmbeddingFor(const std::string& /*modelName*/,
                                                        const std::string& text) override {
            return generateEmbedding(text);
        }

        Result<std::vector<std::vector<float>>>
        generateBatchEmbeddingsFor(const std::string& /*modelName*/,
                                   const std::vector<std::string>& texts) override {
            return generateBatchEmbeddings(texts);
        }

        size_t getEmbeddingDim(const std::string& /*modelName*/) const override {
            return gen_->getEmbeddingDimension();
        }

        Result<daemon::ModelInfo> getModelInfo(const std::string& modelName) const override {
            daemon::ModelInfo info;
            info.name = modelName;
            info.embeddingDim = gen_->getEmbeddingDimension();
            return info;
        }

        // Stubs for other required methods
        Result<void> loadModel(const std::string& /*modelName*/) override {
            return {}; // Already loaded via generator
        }

        Result<void> unloadModel(const std::string& /*modelName*/) override {
            return Error{ErrorCode::NotImplemented, "Unload not supported in CLI mode"};
        }

        bool isModelLoaded(const std::string& /*modelName*/) const override {
            return isAvailable();
        }

        std::vector<std::string> getLoadedModels() const override {
            return isAvailable() ? std::vector<std::string>{"default"} : std::vector<std::string>{};
        }

        size_t getLoadedModelCount() const override { return isAvailable() ? 1 : 0; }

        std::vector<std::string> getAvailableModels() const { return {"default"}; }

        Result<std::vector<daemon::ModelInfo>> listModels() const {
            daemon::ModelInfo info;
            info.name = "default";
            info.embeddingDim = gen_->getEmbeddingDimension();
            return std::vector<daemon::ModelInfo>{info};
        }

        std::string getProviderVersion() const override { return "cli-1.0"; }

        size_t getMemoryUsage() const override {
            return 0; // Unknown in CLI mode
        }

        void releaseUnusedResources() override {
            // No-op in CLI mode
        }

        void shutdown() override {
            // No-op in CLI mode - daemon handles lifecycle
        }

        std::shared_ptr<vector::EmbeddingGenerator>
        getEmbeddingGenerator(const std::string& /*modelName*/) override {
            return gen_; // Return the wrapped generator
        }
    };

    auto provider = std::make_shared<GeneratorModelProvider>(embeddingGenerator);

    // Use a placeholder model name since EmbeddingGenerator doesn't expose it
    std::string modelName = "default";
    if (const char* env = std::getenv("YAMS_PREFERRED_MODEL")) {
        modelName = env;
    }

    return repairMissingEmbeddings(contentStore, metadataRepo, provider, modelName, config,
                                   documentHashes, progressCallback, extractors);
}

bool hasEmbedding(const std::string& documentHash, const std::filesystem::path& dataPath) {
    try {
        vector::VectorDatabaseConfig vdbConfig;
        vdbConfig.database_path = (dataPath / "vectors.db").string();
        vdbConfig.embedding_dim = 0;         // Dimension not needed for existence check
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
    auto allDocs = metadata::queryDocumentsByPattern(*metadataRepo, "%");
    if (!allDocs) {
        return Error{allDocs.error()};
    }

    // Initialize vector database for checking
    vector::VectorDatabaseConfig vdbConfig;
    vdbConfig.database_path = (dataPath / "vectors.db").string();
    vdbConfig.embedding_dim = 0; // Dimension not needed for existence check
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
