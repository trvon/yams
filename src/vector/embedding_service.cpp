#include <spdlog/spdlog.h>
#include <chrono>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <thread>
#include <unistd.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/embedding_service.h>
#include <yams/vector/vector_database.h>

namespace yams::vector {

namespace fs = std::filesystem;

std::unique_ptr<EmbeddingService> EmbeddingService::create(cli::YamsCLI* cli) {
    // Forward declaration limitation - implementation moved to YamsCLI
    // This method is now implemented in yams_cli.cpp to avoid circular dependencies
    return nullptr;
}

EmbeddingService::EmbeddingService(std::shared_ptr<api::IContentStore> store,
                                   std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
                                   std::filesystem::path dataPath)
    : store_(std::move(store)), metadataRepo_(std::move(metadataRepo)),
      dataPath_(std::move(dataPath)) {}

bool EmbeddingService::isAvailable() const {
    return !getAvailableModels().empty();
}

std::vector<std::string> EmbeddingService::getAvailableModels() const {
    std::vector<std::string> availableModels;

    const char* home = std::getenv("HOME");
    if (!home) {
        return availableModels;
    }

    fs::path modelsPath = fs::path(home) / ".yams" / "models";
    if (!fs::exists(modelsPath)) {
        return availableModels;
    }

    for (const auto& entry : fs::directory_iterator(modelsPath)) {
        if (entry.is_directory()) {
            fs::path modelFile = entry.path() / "model.onnx";
            if (fs::exists(modelFile)) {
                availableModels.push_back(entry.path().filename().string());
            }
        }
    }

    return availableModels;
}

Result<void> EmbeddingService::generateEmbeddingForDocument(const std::string& documentHash) {
    return generateEmbeddingsForDocuments({documentHash});
}

bool EmbeddingService::startRepairAsync() {
    std::lock_guard<std::mutex> lock(workerMutex_);
    if (repairRunning_) {
        return false;
    }
    repairRunning_ = true;

#ifdef YAMS_HAVE_JTHREAD
    // Launch stop-aware background worker with std::jthread
    repairThread_ = std::jthread([this](std::stop_token stoken) {
        // Ensure running flag is reset when the thread exits
        struct Reset {
            bool& flag;
            std::mutex& mtx;
            ~Reset() {
                std::lock_guard<std::mutex> l(mtx);
                flag = false;
            }
        } reset{repairRunning_, workerMutex_};

        this->runRepair(stoken);
    });
#else
    stopRequested_.store(false);
    repairThread_ = std::thread([this]() {
        struct Reset {
            bool& flag;
            std::mutex& mtx;
            ~Reset() {
                std::lock_guard<std::mutex> l(mtx);
                flag = false;
            }
        } reset{repairRunning_, workerMutex_};
        this->runRepairLegacy();
    });
#endif

    return true;
}

void EmbeddingService::stopRepair() {
#ifdef YAMS_HAVE_JTHREAD
    std::jthread local;
    {
        std::lock_guard<std::mutex> lock(workerMutex_);
        if (!repairThread_.joinable()) {
            return;
        }
        local = std::move(repairThread_);
    }
    // Request cooperative stop; jthread joins on destruction
    local.request_stop();
#else
    std::thread local;
    {
        std::lock_guard<std::mutex> lock(workerMutex_);
        if (!repairThread_.joinable()) {
            return;
        }
        stopRequested_.store(true);
        local = std::move(repairThread_);
    }
    if (local.joinable())
        local.join();
#endif
}

bool EmbeddingService::isRepairRunning() const {
    std::lock_guard<std::mutex> lock(workerMutex_);
    return repairRunning_;
}

void EmbeddingService::triggerRepairIfNeeded() {
    // Check if we have models available
    if (!isAvailable()) {
        spdlog::debug("No embedding models available, skipping repair");
        return;
    }

    // Quick check if there are missing embeddings (don't do full scan here)
    try {
        vector::VectorDatabaseConfig vdbConfig;
        vdbConfig.database_path = (dataPath_ / "vectors.db").string();
        vdbConfig.embedding_dim = 384; // Will be adjusted based on model

        auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
        if (!vectorDb->initialize()) {
            spdlog::debug("Failed to initialize vector database for health check");
            return;
        }

        // Just check if we have any documents without embeddings (sample check)
        auto docsResult = metadataRepo_->findDocumentsByPath("%");
        if (!docsResult || docsResult.value().empty()) {
            spdlog::debug("No documents to process");
            return;
        }

        // Check a better sample of documents for missing embeddings
        size_t totalDocs = docsResult.value().size();
        size_t missingCount = 0;
        size_t checkedCount = 0;

        // For efficiency, check up to 50 documents spread across the collection
        size_t checkLimit = std::min(size_t(50), totalDocs);
        size_t step = std::max(size_t(1), totalDocs / checkLimit);

        for (size_t i = 0; i < totalDocs && checkedCount < checkLimit; i += step) {
            checkedCount++;
            if (!vectorDb->hasEmbedding(docsResult.value()[i].sha256Hash)) {
                missingCount++;
            }
        }

        // Extrapolate missing count if we only checked a sample
        if (checkedCount < totalDocs && missingCount > 0) {
            float missingRate = static_cast<float>(missingCount) / checkedCount;
            missingCount = static_cast<size_t>(missingRate * totalDocs);
        }

        if (missingCount == 0) {
            spdlog::debug("Health check found no missing embeddings (sampled {} of {} docs)",
                          checkedCount, totalDocs);
            return;
        }

        spdlog::info("Detected ~{} documents missing embeddings (sampled {} of {})", missingCount,
                     checkedCount, totalDocs);

        if (startRepairAsync()) {
            spdlog::info("Repair worker started");
        } else {
            spdlog::debug("Repair worker already running; skip.");
        }

    } catch (const std::exception& e) {
        spdlog::debug("Failed to check embedding health: {}", e.what());
    }
}

#ifdef YAMS_HAVE_JTHREAD
void EmbeddingService::runRepair(std::stop_token stopToken) {
    // Cross-process single-writer lock using a lockfile on vectors.db
    fs::path lockPath = dataPath_ / "vectors.db.lock";
    struct FileLock {
        int fd{-1};
        fs::path path;
        explicit FileLock(const fs::path& p) : path(p) {
            fd = ::open(path.c_str(), O_CREAT | O_RDWR, 0644);
            if (fd >= 0) {
                struct flock fl{};
                fl.l_type = F_WRLCK;
                fl.l_whence = SEEK_SET;
                fl.l_start = 0;
                fl.l_len = 0; // whole file
                if (fcntl(fd, F_SETLK, &fl) == -1) {
                    ::close(fd);
                    fd = -1;
                }
            }
        }
        bool locked() const { return fd >= 0; }
        ~FileLock() {
            if (fd >= 0) {
                struct flock fl{};
                fl.l_type = F_UNLCK;
                fl.l_whence = SEEK_SET;
                fl.l_start = 0;
                fl.l_len = 0;
                (void)fcntl(fd, F_SETLK, &fl);
                ::close(fd);
            }
        }
    } vlock(lockPath);

    if (!vlock.locked()) {
        spdlog::info("Repair thread: another process holds vector DB lock; skipping repair");
        return;
    }

    spdlog::debug("Repair thread started");

    try {
        // Initialize vector DB
        vector::VectorDatabaseConfig vdbConfig;
        vdbConfig.database_path = (dataPath_ / "vectors.db").string();
        vdbConfig.embedding_dim = 384;

        auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
        if (!vectorDb->initialize()) {
            spdlog::error("Repair thread: Failed to initialize vector database");
            return;
        }

        if (stopToken.stop_requested()) {
            spdlog::debug("Repair thread: stop requested before scan");
            return;
        }

        auto docsResult = metadataRepo_->findDocumentsByPath("%");
        if (!docsResult) {
            spdlog::error("Repair thread: Failed to get documents");
            return;
        }

        std::vector<std::string> missingEmbeddings;
        missingEmbeddings.reserve(docsResult.value().size());
        for (const auto& doc : docsResult.value()) {
            if (stopToken.stop_requested()) {
                spdlog::debug("Repair thread: stop requested during scan");
                return;
            }
            if (!vectorDb->hasEmbedding(doc.sha256Hash)) {
                missingEmbeddings.push_back(doc.sha256Hash);
            }
        }

        if (missingEmbeddings.empty()) {
            spdlog::info("Repair thread: All embeddings already generated");
            return;
        }

        spdlog::info("Repair thread: Processing {} documents with missing embeddings",
                     missingEmbeddings.size());

        // Process in batches
        size_t batchSize = 32; // safe default
        if (const char* envBatch = std::getenv("YAMS_EMBED_BATCH")) {
            try {
                unsigned long v = std::stoul(std::string(envBatch));
                // Clamp to a reasonable range to avoid OOM or tiny batches
                if (v < 4UL)
                    v = 4UL;
                if (v > 128UL)
                    v = 128UL;
                batchSize = static_cast<size_t>(v);
            } catch (...) {
                spdlog::warn("Invalid YAMS_EMBED_BATCH='{}', using default {}", envBatch,
                             batchSize);
            }
        }
        spdlog::info("EmbeddingService: using batch size {}", batchSize);
        for (size_t i = 0; i < missingEmbeddings.size(); i += batchSize) {
            if (stopToken.stop_requested()) {
                spdlog::debug("Repair thread: stop requested during processing");
                break;
            }

            size_t end = std::min(i + batchSize, missingEmbeddings.size());
            std::vector<std::string> batch(missingEmbeddings.begin() + i,
                                           missingEmbeddings.begin() + end);

            auto result = generateEmbeddingsInternal(batch, false);
            if (!result) {
                spdlog::debug("Repair thread: Batch processing failed: {}", result.error().message);
            }

            // Small delay between batches to avoid hogging resources
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }

        spdlog::info("Repair thread: Completed processing");

    } catch (const std::exception& e) {
        spdlog::error("Repair thread exception: {}", e.what());
    }

    spdlog::debug("Repair thread stopped");
}
#else
void EmbeddingService::runRepairLegacy() {
    // Cross-process single-writer lock using a lockfile on vectors.db
    fs::path lockPath = dataPath_ / "vectors.db.lock";
    struct FileLock {
        int fd{-1};
        fs::path path;
        explicit FileLock(const fs::path& p) : path(p) {
            fd = ::open(path.c_str(), O_CREAT | O_RDWR, 0644);
            if (fd >= 0) {
                struct flock fl{};
                fl.l_type = F_WRLCK;
                fl.l_whence = SEEK_SET;
                fl.l_start = 0;
                fl.l_len = 0; // whole file
                if (fcntl(fd, F_SETLK, &fl) == -1) {
                    ::close(fd);
                    fd = -1;
                }
            }
        }
        bool locked() const { return fd >= 0; }
        ~FileLock() {
            if (fd >= 0) {
                struct flock fl{};
                fl.l_type = F_UNLCK;
                fl.l_whence = SEEK_SET;
                fl.l_start = 0;
                fl.l_len = 0;
                (void)fcntl(fd, F_SETLK, &fl);
                ::close(fd);
            }
        }
    } vlock(lockPath);

    if (!vlock.locked()) {
        spdlog::info("Repair thread: another process holds vector DB lock; skipping repair");
        return;
    }

    spdlog::debug("Repair thread started");

    try {
        // Initialize vector DB
        vector::VectorDatabaseConfig vdbConfig;
        vdbConfig.database_path = (dataPath_ / "vectors.db").string();
        vdbConfig.embedding_dim = 384;

        auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
        if (!vectorDb->initialize()) {
            spdlog::error("Repair thread: Failed to initialize vector database");
            return;
        }

        if (stopRequested_.load()) {
            spdlog::debug("Repair thread: stop requested before scan");
            return;
        }

        auto docsResult = metadataRepo_->findDocumentsByPath("%");
        if (!docsResult) {
            spdlog::error("Repair thread: Failed to get documents");
            return;
        }

        std::vector<std::string> missingEmbeddings;
        missingEmbeddings.reserve(docsResult.value().size());
        for (const auto& doc : docsResult.value()) {
            if (stopRequested_.load()) {
                spdlog::debug("Repair thread: stop requested during scan");
                return;
            }
            if (!vectorDb->hasEmbedding(doc.sha256Hash)) {
                missingEmbeddings.push_back(doc.sha256Hash);
            }
        }

        if (missingEmbeddings.empty()) {
            spdlog::info("Repair thread: All embeddings already generated");
            return;
        }

        spdlog::info("Repair thread: Processing {} documents with missing embeddings",
                     missingEmbeddings.size());

        // Process in batches
        size_t batchSize = 32; // safe default
        if (const char* envBatch = std::getenv("YAMS_EMBED_BATCH")) {
            try {
                unsigned long v = std::stoul(std::string(envBatch));
                // Clamp to a reasonable range to avoid OOM or tiny batches
                if (v < 4UL)
                    v = 4UL;
                if (v > 128UL)
                    v = 128UL;
                batchSize = static_cast<size_t>(v);
            } catch (...) {
                spdlog::warn("Invalid YAMS_EMBED_BATCH='{}', using default {}", envBatch,
                             batchSize);
            }
        }
        spdlog::info("EmbeddingService: using batch size {}", batchSize);
        for (size_t i = 0; i < missingEmbeddings.size(); i += batchSize) {
            if (stopRequested_.load()) {
                spdlog::debug("Repair thread: stop requested during processing");
                break;
            }

            size_t end = std::min(i + batchSize, missingEmbeddings.size());
            std::vector<std::string> batch(missingEmbeddings.begin() + i,
                                           missingEmbeddings.begin() + end);

            auto result = generateEmbeddingsInternal(batch, false);
            if (!result) {
                spdlog::debug("Repair thread: Batch processing failed: {}", result.error().message);
            }

            // Small delay between batches to avoid hogging resources
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }

        spdlog::info("Repair thread: Completed processing");

    } catch (const std::exception& e) {
        spdlog::error("Repair thread exception: {}", e.what());
    }

    spdlog::debug("Repair thread stopped");
}
#endif

Result<void>
EmbeddingService::generateEmbeddingsForDocuments(const std::vector<std::string>& documentHashes) {
    return generateEmbeddingsInternal(documentHashes, false);
}

Result<void>
EmbeddingService::generateEmbeddingsInternal(const std::vector<std::string>& documentHashes,
                                             bool showProgress) {
    // Check for available models (extracted from repair command logic)
    auto availableModels = getAvailableModels();
    if (availableModels.empty()) {
        return Error{ErrorCode::NotFound, "No embedding models available"};
    }

    try {
        // 1. Configure embedding generation (prefer all-MiniLM-L6-v2 for efficiency)
        std::string selectedModel = availableModels[0];
        for (const auto& model : availableModels) {
            if (model == "all-MiniLM-L6-v2") {
                selectedModel = model;
                break;
            }
        }

        const char* home = std::getenv("HOME");
        fs::path modelsPath = fs::path(home ? home : "") / ".yams" / "models";

        vector::EmbeddingConfig embConfig;
        embConfig.model_path = (modelsPath / selectedModel / "model.onnx").string();
        embConfig.model_name = selectedModel;

        // Set dimensions based on model
        if (selectedModel == "all-MiniLM-L6-v2") {
            embConfig.embedding_dim = 384;
        } else if (selectedModel == "all-mpnet-base-v2") {
            embConfig.embedding_dim = 768;
        } else {
            embConfig.embedding_dim = 384; // Default fallback
        }

        // 2. Initialize embedding generator
        auto embGenerator = std::make_unique<vector::EmbeddingGenerator>(embConfig);
        if (!embGenerator->initialize()) {
            return Error{ErrorCode::InternalError, "Failed to initialize embedding generator"};
        }

        // 3. Initialize vector database
        vector::VectorDatabaseConfig vdbConfig;
        vdbConfig.database_path = (dataPath_ / "vectors.db").string();
        vdbConfig.embedding_dim = embConfig.embedding_dim;

        auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
        if (!vectorDb->initialize()) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to initialize vector database: " + vectorDb->getLastError()};
        }

        // 4. Process documents in batches (extracted logic from repair command)
        size_t batchSize = 32; // safe default
        if (const char* envBatch = std::getenv("YAMS_EMBED_BATCH")) {
            try {
                unsigned long v = std::stoul(std::string(envBatch));
                // Clamp to a reasonable range to avoid OOM or tiny batches
                if (v < 4UL)
                    v = 4UL;
                if (v > 128UL)
                    v = 128UL;
                batchSize = static_cast<size_t>(v);
            } catch (...) {
                spdlog::warn("Invalid YAMS_EMBED_BATCH='{}', using default {}", envBatch,
                             batchSize);
            }
        }
        spdlog::info("EmbeddingService: using batch size {}", batchSize);
        size_t processed = 0;
        size_t skipped = 0;
        size_t failed = 0;

        for (size_t i = 0; i < documentHashes.size(); i += batchSize) {
            size_t end = std::min(i + batchSize, documentHashes.size());
            std::vector<std::string> texts;
            std::vector<std::string> hashes;

            // Collect texts for this batch
            for (size_t j = i; j < end; ++j) {
                const auto& docHash = documentHashes[j];

                // Check if embedding already exists
                if (vectorDb->hasEmbedding(docHash)) {
                    skipped++;
                    continue;
                }

                // Get document content
                auto content = store_->retrieveBytes(docHash);
                if (!content) {
                    failed++;
                    continue;
                }

                std::string text(reinterpret_cast<const char*>(content.value().data()),
                                 content.value().size());
                texts.push_back(text);
                hashes.push_back(docHash);
            }

            if (!texts.empty()) {
                // Generate embeddings for batch
                auto embeddings = embGenerator->generateEmbeddings(texts);
                if (embeddings.empty()) {
                    failed += texts.size();
                    continue;
                }

                // Store embeddings in vector database using batch insert for fewer transactions
                std::vector<vector::VectorRecord> records;
                records.reserve(std::min(embeddings.size(), hashes.size()));
                for (size_t k = 0; k < embeddings.size() && k < hashes.size(); ++k) {
                    // Get document metadata for record
                    auto docResult = metadataRepo_->getDocumentByHash(hashes[k]);

                    vector::VectorRecord record;
                    record.document_hash = hashes[k];
                    record.chunk_id = vector::utils::generateChunkId(hashes[k], 0);
                    record.embedding = embeddings[k];
                    record.content = texts[k].substr(0, 1000); // Store snippet

                    // Add metadata if document exists
                    if (docResult && docResult.value()) {
                        const auto& doc = docResult.value().value();
                        record.metadata["name"] = doc.fileName;
                        record.metadata["mime_type"] = doc.mimeType;
                    }
                    records.push_back(std::move(record));
                }

                if (!records.empty()) {
                    if (vectorDb->insertVectorsBatch(records)) {
                        processed += records.size();
                    } else {
                        failed += records.size();
                    }
                }
            }
        }

        if (showProgress && processed > 0) {
            spdlog::info("Embedding generation completed: {} processed, {} skipped, {} failed",
                         processed, skipped, failed);
        }

        return Result<void>();

    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
                     std::string("Embedding generation failed: ") + e.what()};
    }
}

} // namespace yams::vector
