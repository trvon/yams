#include <spdlog/spdlog.h>
#include <chrono>
#include <filesystem>
#include <thread>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/embedding_service.h>
#include <yams/vector/vector_database.h>

namespace yams::vector {

namespace fs = std::filesystem;

// Static member definition - simple flag to prevent multiple repair threads
std::atomic<bool> EmbeddingService::repairInProgress_(false);

std::unique_ptr<EmbeddingService> EmbeddingService::create(cli::YamsCLI* cli) {
    // We'll implement this properly once we can include the CLI header
    // For now, return nullptr and we'll call the constructor directly
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

void EmbeddingService::triggerRepairIfNeeded() {
    // Check if repair is already in progress
    bool expected = false;
    if (!repairInProgress_.compare_exchange_strong(expected, true)) {
        spdlog::debug("Repair thread already in progress, skipping");
        return;
    }

    // Check if we have models available
    if (!isAvailable()) {
        spdlog::debug("No embedding models available, skipping repair");
        repairInProgress_ = false;
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
            repairInProgress_ = false;
            return;
        }

        // Just check if we have any documents without embeddings (sample check)
        auto docsResult = metadataRepo_->findDocumentsByPath("%");
        if (!docsResult || docsResult.value().empty()) {
            spdlog::debug("No documents to process");
            repairInProgress_ = false;
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
            repairInProgress_ = false;
            return;
        }

        spdlog::info("Detected ~{} documents missing embeddings (sampled {} of {})", missingCount,
                     checkedCount, totalDocs);

        spdlog::info("Starting background repair thread for missing embeddings");

        // Start detached repair thread
        std::thread(runRepair, store_, metadataRepo_, dataPath_).detach();

    } catch (const std::exception& e) {
        spdlog::debug("Failed to check embedding health: {}", e.what());
        repairInProgress_ = false;
    }
}

void EmbeddingService::runRepair(std::shared_ptr<api::IContentStore> store,
                                 std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
                                 std::filesystem::path dataPath) {
    spdlog::debug("Repair thread started");

    try {
        // Create service instance for repair
        EmbeddingService service(store, metadataRepo, dataPath);

        // Get documents without embeddings
        vector::VectorDatabaseConfig vdbConfig;
        vdbConfig.database_path = (dataPath / "vectors.db").string();
        vdbConfig.embedding_dim = 384;

        auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
        if (!vectorDb->initialize()) {
            spdlog::error("Repair thread: Failed to initialize vector database");
            repairInProgress_ = false;
            return;
        }

        auto docsResult = metadataRepo->findDocumentsByPath("%");
        if (!docsResult) {
            spdlog::error("Repair thread: Failed to get documents");
            repairInProgress_ = false;
            return;
        }

        std::vector<std::string> missingEmbeddings;
        for (const auto& doc : docsResult.value()) {
            if (!vectorDb->hasEmbedding(doc.sha256Hash)) {
                missingEmbeddings.push_back(doc.sha256Hash);
            }
        }

        if (missingEmbeddings.empty()) {
            spdlog::info("Repair thread: All embeddings already generated");
            repairInProgress_ = false;
            return;
        }

        spdlog::info("Repair thread: Processing {} documents with missing embeddings",
                     missingEmbeddings.size());

        // Process in batches
        const size_t batchSize = 32;
        for (size_t i = 0; i < missingEmbeddings.size(); i += batchSize) {
            size_t end = std::min(i + batchSize, missingEmbeddings.size());
            std::vector<std::string> batch(missingEmbeddings.begin() + i,
                                           missingEmbeddings.begin() + end);

            auto result = service.generateEmbeddingsInternal(batch, false);
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

    repairInProgress_ = false;
    spdlog::debug("Repair thread stopped");
}

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
        const size_t batchSize = 32;
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

                // Store embeddings in vector database (using exact repair command pattern)
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

                    if (vectorDb->insertVector(record)) {
                        processed++;
                    } else {
                        failed++;
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