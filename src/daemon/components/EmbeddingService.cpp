#include <yams/daemon/components/EmbeddingService.h>

#include <spdlog/spdlog.h>
#include <yams/api/content_store.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/ingest/ingest_helpers.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/document_chunker.h>
#include <yams/vector/vector_database.h>

namespace yams::daemon {

EmbeddingService::EmbeddingService(std::shared_ptr<api::IContentStore> store,
                                   std::shared_ptr<metadata::MetadataRepository> meta,
                                   std::size_t threads)
    : store_(std::move(store)), meta_(std::move(meta)) {
    if (threads == 0)
        threads = 2;
    workers_.reserve(threads);
}

EmbeddingService::~EmbeddingService() {
    shutdown();
}

Result<void> EmbeddingService::initialize() {
    // Get or create the embedding channel from InternalBus
    embedChannel_ = InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
        "embed_jobs", 2048);

    if (!embedChannel_) {
        return Error{ErrorCode::InvalidOperation,
                     "Failed to create embedding channel on InternalBus"};
    }

    // Start worker threads
    stop_.store(false);
    for (std::size_t i = 0; i < workers_.capacity(); ++i) {
        workers_.emplace_back([this] { workerLoop(); });
    }

    spdlog::info("EmbeddingService: initialized with {} workers", workers_.size());
    return Result<void>();
}

void EmbeddingService::shutdown() {
    if (stop_.exchange(true)) {
        return; // Already stopped
    }

    spdlog::info("EmbeddingService: shutting down...");

    // Join all worker threads
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();

    spdlog::info("EmbeddingService: shutdown complete (processed={}, failed={})", processed_.load(),
                 failed_.load());
}

void EmbeddingService::setProviders(
    std::function<std::shared_ptr<IModelProvider>()> providerGetter,
    std::function<std::string()> modelNameGetter,
    std::function<std::shared_ptr<yams::vector::VectorDatabase>()> dbGetter) {
    getModelProvider_ = std::move(providerGetter);
    getPreferredModel_ = std::move(modelNameGetter);
    getVectorDatabase_ = std::move(dbGetter);
}

std::size_t EmbeddingService::queuedJobs() const {
    // SpscQueue doesn't expose size(), so we can't query pending jobs
    // This is acceptable as it's just a metric
    return 0;
}

void EmbeddingService::workerLoop() {
    while (!stop_.load(std::memory_order_relaxed)) {
        InternalEventBus::EmbedJob job;

        // Try to pop a job from the channel (non-blocking with timeout)
        if (embedChannel_ && embedChannel_->try_pop(job)) {
            processEmbedJob(job);
        } else {
            // No jobs available, sleep briefly to avoid busy-waiting
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}

void EmbeddingService::processEmbedJob(const InternalEventBus::EmbedJob& job) {
    // Get providers
    std::shared_ptr<IModelProvider> provider;
    std::string modelName;
    std::shared_ptr<yams::vector::VectorDatabase> vdb;

    if (getModelProvider_)
        provider = getModelProvider_();
    if (getPreferredModel_)
        modelName = getPreferredModel_();
    if (getVectorDatabase_)
        vdb = getVectorDatabase_();

    // Use job's model name if specified, otherwise use preferred
    if (!job.modelName.empty()) {
        modelName = job.modelName;
    }

    if (!provider || modelName.empty() || !vdb) {
        spdlog::warn("EmbeddingService: providers unavailable for batch of {} documents "
                     "(provider={}, model='{}', vdb={})",
                     job.hashes.size(), provider ? "available" : "null", modelName,
                     vdb ? "available" : "null");
        failed_.fetch_add(job.hashes.size());
        return;
    }

    spdlog::debug("EmbeddingService: processing batch of {} documents with model '{}'",
                  job.hashes.size(), modelName);

    std::size_t succeeded = 0;
    std::size_t skipped = 0;

    for (const auto& hash : job.hashes) {
        try {
            // Check if embeddings already exist for this document
            if (job.skipExisting) {
                // Note: VectorDatabase doesn't have countChunks(), so we'll always generate
                // This is acceptable - duplicate detection happens in embed_and_insert_document
            }

            // Get document info
            auto docInfoRes = meta_->getDocumentByHash(hash);
            if (!docInfoRes || !docInfoRes.value().has_value()) {
                spdlog::warn("EmbeddingService: document not found: {}", hash);
                failed_.fetch_add(1);
                continue;
            }

            const auto& docInfo = *docInfoRes.value();
            int64_t docId = docInfo.id;

            // Get document content
            auto contentOpt = meta_->getContent(docId);
            if (!contentOpt || !contentOpt.value().has_value()) {
                spdlog::debug("EmbeddingService: no content for document {}", hash);
                failed_.fetch_add(1);
                continue;
            }

            const auto& text = contentOpt.value().value().contentText;
            if (text.empty()) {
                spdlog::debug("EmbeddingService: empty content for document {}", hash);
                skipped++;
                continue;
            }

            // Generate and insert embeddings
            yams::vector::ChunkingConfig ccfg{};
            spdlog::debug("EmbeddingService: generating embeddings for {} using model '{}'", hash,
                          modelName);

            auto r = yams::ingest::embed_and_insert_document(
                *provider, modelName, *vdb, *meta_, hash, text, docInfo.fileName, docInfo.filePath,
                docInfo.mimeType, ccfg);

            if (!r) {
                spdlog::warn("EmbeddingService: embed/insert failed for {}: {}", hash,
                             r.error().message);
                failed_.fetch_add(1);
            } else {
                spdlog::debug("EmbeddingService: successfully generated {} embeddings for {}",
                              r.value(), hash);
                succeeded++;
            }
        } catch (const std::exception& e) {
            spdlog::error("EmbeddingService: exception processing {}: {}", hash, e.what());
            failed_.fetch_add(1);
        }
    }

    processed_.fetch_add(succeeded);

    spdlog::debug("EmbeddingService: batch complete (succeeded={}, skipped={}, failed={})",
                  succeeded, skipped, job.hashes.size() - succeeded - skipped);
}

} // namespace yams::daemon
