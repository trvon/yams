#include <yams/daemon/components/EmbeddingService.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <thread>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/api/content_store.h>
#include <yams/core/types.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/ingest/ingest_helpers.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/document_chunker.h>
#include <yams/vector/vector_database.h>

namespace yams {
namespace daemon {

EmbeddingService::EmbeddingService(std::shared_ptr<api::IContentStore> store,
                                   std::shared_ptr<metadata::MetadataRepository> meta,
                                   WorkCoordinator* coordinator)
    : store_(std::move(store)), meta_(std::move(meta)), coordinator_(coordinator),
      strand_(coordinator_->makeStrand()) {}

EmbeddingService::~EmbeddingService() {
    shutdown();
}

Result<void> EmbeddingService::initialize() {
    // Use configurable channel capacity from TuneAdvisor
    const std::size_t capacity = TuneAdvisor::embedChannelCapacity();
    embedChannel_ = InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
        "embed_jobs", capacity);

    if (!embedChannel_) {
        return Error{ErrorCode::InvalidOperation,
                     "Failed to create embedding channel on InternalBus"};
    }

    spdlog::info("EmbeddingService: initialized with channel capacity {}", capacity);
    return Result<void>();
}

void EmbeddingService::start() {
    stop_.store(false);
    boost::asio::co_spawn(strand_, channelPoller(), boost::asio::detached);
    spdlog::info("EmbeddingService: started parallel channel poller");
}

void EmbeddingService::shutdown() {
    if (stop_.exchange(true)) {
        return;
    }
    spdlog::info("EmbeddingService: shutting down (processed={}, failed={}, inFlight={})",
                 processed_.load(), failed_.load(), inFlight_.load());

    // Wait for in-flight jobs to complete with timeout
    constexpr int kMaxWaitMs = 5000;
    constexpr int kPollIntervalMs = 50;
    int waited = 0;
    while (inFlight_.load() > 0 && waited < kMaxWaitMs) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollIntervalMs));
        waited += kPollIntervalMs;
    }
    if (inFlight_.load() > 0) {
        spdlog::warn("EmbeddingService: shutdown timeout, {} jobs still in flight",
                     inFlight_.load());
    } else {
        spdlog::info("EmbeddingService: all in-flight jobs completed");
    }
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
    return embedChannel_ ? embedChannel_->size_approx() : 0;
}

std::size_t EmbeddingService::inFlightJobs() const {
    return inFlight_.load();
}

boost::asio::awaitable<void> EmbeddingService::channelPoller() {
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    auto idleDelay = std::chrono::milliseconds(1);
    constexpr auto kMaxIdleDelay = std::chrono::milliseconds(50);

    spdlog::info("[EmbeddingService] Parallel poller started");

    while (!stop_.load()) {
        bool didWork = false;
        InternalEventBus::EmbedJob job;

        // Dynamic concurrency limit from TuneAdvisor (scaled by TuningManager)
        const std::size_t maxConcurrent = TuneAdvisor::postEmbedConcurrent();

        // Dispatch jobs up to concurrency limit
        while (inFlight_.load() < maxConcurrent && embedChannel_ && embedChannel_->try_pop(job)) {
            didWork = true;
            inFlight_.fetch_add(1);
            InternalEventBus::instance().incEmbedConsumed();

            // Dispatch to work executor for parallel processing
            boost::asio::post(coordinator_->getExecutor(), [this, job = std::move(job)]() mutable {
                processEmbedJob(std::move(job));
                inFlight_.fetch_sub(1);
            });
        }

        if (didWork) {
            idleDelay = std::chrono::milliseconds(1);
            continue; // Check for more work immediately
        }

        // Idle - wait before polling again
        timer.expires_after(idleDelay);
        co_await timer.async_wait(boost::asio::use_awaitable);
        if (idleDelay < kMaxIdleDelay) {
            idleDelay *= 2;
        }
    }

    spdlog::info("[EmbeddingService] Parallel poller exited");
}

void EmbeddingService::processEmbedJob(InternalEventBus::EmbedJob job) {
    spdlog::debug("[EmbeddingService] processEmbedJob called with {} hashes", job.hashes.size());
    std::shared_ptr<IModelProvider> provider;
    std::string modelName;
    std::shared_ptr<yams::vector::VectorDatabase> vdb;

    if (getModelProvider_)
        provider = getModelProvider_();
    if (getPreferredModel_)
        modelName = getPreferredModel_();
    if (getVectorDatabase_)
        vdb = getVectorDatabase_();

    spdlog::debug("[EmbeddingService] Callbacks: provider={} model='{}' vdb={}",
                  provider ? "yes" : "no", modelName, vdb ? "yes" : "no");

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

    // ============================================================
    // Phase 1: Gather all document content and metadata
    // ============================================================
    struct DocData {
        std::string hash;
        std::string text;
        std::string fileName;
        std::string filePath;
        std::string mimeType;
    };
    std::vector<DocData> docsToEmbed;
    docsToEmbed.reserve(job.hashes.size());

    std::size_t skipped = 0;
    std::size_t failedGather = 0;

    for (const auto& hash : job.hashes) {
        try {
            auto docInfoRes = meta_->getDocumentByHash(hash);
            if (!docInfoRes || !docInfoRes.value().has_value()) {
                spdlog::warn("EmbeddingService: document not found: {}", hash);
                failedGather++;
                continue;
            }

            const auto& docInfo = *docInfoRes.value();

            if (job.skipExisting && vdb->hasEmbedding(hash)) {
                spdlog::debug("EmbeddingService: skipExisting=true, already embedded: {}", hash);
                skipped++;
                continue;
            }

            auto contentOpt = meta_->getContent(docInfo.id);
            if (!contentOpt || !contentOpt.value().has_value()) {
                spdlog::debug("EmbeddingService: no content for document {}", hash);
                failedGather++;
                continue;
            }

            const auto& text = contentOpt.value().value().contentText;
            if (text.empty()) {
                spdlog::debug("EmbeddingService: empty content for document {}", hash);
                skipped++;
                continue;
            }

            docsToEmbed.push_back(
                {hash, text, docInfo.fileName, docInfo.filePath, docInfo.mimeType});
        } catch (const std::exception& e) {
            spdlog::error("EmbeddingService: exception gathering {}: {}", hash, e.what());
            failedGather++;
        }
    }

    failed_.fetch_add(failedGather);

    if (docsToEmbed.empty()) {
        spdlog::debug("EmbeddingService: no documents to embed after gathering");
        return;
    }

    spdlog::debug("EmbeddingService: gathered {} documents for embedding", docsToEmbed.size());

    // ============================================================
    // Phase 2: Chunk all documents
    // ============================================================
    struct ChunkInfo {
        size_t docIdx;       // Index into docsToEmbed
        std::string chunkId; // Unique chunk ID
        std::string content; // Chunk text
        size_t startOffset;
        size_t endOffset;
    };
    std::vector<ChunkInfo> allChunks;
    std::vector<std::string> allTexts; // For batch embedding call

    yams::vector::ChunkingConfig ccfg{};
    auto chunker =
        yams::vector::createChunker(yams::vector::ChunkingStrategy::SENTENCE_BASED, ccfg, nullptr);

    for (size_t docIdx = 0; docIdx < docsToEmbed.size(); ++docIdx) {
        const auto& doc = docsToEmbed[docIdx];
        auto chunks = chunker->chunkDocument(doc.text, doc.hash);

        if (chunks.empty()) {
            // No chunks produced - use whole document as single chunk
            std::string chunkId = yams::vector::utils::generateChunkId(doc.hash, 0);
            allChunks.push_back({docIdx, chunkId, doc.text, 0, doc.text.size()});
            allTexts.push_back(doc.text);
        } else {
            for (size_t i = 0; i < chunks.size(); ++i) {
                auto& c = chunks[i];
                std::string chunkId = c.chunk_id.empty()
                                          ? yams::vector::utils::generateChunkId(doc.hash, i)
                                          : c.chunk_id;
                allChunks.push_back(
                    {docIdx, chunkId, std::move(c.content), c.start_offset, c.end_offset});
                allTexts.push_back(allChunks.back().content);
            }
        }
    }

    spdlog::debug("EmbeddingService: chunked {} documents into {} chunks", docsToEmbed.size(),
                  allChunks.size());

    // ============================================================
    // Phase 3: Single batch embedding call for ALL chunks
    // ============================================================
    auto embedResult = provider->generateBatchEmbeddingsFor(modelName, allTexts);
    if (!embedResult) {
        spdlog::error("EmbeddingService: batch embedding failed: {}", embedResult.error().message);
        failed_.fetch_add(docsToEmbed.size());
        for (const auto& doc : docsToEmbed) {
            (void)meta_->updateDocumentRepairStatus(doc.hash, metadata::RepairStatus::Failed);
        }
        return;
    }

    auto& embeddings = embedResult.value();
    if (embeddings.size() != allChunks.size()) {
        spdlog::error("EmbeddingService: embedding count mismatch ({} vs {})", embeddings.size(),
                      allChunks.size());
        failed_.fetch_add(docsToEmbed.size());
        for (const auto& doc : docsToEmbed) {
            (void)meta_->updateDocumentRepairStatus(doc.hash, metadata::RepairStatus::Failed);
        }
        return;
    }

    spdlog::debug("EmbeddingService: generated {} embeddings in single batch", embeddings.size());

    // ============================================================
    // Phase 4: Build VectorRecords and batch insert
    // ============================================================
    // Group chunks by document for document-level embedding computation
    std::unordered_map<size_t, std::vector<size_t>> docToChunkIndices;
    for (size_t i = 0; i < allChunks.size(); ++i) {
        docToChunkIndices[allChunks[i].docIdx].push_back(i);
    }

    std::vector<yams::vector::VectorRecord> allRecords;
    allRecords.reserve(allChunks.size() + docsToEmbed.size()); // chunks + doc-level embeddings

    for (size_t docIdx = 0; docIdx < docsToEmbed.size(); ++docIdx) {
        const auto& doc = docsToEmbed[docIdx];
        const auto& chunkIndices = docToChunkIndices[docIdx];

        // Add chunk-level records
        for (size_t chunkIdx : chunkIndices) {
            const auto& chunk = allChunks[chunkIdx];
            yams::vector::VectorRecord rec;
            rec.document_hash = doc.hash;
            rec.chunk_id = chunk.chunkId;
            rec.embedding = std::move(embeddings[chunkIdx]);
            rec.content = chunk.content;
            rec.start_offset = chunk.startOffset;
            rec.end_offset = chunk.endOffset;
            rec.level = yams::vector::EmbeddingLevel::CHUNK;
            rec.metadata["name"] = doc.fileName;
            rec.metadata["mime_type"] = doc.mimeType;
            rec.metadata["path"] = doc.filePath;
            allRecords.push_back(std::move(rec));
        }

        // Compute document-level embedding (average of chunks, normalized)
        if (!chunkIndices.empty() && !embeddings[chunkIndices[0]].empty()) {
            size_t dim = embeddings[chunkIndices[0]].size();
            std::vector<float> docEmbedding(dim, 0.0f);

            for (size_t chunkIdx : chunkIndices) {
                const auto& emb = embeddings[chunkIdx];
                for (size_t j = 0; j < dim && j < emb.size(); ++j) {
                    docEmbedding[j] += emb[j];
                }
            }

            // Normalize
            float norm = 0.0f;
            for (float v : docEmbedding) {
                norm += v * v;
            }
            if (norm > 0.0f) {
                norm = std::sqrt(norm);
                for (float& v : docEmbedding) {
                    v /= norm;
                }
            }

            yams::vector::VectorRecord docRec;
            docRec.document_hash = doc.hash;
            docRec.chunk_id = yams::vector::utils::generateChunkId(doc.hash, 999999);
            docRec.embedding = std::move(docEmbedding);
            docRec.content = doc.text.substr(0, 1000);
            docRec.level = yams::vector::EmbeddingLevel::DOCUMENT;
            for (size_t chunkIdx : chunkIndices) {
                docRec.source_chunk_ids.push_back(allChunks[chunkIdx].chunkId);
            }
            docRec.metadata["name"] = doc.fileName;
            docRec.metadata["mime_type"] = doc.mimeType;
            docRec.metadata["path"] = doc.filePath;
            allRecords.push_back(std::move(docRec));
        }
    }

    // Single batch insert for all records
    if (!vdb->insertVectorsBatch(allRecords)) {
        spdlog::error("EmbeddingService: batch vector insert failed: {}", vdb->getLastError());
        failed_.fetch_add(docsToEmbed.size());
        for (const auto& doc : docsToEmbed) {
            (void)meta_->updateDocumentRepairStatus(doc.hash, metadata::RepairStatus::Failed);
        }
        return;
    }

    // Update metadata and repair status for all succeeded documents
    for (const auto& doc : docsToEmbed) {
        (void)meta_->updateDocumentEmbeddingStatusByHash(doc.hash, true, modelName);
        (void)meta_->updateDocumentRepairStatus(doc.hash, metadata::RepairStatus::Completed);
    }

    processed_.fetch_add(docsToEmbed.size());

    spdlog::debug("EmbeddingService: batch complete (succeeded={}, skipped={}, failed={})",
                  docsToEmbed.size(), skipped, failedGather);
}

} // namespace daemon
} // namespace yams
