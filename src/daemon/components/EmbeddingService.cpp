#include <yams/daemon/components/EmbeddingService.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>
#include <unordered_map>

#include <fmt/format.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/api/content_store.h>
#include <yams/core/types.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningSnapshot.h>
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
    std::size_t capacity = static_cast<std::size_t>(TuneAdvisor::embedChannelCapacity());
    std::size_t postIngestCap = static_cast<std::size_t>(TuneAdvisor::postIngestQueueMax());
    if (postIngestCap > 0) {
        capacity = std::min(capacity, postIngestCap);
    }
    // ONNX limits impose a hard ceiling of 64 concurrent embed jobs.
    // Clamp channel capacity to this ceiling while keeping it at least a sane minimum.
    const std::size_t kOnnxHardLimit = 64u;
    capacity = std::min(capacity, kOnnxHardLimit);
    capacity = std::max<std::size_t>(256u, capacity);
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
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Embed, true);
    boost::asio::co_spawn(strand_, channelPoller(), boost::asio::detached);
    spdlog::info("EmbeddingService: started parallel channel poller");
}

void EmbeddingService::shutdown() {
    if (stop_.exchange(true)) {
        return;
    }
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Embed, false);
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
    const std::size_t ch = embedChannel_ ? embedChannel_->size_approx() : 0;
    const std::size_t pending = this->pendingApprox_.load(std::memory_order_relaxed);
    return ch + pending;
}

std::size_t EmbeddingService::inFlightJobs() const {
    return inFlight_.load();
}

boost::asio::awaitable<void> EmbeddingService::channelPoller() {
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    auto idleDelay = std::chrono::milliseconds(5);
    auto maxIdleDelay = []() {
        auto snap = TuningSnapshotRegistry::instance().get();
        uint32_t pollMs = snap ? snap->workerPollMs : TuneAdvisor::workerPollMs();
        return std::chrono::milliseconds(std::max<uint32_t>(50, pollMs));
    };

    spdlog::info("[EmbeddingService] Parallel poller started");

    std::size_t lastEffectiveConcurrent = 0;
    while (!stop_.load()) {
        bool didWork = false;
        InternalEventBus::EmbedJob job;

        // Dynamic concurrency from TuneAdvisor (scaled by TuningManager), plus local
        // pressure-based ramp to avoid staying under-utilized while embed backlog spikes.
        const std::size_t baseConcurrent =
            std::max<std::size_t>(1, TuneAdvisor::postEmbedConcurrent());
        const std::size_t hardConcurrentCap =
            std::max<std::size_t>(baseConcurrent, TuneAdvisor::getEmbedMaxConcurrency());
        const std::size_t maxBatchSize = TuneAdvisor::resolvedEmbedDocCap();
        const std::size_t channelBacklog = embedChannel_ ? embedChannel_->size_approx() : 0;
        const std::size_t bufferedBacklog = channelBacklog + this->pendingJobs_.size();

        std::size_t effectiveMaxConcurrent = baseConcurrent;
        if (hardConcurrentCap > baseConcurrent) {
            const std::size_t rampThreshold =
                std::max<std::size_t>(baseConcurrent * maxBatchSize, 32u);
            const std::size_t rampStep = std::max<std::size_t>(maxBatchSize, 16u);
            if (bufferedBacklog > rampThreshold) {
                const std::size_t extra =
                    (bufferedBacklog - rampThreshold + (rampStep - 1)) / rampStep;
                effectiveMaxConcurrent =
                    std::min<std::size_t>(hardConcurrentCap, baseConcurrent + extra);
            }
        }
        const std::size_t maxPendingJobs =
            std::max<std::size_t>(maxBatchSize, effectiveMaxConcurrent * 2);
        if (effectiveMaxConcurrent != lastEffectiveConcurrent) {
            spdlog::info(
                "[EmbeddingService] adaptive concurrency: base={} effective={} hard_cap={} "
                "backlog={} channel={} pending={}",
                baseConcurrent, effectiveMaxConcurrent, hardConcurrentCap, bufferedBacklog,
                channelBacklog, this->pendingJobs_.size());
            lastEffectiveConcurrent = effectiveMaxConcurrent;
        }

        // Pull jobs into pending buffer to allow model-based grouping
        while (embedChannel_ && embedChannel_->try_pop(job)) {
            didWork = true;
            this->pendingJobs_.push_back(std::move(job));
            if (this->pendingJobs_.size() >= maxPendingJobs) {
                break;
            }
        }

        // Keep an approximate pending backlog for status/benchmarks.
        this->pendingApprox_.store(this->pendingJobs_.size(), std::memory_order_relaxed);

        if (inFlight_.load() < effectiveMaxConcurrent && !this->pendingJobs_.empty()) {
            std::string defaultModel;
            if (getPreferredModel_) {
                defaultModel = getPreferredModel_();
            }
            // Group pending jobs by model name to reduce model switching
            std::unordered_map<std::string, InternalEventBus::EmbedJob> grouped;
            grouped.reserve(this->pendingJobs_.size());

            std::vector<InternalEventBus::EmbedJob> deferred;
            deferred.reserve(this->pendingJobs_.size());

            for (auto& pending : this->pendingJobs_) {
                if (pending.hashes.empty()) {
                    continue;
                }
                if (pending.modelName.empty() && !defaultModel.empty()) {
                    pending.modelName = defaultModel;
                }
                auto& bucket = grouped[pending.modelName];
                if (bucket.hashes.empty()) {
                    bucket.modelName = pending.modelName;
                    bucket.skipExisting = pending.skipExisting;
                }
                if (bucket.skipExisting != pending.skipExisting) {
                    deferred.push_back(std::move(pending));
                    continue;
                }
                bucket.hashes.insert(bucket.hashes.end(), pending.hashes.begin(),
                                     pending.hashes.end());
            }

            this->pendingJobs_.clear();
            if (!deferred.empty()) {
                this->pendingJobs_.insert(this->pendingJobs_.end(),
                                          std::make_move_iterator(deferred.begin()),
                                          std::make_move_iterator(deferred.end()));
            }

            this->pendingApprox_.store(this->pendingJobs_.size(), std::memory_order_relaxed);

            auto dispatchJob = [this](InternalEventBus::EmbedJob&& dispatch) {
                if (dispatch.hashes.empty()) {
                    return;
                }
                dispatch.batchSize = static_cast<uint32_t>(dispatch.hashes.size());
                inFlight_.fetch_add(1);
                InternalEventBus::instance().incEmbedConsumed(dispatch.batchSize);

                boost::asio::post(
                    coordinator_->getExecutor(), [this, job = std::move(dispatch)]() mutable {
                        struct ScopeGuard {
                            std::atomic<std::size_t>& counter;
                            ~ScopeGuard() { counter.fetch_sub(1); }
                        } guard{inFlight_};

                        try {
                            processEmbedJob(std::move(job));
                        } catch (const std::exception& e) {
                            spdlog::error("[EmbeddingService] Uncaught exception in embed job: {}",
                                          e.what());
                        } catch (...) {
                            spdlog::error("[EmbeddingService] Unknown exception in embed job");
                        }
                    });
            };

            // Dispatch grouped jobs in model-coalesced batches, preferring named models first
            auto dispatchBuckets = [&](bool preferNamed) {
                for (auto& [key, bucket] : grouped) {
                    const bool isNamed = !key.empty();
                    if (preferNamed != isNamed) {
                        continue;
                    }
                    std::size_t offset = 0;
                    while (offset < bucket.hashes.size() &&
                           inFlight_.load() < effectiveMaxConcurrent) {
                        std::size_t take = std::min(maxBatchSize, bucket.hashes.size() - offset);
                        InternalEventBus::EmbedJob split;
                        split.modelName = bucket.modelName;
                        split.skipExisting = bucket.skipExisting;
                        split.hashes.assign(
                            bucket.hashes.begin() + static_cast<std::ptrdiff_t>(offset),
                            bucket.hashes.begin() + static_cast<std::ptrdiff_t>(offset + take));
                        dispatchJob(std::move(split));
                        offset += take;
                    }

                    if (offset < bucket.hashes.size()) {
                        InternalEventBus::EmbedJob remainder;
                        remainder.modelName = bucket.modelName;
                        remainder.skipExisting = bucket.skipExisting;
                        remainder.hashes.assign(bucket.hashes.begin() +
                                                    static_cast<std::ptrdiff_t>(offset),
                                                bucket.hashes.end());
                        this->pendingJobs_.push_back(std::move(remainder));
                    }
                }
            };

            dispatchBuckets(true);
            dispatchBuckets(false);

            this->pendingApprox_.store(this->pendingJobs_.size(), std::memory_order_relaxed);
        }

        if (didWork) {
            idleDelay = std::chrono::milliseconds(5);
            continue; // Check for more work immediately
        }

        // Idle - wait before polling again
        timer.expires_after(idleDelay);
        co_await timer.async_wait(boost::asio::use_awaitable);
        const auto maxIdle = maxIdleDelay();
        if (idleDelay < maxIdle) {
            idleDelay *= 2;
            if (idleDelay > maxIdle) {
                idleDelay = maxIdle;
            }
        }
    }

    this->pendingApprox_.store(0, std::memory_order_relaxed);

    spdlog::info("[EmbeddingService] Parallel poller exited");
}

void EmbeddingService::processEmbedJob(InternalEventBus::EmbedJob job) {
    const bool timingEnabled = []() {
        if (const char* s = std::getenv("YAMS_EMBED_DEBUG_TIMINGS")) {
            return std::string{s} == "1" || std::string{s} == "true" || std::string{s} == "yes";
        }
        return false;
    }();
    uint64_t warnMs = 5000;
    if (const char* s = std::getenv("YAMS_EMBED_TIMING_WARN_MS")) {
        try {
            warnMs = static_cast<uint64_t>(std::stoull(s));
        } catch (...) {
        }
    }

    const auto jobTag = [&]() -> std::string {
        if (!job.hashes.empty()) {
            const auto& h = job.hashes.front();
            return h.size() > 12 ? h.substr(0, 12) : h;
        }
        return std::string{"empty"};
    }();

    auto logPhase = [&](const char* phase, std::chrono::steady_clock::time_point start,
                        const std::string& detail) {
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - start)
                            .count();
        if (timingEnabled || ms >= static_cast<long long>(warnMs)) {
            spdlog::info("[EmbeddingService] job={} phase={} dur_ms={} {}", jobTag, phase, ms,
                         detail);
        }
    };

    spdlog::debug(
        "[EmbeddingService] processEmbedJob job={} hashes={} skipExisting={} modelHint='{}'",
        jobTag, job.hashes.size(), job.skipExisting ? "true" : "false", job.modelName);
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
    const auto tGather = std::chrono::steady_clock::now();
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

            // Check embedding status via metadata repository (separate DB, no VectorDatabase lock)
            // This avoids mutex contention with EntityGraphService's insertEntityVectorsBatch
            if (job.skipExisting) {
                auto hasEmbedRes = meta_->hasDocumentEmbeddingByHash(hash);
                if (hasEmbedRes && hasEmbedRes.value()) {
                    spdlog::debug("EmbeddingService: skipExisting=true, already embedded: {}",
                                  hash);
                    skipped++;
                    continue;
                }
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
        logPhase("gather", tGather,
                 fmt::format("docs_to_embed=0 skipped={} failed_gather={}", skipped, failedGather));
        spdlog::debug("EmbeddingService: no documents to embed after gathering");
        return;
    }

    logPhase("gather", tGather,
             fmt::format("docs_to_embed={} skipped={} failed_gather={} hashes_in_job={} model='{}'",
                         docsToEmbed.size(), skipped, failedGather, job.hashes.size(), modelName));

    spdlog::debug("EmbeddingService: gathered {} documents for embedding", docsToEmbed.size());

    // ============================================================
    // Phase 2: Chunk all documents
    // ============================================================
    const auto tChunk = std::chrono::steady_clock::now();
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

    logPhase("chunk", tChunk,
             fmt::format("docs={} chunks={} avg_chunks_per_doc={:.2f}", docsToEmbed.size(),
                         allChunks.size(),
                         docsToEmbed.empty() ? 0.0
                                             : (static_cast<double>(allChunks.size()) /
                                                static_cast<double>(docsToEmbed.size()))));

    // ============================================================
    // Phase 3: Batch embedding call with sub-batching to avoid timeouts
    // ============================================================
    // Model inference can be slow for large batches. Sub-batch to keep response times reasonable.
    std::size_t kMaxBatchSize = TuneAdvisor::resolvedEmbedDocCap();
    // Ensure batch size does not exceed 64 as required by ONNX limits.
    // If TuneAdvisor reports 0 (unset), default to the maximum allowed (64).
    if (kMaxBatchSize == 0 || kMaxBatchSize > 64) {
        kMaxBatchSize = 64;
    }
    std::vector<std::vector<float>> embeddings;
    embeddings.reserve(allTexts.size());

    for (size_t start = 0; start < allTexts.size(); start += kMaxBatchSize) {
        size_t end = std::min(start + kMaxBatchSize, allTexts.size());
        std::vector<std::string> subBatch(allTexts.begin() + start, allTexts.begin() + end);

        spdlog::debug("EmbeddingService: generating embeddings for batch {}-{} of {}", start, end,
                      allTexts.size());

        const auto tInfer = std::chrono::steady_clock::now();
        auto embedResult = provider->generateBatchEmbeddingsFor(modelName, subBatch);
        logPhase("infer", tInfer,
                 fmt::format("sub_batch=[{}, {}) size={} total_texts={} model='{}'", start, end,
                             subBatch.size(), allTexts.size(), modelName));
        if (!embedResult) {
            spdlog::error("EmbeddingService: batch embedding failed at {}-{}: {}", start, end,
                          embedResult.error().message);
            failed_.fetch_add(docsToEmbed.size());
            std::vector<std::string> failedHashes;
            failedHashes.reserve(docsToEmbed.size());
            for (const auto& doc : docsToEmbed) {
                failedHashes.push_back(doc.hash);
            }
            (void)meta_->batchUpdateDocumentRepairStatuses(failedHashes,
                                                           metadata::RepairStatus::Failed);
            return;
        }

        auto& batchEmbeddings = embedResult.value();
        for (auto& emb : batchEmbeddings) {
            embeddings.push_back(std::move(emb));
        }
    }

    if (embeddings.size() != allChunks.size()) {
        spdlog::error("EmbeddingService: embedding count mismatch ({} vs {})", embeddings.size(),
                      allChunks.size());
        failed_.fetch_add(docsToEmbed.size());
        std::vector<std::string> failedHashes;
        failedHashes.reserve(docsToEmbed.size());
        for (const auto& doc : docsToEmbed) {
            failedHashes.push_back(doc.hash);
        }
        (void)meta_->batchUpdateDocumentRepairStatuses(failedHashes,
                                                       metadata::RepairStatus::Failed);
        return;
    }

    spdlog::debug("EmbeddingService: generated {} embeddings total", embeddings.size());

    // ============================================================
    // Phase 4: Build VectorRecords and batch insert
    // ============================================================
    const auto tBuild = std::chrono::steady_clock::now();
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

    logPhase("build_records", tBuild,
             fmt::format("docs={} chunks={} records={} (chunk+doc)", docsToEmbed.size(),
                         allChunks.size(), allRecords.size()));

    // Chunked batch insert: keeps each DB/HNSW critical section shorter so other
    // embed jobs can make progress under high load.
    const auto tInsert = std::chrono::steady_clock::now();
    const std::size_t insertChunkSize =
        std::clamp<std::size_t>(std::max<std::size_t>(64u, kMaxBatchSize * 2u), 64u, 512u);
    std::size_t insertedRecords = 0;
    for (std::size_t offset = 0; offset < allRecords.size(); offset += insertChunkSize) {
        const std::size_t take = std::min(insertChunkSize, allRecords.size() - offset);
        std::vector<yams::vector::VectorRecord> chunk;
        chunk.reserve(take);
        for (std::size_t i = 0; i < take; ++i) {
            chunk.push_back(std::move(allRecords[offset + i]));
        }
        if (!vdb->insertVectorsBatch(chunk)) {
            logPhase("vdb_insert", tInsert,
                     fmt::format("records={} inserted={} chunk={} result=fail err='{}'",
                                 allRecords.size(), insertedRecords, take, vdb->getLastError()));
            spdlog::error("EmbeddingService: chunked vector insert failed at offset={} size={}: {}",
                          offset, take, vdb->getLastError());
            failed_.fetch_add(docsToEmbed.size());
            std::vector<std::string> failedHashes;
            failedHashes.reserve(docsToEmbed.size());
            for (const auto& doc : docsToEmbed) {
                failedHashes.push_back(doc.hash);
            }
            (void)meta_->batchUpdateDocumentRepairStatuses(failedHashes,
                                                           metadata::RepairStatus::Failed);
            return;
        }
        insertedRecords += take;
    }

    logPhase("vdb_insert", tInsert,
             fmt::format("records={} chunk_size={} chunks={} result=ok", allRecords.size(),
                         insertChunkSize,
                         (allRecords.size() + (insertChunkSize - 1)) / insertChunkSize));

    // Update metadata and repair status for all succeeded documents (single transaction each)
    const auto tMeta = std::chrono::steady_clock::now();
    std::vector<std::string> successHashes;
    successHashes.reserve(docsToEmbed.size());
    for (const auto& doc : docsToEmbed) {
        successHashes.push_back(doc.hash);
    }
    (void)meta_->batchUpdateDocumentEmbeddingStatusByHashes(successHashes, true, modelName);
    (void)meta_->batchUpdateDocumentRepairStatuses(successHashes,
                                                   metadata::RepairStatus::Completed);

    logPhase("metadata_update", tMeta,
             fmt::format("docs={} model='{}'", successHashes.size(), modelName));

    processed_.fetch_add(docsToEmbed.size());

    spdlog::debug("EmbeddingService: batch complete (succeeded={}, skipped={}, failed={})",
                  docsToEmbed.size(), skipped, failedGather);
}

} // namespace daemon
} // namespace yams
