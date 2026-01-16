#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <thread>
#include <unordered_set>
#include <boost/asio.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/api/content_store.h>
#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/resource/external_entity_provider_adapter.h>
#include <yams/extraction/extraction_util.h>
#include <yams/extraction/text_extractor.h>
#include <yams/ingest/ingest_helpers.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/document_chunker.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>

using yams::extraction::util::extractDocumentText;

namespace yams::daemon {

// Dynamic concurrency limits from TuneAdvisor
std::size_t PostIngestQueue::maxExtractionConcurrent() {
    return static_cast<std::size_t>(TuneAdvisor::postExtractionConcurrent());
}

std::size_t PostIngestQueue::maxKgConcurrent() {
    return static_cast<std::size_t>(TuneAdvisor::postKgConcurrent());
}

std::size_t PostIngestQueue::maxSymbolConcurrent() {
    return static_cast<std::size_t>(TuneAdvisor::postSymbolConcurrent());
}

std::size_t PostIngestQueue::maxEntityConcurrent() {
    return static_cast<std::size_t>(TuneAdvisor::postEntityConcurrent());
}

PostIngestQueue::PostIngestQueue(
    std::shared_ptr<api::IContentStore> store, std::shared_ptr<metadata::MetadataRepository> meta,
    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors,
    std::shared_ptr<metadata::KnowledgeGraphStore> kg,
    std::shared_ptr<GraphComponent> graphComponent, WorkCoordinator* coordinator,
    WorkCoordinator* entityCoordinator, std::size_t capacity)
    : store_(std::move(store)), meta_(std::move(meta)), extractors_(std::move(extractors)),
      kg_(std::move(kg)), graphComponent_(std::move(graphComponent)), coordinator_(coordinator),
      entityCoordinator_(entityCoordinator), capacity_(capacity ? capacity : 1000) {
    spdlog::info("[PostIngestQueue] Created (parallel processing via WorkCoordinator)");
}

PostIngestQueue::~PostIngestQueue() {
    stop();
}

void PostIngestQueue::start() {
    spdlog::info("[PostIngestQueue] start() called, stop_={}", stop_.load());
    if (!stop_.load()) {
        spdlog::info("[PostIngestQueue] Spawning channelPoller coroutine...");
        boost::asio::co_spawn(coordinator_->getExecutor(), channelPoller(), boost::asio::detached);
        spdlog::info("[PostIngestQueue] Spawning kgPoller coroutine...");
        boost::asio::co_spawn(coordinator_->getExecutor(), kgPoller(), boost::asio::detached);
        spdlog::info("[PostIngestQueue] Spawning symbolPoller coroutine...");
        boost::asio::co_spawn(coordinator_->getExecutor(), symbolPoller(), boost::asio::detached);
        spdlog::info("[PostIngestQueue] Spawning entityPoller coroutine...");
        auto entityExec =
            entityCoordinator_ ? entityCoordinator_->getExecutor() : coordinator_->getExecutor();
        boost::asio::co_spawn(entityExec, entityPoller(), boost::asio::detached);

        constexpr int maxWaitMs = 100;
        for (int i = 0; i < maxWaitMs && (!started_.load() || !kgStarted_.load() ||
                                          !symbolStarted_.load() || !entityStarted_.load());
             ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        spdlog::info(
            "[PostIngestQueue] Pollers started (extraction={}, kg={}, symbol={}, entity={})",
            started_.load(), kgStarted_.load(), symbolStarted_.load(), entityStarted_.load());
    } else {
        spdlog::warn("[PostIngestQueue] start() skipped because stop_=true");
    }
}

void PostIngestQueue::stop() {
    stop_.store(true);
    spdlog::info("[PostIngestQueue] Stop requested");
}

std::size_t PostIngestQueue::resolveChannelCapacity() const {
    std::size_t cap = capacity_;
    if (cap == 0) {
        cap = static_cast<std::size_t>(TuneAdvisor::postIngestQueueMax());
    }
    if (cap == 0) {
        cap = 1;
    }
    return cap;
}

void PostIngestQueue::checkDrainAndSignal() {
    // Check if queue is now drained (all stages idle)
    if (totalInFlight() == 0) {
        // Only signal if we were previously active (had work)
        bool expected = true;
        if (wasActive_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
            // Queue just became drained - signal corpus stats stale
            if (meta_) {
                meta_->signalCorpusStatsStale();
            }

            // Invoke drain callback (for search engine rebuild trigger)
            DrainCallback cb;
            {
                std::lock_guard<std::mutex> lock(drainCallbackMutex_);
                cb = drainCallback_;
            }
            if (cb) {
                cb();
            }

            spdlog::debug(
                "[PostIngestQueue] Queue drained, signaled corpus stats and drain callback");
        }
    }
}

boost::asio::awaitable<void> PostIngestQueue::channelPoller() {
    spdlog::info("[PostIngestQueue] channelPoller coroutine STARTED");
    const std::size_t channelCapacity = resolveChannelCapacity();
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            "post_ingest", channelCapacity);
    spdlog::info("[PostIngestQueue] channelPoller got channel (cap={})", channelCapacity);

    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
    spdlog::info("[PostIngestQueue] channelPoller got timer");

    started_.store(true);

    auto idleDelay = std::chrono::milliseconds(1);
    constexpr auto kMaxIdleDelay = std::chrono::milliseconds(10);

    while (!stop_.load()) {
        bool didWork = false;
        InternalEventBus::PostIngestTask task;
        const std::size_t batchSize = std::max<std::size_t>(1u, TuneAdvisor::postIngestBatchSize());
        std::vector<InternalEventBus::PostIngestTask> batch;
        batch.reserve(batchSize);
        // Dynamic concurrency limit from TuneAdvisor
        const std::size_t maxConcurrent = maxExtractionConcurrent();
        while (inFlight_.load() < maxConcurrent && batch.size() < batchSize &&
               channel->try_pop(task)) {
            didWork = true;
            inFlight_.fetch_add(1);
            batch.push_back(std::move(task));
        }

        if (didWork && !batch.empty()) {
            wasActive_.store(true, std::memory_order_release);
            const std::size_t batchCount = batch.size();
            boost::asio::post(coordinator_->getExecutor(),
                              [this, batch = std::move(batch), batchCount]() mutable {
                                  processBatch(std::move(batch));
                                  inFlight_.fetch_sub(batchCount);
                                  checkDrainAndSignal();
                              });
        }

        if (didWork) {
            idleDelay = std::chrono::milliseconds(1);
            continue;
        }

        timer.expires_after(idleDelay);
        co_await timer.async_wait(boost::asio::use_awaitable);
        if (idleDelay < kMaxIdleDelay) {
            idleDelay *= 2;
        }
    }

    spdlog::info("[PostIngestQueue] Channel poller exited");
}

void PostIngestQueue::processBatch(std::vector<InternalEventBus::PostIngestTask>&& tasks) {
    if (tasks.empty()) {
        return;
    }

    std::unordered_map<std::string, metadata::DocumentInfo> infoMap;
    std::unordered_map<int64_t, std::vector<std::string>> tagsByDocId;
    static const std::vector<std::string> kEmptyTags;

    std::unordered_map<std::string, std::string> symbolExtensionMap;
    {
        std::lock_guard<std::mutex> lock(extMapMutex_);
        symbolExtensionMap = symbolExtensionMap_;
    }

    std::vector<std::shared_ptr<ExternalEntityProviderAdapter>> entityProviders;
    {
        std::lock_guard<std::mutex> lock(entityMutex_);
        entityProviders = entityProviders_;
    }

    if (meta_) {
        std::vector<std::string> hashes;
        hashes.reserve(tasks.size());
        std::unordered_set<std::string> seen;
        seen.reserve(tasks.size());
        for (const auto& task : tasks) {
            if (seen.insert(task.hash).second) {
                hashes.push_back(task.hash);
            }
        }

        if (!hashes.empty()) {
            auto infoRes = meta_->batchGetDocumentsByHash(hashes);
            if (infoRes) {
                infoMap = std::move(infoRes).value();

                std::vector<int64_t> docIds;
                docIds.reserve(infoMap.size());
                for (const auto& [_, info] : infoMap) {
                    if (info.id >= 0) {
                        docIds.push_back(info.id);
                    }
                }

                if (!docIds.empty()) {
                    auto tagsRes = meta_->batchGetDocumentTags(docIds);
                    if (tagsRes) {
                        tagsByDocId = std::move(tagsRes).value();
                    } else {
                        spdlog::warn("[PostIngestQueue] batchGetDocumentTags failed: {}",
                                     tagsRes.error().message);
                    }
                }
            } else {
                spdlog::warn("[PostIngestQueue] batchGetDocumentsByHash failed: {}",
                             infoRes.error().message);
            }
        }
    }

    // =========================================================================
    // 4-PHASE BATCHED METADATA PROCESSING
    // Phase 1: Prepare - extract text for all tasks
    // Phase 2: Batch DB write - single transaction for all successes
    // Phase 3: Handle failures - update extraction status
    // Phase 4: Dispatch - send to channels
    // =========================================================================

    std::vector<PreparedMetadataEntry> successes;
    std::vector<ExtractionFailure> failures;
    std::vector<const InternalEventBus::PostIngestTask*> fallbackTasks; // Tasks without info

    successes.reserve(tasks.size());
    failures.reserve(tasks.size() / 10); // Expect ~10% failure rate

    // Phase 1: PREPARATION - extract text for all documents
    for (const auto& task : tasks) {
        try {
            auto it = infoMap.find(task.hash);
            if (it != infoMap.end()) {
                const auto tagsIt = tagsByDocId.find(it->second.id);
                const std::vector<std::string>& tags =
                    (tagsIt != tagsByDocId.end()) ? tagsIt->second : kEmptyTags;

                auto result = prepareMetadataEntry(task.hash, task.mime, it->second, tags,
                                                   symbolExtensionMap, entityProviders);

                if (auto* prepared = std::get_if<PreparedMetadataEntry>(&result)) {
                    successes.push_back(std::move(*prepared));
                } else {
                    failures.push_back(std::get<ExtractionFailure>(result));
                }
            } else {
                // No DocumentInfo in batch lookup - use fallback path
                fallbackTasks.push_back(&task);
            }
        } catch (const std::exception& e) {
            spdlog::error("[PostIngestQueue] Preparation failed for {}: {}", task.hash, e.what());
            failed_++;
        }
    }

    // Phase 2: BATCH DB WRITE - single transaction for all successful extractions
    if (!successes.empty() && meta_) {
        std::vector<metadata::BatchContentEntry> entries;
        entries.reserve(successes.size());

        for (const auto& prepared : successes) {
            metadata::BatchContentEntry entry;
            entry.documentId = prepared.documentId;
            entry.title = prepared.fileName;
            entry.contentText = prepared.extractedText;
            entry.mimeType = prepared.mimeType;
            entry.extractionMethod = "post_ingest";
            entry.language = prepared.language;
            entries.push_back(std::move(entry));
        }

        auto batchResult = meta_->batchInsertContentAndIndex(entries);
        if (!batchResult) {
            spdlog::error("[PostIngestQueue] Batch DB write failed: {}",
                          batchResult.error().message);
            if (batchResult.error().message.find("database is locked") != std::string::npos) {
                TuneAdvisor::reportDbLockError();
            }
            // All successes become failures
            for (const auto& prepared : successes) {
                failures.push_back(ExtractionFailure{prepared.documentId, prepared.hash,
                                                     batchResult.error().message});
            }
            successes.clear();
        } else {
            spdlog::info("[PostIngestQueue] Batch DB write succeeded for {} documents",
                         entries.size());
        }
    }

    // Phase 3: HANDLE FAILURES - update extraction status for failed documents
    for (const auto& failure : failures) {
        if (failure.documentId >= 0 && meta_) {
            auto updateRes = meta_->updateDocumentExtractionStatus(
                failure.documentId, false, metadata::ExtractionStatus::Failed,
                failure.errorMessage);
            if (!updateRes) {
                spdlog::warn("[PostIngestQueue] Failed to mark extraction failed for {}: {}",
                             failure.hash, updateRes.error().message);
            }
        }
    }

    // Phase 4: DISPATCH - send successful documents to channels
    const std::size_t embedBatchThreshold = TuneAdvisor::postIngestBatchSize();
    std::vector<std::string> embeddingHashes;
    embeddingHashes.reserve(successes.size() + fallbackTasks.size());

    for (const auto& prepared : successes) {
        if (prepared.shouldDispatchKg) {
            dispatchToKgChannel(prepared.hash, prepared.documentId, prepared.fileName,
                                std::vector<std::string>(prepared.tags));
        }
        if (prepared.shouldDispatchSymbol) {
            dispatchToSymbolChannel(prepared.hash, prepared.documentId, prepared.fileName,
                                    prepared.symbolLanguage);
        }
        if (prepared.shouldDispatchEntity) {
            dispatchToEntityChannel(prepared.hash, prepared.documentId, prepared.fileName,
                                    prepared.extension);
        }
        embeddingHashes.push_back(prepared.hash);
        if (embeddingHashes.size() >= embedBatchThreshold) {
            processEmbeddingBatch(embeddingHashes);
            embeddingHashes.clear();
        }
        processed_++;
        InternalEventBus::instance().incPostConsumed();
    }

    // Handle fallback tasks (those without DocumentInfo) using legacy path
    for (const auto* taskPtr : fallbackTasks) {
        try {
            processMetadataStage(taskPtr->hash, taskPtr->mime, std::nullopt, &kEmptyTags,
                                 symbolExtensionMap, entityProviders);
            embeddingHashes.push_back(taskPtr->hash);
            if (embeddingHashes.size() >= embedBatchThreshold) {
                processEmbeddingBatch(embeddingHashes);
                embeddingHashes.clear();
            }
            processed_++;
            InternalEventBus::instance().incPostConsumed();
        } catch (const std::exception& e) {
            spdlog::error("[PostIngestQueue] Fallback processing failed for {}: {}", taskPtr->hash,
                          e.what());
            failed_++;
        }
    }

    // Dispatch any remaining embedding hashes
    if (!embeddingHashes.empty()) {
        processEmbeddingBatch(embeddingHashes);
    }
}

void PostIngestQueue::enqueue(Task t) {
    static constexpr const char* kChannelName = "post_ingest";
    const std::size_t channelCapacity = resolveChannelCapacity();
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            kChannelName, channelCapacity);

    InternalEventBus::PostIngestTask task;
    task.hash = std::move(t.hash);
    task.mime = std::move(t.mime);

    constexpr int maxRetries = 10;
    constexpr auto baseBackoff = std::chrono::milliseconds(50);
    constexpr auto maxBackoff = std::chrono::milliseconds(1000);

    for (int i = 0; i < maxRetries; ++i) {
        if (channel->try_push(task)) {
            return;
        }
        auto delay = std::min(baseBackoff * (1 << i), maxBackoff);
        std::this_thread::sleep_for(delay);
    }

    spdlog::error("[PostIngestQueue] Channel full after {} retries, dropping task for hash: {}",
                  maxRetries, task.hash);
}

bool PostIngestQueue::tryEnqueue(const Task& t) {
    static constexpr const char* kChannelName = "post_ingest";
    const std::size_t channelCapacity = resolveChannelCapacity();
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            kChannelName, channelCapacity);

    InternalEventBus::PostIngestTask task;
    task.hash = t.hash;
    task.mime = t.mime;

    return channel->try_push(task);
}

bool PostIngestQueue::tryEnqueue(Task&& t) {
    static constexpr const char* kChannelName = "post_ingest";
    const std::size_t channelCapacity = resolveChannelCapacity();
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            kChannelName, channelCapacity);

    InternalEventBus::PostIngestTask task;
    task.hash = std::move(t.hash);
    task.mime = std::move(t.mime);

    return channel->try_push(std::move(task));
}

std::size_t PostIngestQueue::size() const {
    static constexpr const char* kChannelName = "post_ingest";
    const std::size_t channelCapacity = resolveChannelCapacity();
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            kChannelName, channelCapacity);
    return channel ? channel->size_approx() : 0;
}

void PostIngestQueue::processTask(const std::string& hash, const std::string& mime) {
    try {
        std::optional<metadata::DocumentInfo> info;
        std::vector<std::string> tags;

        if (meta_) {
            auto infoRes = meta_->batchGetDocumentsByHash(std::vector<std::string>{hash});
            if (infoRes) {
                auto& infoMap = infoRes.value();
                auto it = infoMap.find(hash);
                if (it != infoMap.end() && it->second.id >= 0) {
                    info = it->second;

                    auto tagsRes = meta_->batchGetDocumentTags(std::vector<int64_t>{it->second.id});
                    if (tagsRes) {
                        auto& tagsById = tagsRes.value();
                        auto tagsIt = tagsById.find(it->second.id);
                        if (tagsIt != tagsById.end()) {
                            tags = tagsIt->second;
                        }
                    } else {
                        spdlog::warn("[PostIngestQueue] batchGetDocumentTags failed: {}",
                                     tagsRes.error().message);
                    }
                }
            } else {
                spdlog::warn("[PostIngestQueue] batchGetDocumentsByHash failed: {}",
                             infoRes.error().message);
            }
        }

        // If metadata lookup didn't find a document, still skip per-doc tag query.
        static const std::vector<std::string> kEmptyTags;
        processMetadataStage(hash, mime, info, info ? &tags : &kEmptyTags, {}, {});
        processEmbeddingBatch(std::vector<std::string>{hash});
        processed_++;
        InternalEventBus::instance().incPostConsumed();
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] Failed to process {}: {}", hash, e.what());
        failed_++;
    }
}

namespace {

inline bool extensionSupportsEntityProviders(
    const std::vector<std::shared_ptr<yams::daemon::ExternalEntityProviderAdapter>>& providers,
    const std::string& extension) {
    for (const auto& provider : providers) {
        if (provider && provider->supports(extension)) {
            return true;
        }
    }
    return false;
}

} // namespace

void PostIngestQueue::processMetadataStage(
    const std::string& hash, const std::string& mime,
    const std::optional<metadata::DocumentInfo>& infoOpt,
    const std::vector<std::string>* tagsOverride,
    const std::unordered_map<std::string, std::string>& symbolExtensionMap,
    const std::vector<std::shared_ptr<ExternalEntityProviderAdapter>>& entityProviders) {
    if (!store_ || !meta_) {
        spdlog::warn("[PostIngestQueue] store or metadata unavailable; dropping task {}", hash);
        return;
    }

    try {
        auto startTime = std::chrono::steady_clock::now();

        int64_t docId = -1;
        std::string fileName;
        std::string mimeType = mime;
        std::string extension;
        metadata::DocumentInfo info;

        if (infoOpt.has_value()) {
            info = infoOpt.value();
        } else {
            auto infoRes = meta_->getDocumentByHash(hash);
            if (infoRes && infoRes.value().has_value()) {
                info = *infoRes.value();
            } else {
                spdlog::warn(
                    "[PostIngestQueue] Metadata not found for hash {}; content may be orphaned",
                    hash);
                return;
            }
        }
        docId = info.id;
        if (!info.fileName.empty())
            fileName = info.fileName;
        if (!info.mimeType.empty())
            mimeType = info.mimeType;
        if (!info.fileExtension.empty())
            extension = info.fileExtension;

        auto txt = extractDocumentText(store_, hash, mimeType, extension, extractors_);
        if (!txt || txt->empty()) {
            spdlog::info("[PostIngestQueue] no text extracted for {} (mime={}, ext={})", hash,
                         mimeType, extension);
            if (docId >= 0) {
                auto updateRes = meta_->updateDocumentExtractionStatus(
                    docId, false, metadata::ExtractionStatus::Failed, "No text extracted");
                if (!updateRes) {
                    spdlog::warn("[PostIngestQueue] Failed to mark extraction failed for {}: {}",
                                 hash, updateRes.error().message);
                }
            }
        } else if (docId >= 0) {
            spdlog::info("[PostIngestQueue] Extracted {} bytes for {} (docId={})", txt->size(),
                         hash, docId);
            auto pr = yams::ingest::persist_content_and_index(*meta_, docId, fileName, *txt,
                                                              mimeType, "post_ingest");
            if (!pr) {
                spdlog::warn("[PostIngestQueue] persist/index failed for {}: {}", hash,
                             pr.error().message);
                // Track lock errors for adaptive concurrency scaling
                if (pr.error().message.find("database is locked") != std::string::npos) {
                    TuneAdvisor::reportDbLockError();
                }
            } else {
                auto duration = std::chrono::steady_clock::now() - startTime;
                double ms = std::chrono::duration<double, std::milli>(duration).count();
                spdlog::info("[PostIngestQueue] Metadata stage completed for {} in {:.2f}ms", hash,
                             ms);
            }
        }

        if (docId >= 0) {
            std::vector<std::string> tags;
            if (tagsOverride) {
                tags = *tagsOverride;
            }
            dispatchToKgChannel(hash, docId, fileName, std::move(tags));

            // Dispatch symbol extraction for code files (if plugin supports this extension)
            {
                // Extension map keys don't have leading dots, but DB stores with dots
                std::string extKey = extension;
                if (!extKey.empty() && extKey[0] == '.') {
                    extKey = extKey.substr(1);
                }
                auto it = symbolExtensionMap.find(extKey);
                if (it != symbolExtensionMap.end()) {
                    dispatchToSymbolChannel(hash, docId, fileName, it->second);
                }
            }

            // Dispatch entity extraction for binary files (if any entity provider supports this
            // extension)
            if (extensionSupportsEntityProviders(entityProviders, extension)) {
                dispatchToEntityChannel(hash, docId, fileName, extension);
            }
        }
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] Metadata stage failed for {}: {}", hash, e.what());
    }
}

std::variant<PostIngestQueue::PreparedMetadataEntry, PostIngestQueue::ExtractionFailure>
PostIngestQueue::prepareMetadataEntry(
    const std::string& hash, const std::string& mime, const metadata::DocumentInfo& info,
    const std::vector<std::string>& tags,
    const std::unordered_map<std::string, std::string>& symbolExtensionMap,
    const std::vector<std::shared_ptr<ExternalEntityProviderAdapter>>& entityProviders) {
    PreparedMetadataEntry prepared;
    prepared.documentId = info.id;
    prepared.hash = hash;
    prepared.fileName = info.fileName;
    prepared.mimeType = mime.empty() ? info.mimeType : mime;
    prepared.extension = info.fileExtension;
    prepared.tags = tags;

    // Extract document text
    auto txt =
        extractDocumentText(store_, hash, prepared.mimeType, prepared.extension, extractors_);
    if (!txt || txt->empty()) {
        spdlog::debug("[PostIngestQueue] no text extracted for {} (mime={}, ext={})", hash,
                      prepared.mimeType, prepared.extension);
        return ExtractionFailure{info.id, hash, "No text extracted"};
    }

    prepared.extractedText = std::move(*txt);

    // Detect language
    double langConfidence = 0.0;
    prepared.language =
        yams::extraction::LanguageDetector::detectLanguage(prepared.extractedText, &langConfidence);

    // Determine dispatch flags
    prepared.shouldDispatchKg = (info.id >= 0);

    // Symbol extraction: check if extension is in the symbol map
    std::string extKey = prepared.extension;
    if (!extKey.empty() && extKey[0] == '.') {
        extKey = extKey.substr(1);
    }
    auto symIt = symbolExtensionMap.find(extKey);
    if (symIt != symbolExtensionMap.end()) {
        prepared.shouldDispatchSymbol = true;
        prepared.symbolLanguage = symIt->second;
    }

    // Entity extraction: check if any provider supports this extension
    prepared.shouldDispatchEntity =
        extensionSupportsEntityProviders(entityProviders, prepared.extension);

    return prepared;
}

void PostIngestQueue::processKnowledgeGraphStage(const std::string& hash, int64_t docId,
                                                 const std::string& filePath,
                                                 const std::vector<std::string>& tags) {
    if (!graphComponent_) {
        spdlog::warn("[PostIngestQueue] KG stage skipped for {} - no graphComponent", hash);
        return;
    }

    spdlog::info("[PostIngestQueue] KG stage starting for {} ({})", filePath, hash.substr(0, 12));

    try {
        auto startTime = std::chrono::steady_clock::now();

        GraphComponent::DocumentGraphContext ctx{
            .documentHash = hash, .filePath = filePath, .tags = tags, .documentDbId = docId};

        auto result = graphComponent_->onDocumentIngested(ctx);
        if (!result) {
            spdlog::warn("[PostIngestQueue] Graph ingestion failed for {}: {}", hash,
                         result.error().message);
        } else {
            auto duration = std::chrono::steady_clock::now() - startTime;
            double ms = std::chrono::duration<double, std::milli>(duration).count();
            spdlog::debug("[PostIngestQueue] KG stage completed for {} in {:.2f}ms", hash, ms);
        }
        InternalEventBus::instance().incKgConsumed();
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] KG stage failed for {}: {}", hash, e.what());
    }
}

void PostIngestQueue::processEmbeddingBatch(const std::vector<std::string>& hashes) {
    if (hashes.empty()) {
        return;
    }

    try {
        // PBI-05b: Use TuneAdvisor for channel capacity
        const std::size_t capacity = TuneAdvisor::embedChannelCapacity();
        auto embedChannel =
            InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
                "embed_jobs", capacity);

        if (!embedChannel) {
            for (std::size_t i = 0; i < hashes.size(); ++i) {
                InternalEventBus::instance().incEmbedDropped();
            }
            spdlog::warn("[PostIngestQueue] Embed channel unavailable for batch of {} hashes",
                         hashes.size());
            return;
        }

        InternalEventBus::EmbedJob job;
        job.hashes = hashes;
        job.batchSize = static_cast<uint32_t>(hashes.size());
        job.skipExisting = true;
        job.modelName = "";

        // PBI-05b: Use blocking push with backpressure to prevent embed job drops.
        // Wait up to 5 seconds for space - this creates backpressure on ingest when
        // embedding can't keep up, which is better than silently dropping jobs.
        constexpr auto kEmbedPushTimeout = std::chrono::seconds(5);
        if (!embedChannel->push_wait(std::move(job), kEmbedPushTimeout)) {
            for (std::size_t i = 0; i < hashes.size(); ++i) {
                InternalEventBus::instance().incEmbedDropped();
            }
            spdlog::warn("[PostIngestQueue] Embed channel full after {}s backpressure, dropping "
                         "batch of {} hashes",
                         kEmbedPushTimeout.count(), hashes.size());
        } else {
            for (std::size_t i = 0; i < hashes.size(); ++i) {
                InternalEventBus::instance().incEmbedQueued();
            }
            spdlog::debug("[PostIngestQueue] Dispatched embedding job for {} hashes",
                          hashes.size());
        }
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] Embedding batch dispatch failed: {}", e.what());
    }
}

void PostIngestQueue::dispatchToKgChannel(const std::string& hash, int64_t docId,
                                          const std::string& filePath,
                                          std::vector<std::string> tags) {
    constexpr std::size_t kgChannelCapacity = 16384;
    auto channel = InternalEventBus::instance().get_or_create_channel<InternalEventBus::KgJob>(
        "kg_jobs", kgChannelCapacity);

    InternalEventBus::KgJob job;
    job.hash = hash;
    job.documentId = docId;
    job.filePath = filePath;
    job.tags = std::move(tags);

    if (!channel->try_push(std::move(job))) {
        spdlog::warn("[PostIngestQueue] KG channel full, dropping job for {}", hash);
        InternalEventBus::instance().incKgDropped();
    } else {
        spdlog::info("[PostIngestQueue] Dispatched KG job for {} ({})", filePath,
                     hash.substr(0, 12));
        InternalEventBus::instance().incKgQueued();
    }
}

boost::asio::awaitable<void> PostIngestQueue::kgPoller() {
    constexpr std::size_t kgChannelCapacity = 16384;
    auto channel = InternalEventBus::instance().get_or_create_channel<InternalEventBus::KgJob>(
        "kg_jobs", kgChannelCapacity);

    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    kgStarted_.store(true);

    auto idleDelay = std::chrono::milliseconds(1);
    constexpr auto kMaxIdleDelay = std::chrono::milliseconds(10);

    while (!stop_.load()) {
        bool didWork = false;
        InternalEventBus::KgJob job;
        // Dynamic concurrency limit from TuneAdvisor
        const std::size_t maxConcurrent = maxKgConcurrent();
        while (kgInFlight_.load() < maxConcurrent && channel->try_pop(job)) {
            didWork = true;
            wasActive_.store(true, std::memory_order_release);
            kgInFlight_.fetch_add(1);
            boost::asio::post(coordinator_->getExecutor(),
                              [this, hash = std::move(job.hash), docId = job.documentId,
                               filePath = std::move(job.filePath), tags = std::move(job.tags)]() {
                                  processKnowledgeGraphStage(hash, docId, filePath, tags);
                                  kgInFlight_.fetch_sub(1);
                                  checkDrainAndSignal();
                              });
        }

        if (didWork) {
            idleDelay = std::chrono::milliseconds(1);
            continue;
        }

        timer.expires_after(idleDelay);
        co_await timer.async_wait(boost::asio::use_awaitable);
        if (idleDelay < kMaxIdleDelay) {
            idleDelay *= 2;
        }
    }

    spdlog::info("[PostIngestQueue] KG poller exited");
}

boost::asio::awaitable<void> PostIngestQueue::symbolPoller() {
    constexpr std::size_t symbolChannelCapacity = 16384;
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::SymbolExtractionJob>(
            "symbol_extraction", symbolChannelCapacity);

    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    symbolStarted_.store(true);
    spdlog::info("[PostIngestQueue] Symbol extraction poller started");

    auto idleDelay = std::chrono::milliseconds(1);
    constexpr auto kMaxIdleDelay = std::chrono::milliseconds(10);

    while (!stop_.load()) {
        bool didWork = false;
        InternalEventBus::SymbolExtractionJob job;
        // Dynamic concurrency limit from TuneAdvisor
        const std::size_t maxConcurrent = maxSymbolConcurrent();
        while (symbolInFlight_.load() < maxConcurrent && channel->try_pop(job)) {
            didWork = true;
            wasActive_.store(true, std::memory_order_release);
            symbolInFlight_.fetch_add(1);
            boost::asio::post(coordinator_->getExecutor(),
                              [this, hash = std::move(job.hash), docId = job.documentId,
                               filePath = std::move(job.filePath),
                               language = std::move(job.language)]() {
                                  processSymbolExtractionStage(hash, docId, filePath, language);
                                  symbolInFlight_.fetch_sub(1);
                                  checkDrainAndSignal();
                              });
        }

        if (didWork) {
            idleDelay = std::chrono::milliseconds(1);
            continue;
        }

        timer.expires_after(idleDelay);
        co_await timer.async_wait(boost::asio::use_awaitable);
        if (idleDelay < kMaxIdleDelay) {
            idleDelay *= 2;
        }
    }

    spdlog::info("[PostIngestQueue] Symbol extraction poller exited");
}

void PostIngestQueue::dispatchToSymbolChannel(const std::string& hash, int64_t docId,
                                              const std::string& filePath,
                                              const std::string& language) {
    constexpr std::size_t symbolChannelCapacity = 16384;
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::SymbolExtractionJob>(
            "symbol_extraction", symbolChannelCapacity);

    InternalEventBus::SymbolExtractionJob job;
    job.hash = hash;
    job.documentId = docId;
    job.filePath = filePath;
    job.language = language;

    if (!channel->try_push(std::move(job))) {
        spdlog::warn("[PostIngestQueue] Symbol channel full, dropping job for {}", hash);
        InternalEventBus::instance().incSymbolDropped();
    } else {
        spdlog::info("[PostIngestQueue] Dispatched symbol extraction job for {} ({}) lang={}",
                     filePath, hash.substr(0, 12), language);
        InternalEventBus::instance().incSymbolQueued();
    }
}

void PostIngestQueue::processSymbolExtractionStage(const std::string& hash,
                                                   [[maybe_unused]] int64_t docId,
                                                   const std::string& filePath,
                                                   const std::string& language) {
    if (!graphComponent_) {
        spdlog::warn("[PostIngestQueue] Symbol extraction skipped for {} - no graphComponent",
                     hash);
        return;
    }

    spdlog::info("[PostIngestQueue] Symbol extraction starting for {} ({}) lang={}", filePath,
                 hash.substr(0, 12), language);

    try {
        auto startTime = std::chrono::steady_clock::now();

        // Use GraphComponent to submit the extraction job
        GraphComponent::EntityExtractionJob extractJob;
        extractJob.documentHash = hash;
        extractJob.filePath = filePath;
        extractJob.language = language;

        // Load content from store
        if (store_) {
            auto contentResult = store_->retrieveBytes(hash);
            if (contentResult) {
                const auto& bytes = contentResult.value();
                extractJob.contentUtf8 =
                    std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
            } else {
                spdlog::warn("[PostIngestQueue] Failed to load content for symbol extraction: {}",
                             hash.substr(0, 12));
                return;
            }
        } else {
            spdlog::warn("[PostIngestQueue] No content store for symbol extraction");
            return;
        }

        auto result = graphComponent_->submitEntityExtraction(std::move(extractJob));
        if (!result) {
            spdlog::warn("[PostIngestQueue] Symbol extraction failed for {}: {}", hash,
                         result.error().message);
        } else {
            auto duration = std::chrono::steady_clock::now() - startTime;
            double ms = std::chrono::duration<double, std::milli>(duration).count();
            spdlog::debug("[PostIngestQueue] Symbol extraction submitted for {} in {:.2f}ms", hash,
                          ms);
        }
        InternalEventBus::instance().incSymbolConsumed();
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] Symbol extraction failed for {}: {}", hash, e.what());
    }
}

void PostIngestQueue::dispatchToEntityChannel(const std::string& hash, int64_t docId,
                                              const std::string& filePath,
                                              const std::string& extension) {
    constexpr std::size_t entityChannelCapacity = 4096;
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::EntityExtractionJob>(
            "entity_extraction", entityChannelCapacity);

    InternalEventBus::EntityExtractionJob job;
    job.hash = hash;
    job.documentId = docId;
    job.filePath = filePath;
    job.extension = extension;

    if (!channel->try_push(std::move(job))) {
        spdlog::warn("[PostIngestQueue] Entity channel full, dropping job for {}", hash);
        InternalEventBus::instance().incEntityDropped();
    } else {
        spdlog::info("[PostIngestQueue] Dispatched entity extraction job for {} ({}) ext={}",
                     filePath, hash.substr(0, 12), extension);
        InternalEventBus::instance().incEntityQueued();
    }
}

boost::asio::awaitable<void> PostIngestQueue::entityPoller() {
    constexpr std::size_t entityChannelCapacity = 4096;
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::EntityExtractionJob>(
            "entity_extraction", entityChannelCapacity);

    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    entityStarted_.store(true);
    spdlog::info("[PostIngestQueue] Entity extraction poller started");

    auto idleDelay = std::chrono::milliseconds(1);
    constexpr auto kMaxIdleDelay = std::chrono::milliseconds(10);

    while (!stop_.load()) {
        bool didWork = false;
        InternalEventBus::EntityExtractionJob job;
        // Dynamic concurrency limit from TuneAdvisor
        const std::size_t maxConcurrent = maxEntityConcurrent();
        while (entityInFlight_.load() < maxConcurrent && channel->try_pop(job)) {
            didWork = true;
            wasActive_.store(true, std::memory_order_release);
            entityInFlight_.fetch_add(1);
            auto entityExec = entityCoordinator_ ? entityCoordinator_->getExecutor()
                                                 : coordinator_->getExecutor();
            boost::asio::post(entityExec, [this, hash = std::move(job.hash), docId = job.documentId,
                                           filePath = std::move(job.filePath),
                                           extension = std::move(job.extension)]() {
                processEntityExtractionStage(hash, docId, filePath, extension);
                entityInFlight_.fetch_sub(1);
                checkDrainAndSignal();
            });
        }

        if (didWork) {
            idleDelay = std::chrono::milliseconds(1);
            continue;
        }

        timer.expires_after(idleDelay);
        co_await timer.async_wait(boost::asio::use_awaitable);
        if (idleDelay < kMaxIdleDelay) {
            idleDelay *= 2;
        }
    }

    spdlog::info("[PostIngestQueue] Entity extraction poller exited");
}

void PostIngestQueue::processEntityExtractionStage(const std::string& hash, int64_t /*docId*/,
                                                   const std::string& filePath,
                                                   const std::string& extension) {
    spdlog::info("[PostIngestQueue] Entity extraction starting for {} ({}) ext={}", filePath,
                 hash.substr(0, 12), extension);

    try {
        auto startTime = std::chrono::steady_clock::now();

        // Find the entity provider that supports this extension
        std::shared_ptr<ExternalEntityProviderAdapter> provider;
        {
            std::lock_guard<std::mutex> lock(entityMutex_);
            for (const auto& p : entityProviders_) {
                if (p && p->supports(extension)) {
                    provider = p;
                    break;
                }
            }
        }

        if (!provider) {
            spdlog::warn("[PostIngestQueue] No entity provider for extension {}", extension);
            return;
        }

        // Load content from store
        std::vector<std::byte> content;
        if (store_) {
            auto contentResult = store_->retrieveBytes(hash);
            if (contentResult) {
                content = std::move(contentResult.value());
            } else {
                spdlog::warn("[PostIngestQueue] Failed to load content for entity extraction: {}",
                             hash.substr(0, 12));
                return;
            }
        } else {
            spdlog::warn("[PostIngestQueue] No content store for entity extraction");
            return;
        }

        if (!kg_) {
            spdlog::warn("[PostIngestQueue] No KG store for entity extraction");
            return;
        }

        // Track cumulative nodeKey -> nodeId mappings across batches
        // This allows edges to reference nodes from previous batches
        std::unordered_map<std::string, std::int64_t> canonicalKeyToId;
        std::unordered_map<std::string, std::int64_t> versionKeyToId;
        size_t totalNodesInserted = 0;
        size_t totalEdgesInserted = 0;
        size_t totalAliasesInserted = 0;
        const std::string snapshotId = hash;

        // NOTE: Entity embeddings (entity_vectors table) are intentionally NOT generated here.
        // The KG nodes/edges/aliases provide precise structural navigation (call graphs,
        // inheritance, containment). Embeddings would add noise for code navigation where
        // exact matches and graph traversal are preferred. The entity_vectors schema exists
        // for future semantic search use cases (e.g., "find similar functions").

        // Use streaming extraction with per-batch KG insertion
        auto result = provider->extractEntitiesStreaming(
            content, filePath,
            [this, &canonicalKeyToId, &versionKeyToId, &totalNodesInserted, &totalEdgesInserted,
             &totalAliasesInserted, &hash, &snapshotId,
             &filePath](ExternalEntityProviderAdapter::EntityResult batch,
                        const ExternalEntityProviderAdapter::ExtractionProgress& progress) -> bool {
                if (batch.nodes.empty()) {
                    return true; // Continue to next batch
                }

                // Wrap all KG operations in a single WriteBatch transaction
                auto batchRes = kg_->beginWriteBatch();
                if (!batchRes) {
                    spdlog::warn("[PostIngestQueue] Failed to begin WriteBatch: {}",
                                 batchRes.error().message);
                    return true; // Continue despite error
                }
                auto& kgBatch = batchRes.value();

                const bool hasSnapshot = !snapshotId.empty();
                std::vector<metadata::KGNode> canonicalNodes;
                std::vector<metadata::KGNode> versionNodes;
                canonicalNodes.reserve(batch.nodes.size());
                versionNodes.reserve(batch.nodes.size());

                for (const auto& node : batch.nodes) {
                    canonicalNodes.push_back(node);

                    if (hasSnapshot) {
                        metadata::KGNode versionNode = node;
                        std::string baseKey = node.nodeKey;
                        versionNode.nodeKey = baseKey + "@snap:" + snapshotId;
                        std::string baseType = node.type.has_value() ? node.type.value() : "entity";
                        versionNode.type = baseType + "_version";

                        nlohmann::json props = nlohmann::json::object();
                        if (node.properties.has_value()) {
                            try {
                                props = nlohmann::json::parse(node.properties.value());
                            } catch (...) {
                                props = nlohmann::json::object();
                            }
                        }
                        props["snapshot_id"] = snapshotId;
                        props["document_hash"] = snapshotId;
                        props["file_path"] = filePath;
                        props["canonical_key"] = baseKey;
                        versionNode.properties = props.dump();
                        versionNodes.push_back(std::move(versionNode));
                    }
                }

                // Insert canonical nodes and get their IDs
                auto canonicalIds = kgBatch->upsertNodes(canonicalNodes);
                if (!canonicalIds) {
                    spdlog::warn("[PostIngestQueue] Failed to insert batch {} nodes: {}",
                                 progress.batchNumber, canonicalIds.error().message);
                    return true; // Continue despite error - partial success
                }

                // Update key maps with this batch's nodes
                for (size_t i = 0; i < canonicalNodes.size() && i < canonicalIds.value().size();
                     ++i) {
                    canonicalKeyToId[canonicalNodes[i].nodeKey] = canonicalIds.value()[i];
                }
                if (hasSnapshot) {
                    // Insert version nodes and get their IDs
                    auto versionIds = kgBatch->upsertNodes(versionNodes);
                    if (!versionIds) {
                        spdlog::warn(
                            "[PostIngestQueue] Failed to insert batch {} version nodes: {}",
                            progress.batchNumber, versionIds.error().message);
                        return true; // Continue despite error - partial success
                    }
                    for (size_t i = 0; i < versionNodes.size() && i < versionIds.value().size();
                         ++i) {
                        versionKeyToId[canonicalNodes[i].nodeKey] = versionIds.value()[i];
                    }
                    totalNodesInserted += versionIds.value().size();

                    // Link canonical to version nodes
                    std::vector<metadata::KGEdge> observedEdges;
                    observedEdges.reserve(versionNodes.size());
                    for (size_t i = 0;
                         i < canonicalNodes.size() && i < canonicalIds.value().size() &&
                         i < versionIds.value().size();
                         ++i) {
                        metadata::KGEdge edge;
                        edge.srcNodeId = canonicalIds.value()[i];
                        edge.dstNodeId = versionIds.value()[i];
                        edge.relation = "observed_as";
                        edge.weight = 1.0f;
                        nlohmann::json props;
                        props["snapshot_id"] = snapshotId;
                        props["document_hash"] = snapshotId;
                        edge.properties = props.dump();
                        observedEdges.push_back(std::move(edge));
                    }
                    if (!observedEdges.empty()) {
                        kgBatch->addEdgesUnique(observedEdges);
                    }
                } else {
                    for (size_t i = 0; i < canonicalNodes.size() && i < canonicalIds.value().size();
                         ++i) {
                        versionKeyToId[canonicalNodes[i].nodeKey] = canonicalIds.value()[i];
                    }
                    totalNodesInserted += canonicalIds.value().size();
                }

                // Resolve and insert edges
                std::vector<metadata::KGEdge> resolvedEdges;
                for (auto& edge : batch.edges) {
                    try {
                        if (!edge.properties)
                            continue;
                        auto props = nlohmann::json::parse(*edge.properties);
                        std::string srcKey = props.value("_src_key", "");
                        std::string dstKey = props.value("_dst_key", "");

                        auto srcIt = versionKeyToId.find(srcKey);
                        auto dstIt = versionKeyToId.find(dstKey);

                        if (srcIt != versionKeyToId.end() && dstIt != versionKeyToId.end()) {
                            edge.srcNodeId = srcIt->second;
                            edge.dstNodeId = dstIt->second;
                            props.erase("_src_key");
                            props.erase("_dst_key");
                            edge.properties = props.dump();
                            resolvedEdges.push_back(std::move(edge));
                        }
                    } catch (...) {
                        // Skip edges we can't parse
                    }
                }

                if (!resolvedEdges.empty()) {
                    kgBatch->addEdgesUnique(resolvedEdges);
                    totalEdgesInserted += resolvedEdges.size();
                }

                // Resolve and insert aliases
                std::vector<metadata::KGAlias> resolvedAliases;
                for (auto& alias : batch.aliases) {
                    if (alias.source && alias.source->starts_with("_node_key:")) {
                        std::string nodeKey = alias.source->substr(10);
                        auto it = canonicalKeyToId.find(nodeKey);
                        if (it != canonicalKeyToId.end()) {
                            alias.nodeId = it->second;
                            alias.source = "ghidra";
                            resolvedAliases.push_back(std::move(alias));
                        }
                    }
                }

                if (!resolvedAliases.empty()) {
                    kgBatch->addAliases(resolvedAliases);
                    totalAliasesInserted += resolvedAliases.size();
                }

                // Commit the WriteBatch transaction
                auto commitRes = kgBatch->commit();
                if (!commitRes) {
                    spdlog::warn("[PostIngestQueue] Failed to commit WriteBatch: {}",
                                 commitRes.error().message);
                }

                const size_t batchNodesInserted =
                    hasSnapshot ? versionNodes.size() : canonicalNodes.size();
                spdlog::info("[PostIngestQueue] Batch {}/{} ingested for {} "
                             "(nodes={}, edges={}, aliases={}, elapsed={:.1f}s)",
                             progress.batchNumber, progress.totalBatchesEstimate,
                             hash.substr(0, 12), batchNodesInserted, resolvedEdges.size(),
                             resolvedAliases.size(), progress.elapsedSeconds);

                return true; // Continue to next batch
            });

        auto duration = std::chrono::steady_clock::now() - startTime;
        double ms = std::chrono::duration<double, std::milli>(duration).count();

        if (result) {
            spdlog::info("[PostIngestQueue] Entity extraction completed for {} in {:.2f}ms "
                         "(batches={}, nodes={}, edges={}, aliases={})",
                         hash.substr(0, 12), ms, result.value().batchNumber, totalNodesInserted,
                         totalEdgesInserted, totalAliasesInserted);
        } else {
            // Partial success - some batches may have been ingested
            if (totalNodesInserted > 0) {
                spdlog::warn("[PostIngestQueue] Entity extraction partial success for {} "
                             "(nodes={}, edges={}, aliases={}, error={})",
                             hash.substr(0, 12), totalNodesInserted, totalEdgesInserted,
                             totalAliasesInserted, result.error().message);
            } else {
                spdlog::warn("[PostIngestQueue] Entity extraction failed for {}: {}",
                             hash.substr(0, 12), result.error().message);
            }
        }

        InternalEventBus::instance().incEntityConsumed();
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] Entity extraction failed for {}: {}", hash, e.what());
    }
}

} // namespace yams::daemon
