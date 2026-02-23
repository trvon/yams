#include <yams/daemon/components/RepairService.h>

#include <yams/compat/thread_stop_compat.h>
#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/content_extractor.h>
#include <yams/extraction/extraction_util.h>
#include <yams/integrity/repair_manager.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_database.h>

#include <yams/core/repair_fsm.h>
#include <yams/daemon/components/ConfigResolver.h>

#include <spdlog/spdlog.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/thread_pool.hpp>

#include <sqlite3.h>
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace yams::daemon {

namespace {

constexpr size_t kMaxTextToPersistInMetadataBytes = 16 * 1024 * 1024; // 16 MiB (best-effort)

// Check if vector operations are disabled via environment variables
bool vectorsDisabledByEnv() {
    if (const char* env = std::getenv("YAMS_DISABLE_VECTORS"); env && *env)
        return true;
    if (const char* env = std::getenv("YAMS_DISABLE_VECTOR_DB"); env && *env)
        return true;
    return false;
}

// Shared thread pool for RepairService background coroutines
struct RepairThreadPool {
    std::unique_ptr<boost::asio::thread_pool> pool_;
    std::once_flag init_flag_;

    static RepairThreadPool& instance() {
        static RepairThreadPool inst;
        return inst;
    }

    boost::asio::any_io_executor get_executor() {
        std::call_once(init_flag_, [this]() {
            uint32_t threads = TuneAdvisor::repairTokensIdle();
            if (threads < 1)
                threads = 1;
            threads = std::max(threads + 1, 2u);
            pool_ = std::make_unique<boost::asio::thread_pool>(threads);
            spdlog::debug("[RepairThreadPool] Initialized with {} threads", threads);
        });
        return pool_->get_executor();
    }

    ~RepairThreadPool() = default;

private:
    RepairThreadPool() = default;
};

// Build extension-to-language map from loaded symbol extractor plugins
std::unordered_map<std::string, std::string> buildExtensionLanguageMap(
    const std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>>& extractors) {
    std::unordered_map<std::string, std::string> result;
    for (const auto& extractor : extractors) {
        if (!extractor || !extractor->table())
            continue;
        auto supported = extractor->getSupportedExtensions();
        for (const auto& [ext, lang] : supported)
            result[ext] = lang;
    }
    return result;
}

// Check if a document is extractable based on MIME type, extension, and plugins
bool canExtractDocument(
    const std::string& mimeType, const std::string& extension,
    const std::vector<std::shared_ptr<extraction::IContentExtractor>>& customExtractors,
    const std::shared_ptr<yams::api::IContentStore>& contentStore, const std::string& hash) {
    for (const auto& extractor : customExtractors) {
        if (extractor && extractor->supports(mimeType, extension))
            return true;
    }
    auto& detector = yams::detection::FileTypeDetector::instance();
    if (!mimeType.empty() && detector.isTextMimeType(mimeType))
        return true;
    if (mimeType.empty() || mimeType == "application/octet-stream" ||
        mimeType == "application/x-octet-stream") {
        if (!extension.empty()) {
            auto detectedMime =
                yams::detection::FileTypeDetector::getMimeTypeFromExtension(extension);
            if (!detectedMime.empty() && detector.isTextMimeType(detectedMime))
                return true;
        }
        if (contentStore) {
            auto bytesRes = contentStore->retrieveBytes(hash);
            if (bytesRes) {
                const auto& bytes = bytesRes.value();
                size_t checkSize = std::min(bytes.size(), size_t(8192));
                std::span<const std::byte> sample(bytes.data(), checkSize);
                if (!yams::detection::isBinaryData(sample))
                    return true;
            }
        }
    }
    return false;
}

// Template helper: queue job with exponential backoff when full
template <typename JobT>
boost::asio::awaitable<bool> queueWithBackoff(std::shared_ptr<SpscQueue<JobT>> queue, JobT&& job,
                                              boost::asio::steady_timer& timer,
                                              std::atomic<bool>& running,
                                              const std::string& jobTypeName, size_t jobSize,
                                              int initialDelayMs = 50, int maxDelayMs = 1000) {
    int retries = 0;
    while (!queue->try_push(std::move(job))) {
        if (!running.load(std::memory_order_relaxed))
            co_return false;
        int delayMs = std::min(initialDelayMs * (1 << retries), maxDelayMs);
        timer.expires_after(std::chrono::milliseconds(delayMs));
        co_await timer.async_wait(boost::asio::use_awaitable);
        retries++;
        if (retries == 1) {
            spdlog::debug("RepairService: {} queue full, waiting (batch size={})", jobTypeName,
                          jobSize);
        }
    }
    if (retries > 0) {
        spdlog::debug("RepairService: queued {} job after {} retries ({} docs)", jobTypeName,
                      retries, jobSize);
    }
    co_return true;
}

} // namespace

// ============================================================================
// Construction / Destruction / Lifecycle
// ============================================================================

RepairService::RepairService(ServiceManager* services, StateComponent* state,
                             std::function<size_t()> activeConnFn, Config cfg)
    : services_(services), state_(state), activeConnFn_(std::move(activeConnFn)),
      cfg_(std::move(cfg)), shutdownState_(std::make_shared<ShutdownState>()) {}

RepairService::~RepairService() {
    stop();
}

std::shared_ptr<metadata::IMetadataRepository> RepairService::getMetadataRepoForRepair() const {
    if (!services_)
        return nullptr;
    return std::static_pointer_cast<metadata::IMetadataRepository>(services_->getMetadataRepo());
}

std::shared_ptr<GraphComponent> RepairService::getGraphComponentForScheduling() const {
    return services_ ? services_->getGraphComponent() : nullptr;
}

std::shared_ptr<metadata::KnowledgeGraphStore> RepairService::getKgStoreForScheduling() const {
    return services_ ? services_->getKgStore() : nullptr;
}

const std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>>&
RepairService::getSymbolExtractorsForScheduling() const {
    static const std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>> kEmpty;
    return services_ ? services_->getSymbolExtractors() : kEmpty;
}

void RepairService::start() {
    if (!cfg_.enable || running_.exchange(true))
        return;
    if (state_) {
        state_->stats.repairRunning.store(true, std::memory_order_relaxed);
    }
    tokens_.store(cfg_.maintenanceTokens);
    shutdownState_->finished.store(false, std::memory_order_relaxed);
    shutdownState_->running.store(true, std::memory_order_relaxed);
    shutdownState_->config = cfg_;

    auto exec = RepairThreadPool::instance().get_executor();
    auto shutdownState = shutdownState_;
    auto* self = this;

    boost::asio::co_spawn(
        exec,
        [self, shutdownState]() -> boost::asio::awaitable<void> {
            spdlog::debug("RepairService coroutine starting");
            try {
                if (!shutdownState->running.load(std::memory_order_acquire))
                    co_return;
                co_await self->backgroundLoop(shutdownState);
            } catch (const std::exception& e) {
                spdlog::error("RepairService coroutine exception: {}", e.what());
            }
        },
        [shutdownState](const std::exception_ptr& eptr) {
            if (eptr) {
                try {
                    std::rethrow_exception(eptr);
                } catch (const std::exception& e) {
                    spdlog::error("RepairService completion handler exception: {}", e.what());
                }
            }
            shutdownState->finished.store(true, std::memory_order_release);
            shutdownState->cv.notify_all();
        });

    spdlog::debug("RepairService started (enable={}, batch={})", cfg_.enable, cfg_.maxBatch);

    // Enqueue initial PathTreeRepair job
    static auto pathTreeQueue =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PathTreeJob>(
            "path_tree_repair_jobs", 32);
    InternalEventBus::PathTreeJob initialJob{1, true};
    (void)pathTreeQueue->try_push(initialJob);
}

void RepairService::stop() {
    if (!running_.exchange(false))
        return;
    if (state_) {
        state_->stats.repairRunning.store(false, std::memory_order_relaxed);
        state_->stats.repairInProgress.store(false, std::memory_order_relaxed);
    }
    shutdownState_->running.store(false, std::memory_order_release);
    queueCv_.notify_all();
    {
        std::unique_lock<std::mutex> lk(shutdownState_->mutex);
        shutdownState_->cv.wait_for(lk, std::chrono::milliseconds(2000),
                                    [this] { return shutdownState_->finished.load(); });
    }
    spdlog::debug("RepairService stopped");
}

bool RepairService::maintenanceAllowed() const {
    if (!activeConnFn_)
        return false;
    return activeConnFn_() == 0;
}

// ============================================================================
// Event-driven interface
// ============================================================================

void RepairService::onDocumentAdded(const DocumentAddedEvent& event) {
    if (!cfg_.enable || !running_)
        return;

    // If PostIngestQueue is handling this document, skip
    if (services_) {
        auto piq = services_->getPostIngestQueue();
        if (piq && piq->started()) {
            spdlog::debug("RepairService: skipping DocumentAdded {} -- handled by PostIngestQueue",
                          event.hash);
            return;
        }
    }

    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        if (pendingSet_.find(event.hash) == pendingSet_.end()) {
            pendingSet_.insert(event.hash);
            pendingDocuments_.push(event.hash);
        }
        if (state_)
            state_->stats.repairQueueDepth.store(static_cast<uint64_t>(pendingDocuments_.size()));
    }
    queueCv_.notify_one();

    // Opportunistically schedule symbol/entity extraction
    try {
        if (services_) {
            auto gc = getGraphComponentForScheduling();
            const auto& symbolExtractors = getSymbolExtractorsForScheduling();
            if (gc && !symbolExtractors.empty()) {
                static thread_local std::unordered_map<std::string, std::string> extToLang;
                static thread_local bool mapInitialized = false;
                if (!mapInitialized) {
                    extToLang = buildExtensionLanguageMap(symbolExtractors);
                    mapInitialized = true;
                }
                std::string extension;
                auto dotPos = event.path.find_last_of('.');
                if (dotPos != std::string::npos)
                    extension = event.path.substr(dotPos + 1);
                std::string dottedExt;
                if (!extension.empty())
                    dottedExt = "." + extension;
                auto it = extToLang.find(extension);
                if (it == extToLang.end() && !dottedExt.empty())
                    it = extToLang.find(dottedExt);
                if (it != extToLang.end()) {
                    auto kg = getKgStoreForScheduling();
                    std::optional<std::int64_t> docId;
                    if (kg) {
                        auto idRes = kg->getDocumentIdByHash(event.hash);
                        if (idRes.has_value())
                            docId = idRes.value();
                    }
                    bool alreadyExtracted = false;
                    if (kg && docId.has_value()) {
                        auto entRes = kg->getDocEntitiesForDocument(docId.value(), 1, 0);
                        if (entRes.has_value() && !entRes.value().empty())
                            alreadyExtracted = true;
                    }
                    if (!alreadyExtracted) {
                        GraphComponent::EntityExtractionJob j;
                        j.documentHash = event.hash;
                        j.filePath = event.path;
                        j.language = it->second;
                        (void)gc->submitEntityExtraction(std::move(j));
                    }
                }
            }
        }
    } catch (const std::exception& e) {
        spdlog::warn("RepairService: symbol extraction scheduling failed: {}", e.what());
    } catch (...) {
    }
}

void RepairService::onDocumentRemoved(const DocumentRemovedEvent& event) {
    if (!cfg_.enable || !running_)
        return;
    spdlog::debug("RepairService: document {} removed", event.hash);
}

void RepairService::enqueueEmbeddingRepair(const std::vector<std::string>& hashes) {
    if (!cfg_.enable || !running_ || hashes.empty())
        return;
    size_t enqueuedCount = 0;
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        for (const auto& hash : hashes) {
            if (pendingSet_.find(hash) == pendingSet_.end()) {
                pendingSet_.insert(hash);
                pendingDocuments_.push(hash);
                ++enqueuedCount;
            }
        }
        if (state_)
            state_->stats.repairQueueDepth.store(static_cast<uint64_t>(pendingDocuments_.size()));
    }
    queueCv_.notify_one();
    spdlog::debug("RepairService: queued {} docs for embedding repair", enqueuedCount);
}

// ============================================================================
// Background Loop (ported from RepairCoordinator::runAsync)
// ============================================================================

boost::asio::awaitable<void>
RepairService::backgroundLoop(std::shared_ptr<ShutdownState> shutdownState) {
    using namespace std::chrono_literals;
    auto ex = co_await boost::asio::this_coro::executor;
    boost::asio::steady_timer timer(ex);

    core::RepairFsm::Config fsmConfig;
    fsmConfig.enable_online_repair = true;
    fsmConfig.max_repair_concurrency = shutdownState->config.maintenanceTokens;
    fsmConfig.repair_backoff_ms = 250;
    fsmConfig.max_retries = static_cast<uint32_t>(shutdownState->config.maxRetries);

    core::RepairFsm fsm(fsmConfig);
    fsm.set_on_state_change([](core::RepairFsm::State state) {
        spdlog::debug("RepairFsm state: {}", core::RepairFsm::to_string(state));
    });

    bool initialScanEnqueued = false;
    bool vectorCleanupDone = false;
    int deferTicks = 0;
    const int minDeferTicks = 50;

    static auto pruneQueue =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PruneJob>("prune_jobs",
                                                                                       128);
    static auto pathTreeQueue =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PathTreeJob>(
            "path_tree_repair_jobs", 32);

    bool pathTreeRepairDone = false;

    while (running_.load(std::memory_order_relaxed) &&
           shutdownState_->running.load(std::memory_order_acquire)) {
        core::RepairFsm::SchedulingHints hints;
        hints.closing = !running_.load(std::memory_order_relaxed) ||
                        !shutdownState_->running.load(std::memory_order_acquire);
        hints.maintenance_allowed = maintenanceAllowed();
        fsm.set_scheduling_hints(hints);

        if (fsm.hints().closing)
            break;

        bool didWork = false;

        // Process prune jobs
        InternalEventBus::PruneJob pruneJob;
        if (pruneQueue->try_pop(pruneJob)) {
            didWork = true;
            InternalEventBus::instance().incPostConsumed();
            spdlog::debug("RepairService: prune job {} needs storage engine refactor",
                          pruneJob.requestId);
        }

        // Process PathTreeRepair jobs
        if (!pathTreeRepairDone) {
            InternalEventBus::PathTreeJob pathTreeJob;
            if (pathTreeQueue->try_pop(pathTreeJob)) {
                didWork = true;
                InternalEventBus::instance().incPostConsumed();
                co_await processPathTreeRepair();
                pathTreeRepairDone = true;
            }
        }

        // Idle backoff
        if (!didWork) {
            auto snap = TuningSnapshotRegistry::instance().get();
            uint32_t pollMs = snap ? snap->workerPollMs : TuneAdvisor::workerPollMs();
            timer.expires_after(std::chrono::milliseconds(std::max<uint32_t>(10, pollMs)));
            co_await timer.async_wait(boost::asio::use_awaitable);
        }

        if (!vectorCleanupDone && maintenanceAllowed()) {
            performVectorCleanup();
            vectorCleanupDone = true;
        }

        // Deferred initial scan
        bool queueEmpty;
        {
            std::lock_guard<std::mutex> lk(queueMutex_);
            queueEmpty = pendingDocuments_.empty();
        }
        if (queueEmpty && !initialScanEnqueued) {
            if (deferTicks < minDeferTicks) {
                ++deferTicks;
            } else if (maintenanceAllowed()) {
                auto scanExec = RepairThreadPool::instance().get_executor();
                boost::asio::co_spawn(
                    scanExec,
                    [this]() -> boost::asio::awaitable<void> { co_await spawnInitialScan(); },
                    boost::asio::detached);
                initialScanEnqueued = true;
            }
        }

        // Drain pending batch
        std::vector<std::string> batch;
        {
            std::lock_guard<std::mutex> lk(queueMutex_);
            while (!pendingDocuments_.empty() && batch.size() < shutdownState->config.maxBatch) {
                auto hash = std::move(pendingDocuments_.front());
                pendingDocuments_.pop();
                pendingSet_.erase(hash);
                batch.push_back(std::move(hash));
            }
            if (state_)
                state_->stats.repairQueueDepth.store(
                    static_cast<uint64_t>(pendingDocuments_.size()));
        }
        if (batch.empty()) {
            timer.expires_after(100ms);
            co_await timer.async_wait(boost::asio::use_awaitable);
            continue;
        }

        if (!running_.load(std::memory_order_relaxed))
            continue;

        if (state_)
            state_->stats.repairIdleTicks++;

        auto [missingEmbeddings, missingFts5] = detectMissingWork(batch);
        auto meta_repo = services_ ? services_->getMetadataRepo() : nullptr;

        // Check ResourceGovernor before queuing repair work.
        // Under elevated pressure the ingestion pipeline is already competing for
        // embed/FTS5 channel capacity; repair should yield to avoid amplifying
        // the pressure that caused the governor to throttle in the first place.
        auto& governor = ResourceGovernor::instance();
        const auto pressureLevel = governor.getPressureLevel();
        if (pressureLevel >= ResourcePressureLevel::Critical) {
            // Re-enqueue the batch so it is not lost; back off and retry later.
            {
                std::lock_guard<std::mutex> lk(queueMutex_);
                for (auto& h : batch) {
                    if (pendingSet_.find(h) == pendingSet_.end()) {
                        pendingSet_.insert(h);
                        pendingDocuments_.push(std::move(h));
                    }
                }
                if (state_)
                    state_->stats.repairQueueDepth.store(
                        static_cast<uint64_t>(pendingDocuments_.size()));
            }
            spdlog::debug("RepairService: deferring batch ({} docs) under {} pressure",
                          batch.size(), pressureLevelName(pressureLevel));
            timer.expires_after(std::chrono::milliseconds(500));
            co_await timer.async_wait(boost::asio::use_awaitable);
            continue;
        }

        // Under Warning pressure, reduce batch sizes so repair cooperates with
        // the ingestion pipeline instead of competing for channel capacity.
        if (pressureLevel >= ResourcePressureLevel::Warning) {
            const size_t reducedCap = std::max<size_t>(1, missingEmbeddings.size() / 2);
            if (missingEmbeddings.size() > reducedCap) {
                // Put excess back into the pending queue for the next iteration.
                std::lock_guard<std::mutex> lk(queueMutex_);
                for (size_t i = reducedCap; i < missingEmbeddings.size(); ++i) {
                    if (pendingSet_.find(missingEmbeddings[i]) == pendingSet_.end()) {
                        pendingSet_.insert(missingEmbeddings[i]);
                        pendingDocuments_.push(missingEmbeddings[i]);
                    }
                }
                missingEmbeddings.resize(reducedCap);
                if (state_)
                    state_->stats.repairQueueDepth.store(
                        static_cast<uint64_t>(pendingDocuments_.size()));
            }
        }

        // Queue embedding repair jobs
        if (!missingEmbeddings.empty()) {
            if (state_)
                state_->stats.repairBatchesAttempted++;
            if (meta_repo) {
                // Mark as Processing before queuing
                auto statusRes = meta_repo->batchUpdateDocumentRepairStatuses(
                    missingEmbeddings, yams::metadata::RepairStatus::Processing);
                (void)statusRes;
            }

            InternalEventBus::EmbedJob job{
                missingEmbeddings, static_cast<uint32_t>(shutdownState->config.maxBatch), true,
                std::string{}, std::vector<InternalEventBus::EmbedPreparedDoc>{}};
            const uint32_t embedCap = TuneAdvisor::embedChannelCapacity();
            static std::shared_ptr<SpscQueue<InternalEventBus::EmbedJob>> embedQ =
                InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
                    "embed_jobs", embedCap);
            bool queued = co_await queueWithBackoff(embedQ, std::move(job), timer, running_,
                                                    "embed", missingEmbeddings.size(), 50, 1000);
            if (queued) {
                InternalEventBus::instance().incEmbedQueued();
            } else {
                if (meta_repo) {
                    // Avoid leaving documents stuck in Processing when enqueue fails (shutdown,
                    // channel pressure). They can be retried by subsequent repair passes.
                    (void)meta_repo->batchUpdateDocumentRepairStatuses(
                        missingEmbeddings, yams::metadata::RepairStatus::Pending);
                }
                InternalEventBus::instance().incEmbedDropped();
            }
        }

        // Queue FTS5 jobs — also gated by governor admission control.
        // FTS5 indexing is less resource-intensive than embedding but still adds
        // I/O pressure; skip under Critical/Emergency to let ingestion proceed.
        bool allowFts5 = (pressureLevel < ResourcePressureLevel::Critical) &&
                         (maintenanceAllowed() ||
                          (shutdownState->config.allowDegraded && activeConnFn_ &&
                           activeConnFn_() <= shutdownState->config.maxActiveDuringDegraded));
        if (!missingFts5.empty() && allowFts5) {
            InternalEventBus::Fts5Job ftsJob{
                .hashes = missingFts5,
                .ids = {},
                .batchSize = static_cast<uint32_t>(shutdownState->config.maxBatch),
                .operation = InternalEventBus::Fts5Operation::ExtractAndIndex};
            static std::shared_ptr<SpscQueue<InternalEventBus::Fts5Job>> fts5Q =
                InternalEventBus::instance().get_or_create_channel<InternalEventBus::Fts5Job>(
                    "fts5_jobs", 512);
            bool queued = co_await queueWithBackoff(fts5Q, std::move(ftsJob), timer, running_,
                                                    "FTS5", missingFts5.size(), 100, 2000);
            if (queued)
                InternalEventBus::instance().incFts5Queued();
            else
                InternalEventBus::instance().incFts5Dropped();
        }

        timer.expires_after(100ms);
        co_await timer.async_wait(boost::asio::use_awaitable);
    }

    spdlog::debug("RepairService background loop stopped");
    co_return;
}

boost::asio::awaitable<void> RepairService::spawnInitialScan() {
    try {
        auto meta = services_ ? services_->getMetadataRepo() : nullptr;
        if (!meta)
            co_return;

        const bool vectorsDisabled = vectorsDisabledByEnv();
        const size_t batchSize = TuneAdvisor::repairStartupBatchSize();
        size_t offset = 0;
        size_t totalEnqueued = 0;

        // Preload all FTS5-indexed rowids once for O(1) lookups in the scan loop.
        std::unordered_set<int64_t> ftsRowIds;
        {
            auto ftsRes = meta->getFts5IndexedRowIdSet();
            if (ftsRes)
                ftsRowIds = std::move(ftsRes.value());
        }

        while (running_.load(std::memory_order_relaxed)) {
            metadata::DocumentQueryOptions opts;
            opts.limit = static_cast<int>(batchSize);
            opts.offset = static_cast<int>(offset);
            auto batchDocs = meta->queryDocuments(opts);
            if (!batchDocs || batchDocs.value().empty())
                break;

            size_t batchEnqueued = 0;
            {
                std::lock_guard<std::mutex> ql(queueMutex_);
                for (const auto& d : batchDocs.value()) {
                    if (!running_.load(std::memory_order_relaxed))
                        break;
                    if (d.repairStatus == yams::metadata::RepairStatus::Completed ||
                        d.repairStatus == yams::metadata::RepairStatus::Processing)
                        continue;

                    // NEW: recover stalled Processing on startup
                    // (handled by detectStuckDocuments during on-demand repair)

                    bool missingEmb = false;
                    if (!vectorsDisabled) {
                        auto hasEmbedRes = meta->hasDocumentEmbeddingByHash(d.sha256Hash);
                        missingEmb = !hasEmbedRes || !hasEmbedRes.value();
                    }
                    bool missingFts =
                        (!d.contentExtracted) &&
                        (d.extractionStatus != yams::metadata::ExtractionStatus::Skipped) &&
                        (d.extractionStatus != yams::metadata::ExtractionStatus::Success);
                    // Also detect successful extraction with missing FTS5 entry.
                    if (!missingFts &&
                        d.extractionStatus == yams::metadata::ExtractionStatus::Success &&
                        ftsRowIds.count(d.id) == 0) {
                        missingFts = true;
                    }

                    if (missingEmb || missingFts) {
                        if (pendingSet_.find(d.sha256Hash) == pendingSet_.end()) {
                            pendingSet_.insert(d.sha256Hash);
                            pendingDocuments_.push(d.sha256Hash);
                            ++batchEnqueued;
                        }
                    }
                }
            }

            totalEnqueued += batchEnqueued;
            offset += batchDocs.value().size();
            co_await boost::asio::post(co_await boost::asio::this_coro::executor,
                                       boost::asio::use_awaitable);
            if (batchDocs.value().size() < batchSize)
                break;
        }

        if (totalEnqueued > 0) {
            totalBacklog_.store(totalEnqueued, std::memory_order_relaxed);
            processed_.store(0, std::memory_order_relaxed);
            if (state_) {
                state_->stats.repairTotalBacklog.store(totalEnqueued, std::memory_order_relaxed);
                state_->stats.repairProcessed.store(0, std::memory_order_relaxed);
            }
            spdlog::debug("RepairService: initial scan queued {} documents", totalEnqueued);
        }
    } catch (const std::exception& e) {
        spdlog::warn("RepairService: initial scan exception: {}", e.what());
    }
    co_return;
}

boost::asio::awaitable<void> RepairService::processPathTreeRepair() {
    using namespace std::chrono_literals;
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    auto repairMgr = services_ ? services_->getRepairManager() : nullptr;
    auto metaRepo = services_ ? services_->getMetadataRepo() : nullptr;
    if (!repairMgr || !metaRepo)
        co_return;

    try {
        auto docsResult = metaRepo->queryDocuments(metadata::DocumentQueryOptions{});
        if (!docsResult || docsResult.value().empty())
            co_return;

        uint64_t created = 0, errors = 0, scanned = 0;
        const size_t batchSz = TuneAdvisor::repairStartupBatchSize();

        for (size_t i = 0; i < docsResult.value().size(); i += batchSz) {
            if (!running_.load(std::memory_order_relaxed))
                break;
            size_t batchEnd = std::min(i + batchSz, docsResult.value().size());
            for (size_t j = i; j < batchEnd; ++j) {
                const auto& doc = docsResult.value()[j];
                if (doc.filePath.empty())
                    continue;
                auto existingNode = metaRepo->findPathTreeNodeByFullPath(doc.filePath);
                if (existingNode && existingNode.value().has_value())
                    continue;
                try {
                    auto treeRes = metaRepo->upsertPathTreeForDocument(doc, doc.id, true,
                                                                       std::span<const float>());
                    if (treeRes)
                        ++created;
                    else
                        ++errors;
                } catch (...) {
                    ++errors;
                }
                ++scanned;
            }
            timer.expires_after(10ms);
            co_await timer.async_wait(boost::asio::use_awaitable);
        }

        spdlog::debug("RepairService: PathTreeRepair complete (created={}, errors={})", created,
                      errors);
    } catch (const std::exception& e) {
        spdlog::debug("RepairService: PathTreeRepair exception: {}", e.what());
    }
}

void RepairService::performVectorCleanup() {
    if (vectorsDisabledByEnv())
        return;
    try {
        auto vectorDb = services_ ? services_->getVectorDatabase() : nullptr;
        if (vectorDb) {
            auto cleanup = vectorDb->cleanupOrphanRows();
            if (cleanup)
                spdlog::debug("RepairService: vector orphan cleanup done");
            else
                spdlog::warn("RepairService: vector cleanup failed: {}", cleanup.error().message);
        }
    } catch (const std::exception& e) {
        spdlog::warn("RepairService: vector cleanup exception: {}", e.what());
    }
}

RepairService::MissingWorkResult
RepairService::detectMissingWork(const std::vector<std::string>& batch) {
    MissingWorkResult result;
    auto content = services_ ? services_->getContentStore() : nullptr;
    auto meta_repo = services_ ? services_->getMetadataRepo() : nullptr;
    if (!(content && meta_repo))
        return result;

    // Detect missing embeddings
    if (!vectorsDisabledByEnv() && meta_repo) {
        for (const auto& hash : batch) {
            auto hasEmbedRes = meta_repo->hasDocumentEmbeddingByHash(hash);
            if (!hasEmbedRes || !hasEmbedRes.value())
                result.missingEmbeddings.push_back(hash);
        }

        if (!result.missingEmbeddings.empty()) {
            auto provider = services_ ? services_->getModelProvider() : nullptr;
            auto vectorDb = services_ ? services_->getVectorDatabase() : nullptr;
            if (provider && provider->isAvailable()) {
                size_t modelDim = 0;
                if (services_) {
                    const auto preferredModel = services_->resolvePreferredModel();
                    if (!preferredModel.empty())
                        modelDim = provider->getEmbeddingDim(preferredModel);
                }
                if (modelDim == 0)
                    modelDim = provider->getEmbeddingDim("");

                size_t storedDim = 0;
                if (vectorDb)
                    storedDim = vectorDb->getConfig().embedding_dim;

                if (modelDim > 0 && storedDim > 0 && modelDim != storedDim) {
                    if (cfg_.autoRebuildOnDimMismatch && !dimMismatchRebuildDone_.exchange(true)) {
                        spdlog::warn(
                            "RepairService: rebuilding vectors (model dim {} != db dim {})",
                            modelDim, storedDim);
                        if (vectorDb) {
                            try {
                                const auto dbPath = vectorDb->getConfig().database_path;
                                yams::vector::SqliteVecBackend backend;
                                auto initRes = backend.initialize(dbPath);
                                if (initRes) {
                                    auto dropRes = backend.dropTables();
                                    if (dropRes) {
                                        auto createRes = backend.createTables(modelDim);
                                        if (createRes)
                                            ConfigResolver::writeVectorSentinel(
                                                cfg_.dataDir, modelDim, "vec0", 1);
                                    }
                                }
                                backend.close();
                            } catch (...) {
                            }
                        }
                    } else {
                        result.missingEmbeddings.clear();
                    }
                }
            }
        }
    }

    // Detect missing FTS5 content
    auto meta = services_ ? services_->getMetadataRepo() : nullptr;
    if (meta) {
        auto customExtractors = services_
                                    ? services_->getContentExtractors()
                                    : std::vector<std::shared_ptr<extraction::IContentExtractor>>{};
        for (const auto& hash : batch) {
            auto docRes = meta->getDocumentByHash(hash);
            if (docRes && docRes.value().has_value()) {
                const auto& d = docRes.value().value();
                if (d.repairStatus == yams::metadata::RepairStatus::Completed ||
                    d.repairStatus == yams::metadata::RepairStatus::Processing)
                    continue;
                if (!d.contentExtracted ||
                    d.extractionStatus != yams::metadata::ExtractionStatus::Success) {
                    if (canExtractDocument(d.mimeType, d.fileExtension, customExtractors, content,
                                           hash))
                        result.missingFts5.push_back(hash);
                } else {
                    // Extraction succeeded — verify FTS5 entry actually exists.
                    // Small batch (per-hash), so per-document lookup is fine.
                    auto ftsRes = meta->hasFtsEntry(d.id);
                    if (ftsRes && !ftsRes.value()) {
                        result.missingFts5.push_back(hash);
                    }
                }
            }
        }
    }

    return result;
}

void RepairService::updateProgressPct() {
    if (!state_)
        return;
    auto tot = totalBacklog_.load(std::memory_order_relaxed);
    state_->stats.repairTotalBacklog.store(tot, std::memory_order_relaxed);
    if (tot == 0)
        return;
    auto done = processed_.load(std::memory_order_relaxed);
    state_->stats.repairProcessed.store(done, std::memory_order_relaxed);
    int pct = static_cast<int>(std::min<std::uint64_t>(100, (done * 100) / tot));
    state_->readiness.vectorIndexProgress.store(pct, std::memory_order_relaxed);
}

// ============================================================================
// On-demand Repair (RPC entry point)
// ============================================================================

RepairResponse RepairService::executeRepair(const RepairRequest& request, ProgressFn progress) {
    std::unique_lock<std::mutex> lock(repairMutex_, std::try_to_lock);
    if (!lock.owns_lock()) {
        // Another repair RPC is already running — return immediately.
        RepairResponse busy;
        busy.success = false;
        busy.errors.push_back(
            "Repair is already in progress. Please wait for the current run to finish.");
        spdlog::info("[RepairService] executeRepair rejected: repair already in progress");
        return busy;
    }

    repairInProgress_.store(true, std::memory_order_release);
    if (state_) {
        state_->stats.repairInProgress.store(true, std::memory_order_relaxed);
    }
    // RAII guard to clear the flag when we leave this scope.
    struct InProgressGuard {
        std::atomic<bool>& flag;
        std::atomic<bool>* statsFlag;
        ~InProgressGuard() {
            flag.store(false, std::memory_order_release);
            if (statsFlag) {
                statsFlag->store(false, std::memory_order_relaxed);
            }
        }
    } inProgressGuard{repairInProgress_, state_ ? &state_->stats.repairInProgress : nullptr};

    RepairResponse response;
    std::vector<RepairOperationResult> results;
    std::vector<std::string> errors;

    auto runOp = [&](const std::string& name, auto&& fn) {
        if (progress) {
            RepairEvent ev;
            ev.phase = "repairing";
            ev.operation = name;
            ev.message = "Starting " + name + "...";
            progress(ev);
        }
        auto result = fn();
        results.push_back(result);
        response.totalOperations++;
        response.totalSucceeded += result.succeeded;
        response.totalFailed += result.failed;
        response.totalSkipped += result.skipped;
        if (result.failed > 0 && !result.message.empty())
            errors.push_back(name + ": " + result.message);
        if (progress) {
            RepairEvent ev;
            ev.phase = "completed";
            ev.operation = name;
            ev.processed = result.processed;
            ev.succeeded = result.succeeded;
            ev.failed = result.failed;
            ev.skipped = result.skipped;
            ev.message = result.message;
            progress(ev);
        }
    };

    bool doOrphans = request.repairOrphans || request.repairAll;
    bool doMime = request.repairMime || request.repairAll;
    bool doDownloads = request.repairDownloads || request.repairAll;
    bool doPathTree = request.repairPathTree || request.repairAll;
    bool doChunks = request.repairChunks || request.repairAll;
    bool doBlockRefs = request.repairBlockRefs || request.repairAll;
    bool doFts5 = request.repairFts5 || request.repairAll;
    bool doEmbeddings = request.repairEmbeddings || request.repairAll;
    bool doStuckDocs = request.repairStuckDocs || request.repairAll;
    bool doOptimize = request.optimizeDb || request.repairAll;

    // Phase 0: Stuck document recovery
    if (doStuckDocs)
        runOp("stuck_docs", [&] { return recoverStuckDocuments(request, progress); });

    // Phase 1: Metadata repair
    if (doOrphans)
        runOp("orphans",
              [&] { return cleanOrphanedMetadata(request.dryRun, request.verbose, progress); });
    if (doMime)
        runOp("mime", [&] { return repairMimeTypes(request.dryRun, request.verbose, progress); });
    if (doDownloads)
        runOp("downloads",
              [&] { return repairDownloads(request.dryRun, request.verbose, progress); });
    if (doPathTree)
        runOp("path_tree",
              [&] { return rebuildPathTree(request.dryRun, request.verbose, progress); });

    // Phase 2: Storage repair
    if (doChunks)
        runOp("chunks",
              [&] { return cleanOrphanedChunks(request.dryRun, request.verbose, progress); });
    if (doBlockRefs)
        runOp("block_refs",
              [&] { return repairBlockReferences(request.dryRun, request.verbose, progress); });

    // Phase 3: Search index repair
    if (doFts5)
        runOp("fts5", [&] { return rebuildFts5Index(request, progress); });
    if (doEmbeddings)
        runOp("embeddings", [&] { return generateMissingEmbeddings(request, progress); });

    // Phase 4: Database maintenance
    if (doOptimize)
        runOp("optimize",
              [&] { return optimizeDatabase(request.dryRun, request.verbose, progress); });

    response.success = errors.empty();
    response.errors = std::move(errors);
    response.operationResults = std::move(results);
    return response;
}

// ============================================================================
// Stuck Document Recovery (NEW)
// ============================================================================

std::vector<RepairService::StuckDocumentInfo>
RepairService::detectStuckDocuments(int32_t maxRetries) {
    std::vector<StuckDocumentInfo> stuck;
    auto meta = services_ ? services_->getMetadataRepo() : nullptr;
    if (!meta)
        return stuck;

    auto now = std::chrono::system_clock::now();
    auto threshold = std::chrono::duration_cast<std::chrono::seconds>(cfg_.stalledThreshold);
    auto cutoffEpoch =
        std::chrono::duration_cast<std::chrono::seconds>((now - threshold).time_since_epoch())
            .count();

    // Use targeted SQL queries for each stuck category instead of loading all docs.

    // Category 1: Failed extraction
    {
        metadata::DocumentQueryOptions opts;
        opts.extractionStatuses = {metadata::ExtractionStatus::Failed};
        opts.maxRepairAttempts = maxRetries;

        auto docsResult = meta->queryDocuments(opts);
        if (docsResult) {
            for (const auto& d : docsResult.value()) {
                stuck.push_back({StuckDocumentInfo::FailedExtraction, d.id, d.sha256Hash,
                                 d.filePath, d.repairAttempts});
            }
        }
    }

    // Category 2: Ghost success (Success but no content row)
    {
        metadata::DocumentQueryOptions opts;
        opts.extractionStatuses = {metadata::ExtractionStatus::Success};
        opts.maxRepairAttempts = maxRetries;
        opts.onlyMissingContent = true;

        auto docsResult = meta->queryDocuments(opts);
        if (docsResult) {
            for (const auto& d : docsResult.value()) {
                stuck.push_back({StuckDocumentInfo::GhostSuccess, d.id, d.sha256Hash, d.filePath,
                                 d.repairAttempts});
            }
        }
    }

    // Category 3: Stalled Pending (indexed/modified before cutoff)
    {
        metadata::DocumentQueryOptions opts;
        opts.extractionStatuses = {metadata::ExtractionStatus::Pending};
        opts.maxRepairAttempts = maxRetries;
        opts.stalledBefore = cutoffEpoch;

        auto docsResult = meta->queryDocuments(opts);
        if (docsResult) {
            for (const auto& d : docsResult.value()) {
                stuck.push_back({StuckDocumentInfo::StalledPending, d.id, d.sha256Hash, d.filePath,
                                 d.repairAttempts});
            }
        }
    }

    // Category 4: Stalled Processing (repairStatus stuck in Processing)
    {
        metadata::DocumentQueryOptions opts;
        opts.repairStatuses = {metadata::RepairStatus::Processing};
        opts.maxRepairAttempts = maxRetries;
        opts.repairAttemptedBefore = cutoffEpoch;

        auto docsResult = meta->queryDocuments(opts);
        if (docsResult) {
            for (const auto& d : docsResult.value()) {
                stuck.push_back({StuckDocumentInfo::StalledProcessing, d.id, d.sha256Hash,
                                 d.filePath, d.repairAttempts});
            }
        }
    }

    return stuck;
}

RepairOperationResult RepairService::recoverStuckDocuments(const RepairRequest& req,
                                                           ProgressFn progress) {
    RepairOperationResult result;
    result.operation = "stuck_docs";

    auto meta = services_ ? services_->getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata repository not available";
        return result;
    }

    auto stuckDocs = detectStuckDocuments(req.maxRetries);
    result.processed = stuckDocs.size();

    if (stuckDocs.empty()) {
        result.message = "No stuck documents found";
        return result;
    }

    spdlog::info("RepairService: found {} stuck documents", stuckDocs.size());

    if (req.dryRun) {
        result.message = "Would recover " + std::to_string(stuckDocs.size()) + " stuck documents";
        result.skipped = stuckDocs.size();
        return result;
    }

    // Re-enqueue stuck documents for re-extraction via a high-priority PostIngestQueue channel.
    // Use back-pressure retries with drain polling to avoid channel overflow and silent doc loss.
    const std::size_t rpcCapacity = static_cast<std::size_t>(TuneAdvisor::postIngestRpcQueueMax());
    const bool useRpcChannel = (rpcCapacity > 0);
    auto postIngestChannel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            useRpcChannel ? "post_ingest_rpc" : "post_ingest",
            useRpcChannel ? rpcCapacity : std::size_t(4096));

    constexpr int kMaxRetryRounds = 20; // 20 rounds × 500ms = 10s max wait per document
    constexpr auto kDrainPollInterval = std::chrono::milliseconds(500);

    for (size_t i = 0; i < stuckDocs.size(); ++i) {
        const auto& s = stuckDocs[i];

        // Re-enqueue for extraction (do not mutate metadata unless enqueue succeeds).
        InternalEventBus::PostIngestTask task;
        task.hash = s.hash;
        task.mime = ""; // will be re-detected

        // Attempt the push with back-pressure retries
        bool pushed = postIngestChannel->try_push(task);
        if (!pushed) {
            // Channel full — wait for consumer to drain, then retry
            for (int retry = 0; retry < kMaxRetryRounds && !pushed; ++retry) {
                std::this_thread::sleep_for(kDrainPollInterval);
                pushed = postIngestChannel->try_push(task);
            }
        }

        if (pushed) {
            // Increment repair attempts
            auto docRes = meta->getDocument(s.docId);
            if (docRes && docRes.value().has_value()) {
                auto doc = docRes.value().value();
                doc.repairAttempts = s.repairAttempts + 1;
                doc.repairAttemptedAt = std::chrono::time_point_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now());
                (void)meta->updateDocument(doc);
            }

            // Reset extraction status to Pending
            (void)meta->updateDocumentExtractionStatus(s.docId, false,
                                                       metadata::ExtractionStatus::Pending,
                                                       "RepairService: recovery attempt");

            // Reset repair status to Pending
            (void)meta->batchUpdateDocumentRepairStatuses({s.hash},
                                                          metadata::RepairStatus::Pending);

            ++result.succeeded;
        } else {
            ++result.failed;
            spdlog::warn("RepairService: PostIngest channel full after retries, could not "
                         "re-enqueue {} (consider increasing YAMS_POST_INGEST_RPC_QUEUE_MAX)",
                         s.hash.substr(0, 12));
        }

        if (progress && (i + 1) % 10 == 0) {
            RepairEvent ev;
            ev.phase = "repairing";
            ev.operation = "stuck_docs";
            ev.processed = i + 1;
            ev.total = stuckDocs.size();
            ev.succeeded = result.succeeded;
            ev.failed = result.failed;
            progress(ev);
        }
    }

    result.message = "Recovered " + std::to_string(result.succeeded) + " stuck documents";
    if (result.failed > 0) {
        result.message += " (" + std::to_string(result.failed) +
                          " could not be enqueued — re-run repair to retry)";
    }
    return result;
}

// ============================================================================
// Core Repair Operations
// ============================================================================

RepairOperationResult RepairService::cleanOrphanedMetadata(bool dryRun, bool verbose,
                                                           ProgressFn progress) {
    RepairOperationResult result;
    result.operation = "orphans";

    auto store = services_ ? services_->getContentStore() : nullptr;
    auto meta = services_ ? services_->getMetadataRepo() : nullptr;
    if (!store || !meta) {
        result.message = "Store or metadata not available";
        return result;
    }

    auto docsResult = metadata::queryDocumentsByPattern(*meta, "%");
    if (!docsResult) {
        result.message = "Failed to query: " + docsResult.error().message;
        return result;
    }

    std::vector<int64_t> orphanedIds;
    for (const auto& doc : docsResult.value()) {
        auto existsResult = store->exists(doc.sha256Hash);
        if (!existsResult || !existsResult.value())
            orphanedIds.push_back(doc.id);
    }

    result.processed = docsResult.value().size();

    if (orphanedIds.empty()) {
        result.message = "No orphaned entries";
        return result;
    }

    if (dryRun) {
        result.skipped = orphanedIds.size();
        result.message = "Would clean " + std::to_string(orphanedIds.size()) + " orphans";
        return result;
    }

    auto batchResult = meta->deleteDocumentsBatch(orphanedIds);
    if (batchResult) {
        result.succeeded = batchResult.value();
    } else {
        // Fallback to individual deletes
        for (int64_t id : orphanedIds) {
            auto del = meta->deleteDocument(id);
            if (del)
                result.succeeded++;
            else
                result.failed++;
        }
    }

    result.message = "Cleaned " + std::to_string(result.succeeded) + " orphans";
    return result;
}

RepairOperationResult RepairService::repairMimeTypes(bool dryRun, bool verbose,
                                                     ProgressFn progress) {
    RepairOperationResult result;
    result.operation = "mime";

    auto meta = services_ ? services_->getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata not available";
        return result;
    }

    auto docsResult = metadata::queryDocumentsByPattern(*meta, "%");
    if (!docsResult) {
        result.message = "Failed to query: " + docsResult.error().message;
        return result;
    }

    std::vector<std::pair<int64_t, std::string>> toRepair;
    for (const auto& doc : docsResult.value()) {
        if (doc.mimeType.empty() || doc.mimeType == "application/octet-stream") {
            std::string detectedMime;
            if (!doc.fileExtension.empty())
                detectedMime =
                    detection::FileTypeDetector::getMimeTypeFromExtension(doc.fileExtension);
            if (detectedMime.empty() || detectedMime == "application/octet-stream") {
                auto pos = doc.fileName.rfind('.');
                if (pos != std::string::npos)
                    detectedMime = detection::FileTypeDetector::getMimeTypeFromExtension(
                        doc.fileName.substr(pos));
            }
            if (detectedMime.empty() || detectedMime == "application/octet-stream")
                detectedMime = "text/plain";
            if (!detectedMime.empty() && detectedMime != "application/octet-stream")
                toRepair.push_back({doc.id, detectedMime});
        }
    }

    result.processed = docsResult.value().size();

    if (toRepair.empty()) {
        result.message = "All documents have valid MIME types";
        return result;
    }

    if (dryRun) {
        result.skipped = toRepair.size();
        result.message = "Would repair " + std::to_string(toRepair.size()) + " MIME types";
        return result;
    }

    auto batchResult = meta->updateDocumentsMimeBatch(toRepair);
    if (batchResult) {
        result.succeeded = batchResult.value();
    } else {
        for (const auto& [id, mimeType] : toRepair) {
            auto docResult = meta->getDocument(id);
            if (docResult && docResult.value()) {
                auto doc = *docResult.value();
                doc.mimeType = mimeType;
                if (meta->updateDocument(doc))
                    result.succeeded++;
                else
                    result.failed++;
            }
        }
    }

    result.message = "Repaired " + std::to_string(result.succeeded) + " MIME types";
    return result;
}

RepairOperationResult RepairService::repairDownloads(bool dryRun, bool verbose,
                                                     ProgressFn progress) {
    RepairOperationResult result;
    result.operation = "downloads";

    auto meta = services_ ? services_->getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata not available";
        return result;
    }

    auto docsResult = metadata::queryDocumentsByPattern(*meta, "%");
    if (!docsResult) {
        result.message = "Failed to query";
        return result;
    }

    auto is_url = [](const std::string& s) { return s.find("://") != std::string::npos; };
    auto extract_host = [](const std::string& url) -> std::string {
        auto p = url.find("://");
        if (p == std::string::npos)
            return {};
        auto rest = url.substr(p + 3);
        auto slash = rest.find('/');
        return (slash == std::string::npos) ? rest : rest.substr(0, slash);
    };
    auto extract_scheme = [](const std::string& url) -> std::string {
        auto p = url.find("://");
        return (p == std::string::npos) ? std::string{} : url.substr(0, p);
    };
    auto filename_from_url = [](std::string url) -> std::string {
        auto lastSlash = url.find_last_of('/');
        if (lastSlash != std::string::npos)
            url = url.substr(lastSlash + 1);
        auto q = url.find('?');
        if (q != std::string::npos)
            url = url.substr(0, q);
        if (url.empty())
            url = "downloaded_file";
        return url;
    };

    for (auto doc : docsResult.value()) {
        std::string sourceUrl;
        if (is_url(doc.filePath))
            sourceUrl = doc.filePath;
        if (sourceUrl.empty())
            continue;

        result.processed++;
        if (dryRun) {
            result.skipped++;
            continue;
        }

        std::string filename = filename_from_url(sourceUrl);
        std::string ext;
        auto dotPos = filename.rfind('.');
        if (dotPos != std::string::npos)
            ext = filename.substr(dotPos);
        doc.filePath = filename;
        doc.fileName = filename;
        doc.fileExtension = ext;

        if (auto up = meta->updateDocument(doc); up) {
            result.succeeded++;
        } else {
            result.failed++;
        }

        try {
            meta->setMetadata(doc.id, "source_url", metadata::MetadataValue(sourceUrl));
            meta->setMetadata(doc.id, "tag", metadata::MetadataValue("downloaded"));
            auto host = extract_host(sourceUrl);
            auto scheme = extract_scheme(sourceUrl);
            if (!host.empty())
                meta->setMetadata(doc.id, "tag", metadata::MetadataValue("host:" + host));
            if (!scheme.empty())
                meta->setMetadata(doc.id, "tag", metadata::MetadataValue("scheme:" + scheme));
        } catch (...) {
        }
    }

    result.message = "Updated " + std::to_string(result.succeeded) + " download documents";
    return result;
}

RepairOperationResult RepairService::rebuildPathTree(bool dryRun, bool verbose,
                                                     ProgressFn progress) {
    RepairOperationResult result;
    result.operation = "path_tree";

    auto meta = services_ ? services_->getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata not available";
        return result;
    }

    // Use RepairManager for path tree repair
    auto concreteRepo = std::dynamic_pointer_cast<metadata::MetadataRepository>(meta);
    if (!concreteRepo) {
        result.message = "Could not get concrete MetadataRepository";
        return result;
    }

    if (dryRun) {
        auto countResult = concreteRepo->countDocsMissingPathTree();
        if (!countResult) {
            result.message = "Failed to count missing path tree entries";
            return result;
        }
        uint64_t missing = countResult.value();
        result.processed = missing;
        result.skipped = missing;
        result.message = "Would rebuild " + std::to_string(missing) + " path tree entries";
        return result;
    }

    integrity::RepairManager repairMgr(*concreteRepo);
    auto repairResult = repairMgr.repairPathTree([](uint64_t, uint64_t) {});
    if (!repairResult) {
        result.message = "Path tree repair failed: " + repairResult.error().message;
        result.failed = 1;
        return result;
    }

    const auto& stats = repairResult.value();
    result.processed = stats.documentsScanned;
    result.succeeded = stats.nodesCreated;
    result.failed = stats.errors;
    result.message = "Created " + std::to_string(stats.nodesCreated) + " path tree entries";
    return result;
}

RepairOperationResult RepairService::cleanOrphanedChunks(bool dryRun, bool verbose,
                                                         ProgressFn progress) {
    RepairOperationResult result;
    result.operation = "chunks";

    namespace fs = std::filesystem;
    fs::path refsDbPath = cfg_.dataDir / "storage" / "refs.db";
    fs::path objectsPath = cfg_.dataDir / "storage" / "objects";

    if (!fs::exists(refsDbPath) || !fs::exists(objectsPath)) {
        result.message = "No refs.db or objects directory found";
        return result;
    }

    sqlite3* db = nullptr;
    if (sqlite3_open(refsDbPath.string().c_str(), &db) != SQLITE_OK) {
        result.message = "Failed to open refs.db";
        return result;
    }

    // Quick-check: count unreferenced block_references entries.
    // If zero, the common case (no orphans expected) can skip the expensive FS walk.
    {
        sqlite3_stmt* countStmt = nullptr;
        const char* countSql = "SELECT COUNT(*) FROM block_references WHERE ref_count = 0";
        int zeroRefCount = 0;
        if (sqlite3_prepare_v2(db, countSql, -1, &countStmt, nullptr) == SQLITE_OK) {
            if (sqlite3_step(countStmt) == SQLITE_ROW)
                zeroRefCount = sqlite3_column_int(countStmt, 0);
            sqlite3_finalize(countStmt);
        }

        if (zeroRefCount == 0) {
            // Also do a quick FS sanity check: count prefix dirs. If the count matches
            // the number of referenced hashes' unique prefixes, no orphans are possible.
            // For now, if refs.db tracks no zero-ref entries, trust it and skip the walk.
            sqlite3_close(db);
            result.message = "No orphaned chunks (quick-check: 0 unreferenced blocks)";
            if (progress) {
                RepairEvent ev;
                ev.phase = "completed";
                ev.operation = "chunks";
                ev.message = result.message;
                progress(ev);
            }
            return result;
        }

        spdlog::info("[RepairService] chunks: {} unreferenced block_references, proceeding "
                     "with filesystem scan",
                     zeroRefCount);
    }

    // Get referenced hashes
    std::set<std::string> referencedHashes;
    sqlite3_stmt* stmt;
    const char* sql = "SELECT block_hash FROM block_references WHERE ref_count > 0";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const char* hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            if (hash)
                referencedHashes.insert(hash);
        }
        sqlite3_finalize(stmt);
    }

    if (progress) {
        RepairEvent ev;
        ev.phase = "repairing";
        ev.operation = "chunks";
        ev.message = "Loaded " + std::to_string(referencedHashes.size()) +
                     " referenced hashes, scanning filesystem...";
        progress(ev);
    }

    // Scan manifests for additional references
    size_t manifestDirsScanned = 0;
    if (fs::exists(objectsPath)) {
        for (const auto& dirEntry : fs::directory_iterator(objectsPath)) {
            if (!fs::is_directory(dirEntry))
                continue;
            ++manifestDirsScanned;
            if (progress && manifestDirsScanned % 64 == 0) {
                RepairEvent ev;
                ev.phase = "repairing";
                ev.operation = "chunks";
                ev.message =
                    "Scanning manifests (" + std::to_string(manifestDirsScanned) + " dirs)...";
                progress(ev);
            }
            for (const auto& fileEntry : fs::directory_iterator(dirEntry.path())) {
                if (!fs::is_regular_file(fileEntry))
                    continue;
                std::string filename = fileEntry.path().filename().string();
                if (filename.size() > 9 && filename.substr(filename.size() - 9) == ".manifest") {
                    try {
                        std::ifstream mf(fileEntry.path(), std::ios::binary);
                        if (!mf)
                            continue;
                        std::vector<char> data((std::istreambuf_iterator<char>(mf)),
                                               std::istreambuf_iterator<char>());
                        if (data.size() >= 28 && data[0] == 'Y' && data[1] == 'M' &&
                            data[2] == 'N' && data[3] == 'F') {
                            size_t pos = 24;
                            uint32_t numChunks = 0;
                            if (pos + 4 <= data.size()) {
                                std::memcpy(&numChunks, data.data() + pos, 4);
                                pos += 4;
                            }
                            for (uint32_t i = 0; i < numChunks && pos + 4 <= data.size(); ++i) {
                                uint32_t hashLen = 0;
                                std::memcpy(&hashLen, data.data() + pos, 4);
                                pos += 4;
                                if (hashLen > 0 && hashLen < 256 && pos + hashLen <= data.size()) {
                                    referencedHashes.insert(
                                        std::string(data.data() + pos, hashLen));
                                    pos += hashLen + 16;
                                } else {
                                    break;
                                }
                            }
                        }
                    } catch (...) {
                    }
                }
            }
        }
    }

    // Find orphaned chunks
    std::vector<std::pair<std::string, fs::path>> orphanedChunks;
    uint64_t bytesToReclaim = 0;
    size_t dirsScanned = 0;
    if (fs::exists(objectsPath)) {
        for (const auto& dirEntry : fs::directory_iterator(objectsPath)) {
            if (!fs::is_directory(dirEntry))
                continue;
            ++dirsScanned;
            // Emit progress every 64 prefix dirs to keep the stream alive
            if (progress && dirsScanned % 64 == 0) {
                RepairEvent ev;
                ev.phase = "repairing";
                ev.operation = "chunks";
                ev.message = "Scanning directory " + std::to_string(dirsScanned) + "...";
                progress(ev);
            }
            std::string dirName = dirEntry.path().filename().string();
            for (const auto& fileEntry : fs::directory_iterator(dirEntry.path())) {
                if (!fs::is_regular_file(fileEntry))
                    continue;
                std::string fn = fileEntry.path().filename().string();
                if (fn.size() > 9 && fn.substr(fn.size() - 9) == ".manifest")
                    continue;
                std::string fullHash = dirName + fn;
                if (referencedHashes.find(fullHash) == referencedHashes.end()) {
                    orphanedChunks.push_back({fullHash, fileEntry.path()});
                    try {
                        bytesToReclaim += fs::file_size(fileEntry.path());
                    } catch (...) {
                    }
                }
            }
        }
    }

    result.processed = orphanedChunks.size();

    if (orphanedChunks.empty()) {
        result.message = "No orphaned chunks";
        sqlite3_close(db);
        return result;
    }

    if (dryRun) {
        result.skipped = orphanedChunks.size();
        result.message = "Would delete " + std::to_string(orphanedChunks.size()) + " chunks";
        sqlite3_close(db);
        return result;
    }

    uint64_t bytesReclaimed = 0;
    for (const auto& [hash, path] : orphanedChunks) {
        try {
            auto size = fs::file_size(path);
            fs::remove(path);
            bytesReclaimed += size;
            result.succeeded++;
        } catch (...) {
            result.failed++;
        }
    }

    // Clean zero-ref entries from refs.db
    sqlite3_exec(db, "DELETE FROM block_references WHERE ref_count = 0", nullptr, nullptr, nullptr);

    sqlite3_close(db);
    result.message = "Deleted " + std::to_string(result.succeeded) + " chunks";
    return result;
}

RepairOperationResult RepairService::repairBlockReferences(bool dryRun, bool verbose,
                                                           ProgressFn progress) {
    RepairOperationResult result;
    result.operation = "block_refs";

    namespace fs = std::filesystem;
    fs::path objectsPath = cfg_.dataDir / "storage" / "objects";
    fs::path refsDbPath = cfg_.dataDir / "storage" / "refs.db";

    if (!fs::exists(objectsPath) || !fs::exists(refsDbPath)) {
        result.message = "No objects directory or refs.db";
        return result;
    }

    auto repairResult = integrity::RepairManager::repairBlockReferences(
        objectsPath, refsDbPath, dryRun, [progress](uint64_t processed, uint64_t total) {
            if (!progress)
                return;
            RepairEvent ev;
            ev.phase = "repairing";
            ev.operation = "block_refs";
            ev.processed = processed;
            ev.total = total;
            ev.message = "Scanning block references";
            progress(ev);
        });
    if (!repairResult) {
        result.message = "Block refs repair failed: " + repairResult.error().message;
        result.failed = 1;
        return result;
    }

    const auto& stats = repairResult.value();
    result.processed = stats.blocksScanned;
    result.succeeded = stats.blocksUpdated;
    result.skipped = stats.blocksSkipped;
    result.failed = stats.errors;
    result.message = "Scanned " + std::to_string(stats.blocksScanned) + ", updated " +
                     std::to_string(stats.blocksUpdated);
    return result;
}

RepairOperationResult RepairService::rebuildFts5Index(const RepairRequest& req,
                                                      ProgressFn progress) {
    RepairOperationResult result;
    result.operation = "fts5";

    const bool dryRun = req.dryRun;
    const bool force = req.force;

    auto store = services_ ? services_->getContentStore() : nullptr;
    auto meta = services_ ? services_->getMetadataRepo() : nullptr;
    if (!store || !meta) {
        result.message = "Store or metadata not available";
        return result;
    }

    auto docs = metadata::queryDocumentsByPattern(*meta, "%");
    if (!docs) {
        result.message = "Failed to enumerate: " + docs.error().message;
        return result;
    }

    // Reset ghost-success documents (Success but no content row)
    for (const auto& d : docs.value()) {
        if (d.extractionStatus != metadata::ExtractionStatus::Success)
            continue;
        auto contentRes = meta->getContent(d.id);
        if (contentRes && !contentRes.value().has_value()) {
            (void)meta->updateDocumentExtractionStatus(d.id, false,
                                                       metadata::ExtractionStatus::Pending,
                                                       "Missing content row; reset by repair");
        }
    }

    auto customExtractors = services_
                                ? services_->getContentExtractors()
                                : std::vector<std::shared_ptr<extraction::IContentExtractor>>{};

    // Pre-load all FTS5 rowids so the incremental skip check below is O(1)
    // per document instead of one SQL round-trip each.
    std::unordered_set<int64_t> ftsRowIds;
    if (!force) {
        auto ftsResult = meta->getFts5IndexedRowIdSet();
        if (ftsResult)
            ftsRowIds = std::move(ftsResult.value());
    }

    result.processed = docs.value().size();
    const size_t totalDocs = docs.value().size();
    size_t docIdx = 0;

    // Emit an initial progress event so the client knows how many documents
    // to expect and doesn't appear to hang on large stores.
    if (progress) {
        RepairEvent ev;
        ev.phase = "repairing";
        ev.operation = "fts5";
        ev.processed = 0;
        ev.total = totalDocs;
        ev.message = "Rebuilding FTS5 index (" + std::to_string(totalDocs) + " documents)";
        progress(ev);
    }

    for (const auto& d : docs.value()) {
        ++docIdx;

        // Emit progress frequently so the streaming layer has events to
        // send (prevents client read timeouts during large rebuilds).
        // Using a small interval (25) keeps the client alive and gives
        // users visible feedback on long operations.
        if (progress && docIdx % 25 == 0) {
            RepairEvent ev;
            ev.phase = "repairing";
            ev.operation = "fts5";
            ev.processed = docIdx;
            ev.total = totalDocs;
            ev.succeeded = result.succeeded;
            ev.failed = result.failed;
            ev.skipped = result.skipped;
            ev.message = "Rebuilding FTS5 index (" + std::to_string(docIdx) + "/" +
                         std::to_string(totalDocs) + ")";
            progress(ev);
        }

        std::string ext = d.fileExtension;
        if (!ext.empty() && ext[0] == '.')
            ext.erase(0, 1);

        // Re-detect MIME for unhelpful types
        std::string effectiveMime = d.mimeType;
        if (effectiveMime.empty() || effectiveMime == "application/octet-stream") {
            if (!ext.empty()) {
                auto detected = detection::FileTypeDetector::getMimeTypeFromExtension(ext);
                if (!detected.empty() && detected != "application/octet-stream")
                    effectiveMime = detected;
            }
            if (effectiveMime.empty() || effectiveMime == "application/octet-stream")
                effectiveMime = "text/plain";
            auto updated = d;
            updated.mimeType = effectiveMime;
            (void)meta->updateDocument(updated);
        }

        if (dryRun) {
            result.skipped++;
            continue;
        }

        // Incremental mode: skip documents that already have successful extraction
        // with a valid content row AND a matching FTS5 index entry. The ghost-success
        // loop above already reset any documents that claim Success but lack content,
        // so remaining Success docs have content — but the FTS5 index may still be
        // missing (e.g. after corruption or interrupted rebuild).
        // Use --force to unconditionally rebuild everything.
        if (!force && d.extractionStatus == metadata::ExtractionStatus::Success) {
            auto contentRes = meta->getContent(d.id);
            if (contentRes && contentRes.value().has_value()) {
                if (ftsRowIds.count(d.id)) {
                    result.skipped++;
                    continue;
                }
            }
        }

        try {
            auto extractedOpt = yams::extraction::util::extractDocumentText(
                store, d.sha256Hash, effectiveMime, ext, customExtractors);
            if (extractedOpt && !extractedOpt->empty()) {
                (void)meta->updateDocumentExtractionStatus(
                    d.id, false, metadata::ExtractionStatus::Pending, "repair fts5 processing");

                metadata::DocumentContent content;
                content.documentId = d.id;
                if (extractedOpt->size() > kMaxTextToPersistInMetadataBytes) {
                    content.contentText = extractedOpt->substr(0, kMaxTextToPersistInMetadataBytes);
                } else {
                    content.contentText = *extractedOpt;
                }
                content.extractionMethod = "repair";
                auto contentResult = meta->insertContent(content);

                // Cap text for FTS5 indexing too — SQLite cannot bind strings > ~2 GB
                // and very large documents cause "string or blob too big" errors.
                const auto& textForIndex = (extractedOpt->size() > kMaxTextToPersistInMetadataBytes)
                                               ? content.contentText
                                               : *extractedOpt;
                auto ir = meta->indexDocumentContent(d.id, d.fileName, textForIndex, effectiveMime);

                if (ir && contentResult) {
                    (void)meta->updateDocumentExtractionStatus(d.id, true,
                                                               metadata::ExtractionStatus::Success);
                    result.succeeded++;
                } else {
                    std::string failMsg = !contentResult ? contentResult.error().message
                                                         : (!ir ? ir.error().message : "unknown");
                    (void)meta->updateDocumentExtractionStatus(
                        d.id, false, metadata::ExtractionStatus::Failed, failMsg);
                    result.failed++;
                }
            } else {
                (void)meta->updateDocumentExtractionStatus(
                    d.id, false, metadata::ExtractionStatus::Skipped, "No extractable text");
                result.skipped++;
            }
        } catch (const std::exception& e) {
            (void)meta->updateDocumentExtractionStatus(
                d.id, false, metadata::ExtractionStatus::Failed, e.what());
            result.failed++;
        }
    }

    result.message = "FTS5 rebuild: " + std::to_string(result.succeeded) + " ok, " +
                     std::to_string(result.failed) + " failed, " + std::to_string(result.skipped) +
                     " skipped";
    return result;
}

RepairOperationResult RepairService::generateMissingEmbeddings(const RepairRequest& req,
                                                               ProgressFn progress) {
    RepairOperationResult result;
    result.operation = "embeddings";

    auto meta = services_ ? services_->getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata not available";
        return result;
    }

    auto docs = metadata::queryDocumentsByPattern(*meta, "%");
    if (!docs) {
        result.message = "Failed to query";
        return result;
    }

    // Filter for embeddable MIME types
    auto isEmbeddable = [&req](const std::string& m) -> bool {
        if (m.rfind("text/", 0) == 0)
            return true;
        if (m == "application/json" || m == "application/xml" || m == "application/x-yaml" ||
            m == "application/yaml")
            return true;
        for (const auto& inc : req.includeMime)
            if (!inc.empty() && (m == inc || m.rfind(inc, 0) == 0))
                return true;
        return false;
    };

    std::vector<std::string> hashes;
    for (const auto& d : docs.value()) {
        if (isEmbeddable(d.mimeType))
            hashes.push_back(d.sha256Hash);
    }

    // Incremental mode: skip documents that already have embeddings.
    // Batch-fetch all embedded hashes in a single query for efficiency.
    // Use --force to regenerate embeddings for all documents unconditionally.
    size_t skippedExisting = 0;
    if (!req.force) {
        auto vectorDb = services_ ? services_->getVectorDatabase() : nullptr;
        if (vectorDb) {
            auto embeddedHashes = vectorDb->getEmbeddedDocumentHashes();
            if (!embeddedHashes.empty()) {
                std::vector<std::string> missing;
                missing.reserve(hashes.size());
                for (auto& h : hashes) {
                    if (embeddedHashes.count(h) == 0) {
                        missing.push_back(std::move(h));
                    } else {
                        ++skippedExisting;
                    }
                }
                hashes = std::move(missing);
            }
        }
    }

    result.processed = hashes.size() + skippedExisting;
    result.skipped = skippedExisting;

    if (hashes.empty()) {
        if (skippedExisting > 0) {
            result.message = "All " + std::to_string(skippedExisting) +
                             " eligible documents already have embeddings";
        } else {
            result.message = "No eligible documents";
        }
        return result;
    }

    if (req.dryRun) {
        result.skipped += hashes.size();
        result.message = "Would generate embeddings for " + std::to_string(hashes.size()) + " docs";
        if (skippedExisting > 0) {
            result.message +=
                " (" + std::to_string(skippedExisting) + " already embedded, skipped)";
        }
        return result;
    }

    // Queue via InternalEventBus for the embedding pipeline to handle
    const uint32_t embedCap = TuneAdvisor::embedChannelCapacity();
    auto embedQ = InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
        "embed_jobs", embedCap);

    // Split into batches
    size_t batchSize = cfg_.maxBatch;
    for (size_t i = 0; i < hashes.size(); i += batchSize) {
        size_t end = std::min(i + batchSize, hashes.size());
        std::vector<std::string> batch(hashes.begin() + i, hashes.begin() + end);

        InternalEventBus::EmbedJob job{batch, static_cast<uint32_t>(batchSize), true,
                                       req.embeddingModel,
                                       std::vector<InternalEventBus::EmbedPreparedDoc>{}};

        // Retry push with backoff (synchronous version for on-demand repair)
        int retries = 0;
        bool pushed = false;
        while (!pushed && retries < 20) {
            pushed = embedQ->try_push(std::move(job));
            if (!pushed) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(50 * (1 << std::min(retries, 5))));
                retries++;
            }
        }

        if (pushed) {
            result.succeeded += batch.size();
            InternalEventBus::instance().incEmbedQueued();
        } else {
            result.failed += batch.size();
            InternalEventBus::instance().incEmbedDropped();
        }

        if (progress) {
            RepairEvent ev;
            ev.phase = "repairing";
            ev.operation = "embeddings";
            ev.processed = std::min(i + batchSize, hashes.size());
            ev.total = hashes.size();
            ev.succeeded = result.succeeded;
            ev.failed = result.failed;
            progress(ev);
        }
    }

    result.message = "Queued " + std::to_string(result.succeeded) + " docs for embedding";
    if (result.skipped > 0) {
        result.message += " (" + std::to_string(result.skipped) + " already embedded, skipped)";
    }
    return result;
}

RepairOperationResult RepairService::optimizeDatabase(bool dryRun, bool verbose,
                                                      ProgressFn progress) {
    RepairOperationResult result;
    result.operation = "optimize";

    namespace fs = std::filesystem;
    fs::path dbPath = cfg_.dataDir / "yams.db";
    fs::path vecDbPath = cfg_.dataDir / "vectors.db";

    int dbCount = 0;
    if (fs::exists(dbPath))
        dbCount++;
    if (fs::exists(vecDbPath))
        dbCount++;

    if (dbCount == 0) {
        result.message = "No databases found";
        return result;
    }

    result.processed = dbCount;

    if (dryRun) {
        result.skipped = dbCount;
        result.message = "Would optimize " + std::to_string(dbCount) + " database(s)";
        return result;
    }

    int succeeded = 0;
    std::string failMsg;

    // Helper: checkpoint WAL + VACUUM + ANALYZE for a single database
    auto optimizeOne = [&](const fs::path& path, const char* label) {
        sqlite3* db = nullptr;
        if (sqlite3_open(path.string().c_str(), &db) != SQLITE_OK) {
            failMsg += std::string(label) + ": failed to open; ";
            return false;
        }

        // Checkpoint WAL first to reduce its size before VACUUM
        int walLog = 0, walCkpt = 0;
        sqlite3_wal_checkpoint_v2(db, nullptr, SQLITE_CHECKPOINT_TRUNCATE, &walLog, &walCkpt);
        spdlog::info("[Optimize] {} WAL checkpoint: log={} checkpointed={}", label, walLog,
                     walCkpt);

        char* errMsg = nullptr;
        if (sqlite3_exec(db, "VACUUM", nullptr, nullptr, &errMsg) == SQLITE_OK) {
            sqlite3_exec(db, "ANALYZE", nullptr, nullptr, nullptr);
            spdlog::info("[Optimize] {} VACUUM + ANALYZE completed", label);
            sqlite3_close(db);
            return true;
        } else {
            std::string error = errMsg ? errMsg : "Unknown error";
            sqlite3_free(errMsg);
            failMsg += std::string(label) + ": " + error + "; ";
            sqlite3_close(db);
            return false;
        }
    };

    if (fs::exists(dbPath) && optimizeOne(dbPath, "yams.db")) {
        succeeded++;
    }
    if (fs::exists(vecDbPath) && optimizeOne(vecDbPath, "vectors.db")) {
        succeeded++;
    }

    result.succeeded = succeeded;
    result.failed = dbCount - succeeded;

    if (result.failed == 0) {
        result.message = "All databases optimized";
    } else {
        result.message = "Optimize partially failed: " + failMsg;
    }

    return result;
}

} // namespace yams::daemon
