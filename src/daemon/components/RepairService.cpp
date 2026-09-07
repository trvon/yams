#include "repair/repair_operation_support.h"
#include <yams/daemon/components/db_salvage.h>
#include <yams/daemon/components/MetadataWriteFacade.h>
#include <yams/daemon/components/repair/repair_operation.h>
#include <yams/daemon/components/RepairService.h>
#include <yams/daemon/components/WriteCoordinator.h>

#include <yams/compat/thread_stop_compat.h>
#include <yams/config/config_helpers.h>
#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningManager.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/components/VectorIndexCoordinator.h>
#include <yams/daemon/metric_keys.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/content_extractor.h>
#include <yams/extraction/extraction_util.h>
#include <yams/integrity/repair_manager.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_database.h>

#include <yams/core/repair_fsm.h>
#include <yams/daemon/components/ConfigResolver.h>

#include <spdlog/spdlog.h>
#include <yams/profiling.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/thread_pool.hpp>

#include <sqlite3.h>
#include <algorithm>
#include <cctype>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <set>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace yams::daemon {

namespace {

using repair::kMaxTextToPersistInMetadataBytes;
using repair::queueWithBackoff;
using repair::submitRepairStatusUpdate;

uint64_t steadyNowMillis() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::steady_clock::now().time_since_epoch())
                                     .count());
}

uint64_t repairOperationCode(std::string_view operation) {
    return metrics::repairOperationCodeForName(operation);
}

// Check the shared vector compatibility policy used by search and initialization.
bool vectorsDisabledByEnv() {
    return !yams::config::resolve_vector_environment().enabled;
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

private:
    RepairThreadPool() = default;
};

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
            auto bytesRes = contentStore->retrieveBytesPrefix(hash, 8192);
            if (bytesRes) {
                const auto& bytes = bytesRes.value();
                std::span<const std::byte> sample(bytes.data(), bytes.size());
                if (!yams::detection::isBinaryData(sample))
                    return true;
            }
        }
    }
    return false;
}

} // namespace

// ============================================================================
// Construction / Destruction / Lifecycle
// ============================================================================

RepairServiceContext makeRepairServiceContext(ServiceManager* services) {
    RepairServiceContext ctx;
    if (!services)
        return ctx;
    ctx.getMetadataRepo = [services] { return services->getMetadataRepo(); };
    ctx.getContentStore = [services] { return services->getContentStore(); };
    ctx.getVectorDatabase = [services] { return services->getVectorDatabase(); };
    ctx.getKgStore = [services] { return services->getKgStore(); };
    ctx.getGraphComponent = [services] { return services->getGraphComponent(); };
    ctx.getPostIngestQueue = [services] { return services->getPostIngestQueue(); };
    ctx.getRepairManager = [services] { return services->getRepairManager(); };
    ctx.getModelProvider = [services] { return services->getModelProvider(); };
    ctx.getEmbeddingQueuedJobs = [services] { return services->getEmbeddingQueuedJobs(); };
    ctx.getEmbeddingInFlightJobs = [services] { return services->getEmbeddingInFlightJobs(); };
    ctx.getContentExtractors = [services] { return services->getContentExtractors(); };
    ctx.getSymbolExtractors =
        [services]() -> const std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>>& {
        return services->getSymbolExtractors();
    };
    ctx.resolvePreferredModel = [services] { return services->resolvePreferredModel(); };
    ctx.getEmbeddingModelName = [services] { return services->getEmbeddingModelName(); };
    ctx.rebuildTopologyArtifacts = [services](const std::string& reason, bool dryRun,
                                              const std::vector<std::string>& hashes) {
        return services->rebuildTopologyArtifacts(reason, dryRun, hashes);
    };
    ctx.rebuildSemanticNeighborGraph = [services](const std::string& reason,
                                                  const std::string& modelName) {
        return services->rebuildSemanticNeighborGraph(reason, modelName);
    };
    ctx.vectorIndexCoordinator = services->getVectorIndexCoordinator().get();
    ctx.getWriteCoordinator = [services] { return services->getWriteCoordinator(); };
    return ctx;
}

RepairService::RepairService(ServiceManager* services, StateComponent* state,
                             std::function<size_t()> activeConnFn, Config cfg)
    : RepairService(makeRepairServiceContext(services), state, activeConnFn, cfg) {}

RepairService::RepairService(RepairServiceContext ctx, StateComponent* state,
                             std::function<size_t()> activeConnFn, Config cfg)
    : ctx_(ctx), state_(state), activeConnFn_(activeConnFn), cfg_(cfg),
      shutdownState_(std::make_shared<ShutdownState>()) {
    coordinator_ = ctx_.vectorIndexCoordinator;
}

RepairService::~RepairService() {
    stop();
}

std::shared_ptr<metadata::IMetadataRepository> RepairService::getMetadataRepoForRepair() const {
    YAMS_ZONE_SCOPED_N("RepairSvc::getMetadataRepoForRepair");
    if (!ctx_.getMetadataRepo)
        return nullptr;
    return std::static_pointer_cast<metadata::IMetadataRepository>(ctx_.getMetadataRepo());
}

void RepairService::start() {
    YAMS_ZONE_SCOPED_N("RepairSvc::start");
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
                co_await self->backgroundLoop(shutdownState.get());
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
    YAMS_ZONE_SCOPED_N("RepairSvc::stop");
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
    {
        std::unique_lock<std::mutex> lk(activeRepairMutex_);
        activeRepairCv_.wait_for(lk, std::chrono::milliseconds(5000),
                                 [this] { return activeRepairExecutions_ == 0; });
    }
    spdlog::debug("RepairService stopped");
}

// ============================================================================
// Background Loop (ported from RepairCoordinator::runAsync)
// ============================================================================

boost::asio::awaitable<void> RepairService::backgroundLoop(ShutdownState* shutdownState) {
    YAMS_ZONE_SCOPED_N("RepairSvc::backgroundLoop");
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
    std::uint32_t deferTicks = 0;
    const std::uint32_t minDeferTicks = shutdownState->config.initialScanDeferTicks;

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
            if (snap && snap->daemonIdle) {
                pollMs = std::max<uint32_t>(TuneAdvisor::idleTickMs(), pollMs);
            }
            timer.expires_after(std::chrono::milliseconds(std::max<uint32_t>(10, pollMs)));
            co_await timer.async_wait(boost::asio::use_awaitable);
        }

        if (!vectorCleanupDone && maintenanceAllowed()) {
            performVectorCleanup();
            vectorCleanupDone = true;
        }

        // Deferred initial scan
        bool queueEmpty = false;
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
            if (state_)
                state_->stats.repairIdleTicks++;
            timer.expires_after(100ms);
            co_await timer.async_wait(boost::asio::use_awaitable);
            continue;
        }

        if (!running_.load(std::memory_order_relaxed))
            continue;

        if (state_)
            state_->stats.repairBusyTicks++;

        auto [missingEmbeddings, missingFts5] = detectMissingWork(batch);
        auto meta_repo = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;

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
            submitRepairStatusUpdate(ctx_, meta_repo, missingEmbeddings,
                                     yams::metadata::RepairStatus::Processing,
                                     "RepairService::queueEmbeddings/processing");

            InternalEventBus::EmbedJob job{
                missingEmbeddings, static_cast<uint32_t>(shutdownState->config.maxBatch), true,
                std::string{},     std::vector<InternalEventBus::EmbedPreparedDoc>{},     nullptr};
            job.updateSemanticGraph = false;
            static std::shared_ptr<SpscQueue<InternalEventBus::EmbedJob>> embedQ =
                InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
                    "embed_jobs", TuneAdvisor::embedChannelCapacity());
            bool queued = co_await queueWithBackoff(embedQ, std::move(job), timer, running_,
                                                    "embed", missingEmbeddings.size(), 50, 1000);
            if (queued) {
                InternalEventBus::instance().incEmbedQueued();
            } else {
                submitRepairStatusUpdate(ctx_, meta_repo, missingEmbeddings,
                                         yams::metadata::RepairStatus::Pending,
                                         "RepairService::queueEmbeddings/rollback");
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
    YAMS_ZONE_SCOPED_N("RepairSvc::spawnInitialScan");
    try {
        auto meta = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;
        if (!meta)
            co_return;

        const bool vectorsDisabled = vectorsDisabledByEnv();
        const size_t batchSize = TuneAdvisor::repairStartupBatchSize();
        size_t offset = 0;
        size_t totalEnqueued = 0;
        bool queueAtCapacity = false;

        // Preload all FTS5-indexed rowids once for O(1) lookups in the scan loop.
        std::unordered_set<int64_t> ftsRowIds;
        {
            auto ftsRes = meta->getFts5IndexedRowIdSet();
            if (ftsRes)
                ftsRowIds = std::move(ftsRes.value());
        }
        std::unordered_set<std::string> embeddedHashes;
        bool haveEmbeddedSnapshot = false;
        if (!vectorsDisabled) {
            auto vectorDb = ctx_.getVectorDatabase ? ctx_.getVectorDatabase() : nullptr;
            if (vectorDb) {
                embeddedHashes = vectorDb->getEmbeddedDocumentHashes();
                haveEmbeddedSnapshot = true;
            }
        }

        while (running_.load(std::memory_order_relaxed) && !queueAtCapacity) {
            if (!maintenanceAllowed()) {
                break;
            }
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
                    if (d.repairStatus == yams::metadata::RepairStatus::Processing)
                        continue;

                    // NEW: recover stalled Processing on startup
                    // (handled by detectStuckDocuments during on-demand repair)

                    bool missingEmb = false;
                    if (!vectorsDisabled) {
                        if (haveEmbeddedSnapshot) {
                            missingEmb = embeddedHashes.find(d.sha256Hash) == embeddedHashes.end();
                        } else {
                            auto hasEmbedRes = meta->hasDocumentEmbeddingByHash(d.sha256Hash);
                            missingEmb = !hasEmbedRes || !hasEmbedRes.value();
                        }
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
                        if (pendingDocuments_.size() >= cfg_.maxPendingRepairs) {
                            queueAtCapacity = true;
                            break;
                        }
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
    YAMS_ZONE_SCOPED_N("RepairSvc::processPathTreeRepair");
    using namespace std::chrono_literals;
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    auto repairMgr = ctx_.getRepairManager ? ctx_.getRepairManager() : nullptr;
    auto metaRepo = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;
    if (!repairMgr || !metaRepo)
        co_return;

    try {
        auto docsResult = metaRepo->queryDocuments(metadata::DocumentQueryOptions{});
        if (!docsResult || docsResult.value().empty())
            co_return;

        uint64_t created = 0, errors = 0;
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
    YAMS_ZONE_SCOPED_N("RepairSvc::performVectorCleanup");
    if (vectorsDisabledByEnv())
        return;
    try {
        auto vectorDb = ctx_.getVectorDatabase ? ctx_.getVectorDatabase() : nullptr;
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

RepairService::MissingWorkFlags RepairService::analyzeMissingWorkForHash(
    const std::string& hash, bool checkEmbeddings,
    const std::shared_ptr<api::IContentStore>& contentStore,
    const std::shared_ptr<metadata::MetadataRepository>& metaRepo,
    const std::vector<std::shared_ptr<extraction::IContentExtractor>>& customExtractors) const {
    MissingWorkFlags flags;
    if (!metaRepo) {
        return flags;
    }

    if (checkEmbeddings) {
        auto hasEmbedRes = metaRepo->hasDocumentEmbeddingByHash(hash);
        flags.missingEmbedding = !hasEmbedRes || !hasEmbedRes.value();
    }

    auto docRes = metaRepo->getDocumentByHash(hash);
    if (!(docRes && docRes.value().has_value())) {
        return flags;
    }

    const auto& d = docRes.value().value();
    if (d.repairStatus == yams::metadata::RepairStatus::Processing) {
        return flags;
    }

    if (!d.contentExtracted || d.extractionStatus != yams::metadata::ExtractionStatus::Success) {
        flags.missingFts5 =
            canExtractDocument(d.mimeType, d.fileExtension, customExtractors, contentStore, hash);
        return flags;
    }

    auto ftsRes = metaRepo->hasFtsEntry(d.id);
    flags.missingFts5 = ftsRes && !ftsRes.value();
    return flags;
}

RepairService::MissingWorkResult
RepairService::detectMissingWork(const std::vector<std::string>& batch) {
    MissingWorkResult result;
    auto content = ctx_.getContentStore ? ctx_.getContentStore() : nullptr;
    auto meta_repo = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;
    if (!(content && meta_repo))
        return result;

    const bool checkEmbeddings = !vectorsDisabledByEnv();
    auto customExtractors = ctx_.getContentExtractors
                                ? ctx_.getContentExtractors()
                                : std::vector<std::shared_ptr<extraction::IContentExtractor>>{};

    std::vector<MissingWorkFlags> flags(batch.size());
    std::vector<std::exception_ptr> errors(batch.size());

    for (std::size_t i = 0; i < batch.size(); ++i) {
        try {
            flags[i] = analyzeMissingWorkForHash(batch[i], checkEmbeddings, content, meta_repo,
                                                 customExtractors);
        } catch (...) {
            errors[i] = std::current_exception();
        }
    }

    for (std::size_t i = 0; i < batch.size(); ++i) {
        if (errors[i]) {
            try {
                std::rethrow_exception(errors[i]);
            } catch (const std::exception& e) {
                spdlog::warn("RepairService: detectMissingWork failed for {}: {}", batch[i],
                             e.what());
            } catch (...) {
                spdlog::warn("RepairService: detectMissingWork failed for {}", batch[i]);
            }
            continue;
        }
        if (flags[i].missingEmbedding) {
            result.missingEmbeddings.push_back(batch[i]);
        }
        if (flags[i].missingFts5) {
            result.missingFts5.push_back(batch[i]);
        }
    }

    if (!result.missingEmbeddings.empty()) {
        auto provider = ctx_.getModelProvider ? ctx_.getModelProvider() : nullptr;
        auto vectorDb = ctx_.getVectorDatabase ? ctx_.getVectorDatabase() : nullptr;
        if (provider && provider->isAvailable()) {
            size_t modelDim = 0;
            if (ctx_.resolvePreferredModel) {
                const auto preferredModel = ctx_.resolvePreferredModel();
                if (!preferredModel.empty())
                    modelDim = provider->getEmbeddingDim(preferredModel);
            }
            if (modelDim == 0)
                modelDim = provider->getEmbeddingDim("");

            size_t storedDim = 0;
            if (vectorDb)
                storedDim = vectorDb->getConfig().embedding_dim;

            if (modelDim > 0 && storedDim > 0 && modelDim != storedDim) {
                static std::atomic<bool> dimMismatchLogged{false};
                if (!dimMismatchLogged.exchange(true)) {
                    spdlog::warn("RepairService: dim mismatch (model={} db={}); "
                                 "skipping automatic vector-table rebuild. Run an explicit "
                                 "foreground vector/embedding repair after choosing the target "
                                 "dimension.",
                                 modelDim, storedDim);
                }
                result.missingEmbeddings.clear();
            }
        }
    }

    return result;
}

void RepairService::updateProgressPct() {
    YAMS_ZONE_SCOPED_N("RepairSvc::updateProgressPct");
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

void RepairService::beginRepairOperation(std::string_view operation) {
    YAMS_ZONE_SCOPED_N("RepairSvc::beginRepairOperation");
    if (!state_)
        return;
    state_->stats.repairCurrentOperationCode.store(repairOperationCode(operation),
                                                   std::memory_order_relaxed);
    state_->stats.repairCurrentOperationStartedMs.store(steadyNowMillis(),
                                                        std::memory_order_relaxed);
}

void RepairService::endRepairOperation() {
    YAMS_ZONE_SCOPED_N("RepairSvc::endRepairOperation");
    if (!state_)
        return;
    state_->stats.repairCurrentOperationCode.store(0, std::memory_order_relaxed);
    state_->stats.repairCurrentOperationStartedMs.store(0, std::memory_order_relaxed);
}

// ============================================================================
// On-demand Repair (RPC entry point)
// ============================================================================

RepairService::RepairExecutionGuard::RepairExecutionGuard(RepairService& owner) {
    bool expected = false;
    if (!owner.repairInProgress_.compare_exchange_strong(expected, true,
                                                         std::memory_order_acq_rel)) {
        return;
    }

    try {
        std::lock_guard<std::mutex> lock(owner.activeRepairMutex_);
        ++owner.activeRepairExecutions_;
    } catch (...) {
        owner.repairInProgress_.store(false, std::memory_order_release);
        throw;
    }
    owner_ = &owner;
    if (owner.state_) {
        owner.state_->stats.repairInProgress.store(true, std::memory_order_relaxed);
    }
}

RepairService::RepairExecutionGuard::~RepairExecutionGuard() {
    if (!owner_) {
        return;
    }

    owner_->repairInProgress_.store(false, std::memory_order_release);
    if (owner_->state_) {
        owner_->state_->stats.repairInProgress.store(false, std::memory_order_relaxed);
    }
    owner_->endRepairOperation();
    {
        std::lock_guard<std::mutex> lock(owner_->activeRepairMutex_);
        if (owner_->activeRepairExecutions_ > 0) {
            --owner_->activeRepairExecutions_;
        }
    }
    owner_->activeRepairCv_.notify_all();
}

RepairService::RepairRun::RepairRun(RepairService& owner, ProgressFn progress,
                                    std::atomic<bool>* cancelRequested)
    : owner_(owner), progress_(std::move(progress)), cancelRequested_(cancelRequested) {}

bool RepairService::RepairRun::isCanceled() const noexcept {
    return cancelRequested_ && cancelRequested_->load(std::memory_order_relaxed);
}

void RepairService::RepairRun::emitCancellation(std::string_view operation) {
    errors_.push_back("Repair canceled");
    if (!progress_) {
        return;
    }
    RepairEvent event;
    event.phase = "error";
    event.operation = operation;
    event.message = "Repair canceled";
    progress_(event);
}

bool RepairService::RepairRun::begin(std::string_view operation) {
    if (isCanceled()) {
        emitCancellation(operation);
        return false;
    }
    if (progress_) {
        RepairEvent event;
        event.phase = "repairing";
        event.operation = operation;
        event.message = "Starting " + std::string(operation) + "...";
        progress_(event);
    }
    owner_.beginRepairOperation(operation);
    return true;
}

bool RepairService::RepairRun::complete(std::string_view operation, RepairOperationResult result) {
    owner_.endRepairOperation();
    ++response_.totalOperations;
    response_.totalSucceeded += result.succeeded;
    response_.totalFailed += result.failed;
    response_.totalSkipped += result.skipped;
    if (result.failed > 0 && !result.message.empty()) {
        errors_.push_back(std::string(operation) + ": " + result.message);
    }
    if (progress_) {
        RepairEvent event;
        event.phase = "completed";
        event.operation = operation;
        event.processed = result.processed;
        event.succeeded = result.succeeded;
        event.failed = result.failed;
        event.skipped = result.skipped;
        event.message = result.message;
        progress_(event);
    }
    results_.push_back(std::move(result));
    if (isCanceled()) {
        emitCancellation(operation);
        return false;
    }
    return true;
}

RepairResponse RepairService::RepairRun::finish() {
    response_.success = errors_.empty();
    response_.errors = std::move(errors_);
    response_.operationResults = std::move(results_);
    return std::move(response_);
}

std::vector<RepairService::OnDemandRepairOperation>
RepairService::buildRepairPlan(const RepairRequest& request) {
    std::vector<OnDemandRepairOperation> operations;
    operations.reserve(13);
    auto include = [&](bool enabled, OnDemandRepairOperation operation) {
        if (enabled) {
            operations.push_back(operation);
        }
    };
    include(request.repairStuckDocs || request.repairAll, OnDemandRepairOperation::StuckDocuments);
    include(request.repairOrphans || request.repairAll, OnDemandRepairOperation::Orphans);
    include(request.repairMime || request.repairAll, OnDemandRepairOperation::Mime);
    include(request.repairDownloads || request.repairAll, OnDemandRepairOperation::Downloads);
    include(request.repairPathTree || request.repairAll, OnDemandRepairOperation::PathTree);
    include(request.repairDedupe || request.repairAll, OnDemandRepairOperation::Dedupe);
    include(request.repairChunks || request.repairAll, OnDemandRepairOperation::Chunks);
    include(request.repairBlockRefs || request.repairAll, OnDemandRepairOperation::BlockReferences);
    include(request.repairGraph || request.repairAll, OnDemandRepairOperation::Graph);
    include(request.repairFts5 || request.repairAll, OnDemandRepairOperation::Fts5);
    include(request.repairEmbeddings || request.repairAll, OnDemandRepairOperation::Embeddings);
    include(request.repairTopology, OnDemandRepairOperation::Topology);
    include(request.optimizeDb || request.repairAll, OnDemandRepairOperation::Optimize);
    return operations;
}

std::string_view RepairService::repairOperationName(OnDemandRepairOperation operation) noexcept {
    const auto* op = repair::repairOperationForCode(static_cast<std::uint64_t>(operation) + 1);
    return op ? op->name() : std::string_view{"unknown"};
}

RepairOperationResult RepairService::runRepairOperation(OnDemandRepairOperation operation,
                                                        const RepairRequest& request,
                                                        const ProgressFn& progress,
                                                        std::atomic<bool>* cancelRequested) {
    static_assert(static_cast<std::uint64_t>(OnDemandRepairOperation::Optimize) + 1 ==
                      metrics::kRepairOperationCodes.size(),
                  "OnDemandRepairOperation must mirror metrics::kRepairOperationCodes");
    auto* op = repair::repairOperationForCode(static_cast<std::uint64_t>(operation) + 1);
    if (!op) {
        RepairOperationResult result;
        result.operation = "unknown";
        result.failed = 1;
        result.message = "Unknown repair operation";
        return result;
    }
    repair::OperationEnv env{ctx_, cfg_, state_, running_};
    return op->run(env, request, progress, cancelRequested);
}

RepairResponse RepairService::executeRepair(const RepairRequest& request, ProgressFn progress,
                                            std::atomic<bool>* cancelRequested) {
    RepairExecutionGuard executionGuard(*this);
    if (!executionGuard) {
        RepairResponse busy;
        busy.success = false;
        busy.errors.push_back(
            "Repair is already in progress. Please wait for the current run to finish.");
        spdlog::info("[RepairService] executeRepair rejected: repair already in progress");
        return busy;
    }

    RepairRun run(*this, std::move(progress), cancelRequested);
    for (const auto operation : buildRepairPlan(request)) {
        const auto name = repairOperationName(operation);
        if (!run.begin(name)) {
            break;
        }
        auto result = runRepairOperation(operation, request, run.progress(), cancelRequested);
        if (!run.complete(name, std::move(result))) {
            break;
        }
    }
    return run.finish();
}

boost::asio::awaitable<RepairResponse>
RepairService::executeRepairAsync(const RepairRequest& request, ProgressFn progress,
                                  std::atomic<bool>* cancelRequested) {
    RepairExecutionGuard executionGuard(*this);
    if (!executionGuard) {
        RepairResponse busy;
        busy.success = false;
        busy.errors.push_back(
            "Repair is already in progress. Please wait for the current run to finish.");
        spdlog::info("[RepairService] executeRepairAsync rejected: repair already in progress");
        co_return busy;
    }

    RepairRun run(*this, std::move(progress), cancelRequested);
    for (const auto operation : buildRepairPlan(request)) {
        const auto name = repairOperationName(operation);
        if (!run.begin(name)) {
            break;
        }

        RepairOperationResult result;
        auto* op = repair::repairOperationForCode(static_cast<std::uint64_t>(operation) + 1);
        if (op && !request.foreground) {
            repair::OperationEnv env{ctx_, cfg_, state_, running_};
            result = co_await op->runAsync(env, request, run.progress(), cancelRequested);
        } else {
            result = runRepairOperation(operation, request, run.progress(), cancelRequested);
        }

        if (!run.complete(name, std::move(result))) {
            break;
        }
    }
    co_return run.finish();
}

// ============================================================================
// Stuck Document Recovery (NEW)
// ============================================================================

// ============================================================================
// Core Repair Operations
// ============================================================================

} // namespace yams::daemon
