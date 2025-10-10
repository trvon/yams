#include <yams/compat/thread_stop_compat.h>
#include <yams/daemon/components/RepairCoordinator.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/metadata/query_helpers.h>

#include <spdlog/spdlog.h>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <yams/core/repair_fsm.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/ipc/repair_scheduling_adapter.h>
#include <yams/extraction/content_extractor.h>
#include <yams/extraction/extraction_util.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/vector/vector_database.h>
// removed temporary maintenance lock coordination
// Asio for coroutine-based loop
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/thread_pool.hpp>

namespace yams::daemon {

namespace {
// Dedicated thread pool for repair operations to avoid blocking worker pool
boost::asio::any_io_executor repair_dedicated_executor() {
    // Use 2 threads: one for coordination, one for DB queries
    static boost::asio::thread_pool pool(2);
    return pool.get_executor();
}
} // namespace

RepairCoordinator::RepairCoordinator(ServiceManager* services, StateComponent* state,
                                     std::function<size_t()> activeConnFn, Config cfg)
    : services_(services), state_(state), activeConnFn_(std::move(activeConnFn)), cfg_(cfg) {}

RepairCoordinator::~RepairCoordinator() {
    stop();
}

void RepairCoordinator::start() {
    if (!cfg_.enable || running_.exchange(true)) {
        return;
    }
    // Initialize maintenance tokens based on config
    tokens_.store(cfg_.maintenanceTokens);
    finished_.store(false, std::memory_order_relaxed);
    // CRITICAL: Use dedicated executor to avoid blocking worker pool
    auto exec = repair_dedicated_executor();
    boost::asio::co_spawn(
        exec,
        [this]() -> boost::asio::awaitable<void> {
            co_await runAsync();
            co_return;
        },
        boost::asio::detached);
    spdlog::info("RepairCoordinator started (awaitable) (enable={}, batch={}, dedicated_threads=2)",
                 cfg_.enable, cfg_.maxBatch);
}

void RepairCoordinator::stop() {
    if (!running_.exchange(false)) {
        return;
    }
    // Clear services pointer to avoid use-after-free races during shutdown.
    services_ = nullptr;
    queueCv_.notify_all();
    // Best-effort wait for coroutine to signal completion
    {
        std::unique_lock<std::mutex> lk(doneMutex_);
        (void)doneCv_.wait_for(lk, std::chrono::milliseconds(1500),
                               [&] { return finished_.load(std::memory_order_relaxed); });
    }
}

bool RepairCoordinator::maintenance_allowed() const {
    if (!activeConnFn_)
        return false;
    return activeConnFn_() == 0;
}

void RepairCoordinator::onDocumentAdded(const DocumentAddedEvent& event) {
    if (!cfg_.enable || !running_) {
        return;
    }

    // If the main post-ingest pipeline is active, it will schedule embeddings
    // immediately after text extraction. Avoid duplicate scheduling here.
    try {
        if (services_ && services_->getPostIngestQueue()) {
            spdlog::debug(
                "RepairCoordinator: skipping DocumentAdded {} — handled by PostIngestQueue",
                event.hash);
            return;
        }
    } catch (...) {
        // fall through to best-effort queue if introspection fails
    }

    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        pendingDocuments_.push(event.hash);
        if (state_)
            state_->stats.repairQueueDepth.store(static_cast<uint64_t>(pendingDocuments_.size()));
    }
    queueCv_.notify_one();
    spdlog::debug("RepairCoordinator: queued document {} for embedding check", event.hash);
}

void RepairCoordinator::onDocumentRemoved(const DocumentRemovedEvent& event) {
    if (!cfg_.enable || !running_) {
        return;
    }

    // For now, just log - in future could clean up orphaned embeddings
    spdlog::debug("RepairCoordinator: document {} removed", event.hash);
}

boost::asio::awaitable<void> RepairCoordinator::runAsync() {
    using namespace std::chrono_literals;
    auto ex = co_await boost::asio::this_coro::executor;
    boost::asio::steady_timer timer(ex);

    core::RepairFsm::Config fsmConfig;
    fsmConfig.enable_online_repair = true;
    fsmConfig.max_repair_concurrency = cfg_.maintenanceTokens;
    fsmConfig.repair_backoff_ms = 250;
    fsmConfig.max_retries = 3;

    core::RepairFsm fsm(fsmConfig);
    fsm.set_on_state_change([](core::RepairFsm::State state) {
        spdlog::debug("RepairFsm state changed to: {}", core::RepairFsm::to_string(state));
    });

    bool initialScanEnqueued = false;
    // Defer initial scan to avoid blocking during startup
    int deferTicks = 0;
    const int minDeferTicks = 50; // ~5 seconds at 100ms/tick

    while (running_.load(std::memory_order_relaxed)) {
        // Skip initial scan entirely if maintenance not allowed or still in startup grace period
        if (pendingDocuments_.empty() && !initialScanEnqueued) {
            if (deferTicks < minDeferTicks) {
                ++deferTicks;
            } else if (maintenance_allowed()) {
                // Initial scan is now DISABLED by default to prevent startup blocking
                // on large databases. Documents will be repaired on-demand via event bus.
                // To enable, set YAMS_REPAIR_INITIAL_SCAN=1
                bool enableInitialScan = false;
                try {
                    if (const char* env = std::getenv("YAMS_REPAIR_INITIAL_SCAN")) {
                        std::string v(env);
                        std::transform(v.begin(), v.end(), v.begin(), ::tolower);
                        enableInitialScan = (v == "1" || v == "true" || v == "yes");
                    }
                } catch (...) {
                }

                if (enableInitialScan) {
                    spdlog::info(
                        "RepairCoordinator: starting async initial scan (batched, non-blocking)");
                    // Spawn async scan on dedicated executor to avoid blocking this coroutine
                    auto scanExec = repair_dedicated_executor();
                    boost::asio::co_spawn(
                        scanExec,
                        [this]() -> boost::asio::awaitable<void> {
                            try {
                                auto meta = services_ ? services_->getMetadataRepo() : nullptr;
                                auto vectorDb =
                                    services_ ? services_->getVectorDatabase() : nullptr;
                                if (!meta || !vectorDb) {
                                    co_return;
                                }

                                // Query in batches to avoid locking DB for too long
                                const size_t batchSize = 1000;
                                size_t offset = 0;
                                size_t totalEnqueued = 0;

                                while (running_.load(std::memory_order_relaxed)) {
                                    // Query batch of documents (limit + offset)
                                    metadata::DocumentQueryOptions opts;
                                    opts.limit = static_cast<int>(batchSize);
                                    opts.offset = static_cast<int>(offset);
                                    auto batchDocs = meta->queryDocuments(opts);

                                    if (!batchDocs || batchDocs.value().empty()) {
                                        break; // No more documents
                                    }

                                    size_t batchEnqueued = 0;
                                    {
                                        std::lock_guard<std::mutex> ql(queueMutex_);
                                        for (const auto& d : batchDocs.value()) {
                                            if (!running_.load(std::memory_order_relaxed))
                                                break;

                                            // Check if document needs repair
                                            const bool missingEmb =
                                                !vectorDb->hasEmbedding(d.sha256Hash);
                                            const bool missingFts =
                                                (!d.contentExtracted) ||
                                                (d.extractionStatus !=
                                                 yams::metadata::ExtractionStatus::Success);

                                            if (missingEmb || missingFts) {
                                                pendingDocuments_.push(d.sha256Hash);
                                                ++batchEnqueued;
                                            }
                                        }
                                    }

                                    totalEnqueued += batchEnqueued;
                                    offset += batchDocs.value().size();

                                    // Yield between batches to let other work proceed
                                    co_await boost::asio::post(
                                        co_await boost::asio::this_coro::executor,
                                        boost::asio::use_awaitable);

                                    // Stop if batch was incomplete (reached end)
                                    if (batchDocs.value().size() < batchSize) {
                                        break;
                                    }
                                }

                                if (totalEnqueued > 0) {
                                    totalBacklog_.store(totalEnqueued, std::memory_order_relaxed);
                                    processed_.store(0, std::memory_order_relaxed);
                                    spdlog::info("RepairCoordinator: async scan complete, queued "
                                                 "{} documents from {} total",
                                                 totalEnqueued, offset);
                                } else {
                                    spdlog::info("RepairCoordinator: async scan complete, no "
                                                 "documents need repair (scanned {})",
                                                 offset);
                                }
                            } catch (const std::exception& e) {
                                spdlog::warn("RepairCoordinator: async scan exception: {}",
                                             e.what());
                            }
                            co_return;
                        },
                        boost::asio::detached);
                }
                initialScanEnqueued = true;
            }
        }

        std::vector<std::string> batch;
        {
            std::lock_guard<std::mutex> lk(queueMutex_);
            while (!pendingDocuments_.empty() && batch.size() < cfg_.maxBatch) {
                batch.push_back(std::move(pendingDocuments_.front()));
                pendingDocuments_.pop();
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

        // Build hints
        RepairSchedulingAdapter::SchedulingHints adapterHints{};
        size_t active = activeConnFn_ ? activeConnFn_() : 0;
        adapterHints.streaming_high_load = (active > 0);
        adapterHints.maintenance_allowed = maintenance_allowed();
        adapterHints.closing = !running_.load(std::memory_order_relaxed);
        core::RepairFsm::SchedulingHints fsmHints{adapterHints.streaming_high_load,
                                                  adapterHints.maintenance_allowed,
                                                  adapterHints.closing};
        fsm.set_scheduling_hints(fsmHints);
        if (adapterHints.closing || (adapterHints.streaming_high_load && batch.empty())) {
            if (state_)
                state_->stats.repairBusyTicks++;
            continue;
        }

        if (state_)
            state_->stats.repairIdleTicks++;
        auto content = services_ ? services_->getContentStore() : nullptr;
        auto meta_repo = services_ ? services_->getMetadataRepo() : nullptr;
        auto embed = services_ ? services_->getEmbeddingGenerator() : nullptr;
        if (!(content && meta_repo && embed)) {
            continue;
        }

        if (fsm.start()) {
            fsm.on_scan_done();
            std::vector<std::string> missing;
            auto vectorDb = services_ ? services_->getVectorDatabase() : nullptr;
            if (vectorDb) {
                for (const auto& hash : batch) {
                    if (!vectorDb->hasEmbedding(hash))
                        missing.push_back(hash);
                }
            }
            fsm.on_detect_done();
            if (!missing.empty()) {
                fsm.on_classify_done();
                fsm.on_isolate_done();
                if (state_)
                    state_->stats.repairBatchesAttempted++;

                // ALWAYS use InternalEventBus (non-blocking, no IPC overhead)
                std::unique_ptr<yams::Result<repair::EmbeddingRepairStats>> statsPtr;
                bool fixSuccess = false;
                if (try_acquire_token()) {
                    InternalEventBus::EmbedJob job{
                        missing, static_cast<uint32_t>(cfg_.maxBatch),
                        true,         // skipExisting
                        std::string{} // modelName (use default)
                    };
                    static std::shared_ptr<SpscQueue<InternalEventBus::EmbedJob>> q =
                        InternalEventBus::instance()
                            .get_or_create_channel<InternalEventBus::EmbedJob>("embed_jobs", 1024);

                    if (!q->try_push(std::move(job))) {
                        spdlog::warn(
                            "RepairCoordinator: embed job queue full; dropping batch size={}",
                            missing.size());
                        InternalEventBus::instance().incEmbedDropped();
                    } else {
                        spdlog::debug(
                            "RepairCoordinator: queued embed job to InternalEventBus ({} docs)",
                            missing.size());
                        InternalEventBus::instance().incEmbedQueued();
                        fixSuccess = true;
                    }

                    // Stats are estimated since actual work is async
                    repair::EmbeddingRepairStats statsLocal{};
                    statsLocal.documentsProcessed = static_cast<uint32_t>(missing.size());
                    statsLocal.embeddingsGenerated = 0;
                    statsLocal.embeddingsSkipped = 0;
                    statsLocal.failedOperations =
                        fixSuccess ? 0 : static_cast<uint32_t>(missing.size());
                    statsPtr =
                        std::make_unique<yams::Result<repair::EmbeddingRepairStats>>(statsLocal);

                    release_token();
                }

                fsm.on_fix_done(fixSuccess);
                if (fixSuccess) {
                    fsm.on_verify_done(true);
                    try {
                        auto store = services_ ? services_->getContentStore() : nullptr;
                        auto meta = services_ ? services_->getMetadataRepo() : nullptr;
                        const auto& extractors =
                            services_ ? services_->getContentExtractors()
                                      : std::vector<
                                            std::shared_ptr<yams::extraction::IContentExtractor>>{};
                        if (store && meta) {
                            size_t fts_ok = 0, fts_fail = 0;
                            for (const auto& h : missing) {
                                auto docRes = meta->getDocumentByHash(h);
                                if (!docRes || !docRes.value().has_value()) {
                                    ++fts_fail;
                                    continue;
                                }
                                const auto& d = docRes.value().value();
                                std::string ext = d.fileExtension;
                                if (!ext.empty() && ext[0] == '.')
                                    ext.erase(0, 1);
                                auto extractedOpt = yams::extraction::util::extractDocumentText(
                                    store, h, d.mimeType, ext, extractors);
                                if (!extractedOpt || extractedOpt->empty()) {
                                    ++fts_fail;
                                    continue;
                                }
                                auto ir = meta->indexDocumentContent(d.id, d.fileName,
                                                                     *extractedOpt, d.mimeType);
                                if (ir) {
                                    (void)meta->updateFuzzyIndex(d.id);
                                    ++fts_ok;
                                } else {
                                    ++fts_fail;
                                }
                            }
                            spdlog::info("RepairCoordinator: FTS5 reindex complete for batch "
                                         "(ok={}, fail={})",
                                         fts_ok, fts_fail);
                        }
                    } catch (const std::exception& e) {
                        spdlog::debug("RepairCoordinator: FTS5 reindex exception: {}", e.what());
                    }
                    fsm.on_reindex_done(true);
                    if (state_ && statsPtr) {
                        state_->stats.repairEmbeddingsGenerated +=
                            static_cast<uint64_t>(statsPtr->value().embeddingsGenerated);
                        state_->stats.repairEmbeddingsSkipped +=
                            static_cast<uint64_t>(statsPtr->value().embeddingsSkipped);
                        state_->stats.repairFailedOperations +=
                            static_cast<uint64_t>(statsPtr->value().failedOperations);
                        auto inc =
                            static_cast<std::uint64_t>(statsPtr->value().embeddingsGenerated +
                                                       statsPtr->value().embeddingsSkipped);
                        if (inc > 0) {
                            processed_.fetch_add(inc, std::memory_order_relaxed);
                            update_progress_pct();
                        }
                    }
                } else {
                    // No embedding work performed; still reindex FTS5 for docs lacking extracted
                    // text
                    try {
                        auto store = services_ ? services_->getContentStore() : nullptr;
                        auto meta = services_ ? services_->getMetadataRepo() : nullptr;
                        const auto& extractors =
                            services_ ? services_->getContentExtractors()
                                      : std::vector<
                                            std::shared_ptr<yams::extraction::IContentExtractor>>{};
                        if (store && meta) {
                            size_t fts_ok = 0, fts_fail = 0;
                            for (const auto& h : batch) {
                                auto docRes = meta->getDocumentByHash(h);
                                if (!docRes || !docRes.value().has_value()) {
                                    ++fts_fail;
                                    continue;
                                }
                                const auto& d = docRes.value().value();
                                if (d.contentExtracted &&
                                    d.extractionStatus ==
                                        yams::metadata::ExtractionStatus::Success) {
                                    continue;
                                }
                                std::string ext = d.fileExtension;
                                if (!ext.empty() && ext[0] == '.')
                                    ext.erase(0, 1);
                                auto extractedOpt = yams::extraction::util::extractDocumentText(
                                    store, h, d.mimeType, ext, extractors);
                                if (!extractedOpt || extractedOpt->empty()) {
                                    ++fts_fail;
                                    continue;
                                }
                                auto ir = meta->indexDocumentContent(d.id, d.fileName,
                                                                     *extractedOpt, d.mimeType);
                                if (ir) {
                                    (void)meta->updateFuzzyIndex(d.id);
                                    ++fts_ok;
                                } else {
                                    ++fts_fail;
                                }
                            }
                            if (fts_ok + fts_fail > 0) {
                                spdlog::info(
                                    "RepairCoordinator: FTS5-only reindex (ok={}, fail={})", fts_ok,
                                    fts_fail);
                            }
                        }
                    } catch (const std::exception& e) {
                        spdlog::debug("RepairCoordinator: FTS5-only reindex exception: {}",
                                      e.what());
                    }
                }
            }
        }
    }

    spdlog::info("RepairCoordinator stopped");
    finished_.store(true, std::memory_order_relaxed);
    doneCv_.notify_all();
    co_return;
}
#if 0
boost::asio::awaitable<void> RepairCoordinator::runAsync() {
    auto ex = co_await boost::asio::this_coro::executor;
    boost::asio::steady_timer timer(ex);
    core::RepairFsm::Config fsmConfig;
    fsmConfig.enable_online_repair = true;
    fsmConfig.max_repair_concurrency = cfg_.maintenanceTokens;
    fsmConfig.repair_backoff_ms = 250;
    fsmConfig.max_retries = 3;

    core::RepairFsm fsm(fsmConfig);
    fsm.set_on_state_change([](core::RepairFsm::State state) {
        spdlog::debug("RepairFsm state changed to: {}", core::RepairFsm::to_string(state));
    });

    bool initialScanEnqueued = false;
    while (running_.load(std::memory_order_relaxed)) {
        // Wait for work (no timeout - purely event driven)
        std::unique_lock<std::mutex> lock(queueMutex_);
        // If no pending work yet, opportunistically enqueue an initial backlog scan once
        if (pendingDocuments_.empty() && !initialScanEnqueued && maintenance_allowed()) {
            lock.unlock();
            try {
                auto meta = services_ ? services_->getMetadataRepo() : nullptr;
                if (meta) {
                    // Scan all docs to find those without embeddings
                    auto vectorDb = services_ ? services_->getVectorDatabase() : nullptr;
                    if (vectorDb) {
                        auto allDocs = metadata::queryDocumentsByPattern(*meta, "%");
                        if (allDocs && !allDocs.value().empty()) {
                            size_t enq = 0;
                            {
                                std::lock_guard<std::mutex> ql(queueMutex_);
                                for (const auto& d : allDocs.value()) {
                                    if (!running_.load(std::memory_order_relaxed))
                                        break;
                                    if (!vectorDb->hasEmbedding(d.sha256Hash)) {
                                        pendingDocuments_.push(d.sha256Hash);
                                        ++enq;
                                    }
                                }
                            }
                            if (enq > 0) {
                                spdlog::info(
                                    "RepairCoordinator: enqueued {} backlog documents for repair",
                                    enq);
                                totalBacklog_.store(static_cast<std::uint64_t>(enq),
                                                    std::memory_order_relaxed);
                                processed_.store(0, std::memory_order_relaxed);
                                update_progress_pct();
                                queueCv_.notify_one();
                            }
                        }
                    }
                }
            } catch (const std::exception& e) {
                spdlog::debug("Initial backlog scan failed: {}", e.what());
            }
            initialScanEnqueued = true;
            // Re-acquire lock before proceeding to wait
            lock.lock();
        }

        queueCv_.wait(lock, [this] { return !pendingDocuments_.empty() || !running_.load(std::memory_order_relaxed); });
        if (!running_.load(std::memory_order_relaxed)) break;

        // Collect batch of documents to process
        std::vector<std::string> batch;
        while (!pendingDocuments_.empty() && batch.size() < cfg_.maxBatch) {
            batch.push_back(pendingDocuments_.front());
            pendingDocuments_.pop();
        }
        // Update queue depth after dequeueing
        if (state_)
            state_->stats.repairQueueDepth.store(static_cast<uint64_t>(pendingDocuments_.size()));
        lock.unlock();

        // Check scheduling hints
        RepairSchedulingAdapter::SchedulingHints adapterHints{};
        size_t active = activeConnFn_ ? activeConnFn_() : 0;
        adapterHints.streaming_high_load = (active > 0);
        adapterHints.maintenance_allowed = maintenance_allowed();
        adapterHints.closing = !running_.load(std::memory_order_relaxed);

        // Convert to RepairFsm hints
        core::RepairFsm::SchedulingHints fsmHints;
        fsmHints.streaming_high_load = adapterHints.streaming_high_load;
        fsmHints.maintenance_allowed = adapterHints.maintenance_allowed;
        fsmHints.closing = adapterHints.closing;

        fsm.set_scheduling_hints(fsmHints);

        if (adapterHints.closing || (adapterHints.streaming_high_load && batch.empty())) {
            if (state_)
                state_->stats.repairBusyTicks++;
            continue;
        }

        // Process batch if we have documents
        if (!batch.empty()) {
            if (state_)
                state_->stats.repairIdleTicks++;

            try {
                auto content = services_ ? services_->getContentStore() : nullptr;
                auto meta_repo = services_ ? services_->getMetadataRepo() : nullptr;
                auto embed = services_ ? services_->getEmbeddingGenerator() : nullptr;

                if (content && meta_repo && embed) {
                    // Start repair FSM
                    if (fsm.start()) {
                        // Scan phase
                        fsm.on_scan_done();

                        // Detect phase - check which documents need embeddings
                        std::vector<std::string> missing;
                        if (!batch.empty()) {
                            // Check specific documents from the queue
                            auto vectorDb = services_ ? services_->getVectorDatabase() : nullptr;
                            if (vectorDb) {
                                for (const auto& hash : batch) {
                                    if (!vectorDb->hasEmbedding(hash)) {
                                        missing.push_back(hash);
                                    }
                                }
                            }
                        }

                        fsm.on_detect_done();

                        if (!missing.empty()) {
                            // Classify phase
                            fsm.on_classify_done();

                            // Isolate phase
                            fsm.on_isolate_done();

                            // Fix phase - generate embeddings
                            if (state_)
                                state_->stats.repairBatchesAttempted++;

                            repair::EmbeddingRepairConfig rcfg;
                            rcfg.batchSize = cfg_.maxBatch;
                            rcfg.skipExisting = true;
                            rcfg.dataPath = cfg_.dataDir;

                            std::unique_ptr<yams::Result<repair::EmbeddingRepairStats>> statsPtr;

                            // Prefer daemon RPC to ensure consistency with add-time embedding pipeline
                            bool fixSuccess = false;
                            bool executed = try_acquire_token();
                            if (executed) {
                                try {
                                    yams::daemon::ClientConfig ccfg;
                                    ccfg.dataDir = services_->getConfig().dataDir;
                                    ccfg.socketPath = services_->getConfig().socketPath;
                                    ccfg.singleUseConnections = true;
                                    ccfg.requestTimeout = std::chrono::seconds(120);
                                    yams::daemon::DaemonClient client(ccfg);
                                    yams::daemon::EmbedDocumentsRequest ed;
                                    ed.modelName = ""; // let provider decide
                                    ed.documentHashes = missing;
                                    ed.batchSize = static_cast<uint32_t>(rcfg.batchSize);
                                    ed.skipExisting = rcfg.skipExisting;
                                    {
                                        auto er = co_await client.streamingEmbedDocuments(ed);
                                        if (er) {
                                            repair::EmbeddingRepairStats statsLocal{};
                                            statsLocal.documentsProcessed = er.value().requested;
                                            statsLocal.embeddingsGenerated = er.value().embedded;
                                            statsLocal.embeddingsSkipped = er.value().skipped;
                                            statsLocal.failedOperations = er.value().failed;
                                            statsPtr = std::make_unique<yams::Result<repair::EmbeddingRepairStats>>(statsLocal);
                                            fixSuccess = true;
                                        } else {
                                            spdlog::debug(
                                                "RepairCoordinator: daemon EmbedDocuments failed: {} — falling back",
                                                er.error().message);
                                        }
                                    }
                                } catch (const std::exception& ex) {
                                    spdlog::debug("RepairCoordinator: daemon embed exception: {} — falling back", ex.what());
                                }
                                // Fallback to direct repair utility
                                auto r = repair::repairMissingEmbeddings(content, meta_repo, embed, rcfg,
                                                                         missing, nullptr,
                                                                         services_->getContentExtractors());
                                statsPtr = std::make_unique<yams::Result<repair::EmbeddingRepairStats>>(std::move(r));
                                fixSuccess = statsPtr && *statsPtr;
                                release_token();
                            }

                            if (!executed) {
                                fsm.on_fix_done(false);
                                continue;
                            }

                            // for daemon path statsPtr is set above, fixSuccess already true
                            fsm.on_fix_done(fixSuccess);

                            if (fixSuccess) {
                                // Verify phase
                                fsm.on_verify_done(true);

                                // Reindex phase (FTS5): opportunistically rebuild text index for affected docs
                                try {
                                    auto store = services_ ? services_->getContentStore() : nullptr;
                                    auto meta = services_ ? services_->getMetadataRepo() : nullptr;
                                    const auto& extractors = services_ ? services_->getContentExtractors() : std::vector<std::shared_ptr<yams::extraction::IContentExtractor>>{};
                                    if (store && meta) {
                                        size_t fts_ok = 0, fts_fail = 0;
                                        for (const auto& h : missing) {
                                            // Lookup document metadata
                                            auto docRes = meta->getDocumentByHash(h);
                                            if (!docRes || !docRes.value().has_value()) {
                                                ++fts_fail;
                                                continue;
                                            }
                                            const auto& d = docRes.value().value();
                                            // Extract text using the same utility path as ingestion
                                            std::string ext = d.fileExtension;
                                            if (!ext.empty() && ext[0] == '.') ext.erase(0, 1);
                                            auto extractedOpt = yams::extraction::util::extractDocumentText(
                                                store, h, d.mimeType, ext, extractors);
                                            if (!extractedOpt || extractedOpt->empty()) {
                                                ++fts_fail;
                                                continue;
                                            }
                                            // Upsert into FTS5 and fuzzy index; ignore failures (best-effort)
                                            auto ir = meta->indexDocumentContent(d.id, d.fileName, *extractedOpt, d.mimeType);
                                            if (ir) {
                                                (void)meta->updateFuzzyIndex(d.id);
                                                ++fts_ok;
                                            } else {
                                                ++fts_fail;
                                            }
                                        }
                                        spdlog::info("RepairCoordinator: FTS5 reindex complete for batch (ok={}, fail={})",
                                                     fts_ok, fts_fail);
                                    }
                                } catch (const std::exception& e) {
                                    spdlog::debug("RepairCoordinator: FTS5 reindex exception: {}", e.what());
                                }

                                fsm.on_reindex_done(true);

                                if (state_) {
                                    state_->stats.repairEmbeddingsGenerated +=
                                        static_cast<uint64_t>(
                                            statsPtr->value().embeddingsGenerated);
                                    state_->stats.repairEmbeddingsSkipped +=
                                        static_cast<uint64_t>(statsPtr->value().embeddingsSkipped);
                                    state_->stats.repairFailedOperations +=
                                        static_cast<uint64_t>(statsPtr->value().failedOperations);
                                    // Update coarse progress for initial backlog
                                    auto inc = static_cast<std::uint64_t>(
                                        statsPtr->value().embeddingsGenerated +
                                        statsPtr->value().embeddingsSkipped);
                                    if (inc > 0) {
                                        processed_.fetch_add(inc, std::memory_order_relaxed);
                                        update_progress_pct();
                                    }
                                }

                                spdlog::info("RepairCoordinator: repaired batch (generated={}, "
                                             "skipped={}, failed={})",
                                             statsPtr->value().embeddingsGenerated,
                                             statsPtr->value().embeddingsSkipped,
                                             statsPtr->value().failedOperations);
                            } else {
                                if (statsPtr) {
                                    spdlog::debug("RepairCoordinator: repair failed: {}",
                                                  statsPtr->error().message);
                                } else {
                                    spdlog::debug("RepairCoordinator: repair skipped/no result");
                                }
                            }
                        }
static bool vector_maintenance_lock_exists(ServiceManager* services) {
    try {
        if (!services) return false;
        auto dir = services->getResolvedDataDir();
        std::error_code ec;
        return std::filesystem::exists(dir / "vector_db_maintenance.lock", ec);
    } catch (...) { return false; }
}
                    }
                }
            } catch (const std::exception& e) {
                spdlog::debug("RepairCoordinator exception: {}", e.what());
            }
        }
    }

    spdlog::info("RepairCoordinator stopped");
    co_return;
}
#endif

// token helpers implemented inline in header

void RepairCoordinator::update_progress_pct() {
    if (!state_)
        return;
    auto tot = totalBacklog_.load(std::memory_order_relaxed);
    if (tot == 0)
        return;
    auto done = processed_.load(std::memory_order_relaxed);
    int pct = static_cast<int>(std::min<std::uint64_t>(100, (done * 100) / tot));
    state_->readiness.vectorIndexProgress.store(pct, std::memory_order_relaxed);
}

} // namespace yams::daemon
