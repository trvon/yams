#include <yams/daemon/components/TuningManager.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/PoolManager.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/resource/OnnxConcurrencyRegistry.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningConfig.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/ipc/fsm_metrics_registry.h>
#include <yams/daemon/ipc/mux_metrics_registry.h>
#include <yams/profiling.h>

namespace yams::daemon {

TuningManager::TuningManager(ServiceManager* sm, StateComponent* state,
                             WorkCoordinator* coordinator)
    : sm_(sm), state_(state), coordinator_(coordinator), strand_(coordinator->getExecutor()) {}

TuningManager::~TuningManager() {
    stop();
}

void TuningManager::start() {
    if (running_.exchange(true))
        return;
    tuningFuture_ = boost::asio::co_spawn(strand_, tuningLoop(), boost::asio::use_future);
}

void TuningManager::stop() {
    if (!running_.exchange(false))
        return;

    try {
        if (tuningFuture_.valid()) {
            tuningFuture_.wait();
            tuningFuture_.get();
        }
    } catch (const std::exception& e) {
        spdlog::debug("TuningManager loop stop wait error: {}", e.what());
    } catch (...) {
    }
}

boost::asio::awaitable<void> TuningManager::tuningLoop() {
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
    spdlog::debug("TuningManager loop started");

    while (running_.load()) {
        YAMS_ZONE_SCOPED_N("TuningManager::loop");
        try {
            tick_once();
        } catch (const std::exception& e) {
            spdlog::debug("TuningManager tick error: {}", e.what());
        } catch (...) {
        }

        // Cadence derived from TuneAdvisor status tick
        auto ms = TuneAdvisor::statusTickMs();
        timer.expires_after(std::chrono::milliseconds(ms));
        co_await timer.async_wait(boost::asio::use_awaitable);
    }

    spdlog::debug("TuningManager loop exiting");
}

void TuningManager::tick_once() {
    YAMS_ZONE_SCOPED_N("TuningManager::tick_once");
    if (!sm_ || !state_)
        return;

    // Don't perform tuning until services are at least partially ready,
    // to avoid acting on default PoolManager configs.
    if (!state_->readiness.metadataRepoReady.load()) {
        return;
    }

    // =========================================================================
    // Resource Governor: collect metrics and respond to memory pressure
    // =========================================================================
    auto& governor = ResourceGovernor::instance();
    ResourceSnapshot govSnap = governor.tick(sm_);

#if defined(TRACY_ENABLE)
    TracyPlot("governor.rss_mb", static_cast<double>(govSnap.rssBytes) / (1024.0 * 1024.0));
    TracyPlot("governor.budget_mb",
              static_cast<double>(govSnap.memoryBudgetBytes) / (1024.0 * 1024.0));
    TracyPlot("governor.pressure_pct", govSnap.memoryPressure * 100.0);
    TracyPlot("governor.level", static_cast<double>(govSnap.level));
    TracyPlot("governor.headroom_pct", govSnap.scalingHeadroom * 100.0);
#endif

    // Gather minimal metrics
    const std::uint64_t activeConns = state_->stats.activeConnections.load();
    const std::uint64_t workerThreads = sm_->getWorkerThreads();
    const std::uint64_t workerQueued = sm_->getWorkerQueueDepth();
    const auto msnap = MuxMetricsRegistry::instance().snapshot();
    const std::uint64_t muxQueuedBytes =
        (msnap.queuedBytes > 0) ? static_cast<std::uint64_t>(msnap.queuedBytes) : 0ull;

    // Idle shrink and pressure grow (centralized mirror of ResourceTuner, using TuneAdvisor)
    auto& pm = PoolManager::instance();

    if (sm_) {
        auto ingestMetrics = sm_->getIngestMetricsSnapshot();
        if (TuneAdvisor::enableParallelIngest()) {
            std::size_t backlog = ingestMetrics.queued;
            std::size_t desired = backlog > 0 ? backlog : 1;
            const auto activeWorkers = ingestMetrics.active;
            if (backlog > 0) {
                const std::size_t perWorker = std::max<std::size_t>(
                    1, static_cast<std::size_t>(TuneAdvisor::ingestBacklogPerWorker()));
                desired = (backlog + perWorker - 1) / perWorker;
            } else if (activeWorkers > 0) {
                desired = activeWorkers;
            }
            std::size_t cap = TuneAdvisor::maxIngestWorkers();
            if (cap == 0)
                cap = std::max<std::size_t>(1, std::thread::hardware_concurrency());
            const std::size_t storageCap = TuneAdvisor::storagePoolSize();
            if (storageCap > 0)
                cap = std::min(cap, storageCap);
            // Gate through ResourceGovernor
            if (TuneAdvisor::enableResourceGovernor()) {
                cap = std::min(cap, static_cast<std::size_t>(governor.maxIngestWorkers()));
            }
            if (cap == 0)
                cap = 1;
            if (desired < 1)
                desired = 1;
            if (desired > cap)
                desired = cap;
            sm_->setIngestWorkerTarget(desired);
#if defined(TRACY_ENABLE)
            TracyPlot("ingest.target_workers", static_cast<double>(desired));
            TracyPlot("ingest.queued", static_cast<double>(backlog));
            TracyPlot("ingest.active", static_cast<double>(activeWorkers));
#endif
        } else {
            sm_->setIngestWorkerTarget(1);
        }
    }

    std::uint64_t maxWorkerQ = 0;
    try {
        maxWorkerQ = TuneAdvisor::maxWorkerQueue(static_cast<size_t>(workerThreads));
    } catch (...) {
    }

    (void)activeConns;
    (void)workerQueued;
    (void)muxQueuedBytes;

    // Search concurrency governance (no dedicated pool)
    try {
        auto searchMetrics = sm_->getSearchLoadMetrics();
        std::uint32_t base = std::max<std::uint32_t>(1, TuneAdvisor::recommendedThreads(0.25));
        std::uint32_t loadHint = searchMetrics.active + searchMetrics.queued;
        std::uint32_t target = std::max(base, loadHint);
        auto advisorLimit = TuneAdvisor::searchConcurrencyLimit();
        if (advisorLimit > 0)
            target = std::min(target, advisorLimit);
        // Gate through ResourceGovernor
        if (TuneAdvisor::enableResourceGovernor()) {
            target = std::min(target, governor.maxSearchConcurrency());
        }
        (void)sm_->applySearchConcurrencyTarget(target);
    } catch (...) {
    }

    // PBI-05a: PostIngestQueue dynamic concurrency scaling
    // Scale up when there's a large backlog, scale down when idle
    try {
        auto* pq = sm_->getPostIngestQueue();
        if (pq) {
            const std::size_t queuedItems = pq->size();
            const std::size_t currentInFlight = pq->totalInFlight();
            const uint32_t hwThreads = TuneAdvisor::hardwareConcurrency();

            // Scale extraction concurrency based on queue depth
            // When queue is large, scale up to process faster
            // Max is capped at 50% of hardware threads or 64, whichever is smaller
            uint32_t extractionTarget = 4; // default
            if (queuedItems > 1000) {
                extractionTarget = std::min<uint32_t>(hwThreads / 2, 32);
            } else if (queuedItems > 500) {
                extractionTarget = std::min<uint32_t>(hwThreads / 4, 16);
            } else if (queuedItems > 100) {
                extractionTarget = std::min<uint32_t>(hwThreads / 8 + 4, 12);
            } else if (queuedItems > 10) {
                extractionTarget = 8;
            } else if (queuedItems == 0 && currentInFlight == 0) {
                extractionTarget = 4; // idle - use minimum
            }
            extractionTarget = std::max<uint32_t>(4, extractionTarget); // floor at default
            // Gate through ResourceGovernor
            if (TuneAdvisor::enableResourceGovernor()) {
                extractionTarget = std::min(extractionTarget, governor.maxExtractionConcurrency());
            }
            TuneAdvisor::setPostExtractionConcurrent(extractionTarget);

            // KG concurrency: scale based on queue depth BUT scale DOWN on DB contention
            // This prevents "database is locked" errors from overwhelming the system
            uint32_t kgTarget = 8;
            const uint64_t dbLockErrors = TuneAdvisor::getAndResetDbLockErrors();
            const uint32_t lockThreshold = TuneAdvisor::dbLockErrorThreshold();

            if (dbLockErrors > lockThreshold * 2) {
                // Severe contention: drop to minimum
                kgTarget = 2;
                spdlog::debug("TuningManager: DB lock errors ({}) severe, KG concurrency -> 2",
                              dbLockErrors);
            } else if (dbLockErrors > lockThreshold) {
                // Moderate contention: reduce significantly
                kgTarget = 4;
                spdlog::debug("TuningManager: DB lock errors ({}) > threshold, KG concurrency -> 4",
                              dbLockErrors);
            } else if (queuedItems > 500) {
                kgTarget = std::min<uint32_t>(hwThreads / 2, 32);
            } else if (queuedItems > 100) {
                kgTarget = 16;
            }
            // Gate through ResourceGovernor
            if (TuneAdvisor::enableResourceGovernor()) {
                kgTarget = std::min(kgTarget, governor.maxKgConcurrency());
            }
            TuneAdvisor::setPostKgConcurrent(kgTarget);

            // Symbol extraction - scale modestly (CPU-bound)
            uint32_t symbolTarget = queuedItems > 100 ? std::min<uint32_t>(hwThreads / 4, 8) : 4;
            TuneAdvisor::setPostSymbolConcurrent(symbolTarget);

            // Entity extraction stays low (heavy binary analysis)
            uint32_t entityTarget = queuedItems > 50 ? 4 : 2;
            TuneAdvisor::setPostEntityConcurrent(entityTarget);

            // PBI-05b: Embedding concurrency scaling
            // Embeddings are compute-heavy (ONNX model inference) and often bottleneck bulk ingest.
            // Scale aggressively when there's backlog to prevent queue overflow and job dropping.
            // Check embed queue depth from InternalEventBus
            auto& bus = InternalEventBus::instance();
            const std::size_t embedQueued = bus.embedQueued();
            const std::size_t embedDropped = bus.embedDropped();

            // Embed concurrency: also scale DOWN on DB contention
            // Embeddings write to vector_db which shares the SQLite connection pool
            uint32_t embedTarget = 4; // conservative baseline (was 2, caused stalls)
            if (dbLockErrors > lockThreshold * 2) {
                // Severe DB contention: drop to minimum
                embedTarget = 1;
                spdlog::debug("TuningManager: DB lock errors ({}) severe, embed concurrency -> 1",
                              dbLockErrors);
            } else if (dbLockErrors > lockThreshold) {
                // Moderate contention: reduce to 2
                embedTarget = 2;
                spdlog::debug(
                    "TuningManager: DB lock errors ({}) > threshold, embed concurrency -> 2",
                    dbLockErrors);
            } else if (embedQueued > 1000 || embedDropped > 100) {
                // Heavy backlog or significant drops - scale to max
                embedTarget = std::min<uint32_t>(hwThreads / 2, 16);
            } else if (embedQueued > 500 || embedDropped > 10) {
                embedTarget = std::min<uint32_t>(hwThreads / 3, 12);
            } else if (embedQueued > 100) {
                embedTarget = std::min<uint32_t>(hwThreads / 4, 8);
            } else if (embedQueued > 10) {
                embedTarget = 4;
            } else if (embedQueued == 0 && queuedItems == 0) {
                embedTarget = 2; // idle - minimum
            }
            embedTarget = std::max<uint32_t>(2, embedTarget); // floor
            // Gate through ResourceGovernor
            if (TuneAdvisor::enableResourceGovernor()) {
                embedTarget = std::min(embedTarget, governor.maxEmbedConcurrency());
            }
            TuneAdvisor::setPostEmbedConcurrent(embedTarget);

#if defined(TRACY_ENABLE)
            TracyPlot("post.queued", static_cast<double>(queuedItems));
            TracyPlot("post.inflight", static_cast<double>(currentInFlight));
            TracyPlot("post.extraction.limit", static_cast<double>(extractionTarget));
            TracyPlot("post.kg.limit", static_cast<double>(kgTarget));
            TracyPlot("post.embed.limit", static_cast<double>(embedTarget));
            TracyPlot("post.embed.queued", static_cast<double>(embedQueued));
            TracyPlot("post.embed.dropped", static_cast<double>(embedDropped));
            TracyPlot("db.lock_errors_window", static_cast<double>(dbLockErrors));
#endif
        }
    } catch (...) {
    }

    // Writer budget observability: ensure a non-zero budget is published
    std::size_t writerBudget = TuneAdvisor::serverWriterBudgetBytesPerTurn();
    if (writerBudget == 0)
        writerBudget = TuneAdvisor::writerBudgetBytesPerTurn();
    if (writerBudget > 0) {
        if (setWriterBudget_) {
            try {
                setWriterBudget_(writerBudget);
            } catch (...) {
            }
        } else {
            MuxMetricsRegistry::instance().setWriterBudget(writerBudget);
        }
    }

    // Expose pool sizes and writer budget to FSM metrics for downstream visibility
    try {
        auto ipcStats = pm.stats("ipc");
        auto ioStats = pm.stats("ipc_io");
        FsmMetricsRegistry::instance().setIpcPoolSize(ipcStats.current_size);
        FsmMetricsRegistry::instance().setIoPoolSize(ioStats.current_size);
        FsmMetricsRegistry::instance().setWriterBudgetBytes(writerBudget);
        // ResourceGovernor metrics
        FsmMetricsRegistry::instance().setGovernorRssBytes(govSnap.rssBytes);
        FsmMetricsRegistry::instance().setGovernorBudgetBytes(govSnap.memoryBudgetBytes);
        FsmMetricsRegistry::instance().setGovernorPressureLevel(
            static_cast<uint8_t>(govSnap.level));
        FsmMetricsRegistry::instance().setGovernorHeadroomPct(
            static_cast<uint8_t>(govSnap.scalingHeadroom * 100.0));
        // ONNX concurrency metrics
        auto onnxSnap = OnnxConcurrencyRegistry::instance().snapshot();
        FsmMetricsRegistry::instance().setOnnxTotalSlots(onnxSnap.totalSlots);
        FsmMetricsRegistry::instance().setOnnxUsedSlots(onnxSnap.usedSlots);
        FsmMetricsRegistry::instance().setOnnxGlinerUsed(
            onnxSnap.lanes[static_cast<size_t>(OnnxLane::Gliner)].used);
        FsmMetricsRegistry::instance().setOnnxEmbedUsed(
            onnxSnap.lanes[static_cast<size_t>(OnnxLane::Embedding)].used);
        FsmMetricsRegistry::instance().setOnnxRerankerUsed(
            onnxSnap.lanes[static_cast<size_t>(OnnxLane::Reranker)].used);
#if defined(TRACY_ENABLE)
        TracyPlot("pool.ipc.size", static_cast<double>(ipcStats.current_size));
        TracyPlot("pool.io.size", static_cast<double>(ioStats.current_size));
        TracyPlot("writer.budget.bytes", static_cast<double>(writerBudget));
        TracyPlot("active.conns", static_cast<double>(activeConns));
        TracyPlot("mux.queued.bytes", static_cast<double>(muxQueuedBytes));
#endif
    } catch (...) {
    }

    // Publish a precomputed tuning snapshot for hot-path consumers
    try {
        auto s = std::make_shared<TuningSnapshot>();
        s->workerPollMs = TuneAdvisor::workerPollMs();
        s->backpressureReadPauseMs = TuneAdvisor::backpressureReadPauseMs();
        s->idleCpuPct = TuneAdvisor::idleCpuThresholdPercent();
        s->idleMuxLowBytes = TuneAdvisor::idleMuxLowBytes();
        s->idleShrinkHoldMs = TuneAdvisor::idleShrinkHoldMs();
        s->poolScaleStep = TuneAdvisor::poolScaleStep();
        s->poolCooldownMs = TuneAdvisor::poolCooldownMs();
        s->poolIpcMin = TuneAdvisor::poolMinSizeIpc();
        s->poolIpcMax = TuneAdvisor::poolMaxSizeIpc();
        s->poolIoMin = TuneAdvisor::poolMinSizeIpcIo();
        s->poolIoMax = TuneAdvisor::poolMaxSizeIpcIo();
        s->writerBudgetBytesPerTurn = writerBudget;
        TuningSnapshotRegistry::instance().set(std::move(s));
    } catch (...) {
    }

    // RepairCoordinator tuning (tokens/batch) centralized here
    try {
        if (setRepair_) {
            using clock = std::chrono::steady_clock;
            auto now = clock::now();
            const uint32_t busyThresh = TuneAdvisor::repairBusyConnThreshold();
            const bool isBusy = (activeConns >= busyThresh);
            if (isBusy) {
                if (repairBusySince_.time_since_epoch().count() == 0)
                    repairBusySince_ = now;
                repairReadySince_ = {};
            } else {
                if (repairReadySince_.time_since_epoch().count() == 0)
                    repairReadySince_ = now;
                repairBusySince_ = {};
            }
            const uint32_t degradeHold = TuneAdvisor::repairDegradeHoldMs();
            const uint32_t readyHold = TuneAdvisor::repairReadyHoldMs();
            const bool busyHeld =
                isBusy && repairBusySince_.time_since_epoch().count() != 0 &&
                std::chrono::duration_cast<std::chrono::milliseconds>(now - repairBusySince_)
                        .count() >= degradeHold;
            [[maybe_unused]] const bool idleHeld =
                !isBusy && repairReadySince_.time_since_epoch().count() != 0 &&
                std::chrono::duration_cast<std::chrono::milliseconds>(now - repairReadySince_)
                        .count() >= readyHold;

            uint32_t tokens =
                busyHeld ? TuneAdvisor::repairTokensBusy() : TuneAdvisor::repairTokensIdle();
            uint32_t batch = TuneAdvisor::repairMaxBatch();

            // Rate limiting: cap batches per second; if exceeded, force tokens=0 this window
            uint32_t maxPerSec = TuneAdvisor::repairMaxBatchesPerSec();
            if (maxPerSec > 0) {
                uint64_t curBatches = 0;
                try {
                    curBatches = state_->stats.repairBatchesAttempted.load();
                } catch (...) {
                }
                if (repairRateWindowStart_.time_since_epoch().count() == 0) {
                    repairRateWindowStart_ = now;
                    repairBatchesAtWindowStart_ = curBatches;
                } else {
                    auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                         now - repairRateWindowStart_)
                                         .count();
                    auto produced = (curBatches >= repairBatchesAtWindowStart_)
                                        ? (curBatches - repairBatchesAtWindowStart_)
                                        : 0;
                    if (elapsedMs < 1000) {
                        if (produced >= maxPerSec)
                            tokens = 0;
                    } else {
                        repairRateWindowStart_ = now;
                        repairBatchesAtWindowStart_ = curBatches;
                    }
                }
            }

            setRepair_(tokens, batch);
        }
    } catch (...) {
    }

    // Idle model maintenance: unload models that have been idle past their timeout
    // This runs via evict_under_pressure(0.0) which triggers performMaintenance()
    // Thresholds are profile-aware: Efficient runs more often, Aggressive only when truly idle
    try {
        if (sm_) {
            auto searchMetrics = sm_->getSearchLoadMetrics();
            std::size_t postIngestQueued = 0;
            std::size_t currentInFlight = 0;
            if (auto* pq = sm_->getPostIngestQueue()) {
                postIngestQueued = pq->size();
                currentInFlight = pq->totalInFlight();
            }

            // Profile-aware thresholds: Efficient=permissive, Balanced=moderate, Aggressive=strict
            const uint32_t connThresh = TuneAdvisor::modelMaintenanceConnThreshold();
            const uint32_t searchThresh = TuneAdvisor::modelMaintenanceSearchThreshold();
            const uint32_t queueThresh = TuneAdvisor::modelMaintenanceQueueThreshold();

            bool canDoMaintenance = (activeConns <= connThresh) &&
                                    (searchMetrics.active <= searchThresh) &&
                                    (postIngestQueued <= queueThresh) && (currentInFlight == 0);

            if (canDoMaintenance) {
                if (auto provider = sm_->getModelProvider()) {
                    provider->releaseUnusedResources();
                }
            }
        }
    } catch (...) {
    }
}

} // namespace yams::daemon
