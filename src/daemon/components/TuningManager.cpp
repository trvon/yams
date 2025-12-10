#include <yams/daemon/components/TuningManager.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <yams/daemon/components/PoolManager.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningConfig.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/ipc/fsm_metrics_registry.h>
#include <yams/daemon/ipc/mux_metrics_registry.h>
#if defined(TRACY_ENABLE)
#include <tracy/Tracy.hpp>
#endif

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
#if defined(TRACY_ENABLE)
        ZoneScopedN("TuningManager::loop");
#endif
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
#if defined(TRACY_ENABLE)
    ZoneScopedN("TuningManager::tick_once");
#endif
    if (!sm_ || !state_)
        return;

    // Don't perform tuning until services are at least partially ready,
    // to avoid acting on default PoolManager configs.
    if (!state_->readiness.metadataRepoReady.load()) {
        return;
    }

    // Gather minimal metrics
    const std::uint64_t activeConns = state_->stats.activeConnections.load();
    const std::uint64_t workerThreads = sm_->getWorkerThreads();
    const std::uint64_t workerQueued = sm_->getWorkerQueueDepth();
    const auto msnap = MuxMetricsRegistry::instance().snapshot();
    const std::uint64_t muxQueuedBytes =
        (msnap.queuedBytes > 0) ? static_cast<std::uint64_t>(msnap.queuedBytes) : 0ull;

    // Idle shrink and pressure grow (centralized mirror of ResourceTuner, using TuneAdvisor)
    auto& pm = PoolManager::instance();

    const std::uint64_t maxMux = TuneAdvisor::maxMuxBytes();
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
        (void)sm_->applySearchConcurrencyTarget(target);
    } catch (...) {
    }

    // Post-ingest tuning (gated on core service readiness)
    try {
        bool servicesReady = false;
        try {
            servicesReady = state_->readiness.databaseReady.load() &&
                            state_->readiness.metadataRepoReady.load() &&
                            state_->readiness.contentStoreReady.load();
        } catch (...) {
        }
        if (servicesReady) {
            std::size_t pqDepth = 0;
            std::size_t pqInflight = 0;
            std::size_t pqCap = 0;
            try {
                if (auto* pq = sm_->getPostIngestQueue()) {
                    pqDepth = pq->size();
                    pqInflight = 0; // Not tracked in strand-based implementation
                    pqCap = pq->capacity();
                }
            } catch (...) {
            }
            apply_post_ingest_control(pqDepth, pqInflight, pqCap, activeConns);
            auto piStats = pm.stats("post_ingest");
            if (piStats.current_size > 0) {
                // Clamp to configured bounds
                auto cfg = sm_->getTuningConfig();
                std::size_t desired = piStats.current_size;
                // Scale desired by profile (aggressive grows faster)
                desired = static_cast<std::size_t>(std::llround(
                    static_cast<double>(desired) * std::max(0.5, TuneAdvisor::profileScale())));
                if (cfg.postIngestThreadsMax > 0)
                    desired = std::min(desired, cfg.postIngestThreadsMax);
                if (cfg.postIngestThreadsMin > 0)
                    desired = std::max(desired, cfg.postIngestThreadsMin);
                (void)sm_->resizePostIngestThreads(desired);
            }
#if defined(TRACY_ENABLE)
            TracyPlot("post_ingest.queued", static_cast<double>(pqDepth));
            TracyPlot("post_ingest.threads", static_cast<double>(piStats.current_size));
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
}

void TuningManager::apply_post_ingest_control(std::size_t queued, std::size_t inflight,
                                              std::size_t capacity, std::uint64_t activeConns) {
#if defined(TRACY_ENABLE)
    ZoneScopedN("TuningManager::apply_post_ingest_control");
#endif
    auto cfg = sm_->getTuningConfig();
    // Choose a target queue around mid between warn and stop, clamped to capacity
    const std::size_t warn = cfg.admitWarnThreshold > 0 ? cfg.admitWarnThreshold : capacity / 2;
    const std::size_t stop =
        cfg.admitStopThreshold > 0 ? cfg.admitStopThreshold : capacity * 9 / 10;
    const std::size_t target = std::min<std::size_t>(capacity, (warn + stop) / 2);

    // PI controller on queue depth; proportional on error, integral to reduce steady-state error
    double err =
        static_cast<double>(static_cast<long long>(queued) - static_cast<long long>(target));
    // Anti-windup: clamp integral
    integratorQueueErr_ = std::clamp(integratorQueueErr_ + err * 0.01, -1000.0, 1000.0);

    // Convert control signal to discrete +/- N adjustments with hold time to avoid thrash
    auto now = std::chrono::steady_clock::now();
    // Make post-ingest control more responsive: tie hold to cooldown and reduce default hold.
    // Effective hold = max(poolCooldownMs, max(300ms, cfg.holdMs/3)).
    {
        uint32_t cfgHold = (cfg.holdMs > 0 ? cfg.holdMs : 3000);
        uint32_t reduced = std::max<uint32_t>(300, cfgHold / 3);
        uint32_t eff = std::max<uint32_t>(TuneAdvisor::poolCooldownMs(), reduced);
        // store into a local as milliseconds
        (void)eff;
    }
    auto hold = std::chrono::milliseconds(
        std::max<uint32_t>(TuneAdvisor::poolCooldownMs(),
                           std::max<uint32_t>(300, (cfg.holdMs > 0 ? cfg.holdMs : 3000) / 3)));
    bool canAdjust =
        (lastPiAdjust_.time_since_epoch().count() == 0) ||
        (std::chrono::duration_cast<std::chrono::milliseconds>(now - lastPiAdjust_) >= hold);

    auto& pm = PoolManager::instance();
    if (!canAdjust)
        return;

    const int step = std::max(1, TuneAdvisor::poolScaleStep());
    // If active connections are high, prefer to shrink post_ingest to keep IPC responsive
    if (activeConns > 0 && queued == 0) {
        pm.apply_delta({"post_ingest", -step, "busy_shrink", TuneAdvisor::poolCooldownMs()});
        lastPiAdjust_ = now;
        return;
    }

    if (queued >= stop) {
        pm.apply_delta({"post_ingest", +std::max(1, step * 2), "queue_at_stop",
                        TuneAdvisor::poolCooldownMs()});
        lastPiAdjust_ = now;
        return;
    }
    if (err > static_cast<double>(warn) * 0.1) {
        pm.apply_delta({"post_ingest", +step, "queue_above_target", TuneAdvisor::poolCooldownMs()});
        lastPiAdjust_ = now;
        return;
    }
    if (queued == 0 && inflight == 0) {
        pm.apply_delta({"post_ingest", -step, "queue_empty", TuneAdvisor::poolCooldownMs()});
        lastPiAdjust_ = now;
        return;
    }
}

} // namespace yams::daemon
