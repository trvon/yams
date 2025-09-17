#include <yams/daemon/components/TuningManager.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <yams/daemon/components/PoolManager.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/ipc/fsm_metrics_registry.h>
#include <yams/daemon/ipc/mux_metrics_registry.h>
#if defined(TRACY_ENABLE)
#include <tracy/Tracy.hpp>
#endif

namespace yams::daemon {

TuningManager::TuningManager(ServiceManager* sm, StateComponent* state) : sm_(sm), state_(state) {}

TuningManager::~TuningManager() {
    stop();
}

void TuningManager::start() {
    if (running_.exchange(true))
        return;
    thread_ = yams::compat::jthread([this](yams::compat::stop_token st) {
        spdlog::debug("TuningManager thread started");
        while (!st.stop_requested() && running_.load()) {
#if defined(TRACY_ENABLE)
            ZoneScopedN("TuningManager::loop");
#endif
            try {
                tick_once();
            } catch (const std::exception& e) {
                spdlog::debug("TuningManager tick error: {}", e.what());
            } catch (...) {
            }
            // Cadence derived from TuneAdvisor status tick for now
            auto ms = TuneAdvisor::statusTickMs();
            std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        }
        spdlog::debug("TuningManager thread exiting");
    });
}

void TuningManager::stop() {
    running_ = false;
    if (thread_.joinable())
        thread_.join();
}

void TuningManager::tick_once() {
#if defined(TRACY_ENABLE)
    ZoneScopedN("TuningManager::tick_once");
#endif
    if (!sm_ || !state_)
        return;

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
    std::uint64_t maxWorkerQ = 0;
    try {
        maxWorkerQ = TuneAdvisor::maxWorkerQueue(static_cast<size_t>(workerThreads));
    } catch (...) {
    }

    const bool noConns = (activeConns == 0);
    const bool noWorkerQ = (workerQueued == 0);
    const bool muxLow =
        muxQueuedBytes < std::max<std::uint64_t>(TuneAdvisor::maxMuxBytes() / 64ull,
                                                 1ull * 1024ull * 1024ull); // ~1/64 of cap or 1MiB
    if (noConns && noWorkerQ && muxLow) {
        pm.apply_delta({"ipc", -1, "central_idle", TuneAdvisor::statusTickMs()});
        pm.apply_delta({"ipc_io", -1, "central_idle", TuneAdvisor::statusTickMs()});
    } else {
        // Pressure grow
        const bool workerHigh =
            (maxWorkerQ > 0) ? (workerQueued > maxWorkerQ) : (workerQueued > (workerThreads * 2));
        const bool muxHigh = (maxMux > 0)
                                 ? (muxQueuedBytes > maxMux)
                                 : (muxQueuedBytes > TuneAdvisor::muxBacklogHighFallbackBytes());
        if (workerHigh) {
            pm.apply_delta({"ipc", +1, "central_worker_queue_high", TuneAdvisor::statusTickMs()});
        }
        // IO pool growth heuristic: only grow when connections per IO thread exceed a threshold
        // to avoid unbounded scaling on any activity.
        try {
            auto ioStats = pm.stats("ipc_io");
            std::uint32_t ioThreads = std::max<std::uint32_t>(ioStats.current_size, 1);
            const std::uint32_t connPerThread = TuneAdvisor::ioConnPerThread();
            bool connHigh = (activeConns > static_cast<std::uint64_t>(ioThreads) * connPerThread);
            if (muxHigh || connHigh) {
                pm.apply_delta({"ipc_io", +1,
                                muxHigh ? "central_mux_backlog" : "central_conns_per_thread_high",
                                TuneAdvisor::statusTickMs()});
            }
        } catch (...) {
            if (muxHigh) {
                pm.apply_delta({"ipc_io", +1, "central_mux_backlog", TuneAdvisor::statusTickMs()});
            }
        }
    }

    // Apply pool target to worker pool when changed
    try {
        auto ipcStats = pm.stats("ipc");
        if (ipcStats.current_size > 0) {
            // Respect hardware capacity by capping to recommendedThreads
            std::size_t cap = TuneAdvisor::recommendedThreads();
            std::size_t desired = std::min<std::size_t>(ipcStats.current_size, cap);
            if (desired == 0)
                desired = 1;
            (void)sm_->resizeWorkerPool(desired);
        }
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
            try {
                if (auto* pq = sm_->getPostIngestQueue())
                    pqDepth = pq->size();
            } catch (...) {
            }
            if (activeConns > 0) {
                pm.apply_delta({"post_ingest", -1, "busy", TuneAdvisor::poolCooldownMs()});
            } else if (pqDepth > 0) {
                pm.apply_delta({"post_ingest", +1, "backlog", TuneAdvisor::poolCooldownMs()});
            } else {
                pm.apply_delta({"post_ingest", -1, "idle", TuneAdvisor::poolCooldownMs()});
            }
            auto piStats = pm.stats("post_ingest");
            if (piStats.current_size > 0)
                (void)sm_->resizePostIngestThreads(piStats.current_size);
        }
    } catch (...) {
    }

    // Writer budget observability: ensure a non-zero budget is published
    std::size_t writerBudget = TuneAdvisor::serverWriterBudgetBytesPerTurn();
    if (writerBudget == 0)
        writerBudget = TuneAdvisor::writerBudgetBytesPerTurn();
    if (writerBudget > 0)
        MuxMetricsRegistry::instance().setWriterBudget(writerBudget);

    // Expose pool sizes and writer budget to FSM metrics for downstream visibility
    try {
        auto ipcStats = pm.stats("ipc");
        auto ioStats = pm.stats("ipc_io");
        FsmMetricsRegistry::instance().setIpcPoolSize(ipcStats.current_size);
        FsmMetricsRegistry::instance().setIoPoolSize(ioStats.current_size);
        FsmMetricsRegistry::instance().setWriterBudgetBytes(writerBudget);
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
            const bool idleHeld =
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

} // namespace yams::daemon
