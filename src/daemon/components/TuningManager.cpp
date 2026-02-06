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
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningConfig.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/ipc/fsm_metrics_registry.h>
#include <yams/daemon/ipc/mux_metrics_registry.h>
#include <yams/daemon/resource/OnnxConcurrencyRegistry.h>
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

    // Seed FsmMetricsRegistry with initial ResourceGovernor data immediately
    // so status requests have valid data before the first async tick completes.
    // This prevents CLI tools from seeing 0 values for governorBudgetBytes.
    try {
        tick_once();
    } catch (const std::exception& e) {
        spdlog::debug("TuningManager initial tick error: {}", e.what());
    } catch (...) {
    }

    // If tick_once early-returned (metadata not ready), seed safe minimums
    // so PostIngestQueue pollers don't start with zero concurrency.
    if (TuneAdvisor::postExtractionConcurrent() == 0) {
        TuneAdvisor::setPostExtractionConcurrentDynamicCap(2);
    }
    if (TuneAdvisor::postEmbedConcurrent() == 0) {
        TuneAdvisor::setPostEmbedConcurrentDynamicCap(1);
    }

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

void TuningManager::configureOnnxConcurrencyRegistry() {
    // Configure OnnxConcurrencyRegistry based on TuneAdvisor settings
    // This should only be called once during initialization
    auto& registry = OnnxConcurrencyRegistry::instance();

    // Get profile-aware settings from TuneAdvisor (already scaled for profile)
    const uint32_t maxConcurrent = TuneAdvisor::onnxMaxConcurrent();
    const uint32_t glinerReserved = TuneAdvisor::onnxGlinerReserved();
    const uint32_t embedReserved = TuneAdvisor::onnxEmbedReserved();
    const uint32_t rerankerReserved = TuneAdvisor::onnxRerankerReserved();

    // Ensure at least 1 shared slot beyond reserved and a hard minimum of 2.
    const uint32_t totalReserved = glinerReserved + embedReserved + rerankerReserved;
    const uint32_t maxSlots = std::max<uint32_t>(std::max<uint32_t>(maxConcurrent, 2u),
                                                 totalReserved + 1);

    // Configure the registry
    registry.setMaxSlots(maxSlots);
    registry.setReservedSlots(OnnxLane::Gliner, glinerReserved);
    registry.setReservedSlots(OnnxLane::Embedding, embedReserved);
    registry.setReservedSlots(OnnxLane::Reranker, rerankerReserved);
    registry.setReservedSlots(OnnxLane::Other, 0);

    spdlog::info(
        "[TuningManager] Configured OnnxConcurrencyRegistry: maxSlots={}, reserved=[gliner={}, "
        "embed={}, reranker={}]",
        maxSlots, glinerReserved, embedReserved, rerankerReserved);
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

    // Configure ONNX concurrency registry once on first tick
    if (!onnxRegistryConfigured_.exchange(true)) {
        configureOnnxConcurrencyRegistry();
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
                cap = static_cast<std::size_t>(TuneAdvisor::recommendedThreads());
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

    // Adaptive worker poll cadence: ease off when idle, tighten when busy.
    try {
        if (!TuneAdvisor::workerPollMsPinned()) {
            const uint32_t basePollMs = TuneAdvisor::workerPollMs();
            const uint32_t minPollMs = 50;
            const uint32_t maxPollMs = 1000;
            auto clampMs = [&](uint32_t v) { return std::max(minPollMs, std::min(maxPollMs, v)); };
            const uint32_t currentMs = clampMs(basePollMs);

            bool workerQHigh = (maxWorkerQ > 0) && (workerQueued >= maxWorkerQ);
            bool muxHigh = false;
            try {
                std::uint64_t muxCap = TuneAdvisor::maxMuxBytes();
                muxHigh = (muxCap > 0 && muxQueuedBytes >= muxCap);
            } catch (...) {
            }
            bool busy = (activeConns > 0) || workerQHigh || muxHigh;
            bool idle = (activeConns == 0) && (workerQueued == 0) && (muxQueuedBytes == 0);
            if (busy) {
                uint32_t next = clampMs(static_cast<uint32_t>(currentMs * 0.5));
                if (next < currentMs) {
                    TuneAdvisor::setWorkerPollMsDynamic(next);
                }
            } else if (idle) {
                uint32_t next = clampMs(static_cast<uint32_t>(currentMs * 1.25));
                if (next > currentMs) {
                    TuneAdvisor::setWorkerPollMsDynamic(next);
                }
            }
        }
    } catch (...) {
    }

    // PBI-05a: PostIngestQueue dynamic concurrency scaling
    // Scale up when there's a large backlog, scale down when idle
    try {
        auto* pq = sm_->getPostIngestQueue();
        if (pq) {
            const std::size_t queuedItems = pq->size();
            [[maybe_unused]] const std::size_t currentInFlight = pq->totalInFlight();

            auto& bus = InternalEventBus::instance();
            const std::size_t embedQueued = bus.embedQueued();
            const std::size_t embedDropped = bus.embedDropped();

            // Derive total post-ingest budget and weighted targets.
            // Note: We compute a temporary scaled budget but do NOT persist it back
            // to TuneAdvisor to avoid runaway scaling. The base budget from
            // recommendedThreads() should remain stable.
            uint32_t baseBudget = TuneAdvisor::postIngestTotalConcurrent();
            uint32_t scaleBias = 0;
            if (embedQueued > 1000 || embedDropped > 100) {
                scaleBias = 2;
            } else if (embedQueued > 250 || embedDropped > 20) {
                scaleBias = 1;
            } else if (queuedItems > 500) {
                scaleBias = 1;
            }
            // Cap scaled budget to hardware concurrency to avoid oversubscription
            uint32_t hwCap = TuneAdvisor::hardwareConcurrency();
            uint32_t totalBudget = std::min<uint32_t>(hwCap, baseBudget + scaleBias);

            const uint64_t dbLockErrors = TuneAdvisor::getAndResetDbLockErrors();
            const uint32_t lockThreshold = TuneAdvisor::dbLockErrorThreshold();

            uint32_t extractionTarget = TuneAdvisor::postExtractionConcurrent();
            uint32_t kgTarget = TuneAdvisor::postKgConcurrent();
            uint32_t symbolTarget = TuneAdvisor::postSymbolConcurrent();
            uint32_t entityTarget = TuneAdvisor::postEntityConcurrent();
            uint32_t titleTarget = TuneAdvisor::postTitleConcurrent();
            uint32_t embedTarget = TuneAdvisor::postEmbedConcurrent();

            // Override with gradient limiter values if enabled
            if (TuneAdvisor::enableGradientLimiters()) {
                if (auto* extractionLimiter = pq->extractionLimiter()) {
                    extractionTarget = extractionLimiter->effectiveLimit();
                }
                if (auto* kgLimiter = pq->kgLimiter()) {
                    kgTarget = kgLimiter->effectiveLimit();
                }
                if (auto* symbolLimiter = pq->symbolLimiter()) {
                    symbolTarget = symbolLimiter->effectiveLimit();
                }
                if (auto* entityLimiter = pq->entityLimiter()) {
                    entityTarget = entityLimiter->effectiveLimit();
                }
                if (auto* titleLimiter = pq->titleLimiter()) {
                    titleTarget = titleLimiter->effectiveLimit();
                }
                if (auto* embedLimiter = pq->embedLimiter()) {
                    embedTarget = embedLimiter->effectiveLimit();
                }
            }

            if (dbLockErrors > lockThreshold * 2) {
                kgTarget = std::min<uint32_t>(kgTarget, 2);
                embedTarget = std::min<uint32_t>(embedTarget, 1);
                spdlog::debug("TuningManager: DB lock errors ({}) severe; KG/embed reduced",
                              dbLockErrors);
            } else if (dbLockErrors > lockThreshold) {
                kgTarget = std::min<uint32_t>(kgTarget, 4);
                embedTarget = std::min<uint32_t>(embedTarget, 2);
                spdlog::debug("TuningManager: DB lock errors ({}) > threshold; KG/embed reduced",
                              dbLockErrors);
            }

            if (TuneAdvisor::enableResourceGovernor()) {
                extractionTarget = std::min(extractionTarget, governor.maxExtractionConcurrency());
                kgTarget = std::min(kgTarget, governor.maxKgConcurrency());
                symbolTarget = std::min(symbolTarget, governor.maxSymbolConcurrency());
                entityTarget = std::min(entityTarget, governor.maxEntityConcurrency());
                titleTarget = std::min(titleTarget, governor.maxTitleConcurrency());
                embedTarget = std::min(embedTarget, governor.maxEmbedConcurrency());
            }

            // Reconcile per-stage targets to the total budget to avoid oversubscription.
            {
                const uint32_t activeMask = TuneAdvisor::postIngestStageActiveMask();
                auto applyActiveMask = [&](uint32_t bit, uint32_t& target) {
                    if ((activeMask & bit) == 0u) {
                        target = 0;
                    }
                };
                applyActiveMask(1u << 0u, extractionTarget);
                applyActiveMask(1u << 1u, kgTarget);
                applyActiveMask(1u << 2u, symbolTarget);
                applyActiveMask(1u << 3u, entityTarget);
                applyActiveMask(1u << 4u, titleTarget);
                applyActiveMask(1u << 5u, embedTarget);

                const bool extractionActive = extractionTarget > 0;
                const bool embedActive = embedTarget > 0;
                const uint32_t minExtraction = (totalBudget >= 1 && extractionActive) ? 1u : 0u;
                uint32_t minEmbed = (totalBudget >= 2 && embedActive) ? 1u : 0u;
                if (totalBudget == 1 && !extractionActive && embedActive) {
                    minEmbed = 1u;
                }

                auto sumTargets = [&]() {
                    return extractionTarget + kgTarget + symbolTarget + entityTarget + titleTarget +
                           embedTarget;
                };

                uint32_t totalTarget = sumTargets();
                if (totalTarget > totalBudget) {
                    struct StageRef {
                        uint32_t* value;
                        uint32_t minValue;
                    };
                    std::array<StageRef, 6> reduceOrder = {
                        StageRef{&entityTarget, 0u},
                        StageRef{&titleTarget, 0u},
                        StageRef{&symbolTarget, 0u},
                        StageRef{&kgTarget, 0u},
                        StageRef{&extractionTarget, minExtraction},
                        StageRef{&embedTarget, minEmbed}};

                    while (totalTarget > totalBudget) {
                        bool progressed = false;
                        for (auto& stage : reduceOrder) {
                            if (*stage.value == 0 || *stage.value <= stage.minValue) {
                                continue;
                            }
                            *stage.value -= 1;
                            totalTarget -= 1;
                            progressed = true;
                            if (totalTarget <= totalBudget) {
                                break;
                            }
                        }
                        if (!progressed) {
                            break;
                        }
                    }
                }
            }

            TuneAdvisor::setPostExtractionConcurrentDynamicCap(extractionTarget);
            TuneAdvisor::setPostKgConcurrentDynamicCap(kgTarget);
            TuneAdvisor::setPostSymbolConcurrentDynamicCap(symbolTarget);
            TuneAdvisor::setPostEntityConcurrentDynamicCap(entityTarget);
            TuneAdvisor::setPostTitleConcurrentDynamicCap(titleTarget);
            TuneAdvisor::setPostEmbedConcurrentDynamicCap(embedTarget);

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

    // Dynamic pool resizing based on load and pressure
    // Adjust pool sizes to match demand while respecting ResourceGovernor limits
    try {
        auto ipcStats = pm.stats("ipc");
        auto ioStats = pm.stats("ipc_io");

        // Calculate pool utilization (active / current size)
        double ipcUtil = (ipcStats.current_size > 0)
                             ? static_cast<double>(activeConns) / ipcStats.current_size
                             : 0.0;
        double ioUtil = (ioStats.current_size > 0)
                            ? static_cast<double>(muxQueuedBytes) / ioStats.current_size
                            : 0.0;

        // Hysteresis: only resize if utilization has been high/low for multiple ticks
        static uint32_t ipcHighTicks = 0;
        static uint32_t ipcLowTicks = 0;
        static uint32_t ioHighTicks = 0;
        static uint32_t ioLowTicks = 0;

        const double highThreshold = 0.80;  // Grow when >80% utilized
        const double lowThreshold = 0.20;   // Shrink when <20% utilized
        const uint32_t hysteresisTicks = 3; // Require 3 consecutive ticks
        const int32_t scaleStep = static_cast<int32_t>(TuneAdvisor::poolScaleStep());

        // IPC pool sizing
        if (ipcUtil > highThreshold) {
            ipcHighTicks++;
            ipcLowTicks = 0;
            if (ipcHighTicks >= hysteresisTicks) {
                // Grow pool
                int32_t target = static_cast<int32_t>(ipcStats.current_size) + scaleStep;
                int32_t maxSize = static_cast<int32_t>(TuneAdvisor::poolMaxSizeIpc());
                // Check with ResourceGovernor if scaling is allowed
                if (TuneAdvisor::enableResourceGovernor() &&
                    !governor.canScaleUp("ipc", scaleStep)) {
                    maxSize = ipcStats.current_size; // Don't grow
                }
                target = std::min(target, maxSize);
                int32_t delta = target - static_cast<int32_t>(ipcStats.current_size);
                if (delta > 0) {
                    pm.apply_delta(
                        {"ipc", delta, "tuning_load_grow", TuneAdvisor::poolCooldownMs()});
                    spdlog::debug("TuningManager: IPC pool grown by {} (util={:.1f}%)", delta,
                                  ipcUtil * 100.0);
                }
                ipcHighTicks = 0;
            }
        } else if (ipcUtil < lowThreshold) {
            ipcLowTicks++;
            ipcHighTicks = 0;
            if (ipcLowTicks >= hysteresisTicks) {
                // Shrink pool
                int32_t target = static_cast<int32_t>(ipcStats.current_size) - scaleStep;
                int32_t minSize = static_cast<int32_t>(TuneAdvisor::poolMinSizeIpc());
                target = std::max(target, minSize);
                int32_t delta = target - static_cast<int32_t>(ipcStats.current_size);
                if (delta < 0) {
                    pm.apply_delta(
                        {"ipc", delta, "tuning_load_shrink", TuneAdvisor::poolCooldownMs()});
                    spdlog::debug("TuningManager: IPC pool shrunk by {} (util={:.1f}%)", -delta,
                                  ipcUtil * 100.0);
                }
                ipcLowTicks = 0;
            }
        } else {
            // Reset hysteresis counters when in middle range
            ipcHighTicks = 0;
            ipcLowTicks = 0;
        }

        // IO pool sizing (similar logic)
        if (ioUtil > highThreshold) {
            ioHighTicks++;
            ioLowTicks = 0;
            if (ioHighTicks >= hysteresisTicks) {
                int32_t target = static_cast<int32_t>(ioStats.current_size) + scaleStep;
                int32_t maxSize = static_cast<int32_t>(TuneAdvisor::poolMaxSizeIpcIo());
                // Check with ResourceGovernor if scaling is allowed
                if (TuneAdvisor::enableResourceGovernor() &&
                    !governor.canScaleUp("ipc_io", scaleStep)) {
                    maxSize = ioStats.current_size; // Don't grow
                }
                target = std::min(target, maxSize);
                int32_t delta = target - static_cast<int32_t>(ioStats.current_size);
                if (delta > 0) {
                    pm.apply_delta(
                        {"ipc_io", delta, "tuning_load_grow", TuneAdvisor::poolCooldownMs()});
                    spdlog::debug("TuningManager: IO pool grown by {} (util={:.1f}%)", delta,
                                  ioUtil * 100.0);
                }
                ioHighTicks = 0;
            }
        } else if (ioUtil < lowThreshold) {
            ioLowTicks++;
            ioHighTicks = 0;
            if (ioLowTicks >= hysteresisTicks) {
                int32_t target = static_cast<int32_t>(ioStats.current_size) - scaleStep;
                int32_t minSize = static_cast<int32_t>(TuneAdvisor::poolMinSizeIpcIo());
                target = std::max(target, minSize);
                int32_t delta = target - static_cast<int32_t>(ioStats.current_size);
                if (delta < 0) {
                    pm.apply_delta(
                        {"ipc_io", delta, "tuning_load_shrink", TuneAdvisor::poolCooldownMs()});
                    spdlog::debug("TuningManager: IO pool shrunk by {} (util={:.1f}%)", -delta,
                                  ioUtil * 100.0);
                }
                ioLowTicks = 0;
            }
        } else {
            ioHighTicks = 0;
            ioLowTicks = 0;
        }
    } catch (...) {
    }

    // Dynamic connection slot resizing based on utilization (PBI-085)
    // Uses same hysteresis pattern as IPC/IO pools for consistency
    try {
        if (setConnectionSlots_ && state_) {
            static uint32_t slotHighTicks = 0;
            static uint32_t slotLowTicks = 0;

            const size_t activeConns = state_->stats.activeConnections.load();
            const size_t maxConns = state_->stats.maxConnections.load(std::memory_order_relaxed);
            const size_t slotsFree =
                state_->stats.connectionSlotsFree.load(std::memory_order_relaxed);

            if (maxConns > 0) {
                double util = static_cast<double>(activeConns) / static_cast<double>(maxConns);

                const double highThreshold = 0.80;  // Grow when >80% utilized
                const double lowThreshold = 0.20;   // Shrink when <20% utilized
                const uint32_t hysteresisTicks = 3; // Require 3 consecutive ticks
                const uint32_t scaleStep = TuneAdvisor::connectionSlotsScaleStep();
                const uint32_t minSlots = TuneAdvisor::connectionSlotsMin();
                const uint32_t maxSlots = TuneAdvisor::connectionSlotsMax();

                if (util > highThreshold) {
                    slotHighTicks++;
                    slotLowTicks = 0;
                    if (slotHighTicks >= hysteresisTicks) {
                        uint32_t target = static_cast<uint32_t>(maxConns) + scaleStep;
                        // Gate through ResourceGovernor
                        if (TuneAdvisor::enableResourceGovernor() &&
                            !governor.canScaleUp("conn_slots", scaleStep)) {
                            target = static_cast<uint32_t>(maxConns); // Don't grow
                        }
                        target = std::min(target, maxSlots);
                        if (target > maxConns) {
                            setConnectionSlots_(target);
                            spdlog::debug("TuningManager: connection slots grown from {} to {} "
                                          "(util={:.1f}%, active={})",
                                          maxConns, target, util * 100.0, activeConns);
                        }
                        slotHighTicks = 0;
                    }
                } else if (util < lowThreshold && activeConns > 0) {
                    // Only shrink if we have some connections (avoid shrinking at idle)
                    slotLowTicks++;
                    slotHighTicks = 0;
                    if (slotLowTicks >= hysteresisTicks) {
                        uint32_t target = (maxConns > scaleStep)
                                              ? static_cast<uint32_t>(maxConns - scaleStep)
                                              : static_cast<uint32_t>(maxConns);
                        target = std::max(target, minSlots);
                        if (target < maxConns) {
                            setConnectionSlots_(target);
                            spdlog::debug("TuningManager: connection slots shrunk from {} to {} "
                                          "(util={:.1f}%, active={})",
                                          maxConns, target, util * 100.0, activeConns);
                        }
                        slotLowTicks = 0;
                    }
                } else {
                    // Reset hysteresis when in middle range
                    slotHighTicks = 0;
                    slotLowTicks = 0;
                }
            }
        }
    } catch (...) {
    }

    // Expose pool sizes and writer budget to FSM metrics for downstream visibility

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
