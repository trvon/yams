#include <yams/daemon/components/TuningManager.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <limits>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <yams/daemon/components/InternalEventBus.h>

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

namespace {
std::atomic<uint8_t> gPostIngestScaleTestMode{
    static_cast<uint8_t>(TuningManager::PostIngestScaleTestMode::Normal)};
}

TuningManager::TuningManager(ServiceManager* sm, StateComponent* state,
                             WorkCoordinator* coordinator)
    : sm_(sm), state_(state), coordinator_(coordinator), strand_(coordinator->getExecutor()) {}

TuningManager::~TuningManager() {
    stop();
}

void TuningManager::start() {
    if (running_.exchange(true))
        return;

    // Issue 3 fix: reset all hysteresis counters on start()
    lastOnnxMax_ = lastOnnxGliner_ = lastOnnxEmbed_ = lastOnnxReranker_ = 0;
    ipcHighTicks_ = ipcLowTicks_ = ioHighTicks_ = ioLowTicks_ = 0;
    slotHighTicks_ = slotLowTicks_ = 0;
    previousPressureLevel_ = 0;
    previousEmbedDropped_ = 0;
    previousPostProcessed_ = 0;
    previousWriteWaitMicros_ = 0;
    previousReadWaitMicros_ = 0;
    previousWriteTimeoutCount_ = 0;
    previousReadTimeoutCount_ = 0;
    previousWriteFailedAcquisitions_ = 0;
    previousReadFailedAcquisitions_ = 0;
    contentionHealthyTicks_ = 0;

    // Seed FsmMetricsRegistry with initial ResourceGovernor data immediately
    // so status requests have valid data before the first async tick completes.
    // This prevents CLI tools from seeing 0 values for governorBudgetBytes.
    try {
        tick_once();
    } catch (const std::exception& e) {
        spdlog::debug("TuningManager initial tick error: {}", e.what());
    } catch (...) {
    }

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

void TuningManager::rebalanceTargetsByQueue(std::array<uint32_t, 6>& targets,
                                            const std::array<uint32_t, 6>& floors,
                                            const std::array<std::size_t, 6>& queueDepths,
                                            const std::array<bool, 6>& active) {
    auto pressure = [&](std::size_t i) -> double {
        if (!active[i] || queueDepths[i] == 0) {
            return 0.0;
        }
        const uint32_t denom = std::max<uint32_t>(1u, targets[i]);
        return static_cast<double>(queueDepths[i]) / static_cast<double>(denom);
    };

    constexpr std::size_t kMaxIterations = 24;
    for (std::size_t iter = 0; iter < kMaxIterations; ++iter) {
        std::size_t receiver = 0;
        std::size_t donor = 0;
        double maxPressure = -1.0;
        double minPressure = std::numeric_limits<double>::infinity();

        for (std::size_t i = 0; i < targets.size(); ++i) {
            if (!active[i]) {
                continue;
            }
            const double p = pressure(i);
            if (p > maxPressure) {
                maxPressure = p;
                receiver = i;
            }
        }

        for (std::size_t i = 0; i < targets.size(); ++i) {
            if (!active[i] || targets[i] <= floors[i]) {
                continue;
            }
            const double p = pressure(i);
            if (p < minPressure) {
                minPressure = p;
                donor = i;
            }
        }

        if (maxPressure <= 0.0 || minPressure == std::numeric_limits<double>::infinity() ||
            donor == receiver) {
            break;
        }
        // Stop once the pressure skew is small enough.
        if (maxPressure <= minPressure * 1.15) {
            break;
        }

        targets[donor] -= 1;
        targets[receiver] += 1;
    }
}

void TuningManager::testing_rebalanceTargetsByQueue(std::array<uint32_t, 6>& targets,
                                                    const std::array<uint32_t, 6>& floors,
                                                    const std::array<std::size_t, 6>& queueDepths,
                                                    const std::array<bool, 6>& active) {
    rebalanceTargetsByQueue(targets, floors, queueDepths, active);
}

uint32_t TuningManager::computeEmbedScaleBias(std::size_t embedQueued, uint64_t embedDroppedDelta,
                                              std::size_t postQueued, std::size_t embedInFlight) {
    uint32_t bias = 0;

    if (postQueued >= 500) {
        bias = 1;
    }

    if (embedQueued >= 128) {
        bias = std::max<uint32_t>(bias, 1);
    }
    if (embedQueued >= 384) {
        bias = std::max<uint32_t>(bias, 2);
    }
    if (embedQueued >= 768) {
        bias = std::max<uint32_t>(bias, 3);
    }
    if (embedQueued >= 1280) {
        bias = std::max<uint32_t>(bias, 4);
    }
    if (embedQueued >= 2048) {
        bias = std::max<uint32_t>(bias, 5);
    }

    if (embedDroppedDelta > 0) {
        bias = std::max<uint32_t>(bias, 2);
    }
    if (embedDroppedDelta > 20) {
        bias = std::max<uint32_t>(bias, 3);
    }
    if (embedDroppedDelta > 100) {
        bias = std::max<uint32_t>(bias, 4);
    }

    // When embedding is active and backlog is present, add extra pressure.
    if (embedInFlight > 0 && embedQueued > 0) {
        bias += 1;
    }
    if (postQueued >= 750 && embedQueued >= 250) {
        bias += 1;
    }

    return std::min<uint32_t>(bias, 8u);
}

uint32_t TuningManager::testing_computeEmbedScaleBias(std::size_t embedQueued,
                                                      uint64_t embedDroppedDelta,
                                                      std::size_t postQueued,
                                                      std::size_t embedInFlight) {
    return computeEmbedScaleBias(embedQueued, embedDroppedDelta, postQueued, embedInFlight);
}

int32_t TuningManager::computeContentionBudgetAdjustment(
    std::size_t waitingRequests, std::uint64_t waitMicrosDelta, std::size_t timeoutDelta,
    std::size_t failedDelta, std::size_t processedDelta, std::uint32_t healthyTicks) {
    if (timeoutDelta > 0 || failedDelta > 0) {
        return -2;
    }
    if (waitingRequests >= 2 || waitMicrosDelta >= 20000) {
        return -1;
    }
    if (waitingRequests == 0 && waitMicrosDelta <= 1000 && processedDelta >= 4 &&
        healthyTicks >= 2) {
        return 1;
    }
    return 0;
}

int32_t TuningManager::testing_computeContentionBudgetAdjustment(
    std::size_t waitingRequests, std::uint64_t waitMicrosDelta, std::size_t timeoutDelta,
    std::size_t failedDelta, std::size_t processedDelta, std::uint32_t healthyTicks) {
    return computeContentionBudgetAdjustment(waitingRequests, waitMicrosDelta, timeoutDelta,
                                             failedDelta, processedDelta, healthyTicks);
}

void TuningManager::testing_setPostIngestScaleTestMode(PostIngestScaleTestMode mode) {
    gPostIngestScaleTestMode.store(static_cast<uint8_t>(mode), std::memory_order_relaxed);
}

TuningManager::PostIngestScaleTestMode TuningManager::testing_postIngestScaleTestMode() {
    return static_cast<PostIngestScaleTestMode>(
        gPostIngestScaleTestMode.load(std::memory_order_relaxed));
}

bool TuningManager::shouldAllowZeroPostIngestTargets(bool daemonIdle, bool postIngestBusy) {
    const auto mode = testing_postIngestScaleTestMode();
    switch (mode) {
        case PostIngestScaleTestMode::ForceBusy:
            return false;
        case PostIngestScaleTestMode::ForceIdle:
            return true;
        case PostIngestScaleTestMode::Normal:
        default:
            return daemonIdle && !postIngestBusy;
    }
}

bool TuningManager::testing_shouldAllowZeroPostIngestTargets(bool daemonIdle, bool postIngestBusy) {
    return shouldAllowZeroPostIngestTargets(daemonIdle, postIngestBusy);
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
    const uint32_t maxSlots =
        std::max<uint32_t>(std::max<uint32_t>(maxConcurrent, 2u), totalReserved + 1);

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

    // Resource governor must keep running during startup so pressure/readiness
    // state remains current even before all metadata services are ready.
    auto& governor = ResourceGovernor::instance();
    ResourceSnapshot govSnap = governor.tick(sm_);

    // Don't perform adaptive tuning until services are at least partially ready,
    // to avoid acting on default pool configs.
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
    // Dynamically clamp ONNX concurrency to governor caps (keeps global pools aligned)
    try {
        auto& registry = OnnxConcurrencyRegistry::instance();
        uint32_t desiredMax = std::max<uint32_t>(2u, TuneAdvisor::onnxMaxConcurrent());
        uint32_t glinerReserved = TuneAdvisor::onnxGlinerReserved();
        uint32_t embedReserved = TuneAdvisor::onnxEmbedReserved();
        uint32_t rerankerReserved = TuneAdvisor::onnxRerankerReserved();

        const bool underPressure = TuneAdvisor::enableResourceGovernor() &&
                                   govSnap.level >= ResourcePressureLevel::Critical;
        if (underPressure) {
            desiredMax = std::min<uint32_t>(desiredMax, governor.maxEmbedConcurrency());
        } else {
            // Under normal conditions, preserve the invariant that there is at least 1 shared slot
            // beyond the configured reserved total.
            const uint32_t totalReservedWanted = glinerReserved + embedReserved + rerankerReserved;
            desiredMax = std::max<uint32_t>(desiredMax, totalReservedWanted + 1u);
        }

        desiredMax = std::max<uint32_t>(desiredMax, 2u);
        if (desiredMax > 64u) { // counting_semaphore<64> limit in registry
            desiredMax = 64u;
        }

        // Always leave at least 1 shared slot beyond reserved budgets when possible.
        uint32_t remainingForReserved = (desiredMax > 1u) ? (desiredMax - 1u) : 0u;
        auto consume = [&remainingForReserved](uint32_t v) {
            if (remainingForReserved == 0)
                return 0u;
            uint32_t take = std::min(v, remainingForReserved);
            remainingForReserved -= take;
            return take;
        };
        uint32_t effGliner = consume(glinerReserved);
        uint32_t effEmbed = consume(embedReserved);
        uint32_t effReranker = consume(rerankerReserved);

        // Apply reserved first, then maxSlots to avoid transient states where reserved > max.
        if (lastOnnxGliner_ != effGliner) {
            registry.setReservedSlots(OnnxLane::Gliner, effGliner);
            lastOnnxGliner_ = effGliner;
        }
        if (lastOnnxEmbed_ != effEmbed) {
            registry.setReservedSlots(OnnxLane::Embedding, effEmbed);
            lastOnnxEmbed_ = effEmbed;
        }
        if (lastOnnxReranker_ != effReranker) {
            registry.setReservedSlots(OnnxLane::Reranker, effReranker);
            lastOnnxReranker_ = effReranker;
        }
        if (lastOnnxMax_ != desiredMax) {
            registry.setMaxSlots(desiredMax);
            lastOnnxMax_ = desiredMax;
            spdlog::info("[TuningManager] ONNX slots set to {}", desiredMax);
        }
    } catch (...) {
    }

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
        auto pq = sm_->getPostIngestQueue();
        if (pq) {
            const std::size_t queuedItems = pq->size();
            [[maybe_unused]] const std::size_t currentInFlight = pq->totalInFlight();
            const std::size_t kgQueued = pq->kgQueueDepth();
            const std::size_t symbolQueued = pq->symbolQueueDepth();
            const std::size_t entityQueued = pq->entityQueueDepth();
            const std::size_t titleQueued = pq->titleQueueDepth();

            auto& bus = InternalEventBus::instance();
            const std::size_t embedQueuedJobs = sm_->getEmbeddingQueuedJobs();
            [[maybe_unused]] const std::size_t embedInFlight = sm_->getEmbeddingInFlightJobs();
            // Prefer document-level backlog pressure over job-count depth.
            // Embed jobs can carry many hashes, so job-depth alone can under-report pressure.
            const uint64_t embedQueuedTotal = bus.embedQueued();
            const uint64_t embedConsumedTotal = bus.embedConsumed();
            const std::size_t embedQueuedDocs = static_cast<std::size_t>(
                (embedQueuedTotal >= embedConsumedTotal) ? (embedQueuedTotal - embedConsumedTotal)
                                                         : embedQueuedTotal);
            const std::size_t embedQueued = std::max(embedQueuedJobs, embedQueuedDocs);
            const uint64_t embedDroppedTotal = bus.embedDropped();
            uint64_t embedDroppedDelta = 0;
            if (embedDroppedTotal >= previousEmbedDropped_) {
                embedDroppedDelta = embedDroppedTotal - previousEmbedDropped_;
            } else {
                // Counter reset/restart: treat current value as delta for this tick.
                embedDroppedDelta = embedDroppedTotal;
            }
            previousEmbedDropped_ = embedDroppedTotal;

            uint32_t baseBudget = TuneAdvisor::postIngestTotalConcurrent();
            uint32_t scaleBias =
                computeEmbedScaleBias(embedQueued, embedDroppedDelta, queuedItems, embedInFlight);
            // Cap scaled budget to hardware concurrency to avoid oversubscription
            uint32_t hwCap = TuneAdvisor::hardwareConcurrency();
            uint32_t totalBudget = std::min<uint32_t>(hwCap, baseBudget + scaleBias);

            std::size_t processedDelta = 0;
            {
                const auto processedNow = pq->processed();
                if (processedNow >= previousPostProcessed_) {
                    processedDelta = processedNow - previousPostProcessed_;
                } else {
                    processedDelta = processedNow;
                }
                previousPostProcessed_ = processedNow;
            }

            std::size_t waitingRequests = 0;
            std::uint64_t waitMicrosDelta = 0;
            std::size_t timeoutDelta = 0;
            std::size_t failedDelta = 0;

            if (auto writePool = sm_->getWriteConnectionPool()) {
                const auto stats = writePool->getStats();
                waitingRequests += stats.waitingRequests;
                waitMicrosDelta += (stats.totalWaitMicros >= previousWriteWaitMicros_)
                                       ? (stats.totalWaitMicros - previousWriteWaitMicros_)
                                       : stats.totalWaitMicros;
                timeoutDelta += (stats.timeoutCount >= previousWriteTimeoutCount_)
                                    ? (stats.timeoutCount - previousWriteTimeoutCount_)
                                    : stats.timeoutCount;
                failedDelta += (stats.failedAcquisitions >= previousWriteFailedAcquisitions_)
                                   ? (stats.failedAcquisitions - previousWriteFailedAcquisitions_)
                                   : stats.failedAcquisitions;
                previousWriteWaitMicros_ = stats.totalWaitMicros;
                previousWriteTimeoutCount_ = stats.timeoutCount;
                previousWriteFailedAcquisitions_ = stats.failedAcquisitions;
            }

            if (auto readPool = sm_->getReadConnectionPool()) {
                const auto stats = readPool->getStats();
                waitingRequests += stats.waitingRequests;
                waitMicrosDelta += (stats.totalWaitMicros >= previousReadWaitMicros_)
                                       ? (stats.totalWaitMicros - previousReadWaitMicros_)
                                       : stats.totalWaitMicros;
                timeoutDelta += (stats.timeoutCount >= previousReadTimeoutCount_)
                                    ? (stats.timeoutCount - previousReadTimeoutCount_)
                                    : stats.timeoutCount;
                failedDelta += (stats.failedAcquisitions >= previousReadFailedAcquisitions_)
                                   ? (stats.failedAcquisitions - previousReadFailedAcquisitions_)
                                   : stats.failedAcquisitions;
                previousReadWaitMicros_ = stats.totalWaitMicros;
                previousReadTimeoutCount_ = stats.timeoutCount;
                previousReadFailedAcquisitions_ = stats.failedAcquisitions;
            }

            const bool healthyWindow =
                (waitingRequests == 0 && waitMicrosDelta <= 1000 && timeoutDelta == 0 &&
                 failedDelta == 0 && processedDelta >= 4);
            if (healthyWindow) {
                contentionHealthyTicks_ =
                    std::min<std::uint32_t>(contentionHealthyTicks_ + 1, 1000u);
            } else {
                contentionHealthyTicks_ = 0;
            }

            const int32_t contentionAdjust = computeContentionBudgetAdjustment(
                waitingRequests, waitMicrosDelta, timeoutDelta, failedDelta, processedDelta,
                contentionHealthyTicks_);
            if (contentionAdjust < 0) {
                const uint32_t drop = static_cast<uint32_t>(-contentionAdjust);
                totalBudget = (totalBudget > 2u) ? std::max<uint32_t>(2u, totalBudget - drop) : 2u;
                contentionHealthyTicks_ = 0;
            } else if (contentionAdjust > 0) {
                totalBudget = std::min<uint32_t>(
                    hwCap, totalBudget + static_cast<uint32_t>(contentionAdjust));
                contentionHealthyTicks_ = 0;
            }

            const uint64_t dbLockErrors = TuneAdvisor::getAndResetDbLockErrors();
            const uint32_t lockThreshold = TuneAdvisor::dbLockErrorThreshold();

            auto budget = TuneAdvisor::postIngestBudgetAll(/*includeDynamicCaps=*/true);
            uint32_t extractionTarget = budget.extraction;
            uint32_t kgTarget = budget.kg;
            uint32_t symbolTarget = budget.symbol;
            uint32_t entityTarget = budget.entity;
            uint32_t titleTarget = budget.title;
            uint32_t embedTarget = budget.embed;
            uint32_t embedFloor = 0;

            // Keep embed fan-out responsive to backlog growth, especially during model warmup
            // and bursty ingestion where job-depth can lag document-depth.
            if (embedQueued >= 128) {
                embedFloor = 2;
            }
            if (embedQueued >= 512) {
                embedFloor = 4;
            }
            if (embedQueued >= 1024) {
                embedFloor = 6;
            }
            if (embedDroppedDelta > 0) {
                embedFloor = std::max<uint32_t>(embedFloor, 4u);
            }
            if (queuedItems >= 512 && embedQueued >= 128) {
                embedFloor = std::max<uint32_t>(embedFloor, 4u);
            }

            // Override with gradient limiter values if enabled
            if (TuneAdvisor::enableGradientLimiters()) {
                // Propagate governor pressure to limiters before reading
                const auto pressureLevel = static_cast<uint8_t>(govSnap.level);
                auto applyPressureToLimiter = [pressureLevel](GradientLimiter* lim) {
                    if (lim)
                        lim->applyPressure(pressureLevel);
                };
                applyPressureToLimiter(pq->extractionLimiter());
                applyPressureToLimiter(pq->kgLimiter());
                applyPressureToLimiter(pq->symbolLimiter());
                applyPressureToLimiter(pq->entityLimiter());
                applyPressureToLimiter(pq->titleLimiter());

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
                // Embed limiter is not currently driven by embed job RTT/completions, so
                // keep embed target derived from budget/governor instead of stale limiter state.
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

            const bool applyGovernorConcurrencyCaps =
                TuneAdvisor::enableResourceGovernor() &&
                govSnap.level >= ResourcePressureLevel::Critical;
            if (applyGovernorConcurrencyCaps) {
                extractionTarget = std::min(extractionTarget, governor.maxExtractionConcurrency());
                kgTarget = std::min(kgTarget, governor.maxKgConcurrency());
                // PBI-081 Phase 3: governor owns a single shared enrich budget.
                // Symbol+entity+title collectively must not exceed enrichCap.
                const uint32_t enrichCap = governor.maxEnrichConcurrency();
                const uint32_t enrichSum = symbolTarget + entityTarget + titleTarget;
                if (enrichSum > enrichCap && enrichSum > 0) {
                    // Scale each sub-stage proportionally to fit within the shared cap.
                    const double scale =
                        static_cast<double>(enrichCap) / static_cast<double>(enrichSum);
                    symbolTarget = static_cast<uint32_t>(symbolTarget * scale);
                    entityTarget = static_cast<uint32_t>(entityTarget * scale);
                    // Title gets the remainder to avoid rounding loss.
                    titleTarget = enrichCap - symbolTarget - entityTarget;
                }
                const uint32_t embedCap = governor.maxEmbedConcurrency();
                embedTarget = std::min(embedCap, std::max(embedTarget, embedFloor));
            } else {
                embedTarget = std::max(embedTarget, embedFloor);
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
                const bool daemonIdle =
                    (activeConns == 0) && (workerQueued == 0) && (muxQueuedBytes == 0);
                const bool postIngestBusy = (queuedItems > 0) || (currentInFlight > 0) ||
                                            (kgQueued > 0) || (symbolQueued > 0) ||
                                            (entityQueued > 0) || (titleQueued > 0) ||
                                            (embedQueued > 0) || (embedInFlight > 0);
                const bool allowZeroTargets =
                    shouldAllowZeroPostIngestTargets(daemonIdle, postIngestBusy);

                const std::array<bool, 6> stageAllowed = {
                    (activeMask & (1u << 0u)) != 0u, (activeMask & (1u << 1u)) != 0u,
                    (activeMask & (1u << 2u)) != 0u, (activeMask & (1u << 3u)) != 0u,
                    (activeMask & (1u << 4u)) != 0u, (activeMask & (1u << 5u)) != 0u,
                };

                const uint32_t minExtraction =
                    (totalBudget >= 1 && stageAllowed[0] && !allowZeroTargets) ? 1u : 0u;
                const uint32_t minKg = (stageAllowed[1] && !allowZeroTargets) ? 1u : 0u;
                const uint32_t minSymbol = (stageAllowed[2] && !allowZeroTargets) ? 1u : 0u;
                const uint32_t minEntity = (stageAllowed[3] && !allowZeroTargets) ? 1u : 0u;
                const uint32_t minTitle = (stageAllowed[4] && !allowZeroTargets) ? 1u : 0u;
                const uint32_t minEmbed =
                    (stageAllowed[5] && !allowZeroTargets) ? std::max(embedFloor, 1u) : 0u;

                if (minExtraction > 0) {
                    extractionTarget = std::max(extractionTarget, minExtraction);
                }
                if (minKg > 0) {
                    kgTarget = std::max(kgTarget, minKg);
                }
                if (minSymbol > 0) {
                    symbolTarget = std::max(symbolTarget, minSymbol);
                }
                if (minEntity > 0) {
                    entityTarget = std::max(entityTarget, minEntity);
                }
                if (minTitle > 0) {
                    titleTarget = std::max(titleTarget, minTitle);
                }
                if (minEmbed > 0) {
                    embedTarget = std::max(embedTarget, minEmbed);
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
                        StageRef{&entityTarget, minEntity},
                        StageRef{&titleTarget, minTitle},
                        StageRef{&symbolTarget, minSymbol},
                        StageRef{&kgTarget, minKg},
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

                // Fill any slack directly to the most backlogged active stages.
                {
                    auto chooseMostBacklogged = [&]() -> int {
                        std::array<std::size_t, 6> depths = {queuedItems,  kgQueued,
                                                             symbolQueued, entityQueued,
                                                             titleQueued,  embedQueued};
                        int best = -1;
                        std::size_t bestDepth = 0;
                        for (std::size_t i = 0; i < depths.size(); ++i) {
                            if (!stageAllowed[i]) {
                                continue;
                            }
                            if (depths[i] > bestDepth) {
                                bestDepth = depths[i];
                                best = static_cast<int>(i);
                            }
                        }
                        return best;
                    };

                    uint32_t totalTarget = sumTargets();
                    while (totalTarget < totalBudget) {
                        const int idx = chooseMostBacklogged();
                        if (idx < 0) {
                            break;
                        }
                        switch (idx) {
                            case 0:
                                ++extractionTarget;
                                break;
                            case 1:
                                ++kgTarget;
                                break;
                            case 2:
                                ++symbolTarget;
                                break;
                            case 3:
                                ++entityTarget;
                                break;
                            case 4:
                                ++titleTarget;
                                break;
                            case 5:
                                ++embedTarget;
                                break;
                            default:
                                break;
                        }
                        ++totalTarget;
                    }
                }

                // Dynamic stage-agnostic rebalance by queue pressure.
                std::array<uint32_t, 6> stageTargets = {extractionTarget, kgTarget,    symbolTarget,
                                                        entityTarget,     titleTarget, embedTarget};
                std::array<uint32_t, 6> stageFloors = {minExtraction, minKg,    minSymbol,
                                                       minEntity,     minTitle, minEmbed};
                std::array<std::size_t, 6> queueDepths = {queuedItems,  kgQueued,    symbolQueued,
                                                          entityQueued, titleQueued, embedQueued};
                rebalanceTargetsByQueue(stageTargets, stageFloors, queueDepths, stageAllowed);
                extractionTarget = stageTargets[0];
                kgTarget = stageTargets[1];
                symbolTarget = stageTargets[2];
                entityTarget = stageTargets[3];
                titleTarget = stageTargets[4];
                embedTarget = stageTargets[5];
            }

            const auto currentPressure = static_cast<uint8_t>(govSnap.level);
            if (currentPressure < previousPressureLevel_) {
                // Pressure dropped — clear all DynamicCaps to let base budget through
                TuneAdvisor::beginDynamicCapWrite();
                TuneAdvisor::setPostExtractionConcurrentDynamicCap(0);
                TuneAdvisor::setPostKgConcurrentDynamicCap(0);
                TuneAdvisor::setPostSymbolConcurrentDynamicCap(0);
                TuneAdvisor::setPostEntityConcurrentDynamicCap(0);
                TuneAdvisor::setPostTitleConcurrentDynamicCap(0);
                TuneAdvisor::setPostEmbedConcurrentDynamicCap(0);
                TuneAdvisor::endDynamicCapWrite();
            }
            previousPressureLevel_ = currentPressure;

            TuneAdvisor::beginDynamicCapWrite();
            TuneAdvisor::setPostExtractionConcurrentDynamicCap(extractionTarget);
            TuneAdvisor::setPostKgConcurrentDynamicCap(kgTarget);
            TuneAdvisor::setPostSymbolConcurrentDynamicCap(symbolTarget);
            TuneAdvisor::setPostEntityConcurrentDynamicCap(entityTarget);
            TuneAdvisor::setPostTitleConcurrentDynamicCap(titleTarget);
            TuneAdvisor::setPostEmbedConcurrentDynamicCap(embedTarget);
            TuneAdvisor::endDynamicCapWrite();

            // Align EmbeddingGenerator global gate with governor caps
            uint32_t baseEmbedGuard = TuneAdvisor::embedMaxConcurrencyBase();
            uint32_t desiredEmbedGuard = baseEmbedGuard;
            if (applyGovernorConcurrencyCaps) {
                desiredEmbedGuard = std::min(desiredEmbedGuard, governor.maxEmbedConcurrency());
            }
            if (desiredEmbedGuard < baseEmbedGuard) {
                TuneAdvisor::setEmbedMaxConcurrencyDynamicCap(desiredEmbedGuard);
            } else {
                TuneAdvisor::setEmbedMaxConcurrencyDynamicCap(0);
            }

#if defined(TRACY_ENABLE)
            TracyPlot("post.queued", static_cast<double>(queuedItems));
            TracyPlot("post.inflight", static_cast<double>(currentInFlight));
            TracyPlot("post.extraction.limit", static_cast<double>(extractionTarget));
            TracyPlot("post.kg.limit", static_cast<double>(kgTarget));
            TracyPlot("post.embed.limit", static_cast<double>(embedTarget));
            TracyPlot("post.embed.queued", static_cast<double>(embedQueued));
            TracyPlot("post.embed.inflight", static_cast<double>(embedInFlight));
            TracyPlot("post.embed.dropped_delta", static_cast<double>(embedDroppedDelta));
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
        auto ipcStats = governor.poolStats("ipc");
        auto ioStats = governor.poolStats("ipc_io");

        // Calculate pool utilization (active / current size)
        double ipcUtil = (ipcStats.current_size > 0)
                             ? static_cast<double>(activeConns) / ipcStats.current_size
                             : 0.0;
        double ioUtil = (ioStats.current_size > 0)
                            ? static_cast<double>(muxQueuedBytes) / ioStats.current_size
                            : 0.0;

        // Hysteresis: only resize if utilization has been high/low for multiple ticks
        // NOTE: These are member variables (not static locals) so they reset on stop/start.
        // See ipcHighTicks_, ipcLowTicks_, ioHighTicks_, ioLowTicks_ in TuningManager.h

        const double highThreshold = 0.80;  // Grow when >80% utilized
        const double lowThreshold = 0.20;   // Shrink when <20% utilized
        const uint32_t hysteresisTicks = 3; // Require 3 consecutive ticks
        const int32_t scaleStep = static_cast<int32_t>(TuneAdvisor::poolScaleStep());

        // IPC pool sizing
        if (ipcUtil > highThreshold) {
            ipcHighTicks_++;
            ipcLowTicks_ = 0;
            if (ipcHighTicks_ >= hysteresisTicks) {
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
                    const auto newSize = governor.applyPoolDelta(
                        {"ipc", delta, "tuning_load_grow", TuneAdvisor::poolCooldownMs()});
                    spdlog::debug("TuningManager: IPC pool grown by {} to {} (util={:.1f}%)", delta,
                                  newSize, ipcUtil * 100.0);
                }
                ipcHighTicks_ = 0;
            }
        } else if (ipcUtil < lowThreshold) {
            ipcLowTicks_++;
            ipcHighTicks_ = 0;
            if (ipcLowTicks_ >= hysteresisTicks) {
                // Shrink pool
                int32_t target = static_cast<int32_t>(ipcStats.current_size) - scaleStep;
                int32_t minSize = static_cast<int32_t>(TuneAdvisor::poolMinSizeIpc());
                target = std::max(target, minSize);
                int32_t delta = target - static_cast<int32_t>(ipcStats.current_size);
                if (delta < 0) {
                    const auto newSize = governor.applyPoolDelta(
                        {"ipc", delta, "tuning_load_shrink", TuneAdvisor::poolCooldownMs()});
                    spdlog::debug("TuningManager: IPC pool shrunk by {} to {} (util={:.1f}%)",
                                  -delta, newSize, ipcUtil * 100.0);
                }
                ipcLowTicks_ = 0;
            }
        } else {
            // Reset hysteresis counters when in middle range
            ipcHighTicks_ = 0;
            ipcLowTicks_ = 0;
        }

        // IO pool sizing (similar logic)
        if (ioUtil > highThreshold) {
            ioHighTicks_++;
            ioLowTicks_ = 0;
            if (ioHighTicks_ >= hysteresisTicks) {
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
                    const auto newSize = governor.applyPoolDelta(
                        {"ipc_io", delta, "tuning_load_grow", TuneAdvisor::poolCooldownMs()});
                    spdlog::debug("TuningManager: IO pool grown by {} to {} (util={:.1f}%)", delta,
                                  newSize, ioUtil * 100.0);
                }
                ioHighTicks_ = 0;
            }
        } else if (ioUtil < lowThreshold) {
            ioLowTicks_++;
            ioHighTicks_ = 0;
            if (ioLowTicks_ >= hysteresisTicks) {
                int32_t target = static_cast<int32_t>(ioStats.current_size) - scaleStep;
                int32_t minSize = static_cast<int32_t>(TuneAdvisor::poolMinSizeIpcIo());
                target = std::max(target, minSize);
                int32_t delta = target - static_cast<int32_t>(ioStats.current_size);
                if (delta < 0) {
                    const auto newSize = governor.applyPoolDelta(
                        {"ipc_io", delta, "tuning_load_shrink", TuneAdvisor::poolCooldownMs()});
                    spdlog::debug("TuningManager: IO pool shrunk by {} to {} (util={:.1f}%)",
                                  -delta, newSize, ioUtil * 100.0);
                }
                ioLowTicks_ = 0;
            }
        } else {
            ioHighTicks_ = 0;
            ioLowTicks_ = 0;
        }
    } catch (...) {
    }

    // Dynamic connection slot resizing based on utilization (PBI-085)
    // Uses same hysteresis pattern as IPC/IO pools for consistency
    try {
        if (setConnectionSlots_ && state_) {
            // NOTE: slotHighTicks_/slotLowTicks_ are member variables (not static locals)
            // so they reset on stop/start. See TuningManager.h

            const size_t activeConns = state_->stats.activeConnections.load();
            const size_t maxConns = state_->stats.maxConnections.load(std::memory_order_relaxed);
            if (maxConns > 0) {
                double util = static_cast<double>(activeConns) / static_cast<double>(maxConns);

                const double highThreshold = 0.80;  // Grow when >80% utilized
                const double lowThreshold = 0.20;   // Shrink when <20% utilized
                const uint32_t hysteresisTicks = 3; // Require 3 consecutive ticks
                const uint32_t scaleStep = TuneAdvisor::connectionSlotsScaleStep();
                const uint32_t minSlots = TuneAdvisor::connectionSlotsMin();
                const uint32_t maxSlots = TuneAdvisor::connectionSlotsMax();

                if (util > highThreshold) {
                    slotHighTicks_++;
                    slotLowTicks_ = 0;
                    if (slotHighTicks_ >= hysteresisTicks) {
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
                        slotHighTicks_ = 0;
                    }
                } else if (util < lowThreshold && activeConns > 0) {
                    // Only shrink if we have some connections (avoid shrinking at idle)
                    slotLowTicks_++;
                    slotHighTicks_ = 0;
                    if (slotLowTicks_ >= hysteresisTicks) {
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
                        slotLowTicks_ = 0;
                    }
                } else {
                    // Reset hysteresis when in middle range
                    slotHighTicks_ = 0;
                    slotLowTicks_ = 0;
                }
            }
        }
    } catch (...) {
    }

    // Expose pool sizes and writer budget to FSM metrics for downstream visibility

    // Expose pool sizes and writer budget to FSM metrics for downstream visibility
    try {
        auto ipcStats = governor.poolStats("ipc");
        auto ioStats = governor.poolStats("ipc_io");
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

            uint32_t tokens = TuneAdvisor::repairTokensIdle();
            if (busyHeld) {
                // Check if PostIngestQueue is actively processing — if so, allow
                // cooperative repair tokens instead of fully halting.
                bool piqActive = false;
                if (sm_) {
                    if (auto pq = sm_->getPostIngestQueue()) {
                        piqActive = pq->started() && (pq->size() > 0 || pq->totalInFlight() > 0);
                    }
                }
                // Under Critical/Emergency pressure the governor has already
                // paused stages and blocked admission; halt repair entirely.
                if (govSnap.level >= ResourcePressureLevel::Critical) {
                    tokens = 0;
                } else if (piqActive) {
                    tokens = TuneAdvisor::repairTokensBusy();
                } else {
                    tokens = TuneAdvisor::repairTokensBusy();
                }
            }
            uint32_t batch = TuneAdvisor::repairMaxBatch();

            // Under Warning pressure, reduce batch size to slow repair throughput
            // proportionally, giving more bus capacity to the ingestion pipeline.
            if (govSnap.level >= ResourcePressureLevel::Warning && batch > 1) {
                batch = std::max<uint32_t>(1, batch / 2);
            }

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
            std::size_t embedQueued = 0;
            std::size_t embedInFlight = 0;
            if (auto pq = sm_->getPostIngestQueue()) {
                postIngestQueued = pq->size();
                currentInFlight = pq->totalInFlight();
            }
            embedQueued = sm_->getEmbeddingQueuedJobs();
            embedInFlight = sm_->getEmbeddingInFlightJobs();

            // Profile-aware thresholds: Efficient=permissive, Balanced=moderate, Aggressive=strict
            const uint32_t connThresh = TuneAdvisor::modelMaintenanceConnThreshold();
            const uint32_t searchThresh = TuneAdvisor::modelMaintenanceSearchThreshold();
            const uint32_t queueThresh = TuneAdvisor::modelMaintenanceQueueThreshold();

            bool canDoMaintenance = (activeConns <= connThresh) &&
                                    (searchMetrics.active <= searchThresh) &&
                                    (postIngestQueued <= queueThresh) && (currentInFlight == 0) &&
                                    (embedQueued == 0) && (embedInFlight == 0);

            if (canDoMaintenance) {
                if (auto provider = sm_->getModelProvider()) {
                    provider->releaseUnusedResources();
                }
            } else {
                spdlog::debug("[TuningManager] Skip model maintenance: conns={} search_active={} "
                              "post_q={} post_inflight={} embed_q={} embed_inflight={}",
                              activeConns, searchMetrics.active, postIngestQueued, currentInFlight,
                              embedQueued, embedInFlight);
            }
        }
    } catch (...) {
    }
}

} // namespace yams::daemon
