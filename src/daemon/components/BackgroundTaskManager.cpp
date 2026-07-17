// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/daemon/components/BackgroundTaskManager.h>
#include <yams/daemon/components/CheckpointManager.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/repair/repair_health_probe.h>
#include <yams/daemon/components/repair/repair_plan_builder.h>
#include <yams/daemon/components/RepairService.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningManager.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/extraction/extraction_util.h>
#include <yams/integrity/repair_manager.h>
#include <yams/metadata/knowledge_graph_store.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <exception>
#include <limits>
#include <stdexcept>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

namespace {

template <typename Rep, typename Period>
boost::asio::awaitable<bool> coWaitForDelay(const std::shared_ptr<boost::asio::steady_timer>& timer,
                                            const std::chrono::duration<Rep, Period>& delay) {
    timer->expires_after(delay);
    try {
        co_await timer->async_wait(boost::asio::use_awaitable);
        co_return true;
    } catch (const boost::system::system_error& e) {
        if (e.code() == boost::asio::error::operation_aborted) {
            co_return false;
        }
        throw;
    }
}

} // namespace

namespace yams::daemon {

BackgroundTaskManager::BackgroundTaskManager(Dependencies deps)
    : deps_(deps), stopRequested_(std::make_shared<std::atomic<bool>>(false)) {
    if (!deps_.executor) {
        throw std::invalid_argument(
            "BackgroundTaskManager: executor cannot be null (must use WorkCoordinator)");
    }
}

BackgroundTaskManager::~BackgroundTaskManager() {
    if (running_.load(std::memory_order_acquire)) {
        stop();
    } else {
        requestStopAndQuiesceTrackedCoroutines();
    }
}

void BackgroundTaskManager::registerTrackedCoroutine() {
    std::lock_guard<std::mutex> lock(trackedCoroutineMutex_);
    ++trackedCoroutineCount_;
}

void BackgroundTaskManager::unregisterTrackedCoroutine() {
    std::lock_guard<std::mutex> lock(trackedCoroutineMutex_);
    if (trackedCoroutineCount_ == 0) {
        return;
    }
    --trackedCoroutineCount_;
    if (trackedCoroutineCount_ == 0) {
        trackedCoroutineCv_.notify_all();
    }
}

void BackgroundTaskManager::registerTrackedTimer(
    const std::shared_ptr<boost::asio::steady_timer>& timer) {
    std::lock_guard<std::mutex> lock(trackedCoroutineMutex_);
    trackedTimers_.erase(std::remove_if(trackedTimers_.begin(), trackedTimers_.end(),
                                        [](const auto& weakTimer) { return weakTimer.expired(); }),
                         trackedTimers_.end());
    trackedTimers_.push_back(timer);
}

void BackgroundTaskManager::cancelTrackedTimers() {
    std::vector<std::shared_ptr<boost::asio::steady_timer>> timers;
    {
        std::lock_guard<std::mutex> lock(trackedCoroutineMutex_);
        trackedTimers_.erase(std::remove_if(trackedTimers_.begin(), trackedTimers_.end(),
                                            [&](const auto& weakTimer) {
                                                auto timer = weakTimer.lock();
                                                if (!timer) {
                                                    return true;
                                                }
                                                timers.push_back(timer);
                                                return false;
                                            }),
                             trackedTimers_.end());
    }

    for (const auto& timer : timers) {
        if (!timer) {
            continue;
        }
        boost::asio::post(timer->get_executor(), [timer]() {
            try {
                timer->cancel();
            } catch (const std::exception& e) {
                spdlog::debug("[BackgroundTaskManager] Timer cancel failed: {}", e.what());
            } catch (...) {
                spdlog::debug("[BackgroundTaskManager] Timer cancel failed: unknown exception");
            }
        });
    }
}

void BackgroundTaskManager::waitForTrackedCoroutines() {
    std::unique_lock<std::mutex> lock(trackedCoroutineMutex_);
    trackedCoroutineCv_.wait(lock, [this] { return trackedCoroutineCount_ == 0; });
    trackedTimers_.clear();
}

void BackgroundTaskManager::requestStopAndQuiesceTrackedCoroutines() {
    stopRequested_->store(true, std::memory_order_release);
    cancelTrackedTimers();
    waitForTrackedCoroutines();
}

void BackgroundTaskManager::start() {
    // Idempotent check
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        spdlog::debug("[BackgroundTaskManager] Already running, skipping start");
        return;
    }

    {
        std::lock_guard<std::mutex> lock(trackedCoroutineMutex_);
        trackedTimers_.clear();
    }
    stopRequested_->store(false, std::memory_order_release);

    spdlog::debug("[BackgroundTaskManager] Starting background task coroutines");

    // Validate ServiceManager is available
    if (deps_.serviceManager.expired()) {
        spdlog::error(
            "[BackgroundTaskManager] Cannot launch consumers: ServiceManager weak_ptr expired");
        running_.store(false, std::memory_order_release);
        throw std::runtime_error("BackgroundTaskManager: ServiceManager weak_ptr expired");
    }

    // Launch all consumer coroutines
    // NOTE: EmbedJob is handled by EmbeddingService (on a strand for serialization)
    // Do NOT launch a duplicate consumer here - it causes race conditions and
    // Windows thread pool exhaustion ("resource deadlock would occur").
    try {
        launchFts5JobConsumer();
        launchOrphanScanTask();
        launchCheckpointTask();
        launchStorageGcTask();
        launchGraphPruneTask();
        launchAutoRepairTask();
        spdlog::debug("[BackgroundTaskManager] Background tasks launched successfully");
    } catch (const std::exception& e) {
        spdlog::error("[BackgroundTaskManager] Failed to launch background tasks: {}", e.what());
        running_.store(false, std::memory_order_release);
        requestStopAndQuiesceTrackedCoroutines();
        deps_.lifecycleFsm.setSubsystemDegraded("background_tasks", true, e.what());
        throw;
    }
}

void BackgroundTaskManager::stop() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
        spdlog::warn("[BackgroundTaskManager] Stop called but not running");
        return;
    }

    spdlog::debug("[BackgroundTaskManager] Stopping background tasks");
    requestStopAndQuiesceTrackedCoroutines();
    spdlog::debug("[BackgroundTaskManager] Background tasks quiesced");
}

// NOTE: EmbedJob consumer was removed - EmbeddingService now handles embed_jobs
// on a strand for proper serialization. Having two consumers caused race conditions
// and Windows thread pool exhaustion ("resource deadlock would occur").

void BackgroundTaskManager::launchFts5JobConsumer() {
    auto self = deps_.serviceManager.lock();
    if (!self) {
        throw std::runtime_error(
            "BackgroundTaskManager: ServiceManager weak_ptr expired in launchFts5JobConsumer");
    }

    auto exec = deps_.executor;
    auto stopFlag = stopRequested_;

    spdlog::debug("[BackgroundTaskManager] Launching Fts5Job consumer");
    registerTrackedCoroutine();
    try {
        boost::asio::co_spawn(
            exec,
            [this, self, stopFlag]() -> boost::asio::awaitable<void> {
                auto coroutineGuard =
                    std::shared_ptr<void>(nullptr, [this](void*) { unregisterTrackedCoroutine(); });
                spdlog::debug("[Fts5Job] Consumer started");
                using Bus = yams::daemon::InternalEventBus;
                auto channel =
                    Bus::instance().get_or_create_channel<Bus::Fts5Job>("fts5_jobs", 512);
                using namespace std::chrono_literals;

                auto executor = co_await boost::asio::this_coro::executor;
                auto timer =
                    std::make_shared<boost::asio::steady_timer>(boost::asio::make_strand(executor));
                registerTrackedTimer(timer);
                if (stopFlag->load(std::memory_order_acquire)) {
                    co_return;
                }

                const uint32_t startupDelayMs = TuneAdvisor::fts5StartupDelayMs();
                const uint32_t startupThrottleMs = TuneAdvisor::fts5StartupThrottleMs();
                const uint32_t normalThrottleMs = 10;
                const auto idleThrottleMs = []() {
                    auto snap = TuningSnapshotRegistry::instance().get();
                    if (snap && snap->daemonIdle) {
                        return std::max<uint32_t>(TuneAdvisor::idleTickMs(), snap->workerPollMs);
                    }
                    uint32_t pollMs = snap ? snap->workerPollMs : TuneAdvisor::workerPollMs();
                    return std::max<uint32_t>(10, pollMs);
                };

                spdlog::debug(
                    "[Fts5Job] Startup delay={}ms, startup throttle={}ms, normal throttle={}ms",
                    startupDelayMs, startupThrottleMs, normalThrottleMs);

                bool startupComplete = false;
                int64_t jobsProcessedDuringStartup = 0;
                constexpr int64_t startupJobThreshold = 100;

                // Rate-limited aggregation to avoid log spam when cleaning large numbers of
                // orphans.
                //
                // Important: the consumer can briefly observe the channel as empty between producer
                // bursts. If we flush on every "idle" observation, we can still spam logs.
                // Instead we:
                // - log at most once per interval while busy
                // - flush only after we've been truly idle for a short hold window
                size_t pendingOrphansRemoved{0};
                size_t pendingOrphansSkipped{0};
                auto lastOrphanInfoLog = std::chrono::steady_clock::time_point{};
                auto idleSince = std::chrono::steady_clock::time_point{};
                constexpr auto kOrphanInfoLogInterval = 2s;
                constexpr auto kOrphanIdleFlushHold = 250ms;

                if (startupDelayMs > 0) {
                    if (!co_await coWaitForDelay(timer,
                                                 std::chrono::milliseconds(startupDelayMs))) {
                        co_return;
                    }
                }
                startupComplete = true;
                spdlog::debug("[Fts5Job] Startup phase complete, entering normal operation");

                while (!stopFlag->load(std::memory_order_acquire)) {
                    Bus::Fts5Job job;
                    if (channel && channel->try_pop(job)) {
                        idleSince = std::chrono::steady_clock::time_point{};
                        const auto [store, meta] =
                            std::tuple{self->getContentStore(), self->getMetadataRepo()};

                        if (!meta) {
                            spdlog::debug("[Fts5Job] Metadata not ready, dropping {} docs",
                                          job.hashes.size());
                        } else if (job.operation == Bus::Fts5Operation::ExtractAndIndex) {
                            auto postIngest = self->getPostIngestQueue();
                            if (!postIngest) {
                                spdlog::debug(
                                    "[Fts5Job] PostIngestQueue not ready, dropping {} docs",
                                    job.hashes.size());
                                continue;
                            }

                            size_t enqueued{0}, skipped{0};
                            for (const auto& h : job.hashes) {
                                std::string mime;
                                if (meta) {
                                    auto docRes = meta->getDocumentByHash(h);
                                    if (docRes && docRes.value().has_value()) {
                                        mime = docRes.value()->mimeType;
                                    }
                                }

                                if (postIngest->tryEnqueue(
                                        PostIngestQueue::Task{h, mime, -1, {}})) {
                                    ++enqueued;
                                } else {
                                    ++skipped;
                                }
                            }

                            if (enqueued > 0) {
                                spdlog::debug("[Fts5Job] Enqueued {} docs to PostIngestQueue ({} "
                                              "skipped/duplicate)",
                                              enqueued, skipped);
                            }
                        } else if (job.operation == Bus::Fts5Operation::RemoveOrphans) {
                            // Remove orphans directly by document rowid
                            size_t removed{0};
                            size_t failed{0};
                            for (int64_t docId : job.ids) {
                                if (auto removeRes = meta->removeFromIndex(docId); removeRes) {
                                    ++removed;
                                } else {
                                    ++failed;
                                }
                            }

                            if (removed > 0 || failed > 0) {
                                pendingOrphansRemoved += removed;
                                pendingOrphansSkipped += failed;
                                auto now = std::chrono::steady_clock::now();
                                if (lastOrphanInfoLog.time_since_epoch().count() == 0 ||
                                    (now - lastOrphanInfoLog) >= kOrphanInfoLogInterval) {
                                    spdlog::debug("[Fts5Job] Removed {} orphans ({} failed)",
                                                  pendingOrphansRemoved, pendingOrphansSkipped);
                                    pendingOrphansRemoved = 0;
                                    pendingOrphansSkipped = 0;
                                    lastOrphanInfoLog = now;
                                }
                                Bus::instance().incOrphansRemoved(removed);
                            }
                        }

                        Bus::instance().incFts5Consumed();

                        ++jobsProcessedDuringStartup;
                        uint32_t throttleMs =
                            (startupComplete && jobsProcessedDuringStartup > startupJobThreshold)
                                ? normalThrottleMs
                                : startupThrottleMs;

                        if (!co_await coWaitForDelay(timer,
                                                     std::chrono::milliseconds(throttleMs))) {
                            break;
                        }

                        continue;
                    }

                    // Flush pending orphan log once we've been truly idle for a moment.
                    // This avoids "idle thrash" log spam while still ensuring the final totals
                    // get emitted when a burst completes.
                    auto now = std::chrono::steady_clock::now();
                    if (idleSince.time_since_epoch().count() == 0) {
                        idleSince = now;
                    }
                    if (pendingOrphansRemoved > 0 && (now - idleSince) >= kOrphanIdleFlushHold) {
                        spdlog::debug("[Fts5Job] Removed {} orphans ({} skipped)",
                                      pendingOrphansRemoved, pendingOrphansSkipped);
                        pendingOrphansRemoved = 0;
                        pendingOrphansSkipped = 0;
                        lastOrphanInfoLog = now;
                    }

                    if (!co_await coWaitForDelay(timer,
                                                 std::chrono::milliseconds(idleThrottleMs()))) {
                        break;
                    }
                }
                spdlog::debug("[Fts5Job] Consumer stopped");
                co_return;
            },
            boost::asio::detached);
    } catch (...) {
        unregisterTrackedCoroutine();
        throw;
    }
}

void BackgroundTaskManager::launchOrphanScanTask() {
    auto self = deps_.serviceManager.lock();
    if (!self) {
        throw std::runtime_error(
            "BackgroundTaskManager: ServiceManager weak_ptr expired in launchOrphanScanTask");
    }

    auto exec = deps_.executor;
    auto stopFlag = stopRequested_;

    spdlog::debug("[BackgroundTaskManager] Launching OrphanScan task");
    registerTrackedCoroutine();
    try {
        boost::asio::co_spawn(
            exec,
            [this, self, stopFlag]() -> boost::asio::awaitable<void> {
                auto coroutineGuard =
                    std::shared_ptr<void>(nullptr, [this](void*) { unregisterTrackedCoroutine(); });
                using Bus = yams::daemon::InternalEventBus;
                using namespace std::chrono_literals;

                auto executor = co_await boost::asio::this_coro::executor;
                auto timer =
                    std::make_shared<boost::asio::steady_timer>(boost::asio::make_strand(executor));
                registerTrackedTimer(timer);
                if (stopFlag->load(std::memory_order_acquire)) {
                    co_return;
                }

                if (!co_await coWaitForDelay(timer, 5min)) {
                    co_return;
                }

                while (!stopFlag->load(std::memory_order_acquire)) {
                    if (auto meta = self->getMetadataRepo(); meta) {
                        spdlog::debug("[OrphanScan] Starting scan");

                        auto fts5IdsRes = meta->getAllFts5IndexedDocumentIds();
                        if (!fts5IdsRes) {
                            spdlog::warn("[OrphanScan] Query failed: {}",
                                         fts5IdsRes.error().message);
                        } else if (const auto& fts5Ids = fts5IdsRes.value(); !fts5Ids.empty()) {
                            yams::metadata::DocumentQueryOptions opts;
                            opts.limit = std::numeric_limits<int>::max();
                            auto allDocsRes = meta->queryDocuments(opts);

                            if (allDocsRes && !allDocsRes.value().empty()) {
                                std::unordered_set<int64_t> validDocIds;
                                std::unordered_map<int64_t, std::string> docIdToHash;
                                for (const auto& doc : allDocsRes.value()) {
                                    validDocIds.insert(doc.id);
                                    docIdToHash[doc.id] = doc.sha256Hash;
                                }

                                std::vector<int64_t> orphanIds;
                                for (int64_t fts5Id : fts5Ids) {
                                    if (!validDocIds.contains(fts5Id)) {
                                        orphanIds.push_back(fts5Id);
                                    }
                                }

                                if (orphanIds.empty()) {
                                    spdlog::debug("[OrphanScan] No orphans ({} entries checked)",
                                                  fts5Ids.size());
                                } else {
                                    spdlog::debug("[OrphanScan] Detected {} orphans",
                                                  orphanIds.size());
                                    Bus::instance().incOrphansDetected(orphanIds.size());

                                    auto fts5Q =
                                        Bus::instance().get_or_create_channel<Bus::Fts5Job>(
                                            "fts5_jobs", 512);
                                    constexpr size_t BATCH_SIZE = 50;

                                    for (size_t i = 0; i < orphanIds.size(); i += BATCH_SIZE) {
                                        size_t batchEnd =
                                            std::min(i + BATCH_SIZE, orphanIds.size());
                                        std::vector<int64_t> batch(orphanIds.begin() + i,
                                                                   orphanIds.begin() + batchEnd);

                                        Bus::Fts5Job orphanJob;
                                        orphanJob.ids = batch;
                                        orphanJob.batchSize = static_cast<uint32_t>(BATCH_SIZE);
                                        orphanJob.operation = Bus::Fts5Operation::RemoveOrphans;

                                        if (!fts5Q->try_push(orphanJob)) {
                                            spdlog::debug("[OrphanScan] Queue full, batch dropped");
                                        } else {
                                            TuningManager::notifyWakeup();
                                        }

                                        // Yield between batches to avoid flooding the queue
                                        if (!co_await coWaitForDelay(timer, 50ms)) {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    auto now = std::chrono::system_clock::now();
                    auto epochMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                       now.time_since_epoch())
                                       .count();
                    Bus::instance().setLastOrphanScanTime(static_cast<uint64_t>(epochMs));

                    auto intervalHours = TuneAdvisor::orphanScanIntervalHours();
                    if (!co_await coWaitForDelay(timer, std::chrono::hours(intervalHours))) {
                        break;
                    }
                }
                spdlog::debug("[OrphanScan] Task stopped");
                co_return;
            },
            boost::asio::detached);
    } catch (...) {
        unregisterTrackedCoroutine();
        throw;
    }
}

void BackgroundTaskManager::launchCheckpointTask() {
    auto self = deps_.serviceManager.lock();
    if (!self) {
        throw std::runtime_error(
            "BackgroundTaskManager: ServiceManager weak_ptr expired in launchCheckpointTask");
    }

    auto checkpointMgr = self->getCheckpointManager();
    if (!checkpointMgr) {
        spdlog::debug("[BackgroundTaskManager] CheckpointManager not available, skipping task");
        return;
    }

    checkpointMgr->start();
    spdlog::debug("[BackgroundTaskManager] CheckpointManager started");
}

void BackgroundTaskManager::launchStorageGcTask() {
    auto self = deps_.serviceManager.lock();
    if (!self) {
        throw std::runtime_error(
            "BackgroundTaskManager: ServiceManager weak_ptr expired in launchStorageGcTask");
    }

    auto exec = deps_.executor;
    auto stopFlag = stopRequested_;

    spdlog::debug("[BackgroundTaskManager] Launching StorageGC producer + consumer");

    // Producer: periodic timer that enqueues StorageGcJob to the bus
    registerTrackedCoroutine();
    try {
        boost::asio::co_spawn(
            exec,
            [this, stopFlag]() -> boost::asio::awaitable<void> {
                auto coroutineGuard =
                    std::shared_ptr<void>(nullptr, [this](void*) { unregisterTrackedCoroutine(); });
                using namespace std::chrono_literals;
                using Bus = yams::daemon::InternalEventBus;

                auto executor = co_await boost::asio::this_coro::executor;
                auto timer =
                    std::make_shared<boost::asio::steady_timer>(boost::asio::make_strand(executor));
                registerTrackedTimer(timer);
                if (stopFlag->load(std::memory_order_acquire)) {
                    co_return;
                }

                // Initial delay: 10 minutes after daemon startup
                if (!co_await coWaitForDelay(timer, 10min)) {
                    co_return;
                }

                // Adaptive interval: start at 1h, double on no-op (max 4h)
                auto interval = 1h;
                constexpr auto kMaxInterval = std::chrono::hours(4);

                constexpr std::size_t kChannelCapacity = 4;
                auto channel = Bus::instance().get_or_create_channel<Bus::StorageGcJob>(
                    "storage_gc_jobs", kChannelCapacity);

                while (!stopFlag->load(std::memory_order_acquire)) {
                    Bus::StorageGcJob job{};
                    if (channel->try_push(job)) {
                        Bus::instance().incGcQueued();
                        TuningManager::notifyWakeup();
                        spdlog::debug("[StorageGC] Enqueued GC job");
                    } else {
                        Bus::instance().incGcDropped();
                        spdlog::debug("[StorageGC] Channel full, skipping cycle");
                    }

                    if (!co_await coWaitForDelay(timer, interval)) {
                        break;
                    }

                    // Double interval on idle, cap at kMaxInterval
                    if (interval < kMaxInterval) {
                        interval = std::min(interval * 2, kMaxInterval);
                    }
                }
                spdlog::debug("[StorageGC] Producer stopped");
                co_return;
            },
            boost::asio::detached);
    } catch (...) {
        unregisterTrackedCoroutine();
        throw;
    }

    // Consumer: polls StorageGcJob channel and runs garbageCollect()
    registerTrackedCoroutine();
    try {
        boost::asio::co_spawn(
            exec,
            [this, self, stopFlag]() -> boost::asio::awaitable<void> {
                auto coroutineGuard =
                    std::shared_ptr<void>(nullptr, [this](void*) { unregisterTrackedCoroutine(); });
                using namespace std::chrono_literals;
                using Bus = yams::daemon::InternalEventBus;

                auto executor = co_await boost::asio::this_coro::executor;
                auto timer =
                    std::make_shared<boost::asio::steady_timer>(boost::asio::make_strand(executor));
                registerTrackedTimer(timer);
                if (stopFlag->load(std::memory_order_acquire)) {
                    co_return;
                }

                constexpr std::size_t kChannelCapacity = 4;
                auto channel = Bus::instance().get_or_create_channel<Bus::StorageGcJob>(
                    "storage_gc_jobs", kChannelCapacity);

                constexpr auto kMinIdleDelay = std::chrono::milliseconds(100);
                constexpr auto kMaxIdleDelay = std::chrono::milliseconds(5000);
                auto idleDelay = kMinIdleDelay;

                while (!stopFlag->load(std::memory_order_acquire)) {
                    Bus::StorageGcJob job;
                    if (channel->try_pop(job)) {
                        idleDelay = kMinIdleDelay; // reset backoff on work

                        auto store = self->getContentStore();
                        if (!store) {
                            spdlog::debug("[StorageGC] ContentStore not ready, dropping job");
                            continue;
                        }

                        spdlog::debug("[StorageGC] Starting garbage collection");
                        try {
                            auto result = store->garbageCollect(nullptr);
                            if (!result) {
                                spdlog::warn("[StorageGC] Failed: {}", result.error().message);
                            } else {
                                Bus::instance().incGcConsumed();
                                spdlog::debug("[StorageGC] Collection completed successfully");
                            }
                        } catch (const std::exception& e) {
                            spdlog::warn("[StorageGC] Exception: {}", e.what());
                        }
                        continue;
                    }

                    // Idle: adaptive backoff
                    if (!co_await coWaitForDelay(timer, idleDelay)) {
                        break;
                    }
                    if (idleDelay < kMaxIdleDelay) {
                        idleDelay = std::min(idleDelay * 2, kMaxIdleDelay);
                    }
                }
                spdlog::debug("[StorageGC] Consumer stopped");
                co_return;
            },
            boost::asio::detached);
    } catch (...) {
        unregisterTrackedCoroutine();
        throw;
    }
}

void BackgroundTaskManager::launchGraphPruneTask() {
    auto self = deps_.serviceManager.lock();
    if (!self) {
        throw std::runtime_error(
            "BackgroundTaskManager: ServiceManager weak_ptr expired in launchGraphPruneTask");
    }

    const auto& cfg = self->getConfig().graphPrune;
    if (!cfg.enabled) {
        spdlog::debug("[BackgroundTaskManager] Graph prune disabled, skipping task");
        return;
    }

    auto exec = deps_.executor;
    auto stopFlag = stopRequested_;
    const auto interval = cfg.interval;
    const auto initialDelay = cfg.initialDelay;
    const auto keepLatest = cfg.keepLatestPerCanonical;

    spdlog::debug("[BackgroundTaskManager] Launching GraphPrune task");
    registerTrackedCoroutine();
    try {
        boost::asio::co_spawn(
            exec,
            [this, self, stopFlag, interval, initialDelay,
             keepLatest]() -> boost::asio::awaitable<void> {
                auto coroutineGuard =
                    std::shared_ptr<void>(nullptr, [this](void*) { unregisterTrackedCoroutine(); });
                using namespace std::chrono_literals;
                auto executor = co_await boost::asio::this_coro::executor;
                auto timer =
                    std::make_shared<boost::asio::steady_timer>(boost::asio::make_strand(executor));
                registerTrackedTimer(timer);
                if (stopFlag->load(std::memory_order_acquire)) {
                    co_return;
                }

                if (initialDelay.count() > 0 && !co_await coWaitForDelay(timer, initialDelay)) {
                    co_return;
                }

                while (!stopFlag->load(std::memory_order_acquire)) {
                    auto kg = self->getKgStore();
                    if (kg) {
                        yams::metadata::GraphVersionPruneConfig pruneCfg;
                        pruneCfg.keepLatestPerCanonical = keepLatest;
                        auto res = kg->pruneVersionNodes(pruneCfg);
                        if (!res) {
                            spdlog::warn("[GraphPrune] prune failed: {}", res.error().message);
                        } else if (res.value() > 0) {
                            spdlog::debug("[GraphPrune] pruned {} version nodes", res.value());
                        }
                    }

                    if (!co_await coWaitForDelay(timer, interval)) {
                        break;
                    }
                }

                co_return;
            },
            boost::asio::detached);
    } catch (...) {
        unregisterTrackedCoroutine();
        throw;
    }
}

void BackgroundTaskManager::launchAutoRepairTask() {
    auto self = deps_.serviceManager.lock();
    if (!self) {
        throw std::runtime_error(
            "BackgroundTaskManager: ServiceManager weak_ptr expired in launchAutoRepairTask");
    }

    if (!self->getConfig().enableAutoRepair) {
        spdlog::debug("[BackgroundTaskManager] Auto-repair disabled, skipping task");
        return;
    }

    const auto initialDelay = std::chrono::minutes(TuneAdvisor::repairAutoInitialDelayMinutes());
    const auto fastInterval = std::chrono::minutes(TuneAdvisor::repairAutoFastMinutes());
    const auto warmInterval = std::chrono::hours(TuneAdvisor::repairAutoWarmHours());
    const auto coldInterval = std::chrono::hours(TuneAdvisor::repairAutoColdHours());
    const auto retryDelay = std::chrono::minutes(2);

    if (fastInterval.count() == 0 && warmInterval.count() == 0 && coldInterval.count() == 0) {
        spdlog::debug("[BackgroundTaskManager] Auto-repair intervals disabled, skipping task");
        return;
    }

    auto exec = deps_.executor;
    auto stopFlag = stopRequested_;

    spdlog::debug("[BackgroundTaskManager] Launching AutoRepair task");
    registerTrackedCoroutine();
    try {
        boost::asio::co_spawn(
            exec,
            [this, self, stopFlag, initialDelay, fastInterval, warmInterval, coldInterval,
             retryDelay]() -> boost::asio::awaitable<void> {
                auto coroutineGuard =
                    std::shared_ptr<void>(nullptr, [this](void*) { unregisterTrackedCoroutine(); });
                using namespace std::chrono_literals;
                auto executor = co_await boost::asio::this_coro::executor;
                auto timer =
                    std::make_shared<boost::asio::steady_timer>(boost::asio::make_strand(executor));
                registerTrackedTimer(timer);
                if (stopFlag->load(std::memory_order_acquire)) {
                    co_return;
                }

                if (initialDelay.count() > 0 && !co_await coWaitForDelay(timer, initialDelay)) {
                    co_return;
                }

                // One-shot salvage sweep-up: when the daemon recovered from a
                // corrupt metadata DB and salvaged document records, rebuild
                // derived indexes (FTS5, graph, embeddings) from the recovered rows.
                // Runs once after initial delay; subsequent tiered ticks handle
                // any remaining repair work.
                {
                    std::string recoveredFrom;
                    {
                        std::lock_guard<std::mutex> lk(self->getState().readiness.recoveryMutex);
                        recoveredFrom = self->getState().readiness.databaseRecoveredFrom;
                    }
                    if (!recoveredFrom.empty() && recoveredFrom.rfind("salvaged-", 0) == 0) {
                        spdlog::info("[AutoRepair] Detected salvage recovery ({}), "
                                     "scheduling derived-index rebuild",
                                     recoveredFrom);
                        auto rs = self->getRepairServiceShared();
                        if (rs && !rs->isRepairInProgress()) {
                            RepairRequest salvageRebuild;
                            salvageRebuild.repairFts5 = true;
                            salvageRebuild.repairGraph = true;
                            salvageRebuild.repairTopology = true;
                            salvageRebuild.repairEmbeddings = true;
                            salvageRebuild.repairPathTree = true;
                            salvageRebuild.foreground = false;
                            if (repair::RepairPlanBuilder::hasWork(salvageRebuild)) {
                                RepairResponse resp = co_await rs->executeRepairAsync(
                                    salvageRebuild, nullptr, stopFlag.get());
                                if (resp.success) {
                                    spdlog::info("[AutoRepair] Salvage sweep-up completed");
                                } else {
                                    spdlog::warn("[AutoRepair] Salvage sweep-up had errors");
                                }
                            }
                        }
                    }
                }

                const auto maxTime = std::chrono::steady_clock::time_point::max();
                auto now = std::chrono::steady_clock::now();
                auto nextFast = (fastInterval.count() > 0) ? now + fastInterval : maxTime;
                auto nextWarm = (warmInterval.count() > 0) ? now + warmInterval : maxTime;
                auto nextCold = (coldInterval.count() > 0) ? now + coldInterval : maxTime;

                while (!stopFlag->load(std::memory_order_acquire)) {
                    now = std::chrono::steady_clock::now();

                    auto rs = self->getRepairServiceShared();
                    if (!rs) {
                        if (!co_await coWaitForDelay(timer, 1min)) {
                            break;
                        }
                        continue;
                    }

                    const auto& state = self->getState();
                    const bool queueEmpty =
                        state.stats.repairQueueDepth.load(std::memory_order_relaxed) == 0;
                    const bool busy =
                        state.stats.activeConnections.load(std::memory_order_relaxed) > 0;

                    auto canRunAuto = [&]() {
                        if (busy)
                            return false;
                        if (!queueEmpty)
                            return false;
                        if (rs->isRepairInProgress())
                            return false;
                        return true;
                    };

                    auto runPlan = [&](const std::string& tier,
                                       const RepairRequest& req) -> boost::asio::awaitable<void> {
                        if (!repair::RepairPlanBuilder::hasWork(req)) {
                            co_return;
                        }
                        spdlog::info("[AutoRepair] tick {}: executing", tier);
                        RepairResponse resp =
                            !req.foreground
                                ? co_await rs->executeRepairAsync(req, nullptr, stopFlag.get())
                                : rs->executeRepair(req, nullptr, stopFlag.get());
                        if (!resp.success && !resp.errors.empty()) {
                            spdlog::warn("[AutoRepair] {} completed with errors: {}", tier,
                                         resp.errors.front());
                        }
                        co_return;
                    };

                    if (fastInterval.count() > 0 && now >= nextFast) {
                        if (canRunAuto()) {
                            repair::RepairHealthProbe probe(
                                self->getMetadataRepo(), self->getVectorDatabase(),
                                self->getGraphComponent(), self->getKgStore());
                            repair::RepairHealthOptions opts;
                            opts.checkFts5 = false;
                            opts.checkEmbeddings = false;
                            opts.checkGraph = true;
                            opts.scanDocuments = false;
                            auto health = probe.probe(opts);
                            auto req = repair::RepairPlanBuilder::buildFast(health);
                            co_await runPlan("fast", req);
                            nextFast = now + fastInterval;
                        } else {
                            nextFast = now + retryDelay;
                        }
                    }

                    if (warmInterval.count() > 0 && now >= nextWarm) {
                        if (canRunAuto()) {
                            repair::RepairHealthProbe probe(
                                self->getMetadataRepo(), self->getVectorDatabase(),
                                self->getGraphComponent(), self->getKgStore());
                            repair::RepairHealthOptions opts;
                            auto health = probe.probe(opts);
                            spdlog::info(
                                "[AutoRepair] health: docs={} fts5_missing={} embed_missing={} "
                                "graph_gap={} graph_ok={}",
                                health.documentsScanned, health.missingFts5,
                                health.missingEmbeddings, health.graphDocNodeGap,
                                health.graphIntegrityOk);
                            for (const auto& issue : health.issues) {
                                spdlog::debug("[AutoRepair] health issue: {}", issue);
                            }
                            auto req = repair::RepairPlanBuilder::buildWarm(health, 3);
                            co_await runPlan("warm", req);
                            nextWarm = now + warmInterval;
                        } else {
                            nextWarm = now + retryDelay;
                        }
                    }

                    if (coldInterval.count() > 0 && now >= nextCold) {
                        if (canRunAuto()) {
                            auto req = repair::RepairPlanBuilder::buildCold();
                            co_await runPlan("cold", req);
                            nextCold = now + coldInterval;
                        } else {
                            nextCold = now + retryDelay;
                        }
                    }

                    auto nextWake = std::min(nextFast, std::min(nextWarm, nextCold));
                    auto wait =
                        nextWake == maxTime ? 5min : ((nextWake > now) ? (nextWake - now) : 1s);
                    if (wait < 1s) {
                        wait = 1s;
                    }
                    if (!co_await coWaitForDelay(timer, wait)) {
                        break;
                    }
                }
                co_return;
            },
            boost::asio::detached);
    } catch (...) {
        unregisterTrackedCoroutine();
        throw;
    }
}

} // namespace yams::daemon
