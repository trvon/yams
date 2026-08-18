#include <yams/daemon/components/VectorIndexCoordinator.h>

#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_database.h>

#include <spdlog/spdlog.h>
#include <yams/profiling.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <algorithm>
#include <chrono>
#include <future>

namespace yams::daemon {
namespace {

uint64_t steadyNowNs() noexcept {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                     std::chrono::steady_clock::now().time_since_epoch())
                                     .count());
}

uint64_t elapsedMs(uint64_t startedAtNs, uint64_t nowNs) noexcept {
    return startedAtNs == 0 || nowNs < startedAtNs ? 0 : (nowNs - startedAtNs) / 1'000'000;
}

} // namespace

// ── BulkScope ───────────────────────────────────────────────────────────────

VectorIndexCoordinator::BulkScope::BulkScope(BulkScope&& other) noexcept : owner_(other.owner_) {
    other.owner_ = nullptr;
}

VectorIndexCoordinator::BulkScope&
VectorIndexCoordinator::BulkScope::operator=(BulkScope&& other) noexcept {
    if (this != &other) {
        if (owner_) {
            owner_->releaseBulkScope();
        }
        owner_ = other.owner_;
        other.owner_ = nullptr;
    }
    return *this;
}

VectorIndexCoordinator::BulkScope::~BulkScope() {
    if (owner_) {
        owner_->releaseBulkScope();
    }
}

// ── VectorIndexCoordinator ──────────────────────────────────────────────────

VectorIndexCoordinator::VectorIndexCoordinator(boost::asio::any_io_executor exec,
                                               std::shared_ptr<vector::VectorDatabase> vdb,
                                               StateComponent* state)
    : strand_(boost::asio::make_strand(exec)),
      callbackLifetime_(std::make_shared<CallbackLifetime>()), vectorDb_(std::move(vdb)),
      state_(state) {
    callbackLifetime_->owner = this;
}

VectorIndexCoordinator::~VectorIndexCoordinator() {
    std::lock_guard<std::mutex> lock(callbackLifetime_->mutex);
    callbackLifetime_->owner = nullptr;
}

// Synchronous bridge used by non-Asio workers such as memory-sync apply.
Result<void> VectorIndexCoordinator::requestRebuildBlocking(RebuildReason reason) {
    if (strand_.running_in_this_thread()) {
        return Error{ErrorCode::InvalidState,
                     "blocking vector rebuild cannot run on the coordinator strand"};
    }
    try {
        auto future =
            boost::asio::co_spawn(strand_, requestRebuild(reason), boost::asio::use_future);
        return future.get();
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    }
}

void VectorIndexCoordinator::fireRebuild(RebuildReason reason) noexcept {
    // Delegate to requestRebuild() on the strand, detached.  This gives the
    // caller the full coalescing + waiter-registration semantics without
    // imposing an awaitable return, and it fixes the "dropped reason" bug that
    // the old postRebuild() had (it skipped waiter registration, so a rebuild
    // end-of-loop could drop the pending bit).
    try {
        boost::asio::co_spawn(
            strand_, requestRebuild(reason), [](std::exception_ptr e, Result<void> r) {
                if (e) {
                    try {
                        std::rethrow_exception(e);
                    } catch (const std::exception& ex) {
                        spdlog::warn("[VectorIndexCoordinator] fireRebuild failed: {}", ex.what());
                    } catch (...) {
                        spdlog::warn("[VectorIndexCoordinator] fireRebuild failed (unknown)");
                    }
                } else if (!r) {
                    spdlog::debug("[VectorIndexCoordinator] fireRebuild: rebuild returned error");
                }
            });
    } catch (const std::exception& ex) {
        spdlog::warn("[VectorIndexCoordinator] fireRebuild spawn failed: {}", ex.what());
    } catch (...) {
        spdlog::warn("[VectorIndexCoordinator] fireRebuild spawn failed (unknown)");
    }
}

// Helper: get the VDB under the mutex for callers on any thread.
static std::shared_ptr<vector::VectorDatabase>
getVdbLocked(std::mutex& mu, const std::shared_ptr<vector::VectorDatabase>& vdb) {
    std::lock_guard<std::mutex> lk(mu);
    return vdb;
}

static void
warnIfRebuildExceedsBudget(const std::shared_ptr<vector::VectorDatabase>& vdb) noexcept {
    if (!vdb) {
        return;
    }
    const std::uint64_t budget = ResourceGovernor::instance().recommendVectorRebuildBudgetBytes();
    if (budget == 0) {
        return;
    }
    std::size_t count = 0;
    std::size_t dim = 0;
    try {
        count = vdb->getVectorCount();
        dim = vdb->getEmbeddingDim();
    } catch (...) {
        return;
    }
    if (count == 0 || dim == 0) {
        return;
    }
    const std::uint64_t rawBytes =
        static_cast<std::uint64_t>(count) * static_cast<std::uint64_t>(dim) * sizeof(float);
    const std::uint64_t predictedBytes = rawBytes + (rawBytes / 2);
    if (predictedBytes > budget) {
        spdlog::warn("[VectorIndexCoordinator] SPQ rebuild may exceed memory budget "
                     "(predicted={}MB budget={}MB count={} dim={})",
                     predictedBytes >> 20, budget >> 20, count, dim);
    }
}

void VectorIndexCoordinator::postTelemetryRefresh() noexcept {
    boost::asio::post(strand_, [this]() {
        VectorIndexTelemetry t = snapshot();
        t.activeBulkScopes = activeBulkScopes_.load(std::memory_order_relaxed);
        t.pendingReasons = pendingReasons_.load(std::memory_order_relaxed);
        t.rebuildEpoch = rebuildEpoch_.load(std::memory_order_relaxed);
        publishTelemetry(t);
    });
}

VectorIndexCoordinator::BulkScope VectorIndexCoordinator::beginBulkIngest(RebuildReason reason) {
    YAMS_ZONE_SCOPED_N("VecIdxCoord::beginBulkIngest");
    pendingReasons_.fetch_or(static_cast<uint32_t>(reason), std::memory_order_relaxed);

    // fetch_add is atomic: exactly one caller sees prev == 0 and enters bulk mode.
    const uint32_t prev = activeBulkScopes_.fetch_add(1, std::memory_order_acq_rel);
    if (prev == 0) {
        // Serialise the backend begin-bulk through the strand so that no other
        // strand-owned operation (buildIndex/persist/finalize) runs concurrently
        // with this beginBulkLoad call on the backend.  The caller does NOT need
        // to wait for the strand to acknowledge — subsequent insertVectorsBatch
        // calls go through VDB's own mutex, so ordering with beginBulkLoad is
        // enforced by VDB's internal serialisation regardless of how soon the
        // strand runs our lambda.
        boost::asio::post(strand_, [this]() {
            auto vdb = getVdbLocked(vdbMutex_, vectorDb_);
            if (vdb) {
                if (!vdb->beginBulkLoad()) {
                    spdlog::warn("[VectorIndexCoordinator] beginBulkLoad failed: {}",
                                 vdb->getLastError());
                }
            }
            VectorIndexTelemetry t = snapshot();
            t.activeBulkScopes = activeBulkScopes_.load(std::memory_order_relaxed);
            t.pendingReasons = pendingReasons_.load(std::memory_order_relaxed);
            publishTelemetry(t);
        });
    } else {
        postTelemetryRefresh();
    }

    return BulkScope(this);
}

void VectorIndexCoordinator::releaseBulkScope() noexcept {
    const uint32_t prev = activeBulkScopes_.fetch_sub(1, std::memory_order_acq_rel);
    if (prev == 1) {
        // Last scope: post finalize+persist to strand.
        boost::asio::post(strand_, [this]() { doFinalizeOnStrand(); });
    } else {
        postTelemetryRefresh();
    }
}

void VectorIndexCoordinator::doFinalizeOnStrand() {
    YAMS_ZONE_SCOPED_N("VecIdxCoord::doFinalizeOnStrand");
    // Running on the strand.
    auto vdb = getVdbLocked(vdbMutex_, vectorDb_);
    if (vdb) {
        if (!vdb->finalizeBulkLoad()) {
            spdlog::warn("[VectorIndexCoordinator] finalizeBulkLoad failed: {}",
                         vdb->getLastError());
        }
        if (buildsSuppressed_.load(std::memory_order_relaxed)) {
            spdlog::warn("[VectorIndexCoordinator] index build/persist suppressed by memory "
                         "instrumentation profile");
        } else {
            try {
                warnIfRebuildExceedsBudget(vdb);
                vdb->buildIndex();
                if (!vdb->persistIndex()) {
                    spdlog::warn("[VectorIndexCoordinator] persistIndex returned failure: {}",
                                 vdb->getLastError());
                }
            } catch (const std::exception& e) {
                spdlog::warn("[VectorIndexCoordinator] index build/persist failed: {}", e.what());
            } catch (...) {
                spdlog::warn("[VectorIndexCoordinator] index build/persist failed (unknown)");
            }
        }
    }

    const uint64_t epoch = rebuildEpoch_.fetch_add(1, std::memory_order_acq_rel) + 1;

    VectorIndexTelemetry t{};
    t.rebuildEpoch = epoch;
    t.activeBulkScopes = 0;
    t.rebuilding = false;
    t.ready = true;
    t.progressPct = 100;
    t.pendingReasons = 0;
    publishTelemetry(t);

    if (state_) {
        state_->readiness.vectorIndexReady.store(true, std::memory_order_relaxed);
        state_->readiness.vectorIndexProgress.store(100, std::memory_order_relaxed);
    }

    notifyWaiters(epoch, Result<void>{});
}

boost::asio::awaitable<Result<void>> VectorIndexCoordinator::requestRebuild(RebuildReason reason) {
    YAMS_ZONE_SCOPED_N("VecIdxCoord::requestRebuild");
    // Register the completion handler while holding the same mutex used by epoch completion.
    // The rebuild cannot publish an epoch result before this caller has a waiter for it.
    co_return co_await boost::asio::async_initiate<const boost::asio::use_awaitable_t<>&,
                                                   void(Result<void>)>(
        [this, reason](auto&& handler) mutable {
            pendingReasons_.fetch_or(static_cast<uint32_t>(reason), std::memory_order_acq_rel);

            auto h = std::make_shared<std::decay_t<decltype(handler)>>(
                std::forward<decltype(handler)>(handler));
            bool startRebuild = false;
            uint32_t reasons = 0;
            {
                std::lock_guard<std::mutex> lk(waitersMutex_);
                const uint64_t targetEpoch = rebuildEpoch_.load(std::memory_order_acquire) + 1;
                waiters_.push_back(
                    {targetEpoch, [h](Result<void> r) mutable { (*h)(std::move(r)); }});

                bool expected = false;
                startRebuild = rebuildInFlight_.compare_exchange_strong(expected, true,
                                                                        std::memory_order_acq_rel);
                if (startRebuild) {
                    reasons = pendingReasons_.exchange(0, std::memory_order_acq_rel);
                }
            }

            if (startRebuild) {
                auto t = snapshot();
                t.rebuilding = true;
                t.ready = false;
                t.progressPct = 0;
                t.pendingReasons = reasons;
                publishTelemetry(t);

                if (state_) {
                    state_->readiness.vectorIndexReady.store(false, std::memory_order_relaxed);
                    state_->readiness.vectorIndexProgress.store(0, std::memory_order_relaxed);
                }

                boost::asio::post(strand_, [this, reasons]() { doRebuildOnStrand(reasons); });
            }

            if (afterRebuildAdmissionForTesting_) {
                afterRebuildAdmissionForTesting_();
            }
        },
        boost::asio::use_awaitable);
}

void VectorIndexCoordinator::doRebuildOnStrand(uint32_t /*reasons*/) {
    YAMS_ZONE_SCOPED_N("VecIdxCoord::doRebuildOnStrand");
    // Called via post(strand_, ...) — always deferred, never inline.
    // post() guarantees this runs AFTER all currently-queued strand items, so
    // every concurrent requestRebuild caller has already stored its waiter before
    // we bump the epoch (fixing the coalescing invariant).
    Result<void> rebuildResult;
    {
        auto vdb = getVdbLocked(vdbMutex_, vectorDb_);
        if (!vdb) {
            rebuildResult = Error{ErrorCode::InvalidState, "vector database is unavailable"};
        } else if (buildsSuppressed_.load(std::memory_order_relaxed)) {
            rebuildResult = Error{ErrorCode::InvalidState, "vector index rebuild is suppressed"};
        } else {
            try {
                warnIfRebuildExceedsBudget(vdb);
                if (auto built = vdb->buildIndexChecked(); !built) {
                    rebuildResult = built.error();
                } else if (auto persisted = vdb->persistIndexChecked(); !persisted) {
                    rebuildResult = persisted.error();
                }
            } catch (const std::exception& e) {
                rebuildResult = Error{ErrorCode::InternalError, e.what()};
            } catch (...) {
                rebuildResult =
                    Error{ErrorCode::InternalError, "vector index rebuild threw unknown error"};
            }
        }
    }

    const uint64_t epoch = rebuildEpoch_.fetch_add(1, std::memory_order_acq_rel) + 1;

    // Drain any reasons that arrived during rebuild — but only start a follow-up
    // rebuild if there are active waiters expecting a future epoch.  Without this
    // guard, concurrent same-reason callers cause spurious extra rebuilds.
    notifyWaiters(epoch, rebuildResult);
    bool hasNewWaiters = false;
    {
        std::lock_guard<std::mutex> lk(waitersMutex_);
        hasNewWaiters = std::any_of(waiters_.begin(), waiters_.end(),
                                    [epoch](const Waiter& w) { return w.targetEpoch > epoch; });
    }
    const uint32_t nextReasons = pendingReasons_.exchange(0, std::memory_order_acq_rel);
    const bool startNext = (nextReasons != 0 && hasNewWaiters);
    rebuildInFlight_.store(startNext, std::memory_order_release);

    const bool rebuildSucceeded = rebuildResult.has_value();
    VectorIndexTelemetry t{};
    t.rebuildEpoch = epoch;
    t.rebuilding = startNext;
    t.ready = rebuildSucceeded && !startNext;
    t.progressPct = rebuildSucceeded && !startNext ? 100u : 0u;
    t.activeBulkScopes = activeBulkScopes_.load(std::memory_order_relaxed);
    t.pendingReasons = startNext ? nextReasons : 0u;
    publishTelemetry(t);

    if (state_) {
        state_->readiness.vectorIndexReady.store(rebuildSucceeded && !startNext,
                                                 std::memory_order_relaxed);
        state_->readiness.vectorIndexProgress.store(rebuildSucceeded && !startNext ? 100u : 0u,
                                                    std::memory_order_relaxed);
    }

    if (startNext) {
        // Use post() (not co_spawn/dispatch) to keep the deferred semantics.
        boost::asio::post(strand_, [this, nextReasons]() { doRebuildOnStrand(nextReasons); });
    }
}

boost::asio::awaitable<Result<void>> VectorIndexCoordinator::initialBuildIfNeeded() {
    YAMS_ZONE_SCOPED_N("VecIdxCoord::initialBuildIfNeeded");
    co_await boost::asio::dispatch(strand_, boost::asio::use_awaitable);

    if (buildsSuppressed_.load(std::memory_order_relaxed)) {
        spdlog::warn("[VectorIndexCoordinator] initial build/load suppressed by memory "
                     "instrumentation profile");
        co_return Result<void>{};
    }

    auto vdb = getVdbLocked(vdbMutex_, vectorDb_);
    if (!vdb || !vdb->isInitialized()) {
        co_return Result<void>{};
    }

    try {
        const auto rows = vdb->getVectorCount();
        if (rows > 0) {
            if (vdb->prepareSearchIndex()) {
                if (state_) {
                    state_->readiness.vectorIndexReady.store(true, std::memory_order_relaxed);
                    state_->readiness.vectorIndexProgress.store(100, std::memory_order_relaxed);
                }
                const uint64_t epoch = rebuildEpoch_.load(std::memory_order_relaxed);
                VectorIndexTelemetry t{};
                t.rebuildEpoch = epoch;
                t.ready = true;
                t.progressPct = 100;
                t.activeBulkScopes = activeBulkScopes_.load(std::memory_order_relaxed);
                publishTelemetry(t);
            } else {
                spdlog::warn(
                    "[VectorIndexCoordinator] prepareSearchIndex failed: {} - falling back "
                    "to rebuild",
                    vdb->getLastError());
                co_return co_await requestRebuild(RebuildReason::InitialBuild);
            }
        }
    } catch (const std::exception& ex) {
        spdlog::warn("[VectorIndexCoordinator] initialBuildIfNeeded failed: {}", ex.what());
    } catch (...) {
        spdlog::warn("[VectorIndexCoordinator] initialBuildIfNeeded failed (unknown)");
    }

    co_return Result<void>{};
}

bool VectorIndexCoordinator::requestCheckpoint() noexcept {
    // Keep one shared persistence attempt. A timed-out caller must not enqueue another callback
    // behind a stalled strand, or periodic checkpoints amplify one executor stall indefinitely.
    checkpointRequests_.fetch_add(1, std::memory_order_relaxed);

    std::shared_ptr<CheckpointAttempt> attempt;
    bool shouldPost = false;
    try {
        std::lock_guard<std::mutex> lock(checkpointMutex_);
        if (checkpointAttempt_) {
            attempt = checkpointAttempt_;
            checkpointCoalesced_.fetch_add(1, std::memory_order_relaxed);
        } else {
            auto completion = std::make_shared<std::promise<bool>>();
            const uint64_t generation =
                checkpointGeneration_.fetch_add(1, std::memory_order_relaxed) + 1;
            attempt = std::make_shared<CheckpointAttempt>(
                CheckpointAttempt{generation, completion, completion->get_future().share()});
            checkpointAttempt_ = attempt;
            checkpointQueuedAtNs_.store(steadyNowNs(), std::memory_order_relaxed);
            checkpointRunningAtNs_.store(0, std::memory_order_relaxed);
            checkpointPhase_.store(VectorCheckpointPhase::Queued, std::memory_order_release);
            shouldPost = true;
        }
    } catch (const std::exception& ex) {
        spdlog::warn("[VectorIndexCoordinator] requestCheckpoint admission failed: {}", ex.what());
        return false;
    } catch (...) {
        return false;
    }

    if (shouldPost) {
        try {
            auto lifetime = callbackLifetime_;
            boost::asio::post(strand_, [lifetime = std::move(lifetime), attempt]() mutable {
                // A queued callback may execute after a timed-out caller and coordinator teardown.
                // Copy backend ownership while the owner gate is held, then release the gate before
                // persistence so teardown is never blocked by a slow backend call.
                auto completeWithoutOwner = [&attempt]() noexcept {
                    try {
                        attempt->completion->set_value(false);
                    } catch (...) {
                        const auto ignored = std::current_exception();
                        (void)ignored;
                    }
                };

                std::shared_ptr<vector::VectorDatabase> vdb;
                {
                    std::lock_guard<std::mutex> lifetimeLock(lifetime->mutex);
                    auto* owner = lifetime->owner;
                    if (!owner) {
                        completeWithoutOwner();
                        return;
                    }
                    owner->checkpointStarted_.fetch_add(1, std::memory_order_relaxed);
                    owner->checkpointRunningAtNs_.store(steadyNowNs(), std::memory_order_relaxed);
                    owner->checkpointPhase_.store(VectorCheckpointPhase::Running,
                                                  std::memory_order_release);
                    vdb = getVdbLocked(owner->vdbMutex_, owner->vectorDb_);
                }

                bool persisted = false;
                if (vdb && vdb->isInitialized()) {
                    try {
                        persisted = vdb->persistIndex();
                    } catch (const std::exception& ex) {
                        spdlog::warn(
                            "[VectorIndexCoordinator] requestCheckpoint persistIndex threw: {}",
                            ex.what());
                    } catch (...) {
                        spdlog::warn(
                            "[VectorIndexCoordinator] requestCheckpoint persistIndex threw "
                            "(unknown)");
                    }
                }

                std::lock_guard<std::mutex> lifetimeLock(lifetime->mutex);
                if (auto* owner = lifetime->owner) {
                    owner->completeCheckpointAttempt(attempt, persisted, true);
                } else {
                    completeWithoutOwner();
                }
            });
        } catch (const std::exception& ex) {
            spdlog::warn("[VectorIndexCoordinator] requestCheckpoint post failed: {}", ex.what());
            completeCheckpointAttempt(attempt, false, false);
        } catch (...) {
            completeCheckpointAttempt(attempt, false, false);
        }
    }

    if (attempt->result.wait_for(checkpointWaitTimeout_) != std::future_status::ready) {
        checkpointTimedOut_.fetch_add(1, std::memory_order_relaxed);
        spdlog::warn(
            "[VectorIndexCoordinator] requestCheckpoint timed out after {}ms; shared persistence "
            "attempt remains queued or running",
            checkpointWaitTimeout_.count());
        return false;
    }
    try {
        return attempt->result.get();
    } catch (const std::exception& ex) {
        spdlog::warn("[VectorIndexCoordinator] requestCheckpoint completion failed: {}", ex.what());
    } catch (...) {
        spdlog::warn("[VectorIndexCoordinator] requestCheckpoint completion failed (unknown)");
    }
    return false;
}

void VectorIndexCoordinator::completeCheckpointAttempt(
    const std::shared_ptr<CheckpointAttempt>& attempt, bool result, bool callbackStarted) noexcept {
    if (callbackStarted) {
        checkpointCompleted_.fetch_add(1, std::memory_order_relaxed);
    } else {
        checkpointPostFailures_.fetch_add(1, std::memory_order_relaxed);
    }

    {
        std::lock_guard<std::mutex> lock(checkpointMutex_);
        if (checkpointAttempt_ == attempt) {
            checkpointAttempt_.reset();
            checkpointQueuedAtNs_.store(0, std::memory_order_relaxed);
            checkpointRunningAtNs_.store(0, std::memory_order_relaxed);
            checkpointPhase_.store(VectorCheckpointPhase::Idle, std::memory_order_release);
        }
    }

    try {
        attempt->completion->set_value(result);
    } catch (...) {
        // A double completion would indicate a lifecycle bug; preserve noexcept at this boundary.
        const auto ignored = std::current_exception();
        (void)ignored;
    }
}

VectorIndexTelemetry VectorIndexCoordinator::snapshot() const noexcept {
    std::lock_guard<std::mutex> lock(telemetryMutex_);
    return telemetry_;
}

VectorCheckpointTelemetry VectorIndexCoordinator::checkpointSnapshot() const noexcept {
    VectorCheckpointTelemetry telemetry;
    telemetry.phase = checkpointPhase_.load(std::memory_order_acquire);
    telemetry.generation = checkpointGeneration_.load(std::memory_order_relaxed);
    telemetry.requests = checkpointRequests_.load(std::memory_order_relaxed);
    telemetry.coalesced = checkpointCoalesced_.load(std::memory_order_relaxed);
    telemetry.started = checkpointStarted_.load(std::memory_order_relaxed);
    telemetry.completed = checkpointCompleted_.load(std::memory_order_relaxed);
    telemetry.timedOut = checkpointTimedOut_.load(std::memory_order_relaxed);
    telemetry.postFailures = checkpointPostFailures_.load(std::memory_order_relaxed);

    const uint64_t nowNs = steadyNowNs();
    if (telemetry.phase == VectorCheckpointPhase::Queued) {
        telemetry.queuedAgeMs =
            elapsedMs(checkpointQueuedAtNs_.load(std::memory_order_relaxed), nowNs);
    } else if (telemetry.phase == VectorCheckpointPhase::Running) {
        telemetry.runningAgeMs =
            elapsedMs(checkpointRunningAtNs_.load(std::memory_order_relaxed), nowNs);
    }
    return telemetry;
}

void VectorIndexCoordinator::publishTelemetry(const VectorIndexTelemetry& tel) noexcept {
    std::lock_guard<std::mutex> lock(telemetryMutex_);
    telemetry_ = tel;
}

// ── Waiters ──────────────────────────────────────────────────────────────────

void VectorIndexCoordinator::notifyWaiters(uint64_t currentEpoch, const Result<void>& result) {
    YAMS_ZONE_SCOPED_N("VecIdxCoord::notifyWaiters");
    // Drain waiters whose targetEpoch is satisfied.  Protected by waitersMutex_.
    std::vector<RebuildCompletion> toNotify;
    {
        std::lock_guard<std::mutex> lk(waitersMutex_);
        auto it = waiters_.begin();
        while (it != waiters_.end()) {
            if (it->targetEpoch <= currentEpoch) {
                toNotify.push_back(std::move(it->handler));
                it = waiters_.erase(it);
            } else {
                ++it;
            }
        }
    }
    for (auto& h : toNotify) {
        if (result) {
            h(Result<void>{});
        } else {
            h(result.error());
        }
    }
}

} // namespace yams::daemon
