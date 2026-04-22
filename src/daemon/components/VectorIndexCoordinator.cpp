#include <yams/daemon/components/VectorIndexCoordinator.h>

#include <yams/daemon/components/StateComponent.h>
#include <yams/vector/vector_database.h>

#include <spdlog/spdlog.h>

#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>

namespace yams::daemon {

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
    : strand_(boost::asio::make_strand(exec)), vectorDb_(std::move(vdb)), state_(state) {}

VectorIndexCoordinator::~VectorIndexCoordinator() = default;

void VectorIndexCoordinator::postRebuild(RebuildReason reason) noexcept {
    pendingReasons_.fetch_or(static_cast<uint32_t>(reason), std::memory_order_relaxed);
    boost::asio::post(strand_, [this, reason]() {
        pendingReasons_.fetch_or(static_cast<uint32_t>(reason), std::memory_order_relaxed);
        if (!rebuildInFlight_) {
            rebuildInFlight_ = true;
            const uint32_t reasons = pendingReasons_.exchange(0, std::memory_order_relaxed);
            auto t = readTelemetrySeqlock();
            t.rebuilding = true;
            t.pendingReasons = reasons;
            publishTelemetry(t);
            if (state_) {
                state_->readiness.vectorIndexReady.store(false, std::memory_order_relaxed);
                state_->readiness.vectorIndexProgress.store(0, std::memory_order_relaxed);
            }
            boost::asio::co_spawn(strand_, doRebuild(reasons), boost::asio::detached);
        }
    });
}

// Helper: get the VDB under the mutex for callers on any thread.
static std::shared_ptr<vector::VectorDatabase>
getVdbLocked(std::mutex& mu, const std::shared_ptr<vector::VectorDatabase>& vdb) {
    std::lock_guard<std::mutex> lk(mu);
    return vdb;
}

VectorIndexCoordinator::BulkScope VectorIndexCoordinator::beginBulkIngest(RebuildReason reason) {
    pendingReasons_.fetch_or(static_cast<uint32_t>(reason), std::memory_order_relaxed);

    // fetch_add is atomic: exactly one caller sees prev == 0 and enters bulk mode.
    const uint32_t prev = activeBulkScopes_.fetch_add(1, std::memory_order_acq_rel);
    if (prev == 0) {
        auto vdb = getVdbLocked(vdbMutex_, vectorDb_);
        if (vdb) {
            if (!vdb->beginBulkLoad()) {
                spdlog::warn("[VectorIndexCoordinator] beginBulkLoad failed: {}",
                             vdb->getLastError());
            }
        }
    }

    // Update seqlock telemetry (best-effort; written from any thread here,
    // so we use a quick publish that may race with strand writes but is
    // acceptable for the activeBulkScopes counter).
    auto t = readTelemetrySeqlock();
    t.activeBulkScopes = prev + 1;
    publishTelemetry(t);

    return BulkScope(this);
}

void VectorIndexCoordinator::releaseBulkScope() noexcept {
    const uint32_t prev = activeBulkScopes_.fetch_sub(1, std::memory_order_acq_rel);
    if (prev == 1) {
        // Last scope: post finalize+persist to strand.
        boost::asio::post(strand_, [this]() { doFinalizeOnStrand(); });
    } else {
        auto t = readTelemetrySeqlock();
        t.activeBulkScopes = (prev > 1) ? (prev - 1) : 0;
        publishTelemetry(t);
    }
}

void VectorIndexCoordinator::doFinalizeOnStrand() {
    // Running on the strand.
    auto vdb = getVdbLocked(vdbMutex_, vectorDb_);
    if (vdb) {
        if (!vdb->finalizeBulkLoad()) {
            spdlog::warn("[VectorIndexCoordinator] finalizeBulkLoad failed: {}",
                         vdb->getLastError());
        }
        try {
            vdb->buildIndex();
            vdb->persistIndex();
        } catch (const std::exception& e) {
            spdlog::warn("[VectorIndexCoordinator] index build/persist failed: {}", e.what());
        } catch (...) {
            spdlog::warn("[VectorIndexCoordinator] index build/persist failed (unknown)");
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

    notifyWaiters(epoch);
}

boost::asio::awaitable<Result<void>> VectorIndexCoordinator::requestRebuild(RebuildReason reason) {
    // Ensure we run on the strand.
    co_await boost::asio::dispatch(strand_, boost::asio::use_awaitable);

    pendingReasons_.fetch_or(static_cast<uint32_t>(reason), std::memory_order_relaxed);
    const uint64_t targetEpoch = rebuildEpoch_.load(std::memory_order_relaxed) + 1;

    if (!rebuildInFlight_) {
        rebuildInFlight_ = true;
        const uint32_t reasons = pendingReasons_.exchange(0, std::memory_order_relaxed);

        // Update telemetry: rebuilding started.
        auto t = readTelemetrySeqlock();
        t.rebuilding = true;
        t.pendingReasons = reasons;
        publishTelemetry(t);

        if (state_) {
            state_->readiness.vectorIndexReady.store(false, std::memory_order_relaxed);
            state_->readiness.vectorIndexProgress.store(0, std::memory_order_relaxed);
        }

        boost::asio::co_spawn(strand_, doRebuild(reasons), boost::asio::detached);
    }

    // If epoch already satisfies the target (rebuild was very fast), return immediately.
    if (rebuildEpoch_.load(std::memory_order_relaxed) >= targetEpoch) {
        co_return Result<void>{};
    }

    // Suspend until epoch advances.  doRebuild will call notifyWaiters which
    // invokes our completion handler on the strand's executor.
    co_return co_await boost::asio::async_initiate<const boost::asio::use_awaitable_t<>&,
                                                   void(Result<void>)>(
        [this, targetEpoch](auto&& handler) mutable {
            if (rebuildEpoch_.load(std::memory_order_relaxed) >= targetEpoch) {
                // Already done — complete inline.
                auto ex = boost::asio::get_associated_executor(handler, strand_);
                boost::asio::post(ex, [h = std::move(handler)]() mutable { h(Result<void>{}); });
            } else {
                // Wrap the move-only handler in a shared_ptr so std::function can store it.
                auto h = std::make_shared<std::decay_t<decltype(handler)>>(std::move(handler));
                waiters_.push_back(
                    {targetEpoch, [h](Result<void> r) mutable { (*h)(std::move(r)); }});
            }
        },
        boost::asio::use_awaitable);
}

boost::asio::awaitable<void> VectorIndexCoordinator::doRebuild(uint32_t /*reasons*/) {
    // Running on the strand.
    {
        auto vdb = getVdbLocked(vdbMutex_, vectorDb_);
        if (vdb) {
            try {
                vdb->buildIndex();
                vdb->persistIndex();
            } catch (const std::exception& e) {
                spdlog::warn("[VectorIndexCoordinator] doRebuild build/persist failed: {}",
                             e.what());
            } catch (...) {
                spdlog::warn("[VectorIndexCoordinator] doRebuild build/persist failed (unknown)");
            }
        }
    }

    const uint64_t epoch = rebuildEpoch_.fetch_add(1, std::memory_order_acq_rel) + 1;

    // Drain any reasons that arrived during rebuild — but only start a follow-up
    // rebuild if there are active waiters expecting a future epoch.  Without this
    // guard, concurrent same-reason callers cause spurious extra rebuilds.
    notifyWaiters(epoch);
    const bool hasNewWaiters =
        std::any_of(waiters_.begin(), waiters_.end(),
                    [epoch](const Waiter& w) { return w.targetEpoch > epoch; });
    const uint32_t nextReasons = pendingReasons_.exchange(0, std::memory_order_relaxed);
    rebuildInFlight_ = (nextReasons != 0 && hasNewWaiters);

    VectorIndexTelemetry t{};
    t.rebuildEpoch = epoch;
    t.rebuilding = rebuildInFlight_;
    t.ready = !rebuildInFlight_;
    t.progressPct = 100;
    t.activeBulkScopes = activeBulkScopes_.load(std::memory_order_relaxed);
    t.pendingReasons = rebuildInFlight_ ? nextReasons : 0u;
    publishTelemetry(t);

    if (state_) {
        state_->readiness.vectorIndexReady.store(!rebuildInFlight_, std::memory_order_relaxed);
        state_->readiness.vectorIndexProgress.store(100, std::memory_order_relaxed);
    }

    if (rebuildInFlight_) {
        boost::asio::co_spawn(strand_, doRebuild(nextReasons), boost::asio::detached);
    }

    co_return;
}

boost::asio::awaitable<Result<void>> VectorIndexCoordinator::initialBuildIfNeeded() {
    co_await boost::asio::dispatch(strand_, boost::asio::use_awaitable);

    auto vdb = getVdbLocked(vdbMutex_, vectorDb_);
    if (!vdb || !vdb->isInitialized()) {
        co_return Result<void>{};
    }

    try {
        const auto rows = vdb->getVectorCount();
        if (rows > 0 && !vdb->hasReusablePersistedSearchIndex()) {
            // Rows exist but no persisted HNSW — trigger a rebuild.
            co_return co_await requestRebuild(RebuildReason::InitialBuild);
        } else if (rows > 0) {
            // Index exists: load it.
            if (!vdb->prepareSearchIndex()) {
                spdlog::warn("[VectorIndexCoordinator] prepareSearchIndex failed: {}",
                             vdb->getLastError());
            } else {
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
            }
        }
    } catch (...) {
    }

    co_return Result<void>{};
}

VectorIndexTelemetry VectorIndexCoordinator::snapshot() const noexcept {
    return readTelemetrySeqlock();
}

// ── Seqlock helpers ─────────────────────────────────────────────────────────

void VectorIndexCoordinator::publishTelemetry(const VectorIndexTelemetry& tel) noexcept {
    // Begin write: bump to odd.
    seqVersion_.fetch_add(1, std::memory_order_release);
    seqData_ = tel;
    // End write: bump to even.
    seqVersion_.fetch_add(1, std::memory_order_release);
}

VectorIndexTelemetry VectorIndexCoordinator::readTelemetrySeqlock() const noexcept {
    while (true) {
        const uint64_t v1 = seqVersion_.load(std::memory_order_acquire);
        if (v1 & 1u) {
            // Writer active — spin.
            continue;
        }
        const VectorIndexTelemetry t = seqData_;
        std::atomic_thread_fence(std::memory_order_acquire);
        const uint64_t v2 = seqVersion_.load(std::memory_order_relaxed);
        if (v1 == v2) {
            return t;
        }
    }
}

// ── Waiters ──────────────────────────────────────────────────────────────────

void VectorIndexCoordinator::notifyWaiters(uint64_t currentEpoch) {
    // Called on the strand after an epoch bump.
    std::vector<RebuildCompletion> toNotify;
    auto it = waiters_.begin();
    while (it != waiters_.end()) {
        if (it->targetEpoch <= currentEpoch) {
            toNotify.push_back(std::move(it->handler));
            it = waiters_.erase(it);
        } else {
            ++it;
        }
    }
    for (auto& h : toNotify) {
        h(Result<void>{});
    }
}

} // namespace yams::daemon
