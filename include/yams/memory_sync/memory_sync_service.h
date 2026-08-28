// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>

// pi-lens-ignore: fatal error
#include <yams/core/types.h>
#include <yams/memory_sync/memory_sync.h>
#include <yams/storage/storage_backend.h>

namespace yams::memory_sync {

/// Typed configuration for the memory-sync service. The backend itself is injected
/// (so filesystem and S3/R2 resolution stay with the caller, e.g.
/// StorageBackendFactory + resolveStorageBootstrapDecision). No YAMS_* env knobs.
struct MemorySyncConfig {
    std::string nodeId;                 /// this daemon's writer identity
    std::uint32_t syncIntervalMs{5000}; /// periodic reconcile cadence
    std::string corpusId{"local-test-corpus"};
    std::uint64_t corpusEpoch{1};
    MemorySyncLimits limits{};
    bool cleanupOwnedNamespaceOnStop{false};
    std::shared_ptr<const WriterAuthenticator> writerAuth; /// optional schema-v4 signer/verifier

    MemorySyncConfig() = default;
    explicit MemorySyncConfig(std::string writerId, std::uint32_t intervalMs = 5000,
                              std::string corpus = "local-test-corpus", std::uint64_t epoch = 1,
                              MemorySyncLimits resourceLimits = {}, bool cleanupOwned = false,
                              std::shared_ptr<const WriterAuthenticator> auth = {})
        : nodeId(std::move(writerId)), syncIntervalMs(intervalMs), corpusId(std::move(corpus)),
          corpusEpoch(epoch), limits(resourceLimits), cleanupOwnedNamespaceOnStop(cleanupOwned),
          writerAuth(std::move(auth)) {}
};

/// Daemon-facing service that runs the memory-sync loop periodically and exposes
/// publish/read/syncOnce. Owns the backend; start/stop are idempotent and stop is
/// noexcept-safe for the shutdown path.
class MemorySyncService {
public:
    MemorySyncService(std::unique_ptr<storage::IStorageBackend> backend, MemorySyncConfig config,
                      std::function<bool()> canAdmitRemoteWork = {},
                      bool allowLegacyUnbound = false,
                      std::unique_ptr<storage::IStorageBackend> sessionMaintenanceBackend = {},
                      std::string sessionLeaseKey = {})
        : backend_(std::move(backend)),
          sessionMaintenanceBackend_(std::move(sessionMaintenanceBackend)),
          sessionLeaseKey_(std::move(sessionLeaseKey)), config_(std::move(config)),
          loop_(*backend_, config_.nodeId, config_.corpusId, config_.corpusEpoch,
                allowLegacyUnbound, config_.limits,
                MemorySyncControl{
                    .isCancelled = [this] { return stop_.load(std::memory_order_acquire); },
                    .canAdmitRemoteWork = std::move(canAdmitRemoteWork)},
                {}, config_.writerAuth) {}

    ~MemorySyncService() { stop(); }

    MemorySyncService(const MemorySyncService&) = delete;
    MemorySyncService& operator=(const MemorySyncService&) = delete;

    /// Start the periodic sync worker (idempotent).
    Result<void> start() {
        std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex_);
        if (worker_.joinable()) {
            return {};
        }
        if (config_.syncIntervalMs == 0) {
            return Error{ErrorCode::InvalidArgument,
                         "memory_sync.syncIntervalMs must be greater than zero"};
        }
        backend_->resetCancel(); // A restarted worker receives a fresh cancellation generation.
        stop_.store(false, std::memory_order_release);
        if (auto lease = refreshSessionLease(); !lease) {
            return lease.error();
        }
        namespaceCleaned_.store(false, std::memory_order_release);
        try {
            worker_ = std::thread([this] { workerLoop(); });
        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError, e.what()};
        }
        return {};
    }

    /// Stop the periodic worker and join (idempotent, noexcept).
    void stop() noexcept {
        stop_.store(true, std::memory_order_release);
        backend_->requestCancel();
        sleepCv_.notify_all();
        if (std::hash<std::thread::id>{}(std::this_thread::get_id()) ==
            workerIdHash_.load(std::memory_order_acquire)) {
            return; // The worker exits after the callback; a non-worker owner performs the join.
        }
        std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex_);
        // A concurrent start may have acquired the lifecycle lock after the first request.
        // Reassert cancellation while holding the lock before joining that generation.
        stop_.store(true, std::memory_order_release);
        sleepCv_.notify_all();
        if (worker_.joinable()) {
            try {
                worker_.join();
            } catch (...) {
                shutdownFailures_.fetch_add(1, std::memory_order_relaxed);
            }
        }
        if (config_.cleanupOwnedNamespaceOnStop && !namespaceCleaned_.exchange(true)) {
            backend_->resetCancel();
            if (!backend_->clear()) {
                shutdownFailures_.fetch_add(1, std::memory_order_relaxed);
            }
            if (sessionMaintenanceBackend_ && !sessionLeaseKey_.empty() &&
                !sessionMaintenanceBackend_->remove(sessionLeaseKey_)) {
                shutdownFailures_.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }

    bool started() const noexcept {
        std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex_);
        return worker_.joinable() && !stop_.load(std::memory_order_acquire);
    }

    bool stopRequested() const noexcept { return stop_.load(std::memory_order_acquire); }

    /// Called after a successful periodic reconciliation and after releasing loopMutex_.
    /// The callback may safely use this service's public read/sync methods.
    void setAfterSyncCallback(std::function<void()> callback) {
        std::lock_guard<std::mutex> lock(callbackMutex_);
        afterSyncCallback_ = std::move(callback);
    }

    Result<void> publish(std::string_view key, std::span<const std::byte> content) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        auto result = loop_.publish(key, content);
        if (result) {
            refreshCommittedState();
        }
        return result;
    }

    Result<void> erase(std::string_view key, std::string tombstonePayload = {}) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        auto result = loop_.erase(key, std::move(tombstonePayload));
        if (result) {
            refreshCommittedState();
        }
        return result;
    }

    /// Publish only when the winning cached payload differs. This makes periodic
    /// corpus backfill safe without creating a new causal envelope every cycle.
    Result<bool> publishIfChanged(std::string_view key, std::span<const std::byte> content) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        auto cached = loop_.readCached(key);
        if (cached && std::ranges::equal(cached.value(), content)) {
            return false;
        }
        auto published = loop_.publish(key, content);
        if (!published) {
            return published.error();
        }
        refreshCommittedState();
        return true;
    }

    Result<std::vector<std::byte>> read(std::string_view key) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        auto result = loop_.read(key);
        if (result) {
            refreshCommittedState();
        }
        return result;
    }

    /// Return a value observed by the periodic worker without initiating sync.
    /// Reads the committed snapshot, so a slow reconciliation (e.g. a remote
    /// backend) can never stall IPC status or cached reads.
    Result<std::vector<std::byte>> readCached(std::string_view key) const {
        std::shared_ptr<const MemorySyncLoop::CommittedState> state = committedSnapshot();
        if (!state) {
            return Error{ErrorCode::NotFound, "memory sync has not reconciled"};
        }
        const auto record = state->merged.find(std::string(key));
        if (record == state->merged.end()) {
            return Error{ErrorCode::NotFound, "no memory record for key"};
        }
        if (record->second.isTombstone()) {
            return Error{ErrorCode::NotFound, "memory record was deleted"};
        }
        const auto cached = state->cachedBlobs.find(record->second.entryHash);
        if (cached == state->cachedBlobs.end()) {
            return Error{ErrorCode::NotFound, "memory sync blob was not hydrated during sync"};
        }
        return cached->second;
    }

    std::size_t mergedRecordCount() const noexcept {
        const auto state = committedSnapshot();
        return state ? state->merged.size() : 0;
    }

    std::size_t quarantinedRecordCount() const noexcept {
        const auto state = committedSnapshot();
        return state ? state->quarantined.size() : 0;
    }

    /// Bounded snapshot of current quarantine reasons (cleared each scan page).
    std::map<std::string, std::string> quarantinedReasons() const {
        const auto state = committedSnapshot();
        return state ? state->quarantined : std::map<std::string, std::string>{};
    }

    std::size_t authFailureCount() const noexcept {
        const auto state = committedSnapshot();
        return state ? state->authFailures : 0;
    }

    std::uint64_t successfulSyncCycles() const noexcept {
        return successfulSyncCycles_.load(std::memory_order_relaxed);
    }

    std::uint64_t failedSyncCycles() const noexcept {
        return failedSyncCycles_.load(std::memory_order_relaxed);
    }

    std::uint64_t lastSuccessfulSyncAgeMs() const noexcept {
        const auto completed = lastSuccessfulSyncSteadyMs_.load(std::memory_order_acquire);
        if (completed == 0) {
            return std::numeric_limits<std::uint64_t>::max();
        }
        const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now().time_since_epoch())
                             .count();
        return now <= completed ? 0 : static_cast<std::uint64_t>(now - completed);
    }

    Result<std::map<std::string, MemoryIndexRecord>> syncOnce() {
        if (callbackOwner_ == this && callbackSnapshot_ != nullptr) {
            return *callbackSnapshot_;
        }
        std::lock_guard<std::mutex> lock(loopMutex_);
        auto result = loop_.sync();
        if (result) {
            refreshCommittedState();
        }
        return result;
    }

    Result<std::map<std::string, MemoryIndexRecord>> syncFully() {
        std::lock_guard<std::mutex> lock(loopMutex_);
        auto result = loop_.syncFully();
        if (result) {
            refreshCommittedState();
        }
        return result;
    }

    Result<MemoryDeltaBatch> exportLocalDeltasAfter(const VersionVector& peerVersion,
                                                    std::size_t maxDeltas = 128) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.exportLocalDeltasAfter(peerVersion, maxDeltas);
    }

    Result<DeltaApplyResult> applyDeltas(std::span<const MemoryDelta> deltas) {
        Result<DeltaApplyResult> result;
        {
            std::lock_guard<std::mutex> lock(loopMutex_);
            result = loop_.applyDeltas(deltas);
            if (result) {
                refreshCommittedState();
            }
        }
        if (result && result.value().merged != 0) {
            invokeAfterSyncCallback();
        }
        return result;
    }

    VersionVector currentVersion() const {
        const auto state = committedSnapshot();
        return state ? state->version : VersionVector{};
    }

#ifdef YAMS_TESTING
    /// Observe periodic reconciliation without triggering another sync cycle.
    bool testingHasMergedRecord(std::string_view key) const {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.hasMergedRecord(key);
    }
#endif

private:
    /// Immutable state published for IPC readers. Readers never take loopMutex_
    /// (held for the whole reconciliation on slow backends), so a slow sync can
    /// not stall status or cached reads; they consume this snapshot instead.
    std::shared_ptr<const MemorySyncLoop::CommittedState> committedSnapshot() const {
        std::lock_guard<std::mutex> lock(snapshotMutex_);
        return committed_;
    }

    /// Refresh the committed snapshot from the loop. Callers must hold loopMutex_
    /// so the loop state is quiescent while it is copied.
    void refreshCommittedState() {
        auto state = std::make_shared<MemorySyncLoop::CommittedState>(loop_.committedState());
        std::lock_guard<std::mutex> lock(snapshotMutex_);
        committed_ = std::move(state);
    }

    void invokeAfterSyncCallback(
        const std::map<std::string, MemoryIndexRecord>* snapshot = nullptr) noexcept {
        std::function<void()> callback;
        {
            std::lock_guard<std::mutex> lock(callbackMutex_);
            callback = afterSyncCallback_;
        }
        if (!callback || stop_.load(std::memory_order_acquire)) {
            return;
        }
        callbackOwner_ = snapshot != nullptr ? this : nullptr;
        callbackSnapshot_ = snapshot;
        try {
            callback();
        } catch (...) {
            // Adapter failures are isolated from the convergence/session worker.
            callbackFailures_.fetch_add(1, std::memory_order_relaxed);
        }
        callbackSnapshot_ = nullptr;
        callbackOwner_ = nullptr;
    }

    void workerLoop() {
        workerIdHash_.store(std::hash<std::thread::id>{}(std::this_thread::get_id()),
                            std::memory_order_release);
        while (!stop_.load(std::memory_order_acquire)) {
            Result<std::map<std::string, MemoryIndexRecord>> snapshot =
                Error{ErrorCode::InternalError, "memory sync did not reconcile"};
            try {
                std::lock_guard<std::mutex> lock(loopMutex_);
                snapshot = loop_.sync();
                if (snapshot) {
                    refreshCommittedState();
                }
            } catch (const std::exception& e) {
                snapshot = Error{ErrorCode::InternalError,
                                 std::string("memory sync reconciliation threw: ") + e.what()};
            } catch (...) {
                snapshot = Error{ErrorCode::InternalError,
                                 "memory sync reconciliation threw an unknown exception"};
            }
            if (snapshot) {
                auto lease = refreshSessionLease();
                if (!lease) {
                    // An unrenewed temporary lease makes this cycle unhealthy. Do not expose
                    // winners or advance freshness while another peer may collect the namespace.
                    failedSyncCycles_.fetch_add(1, std::memory_order_relaxed);
                } else {
                    successfulSyncCycles_.fetch_add(1, std::memory_order_relaxed);
                    lastSuccessfulSyncSteadyMs_.store(
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now().time_since_epoch())
                            .count(),
                        std::memory_order_release);
                    invokeAfterSyncCallback(&snapshot.value());
                }
            } else {
                failedSyncCycles_.fetch_add(1, std::memory_order_relaxed);
            }
            std::unique_lock<std::mutex> sleepLock(sleepMutex_);
            sleepCv_.wait_for(sleepLock, std::chrono::milliseconds(config_.syncIntervalMs),
                              [this] { return stop_.load(std::memory_order_acquire); });
        }
        workerIdHash_.store(0, std::memory_order_release);
    }

    Result<void> refreshSessionLease() noexcept {
        try {
            if (!sessionMaintenanceBackend_ || sessionLeaseKey_.empty()) {
                return {};
            }
            const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
            const auto value = std::to_string(now);
            return sessionMaintenanceBackend_->store(
                sessionLeaseKey_,
                std::span<const std::byte>{reinterpret_cast<const std::byte*>(value.data()),
                                           value.size()});
        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError,
                         std::string("memory sync session lease refresh threw: ") + e.what()};
        } catch (...) {
            return Error{ErrorCode::InternalError,
                         "memory sync session lease refresh threw an unknown exception"};
        }
    }

    std::unique_ptr<storage::IStorageBackend> backend_;
    std::unique_ptr<storage::IStorageBackend> sessionMaintenanceBackend_;
    std::string sessionLeaseKey_;
    MemorySyncConfig config_;
    MemorySyncLoop loop_;
    std::atomic<bool> stop_{false};
    std::thread worker_;
    std::atomic<std::size_t> workerIdHash_{0};
    mutable std::mutex lifecycleMutex_;
    std::mutex sleepMutex_;
    std::condition_variable sleepCv_;
    inline static thread_local const MemorySyncService* callbackOwner_{nullptr};
    inline static thread_local const std::map<std::string, MemoryIndexRecord>* callbackSnapshot_{
        nullptr};
    // Serializes all MemorySyncLoop access (worker sync vs publish/read/syncOnce);
    // the loop keeps mutable version-vector/logical-clock/merged state.
    mutable std::mutex loopMutex_;
    mutable std::mutex callbackMutex_;
    std::function<void()> afterSyncCallback_;
    std::atomic<std::uint64_t> callbackFailures_{0};
    std::atomic<std::uint64_t> shutdownFailures_{0};
    std::atomic<std::uint64_t> successfulSyncCycles_{0};
    std::atomic<std::uint64_t> failedSyncCycles_{0};
    std::atomic<std::int64_t> lastSuccessfulSyncSteadyMs_{0};
    std::atomic<bool> namespaceCleaned_{false};
    mutable std::mutex snapshotMutex_;
    std::shared_ptr<const MemorySyncLoop::CommittedState> committed_;
};

} // namespace yams::memory_sync
