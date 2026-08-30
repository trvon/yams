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
    std::string controlScope;                              /// authenticated local-store generation

    MemorySyncConfig() = default;
    explicit MemorySyncConfig(std::string writerId, std::uint32_t intervalMs = 5000,
                              std::string corpus = "local-test-corpus", std::uint64_t epoch = 1,
                              MemorySyncLimits resourceLimits = {}, bool cleanupOwned = false,
                              std::shared_ptr<const WriterAuthenticator> auth = {},
                              std::string authenticatedControlScope = {})
        : nodeId(std::move(writerId)), syncIntervalMs(intervalMs), corpusId(std::move(corpus)),
          corpusEpoch(epoch), limits(resourceLimits), cleanupOwnedNamespaceOnStop(cleanupOwned),
          writerAuth(std::move(auth)), controlScope(std::move(authenticatedControlScope)) {}
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
                {}, config_.writerAuth, config_.controlScope) {}

    ~MemorySyncService() { stop(); }

    MemorySyncService(const MemorySyncService&) = delete;
    MemorySyncService& operator=(const MemorySyncService&) = delete;

    /// Start the periodic sync worker (idempotent).
    Result<void> start() {
        if (activeCallbackSnapshot() != nullptr) {
            return Error{ErrorCode::InvalidState,
                         "memory sync cannot restart from its active callback"};
        }
        std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex_);
        const bool restarting = stop_.load(std::memory_order_acquire);
        if (worker_.joinable()) {
            if (!stop_.load(std::memory_order_acquire)) {
                return {};
            }
            try {
                worker_.join();
            } catch (const std::exception& e) {
                return Error{ErrorCode::InternalError, e.what()};
            }
        }
        std::unique_lock<std::recursive_mutex> callbackDrain(callbackExecutionMutex_,
                                                             std::defer_lock);
        if (restarting) {
            callbackDrain.lock();
        }
        if (config_.syncIntervalMs == 0) {
            return Error{ErrorCode::InvalidArgument,
                         "memory_sync.syncIntervalMs must be greater than zero"};
        }
        backend_->resetCancel(); // A restarted worker receives a fresh cancellation generation.
        callbackGeneration_.fetch_add(1, std::memory_order_acq_rel);
        stop_.store(false, std::memory_order_release);
        if (auto lease = refreshSessionLease(); !lease) {
            return lease.error();
        }
        namespaceCleaned_.store(false, std::memory_order_release);
        workerRunning_.store(true, std::memory_order_release);
        try {
            worker_ = std::thread([this] { workerLoop(); });
        } catch (const std::exception& e) {
            workerRunning_.store(false, std::memory_order_release);
            return Error{ErrorCode::InternalError, e.what()};
        }
        return {};
    }

    /// Stop the periodic worker and join (idempotent, noexcept).
    void stop() noexcept {
        stop_.store(true, std::memory_order_release);
        callbackGeneration_.fetch_add(1, std::memory_order_acq_rel);
        backend_->requestCancel();
        sleepCv_.notify_all();
        if (activeCallbackSnapshot() != nullptr ||
            std::hash<std::thread::id>{}(std::this_thread::get_id()) ==
                workerIdHash_.load(std::memory_order_acquire)) {
            // A callback may own callbackExecutionMutex_ while the worker is waiting for it.
            // Leave joining to a non-callback owner to avoid a stop/join lock cycle.
            return;
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
        // Direct-session callbacks do not belong to worker_. Drain the active callback before
        // releasing lifecycleMutex_. Generation checks discard callbacks queued before stop().
        std::lock_guard<std::recursive_mutex> callbackDrain(callbackExecutionMutex_);
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
        return workerRunning_.load(std::memory_order_acquire) &&
               !stop_.load(std::memory_order_acquire);
    }

    bool stopRequested() const noexcept { return stop_.load(std::memory_order_acquire); }
    [[nodiscard]] const std::string& localNodeId() const noexcept { return config_.nodeId; }
    [[nodiscard]] bool writerAuthenticationReady() const noexcept {
        return config_.writerAuth && config_.writerAuth->required();
    }
    [[nodiscard]] bool directP2pReady(std::string_view nodeId, std::string_view corpusId,
                                      std::uint64_t corpusEpoch) const noexcept {
        return writerAuthenticationReady() && !config_.controlScope.empty() &&
               fullRecoveryCompleted_.load(std::memory_order_acquire) &&
               !legacyUnauthenticatedHistory_.load(std::memory_order_acquire) &&
               config_.nodeId == nodeId && config_.corpusId == corpusId &&
               config_.corpusEpoch == corpusEpoch;
    }
    [[nodiscard]] bool legacyUnauthenticatedHistoryObserved() const {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.legacyUnauthenticatedHistoryObserved();
    }

    /// Called after a successful periodic reconciliation and after releasing loopMutex_.
    /// The callback may safely use this service's public read/sync methods.
    void setAfterSyncCallback(std::function<void()> callback) {
        std::lock_guard<std::mutex> lock(callbackMutex_);
        afterSyncCallback_ = std::move(callback);
    }

    Result<void> publish(std::string_view key, std::span<const std::byte> content) {
        Result<void> result;
        bool quarantineChanged = false;
        {
            std::lock_guard<std::mutex> lock(loopMutex_);
            const auto generation = loop_.durableQuarantineGeneration();
            result = loop_.publish(key, content);
            quarantineChanged = generation != loop_.durableQuarantineGeneration();
            if (result || quarantineChanged) {
                refreshCommittedState();
            }
        }
        if (quarantineChanged) {
            invokeAfterSyncCallback();
        }
        return result;
    }

    Result<void> erase(std::string_view key, std::string tombstonePayload = {}) {
        Result<void> result;
        bool quarantineChanged = false;
        {
            std::lock_guard<std::mutex> lock(loopMutex_);
            const auto generation = loop_.durableQuarantineGeneration();
            result = loop_.erase(key, std::move(tombstonePayload));
            quarantineChanged = generation != loop_.durableQuarantineGeneration();
            if (result || quarantineChanged) {
                refreshCommittedState();
            }
        }
        if (quarantineChanged) {
            invokeAfterSyncCallback();
        }
        return result;
    }

    Result<void> stageErase(std::string_view key, std::string tombstonePayload, bool ready = false,
                            EraseReadinessProbe readinessProbe = EraseReadinessProbe::Explicit) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.stageErase(key, std::move(tombstonePayload), ready, readinessProbe);
    }

    Result<void> stageErases(std::span<const EraseStageRequest> requests) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.stageErases(requests);
    }

    Result<std::vector<PendingEraseIntent>> pendingErases() {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.pendingErases();
    }

    Result<void> cancelStagedErase(std::string_view key) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.cancelStagedErase(key);
    }

    Result<void> publishStagedErase(std::string_view key,
                                    MemorySyncLoop::EraseReadyValidator validator = {}) {
        Result<void> result;
        bool quarantineChanged = false;
        {
            std::lock_guard<std::mutex> lock(loopMutex_);
            const auto generation = loop_.durableQuarantineGeneration();
            result = loop_.publishStagedErase(key, std::move(validator));
            quarantineChanged = generation != loop_.durableQuarantineGeneration();
            if (result || quarantineChanged) {
                refreshCommittedState();
            }
        }
        if (quarantineChanged) {
            invokeAfterSyncCallback();
        }
        return result;
    }

    /// Publish only when the winning cached payload differs. This makes periodic
    /// corpus backfill safe without creating a new causal envelope every cycle.
    Result<bool> publishIfChanged(std::string_view key, std::span<const std::byte> content) {
        Result<bool> result;
        bool quarantineChanged = false;
        {
            std::lock_guard<std::mutex> lock(loopMutex_);
            auto cached = loop_.readCached(key);
            if (cached && std::ranges::equal(cached.value(), content)) {
                return false;
            }
            const auto generation = loop_.durableQuarantineGeneration();
            auto published = loop_.publish(key, content);
            quarantineChanged = generation != loop_.durableQuarantineGeneration();
            if (!published) {
                result = published.error();
            } else {
                result = true;
            }
            if (result || quarantineChanged) {
                refreshCommittedState();
            }
        }
        if (quarantineChanged) {
            invokeAfterSyncCallback();
        }
        return result;
    }

    Result<std::vector<std::byte>> read(std::string_view key) {
        Result<std::vector<std::byte>> result;
        bool quarantineChanged = false;
        {
            std::lock_guard<std::mutex> lock(loopMutex_);
            const auto generation = loop_.durableQuarantineGeneration();
            result = loop_.read(key);
            quarantineChanged = generation != loop_.durableQuarantineGeneration();
            if (result || quarantineChanged) {
                refreshCommittedState();
            }
        }
        if (quarantineChanged) {
            invokeAfterSyncCallback();
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

    [[nodiscard]] bool hasCommittedTombstone(std::string_view key) const noexcept {
        const auto state = committedSnapshot();
        if (!state) {
            return false;
        }
        const auto record = state->merged.find(std::string(key));
        return record != state->merged.end() && record->second.isTombstone();
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
        if (const auto* callbackSnapshot = activeCallbackSnapshot()) {
            return *callbackSnapshot;
        }
        Result<std::map<std::string, MemoryIndexRecord>> result;
        bool quarantineChanged = false;
        {
            std::lock_guard<std::mutex> lock(loopMutex_);
            const auto generation = loop_.durableQuarantineGeneration();
            result = loop_.sync();
            if (loop_.legacyUnauthenticatedHistoryObserved()) {
                legacyUnauthenticatedHistory_.store(true, std::memory_order_release);
                fullRecoveryCompleted_.store(false, std::memory_order_release);
            }
            quarantineChanged = generation != loop_.durableQuarantineGeneration();
            if (result || quarantineChanged) {
                refreshCommittedState();
            }
        }
        if (quarantineChanged) {
            invokeAfterSyncCallback();
        }
        return result;
    }

    Result<std::map<std::string, MemoryIndexRecord>> syncFully() {
        Result<std::map<std::string, MemoryIndexRecord>> result;
        bool quarantineChanged = false;
        {
            std::lock_guard<std::mutex> lock(loopMutex_);
            const auto generation = loop_.durableQuarantineGeneration();
            result = loop_.syncFully();
            const bool legacyHistory = loop_.legacyUnauthenticatedHistoryObserved();
            legacyUnauthenticatedHistory_.store(legacyHistory, std::memory_order_release);
            fullRecoveryCompleted_.store(result.has_value() && !legacyHistory,
                                         std::memory_order_release);
            quarantineChanged = generation != loop_.durableQuarantineGeneration();
            if (result || quarantineChanged) {
                refreshCommittedState();
            }
        }
        if (quarantineChanged) {
            invokeAfterSyncCallback();
        }
        return result;
    }

    Result<MemoryDeltaBatch> exportLocalDeltasAfter(
        const VersionVector& peerVersion, std::size_t maxDeltas = 128,
        std::uint64_t maxWriterCounter = std::numeric_limits<std::uint64_t>::max()) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.exportLocalDeltasAfter(peerVersion, maxDeltas, maxWriterCounter);
    }

    Result<ColdBootstrapSnapshot> exportColdBootstrap(const ReplicationState& frozen,
                                                      std::size_t maxRecords,
                                                      std::size_t maxPayloadBytes) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.exportColdBootstrap(frozen, maxRecords, maxPayloadBytes);
    }

    Result<DeltaApplyResult> applyColdBootstrap(const ColdBootstrapSnapshot& snapshot,
                                                std::string_view authenticatedWitness,
                                                std::size_t maxRecords,
                                                std::size_t maxPayloadBytes) {
        Result<DeltaApplyResult> result;
        {
            std::lock_guard<std::mutex> lock(loopMutex_);
            result = loop_.applyColdBootstrap(snapshot, authenticatedWitness, maxRecords,
                                              maxPayloadBytes);
            if (result) {
                refreshCommittedState();
            }
        }
        if (result && result.value().merged != 0) {
            invokeAfterSyncCallback();
        }
        return result;
    }

    Result<DeltaApplyResult> applyDeltas(std::span<const MemoryDelta> deltas) {
        Result<DeltaApplyResult> result;
        bool quarantineChanged = false;
        {
            std::lock_guard<std::mutex> lock(loopMutex_);
            const auto generation = loop_.durableQuarantineGeneration();
            result = loop_.applyDeltas(deltas);
            quarantineChanged = generation != loop_.durableQuarantineGeneration();
            if (result || quarantineChanged) {
                refreshCommittedState();
            }
        }
        if ((result && result.value().merged != 0) || quarantineChanged) {
            invokeAfterSyncCallback();
        }
        return result;
    }

    Result<bool> quarantineWriter(std::string_view writerId, std::string_view sourceNodeId) {
        Result<bool> result;
        {
            std::lock_guard<std::mutex> lock(loopMutex_);
            result = loop_.quarantineWriter(writerId, sourceNodeId);
            if (result && result.value()) {
                refreshCommittedState();
            }
        }
        if (result && result.value()) {
            invokeAfterSyncCallback();
        }
        return result;
    }

    ReplicationState replicationState() const {
        const auto state = committedSnapshot();
        return state ? state->replication : ReplicationState{};
    }

    Result<WriterHistoryCommitment> localHistoryCommitmentAt(std::uint64_t counter) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.localHistoryCommitmentAt(counter);
    }

    Result<WriterHistoryCommitment> localHistoryWindowAfter(std::uint64_t peerCounter,
                                                            std::size_t maxRecords,
                                                            std::size_t maxWireBytes) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.localHistoryWindowAfter(peerCounter, maxRecords, maxWireBytes);
    }

    Result<void> validateHistoryExtension(std::span<const MemoryDelta> deltas,
                                          const WriterHistoryCommitment& expectedFrontier) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.validateHistoryExtension(deltas, expectedFrontier);
    }

    VersionVector currentVersion() const { return replicationState().version; }

#ifdef YAMS_TESTING
    /// Observe periodic reconciliation without triggering another sync cycle.
    bool testingHasMergedRecord(std::string_view key) const {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.hasMergedRecord(key);
    }
#endif

private:
    struct CallbackContext {
        const MemorySyncService* owner;
        const std::map<std::string, MemoryIndexRecord>* snapshot;
        CallbackContext* previous;
    };

    const std::map<std::string, MemoryIndexRecord>* activeCallbackSnapshot() const noexcept {
        for (auto* context = callbackContext_; context != nullptr; context = context->previous) {
            if (context->owner == this) {
                return context->snapshot;
            }
        }
        return nullptr;
    }

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

    void invokeAfterSyncCallback() noexcept {
        if (activeCallbackSnapshot() != nullptr) {
            callbackPending_.store(true, std::memory_order_release);
            return; // The outer callback coalesces recursive callback-side publications.
        }
        const auto callbackGeneration = callbackGeneration_.load(std::memory_order_acquire);
        while (!callbackExecutionMutex_.try_lock()) {
            if (stop_.load(std::memory_order_acquire) ||
                callbackGeneration != callbackGeneration_.load(std::memory_order_acquire)) {
                return;
            }
            std::this_thread::yield();
        }
        std::unique_lock<std::recursive_mutex> executionLock(callbackExecutionMutex_,
                                                             std::adopt_lock);
        if (callbackGeneration != callbackGeneration_.load(std::memory_order_acquire)) {
            return;
        }
        for (;;) {
            callbackPending_.store(false, std::memory_order_release);
            std::function<void()> callback;
            {
                std::lock_guard<std::mutex> lock(callbackMutex_);
                callback = afterSyncCallback_;
            }
            if (!callback || stop_.load(std::memory_order_acquire)) {
                return;
            }
            std::map<std::string, MemoryIndexRecord> adapterSnapshot;
            {
                std::lock_guard<std::mutex> lock(loopMutex_);
                adapterSnapshot = loop_.adapterState();
            }
            CallbackContext context{this, &adapterSnapshot, callbackContext_};
            callbackContext_ = &context;
            try {
                callback();
            } catch (...) {
                // Adapter failures are isolated from the convergence/session worker.
                callbackFailures_.fetch_add(1, std::memory_order_relaxed);
            }
            callbackContext_ = context.previous;
            if (!callbackPending_.exchange(false, std::memory_order_acq_rel) ||
                stop_.load(std::memory_order_acquire)) {
                return;
            }
        }
    }

    void workerLoop() {
        workerIdHash_.store(std::hash<std::thread::id>{}(std::this_thread::get_id()),
                            std::memory_order_release);
        while (!stop_.load(std::memory_order_acquire)) {
            Result<std::map<std::string, MemoryIndexRecord>> snapshot =
                Error{ErrorCode::InternalError, "memory sync did not reconcile"};
            bool quarantineChanged = false;
            {
                std::lock_guard<std::mutex> lock(loopMutex_);
                const auto generation = loop_.durableQuarantineGeneration();
                try {
                    snapshot = loop_.sync();
                    if (snapshot) {
                        auto replayed = loop_.replayReadyErases();
                        if (!replayed) {
                            snapshot = replayed.error();
                        } else if (replayed.value() != 0) {
                            snapshot = loop_.syncFully();
                        }
                    }
                } catch (const std::exception& e) {
                    snapshot = Error{ErrorCode::InternalError,
                                     std::string("memory sync reconciliation threw: ") + e.what()};
                } catch (...) {
                    snapshot = Error{ErrorCode::InternalError,
                                     "memory sync reconciliation threw an unknown exception"};
                }
                if (loop_.legacyUnauthenticatedHistoryObserved()) {
                    legacyUnauthenticatedHistory_.store(true, std::memory_order_release);
                    fullRecoveryCompleted_.store(false, std::memory_order_release);
                }
                quarantineChanged = generation != loop_.durableQuarantineGeneration();
                if (snapshot || quarantineChanged) {
                    refreshCommittedState();
                }
            }
            bool callbackInvoked = false;
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
                    invokeAfterSyncCallback();
                    callbackInvoked = true;
                }
            } else {
                failedSyncCycles_.fetch_add(1, std::memory_order_relaxed);
            }
            if (quarantineChanged && !callbackInvoked) {
                invokeAfterSyncCallback();
            }
            std::unique_lock<std::mutex> sleepLock(sleepMutex_);
            sleepCv_.wait_for(sleepLock, std::chrono::milliseconds(config_.syncIntervalMs),
                              [this] { return stop_.load(std::memory_order_acquire); });
        }
        workerRunning_.store(false, std::memory_order_release);
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
    std::atomic<bool> workerRunning_{false};
    std::atomic<std::size_t> workerIdHash_{0};
    mutable std::mutex lifecycleMutex_;
    std::mutex sleepMutex_;
    std::condition_variable sleepCv_;
    inline static thread_local CallbackContext* callbackContext_{nullptr};
    // Serializes all MemorySyncLoop access (worker sync vs publish/read/syncOnce);
    // the loop keeps mutable version-vector/logical-clock/merged state.
    mutable std::mutex loopMutex_;
    mutable std::mutex callbackMutex_;
    // Serialize adapter callbacks process-wide: callbacks are arbitrary and can call between
    // services, so per-service locks permit an A↔B ABBA deadlock. Recursive acquisition permits
    // same-thread A→B nesting while CallbackContext coalesces A→B→A re-entry.
    inline static std::recursive_mutex callbackExecutionMutex_;
    std::atomic<std::uint64_t> callbackGeneration_{0};
    std::atomic<bool> callbackPending_{false};
    std::function<void()> afterSyncCallback_;
    std::atomic<std::uint64_t> callbackFailures_{0};
    std::atomic<std::uint64_t> shutdownFailures_{0};
    std::atomic<std::uint64_t> successfulSyncCycles_{0};
    std::atomic<std::uint64_t> failedSyncCycles_{0};
    std::atomic<std::int64_t> lastSuccessfulSyncSteadyMs_{0};
    std::atomic<bool> namespaceCleaned_{false};
    std::atomic<bool> fullRecoveryCompleted_{false};
    std::atomic<bool> legacyUnauthenticatedHistory_{false};
    mutable std::mutex snapshotMutex_;
    std::shared_ptr<const MemorySyncLoop::CommittedState> committed_;
};

} // namespace yams::memory_sync
