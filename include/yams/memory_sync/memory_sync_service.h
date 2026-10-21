// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <typeinfo>
#include <utility>
#include <vector>

// pi-lens-ignore: fatal error
#include <yams/core/types.h>
#include <yams/memory_sync/lifecycle_drain.h>
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
    bool exclusiveBackend{false}; /// only this service writes the backend (direct op store)

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
    struct AdapterLifetime {
        std::shared_mutex mutex;
        MemorySyncService* owner;
        explicit AdapterLifetime(MemorySyncService* service) : owner(service) {}
    };

public:
    /// Defer checks every work unit and returns ResourceBusy for local pressure/contention
    /// instead of sleeping. Target transactions can roll back and retry outside the batch.
    /// Capacity and remote-admission errors retain their original codes.
    enum class AdapterPressureMode : std::uint8_t { Wait, Defer };

    /// An opaque pinned adapter generation. It owns its metadata, not a service/backend
    /// reference; payload I/O always requires the still-live originating service.
    class AdapterView {
        friend class MemorySyncService;
        using Rows = std::map<std::string, MemoryIndexRecord>;
        std::shared_ptr<const MemorySyncLoop::CommittedState> state_;
        std::shared_ptr<const Rows> overlays_;
        std::shared_ptr<const std::byte> owner_;
        AdapterPressureMode pressureMode_;

        AdapterView(std::shared_ptr<const MemorySyncLoop::CommittedState> state,
                    std::shared_ptr<const Rows> overlays, std::shared_ptr<const std::byte> owner,
                    AdapterPressureMode pressureMode)
            : state_(std::move(state)), overlays_(std::move(overlays)), owner_(std::move(owner)),
              pressureMode_(pressureMode) {}

        Result<MemoryIndexRecord> find(std::string_view key) const {
            if (state_->winners) {
                auto record = state_->winners->find(key);
                if (record || record.error().code != ErrorCode::NotFound) {
                    return record;
                }
            }
            const auto found = overlays_->find(std::string(key));
            if (found == overlays_->end()) {
                return Error{ErrorCode::NotFound, "no record in the pinned adapter view"};
            }
            return found->second;
        }

        Result<MemorySyncLoop::WinnerPage> page(std::string_view prefix, std::string_view afterKey,
                                                std::size_t limit) const {
            MemorySyncLoop::WinnerPage disk;
            if (state_->winners) {
                auto selected = state_->winners->page(prefix, afterKey, limit);
                if (!selected) {
                    return selected.error();
                }
                disk = std::move(selected.value());
            }
            const std::string start =
                afterKey < prefix ? std::string(prefix) : std::string(afterKey);
            auto overlay = overlays_->lower_bound(start);
            if (overlay != overlays_->end() && overlay->first == afterKey) {
                ++overlay;
            }
            auto hasOverlay = [&] {
                return overlay != overlays_->end() && overlay->first.starts_with(prefix);
            };
            auto row = disk.begin();
            MemorySyncLoop::WinnerPage out;
            while (out.size() < limit && (row != disk.end() || hasOverlay())) {
                if (row != disk.end() && (!hasOverlay() || row->first <= overlay->first)) {
                    if (hasOverlay() && row->first == overlay->first) {
                        ++overlay;
                    }
                    out.push_back(std::move(*row++));
                } else {
                    out.emplace_back(overlay->first, overlay->second);
                    ++overlay;
                }
            }
            return out;
        }
    };

    /// Admission/pressure probes must be thread-safe: pinned readers and the worker may
    /// cooperate concurrently. Probes never run under the cooperation-state mutex.
    MemorySyncService(std::unique_ptr<storage::IStorageBackend> backend, MemorySyncConfig config,
                      std::function<bool()> canAdmitRemoteWork = {},
                      bool allowLegacyUnbound = false,
                      std::unique_ptr<storage::IStorageBackend> sessionMaintenanceBackend = {},
                      std::string sessionLeaseKey = {},
                      std::function<bool()> shouldYieldLocalWork = {})
        : backend_(std::move(backend)),
          sessionMaintenanceBackend_(std::move(sessionMaintenanceBackend)),
          sessionLeaseKey_(std::move(sessionLeaseKey)), config_(std::move(config)),
          shouldYieldLocalWork_(std::move(shouldYieldLocalWork)),
          loop_(*backend_, config_.nodeId, config_.corpusId, config_.corpusEpoch,
                allowLegacyUnbound, config_.limits,
                MemorySyncControl{
                    .isCancelled = [this] { return stop_.load(std::memory_order_acquire); },
                    .canAdmitRemoteWork = std::move(canAdmitRemoteWork),
                    .exclusiveBackend = config_.exclusiveBackend,
                    .cooperate = [this] { return cooperateWithPressure(); }},
                {}, config_.writerAuth, config_.controlScope) {}

    ~MemorySyncService() {
        stop();
        // Index handles may outlive the service. Finish active admission checks, then
        // revoke their owner before any referenced service members are destroyed.
        std::unique_lock lifetimeLock(adapterLifetime_->mutex);
        adapterLifetime_->owner = nullptr;
    }

    MemorySyncService(const MemorySyncService&) = delete;
    MemorySyncService& operator=(const MemorySyncService&) = delete;

    /// Start the periodic sync worker (idempotent).
    Result<void> start() {
        if (activeCallbackSnapshot() != nullptr) {
            return Error{ErrorCode::InvalidState,
                         "memory sync cannot restart from its active callback"};
        }
        std::unique_lock<std::mutex> lifecycleLock(lifecycleMutex_);
        const bool restarting = stop_.load(std::memory_order_acquire);
        std::unique_lock<std::recursive_mutex> callbackDrain(callbackExecutionMutex_,
                                                             std::defer_lock);
        if (restarting) {
            callbackDrain = detail::acquireCallbackDrain(lifecycleLock, callbackExecutionMutex_);
        }
        // Draining temporarily releases lifecycle ownership. Another start may
        // already have supplied a live worker; never overwrite its joinable thread.
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
        std::unique_lock<std::mutex> lifecycleLock(lifecycleMutex_);
        const auto cancelAndJoin = [this] {
            stop_.store(true, std::memory_order_release);
            backend_->requestCancel();
            sleepCv_.notify_all();
            if (worker_.joinable()) {
                try {
                    worker_.join();
                } catch (...) {
                    shutdownFailures_.fetch_add(1, std::memory_order_relaxed);
                }
            }
        };
        // A concurrent start may have acquired lifecycle ownership after the first request.
        cancelAndJoin();
        // Direct-session callbacks do not belong to worker_. Release lifecycle ownership
        // while draining them, then reclaim it under the process-wide callback gate.
        auto callbackDrain = detail::acquireCallbackDrain(lifecycleLock, callbackExecutionMutex_);
        // A restart may have linearized in that gap. Revoke its queued callbacks and
        // cancel/join that generation as well before cleanup or returning to the owner.
        callbackGeneration_.fetch_add(1, std::memory_order_acq_rel);
        cancelAndJoin();
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
#ifdef YAMS_TESTING
    /// Deterministic pressure-wait observation without changing the service instance layout.
    static std::function<void()>*
    testingSetPressurePauseObserver(std::function<void()>* observer) noexcept {
        return std::exchange(testingPressurePauseObserver_, observer);
    }
    void testingExpireWorkQuantum() {
        std::lock_guard<std::mutex> lock(cooperationMutex_);
        nextCooperativeCheck_ = {};
    }
#endif
    [[nodiscard]] const std::string& localNodeId() const noexcept { return config_.nodeId; }
    /// Immutable per-value bound; a single value must also fit the resident cache.
    [[nodiscard]] std::size_t maxPublishValueBytes() const noexcept {
        return std::min(config_.limits.maxValueBytes, config_.limits.maxCacheBytes);
    }
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

    /// Atomic against SDK ingress/writes: an obsolete observation never reserves a
    /// tombstone counter or deletes a newer winner. Existing erase intents are not stolen.
    Result<bool> eraseIfCurrent(std::string_view key, const MemoryIndexRecord& expected,
                                std::string tombstonePayload = {}) {
        Result<bool> result;
        bool quarantineChanged = false;
        {
            std::lock_guard<std::mutex> lock(loopMutex_);
            const auto generation = loop_.durableQuarantineGeneration();
            result = loop_.eraseIfCurrent(key, expected, std::move(tombstonePayload));
            quarantineChanged = generation != loop_.durableQuarantineGeneration();
            if ((result && result.value()) || quarantineChanged) {
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

    /// Publish only when the committed content digest differs, even after cache eviction.
    /// This makes periodic
    /// corpus backfill safe without creating a new causal envelope every cycle.
    Result<bool> publishIfChanged(std::string_view key, std::span<const std::byte> content) {
        Result<bool> result;
        bool quarantineChanged = false;
        {
            std::lock_guard<std::mutex> lock(loopMutex_);
            auto matches = loop_.matchesCommittedValue(key, content);
            if (!matches) {
                return matches.error();
            }
            if (matches.value()) {
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

    /// Explicit on-demand I/O against the last committed generation; never starts a sweep.
    /// Cache warming does not copy/publish a service snapshot per item. Writes and the
    /// periodic worker continue to publish the nonblocking cache-only IPC view.
    Result<std::vector<std::byte>> readCommitted(std::string_view key) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.readCommitted(key);
    }

    /// Point read without a sweep or service snapshot copy. An obsolete envelope is
    /// represented by an empty optional; missing/corrupt payload and quarantine stay errors.
    Result<std::optional<std::vector<std::byte>>>
    readCommittedIfCurrent(std::string_view key, const MemoryIndexRecord& expected) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        if (expected.logicalKey != key) {
            return Error{ErrorCode::InvalidArgument,
                         "conditional read key does not match envelope"};
        }
        if (auto ready = loop_.beforeRemoteWork(); !ready) {
            return ready.error();
        }
        if (expected.isTombstone()) {
            return std::optional<std::vector<std::byte>>{};
        }
        if (auto valid = loop_.validatePinnedPayloadRead(expected); !valid) {
            return valid.error();
        }
        auto current = loop_.committedWinner(key);
        if (!current) {
            if (current.error().code == ErrorCode::NotFound) {
                return std::optional<std::vector<std::byte>>{};
            }
            return current.error();
        }
        if (current.value() != expected) {
            return std::optional<std::vector<std::byte>>{};
        }
        auto payload = loop_.readCommitted(key);
        if (!payload) {
            return payload.error();
        }
        return std::optional<std::vector<std::byte>>{std::move(payload.value())};
    }

    /// Reconcile once outside callbacks, or reuse the callback's pinned generation. Subsequent
    /// pages and payload reads do not reconcile again or copy the service's value cache.
    Result<AdapterView>
    captureAdapterView(AdapterPressureMode pressureMode = AdapterPressureMode::Wait) {
        if (pressureMode != AdapterPressureMode::Wait &&
            pressureMode != AdapterPressureMode::Defer) {
            return Error{ErrorCode::InvalidArgument, "invalid adapter pressure policy"};
        }
        if (pressureMode == AdapterPressureMode::Defer) {
            if (auto ready = beforeAdapterWork(pressureMode); !ready) {
                return ready.error();
            }
        }
        for (auto* context = callbackContext_; context != nullptr; context = context->previous) {
            if (context->owner == this) {
                return AdapterView{context->state, context->snapshot, adapterIdentity_,
                                   pressureMode};
            }
        }
        if (auto recovered = syncFully(); !recovered) {
            return recovered.error();
        }
        std::lock_guard<std::mutex> lock(loopMutex_);
        return AdapterView{committedSnapshot(),
                           std::make_shared<const AdapterView::Rows>(loop_.adapterState()),
                           adapterIdentity_, pressureMode};
    }

    /// Check admission/cancellation for a pinned pass without starting a new sweep.
    /// Defer callers can use this between target work units and immediately before commit.
    Result<void> checkAdapterWork(const AdapterView& view) {
        if (view.owner_ != adapterIdentity_) {
            return Error{ErrorCode::InvalidArgument, "adapter view belongs to another service"};
        }
        return beforeAdapterWork(view.pressureMode_);
    }

    /// Allocate disposable, disk-backed bookkeeping for a pinned adapter pass. It shares
    /// the native corpus scratch/lease quota but never changes winner or value-cache limits.
    /// No mutable loop lock is held during admission, pressure waits or workspace creation.
    /// Index operations remain admission/cancellation-bound to this service; escaped
    /// handles safely return OperationCancelled after owner destruction.
    Result<std::shared_ptr<MemorySyncLoop::AdapterIndex>>
    createAdapterIndex(const AdapterView& view) {
        if (auto ready = checkAdapterWork(view); !ready) {
            return ready.error();
        }
        std::filesystem::path directory;
        if (typeid(*backend_) == typeid(storage::FilesystemBackend)) {
            directory = static_cast<storage::FilesystemBackend&>(*backend_).scratchDirectory();
        }
        const auto rowBytes =
            std::max<std::size_t>(4096, std::min<std::size_t>(config_.limits.maxEnvelopeBytes,
                                                              std::numeric_limits<int>::max() / 4) *
                                            4);
        auto workCheck = [lifetime = adapterLifetime_,
                          mode = view.pressureMode_]() -> Result<void> {
            std::shared_lock<std::shared_mutex> lock(lifetime->mutex, std::defer_lock);
            if (mode == AdapterPressureMode::Defer) {
                if (!lock.try_lock()) {
                    return Error{ErrorCode::ResourceExhausted, "adapter owner is draining"};
                }
            } else {
                lock.lock();
            }
            if (lifetime->owner == nullptr) {
                return Error{ErrorCode::OperationCancelled, "adapter owner was destroyed"};
            }
            return lifetime->owner->beforeAdapterWork(mode);
        };
        return MemorySyncLoop::createAdapterIndex(directory, config_.limits.maxScanBytes, rowBytes,
                                                  std::move(workCheck));
    }

    /// Stream bounded, ordered pages from one immutable generation. Visitor references are
    /// valid only during that call. Local pressure/cancellation is checked between work units.
    template <typename Visitor>
    Result<void> visitAdapterWinners(const AdapterView& view, std::string_view prefix,
                                     Visitor&& visitor) {
        if (view.owner_ != adapterIdentity_) {
            return Error{ErrorCode::InvalidArgument, "adapter view belongs to another service"};
        }
        const auto limit = std::min<std::size_t>(config_.limits.maxIndexObjectsPerSync, 64);
        if (limit == 0) {
            return Error{ErrorCode::ResourceExhausted, "adapter page budget is zero"};
        }
        const auto cooperate = [this, mode = view.pressureMode_]() -> Result<void> {
            return cooperateWithPressure(mode);
        };
        std::string cursor;
        for (;;) {
            if (auto ready = cooperate(); !ready) {
                return ready.error();
            }
            auto page = view.page(prefix, cursor, limit);
            if (!page) {
                return page.error();
            }
            if (page.value().empty()) {
                return {};
            }
            for (const auto& [key, record] : page.value()) {
                if (key <= cursor || !key.starts_with(prefix)) {
                    return Error{ErrorCode::InvalidData, "adapter cursor did not advance"};
                }
                if (auto ready = cooperate(); !ready) {
                    return ready.error();
                }
                if (auto visited = visitor(key, record); !visited) {
                    return visited.error();
                }
                cursor = key;
            }
        }
    }

    /// Read exactly the selected generation, not whichever winner is current by the time a
    /// later page applies. A missing/collected historical blob is an error, never a newer value.
    Result<std::vector<std::byte>> readAdapterPayload(const AdapterView& view,
                                                      std::string_view key) {
        if (view.owner_ != adapterIdentity_) {
            return Error{ErrorCode::InvalidArgument, "adapter view belongs to another service"};
        }
        // Cooperate before taking the mutable loop lock, including on cache hits.
        if (auto ready = beforeAdapterWork(view.pressureMode_); !ready) {
            return ready.error();
        }
        MemoryIndexRecord record;
        {
            auto lock = lockAdapterState(view.pressureMode_);
            if (!lock) {
                return lock.error();
            }
            auto selected = view.find(key);
            if (!selected) {
                return selected.error();
            }
            record = std::move(selected.value());
            const auto cached = view.state_->cachedBlobs.find(record.entryHash);
            auto prepared = loop_.preparePinnedPayload(
                record, cached == view.state_->cachedBlobs.end() ? nullptr : &cached->second);
            if (!prepared) {
                return prepared.error();
            }
            if (prepared.value()) {
                return std::move(*prepared.value());
            }
            // Only the built-in filesystem's independent streams and immutable base path
            // have been audited for concurrent reads/writes. Preserve serialization for
            // unknown wrappers/plugins rather than inventing a backend threading contract.
            if (typeid(*backend_) != typeid(storage::FilesystemBackend)) {
                auto fetched = loop_.fetchPinnedPayload(record);
                if (auto valid = loop_.validatePinnedPayloadRead(record); !valid) {
                    return valid.error();
                }
                return fetched;
            }
        }
        auto fetched = loop_.fetchPinnedPayload(record);
        {
            auto lock = lockAdapterState(view.pressureMode_);
            if (!lock) {
                return lock.error();
            }
            // A durable quarantine or stop may have completed during native payload I/O.
            // Never return bytes merely because the old pinned record was once authorized.
            if (auto valid = loop_.validatePinnedPayloadRead(record); !valid) {
                return valid.error();
            }
        }
        return fetched;
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

    /// Up to `limit` committed winners whose logical key starts with `prefix` and sorts after
    /// `afterKey`, in key order. The cursor is a logical key, not a position, so a caller paging
    /// through the winners neither skips nor repeats a key when records are published or merged
    /// between pages.
    [[nodiscard]] Result<MemorySyncLoop::WinnerPage>
    readCommittedWinnersAfter(std::string_view prefix, std::string_view afterKey,
                              std::size_t limit) const {
        MemorySyncLoop::WinnerPage page;
        const auto state = committedSnapshot();
        if (!state || limit == 0) {
            return page;
        }
        if (state->winners) {
            return state->winners->page(prefix, afterKey, limit);
        }
        const std::string start = afterKey < prefix ? std::string(prefix) : std::string(afterKey);
        auto it = state->merged.lower_bound(start);
        if (it != state->merged.end() && it->first == afterKey) {
            ++it;
        }
        for (; it != state->merged.end() && page.size() < limit; ++it) {
            if (!it->first.starts_with(prefix)) {
                break;
            }
            page.emplace_back(it->first, it->second);
        }
        return page;
    }

    /// Compatibility wrapper: disk errors must not masquerade as an exhausted cursor.
    [[nodiscard]] MemorySyncLoop::WinnerPage committedWinnersAfter(std::string_view prefix,
                                                                   std::string_view afterKey,
                                                                   std::size_t limit) const {
        auto page = readCommittedWinnersAfter(prefix, afterKey, limit);
        if (!page) {
            throw std::runtime_error(page.error().message);
        }
        return std::move(page.value());
    }

    std::size_t mergedRecordCount() const noexcept {
        const auto state = committedSnapshot();
        return state ? (state->winners ? state->winners->size() : state->merged.size()) : 0;
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

    Result<bool> quarantineWriter(std::string_view writerId, std::string_view sourceNodeId,
                                  std::string_view reason) {
        Result<bool> result;
        {
            std::lock_guard<std::mutex> lock(loopMutex_);
            result = loop_.quarantineWriter(writerId, sourceNodeId, reason);
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

    Result<WriterHistoryCommitment> historyCommitmentAt(std::string_view writerId,
                                                        std::uint64_t counter) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.historyCommitmentAt(writerId, counter);
    }

    Result<std::optional<WriterHistoryViolation>>
    validateHistoryExtension(std::string_view writerId, std::span<const MemoryDelta> deltas,
                             const WriterHistoryCommitment& expectedFrontier) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.validateHistoryExtension(writerId, deltas, expectedFrontier);
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
        std::shared_ptr<const std::map<std::string, MemoryIndexRecord>> snapshot;
        std::shared_ptr<const MemorySyncLoop::CommittedState> state;
        CallbackContext* previous;
    };

    const std::map<std::string, MemoryIndexRecord>* activeCallbackSnapshot() const noexcept {
        for (auto* context = callbackContext_; context != nullptr; context = context->previous) {
            if (context->owner == this) {
                return context->snapshot.get();
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
            std::shared_ptr<const std::map<std::string, MemoryIndexRecord>> adapterSnapshot;
            std::shared_ptr<const MemorySyncLoop::CommittedState> state;
            {
                std::lock_guard<std::mutex> lock(loopMutex_);
                adapterSnapshot = std::make_shared<const AdapterView::Rows>(loop_.adapterState());
                state = committedSnapshot();
            }
            CallbackContext context{this, std::move(adapterSnapshot), std::move(state),
                                    callbackContext_};
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

    Result<std::unique_lock<std::mutex>> lockAdapterState(AdapterPressureMode mode) {
        std::unique_lock<std::mutex> lock(loopMutex_, std::defer_lock);
        if (mode == AdapterPressureMode::Defer) {
            if (auto ready = cooperateWithPressure(mode); !ready) {
                return ready.error();
            }
            // Do not queue behind another caller's pressure-paced scan while a target
            // transaction is open. Retry the entire atomic adapter pass outside it.
            if (!lock.try_lock()) {
                if (stop_.load(std::memory_order_acquire)) {
                    return Error{ErrorCode::OperationCancelled, "adapter work cancelled"};
                }
                return Error{ErrorCode::ResourceBusy, "adapter mutable state is busy"};
            }
        } else {
            lock.lock();
        }
        return lock;
    }

    Result<void> beforeAdapterWork(AdapterPressureMode mode) {
        if (mode == AdapterPressureMode::Wait) {
            return loop_.beforeRemoteWork();
        }
        if (auto ready = cooperateWithPressure(mode); !ready) {
            return ready.error();
        }
        return loop_.checkWorkAdmission();
    }

    Result<void> cooperateWithPressure(AdapterPressureMode mode = AdapterPressureMode::Wait) {
        if (stop_.load(std::memory_order_acquire)) {
            return Error{ErrorCode::OperationCancelled, "memory sync background work cancelled"};
        }
        if (!shouldYieldLocalWork_) {
            return {};
        }
        if (mode == AdapterPressureMode::Defer) {
            if (shouldYieldLocalWork_()) {
                return Error{ErrorCode::ResourceBusy, "adapter work deferred by local pressure"};
            }
            return {};
        }
        constexpr auto kWorkQuantum = std::chrono::milliseconds(2);
        constexpr auto kPressurePause = std::chrono::milliseconds(20);
        const auto now = std::chrono::steady_clock::now();
        {
            std::lock_guard<std::mutex> lock(cooperationMutex_);
            if (now < nextCooperativeCheck_) {
                return {};
            }
            nextCooperativeCheck_ = now + kWorkQuantum;
        }
        if (!shouldYieldLocalWork_()) {
            return {};
        }
        // Preserve the scan cursor and authenticated scratch identities across pressure.
        // Bounded pauses admit useful progress without spinning or holding shutdown hostage.
#ifdef YAMS_TESTING
        if (testingPressurePauseObserver_ != nullptr) {
            (*testingPressurePauseObserver_)();
        }
#endif
        std::unique_lock<std::mutex> lock(sleepMutex_);
        if (sleepCv_.wait_for(lock, kPressurePause,
                              [this] { return stop_.load(std::memory_order_acquire); })) {
            return Error{ErrorCode::OperationCancelled, "memory sync background work cancelled"};
        }
        lock.unlock();
        {
            std::lock_guard<std::mutex> stateLock(cooperationMutex_);
            nextCooperativeCheck_ =
                std::max(nextCooperativeCheck_, std::chrono::steady_clock::now() + kWorkQuantum);
        }
        return {};
    }

    void workerLoop() {
        workerIdHash_.store(std::hash<std::thread::id>{}(std::this_thread::get_id()),
                            std::memory_order_release);
        while (!stop_.load(std::memory_order_acquire)) {
            Result<std::map<std::string, MemoryIndexRecord>> snapshot =
                Error{ErrorCode::InternalError, "memory sync did not reconcile"};
            bool quarantineChanged = false;
            bool completedSweep = false;
            {
                std::lock_guard<std::mutex> lock(loopMutex_);
                const auto generation = loop_.durableQuarantineGeneration();
                try {
                    snapshot = loop_.sync();
                    if (snapshot && !loop_.sweepInProgress()) {
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
                completedSweep = snapshot && !loop_.sweepInProgress();
                if (loop_.legacyUnauthenticatedHistoryObserved()) {
                    legacyUnauthenticatedHistory_.store(true, std::memory_order_release);
                    fullRecoveryCompleted_.store(false, std::memory_order_release);
                } else if (completedSweep) {
                    fullRecoveryCompleted_.store(true, std::memory_order_release);
                }
                quarantineChanged = generation != loop_.durableQuarantineGeneration();
                if (completedSweep || quarantineChanged) {
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
                } else if (completedSweep) {
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
    const std::shared_ptr<const std::byte> adapterIdentity_{std::make_shared<const std::byte>()};
    const std::shared_ptr<AdapterLifetime> adapterLifetime_{
        std::make_shared<AdapterLifetime>(this)};
    std::function<bool()> shouldYieldLocalWork_;
    std::mutex cooperationMutex_; // Protects the quantum only, never held across a probe/wait.
    std::chrono::steady_clock::time_point nextCooperativeCheck_{};
#ifdef YAMS_TESTING
    inline static thread_local std::function<void()>* testingPressurePauseObserver_{nullptr};
#endif
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
