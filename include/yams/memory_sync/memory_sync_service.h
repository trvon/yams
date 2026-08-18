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
                MemorySyncControl{[this] { return stop_.load(std::memory_order_acquire); },
                                  std::move(canAdmitRemoteWork)},
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
        return loop_.publish(key, content);
    }

    Result<void> erase(std::string_view key, std::string tombstonePayload = {}) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.erase(key, std::move(tombstonePayload));
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
        return true;
    }

    Result<std::vector<std::byte>> read(std::string_view key) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.read(key);
    }

    /// Return a value observed by the periodic worker without initiating sync.
    Result<std::vector<std::byte>> readCached(std::string_view key) const {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.readCached(key);
    }

    std::size_t mergedRecordCount() const noexcept {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.mergedRecordCount();
    }

    std::size_t quarantinedRecordCount() const noexcept {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.quarantinedRecordCount();
    }

    std::size_t authFailureCount() const noexcept {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.authFailureCount();
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
        if (callbackOwner_ == this && callbackSnapshot_) {
            return *callbackSnapshot_;
        }
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.sync();
    }

#ifdef YAMS_TESTING
    /// Observe periodic reconciliation without triggering another sync cycle.
    bool testingHasMergedRecord(std::string_view key) const {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.hasMergedRecord(key);
    }
#endif

private:
    void workerLoop() {
        workerIdHash_.store(std::hash<std::thread::id>{}(std::this_thread::get_id()),
                            std::memory_order_release);
        while (!stop_.load(std::memory_order_acquire)) {
            Result<std::map<std::string, MemoryIndexRecord>> snapshot =
                Error{ErrorCode::InternalError, "memory sync did not reconcile"};
            try {
                std::lock_guard<std::mutex> lock(loopMutex_);
                snapshot = loop_.sync();
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
                    std::function<void()> callback;
                    {
                        std::lock_guard<std::mutex> lock(callbackMutex_);
                        callback = afterSyncCallback_;
                    }
                    if (callback && !stop_.load(std::memory_order_acquire)) {
                        callbackOwner_ = this;
                        callbackSnapshot_ = &snapshot.value();
                        try {
                            callback();
                        } catch (...) {
                            // A consumer failure must not terminate the convergence worker.
                            callbackFailures_.fetch_add(1, std::memory_order_relaxed);
                        }
                        callbackSnapshot_ = nullptr;
                        callbackOwner_ = nullptr;
                    }
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
};

} // namespace yams::memory_sync
