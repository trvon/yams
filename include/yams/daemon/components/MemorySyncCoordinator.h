// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/core/types.h>
#include <yams/memory_sync/memory_sync_service.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace yams::api {
class IContentStore;
}
namespace yams::metadata {
class MetadataRepository;
class KnowledgeGraphStore;
} // namespace yams::metadata
namespace yams::vector {
class VectorDatabase;
}

namespace yams::daemon::p2p {
class P2pManager;
}

namespace yams::daemon {

struct DaemonConfig;
class VectorIndexCoordinator;

/**
 * Owns the P2P memory-sync service and everything the daemon layers on top of it: the ordered
 * apply pipeline that lands replicated winners into the local stores, the document-delete
 * outbox, the bounded backfill publisher, and the stage observers tests hook into.
 * ServiceManager constructs one, forwards its public memory-sync API to it, and stops it before
 * worker teardown. Everything it needs from the daemon arrives through Dependencies.
 */
class MemorySyncCoordinator {
public:
    struct Dependencies {
        const DaemonConfig* config{nullptr};
        std::function<std::shared_ptr<api::IContentStore>()> getContentStore;
        std::function<std::shared_ptr<metadata::MetadataRepository>()> getMetadataRepo;
        std::function<std::shared_ptr<metadata::KnowledgeGraphStore>()> getKgStore;
        std::function<std::shared_ptr<vector::VectorDatabase>()> getVectorDatabase;
        std::function<std::shared_ptr<VectorIndexCoordinator>()> getVectorIndexCoordinator;
        std::function<p2p::P2pManager*()> getP2pManager; // may return nullptr
        std::function<void(const std::string& hash, const std::string& mime)> enqueuePostIngest;
    };

    struct MemorySyncStatus {
        bool started{false};
        std::uint64_t records{0};
        std::uint64_t quarantinedRecords{0};
        std::uint64_t authFailures{0};
        std::uint64_t successfulCycles{0};
        std::uint64_t failedCycles{0};
        std::uint64_t lastSuccessAgeMs{0};
        std::string backend;
        std::string nodeId;
        std::string corpusId;
        std::uint64_t corpusEpoch{0};
        std::string mode;
        std::string trustMode;
        std::uint64_t peerCount{0};
    };

    explicit MemorySyncCoordinator(Dependencies deps);

    // ── Lifecycle (called by ServiceManager in startup/shutdown order) ──
    Result<void> initializeMemorySync(const std::filesystem::path& dataDir);
    Result<void> configureMemorySyncApply();
    /// Stop and release the sync worker. Safe to call when never started.
    void shutdown();

    yams::memory_sync::MemorySyncService* service() const noexcept { return memorySync_.get(); }

    // ── Public memory-sync API (forwarded by ServiceManager) ──
    Result<void> publishMemorySync(const std::string& key, const std::string& value);
    Result<void> deleteMemorySync(const std::string& key);
    Result<std::string> readMemorySyncCached(const std::string& key) const;
    Result<MemorySyncStatus> getMemorySyncStatus() const;
    Result<void> stageMemorySyncDocumentDelete(std::string_view contentHash,
                                               bool retainContent = false);
    Result<void> publishMemorySyncDocumentDelete(std::string_view contentHash,
                                                 bool retainContent = false);

    // ── Test hooks (mirrored by ServiceManager::testing*) ──
    void testingSetMemorySyncService(std::unique_ptr<yams::memory_sync::MemorySyncService> s) {
        memorySync_ = std::move(s);
    }
    void testingSetMemorySyncStageObserver(std::function<void(std::string_view)> observer) {
        std::lock_guard<std::mutex> lock(memorySyncStageObserverMutex_);
        memorySyncStageObserver_ = std::move(observer);
    }
    void testingSetMemorySyncDeleteOutboxObserver(std::function<void(std::string_view)> observer) {
        std::lock_guard<std::mutex> lock(memorySyncDeleteOutboxObserverMutex_);
        memorySyncDeleteOutboxObserver_ = std::move(observer);
    }
    void testingApplyMemorySyncWinners() { applyMemorySyncWinners(); }
    void testingPublishMemorySyncBackfill() { publishMemorySyncBackfill(); }
    void testingSetMemorySyncBackfillItemBudget(std::size_t budget) {
        std::lock_guard<std::mutex> lock(memorySyncBackfillMutex_);
        memorySyncBackfillState_.itemBudgetPerCycle = std::max<std::size_t>(budget, 1);
    }
    bool testingMemorySyncApplyLockHeld() {
        std::unique_lock<std::mutex> lock(memorySyncApplyMutex_, std::try_to_lock);
        return !lock.owns_lock();
    }
    std::uint64_t testingMemorySyncApplyAttempts() const noexcept {
        return memorySyncApplyAttempts_.load(std::memory_order_acquire);
    }
    bool testingMemorySyncBackfillLockHeld() {
        std::unique_lock<std::mutex> lock(memorySyncBackfillMutex_, std::try_to_lock);
        return !lock.owns_lock();
    }
    std::uint64_t testingMemorySyncBackfillAttempts() const noexcept {
        return memorySyncBackfillAttempts_.load(std::memory_order_acquire);
    }

private:
    void applyMemorySyncWinners() noexcept;
    bool drainMemorySyncDocumentDeleteOutbox() noexcept;
    Result<bool> memorySyncDeleteLocallyAbsent(std::string_view contentHash,
                                               memory_sync::EraseReadinessProbe probe) const;
    void publishMemorySyncBackfill() noexcept;
    void notifyMemorySyncStage(std::string_view stage) noexcept;
    void notifyMemorySyncDeleteOutboxStage(std::string_view stage) noexcept;
    Result<std::size_t> applyMemorySyncContentBlobs();

    Dependencies deps_;
    const DaemonConfig& config_;

    std::unique_ptr<yams::memory_sync::MemorySyncService> memorySync_;
    mutable std::mutex memorySyncDeleteOutboxMutex_;
    mutable std::mutex memorySyncDeleteOutboxObserverMutex_;
    std::function<void(std::string_view)> memorySyncDeleteOutboxObserver_;
    mutable std::mutex memorySyncStageObserverMutex_;
    std::function<void(std::string_view)> memorySyncStageObserver_;
    // Direct sessions may finish concurrently. Serialize the daemon adapter pipeline and its
    // vector rebuild state; backfill has a separate lock so focused maintenance calls are safe.
    mutable std::mutex memorySyncApplyMutex_;
    mutable std::mutex memorySyncBackfillMutex_;
    std::atomic<std::uint64_t> memorySyncApplyAttempts_{0};
    std::atomic<std::uint64_t> memorySyncBackfillAttempts_{0};
    bool memorySyncVectorRebuildDirty_{false};
    struct MemorySyncBackfillState {
        enum class Domain { Documents, Vectors, Topology };

        std::int64_t documentIdCursor{0};
        std::string vectorDocumentHashCursor;
        std::string vectorChunkIdCursor;
        bool topologySnapshotInitialized{false};
        std::vector<std::string> topologyNodeTypes;
        std::size_t topologyTypeIndex{0};
        std::unordered_map<std::string, std::size_t> topologyNodeOffsets;
        std::size_t topologyEdgeOffset{0};
        std::int64_t topologyNodeId{0};
        std::string topologyNodeKey;
        bool topologyNodeActive{false};
        Domain nextDomain{Domain::Documents};
        std::size_t itemBudgetPerCycle{256};
        std::chrono::milliseconds timeBudgetPerCycle{100};
    };
    MemorySyncBackfillState memorySyncBackfillState_;
    std::chrono::steady_clock::time_point nextMemorySyncBackfill_{};
};

} // namespace yams::daemon
