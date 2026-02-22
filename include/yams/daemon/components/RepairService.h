#pragma once

#include <yams/daemon/ipc/ipc_protocol.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <unordered_set>
#include <vector>
#include <boost/asio/awaitable.hpp>

namespace yams::metadata {
class KnowledgeGraphStore;
class MetadataRepository;
class IMetadataRepository;
} // namespace yams::metadata

namespace yams::api {
class IContentStore;
} // namespace yams::api

namespace yams::extraction {
class IContentExtractor;
} // namespace yams::extraction

namespace yams::daemon {

class ServiceManager;
struct StateComponent;
class GraphComponent;
class AbiSymbolExtractorAdapter;

/**
 * @brief Centralized repair service that owns all repair logic.
 *
 * Replaces the old RepairCoordinator with a unified daemon-managed
 * repair system.  The CLI sends RepairRequest via RPC; progress is
 * streamed back as RepairEvent messages.
 *
 * Responsibilities:
 *  - Background scan + detect + repair loop (runs continuously)
 *  - On-demand repair triggered by RPC (RepairRequest)
 *  - Stuck-document recovery (failed extraction, ghost success, stalled)
 *  - Delegates to RepairManager for block-level operations
 *  - Delegates to InternalEventBus for embedding / FTS5 jobs
 */
class RepairService {
public:
    struct Config {
        bool enable{false};
        std::filesystem::path dataDir{};
        std::uint32_t maxBatch{16};
        std::uint32_t maintenanceTokens{1};
        bool allowDegraded{true};
        std::uint32_t maxActiveDuringDegraded{1};
        bool autoRebuildOnDimMismatch{true};
        std::int32_t maxRetries{3};
        std::chrono::seconds stalledThreshold{3600}; // 1 hour
    };

    // Event types for document operations (same as old RepairCoordinator)
    struct DocumentAddedEvent {
        std::string hash;
        std::string path;
    };

    struct DocumentRemovedEvent {
        std::string hash;
    };

    using ProgressFn = std::function<void(const RepairEvent&)>;

    RepairService(ServiceManager* services, StateComponent* state,
                  std::function<size_t()> activeConnFn, Config cfg);
    virtual ~RepairService();

    // ── Lifecycle ──
    void start();
    void stop();

    // ── Event-driven interface ──
    void onDocumentAdded(const DocumentAddedEvent& event);
    void onDocumentRemoved(const DocumentRemovedEvent& event);
    void enqueueEmbeddingRepair(const std::vector<std::string>& hashes);

    // ── On-demand repair (from RPC) ──
    RepairResponse executeRepair(const RepairRequest& request, ProgressFn progress);

    /// Returns true if an on-demand repair RPC is currently executing.
    bool isRepairInProgress() const noexcept {
        return repairInProgress_.load(std::memory_order_acquire);
    }

    // ── Live tuning hooks ──
    void setMaintenanceTokens(std::uint32_t tokens) {
        tokens_.store(tokens, std::memory_order_relaxed);
        cfg_.maintenanceTokens = tokens;
    }
    void setMaxBatch(std::uint32_t maxBatch) { cfg_.maxBatch = maxBatch; }

private:
    // ── Shutdown coordination ──
    struct ShutdownState {
        std::atomic<bool> finished{false};
        std::atomic<bool> running{true};
        std::mutex mutex;
        std::condition_variable cv;
        Config config;
    };

    // ── Background loop (absorbs RepairCoordinator::runAsync) ──
    boost::asio::awaitable<void> backgroundLoop(std::shared_ptr<ShutdownState> shutdownState);
    boost::asio::awaitable<void> spawnInitialScan();
    boost::asio::awaitable<void> processPathTreeRepair();
    void performVectorCleanup();
    bool maintenanceAllowed() const;

    // ── Detect missing work ──
    struct MissingWorkResult {
        std::vector<std::string> missingEmbeddings;
        std::vector<std::string> missingFts5;
    };
    MissingWorkResult detectMissingWork(const std::vector<std::string>& batch);

    // ── Dependencies (virtual for unit tests) ──
    virtual std::shared_ptr<metadata::IMetadataRepository> getMetadataRepoForRepair() const;

    // ── Core repair operations (each returns per-op result) ──
    RepairOperationResult cleanOrphanedMetadata(bool dryRun, bool verbose, ProgressFn progress);
    RepairOperationResult repairMimeTypes(bool dryRun, bool verbose, ProgressFn progress);
    RepairOperationResult repairDownloads(bool dryRun, bool verbose, ProgressFn progress);
    RepairOperationResult rebuildPathTree(bool dryRun, bool verbose, ProgressFn progress);
    RepairOperationResult cleanOrphanedChunks(bool dryRun, bool verbose, ProgressFn progress);
    RepairOperationResult repairBlockReferences(bool dryRun, bool verbose, ProgressFn progress);
    RepairOperationResult rebuildFts5Index(const RepairRequest& req, ProgressFn progress);
    RepairOperationResult generateMissingEmbeddings(const RepairRequest& req, ProgressFn progress);
    RepairOperationResult optimizeDatabase(bool dryRun, bool verbose, ProgressFn progress);

    // ── NEW: stuck document recovery ──
    RepairOperationResult recoverStuckDocuments(const RepairRequest& req, ProgressFn progress);

    struct StuckDocumentInfo {
        enum Category { FailedExtraction, GhostSuccess, StalledPending, StalledProcessing };
        Category category;
        int64_t docId{0};
        std::string hash;
        std::string path;
        int repairAttempts{0};
    };
    std::vector<StuckDocumentInfo> detectStuckDocuments(int32_t maxRetries);

    // ── Symbol extraction scheduling (ported from RepairCoordinator) ──
    virtual std::shared_ptr<GraphComponent> getGraphComponentForScheduling() const;
    virtual std::shared_ptr<metadata::KnowledgeGraphStore> getKgStoreForScheduling() const;
    virtual const std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>>&
    getSymbolExtractorsForScheduling() const;

    // ── Token gating ──
    bool tryAcquireToken() {
        if (cfg_.maintenanceTokens == 0)
            return true;
        auto cur = tokens_.load();
        while (cur > 0) {
            if (tokens_.compare_exchange_weak(cur, cur - 1))
                return true;
        }
        return false;
    }
    void releaseToken() {
        if (cfg_.maintenanceTokens == 0)
            return;
        auto cur = tokens_.load();
        while (cur < cfg_.maintenanceTokens) {
            if (tokens_.compare_exchange_weak(cur, cur + 1))
                return;
        }
    }

    // ── Progress helper ──
    void updateProgressPct();

    // ── Members ──
    ServiceManager* services_;
    StateComponent* state_;
    std::function<size_t()> activeConnFn_;
    Config cfg_;
    std::atomic<std::uint32_t> tokens_{0};

    // Pending-document queue (background loop)
    std::queue<std::string> pendingDocuments_;
    std::unordered_set<std::string> pendingSet_;
    mutable std::mutex queueMutex_;
    std::condition_variable queueCv_;
    std::atomic<bool> running_{false};

    // Progress tracking
    std::atomic<std::uint64_t> totalBacklog_{0};
    std::atomic<std::uint64_t> processed_{0};

    std::atomic<bool> dimMismatchRebuildDone_{false};
    std::shared_ptr<ShutdownState> shutdownState_;

    // On-demand repair serialization (only one RPC repair at a time)
    std::mutex repairMutex_;
    std::atomic<bool> repairInProgress_{false};
};

} // namespace yams::daemon
