#pragma once

#include <yams/core/types.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <vector>

#include <yams/compat/boost_asio_core.h> // IWYU pragma: keep

namespace yams::vector {
class VectorDatabase;
} // namespace yams::vector

namespace yams::daemon {

struct StateComponent;

enum class RebuildReason : uint32_t {
    DimensionMismatch = 1u << 0,
    EmbeddingBatch = 1u << 1,
    TopologyDirty = 1u << 2,
    Manual = 1u << 3,
    InitialBuild = 1u << 4,
};

/**
 * @brief Snapshot of vector index state.
 */
struct VectorIndexTelemetry {
    uint64_t rebuildEpoch{0};     ///< Monotonic; bumped at each finalize
    uint32_t pendingReasons{0};   ///< Bitmask of RebuildReason values queued
    uint32_t activeBulkScopes{0}; ///< Reference count of live BulkScope handles
    bool rebuilding{false};       ///< True while a rebuild is in-flight
    bool ready{false};            ///< True after the first successful finalize
    uint32_t progressPct{0};      ///< 0–100
};

enum class VectorCheckpointPhase : uint8_t { Idle, Queued, Running };

struct VectorCheckpointTelemetry {
    VectorCheckpointPhase phase{VectorCheckpointPhase::Idle};
    uint64_t generation{0};
    uint64_t requests{0};
    uint64_t coalesced{0};
    uint64_t started{0};
    uint64_t completed{0};
    uint64_t timedOut{0};
    uint64_t postFailures{0};
    uint64_t queuedAgeMs{0};
    uint64_t runningAgeMs{0};
};

/**
 * @brief Single owner of all vector-index mutations.
 *
 * All beginBulkLoad / finalizeBulkLoad / persistIndex / buildIndex calls flow
 * through this class. Exposes a synchronized telemetry snapshot for status and
 * diagnostics.
 *
 * Execution model: a Boost.Asio strand on the daemon's existing io_context.
 */
class VectorIndexCoordinator {
public:
    /**
     * @brief RAII handle for a bulk-ingest window.
     *
     * beginBulkLoad() is called when the first BulkScope is created.
     * finalizeBulkLoad() + buildIndex() + persistIndex() are posted to the
     * strand when the last live BulkScope is destroyed.
     */
    class BulkScope {
    public:
        BulkScope() = default;
        BulkScope(BulkScope&& other) noexcept;
        BulkScope& operator=(BulkScope&& other) noexcept;
        ~BulkScope();
        BulkScope(const BulkScope&) = delete;
        BulkScope& operator=(const BulkScope&) = delete;

    private:
        friend class VectorIndexCoordinator;
        explicit BulkScope(VectorIndexCoordinator* owner) noexcept : owner_(owner) {}
        VectorIndexCoordinator* owner_{nullptr};
    };

    /**
     * @param exec   Executor from the daemon's WorkCoordinator (or test io_context).
     * @param vdb    Shared pointer to the live VectorDatabase.
     * @param state  StateComponent for vectorIndexReady / vectorIndexProgress updates.
     */
    VectorIndexCoordinator(boost::asio::any_io_executor exec,
                           std::shared_ptr<vector::VectorDatabase> vdb, StateComponent* state);

    ~VectorIndexCoordinator();

    // Non-copyable, non-movable (shared ownership via shared_ptr expected).
    VectorIndexCoordinator(const VectorIndexCoordinator&) = delete;
    VectorIndexCoordinator& operator=(const VectorIndexCoordinator&) = delete;

    /**
     * @brief Ref-counted bulk window.
     *
     * The first call enters bulk mode (beginBulkLoad).  The scope returned must
     * be held for the duration of inserts.  When the last scope is destroyed a
     * finalize+persist is posted onto the strand.
     *
     * Safe to call from any thread.
     */
    BulkScope beginBulkIngest(RebuildReason reason);

    /**
     * @brief Fire-and-forget rebuild request for sync callers.
     *
     * Internally co_spawns requestRebuild() detached on the strand so that the
     * caller gets the full coalescing/waiter semantics without having to await.
     * Safe to call from any thread.
     */
    void fireRebuild(RebuildReason reason) noexcept;

    /// Request a coordinated rebuild and wait for build/persist completion.
    /// Returns InvalidState when invoked from the coordinator strand.
    Result<void> requestRebuildBlocking(RebuildReason reason);

    /**
     * @brief Synchronously persist the index through the coordinator strand.
     *
     * Serialises with in-flight rebuilds (no concurrent build/persist on the
     * backend). Used by CheckpointManager so that periodic checkpoints do not
     * race the rebuild path. Blocks the caller until the strand completes.
     * Returns false if persistence failed or the VDB is unavailable.
     */
    bool requestCheckpoint() noexcept;

    /**
     * @brief Coalescing rebuild request.
     *
     * Multiple concurrent callers sharing the same reason are coalesced into a
     * single rebuild.  All awaitables resolve after the next finalize that covers
     * their reason.
     *
     * Must be co_awaited from a coroutine whose executor is compatible with the
     * coordinator's strand (or any executor that can post to it).
     */
    boost::asio::awaitable<Result<void>> requestRebuild(RebuildReason reason);

    /**
     * @brief Thread-safe telemetry snapshot.
     *
     * Safe to call from any thread, including the status RPC path.
     */
    VectorIndexTelemetry snapshot() const noexcept;

    /**
     * @brief Snapshot checkpoint admission and execution state.
     *
     * Queued means the strand callback has not started. Running means persistence entered the
     * callback and may be waiting in the vector backend.
     */
    VectorCheckpointTelemetry checkpointSnapshot() const noexcept;

    /**
     * @brief One-shot initial build/load called by ServiceManager after VDB is ready.
     *
     * Validates any backend-supported persisted search index and otherwise prepares a query-ready
     * index from authoritative vector rows. No-ops if no rows exist.
     */
    boost::asio::awaitable<Result<void>> initialBuildIfNeeded();

    /**
     * @brief Update the VectorDatabase handle (called after DB initialization).
     * Safe to call from any thread.
     */
    void setVectorDatabase(std::shared_ptr<vector::VectorDatabase> vdb) {
        std::lock_guard<std::mutex> lk(vdbMutex_);
        vectorDb_ = std::move(vdb);
    }

    /**
     * @brief Suppress buildIndex/persistIndex work while preserving inserts and telemetry.
     *
     * Used by memory instrumentation profiles so daemon startup can be measured without
     * concurrent SPQ/PQ rebuild allocation churn.
     */
    void setBuildsSuppressed(bool suppressed) noexcept {
        buildsSuppressed_.store(suppressed, std::memory_order_relaxed);
    }

#ifdef YAMS_TESTING
    void testing_setCheckpointWaitTimeout(std::chrono::milliseconds timeout) noexcept {
        checkpointWaitTimeout_ = timeout;
    }
    uint32_t testing_pendingReasons() const noexcept {
        return pendingReasons_.load(std::memory_order_relaxed);
    }
    uint32_t testing_activeScopes() const noexcept {
        return activeBulkScopes_.load(std::memory_order_relaxed);
    }
    uint64_t testing_rebuildEpoch() const noexcept {
        return rebuildEpoch_.load(std::memory_order_relaxed);
    }
    uint64_t testing_checkpointCallbacksStarted() const noexcept {
        return checkpointStarted_.load(std::memory_order_relaxed);
    }
    // Expose strand so tests may submit work serialised with the coordinator.
    boost::asio::strand<boost::asio::any_io_executor>& testing_strand() noexcept { return strand_; }
    void testing_setAfterRebuildAdmission(std::function<void()> callback) {
        afterRebuildAdmissionForTesting_ = std::move(callback);
    }
#endif

private:
    // Called from BulkScope destructor (any thread).
    void releaseBulkScope() noexcept;

    // Post a telemetry refresh to the strand.
    void postTelemetryRefresh() noexcept;

    // Runs on the strand: finalizes bulk window and persists index.
    void doFinalizeOnStrand();

    // Plain function (not a coroutine): must be called via post(strand_, ...) to
    // guarantee deferred execution.  Boost's co_spawn uses dispatch() which runs
    // inline when already on the strand, defeating the coalescing invariant.
    void doRebuildOnStrand(uint32_t reasons);

    void publishTelemetry(const VectorIndexTelemetry& tel) noexcept;

    struct CheckpointAttempt {
        uint64_t generation{0};
        std::shared_ptr<std::promise<bool>> completion;
        std::shared_future<bool> result;
    };
    struct CallbackLifetime {
        std::mutex mutex;
        VectorIndexCoordinator* owner{nullptr};
    };
    void completeCheckpointAttempt(const std::shared_ptr<CheckpointAttempt>& attempt, bool result,
                                   bool callbackStarted) noexcept;

    // Drain and notify all waiters whose targetEpoch <= current epoch.
    // Called on the strand after an epoch bump.
    void notifyWaiters(uint64_t currentEpoch, const Result<void>& result);

    boost::asio::strand<boost::asio::any_io_executor> strand_;
    std::shared_ptr<CallbackLifetime> callbackLifetime_;
    mutable std::mutex vdbMutex_;
    std::shared_ptr<vector::VectorDatabase>
        vectorDb_; // guarded by vdbMutex_ for set, strand for use
    StateComponent* state_{nullptr};

    // ── Bulk-scope ref-count (thread-safe atomic) ──────────────────────────
    std::atomic<uint32_t> activeBulkScopes_{0};

    // ── Strand-protected rebuild state ─────────────────────────────────────
    // All accesses below are serialized by strand_ (no additional mutex needed).
    std::atomic<uint32_t> pendingReasons_{0}; ///< ORed bitmask of queued reasons
    // rebuildInFlight_ is atomic so that compare_exchange gives exactly-once semantics
    // even if the strand dispatch hasn't fully transferred the calling coroutine.
    std::atomic<bool> rebuildInFlight_{false};
    std::atomic<uint64_t> rebuildEpoch_{0};
    std::atomic<bool> buildsSuppressed_{false};
    std::chrono::milliseconds checkpointWaitTimeout_{std::chrono::seconds(30)};

    mutable std::mutex checkpointMutex_;
    std::shared_ptr<CheckpointAttempt> checkpointAttempt_; // guarded by checkpointMutex_
    std::atomic<VectorCheckpointPhase> checkpointPhase_{VectorCheckpointPhase::Idle};
    std::atomic<uint64_t> checkpointGeneration_{0};
    std::atomic<uint64_t> checkpointRequests_{0};
    std::atomic<uint64_t> checkpointCoalesced_{0};
    std::atomic<uint64_t> checkpointStarted_{0};
    std::atomic<uint64_t> checkpointCompleted_{0};
    std::atomic<uint64_t> checkpointTimedOut_{0};
    std::atomic<uint64_t> checkpointPostFailures_{0};
    std::atomic<uint64_t> checkpointQueuedAtNs_{0};
    std::atomic<uint64_t> checkpointRunningAtNs_{0};

    // Waiters registered by requestRebuild; drained when epoch advances.
    using RebuildCompletion = std::function<void(Result<void>)>;
    struct Waiter {
        uint64_t targetEpoch;
        RebuildCompletion handler;
    };
    std::mutex waitersMutex_; // protects waiters_ (accessed from multiple strand posts)
    std::vector<Waiter> waiters_;
    std::function<void()> afterRebuildAdmissionForTesting_;

    mutable std::mutex telemetryMutex_;
    VectorIndexTelemetry telemetry_{};
};

} // namespace yams::daemon
