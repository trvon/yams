#pragma once

#include <yams/core/types.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/strand.hpp>

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
 * @brief Lock-free snapshot of vector index state.
 *
 * Written only from the coordinator's strand. Read lock-free via a seqlock in snapshot().
 * Keep this POD-ish to fit the seqlock pattern.
 */
struct VectorIndexTelemetry {
    uint64_t rebuildEpoch{0};     ///< Monotonic; bumped at each finalize
    uint32_t pendingReasons{0};   ///< Bitmask of RebuildReason values queued
    uint32_t activeBulkScopes{0}; ///< Reference count of live BulkScope handles
    bool rebuilding{false};       ///< True while a rebuild is in-flight
    bool ready{false};            ///< True after the first successful finalize
    uint32_t progressPct{0};      ///< 0–100
};

/**
 * @brief Single owner of all vector-index mutations.
 *
 * All beginBulkLoad / finalizeBulkLoad / persistIndex / buildIndex calls flow
 * through this class.  Exposes a lock-free telemetry snapshot so the status
 * fast-path never blocks behind a rebuild mutex.
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
     * @brief Lock-free telemetry snapshot.
     *
     * Safe to call from any thread, including the status RPC path.  Uses a
     * seqlock so readers never block behind a rebuild.
     */
    VectorIndexTelemetry snapshot() const noexcept;

    /**
     * @brief One-shot initial build/load called by ServiceManager after VDB is ready.
     *
     * If the database has rows but no persisted HNSW index, triggers a rebuild.
     * If the persisted index exists, loads it and marks vectorIndexReady.
     * No-ops if no rows exist.
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

#ifdef YAMS_TESTING
    uint32_t testing_pendingReasons() const noexcept {
        return pendingReasons_.load(std::memory_order_relaxed);
    }
    uint32_t testing_activeScopes() const noexcept {
        return activeBulkScopes_.load(std::memory_order_relaxed);
    }
    uint64_t testing_rebuildEpoch() const noexcept {
        return rebuildEpoch_.load(std::memory_order_relaxed);
    }
    // Expose strand so tests may submit work serialised with the coordinator.
    boost::asio::strand<boost::asio::any_io_executor>& testing_strand() noexcept { return strand_; }
#endif

private:
    // Called from BulkScope destructor (any thread).
    void releaseBulkScope() noexcept;

    // Post a telemetry refresh to the strand. Single-writer invariant: all
    // publishTelemetry() writes happen on strand_.
    void postTelemetryRefresh() noexcept;

    // Runs on the strand: finalizes bulk window and persists index.
    void doFinalizeOnStrand();

    // Plain function (not a coroutine): must be called via post(strand_, ...) to
    // guarantee deferred execution.  Boost's co_spawn uses dispatch() which runs
    // inline when already on the strand, defeating the coalescing invariant.
    void doRebuildOnStrand(uint32_t reasons);

    // Seqlock helpers — writer must run on strand.
    void publishTelemetry(const VectorIndexTelemetry& tel) noexcept;
    VectorIndexTelemetry readTelemetrySeqlock() const noexcept;

    // Drain and notify all waiters whose targetEpoch <= current epoch.
    // Called on the strand after an epoch bump.
    void notifyWaiters(uint64_t currentEpoch);

    boost::asio::strand<boost::asio::any_io_executor> strand_;
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

    // Waiters registered by requestRebuild; drained when epoch advances.
    using RebuildCompletion = std::function<void(Result<void>)>;
    struct Waiter {
        uint64_t targetEpoch;
        RebuildCompletion handler;
    };
    std::mutex waitersMutex_; // protects waiters_ (accessed from multiple strand posts)
    std::vector<Waiter> waiters_;

    // ── Seqlock for lock-free snapshot reads ───────────────────────────────
    mutable std::atomic<uint64_t> seqVersion_{0}; // even = stable, odd = writing
    VectorIndexTelemetry seqData_{};              // written only on strand
};

} // namespace yams::daemon
