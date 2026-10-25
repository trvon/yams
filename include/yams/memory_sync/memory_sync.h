// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <future>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

// pi-lens-ignore: fatal error
#include <nlohmann/json.hpp>

#include <yams/core/types.h>
#include <yams/crypto/hasher.h>
#include <yams/memory_sync/key_policy.h>
#include <yams/memory_sync/records.h>
#include <yams/memory_sync/version_vector.h>
#include <yams/memory_sync/writer_auth.h>
#include <yams/storage/storage_backend.h>

namespace yams::memory_sync {

struct MemorySyncControl {
    std::function<bool()> isCancelled{};
    std::function<bool()> canAdmitRemoteWork;
    /// The loop is the backend's only writer (direct transport's local op store), so a
    /// complete scan stays valid until the loop itself stores or removes an object.
    bool exclusiveBackend{false};
    /// Cooperative local/background pacing. May wait briefly but must remain cancellable.
    /// Unlike admission rejection it preserves the current bounded sweep's progress.
    std::function<Result<void>()> cooperate;
};

/// Tombstone collection is disabled unless the deployment supplies its complete,
/// stable replica set. Every configured peer must acknowledge the exact tombstone
/// operation after the minimum retention horizon before history can be removed.
struct TombstoneGcPolicy {
    std::set<NodeId> requiredPeers;
    // pi-lens-ignore: no-bit-fields
    std::chrono::milliseconds minRetention{
        std::chrono::milliseconds::max()}; // NOLINT(no-bit-fields)
};

/// Typed reconciliation/resource limits. These bound work retained or requested by
/// one loop; callers tune them through MemorySyncConfig rather than environment knobs.
struct MemorySyncLimits {
    std::size_t maxIndexObjectsPerSync{512};
    std::size_t maxEnvelopeBytes{std::size_t{64} * 1024};
    std::size_t maxValueBytes{std::size_t{16} * 1024 * 1024};
    std::size_t maxMergedKeys{4096};
    std::size_t maxCacheBytes{std::size_t{64} * 1024 * 1024};
    std::size_t maxTrackedIdentities{8192};
    /// Envelopes whose signature this loop has already checked. The set holds the
    /// envelope digest, so an entry is roughly 150 bytes at capacity.
    std::size_t maxVerifiedEnvelopes{65536};
    /// Maximum legacy index objects inspected while building missing counter entries.
    std::size_t maxHistoryMigrationObjects{8192};
    /// Maximum legacy envelope bytes retained while rebuilding one writer's counter index.
    std::size_t maxHistoryMigrationBytes{std::size_t{64} * 1024 * 1024};
    /// Aggregate encoded envelope + index-key bytes observed across a complete sweep,
    /// charged before retaining causal identities (even for misplaced envelopes).
    /// Together with maxTrackedIdentities (also the candidate-count ceiling), bounds
    /// decoded scan state in proportion to input. Not an allocator/RSS ceiling.
    std::size_t maxScanBytes{std::size_t{64} * 1024 * 1024};
};

struct MemoryDelta {
    std::string logicalKey;
    MemoryIndexRecord record;
    // pi-lens-ignore: no-bit-fields
    std::vector<std::byte> payload{}; // NOLINT(no-bit-fields) -- empty for tombstones
};

struct MemoryDeltaBatch {
    std::vector<MemoryDelta> deltas;
    bool hasMore{false};
};

struct DeltaApplyResult {
    std::size_t received{0};
    std::size_t merged{0};
    std::size_t replayed{0};
    std::map<std::string, std::string> quarantined{};
    VersionVector version;
};

enum class EraseReadinessProbe : std::uint8_t { Explicit, MetadataAbsent, ContentAbsent };

/// Durable local intent retained until its exact tombstone envelope is committed to history.
struct PendingEraseIntent {
    std::string logicalKey;
    std::string tombstonePayload;
    EraseReadinessProbe readinessProbe{EraseReadinessProbe::Explicit};
    bool ready{false};
    bool prepared{false};
    std::uint64_t preparedCounter{0};
};

/// One member of an all-or-rollback pre-delete staging batch.
struct EraseStageRequest {
    std::string logicalKey;
    std::string tombstonePayload;
    EraseReadinessProbe readinessProbe{EraseReadinessProbe::Explicit};
};

struct WriterHistoryCommitment {
    std::uint64_t counter{0};
    std::string digest;

    bool operator==(const WriterHistoryCommitment&) const = default;
};

/// Evidence that a writer's staged window contradicts its authenticated history: a fork, a
/// forged frontier, a gap, or a record that fails authentication. Only this warrants durable
/// writer quarantine; local or racy failures are reported as an Error instead.
struct WriterHistoryViolation {
    ErrorCode code{ErrorCode::InvalidData};
    std::string reason;
};

struct ReplicationState {
    VersionVector version;
    // pi-lens-ignore: no-bit-fields
    std::map<NodeId, WriterHistoryCommitment> commitments; // NOLINT(no-bit-fields)
    std::set<NodeId> quarantinedWriters{};
};

/// Authenticated direct-P2P zero-state snapshot. The serving peer is a witness for the exact
/// frozen frontier; winner envelopes retain their original writer signatures.
struct ColdBootstrapSnapshot {
    VersionVector frontier;
    // pi-lens-ignore: no-bit-fields
    std::map<NodeId, WriterHistoryCommitment> commitments; // NOLINT(no-bit-fields)
    std::vector<MemoryDelta> winners{};
    std::string rootDigest{};
    DetachedWriterSignature witnessSignature{};
};

/// Backend-agnostic memory sync loop. Memory records are published as
/// content-addressed blobs plus one version-vector-tagged index record per write;
/// `sync()` lists the index, pulls records, and LWW-merges to a deterministic
/// winner per logical key. The storage backend is injected, so filesystem and
/// S3/R2 are interchangeable; optional schema-v4 authentication is verified before hydration.
class MemorySyncLoop {
public:
    MemorySyncLoop(storage::IStorageBackend& backend, NodeId nodeId,
                   std::string corpusId = "local-test-corpus", std::uint64_t corpusEpoch = 1,
                   bool allowLegacyUnbound = false, MemorySyncLimits limits = {},
                   MemorySyncControl control = {}, TombstoneGcPolicy tombstoneGc = {},
                   std::shared_ptr<const WriterAuthenticator> writerAuth = {},
                   std::string controlScope = {})
        : countedBackend_(
              std::make_unique<WriteCountingBackend>(backend, control.exclusiveBackend, limits)),
          backend_(countedBackend_.get()), nodeId_(std::move(nodeId)),
          corpusId_(std::move(corpusId)), corpusEpoch_(corpusEpoch),
          allowLegacyUnbound_(allowLegacyUnbound), limits_(limits), control_(std::move(control)),
          tombstoneGc_(std::move(tombstoneGc)), writerAuth_(std::move(writerAuth)),
          controlScope_(controlScope.empty() ? corpusId_ + ":" + std::to_string(corpusEpoch_)
                                             : std::move(controlScope)) {
        if (control_.exclusiveBackend && !backend.isRemote()) {
            if (auto* filesystem = dynamic_cast<storage::FilesystemBackend*>(&backend)) {
                sweepDirectory_ = filesystem->scratchDirectory();
            }
        }
        if (control_.cooperate) {
            if (auto* filesystem = dynamic_cast<storage::FilesystemBackend*>(&backend)) {
                filesystem->setListingWorkCheck(control_.cooperate);
            }
        }
    }

#ifdef YAMS_TESTING
    static std::unique_ptr<storage::IStorageBackend>
    testing_countedBackend(storage::IStorageBackend& backend, bool exclusive,
                           MemorySyncLimits limits = {},
                           std::function<void(std::string_view)> beforeCacheAllocation = {},
                           bool lexicalTestBackend = false);
#endif

    /// Publish `content` under `logicalKey`. Stores the content-addressed blob and
    /// an index record stamped with this node's version vector and hybrid timestamp.
    Result<void> publish(std::string_view logicalKey, std::span<const std::byte> content);

    /// Publish a causally ordered deletion through the durable local outbox. The exact
    /// prepared envelope remains replayable until its index/history commit succeeds.
    Result<void> erase(std::string_view logicalKey, std::string tombstonePayload = {});
    /// Prepare and durably publish an own-record deletion only while the exact observed
    /// envelope is still current. False means obsolete; errors retain their original type.
    Result<bool> eraseIfCurrent(std::string_view logicalKey, const MemoryIndexRecord& expected,
                                std::string tombstonePayload = {});

    using EraseReadyValidator = std::function<Result<bool>()>;

    /// Persist an erase intent before a separate local deletion. `ready=false` intents are never
    /// published by generic retry; the owner promotes them only after observing local absence.
    Result<void> stageErase(std::string_view logicalKey, std::string tombstonePayload,
                            bool ready = false,
                            EraseReadinessProbe readinessProbe = EraseReadinessProbe::Explicit);

    /// Reserve capacity for a related set of pre-delete intents before storing any member.
    /// Backend failures roll back only intents newly created by this call.
    Result<void> stageErases(std::span<const EraseStageRequest> requests);

    Result<std::vector<PendingEraseIntent>> pendingErases();

    /// Promote and publish one staged intent. Once prepared, the exact signed envelope is reused
    /// across every retry so an outbox cleanup failure cannot create a later deletion.
    Result<void> publishStagedErase(std::string_view logicalKey,
                                    EraseReadyValidator validator = {});

    Result<void> cancelStagedErase(std::string_view logicalKey);

    /// Retry only intents whose owning local deletion has already been confirmed.
    Result<std::size_t> replayReadyErases();

    /// Reconcile against the backend: list the index, pull every record, LWW-merge
    /// per logical key, and fold remote histories into this node's version vector.
    /// Returns the merged `logicalKey -> winning record` map.
    Result<std::map<std::string, MemoryIndexRecord>> sync();
    /// A successful page is progress, not a complete security/visibility decision.
    [[nodiscard]] bool sweepInProgress() const noexcept { return !scanCursor_.empty(); }

    /// Complete the current bounded sweep and return a full committed view.
    /// Direct mode calls this against its local durable op store at startup.
    Result<std::map<std::string, MemoryIndexRecord>> syncFully();

    /// Export this writer's durable operations newer than the peer's causal
    /// watermark. The backend is local in direct mode, so this replaces remote
    /// shared-namespace scans with a bounded local op-store read.
    Result<MemoryDeltaBatch> exportLocalDeltasAfter(
        const VersionVector& peerVersion, std::size_t maxDeltas = 128,
        std::uint64_t maxWriterCounter = std::numeric_limits<std::uint64_t>::max());

    /// Export the complete winner set at an exact handshake-frozen frontier. This is used only
    /// for bounded authenticated zero-state bootstrap; normal replication remains writer-local.
    Result<ColdBootstrapSnapshot> exportColdBootstrap(const ReplicationState& frozen,
                                                      std::size_t maxRecords,
                                                      std::size_t maxPayloadBytes);

    /// Install an authenticated peer-witnessed winner snapshot into a durably empty replica.
    /// A prepared journal makes promotion idempotent across checkpoint/index/cleanup crashes.
    Result<DeltaApplyResult> applyColdBootstrap(const ColdBootstrapSnapshot& snapshot,
                                                std::string_view authenticatedWitness,
                                                std::size_t maxRecords,
                                                std::size_t maxPayloadBytes);

    /// Apply peer-delivered operations directly through the same causal/LWW
    /// state used by shared-store reconciliation. Valid operations are persisted
    /// to the local backend before becoming visible; replay is idempotent, recent
    /// operation forks are quarantined, and causal gaps/dependency gaps fail closed.
    Result<DeltaApplyResult> applyDeltas(std::span<const MemoryDelta> deltas);

    [[nodiscard]] VersionVector currentVersion() const;

    [[nodiscard]] ReplicationState replicationState() const;

    /// Resolve a constant-size commitment for this writer's exact historical prefix without
    /// hydrating payload blobs. Used only by the authenticated handshake proof round.
    Result<WriterHistoryCommitment> localHistoryCommitmentAt(std::uint64_t counter);

    /// Select the largest contiguous local prefix that fits both negotiated record and wire
    /// bounds. Only one payload is hydrated at a time; accepted payloads are not retained here.
    Result<WriterHistoryCommitment> localHistoryWindowAfter(std::uint64_t peerCounter,
                                                            std::size_t maxRecords,
                                                            std::size_t maxWireBytes);

    /// Commitment for any writer's exact prefix in local durable history.
    Result<WriterHistoryCommitment> historyCommitmentAt(std::string_view writerId,
                                                        std::uint64_t counter);

    /// Validate a complete bounded incoming session against the authenticated writer's
    /// handshake-frozen frontier before any operation becomes visible. Another session with
    /// the same writer may have advanced the durable prefix since the handshake: staged
    /// operations at or below it are replays only if their record hashes match durable history
    /// (any other record there is a fork), and the rest must chain from it exactly to the
    /// frontier. An empty window verifies the durable prefix at the frontier. Returns a
    /// violation for writer faults and an Error for local or transient failures.
    Result<std::optional<WriterHistoryViolation>>
    validateHistoryExtension(std::string_view writerId, std::span<const MemoryDelta> deltas,
                             const WriterHistoryCommitment& expectedFrontier);

    [[nodiscard]] bool writerQuarantined(std::string_view writerId) const noexcept;
    [[nodiscard]] std::uint64_t durableQuarantineGeneration() const noexcept {
        return durableQuarantineGeneration_;
    }

    /// Durably invalidate one writer before removing any visible winners. The checkpoint is
    /// persisted first so a crash cannot make invalidated history visible again after restart.
    /// Every new quarantine is logged here, with `reason` and the writer's local counters, so
    /// no caller can quarantine silently; returns false (and logs nothing) when the writer was
    /// already quarantined.
    Result<bool> quarantineWriter(std::string_view writerId, std::string_view sourceNodeId,
                                  std::string_view reason);

    /// Collect tombstone history only when the complete configured replica set has
    /// acknowledged the exact delete operation and the retention horizon has elapsed.
    /// Empty replica sets disable collection. A history larger than one bounded page
    /// is retained rather than partially collected.
    Result<std::size_t> collectTombstoneGarbage(std::uint64_t now = nowMs());

    /// Return the number of records observed by the most recent reconciliation.
    /// This is intentionally side-effect free so status callers do not force sync.
    [[nodiscard]] std::size_t mergedRecordCount() const noexcept;
    [[nodiscard]] std::size_t quarantinedRecordCount() const noexcept;
    [[nodiscard]] std::size_t authFailureCount() const noexcept;

    /// Immutable copy of the reconciled state. IPC readers consume this instead
    /// of the live maps so a slow reconciliation cannot block status/cached reads;
    /// the owning service refreshes it at commit points while holding its lock.
    using WinnerPage = std::vector<std::pair<std::string, MemoryIndexRecord>>;
    /// A pinned, immutable committed generation. Disk-backed lookups are explicitly fallible;
    /// cached reads continue to use only the bounded resident maps below.
    class WinnerView {
    public:
        /// Names one pinned winner set. Two equal identities hold the same records: the
        /// instance is unique per store for the process lifetime and the generation advances
        /// on every winner change.
        struct Identity {
            std::uint64_t instance{0};
            std::int64_t generation{0};
            bool operator==(const Identity&) const = default;
        };
        virtual ~WinnerView() = default;
        /// nullopt when this view cannot name its contents (for example while the store is
        /// still being built); callers must then treat the contents as changed.
        [[nodiscard]] virtual std::optional<Identity> identity() const noexcept {
            return std::nullopt;
        }
        [[nodiscard]] virtual std::size_t size() const noexcept = 0;
        virtual Result<MemoryIndexRecord> find(std::string_view key) const = 0;
        virtual Result<WinnerPage> page(std::string_view prefix, std::string_view afterKey,
                                        std::size_t limit) const = 0;
    };
    /// Disposable indexed adapter bookkeeping, not an authenticated record source. The
    /// implementation bounds its SQLite cache, row size, file size and live workspace count.
    /// Callers retain only a row at a time; failures are never reported as absent rows.
    class AdapterIndex {
    public:
        virtual ~AdapterIndex() = default;
        virtual Result<void> put(std::string_view key, std::string_view value) = 0;
        virtual Result<std::optional<std::string>> lookup(std::string_view key) const = 0;
    };
    static Result<std::shared_ptr<AdapterIndex>>
    createAdapterIndex(const std::filesystem::path& directory, std::size_t maxBytes,
                       std::size_t maxRowBytes, const std::function<Result<void>()>& cooperate);
#ifdef YAMS_TESTING
    static void testingSetScratchAllocationObserver(std::function<void()> observer);
#endif

    struct CommittedState {
        // pi-lens-ignore: no-bit-fields
        std::map<std::string, MemoryIndexRecord> merged; // NOLINT(no-bit-fields)
        // pi-lens-ignore: no-bit-fields
        std::map<std::string, std::vector<std::byte>> cachedBlobs; // NOLINT(no-bit-fields)
        // pi-lens-ignore: no-bit-fields
        std::map<std::string, std::string> quarantined; // NOLINT(no-bit-fields)
        std::size_t authFailures{0};
        ReplicationState replication;
        std::shared_ptr<const WinnerView> winners;
    };
    [[nodiscard]] std::map<std::string, MemoryIndexRecord> adapterState() const;

    [[nodiscard]] CommittedState committedState() const;

    /// Bounded snapshot of current quarantine reasons (cleared each scan page).
    [[nodiscard]] std::map<std::string, std::string> quarantinedReasons() const;

    [[nodiscard]] bool legacyUnauthenticatedHistoryObserved() const noexcept;

    /// Read only the last periodically reconciled winner. Daemon IPC uses this
    /// path so polling a peer cannot manufacture convergence by forcing sync.
    [[nodiscard]] Result<std::vector<std::byte>> readCached(std::string_view logicalKey) const;

    /// Read the winning content for `logicalKey`. Re-syncs to stay current.
#ifdef YAMS_TESTING
    [[nodiscard]] bool hasMergedRecord(std::string_view logicalKey) const {
        return committedWinner(logicalKey).has_value();
    }
    [[nodiscard]] std::size_t testingScanCandidateCount() const noexcept {
        return scanCandidates_.size();
    }
    [[nodiscard]] std::size_t testingScanBytes() const noexcept { return scanBytes_; }
    [[nodiscard]] std::size_t testingSweepAttempts() const noexcept { return sweepAttempts_; }
    [[nodiscard]] bool testingSweepDegraded() const noexcept { return sweepDegraded_; }
    void testingExpireSweepBackoff() noexcept { nextSweepAttempt_ = {}; }
    [[nodiscard]] std::size_t testingCachedBlobCount() const noexcept {
        return cachedBlobs_.size();
    }
    [[nodiscard]] std::size_t testingVerifiedEnvelopeCount() const noexcept {
        return verifiedEnvelopes_.size();
    }
    [[nodiscard]] std::size_t testingVerifyAttempts() const noexcept {
        return fixedLayoutVerifyAttempts_;
    }
#endif

    Result<std::vector<std::byte>> read(std::string_view logicalKey);
    /// Read the last complete generation on demand without starting a new sweep.
    Result<std::vector<std::byte>> readCommitted(std::string_view logicalKey);
    Result<bool> matchesCommittedValue(std::string_view logicalKey,
                                       std::span<const std::byte> content) const;

private:
    friend class MemorySyncService;
    /// Pinned reads split mutable checks/cache selection from immutable backend I/O.
    /// The service holds its loop lock for prepare/validate, not for a native filesystem
    /// fetch. Fetch never reads mutable winners/caches or warms historical metadata.
    Result<void> validatePinnedPayloadRead(const MemoryIndexRecord& record) const;
    Result<std::optional<std::vector<std::byte>>>
    preparePinnedPayload(const MemoryIndexRecord& record,
                         const std::vector<std::byte>* trustedCached) const;
    Result<std::vector<std::byte>> fetchPinnedPayload(const MemoryIndexRecord& record) const;
    class SweepStore;
    Result<std::shared_ptr<SweepStore>> makeSweepStore();
    Result<void> ensureCommittedStore();
    Result<MemoryIndexRecord> committedWinner(std::string_view logicalKey) const;
    void cacheWinner(std::string_view logicalKey, const MemoryIndexRecord& record,
                     std::span<const std::byte> payload);
    Result<void> finalizeExclusiveSweep();
    Result<void>
    validateFrontierCommitments(const VersionVector& frontier,
                                const std::map<NodeId, WriterHistoryCommitment>& commitments) const;

    Result<void>
    validateBootstrapWinner(const MemoryDelta& delta, const VersionVector& frontier,
                            const std::map<NodeId, WriterHistoryCommitment>& commitments);

    static nlohmann::json
    commitmentsJson(const std::map<NodeId, WriterHistoryCommitment>& commitments);

    std::string coldBootstrapRoot(const ColdBootstrapSnapshot& snapshot) const;

    std::string coldBootstrapPrefix() const;

    std::string coldBootstrapJournalKey() const;

    std::string coldBootstrapEntryKey(std::size_t index) const;

    std::string coldBootstrapPayloadKey(std::size_t index) const;

    Result<void> clearColdBootstrapStaging();

    Result<void> clearColdBootstrapAfterCommit();

    Result<void> persistColdBootstrapJournal(const ColdBootstrapSnapshot& snapshot,
                                             std::string_view witness);

    Result<ColdBootstrapSnapshot> loadColdBootstrapJournal(std::string& witness);

    Result<DeltaApplyResult> finalizeColdBootstrap(const ColdBootstrapSnapshot& snapshot,
                                                   std::string_view witness);

    Result<void> recoverColdBootstrap();

    Result<std::optional<MemoryDelta>> loadLocalDelta(const std::string& key,
                                                      const VersionVector& peerVersion);

    struct ValidatedDelta {
        // pi-lens-ignore: no-bit-fields
        std::vector<std::byte> recordBytes; // NOLINT(no-bit-fields)
        std::string recordHash;
        // pi-lens-ignore: no-bit-fields
        std::pair<std::string, std::string> fingerprint; // NOLINT(no-bit-fields)
    };

    enum class DirectDeltaAction : std::uint8_t { Apply, Replay, Reject };
    struct DirectDeltaDecision {
        // pi-lens-ignore: no-bit-fields
        DirectDeltaAction action{DirectDeltaAction::Reject}; // NOLINT(no-bit-fields)
        std::string reason;
    };

    struct DirectWinnerPlan {
        // pi-lens-ignore: no-bit-fields
        bool becomesWinner{false}; // NOLINT(no-bit-fields)
        std::optional<std::string> oldCachedHash;
    };

    Result<ValidatedDelta> validateDirectDelta(const MemoryDelta& delta);

    DirectDeltaDecision classifyDirectDelta(const MemoryDelta& delta,
                                            const ValidatedDelta& validated) const;

    Result<DirectWinnerPlan> planDirectWinner(const MemoryDelta& delta) const;

    Result<void> persistDirectDelta(const MemoryDelta& delta, const ValidatedDelta& validated);

    void rememberDirectOperation(const MemoryDelta& delta, const ValidatedDelta& validated);

    Result<bool> commitDirectDelta(const MemoryDelta& delta, const DirectWinnerPlan& plan);

    void quarantineDirectDelta(const MemoryDelta& delta, std::string reason,
                               DeltaApplyResult& result);

    struct ScanCandidate {
        std::string indexKey;
        std::string logicalKey;
        std::string recordHash;
        MemoryIndexRecord record;
    };

    struct HistoryEntry {
        std::string writerId;
        std::uint64_t counter{0};
        std::string logicalKey;
        std::string recordHash;
        std::string prefixDigest{};
    };

    struct HistoryMigrationState {
        std::string phase{"scan"};
        std::string cursor{};
        std::uint64_t targetCounter{0};
        std::uint64_t nextCounter{1};
        std::string prefixDigest{};
    };

    struct EraseIntentState {
        std::string logicalKey;
        std::string tombstonePayload;
        EraseReadinessProbe readinessProbe{EraseReadinessProbe::Explicit};
        bool ready{false};
        std::optional<MemoryIndexRecord> authorization;
        std::optional<MemoryIndexRecord> record;
        std::string recordHash;
        WriterHistoryCommitment preparedCommitment;
    };

    static std::string_view readinessProbeName(EraseReadinessProbe probe);

    static std::optional<EraseReadinessProbe> parseReadinessProbe(std::string_view name);

    std::string eraseAuthorizationPayload(std::string_view tombstonePayload,
                                          EraseReadinessProbe probe) const;

    std::size_t maxEraseOutboxBytes() const;

    std::string eraseOutboxPrefix() const;

    std::string eraseOutboxKey(std::string_view logicalKey) const;

    Result<EraseIntentState> decodeEraseIntent(std::string_view key,
                                               std::span<const std::byte> encoded);

    Result<void> storeEraseIntent(const EraseIntentState& intent);

    Result<std::optional<EraseIntentState>> loadEraseIntent(std::string_view logicalKey);

    Result<std::vector<EraseIntentState>> loadEraseIntents();

    Result<MemoryIndexRecord> prepareEraseRecord(std::string_view logicalKey,
                                                 std::string_view tombstonePayload);

    Result<WriterHistoryCommitment> preparedEraseCommitment(const MemoryIndexRecord& record,
                                                            std::string_view recordHash) const;

    Result<void> prepareEraseIntent(EraseIntentState& intent);
    Result<void> commitPreparedErase(const EraseIntentState& intent);

    Result<void> replayReadyErasesAfterReconcile();

    Result<void> reconcileBeforeWrite(bool allowDeferredPreparedErase = false);

    /// Immutable cancellation/remote-admission checks without a pressure wait.
    [[nodiscard]] Result<void> checkWorkAdmission() const;
    [[nodiscard]] Result<void> beforeRemoteWork() const;

    /// Direct-P2P ingress is remote work by definition even though it lands on a local backend,
    /// so the resource-admission gate applies unconditionally here (unlike shared-store sync,
    /// which gates on `backend_->isRemote()`).
    [[nodiscard]] Result<void> beforeDirectIngress() const;

    static std::uint64_t nowMs() {
        return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                              std::chrono::system_clock::now().time_since_epoch())
                                              .count());
    }

    static std::string acknowledgementKey(std::string_view operationId, std::string_view peerId);

    static std::string blobKey(std::string_view hash);

    static std::string indexKey(std::string_view logicalKey, std::string_view hash);

    static std::string logicalKeyFromIndexKey(std::string_view key);

    static std::string recordHashFromIndexKey(std::string_view key);

    std::string historyScopeHash(std::string_view writerId) const;

    static std::string paddedCounter(std::uint64_t counter);

    Result<void> signControlArtifact(nlohmann::json& artifact, std::string_view domain) const;

    Result<void> verifyControlArtifact(const nlohmann::json& artifact, std::string_view domain,
                                       bool requireLocalSigner = true) const;

    std::string historyEntryKey(std::string_view writerId, std::uint64_t counter) const;

    std::string historyMigrationStateKey(std::string_view writerId) const;

    std::string historyMigrationCandidateKey(std::string_view writerId,
                                             std::uint64_t counter) const;

    Result<HistoryEntry> loadHistoryEntry(std::string_view writerId, std::uint64_t counter) const;

    Result<void> storeHistoryEntry(const MemoryIndexRecord& record, std::string_view recordHash,
                                   const WriterHistoryCommitment& commitment);

    Result<void> storeHistoryMigrationState(std::string_view writerId,
                                            const HistoryMigrationState& state);

    Result<std::optional<HistoryMigrationState>>
    loadHistoryMigrationState(std::string_view writerId) const;

    Result<void> storeHistoryMigrationCandidate(const HistoryEntry& candidate);

    Result<HistoryEntry> loadHistoryMigrationCandidate(std::string_view writerId,
                                                       std::uint64_t counter) const;

    Result<void> ensureHistoryEntries(std::string_view writerId, std::uint64_t targetCounter,
                                      bool forceRebuild = false);

    std::string replicationCheckpointKey() const;

    Result<void> ensureDurableQuarantineLoaded();

    Result<void>
    persistReplicationCheckpoint(const std::map<NodeId, WriterHistoryCommitment>& commitments,
                                 const std::map<std::string, std::string>& quarantinedWriters);

    std::string historySeed(std::string_view writerId) const;

    std::string advanceHistory(std::string_view writerId, const WriterHistoryCommitment& previous,
                               std::string_view recordHash, std::uint64_t counter) const;

    Result<void> commitScannedHistory(const std::vector<ScanCandidate>& candidates);
    /// One page of a complete sweep. Fails closed when the sweep cannot fit its bounds.
    Result<std::map<std::string, MemoryIndexRecord>> syncImpl();
    void noteSweepFailure(ErrorCode code, std::string_view reason);
    void clearSweepDegraded() noexcept;
    void discardSweep() noexcept;

    Result<WriterHistoryCommitment> computeHistoryCommitmentAt(std::string_view writerId,
                                                               std::uint64_t targetCounter);

    /// Record hash durable history committed to at `counter` (at most the writer's durable
    /// prefix), rebuilding missing history entries like computeHistoryCommitmentAt.
    Result<std::string> durableRecordHashAt(std::string_view writerId, std::uint64_t counter);

    Result<void> requireLocalHistoryCommitment() const;

    Result<void> commitRecordHistory(const MemoryIndexRecord& record, std::string_view recordHash);

    Result<void> commitDirectHistory(const MemoryDelta& delta, const ValidatedDelta& validated);

    Result<MemoryIndexRecord> makeQuarantineInvalidation(std::string_view logicalKey,
                                                         const MemoryIndexRecord& record);

    Result<void> captureQuarantineInvalidation(std::string_view logicalKey,
                                               const MemoryIndexRecord& record);

    void removeVisibleWinnersFrom(std::string_view writerId);

    void removeQuarantinedWinners();

    void pruneUnreferencedCachedBlobs();

    static std::string hashContent(std::span<const std::byte> content);

    static std::vector<std::byte> serialize(const MemoryIndexRecord& record);

    static Result<MemoryIndexRecord> deserialize(std::span<const std::byte> bytes);

    void quarantine(std::string_view key, std::string_view reason);

    void recordAuthFailure() noexcept;

    /// Verify once per distinct envelope. The key is the digest of the whole serialized
    /// envelope, signature included, so re-signing the same content cannot reuse an
    /// entry. Only successful verifications are remembered, and a full or failing cache
    /// falls back to verifying again.
    bool verifiedEnvelope(const std::string& envelopeHash, const MemoryIndexRecord& record);

    /// Forwards to the injected backend and counts every mutation the loop makes, so an
    /// exclusive backend's complete scan can be reused without enumerating write paths.
    /// Exclusive filesystem listings retain one bounded, sorted snapshot per prefix; other
    /// exclusive backends retain the old exact-page cache. Mutation/listing/cache access
    /// requires the service loop mutex. retrieve/isRemote only forward to the stable inner
    /// backend; native filesystem pinned reads may use those two concurrently.
    class WriteCountingBackend final : public storage::IStorageBackend {
    public:
        WriteCountingBackend(storage::IStorageBackend& inner, bool exclusive,
                             const MemorySyncLimits& limits)
            : inner_(inner), exclusive_(exclusive),
              maxRetainedKeys_(std::min(limits.maxIndexObjectsPerSync, kMaxKeysPerPage) *
                               kMaxCachedPages),
              maxRetainedKeyBytes_(std::min(limits.maxCacheBytes, kMaxRetainedKeyBytes)) {}

        // Keep the layout identical in library and test translation units; only the
        // YAMS_TESTING factory can install these otherwise-empty test controls.
        std::function<void(std::string_view)> testing_beforeCacheAllocation;
        bool testing_lexicalBackend{false};
        [[nodiscard]] std::uint64_t writes() const noexcept { return writes_; }
        [[nodiscard]] bool completeScanReuseEligible() const noexcept {
            return exclusive_ && !listingCacheDisabled_;
        }

        Result<void> initialize(const storage::BackendConfig& config) override {
            return inner_.initialize(config);
        }
        Result<void> store(std::string_view key, std::span<const std::byte> data) override {
            beforeMutation(key);
            auto stored = inner_.store(key, data);
            finishStore(key, stored.has_value());
            return stored;
        }
        Result<std::vector<std::byte>> retrieve(std::string_view key) const override {
            return inner_.retrieve(key);
        }
        Result<bool> exists(std::string_view key) const override { return inner_.exists(key); }
        Result<void> remove(std::string_view key) override {
            beforeMutation(key);
            auto removed = inner_.remove(key);
            finishRemove(key, removed.has_value());
            return removed;
        }
        Result<std::vector<std::string>> list(std::string_view prefix = "") const override {
            return inner_.list(prefix);
        }
        Result<storage::ObjectListPage> listPage(std::string_view prefix,
                                                 std::optional<std::string_view> cursor,
                                                 std::size_t limit) const override;
        Result<::yams::StorageStats> getStats() const override { return inner_.getStats(); }
        std::future<Result<void>> storeAsync(std::string_view key,
                                             std::span<const std::byte> data) override {
            beforeMutation(key);
            // The future may mutate the backend after this call returns. Never attach a callback
            // to wrapper state; permanently disable both listing and complete-scan reuse instead.
            listingCacheDisabled_ = true;
            clearListingCaches();
            return inner_.storeAsync(key, data);
        }
        std::future<Result<std::vector<std::byte>>>
        retrieveAsync(std::string_view key) const override {
            return inner_.retrieveAsync(key);
        }
        std::string getType() const override { return inner_.getType(); }
        bool isRemote() const override { return inner_.isRemote(); }
        Result<void> flush() override { return inner_.flush(); }
        Result<void> clear() override {
            incrementWrites();
            clearListingCaches();
            return inner_.clear();
        }
        void requestCancel() noexcept override { inner_.requestCancel(); }
        void resetCancel() noexcept override { inner_.resetCancel(); }

    private:
        static constexpr std::size_t kMaxCachedPages = 64;
        static constexpr std::size_t kMaxPrefixes = 64;
        static constexpr std::size_t kMaxKeysPerPage = 512;
        static constexpr std::size_t kMaxRetainedKeyBytes = std::size_t{8} * 1024 * 1024;

        struct PrefixListing {
            std::vector<std::string> keys;
            std::string expectedCursor;
            bool complete{false};
        };

        [[nodiscard]] bool filesystemSnapshotEligible() const;
        void incrementWrites();
        void beforeMutation(std::string_view key);
        void finishStore(std::string_view key, bool success);
        void finishRemove(std::string_view key, bool success);
        void clearExactPages() const;
        void clearListingCaches() const noexcept;
        void invalidateIncompleteListings(std::string_view key);
        void invalidateCompleteListings(std::string_view key);
        void updateCompleteListingsAfterStore(std::string_view key);
        void updateCompleteListingsAfterRemove(std::string_view key);
        [[nodiscard]] bool canRetainKeys(const std::vector<std::string>& keys) const;
        [[nodiscard]] bool canRetainKey(std::string_view key) const;
        void accountKeys(const std::vector<std::string>& keys) const;
        void unaccountKeys(const std::vector<std::string>& keys) const;
        void observeFilesystemPage(std::string_view prefix, std::optional<std::string_view> cursor,
                                   const storage::ObjectListPage& page) const;
        [[nodiscard]] storage::ObjectListPage
        pageFromSnapshot(const PrefixListing& listing, std::optional<std::string_view> cursor,
                         std::size_t limit) const;

        storage::IStorageBackend& inner_;
        bool exclusive_{false};
        bool listingCacheDisabled_{false};
        std::size_t maxRetainedKeys_{0};
        std::size_t maxRetainedKeyBytes_{0};
        std::uint64_t writes_{0};
        mutable std::size_t retainedKeyCount_{0};
        mutable std::size_t retainedKeyBytes_{0};
        mutable std::map<std::string, storage::ObjectListPage> pages_;
        mutable std::map<std::string, PrefixListing, std::less<>> prefixListings_;
    };

    /// Mutation count and quarantine generation observed when the last complete scan ended.
    struct CompleteScanStamp {
        std::uint64_t writes{0};
        std::uint64_t quarantineGeneration{0};
    };

    std::unique_ptr<WriteCountingBackend> countedBackend_;
    storage::IStorageBackend* backend_;
    std::optional<CompleteScanStamp> completeScan_;
    NodeId nodeId_;
    std::string corpusId_;
    std::uint64_t corpusEpoch_{0};
    bool allowLegacyUnbound_{false};
    MemorySyncLimits limits_;
    MemorySyncControl control_;
    TombstoneGcPolicy tombstoneGc_;
    std::shared_ptr<const WriterAuthenticator> writerAuth_;
    std::string controlScope_;
    VersionVector version_;
    std::uint64_t logicalClock_{0};
    std::map<std::string, MemoryIndexRecord> merged_;
    std::map<std::string, std::vector<std::byte>> cachedBlobs_;
    std::size_t cachedBytes_{0};
    std::string scanCursor_;
    std::map<std::string, std::pair<std::string, std::string>> operations_;
    std::map<std::string, std::pair<std::string, std::string>> vectorIdentities_;
    std::vector<ScanCandidate> scanCandidates_;
    std::filesystem::path sweepDirectory_;
    std::shared_ptr<SweepStore> sweepStore_;
    std::shared_ptr<SweepStore> committedStore_;
    std::size_t scanBytes_{0};
    /// A sweep that cannot satisfy its own bounds is an environmental condition, not a
    /// transient one. Retrying it on every caller attempt re-walks the whole store and
    /// pins a core without ever making progress, so record the failure and gate retries
    /// behind a bounded exponential backoff until a complete sweep succeeds again.
    bool sweepDegraded_{false};
    /// Set only by the sweep's own budget sites. A resource-governor denial reports
    /// ResourceExhausted too, but it is transient backpressure that must never be
    /// latched as an unsatisfiable sweep.
    bool sweepBoundExhausted_{false};
    ErrorCode sweepDegradedCode_{ErrorCode::ResourceExhausted};
    std::string sweepDegradedReason_;
    std::size_t sweepDegradedAttempts_{0};
    std::chrono::steady_clock::time_point nextSweepAttempt_{};
    std::size_t sweepAttempts_{0};
    std::set<std::string> forkedOperations_;
    std::map<std::string, std::pair<std::string, std::string>> deltaOperations_;
    std::deque<std::string> deltaOperationOrder_;
    std::map<std::string, std::string> quarantined_;
    std::map<std::string, std::string> durableQuarantinedWriters_;
    std::map<NodeId, WriterHistoryCommitment> historyCommitments_;
    std::map<std::string, MemoryIndexRecord> quarantineInvalidations_;
    std::map<std::string, MemoryIndexRecord> quarantineInvalidationSources_;
    bool durableQuarantineLoaded_{false};
    bool recoveringColdBootstrap_{false};
    std::uint64_t durableQuarantineGeneration_{0};
    std::size_t authFailures_{0};
    bool legacyUnauthenticatedHistory_{false};
    /// Positive-only memo of envelopes that already passed signature verification.
    /// Keyed by the digest of the whole serialized envelope, signature included, so a
    /// swapped signature cannot reuse an entry. Trust set, corpus, epoch and signature
    /// policy are immutable for a loop, so rebuilding the loop is the invalidation.
    std::unordered_set<std::string> verifiedEnvelopes_;
    /// Verification attempts, cache hits included. Unconditional so the object layout
    /// stays identical in library and test translation units.
    std::size_t fixedLayoutVerifyAttempts_{0};
};

#ifdef YAMS_TESTING
inline std::unique_ptr<storage::IStorageBackend> MemorySyncLoop::testing_countedBackend(
    storage::IStorageBackend& backend, bool exclusive, MemorySyncLimits limits,
    std::function<void(std::string_view)> beforeCacheAllocation, bool lexicalTestBackend) {
    auto counted = std::make_unique<WriteCountingBackend>(backend, exclusive, limits);
    counted->testing_beforeCacheAllocation = std::move(beforeCacheAllocation);
    counted->testing_lexicalBackend = lexicalTestBackend;
    return counted;
}
#endif

} // namespace yams::memory_sync
