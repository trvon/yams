// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <limits>
#include <map>
#include <set>
#include <span>
#include <string>
#include <string_view>
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
    std::function<bool()> isCancelled;
    std::function<bool()> canAdmitRemoteWork;
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
    /// Maximum legacy index objects inspected while building missing counter entries.
    std::size_t maxHistoryMigrationObjects{8192};
    /// Maximum legacy envelope bytes retained while rebuilding one writer's counter index.
    std::size_t maxHistoryMigrationBytes{std::size_t{64} * 1024 * 1024};
};

struct MemoryDelta {
    std::string logicalKey;
    MemoryIndexRecord record;
    // pi-lens-ignore: no-bit-fields
    std::vector<std::byte> payload; // NOLINT(no-bit-fields) -- empty for tombstones
};

struct MemoryDeltaBatch {
    std::vector<MemoryDelta> deltas;
    bool hasMore{false};
};

struct DeltaApplyResult {
    std::size_t received{0};
    std::size_t merged{0};
    std::size_t replayed{0};
    std::map<std::string, std::string> quarantined;
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

struct ReplicationState {
    VersionVector version;
    // pi-lens-ignore: no-bit-fields
    std::map<NodeId, WriterHistoryCommitment> commitments; // NOLINT(no-bit-fields)
    std::set<NodeId> quarantinedWriters;
};

/// Authenticated direct-P2P zero-state snapshot. The serving peer is a witness for the exact
/// frozen frontier; winner envelopes retain their original writer signatures.
struct ColdBootstrapSnapshot {
    VersionVector frontier;
    // pi-lens-ignore: no-bit-fields
    std::map<NodeId, WriterHistoryCommitment> commitments; // NOLINT(no-bit-fields)
    std::vector<MemoryDelta> winners;
    std::string rootDigest;
    DetachedWriterSignature witnessSignature;
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
        : backend_(&backend), nodeId_(std::move(nodeId)), corpusId_(std::move(corpusId)),
          corpusEpoch_(corpusEpoch), allowLegacyUnbound_(allowLegacyUnbound), limits_(limits),
          control_(std::move(control)), tombstoneGc_(std::move(tombstoneGc)),
          writerAuth_(std::move(writerAuth)),
          controlScope_(controlScope.empty() ? corpusId_ + ":" + std::to_string(corpusEpoch_)
                                             : std::move(controlScope)) {}

    /// Publish `content` under `logicalKey`. Stores the content-addressed blob and
    /// an index record stamped with this node's version vector and hybrid timestamp.
    Result<void> publish(std::string_view logicalKey, std::span<const std::byte> content);

    /// Publish a causally ordered deletion through the durable local outbox. The exact
    /// prepared envelope remains replayable until its index/history commit succeeds.
    Result<void> erase(std::string_view logicalKey, std::string tombstonePayload = {});

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

    /// Validate a complete bounded incoming session against the authenticated writer's
    /// advertised frontier before any operation becomes visible.
    Result<void> validateHistoryExtension(std::span<const MemoryDelta> deltas,
                                          const WriterHistoryCommitment& expectedFrontier);

    [[nodiscard]] bool writerQuarantined(std::string_view writerId) const noexcept;
    [[nodiscard]] std::uint64_t durableQuarantineGeneration() const noexcept {
        return durableQuarantineGeneration_;
    }

    /// Durably invalidate one writer before removing any visible winners. The checkpoint is
    /// persisted first so a crash cannot make invalidated history visible again after restart.
    Result<bool> quarantineWriter(std::string_view writerId, std::string_view sourceNodeId);

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
    struct CommittedState {
        // pi-lens-ignore: no-bit-fields
        std::map<std::string, MemoryIndexRecord> merged; // NOLINT(no-bit-fields)
        // pi-lens-ignore: no-bit-fields
        std::map<std::string, std::vector<std::byte>> cachedBlobs; // NOLINT(no-bit-fields)
        // pi-lens-ignore: no-bit-fields
        std::map<std::string, std::string> quarantined; // NOLINT(no-bit-fields)
        std::size_t authFailures{0};
        ReplicationState replication;
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
        return merged_.contains(std::string(logicalKey));
    }
    [[nodiscard]] std::size_t testingCachedBlobCount() const noexcept {
        return cachedBlobs_.size();
    }
#endif

    Result<std::vector<std::byte>> read(std::string_view logicalKey);

private:
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

    bool commitDirectDelta(const MemoryDelta& delta, const DirectWinnerPlan& plan);

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
        std::string prefixDigest;
    };

    struct HistoryMigrationState {
        std::string phase{"scan"};
        std::string cursor;
        std::uint64_t targetCounter{0};
        std::uint64_t nextCounter{1};
        std::string prefixDigest;
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

    Result<void> commitPreparedErase(const EraseIntentState& intent);

    Result<void> replayReadyErasesAfterReconcile();

    Result<void> reconcileBeforeWrite(bool allowDeferredPreparedErase = false);

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

    Result<WriterHistoryCommitment> computeHistoryCommitmentAt(std::string_view writerId,
                                                               std::uint64_t targetCounter);

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

    storage::IStorageBackend* backend_;
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
};

} // namespace yams::memory_sync
