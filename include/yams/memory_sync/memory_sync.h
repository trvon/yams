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
                   std::shared_ptr<const WriterAuthenticator> writerAuth = {})
        : backend_(&backend), nodeId_(std::move(nodeId)), corpusId_(std::move(corpusId)),
          corpusEpoch_(corpusEpoch), allowLegacyUnbound_(allowLegacyUnbound), limits_(limits),
          control_(std::move(control)), tombstoneGc_(std::move(tombstoneGc)),
          writerAuth_(std::move(writerAuth)) {}

    /// Publish `content` under `logicalKey`. Stores the content-addressed blob and
    /// an index record stamped with this node's version vector and hybrid timestamp.
    Result<void> publish(std::string_view logicalKey, std::span<const std::byte> content) {
        if (auto valid = validateLogicalKey(logicalKey); !valid) {
            return valid.error();
        }
        if (content.size() > limits_.maxValueBytes || content.size() > limits_.maxCacheBytes) {
            return Error{ErrorCode::InvalidArgument,
                         "memory sync value exceeds configured resident limit"};
        }

        // Reconcile first so this node's version vector and logical clock resume from
        // the last known state. Preserves causal ordering across fresh loop instances
        // (e.g. a stateless CLI process publishing after a prior one).
        if (auto reconciled = reconcileBeforeWrite(); !reconciled) {
            return reconciled.error();
        }
        if (writerQuarantined(nodeId_)) {
            return Error{ErrorCode::InvalidState, "quarantined writer cannot publish"};
        }
        if (auto committed = requireLocalHistoryCommitment(); !committed) {
            return committed.error();
        }

        const std::string hash = hashContent(content);
        if (auto ready = beforeRemoteWork(); !ready) {
            return ready.error();
        }
        const auto existingBlob = backend_->exists(blobKey(hash));
        if (!existingBlob) {
            return existingBlob.error();
        }
        if (!existingBlob.value()) {
            if (auto r = backend_->store(blobKey(hash), content); !r) {
                return r.error();
            }
        }
        if (nodeId_.empty() || nodeId_ == "default" || corpusId_.empty() || corpusEpoch_ == 0) {
            return Error{ErrorCode::InvalidArgument, "memory sync identity is not configured"};
        }
        if (!version_.increment(nodeId_)) {
            return Error{ErrorCode::InvalidState, "memory sync writer counter exhausted"};
        }
        ++logicalClock_;

        MemoryIndexRecord record;
        record.entryHash = hash;
        record.ts.physicalMs = nowMs();
        record.ts.logical = logicalClock_;
        record.origin = nodeId_;
        record.vv = version_;
        record.corpusId = corpusId_;
        record.corpusEpoch = corpusEpoch_;
        record.operationId = makeOperationId(nodeId_, version_.get(nodeId_));
        record.logicalKey = logicalKey;
        record.recordKind = std::string(kMemoryValueRecordKind);
        if (writerAuth_) {
            if (auto signedRecord = writerAuth_->sign(record); !signedRecord) {
                return signedRecord.error();
            }
        }

        const auto recordBytes = serialize(record);
        const std::string recordHash = hashContent(recordBytes);
        if (auto ready = beforeRemoteWork(); !ready) {
            return ready.error();
        }
        if (auto r = backend_->store(indexKey(logicalKey, recordHash), recordBytes); !r) {
            return r.error();
        }
        return commitRecordHistory(record, recordHash);
    }

    /// Publish a causally ordered deletion through the durable local outbox. The exact
    /// prepared envelope remains replayable until its index/history commit succeeds.
    Result<void> erase(std::string_view logicalKey, std::string tombstonePayload = {}) {
        if (auto staged = stageErase(logicalKey, tombstonePayload, true); !staged) {
            return staged.error();
        }
        return publishStagedErase(logicalKey);
    }

    using EraseReadyValidator = std::function<Result<bool>()>;

    /// Persist an erase intent before a separate local deletion. `ready=false` intents are never
    /// published by generic retry; the owner promotes them only after observing local absence.
    Result<void> stageErase(std::string_view logicalKey, std::string tombstonePayload,
                            bool ready = false,
                            EraseReadinessProbe readinessProbe = EraseReadinessProbe::Explicit) {
        if (auto valid = validateLogicalKey(logicalKey); !valid) {
            return valid.error();
        }
        if (tombstonePayload.size() > limits_.maxEnvelopeBytes) {
            return Error{ErrorCode::InvalidArgument,
                         "memory sync tombstone payload exceeds envelope limit"};
        }
        if (nodeId_.empty() || nodeId_ == "default" || corpusId_.empty() || corpusEpoch_ == 0) {
            return Error{ErrorCode::InvalidArgument, "memory sync identity is not configured"};
        }
        auto existing = loadEraseIntent(logicalKey);
        if (!existing) {
            return existing.error();
        }
        EraseIntentState intent;
        if (existing.value()) {
            if (existing.value()->tombstonePayload != tombstonePayload ||
                existing.value()->readinessProbe != readinessProbe) {
                return Error{ErrorCode::InvalidState,
                             "staged erase payload conflicts with retained intent"};
            }
            intent = std::move(*existing.value());
            if (!ready || intent.ready) {
                return {};
            }
        } else {
            intent.logicalKey = logicalKey;
            intent.tombstonePayload = std::move(tombstonePayload);
            intent.readinessProbe = readinessProbe;
            if (!ready) {
                if (writerAuth_) {
                    if (auto reconciled = reconcileBeforeWrite(); !reconciled) {
                        return reconciled.error();
                    }
                    auto authorization = prepareEraseRecord(
                        intent.logicalKey,
                        eraseAuthorizationPayload(intent.tombstonePayload, readinessProbe));
                    if (!authorization) {
                        return authorization.error();
                    }
                    intent.authorization = std::move(authorization.value());
                }
                return storeEraseIntent(intent);
            }
        }
        if (auto reconciled = reconcileBeforeWrite(); !reconciled) {
            return reconciled.error();
        }
        if (writerAuth_ && !intent.authorization) {
            auto authorization = prepareEraseRecord(
                intent.logicalKey,
                eraseAuthorizationPayload(intent.tombstonePayload, intent.readinessProbe));
            if (!authorization) {
                return authorization.error();
            }
            intent.authorization = std::move(authorization.value());
        }
        auto prepared = prepareEraseRecord(intent.logicalKey, intent.tombstonePayload);
        if (!prepared) {
            return prepared.error();
        }
        intent.ready = true;
        intent.record = std::move(prepared.value());
        intent.recordHash = hashContent(serialize(*intent.record));
        auto commitment = preparedEraseCommitment(*intent.record, intent.recordHash);
        if (!commitment) {
            return commitment.error();
        }
        intent.preparedCommitment = std::move(commitment.value());
        return storeEraseIntent(intent);
    }

    Result<std::vector<PendingEraseIntent>> pendingErases() {
        auto loaded = loadEraseIntents();
        if (!loaded) {
            return loaded.error();
        }
        std::vector<PendingEraseIntent> pending;
        pending.reserve(loaded.value().size());
        for (const auto& intent : loaded.value()) {
            pending.push_back(PendingEraseIntent{
                .logicalKey = intent.logicalKey,
                .tombstonePayload = intent.tombstonePayload,
                .readinessProbe = intent.readinessProbe,
                .ready = intent.ready,
                .prepared = intent.record.has_value(),
                .preparedCounter = intent.record ? intent.record->vv.get(nodeId_) : 0});
        }
        return pending;
    }

    /// Promote and publish one staged intent. Once prepared, the exact signed envelope is reused
    /// across every retry so an outbox cleanup failure cannot create a later deletion.
    Result<void> publishStagedErase(std::string_view logicalKey,
                                    EraseReadyValidator validator = {}) {
        auto loaded = loadEraseIntent(logicalKey);
        if (!loaded) {
            return loaded.error();
        }
        if (!loaded.value()) {
            return Error{ErrorCode::NotFound, "staged erase intent is missing"};
        }
        auto intent = std::move(*loaded.value());
        if (!intent.ready) {
            if (auto reconciled = reconcileBeforeWrite(true); !reconciled) {
                return reconciled.error();
            }
            auto allIntents = loadEraseIntents();
            if (!allIntents) {
                return allIntents.error();
            }
            const bool earlierPrepared =
                std::ranges::any_of(allIntents.value(), [&](const EraseIntentState& candidate) {
                    return candidate.logicalKey != intent.logicalKey && candidate.ready &&
                           candidate.record.has_value();
                });
            if (earlierPrepared) {
                return Error{ErrorCode::InvalidState,
                             "an earlier prepared deletion must commit or cancel first"};
            }
            if (writerAuth_ && !intent.authorization) {
                return Error{ErrorCode::Unauthorized,
                             "erase intent lost its signed readiness authorization"};
            }
            auto prepared = prepareEraseRecord(intent.logicalKey, intent.tombstonePayload);
            if (!prepared) {
                return prepared.error();
            }
            intent.ready = true;
            intent.record = std::move(prepared.value());
            intent.recordHash = hashContent(serialize(*intent.record));
            auto commitment = preparedEraseCommitment(*intent.record, intent.recordHash);
            if (!commitment) {
                return commitment.error();
            }
            intent.preparedCommitment = std::move(commitment.value());
            if (auto stored = storeEraseIntent(intent); !stored) {
                return stored.error();
            }
        } else if (auto reconciled = reconcileBeforeWrite(true); !reconciled) {
            return reconciled.error();
        }
        auto retained = loadEraseIntent(logicalKey);
        if (!retained) {
            return retained.error();
        }
        if (!retained.value()) {
            return {};
        }
        intent = std::move(*retained.value());
        const auto durable = historyCommitments_.find(nodeId_);
        const bool pointOfNoReturn =
            durable != historyCommitments_.end() && durable->second == intent.preparedCommitment;
        if (validator && !pointOfNoReturn) {
            auto allowed = validator();
            if (!allowed) {
                return allowed.error();
            }
            if (!allowed.value()) {
                return backend_->remove(eraseOutboxKey(logicalKey));
            }
        }
        return commitPreparedErase(intent);
    }

    Result<void> cancelStagedErase(std::string_view logicalKey) {
        if (auto loaded = ensureDurableQuarantineLoaded(); !loaded) {
            return loaded.error();
        }
        auto intent = loadEraseIntent(logicalKey);
        if (!intent) {
            return intent.error();
        }
        if (!intent.value()) {
            return {};
        }
        if (intent.value()->ready && !intent.value()->record) {
            return Error{ErrorCode::InvalidState, "ready erase intent lacks prepared envelope"};
        }
        const auto durable = historyCommitments_.find(nodeId_);
        if (intent.value()->record && durable != historyCommitments_.end() &&
            durable->second == intent.value()->preparedCommitment) {
            // The full-history frontier already includes this exact operation. Cancellation is no
            // longer safe; finish its idempotent index publication instead.
            return commitPreparedErase(*intent.value());
        }
        return backend_->remove(eraseOutboxKey(logicalKey));
    }

    /// Retry only intents whose owning local deletion has already been confirmed.
    Result<std::size_t> replayReadyErases() {
        auto before = loadEraseIntents();
        if (!before) {
            return before.error();
        }
        const auto ready =
            std::ranges::count_if(before.value(), [](const EraseIntentState& intent) {
                return intent.ready && intent.readinessProbe == EraseReadinessProbe::Explicit;
            });
        if (ready == 0) {
            return std::size_t{0};
        }
        if (auto replayed = reconcileBeforeWrite(true); !replayed) {
            return replayed.error();
        }
        return static_cast<std::size_t>(ready);
    }

    /// Reconcile against the backend: list the index, pull every record, LWW-merge
    /// per logical key, and fold remote histories into this node's version vector.
    /// Returns the merged `logicalKey -> winning record` map.
    Result<std::map<std::string, MemoryIndexRecord>> sync() {
        if (auto loaded = ensureDurableQuarantineLoaded(); !loaded) {
            return loaded.error();
        }
        if (auto ready = beforeRemoteWork(); !ready) {
            return ready.error();
        }
        auto listed = backend_->listPage(
            "index/",
            scanCursor_.empty() ? std::nullopt : std::optional<std::string_view>{scanCursor_},
            limits_.maxIndexObjectsPerSync);
        if (!listed) {
            return listed.error();
        }

        std::vector<ScanCandidate> pageCandidates;
        quarantined_.clear();

        auto page = std::move(listed.value());
        auto keys = std::move(page.keys);
        if (scanCursor_.empty()) {
            operations_.clear();
            vectorIdentities_.clear();
            forkedOperations_.clear();
            scanCandidates_.clear();
        }
        std::ranges::sort(keys);

        for (auto keyIt = keys.begin(); keyIt != keys.end(); ++keyIt) {
            const auto& key = *keyIt;
            const std::string logicalKey = logicalKeyFromIndexKey(key);
            const std::string expectedRecordHash = recordHashFromIndexKey(key);
            if (logicalKey.empty() || !isSha256Digest(expectedRecordHash)) {
                quarantine(key, "invalid index key shape");
                continue;
            }
            if (auto ready = beforeRemoteWork(); !ready) {
                return ready.error();
            }
            auto fetched = backend_->retrieve(key);
            if (!fetched) {
                quarantine(key, fetched.error().message);
                continue;
            }
            if (fetched.value().size() > limits_.maxEnvelopeBytes) {
                quarantine(key, "index envelope exceeds configured size limit");
                continue;
            }
            if (hashContent(fetched.value()) != expectedRecordHash) {
                quarantine(key, "index envelope digest mismatch");
                continue;
            }
            auto record = deserialize(fetched.value());
            if (!record) {
                quarantine(key, record.error().message);
                continue;
            }
            std::string candidateRecordHash = expectedRecordHash;
            if (record.value().schemaVersion < kMemoryIndexSchemaVersion) {
                if (!allowLegacyUnbound_ || (writerAuth_ && writerAuth_->required())) {
                    quarantine(key, "legacy envelope lacks authenticated corpus binding");
                    continue;
                }
                if (record.value().origin.empty() ||
                    record.value().vv.get(record.value().origin) == 0) {
                    quarantine(key, "legacy envelope has invalid causal origin");
                    continue;
                }
                record.value().schemaVersion = kMemoryIndexSchemaVersion;
                record.value().corpusId = corpusId_;
                record.value().corpusEpoch = corpusEpoch_;
                record.value().logicalKey = logicalKey;
                record.value().recordKind = std::string(kMemoryValueRecordKind);
                record.value().operationId = makeOperationId(
                    record.value().origin, record.value().vv.get(record.value().origin));
                const auto upgradedBytes = serialize(record.value());
                candidateRecordHash = hashContent(upgradedBytes);
                const auto upgradedKey = indexKey(logicalKey, candidateRecordHash);
                const auto upgradedExists = backend_->exists(upgradedKey);
                if (!upgradedExists) {
                    quarantine(key, upgradedExists.error().message);
                    continue;
                }
                if (!upgradedExists.value()) {
                    if (auto upgraded = backend_->store(upgradedKey, upgradedBytes); !upgraded) {
                        quarantine(key, upgraded.error().message);
                        continue;
                    }
                }
            }
            const bool authenticatedSchema =
                record.value().schemaVersion == kAuthenticatedMemoryIndexSchemaVersion;
            const bool authenticationRequired = writerAuth_ && writerAuth_->required();
            if ((authenticatedSchema || authenticationRequired) &&
                (!writerAuth_ || !writerAuth_->verify(record.value()))) {
                recordAuthFailure();
                quarantine(key, "writer authentication failed");
                continue;
            }
            if (!record.value().hasValidCausalIdentity(corpusId_, corpusEpoch_)) {
                quarantine(key,
                           "record identity does not match configured corpus or causal origin");
                continue;
            }

            // Operation IDs are writer-global within a corpus epoch. Observe them
            // before validating the index logical key so a copied envelope cannot
            // leave the correctly placed sibling accepted as a valid operation.
            if (!operations_.contains(record.value().operationId) &&
                operations_.size() >= limits_.maxTrackedIdentities) {
                quarantine(key, "operation identity tracking limit reached");
                continue;
            }
            const auto [operation, inserted] = operations_.emplace(
                record.value().operationId, std::pair{candidateRecordHash, logicalKey});
            if (!inserted && operation->second != std::pair{candidateRecordHash, logicalKey}) {
                forkedOperations_.insert(record.value().operationId);
            }

            if (!record.value().hasValidIdentity(corpusId_, corpusEpoch_, logicalKey)) {
                quarantine(key, "record logical identity does not match its index key");
                continue;
            }
            const std::string vectorIdentity = nlohmann::json(record.value().vv).dump();
            if (!vectorIdentities_.contains(vectorIdentity) &&
                vectorIdentities_.size() >= limits_.maxTrackedIdentities) {
                quarantine(key, "vector identity tracking limit reached");
                continue;
            }
            const auto [vector, vectorInserted] = vectorIdentities_.emplace(
                vectorIdentity, std::pair{candidateRecordHash, record.value().operationId});
            if (!vectorInserted && vector->second.first != candidateRecordHash) {
                forkedOperations_.insert(vector->second.second);
                forkedOperations_.insert(record.value().operationId);
            }
            pageCandidates.push_back(
                ScanCandidate{key, logicalKey, candidateRecordHash, std::move(record.value())});
        }

        scanCandidates_.insert(scanCandidates_.end(),
                               std::make_move_iterator(pageCandidates.begin()),
                               std::make_move_iterator(pageCandidates.end()));
        scanCursor_ = page.nextCursor.value_or(std::string{});
        if (!scanCursor_.empty()) {
            // A bounded page is not a complete security decision. Keep the last committed
            // snapshot visible until the sweep has observed every operation/fork identity.
            return merged_;
        }

        auto candidates = std::move(scanCandidates_);
        scanCandidates_.clear();
        for (const auto& candidate : candidates) {
            if (writerQuarantined(candidate.record.origin)) {
                if (auto captured =
                        captureQuarantineInvalidation(candidate.logicalKey, candidate.record);
                    !captured) {
                    return captured.error();
                }
            }
        }
        if (auto committed = commitScannedHistory(candidates); !committed) {
            return committed.error();
        }
        const auto previousMerged = merged_;
        auto merged = merged_;
        for (const auto& candidate : candidates) {
            if (writerQuarantined(candidate.record.origin)) {
                quarantine(candidate.indexKey, "writer is durably quarantined");
                continue;
            }
            if (forkedOperations_.contains(candidate.record.operationId)) {
                quarantine(candidate.indexKey, "duplicate writer operation fork");
                continue;
            }
            version_.merge(candidate.record.vv);
            const auto it = merged.find(candidate.logicalKey);
            if (it == merged.end()) {
                if (merged.size() >= limits_.maxMergedKeys) {
                    quarantine(candidate.indexKey, "merged key limit reached");
                    continue;
                }
                merged[candidate.logicalKey] = candidate.record;
            } else if (resolveLww(candidate.record, it->second) == LwwDecision::First) {
                merged[candidate.logicalKey] = candidate.record;
            }
        }

        // Forks and durable writer quarantine invalidate earlier winners.
        for (auto it = merged.begin(); it != merged.end();) {
            if (writerQuarantined(it->second.origin) ||
                forkedOperations_.contains(it->second.operationId)) {
                it = merged.erase(it);
            } else {
                ++it;
            }
        }

        // Hydrate only selected winners. Historical losers never trigger blob reads.
        for (auto it = merged.begin(); it != merged.end();) {
            if (it->second.isTombstone()) {
                ++it;
                continue;
            }
            const auto& hash = it->second.entryHash;
            if (cachedBlobs_.contains(hash)) {
                ++it;
                continue;
            }
            if (auto ready = beforeRemoteWork(); !ready) {
                return ready.error();
            }
            auto blob = backend_->retrieve(blobKey(hash));
            const auto previous = previousMerged.find(it->first);
            auto oldCached = cachedBlobs_.end();
            bool oldWinnerIsShared = false;
            if (previous != previousMerged.end() && previous->second.entryHash != hash) {
                oldCached = cachedBlobs_.find(previous->second.entryHash);
                oldWinnerIsShared = std::ranges::any_of(merged, [&](const auto& entry) {
                    return entry.first != it->first &&
                           entry.second.entryHash == previous->second.entryHash;
                });
            }
            std::size_t projectedCacheBytes = cachedBytes_ + (blob ? blob.value().size() : 0);
            if (oldCached != cachedBlobs_.end() && !oldWinnerIsShared) {
                projectedCacheBytes -= oldCached->second.size();
            }
            const bool valid = blob && blob.value().size() <= limits_.maxValueBytes &&
                               hashContent(blob.value()) == hash;
            if (!valid || projectedCacheBytes > limits_.maxCacheBytes) {
                quarantine("index/" + it->first,
                           !blob ? blob.error().message
                                 : "winner blob exceeds configured size or cache limit");
                if (previous != previousMerged.end() &&
                    cachedBlobs_.contains(previous->second.entryHash)) {
                    it->second = previous->second;
                    ++it;
                } else {
                    it = merged.erase(it);
                }
                continue;
            }
            if (oldCached != cachedBlobs_.end() && !oldWinnerIsShared) {
                cachedBytes_ -= oldCached->second.size();
                cachedBlobs_.erase(oldCached);
            }
            cachedBytes_ += blob.value().size();
            cachedBlobs_.emplace(hash, std::move(blob.value()));
            ++it;
        }

        merged_ = std::move(merged);
        for (const auto& [_, record] : merged_) {
            if (record.isTombstone() && tombstoneGc_.requiredPeers.contains(nodeId_)) {
                if (auto ready = beforeRemoteWork(); !ready) {
                    return ready.error();
                }
                if (auto acknowledged =
                        backend_->store(acknowledgementKey(record.operationId, nodeId_),
                                        std::span<const std::byte>{});
                    !acknowledged) {
                    return acknowledged.error();
                }
            }
        }
        std::set<std::string> winningHashes;
        for (const auto& [_, record] : merged_) {
            if (!record.isTombstone()) {
                winningHashes.insert(record.entryHash);
            }
        }
        for (auto it = cachedBlobs_.begin(); it != cachedBlobs_.end();) {
            if (!winningHashes.contains(it->first)) {
                cachedBytes_ -= it->second.size();
                it = cachedBlobs_.erase(it);
            } else {
                ++it;
            }
        }
        return merged_;
    }

    /// Complete the current bounded sweep and return a full committed view.
    /// Direct mode calls this against its local durable op store at startup.
    Result<std::map<std::string, MemoryIndexRecord>> syncFully() {
        for (;;) {
            auto result = sync();
            if (!result || scanCursor_.empty()) {
                return result;
            }
        }
    }

    /// Export this writer's durable operations newer than the peer's causal
    /// watermark. The backend is local in direct mode, so this replaces remote
    /// shared-namespace scans with a bounded local op-store read.
    Result<MemoryDeltaBatch> exportLocalDeltasAfter(
        const VersionVector& peerVersion, std::size_t maxDeltas = 128,
        std::uint64_t maxWriterCounter = std::numeric_limits<std::uint64_t>::max()) {
        if (auto loaded = ensureDurableQuarantineLoaded(); !loaded) {
            return loaded.error();
        }
        if (writerQuarantined(nodeId_)) {
            return Error{ErrorCode::InvalidState, "quarantined writer cannot export deltas"};
        }
        if (maxDeltas == 0) {
            return Error{ErrorCode::InvalidArgument, "direct delta batch size must be positive"};
        }
        std::vector<MemoryDelta> candidates;
        std::optional<std::string> cursor;
        for (;;) {
            auto listed = backend_->listPage(
                "index/", cursor ? std::optional<std::string_view>{*cursor} : std::nullopt,
                limits_.maxIndexObjectsPerSync);
            if (!listed) {
                return listed.error();
            }
            auto page = std::move(listed.value());
            for (const auto& key : page.keys) {
                auto delta = loadLocalDelta(key, peerVersion);
                if (!delta) {
                    return delta.error();
                }
                if (delta.value()) {
                    candidates.push_back(std::move(*delta.value()));
                }
            }
            cursor = std::move(page.nextCursor);
            if (!cursor) {
                break;
            }
        }
        std::erase_if(candidates, [&](const MemoryDelta& delta) {
            return delta.record.vv.get(nodeId_) > maxWriterCounter;
        });
        std::ranges::sort(candidates, [](const MemoryDelta& lhs, const MemoryDelta& rhs) {
            const auto leftCounter = lhs.record.vv.get(lhs.record.origin);
            const auto rightCounter = rhs.record.vv.get(rhs.record.origin);
            return std::tie(leftCounter, lhs.record.operationId) <
                   std::tie(rightCounter, rhs.record.operationId);
        });
        const bool hasMore = candidates.size() > maxDeltas;
        if (hasMore) {
            candidates.resize(maxDeltas);
        }
        return MemoryDeltaBatch{.deltas = std::move(candidates), .hasMore = hasMore};
    }

    /// Apply peer-delivered operations directly through the same causal/LWW
    /// state used by shared-store reconciliation. Valid operations are persisted
    /// to the local backend before becoming visible; replay is idempotent, recent
    /// operation forks are quarantined, and causal gaps/dependency gaps fail closed.
    Result<DeltaApplyResult> applyDeltas(std::span<const MemoryDelta> deltas) {
        if (auto loaded = ensureDurableQuarantineLoaded(); !loaded) {
            return loaded.error();
        }
        std::set<std::string> projectedKeys;
        for (const auto& [key, record] : merged_) {
            (void)record;
            projectedKeys.insert(key);
        }
        for (const auto& delta : deltas) {
            projectedKeys.insert(delta.logicalKey);
            if (projectedKeys.size() > limits_.maxMergedKeys) {
                return Error{ErrorCode::ResourceExhausted,
                             "direct deltas exceed configured merged-key limit"};
            }
        }

        DeltaApplyResult result;
        result.received = deltas.size();
        quarantined_.clear();
        std::vector<const MemoryDelta*> ordered;
        ordered.reserve(deltas.size());
        for (const auto& delta : deltas) {
            ordered.push_back(&delta);
        }
        std::ranges::sort(ordered, [](const MemoryDelta* lhs, const MemoryDelta* rhs) {
            return std::tuple{lhs->record.origin, lhs->record.vv.get(lhs->record.origin)} <
                   std::tuple{rhs->record.origin, rhs->record.vv.get(rhs->record.origin)};
        });

        for (const auto* delta : ordered) {
            if (writerQuarantined(delta->record.origin)) {
                result.quarantined[delta->record.operationId] = "writer is durably quarantined";
                continue;
            }
            auto validated = validateDirectDelta(*delta);
            if (!validated) {
                quarantineDirectDelta(*delta, validated.error().message, result);
                continue;
            }
            const auto decision = classifyDirectDelta(*delta, validated.value());
            if (decision.action == DirectDeltaAction::Replay) {
                ++result.replayed;
                continue;
            }
            if (decision.action == DirectDeltaAction::Reject) {
                if (decision.reason == "duplicate writer operation fork") {
                    auto durable = quarantineWriter(delta->record.origin, nodeId_);
                    if (!durable) {
                        return durable.error();
                    }
                }
                quarantineDirectDelta(*delta, decision.reason, result);
                continue;
            }
            auto plan = planDirectWinner(*delta);
            if (!plan) {
                quarantineDirectDelta(*delta, plan.error().message, result);
                continue;
            }
            if (auto persisted = persistDirectDelta(*delta, validated.value()); !persisted) {
                return persisted.error();
            }
            if (auto committed = commitDirectHistory(*delta, validated.value()); !committed) {
                return committed.error();
            }
            rememberDirectOperation(*delta, validated.value());
            result.merged += commitDirectDelta(*delta, plan.value()) ? 1U : 0U;
        }
        result.version = version_;
        return result;
    }

    [[nodiscard]] VersionVector currentVersion() const { return version_; }

    [[nodiscard]] ReplicationState replicationState() const {
        std::set<NodeId> quarantined;
        for (const auto& [writer, _] : durableQuarantinedWriters_) {
            quarantined.insert(writer);
        }
        return ReplicationState{.version = version_,
                                .commitments = historyCommitments_,
                                .quarantinedWriters = std::move(quarantined)};
    }

    /// Resolve a constant-size commitment for this writer's exact historical prefix without
    /// hydrating payload blobs. Used only by the authenticated handshake proof round.
    Result<WriterHistoryCommitment> localHistoryCommitmentAt(std::uint64_t counter) {
        if (counter == 0) {
            return Error{ErrorCode::InvalidArgument,
                         "zero history prefix has no materialized commitment"};
        }
        if (auto loaded = ensureDurableQuarantineLoaded(); !loaded) {
            return loaded.error();
        }
        return computeHistoryCommitmentAt(nodeId_, counter);
    }

    /// Validate a complete bounded incoming session against the authenticated writer's
    /// advertised frontier before any operation becomes visible.
    Result<void> validateHistoryExtension(std::span<const MemoryDelta> deltas,
                                          const WriterHistoryCommitment& expectedFrontier) {
        if (deltas.empty()) {
            return Error{ErrorCode::InvalidArgument, "history extension is empty"};
        }
        if (auto loaded = ensureDurableQuarantineLoaded(); !loaded) {
            return loaded.error();
        }
        const auto& writer = deltas.front().record.origin;
        if (writer.empty() || writerQuarantined(writer) ||
            !isSha256Digest(expectedFrontier.digest)) {
            return Error{ErrorCode::InvalidState,
                         "history extension writer or frontier is invalid"};
        }
        auto commitment = historyCommitments_.contains(writer) ? historyCommitments_.at(writer)
                                                               : WriterHistoryCommitment{};
        if (commitment.counter >= expectedFrontier.counter) {
            const bool exceedsFrozenFrontier =
                std::ranges::any_of(deltas, [&](const MemoryDelta& delta) {
                    return delta.record.vv.get(writer) > expectedFrontier.counter;
                });
            if (exceedsFrozenFrontier) {
                return Error{ErrorCode::InvalidData,
                             "staged delta exceeds handshake-frozen writer frontier"};
            }
            auto existing = computeHistoryCommitmentAt(writer, expectedFrontier.counter);
            if (!existing) {
                return existing.error();
            }
            return existing.value() == expectedFrontier
                       ? Result<void>{}
                       : Result<void>{
                             Error{ErrorCode::HashMismatch,
                                   "existing writer history does not match advertised frontier"}};
        }
        std::vector<const MemoryDelta*> ordered;
        ordered.reserve(deltas.size());
        for (const auto& delta : deltas) {
            if (delta.record.origin != writer) {
                return Error{ErrorCode::Unauthorized,
                             "history extension mixes authenticated writers"};
            }
            ordered.push_back(&delta);
        }
        std::ranges::sort(ordered, [&](const MemoryDelta* lhs, const MemoryDelta* rhs) {
            return lhs->record.vv.get(writer) < rhs->record.vv.get(writer);
        });
        for (const auto* delta : ordered) {
            auto validated = validateDirectDelta(*delta);
            if (!validated) {
                return validated.error();
            }
            const auto counter = delta->record.vv.get(writer);
            if (counter != commitment.counter + 1) {
                return Error{ErrorCode::InvalidData, "history extension is not contiguous"};
            }
            commitment = WriterHistoryCommitment{
                .counter = counter,
                .digest =
                    advanceHistory(writer, commitment, validated.value().recordHash, counter)};
        }
        if (commitment != expectedFrontier) {
            return Error{ErrorCode::HashMismatch,
                         "history extension does not match advertised frontier"};
        }
        return {};
    }

    [[nodiscard]] bool writerQuarantined(std::string_view writerId) const noexcept {
        return durableQuarantinedWriters_.contains(std::string(writerId));
    }
    [[nodiscard]] std::uint64_t durableQuarantineGeneration() const noexcept {
        return durableQuarantineGeneration_;
    }

    /// Durably invalidate one writer before removing any visible winners. The checkpoint is
    /// persisted first so a crash cannot make invalidated history visible again after restart.
    Result<bool> quarantineWriter(std::string_view writerId, std::string_view sourceNodeId) {
        if (writerId.empty() || sourceNodeId.empty()) {
            return Error{ErrorCode::InvalidArgument, "writer quarantine identity is empty"};
        }
        if (auto loaded = ensureDurableQuarantineLoaded(); !loaded) {
            return loaded.error();
        }
        if (writerQuarantined(writerId)) {
            return false;
        }
        if (durableQuarantinedWriters_.size() >= limits_.maxTrackedIdentities) {
            return Error{ErrorCode::ResourceExhausted, "writer quarantine limit reached"};
        }
        auto next = durableQuarantinedWriters_;
        next.emplace(std::string(writerId), std::string(sourceNodeId));
        auto invalidations = quarantineInvalidations_;
        auto invalidationSources = quarantineInvalidationSources_;
        for (const auto& [key, record] : merged_) {
            if (record.origin != writerId) {
                continue;
            }
            auto invalidation = makeQuarantineInvalidation(key, record);
            if (invalidation) {
                invalidations[key] = std::move(invalidation.value());
                invalidationSources[key] = record;
            }
        }
        if (auto persisted = persistReplicationCheckpoint(historyCommitments_, next); !persisted) {
            return persisted.error();
        }
        durableQuarantinedWriters_ = std::move(next);
        quarantineInvalidations_ = std::move(invalidations);
        quarantineInvalidationSources_ = std::move(invalidationSources);
        ++durableQuarantineGeneration_;
        removeVisibleWinnersFrom(writerId);
        return true;
    }

    /// Collect tombstone history only when the complete configured replica set has
    /// acknowledged the exact delete operation and the retention horizon has elapsed.
    /// Empty replica sets disable collection. A history larger than one bounded page
    /// is retained rather than partially collected.
    Result<std::size_t> collectTombstoneGarbage(std::uint64_t now = nowMs()) {
        if (tombstoneGc_.requiredPeers.empty()) {
            return std::size_t{0};
        }
        std::size_t collected = 0;
        for (auto tombstone = merged_.begin(); tombstone != merged_.end();) {
            const auto& record = tombstone->second;
            const auto retentionMs = static_cast<std::uint64_t>(
                std::max<std::chrono::milliseconds::rep>(0, tombstoneGc_.minRetention.count()));
            if (!record.isTombstone() || now < record.ts.physicalMs ||
                now - record.ts.physicalMs < retentionMs) {
                ++tombstone;
                continue;
            }
            bool fullyAcknowledged = true;
            for (const auto& peer : tombstoneGc_.requiredPeers) {
                if (auto ready = beforeRemoteWork(); !ready) {
                    return ready.error();
                }
                const auto exists = backend_->exists(acknowledgementKey(record.operationId, peer));
                if (!exists) {
                    return exists.error();
                }
                if (!exists.value()) {
                    fullyAcknowledged = false;
                    break;
                }
            }
            if (!fullyAcknowledged) {
                ++tombstone;
                continue;
            }
            const std::string prefix = "index/" + tombstone->first + "/";
            auto page = backend_->listPage(prefix, std::nullopt, limits_.maxIndexObjectsPerSync);
            if (!page) {
                return page.error();
            }
            if (page.value().nextCursor) {
                ++tombstone;
                continue;
            }
            const auto tombstoneKey = indexKey(tombstone->first, hashContent(serialize(record)));
            for (const auto& key : page.value().keys) {
                if (key != tombstoneKey) {
                    if (auto removed = backend_->remove(key); !removed) {
                        return removed.error();
                    }
                }
            }
            if (auto removed = backend_->remove(tombstoneKey); !removed) {
                return removed.error();
            }
            for (const auto& peer : tombstoneGc_.requiredPeers) {
                if (auto removed = backend_->remove(acknowledgementKey(record.operationId, peer));
                    !removed) {
                    return removed.error();
                }
            }
            tombstone = merged_.erase(tombstone);
            ++collected;
        }
        return collected;
    }

    /// Return the number of records observed by the most recent reconciliation.
    /// This is intentionally side-effect free so status callers do not force sync.
    [[nodiscard]] std::size_t mergedRecordCount() const noexcept { return merged_.size(); }
    [[nodiscard]] std::size_t quarantinedRecordCount() const noexcept {
        return quarantined_.size();
    }
    [[nodiscard]] std::size_t authFailureCount() const noexcept { return authFailures_; }

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
    [[nodiscard]] std::map<std::string, MemoryIndexRecord> adapterState() const {
        auto state = merged_;
        for (const auto& [key, tombstone] : quarantineInvalidations_) {
            if (!state.contains(key)) {
                state.emplace(key, tombstone);
            }
        }
        return state;
    }

    [[nodiscard]] CommittedState committedState() const {
        return CommittedState{.merged = merged_,
                              .cachedBlobs = cachedBlobs_,
                              .quarantined = quarantined_,
                              .authFailures = authFailures_,
                              .replication = replicationState()};
    }

    /// Bounded snapshot of current quarantine reasons (cleared each scan page).
    [[nodiscard]] std::map<std::string, std::string> quarantinedReasons() const {
        return quarantined_;
    }

    /// Read only the last periodically reconciled winner. Daemon IPC uses this
    /// path so polling a peer cannot manufacture convergence by forcing sync.
    [[nodiscard]] Result<std::vector<std::byte>> readCached(std::string_view logicalKey) const {
        if (auto valid = validateLogicalKey(logicalKey); !valid) {
            return valid.error();
        }
        const auto it = merged_.find(std::string(logicalKey));
        if (it == merged_.end()) {
            return Error{ErrorCode::NotFound, "no memory record for key"};
        }
        if (it->second.isTombstone()) {
            return Error{ErrorCode::NotFound, "memory record was deleted"};
        }
        const auto cached = cachedBlobs_.find(it->second.entryHash);
        if (cached == cachedBlobs_.end()) {
            return Error{ErrorCode::NotFound, "memory sync blob was not hydrated during sync"};
        }
        return cached->second;
    }

    /// Read the winning content for `logicalKey`. Re-syncs to stay current.
#ifdef YAMS_TESTING
    [[nodiscard]] bool hasMergedRecord(std::string_view logicalKey) const {
        return merged_.contains(std::string(logicalKey));
    }
    [[nodiscard]] std::size_t testingCachedBlobCount() const noexcept {
        return cachedBlobs_.size();
    }
#endif

    Result<std::vector<std::byte>> read(std::string_view logicalKey) {
        if (auto valid = validateLogicalKey(logicalKey); !valid) {
            return valid.error();
        }
        auto merged = sync();
        if (!merged) {
            return merged.error();
        }
        return readCached(logicalKey);
    }

private:
    Result<std::optional<MemoryDelta>> loadLocalDelta(const std::string& key,
                                                      const VersionVector& peerVersion) {
        const std::string logicalKey = logicalKeyFromIndexKey(key);
        const std::string expectedRecordHash = recordHashFromIndexKey(key);
        if (logicalKey.empty() || !isSha256Digest(expectedRecordHash)) {
            return Error{ErrorCode::InvalidData, "invalid local direct-delta index key"};
        }
        auto fetched = backend_->retrieve(key);
        if (!fetched) {
            return fetched.error();
        }
        if (fetched.value().size() > limits_.maxEnvelopeBytes ||
            hashContent(fetched.value()) != expectedRecordHash) {
            return Error{ErrorCode::InvalidData,
                         "local direct-delta envelope failed integrity validation"};
        }
        auto record = deserialize(fetched.value());
        if (!record) {
            return record.error();
        }
        if (record.value().origin != nodeId_ ||
            record.value().vv.get(nodeId_) <= peerVersion.get(nodeId_)) {
            return std::optional<MemoryDelta>{};
        }
        if (!record.value().hasValidIdentity(corpusId_, corpusEpoch_, logicalKey)) {
            return Error{ErrorCode::InvalidData,
                         "local direct-delta envelope has invalid causal identity"};
        }
        const bool authenticated =
            record.value().schemaVersion == kAuthenticatedMemoryIndexSchemaVersion;
        if ((authenticated || (writerAuth_ && writerAuth_->required())) &&
            (!writerAuth_ || !writerAuth_->verify(record.value()))) {
            return Error{ErrorCode::Unauthorized,
                         "local direct-delta envelope failed writer authentication"};
        }
        std::vector<std::byte> payload;
        if (!record.value().isTombstone()) {
            auto blob = backend_->retrieve(blobKey(record.value().entryHash));
            if (!blob) {
                return blob.error();
            }
            if (blob.value().size() > limits_.maxValueBytes ||
                hashContent(blob.value()) != record.value().entryHash) {
                return Error{ErrorCode::HashMismatch,
                             "local direct-delta payload failed integrity validation"};
            }
            payload = std::move(blob.value());
        }
        return std::optional<MemoryDelta>{MemoryDelta{.logicalKey = logicalKey,
                                                      .record = std::move(record.value()),
                                                      .payload = std::move(payload)}};
    }

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

    Result<ValidatedDelta> validateDirectDelta(const MemoryDelta& delta) {
        if (auto valid = validateLogicalKey(delta.logicalKey); !valid) {
            return valid.error();
        }
        const auto& record = delta.record;
        if (!record.hasValidIdentity(corpusId_, corpusEpoch_, delta.logicalKey)) {
            return Error{ErrorCode::InvalidData, "direct delta has invalid causal identity"};
        }
        if (record.schemaVersion < kMemoryIndexSchemaVersion) {
            return Error{ErrorCode::NotSupported, "direct delta schema is unsupported"};
        }
        const bool authenticated = record.schemaVersion == kAuthenticatedMemoryIndexSchemaVersion;
        if ((authenticated || (writerAuth_ && writerAuth_->required())) &&
            (!writerAuth_ || !writerAuth_->verify(record))) {
            recordAuthFailure();
            return Error{ErrorCode::Unauthorized, "direct delta writer authentication failed"};
        }
        const bool invalidPayload = record.isTombstone()
                                        ? !delta.payload.empty()
                                        : (delta.payload.size() > limits_.maxValueBytes ||
                                           hashContent(delta.payload) != record.entryHash);
        if (invalidPayload) {
            return Error{ErrorCode::HashMismatch,
                         "direct delta payload failed integrity validation"};
        }
        auto recordBytes = serialize(record);
        if (recordBytes.size() > limits_.maxEnvelopeBytes) {
            return Error{ErrorCode::InvalidData,
                         "direct delta envelope exceeds configured size limit"};
        }
        const std::string recordHash = hashContent(recordBytes);
        return ValidatedDelta{.recordBytes = std::move(recordBytes),
                              .recordHash = recordHash,
                              .fingerprint = {recordHash, delta.logicalKey}};
    }

    DirectDeltaDecision classifyDirectDelta(const MemoryDelta& delta,
                                            const ValidatedDelta& validated) const {
        const auto& record = delta.record;
        const auto tracked = deltaOperations_.find(record.operationId);
        if (tracked != deltaOperations_.end() && tracked->second != validated.fingerprint) {
            return {DirectDeltaAction::Reject, "duplicate writer operation fork"};
        }
        const auto writerCounter = record.vv.get(record.origin);
        const auto currentWriterCounter = version_.get(record.origin);
        if (writerCounter <= currentWriterCounter) {
            return {DirectDeltaAction::Replay, {}};
        }
        if (writerCounter != currentWriterCounter + 1) {
            return {DirectDeltaAction::Reject, "direct delta has a causal writer gap"};
        }
        const bool missingDependency =
            std::ranges::any_of(record.vv.counters(), [&](const auto& counter) {
                return counter.first != record.origin &&
                       counter.second > version_.get(counter.first);
            });
        return missingDependency
                   ? DirectDeltaDecision{DirectDeltaAction::Reject,
                                         "direct delta has an unresolved causal dependency"}
                   : DirectDeltaDecision{DirectDeltaAction::Apply, {}};
    }

    Result<DirectWinnerPlan> planDirectWinner(const MemoryDelta& delta) const {
        const auto winner = merged_.find(delta.logicalKey);
        DirectWinnerPlan plan;
        plan.becomesWinner = winner == merged_.end() ||
                             resolveLww(delta.record, winner->second) == LwwDecision::First;
        std::size_t projectedCacheBytes = cachedBytes_;
        if (plan.becomesWinner && winner != merged_.end() && !winner->second.isTombstone() &&
            winner->second.entryHash != delta.record.entryHash) {
            const auto cached = cachedBlobs_.find(winner->second.entryHash);
            const bool shared = std::ranges::any_of(merged_, [&](const auto& entry) {
                return entry.first != delta.logicalKey &&
                       entry.second.entryHash == winner->second.entryHash;
            });
            if (cached != cachedBlobs_.end() && !shared) {
                projectedCacheBytes -= cached->second.size();
                plan.oldCachedHash = winner->second.entryHash;
            }
        }
        if (plan.becomesWinner && !delta.record.isTombstone() &&
            !cachedBlobs_.contains(delta.record.entryHash)) {
            projectedCacheBytes += delta.payload.size();
        }
        if (projectedCacheBytes > limits_.maxCacheBytes) {
            return Error{ErrorCode::ResourceExhausted,
                         "direct delta exceeds configured cache limit"};
        }
        return plan;
    }

    Result<void> persistDirectDelta(const MemoryDelta& delta, const ValidatedDelta& validated) {
        if (!delta.record.isTombstone()) {
            auto exists = backend_->exists(blobKey(delta.record.entryHash));
            if (!exists) {
                return exists.error();
            }
            if (!exists.value()) {
                if (auto stored = backend_->store(blobKey(delta.record.entryHash), delta.payload);
                    !stored) {
                    return stored.error();
                }
            }
        }
        return backend_->store(indexKey(delta.logicalKey, validated.recordHash),
                               validated.recordBytes);
    }

    void rememberDirectOperation(const MemoryDelta& delta, const ValidatedDelta& validated) {
        if (deltaOperations_.contains(delta.record.operationId)) {
            return;
        }
        if (deltaOperationOrder_.size() >= limits_.maxTrackedIdentities &&
            !deltaOperationOrder_.empty()) {
            deltaOperations_.erase(deltaOperationOrder_.front());
            deltaOperationOrder_.pop_front();
        }
        deltaOperations_[delta.record.operationId] = validated.fingerprint;
        deltaOperationOrder_.push_back(delta.record.operationId);
    }

    bool commitDirectDelta(const MemoryDelta& delta, const DirectWinnerPlan& plan) {
        version_.merge(delta.record.vv);
        if (!plan.becomesWinner) {
            return false;
        }
        if (plan.oldCachedHash) {
            const auto cached = cachedBlobs_.find(*plan.oldCachedHash);
            if (cached != cachedBlobs_.end()) {
                cachedBytes_ -= cached->second.size();
                cachedBlobs_.erase(cached);
            }
        }
        if (!delta.record.isTombstone() && !cachedBlobs_.contains(delta.record.entryHash)) {
            cachedBytes_ += delta.payload.size();
            cachedBlobs_[delta.record.entryHash] = delta.payload;
        }
        merged_[delta.logicalKey] = delta.record;
        return true;
    }

    void quarantineDirectDelta(const MemoryDelta& delta, std::string reason,
                               DeltaApplyResult& result) {
        const std::string identity = delta.logicalKey + "#" + delta.record.operationId;
        result.quarantined[identity] = reason;
        quarantined_[identity] = std::move(reason);
    }

    struct ScanCandidate {
        std::string indexKey;
        std::string logicalKey;
        std::string recordHash;
        MemoryIndexRecord record;
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

    static std::string_view readinessProbeName(EraseReadinessProbe probe) {
        switch (probe) {
            case EraseReadinessProbe::Explicit:
                return "explicit";
            case EraseReadinessProbe::MetadataAbsent:
                return "metadata_absent";
            case EraseReadinessProbe::ContentAbsent:
                return "content_absent";
        }
        return "invalid";
    }

    static std::optional<EraseReadinessProbe> parseReadinessProbe(std::string_view name) {
        if (name == "explicit") {
            return EraseReadinessProbe::Explicit;
        }
        if (name == "metadata_absent") {
            return EraseReadinessProbe::MetadataAbsent;
        }
        if (name == "content_absent") {
            return EraseReadinessProbe::ContentAbsent;
        }
        return std::nullopt;
    }

    std::string eraseAuthorizationPayload(std::string_view tombstonePayload,
                                          EraseReadinessProbe probe) const {
        return nlohmann::json::array({"yams-erase-authorization-v1", std::string(tombstonePayload),
                                      readinessProbeName(probe)})
            .dump();
    }

    std::size_t maxEraseOutboxBytes() const {
        return limits_.maxEnvelopeBytes > std::numeric_limits<std::size_t>::max() / 3U
                   ? std::numeric_limits<std::size_t>::max()
                   : limits_.maxEnvelopeBytes * 3U;
    }

    std::string eraseOutboxPrefix() const {
        const std::string identity =
            nlohmann::json::array({"yams-erase-outbox-v1", corpusId_, corpusEpoch_, nodeId_})
                .dump();
        return "outbox/erase-v1/" +
               hashContent(std::span<const std::byte>{
                   reinterpret_cast<const std::byte*>(identity.data()), identity.size()}) +
               "/";
    }

    std::string eraseOutboxKey(std::string_view logicalKey) const {
        const std::string key(logicalKey);
        return eraseOutboxPrefix() +
               hashContent(std::span<const std::byte>{
                   reinterpret_cast<const std::byte*>(key.data()), key.size()});
    }

    Result<EraseIntentState> decodeEraseIntent(std::string_view key,
                                               std::span<const std::byte> encoded) {
        if (encoded.size() > maxEraseOutboxBytes()) {
            return Error{ErrorCode::InvalidData, "erase outbox entry exceeds configured limit"};
        }
        try {
            const std::string_view text(reinterpret_cast<const char*>(encoded.data()),
                                        encoded.size());
            const auto json = nlohmann::json::parse(text);
            if (json.at("schema_version").get<std::uint32_t>() != 1 ||
                json.at("corpus_id").get<std::string>() != corpusId_ ||
                json.at("corpus_epoch").get<std::uint64_t>() != corpusEpoch_ ||
                json.at("node_id").get<std::string>() != nodeId_) {
                return Error{ErrorCode::InvalidData, "erase outbox identity is invalid"};
            }
            EraseIntentState intent;
            intent.logicalKey = json.at("logical_key").get<std::string>();
            intent.tombstonePayload = json.at("tombstone_payload").get<std::string>();
            const auto readiness =
                parseReadinessProbe(json.at("readiness_probe").get<std::string>());
            if (!readiness) {
                return Error{ErrorCode::InvalidData, "erase outbox readiness probe is invalid"};
            }
            intent.readinessProbe = *readiness;
            intent.ready = json.at("ready").get<bool>();
            if (auto valid = validateLogicalKey(intent.logicalKey);
                !valid || intent.tombstonePayload.size() > limits_.maxEnvelopeBytes ||
                eraseOutboxKey(intent.logicalKey) != key) {
                return Error{ErrorCode::InvalidData, "erase outbox key or payload is invalid"};
            }
            if (json.contains("authorization")) {
                intent.authorization = json.at("authorization").get<MemoryIndexRecord>();
                if (!intent.authorization->isTombstone() ||
                    !intent.authorization->hasValidIdentity(corpusId_, corpusEpoch_,
                                                            intent.logicalKey) ||
                    intent.authorization->origin != nodeId_ ||
                    intent.authorization->tombstonePayload !=
                        eraseAuthorizationPayload(intent.tombstonePayload, intent.readinessProbe) ||
                    !writerAuth_ || !writerAuth_->verify(*intent.authorization)) {
                    return Error{ErrorCode::Unauthorized, "erase outbox authorization is invalid"};
                }
            }
            if (json.contains("record")) {
                intent.record = json.at("record").get<MemoryIndexRecord>();
                intent.recordHash = json.at("record_hash").get<std::string>();
                intent.preparedCommitment = WriterHistoryCommitment{
                    .counter = json.at("prepared_commitment").at("counter").get<std::uint64_t>(),
                    .digest = json.at("prepared_commitment").at("digest").get<std::string>()};
                const auto recordBytes = serialize(*intent.record);
                if (!intent.ready || !intent.record->isTombstone() ||
                    intent.record->tombstonePayload != intent.tombstonePayload ||
                    !intent.record->hasValidIdentity(corpusId_, corpusEpoch_, intent.logicalKey) ||
                    intent.record->origin != nodeId_ ||
                    intent.record->operationId !=
                        makeOperationId(nodeId_, intent.record->vv.get(nodeId_)) ||
                    !isSha256Digest(intent.recordHash) ||
                    intent.preparedCommitment.counter != intent.record->vv.get(nodeId_) ||
                    !isSha256Digest(intent.preparedCommitment.digest) ||
                    hashContent(recordBytes) != intent.recordHash ||
                    recordBytes.size() > limits_.maxEnvelopeBytes) {
                    return Error{ErrorCode::InvalidData,
                                 "prepared erase outbox envelope is invalid"};
                }
                const bool authenticated =
                    intent.record->schemaVersion == kAuthenticatedMemoryIndexSchemaVersion;
                if ((authenticated || (writerAuth_ && writerAuth_->required())) &&
                    (!writerAuth_ || !writerAuth_->verify(*intent.record))) {
                    return Error{ErrorCode::Unauthorized,
                                 "prepared erase outbox authentication failed"};
                }
            } else if (json.contains("record_hash") || json.contains("prepared_commitment") ||
                       intent.ready) {
                return Error{ErrorCode::InvalidData, "erase outbox preparation is incomplete"};
            }
            if (writerAuth_ && writerAuth_->required() && !intent.authorization) {
                return Error{ErrorCode::Unauthorized,
                             "erase outbox pending intent lacks writer authorization"};
            }
            return intent;
        } catch (const std::exception& error) {
            return Error{ErrorCode::InvalidData,
                         std::string("invalid erase outbox entry: ") + error.what()};
        }
    }

    Result<void> storeEraseIntent(const EraseIntentState& intent) {
        nlohmann::json encoded{{"schema_version", 1},
                               {"corpus_id", corpusId_},
                               {"corpus_epoch", corpusEpoch_},
                               {"node_id", nodeId_},
                               {"logical_key", intent.logicalKey},
                               {"tombstone_payload", intent.tombstonePayload},
                               {"readiness_probe", readinessProbeName(intent.readinessProbe)},
                               {"ready", intent.ready}};
        if (intent.authorization) {
            encoded["authorization"] = *intent.authorization;
        }
        if (intent.record) {
            encoded["record"] = *intent.record;
            encoded["record_hash"] = intent.recordHash;
            encoded["prepared_commitment"] = {{"counter", intent.preparedCommitment.counter},
                                              {"digest", intent.preparedCommitment.digest}};
        }
        const std::string bytes = encoded.dump();
        if (bytes.size() > maxEraseOutboxBytes()) {
            return Error{ErrorCode::ResourceExhausted,
                         "erase outbox entry exceeds configured limit"};
        }
        if (auto ready = beforeRemoteWork(); !ready) {
            return ready.error();
        }
        return backend_->store(eraseOutboxKey(intent.logicalKey),
                               std::span<const std::byte>{
                                   reinterpret_cast<const std::byte*>(bytes.data()), bytes.size()});
    }

    Result<std::optional<EraseIntentState>> loadEraseIntent(std::string_view logicalKey) {
        const auto key = eraseOutboxKey(logicalKey);
        auto exists = backend_->exists(key);
        if (!exists) {
            return exists.error();
        }
        if (!exists.value()) {
            return std::optional<EraseIntentState>{};
        }
        auto encoded = backend_->retrieve(key);
        if (!encoded) {
            return encoded.error();
        }
        auto decoded = decodeEraseIntent(key, encoded.value());
        if (!decoded) {
            return decoded.error();
        }
        return std::optional<EraseIntentState>{std::move(decoded.value())};
    }

    Result<std::vector<EraseIntentState>> loadEraseIntents() {
        auto listed =
            backend_->listPage(eraseOutboxPrefix(), std::nullopt, limits_.maxMergedKeys + 1U);
        if (!listed) {
            return listed.error();
        }
        if (listed.value().nextCursor || listed.value().keys.size() > limits_.maxMergedKeys) {
            return Error{ErrorCode::ResourceExhausted,
                         "erase outbox exceeds configured retained-intent bound"};
        }
        std::vector<EraseIntentState> intents;
        intents.reserve(listed.value().keys.size());
        for (const auto& key : listed.value().keys) {
            auto encoded = backend_->retrieve(key);
            if (!encoded) {
                return encoded.error();
            }
            auto decoded = decodeEraseIntent(key, encoded.value());
            if (!decoded) {
                return decoded.error();
            }
            intents.push_back(std::move(decoded.value()));
        }
        return intents;
    }

    Result<MemoryIndexRecord> prepareEraseRecord(std::string_view logicalKey,
                                                 std::string_view tombstonePayload) {
        if (writerQuarantined(nodeId_)) {
            return Error{ErrorCode::InvalidState, "quarantined writer cannot erase"};
        }
        if (auto committed = requireLocalHistoryCommitment(); !committed) {
            return committed.error();
        }
        auto nextVersion = version_;
        if (!nextVersion.increment(nodeId_)) {
            return Error{ErrorCode::InvalidState, "memory sync writer counter exhausted"};
        }
        MemoryIndexRecord record;
        record.ts.physicalMs = nowMs();
        record.ts.logical = logicalClock_ + 1;
        record.origin = nodeId_;
        record.vv = std::move(nextVersion);
        record.corpusId = corpusId_;
        record.corpusEpoch = corpusEpoch_;
        record.operationId = makeOperationId(nodeId_, record.vv.get(nodeId_));
        record.logicalKey = logicalKey;
        record.recordKind = std::string(kMemoryTombstoneRecordKind);
        record.tombstonePayload = tombstonePayload;
        if (writerAuth_) {
            if (auto signedRecord = writerAuth_->sign(record); !signedRecord) {
                return signedRecord.error();
            }
        }
        const auto bytes = serialize(record);
        if (bytes.size() > limits_.maxEnvelopeBytes) {
            return Error{ErrorCode::InvalidArgument,
                         "memory sync tombstone envelope exceeds configured limit"};
        }
        return record;
    }

    Result<WriterHistoryCommitment> preparedEraseCommitment(const MemoryIndexRecord& record,
                                                            std::string_view recordHash) const {
        const auto prior = historyCommitments_.find(nodeId_);
        const auto previous =
            prior == historyCommitments_.end() ? WriterHistoryCommitment{} : prior->second;
        const auto counter = record.vv.get(nodeId_);
        if (counter != previous.counter + 1U) {
            return Error{ErrorCode::InvalidState,
                         "prepared erase does not extend local writer history"};
        }
        return WriterHistoryCommitment{
            .counter = counter, .digest = advanceHistory(nodeId_, previous, recordHash, counter)};
    }

    Result<void> commitPreparedErase(const EraseIntentState& intent) {
        if (!intent.ready || !intent.record || !isSha256Digest(intent.recordHash)) {
            return Error{ErrorCode::InvalidState, "erase intent is not prepared"};
        }
        const auto operation = operations_.find(intent.record->operationId);
        if (operation != operations_.end()) {
            if (operation->second != std::pair{intent.recordHash, intent.logicalKey}) {
                return Error{ErrorCode::InvalidState,
                             "prepared erase operation conflicts with committed history"};
            }
            const auto commitment = historyCommitments_.find(nodeId_);
            if (commitment == historyCommitments_.end() ||
                commitment->second.counter < intent.record->vv.get(nodeId_)) {
                return Error{ErrorCode::InvalidState,
                             "prepared erase index lacks a durable history commitment"};
            }
            return backend_->remove(eraseOutboxKey(intent.logicalKey));
        }
        const auto prior = historyCommitments_.find(nodeId_);
        const auto current =
            prior == historyCommitments_.end() ? WriterHistoryCommitment{} : prior->second;
        if (current.counter == intent.preparedCommitment.counter) {
            if (current != intent.preparedCommitment) {
                return Error{ErrorCode::HashMismatch,
                             "prepared erase commitment conflicts with durable history"};
            }
        } else if (current.counter + 1U == intent.preparedCommitment.counter) {
            auto expected = preparedEraseCommitment(*intent.record, intent.recordHash);
            if (!expected || expected.value() != intent.preparedCommitment) {
                return Error{ErrorCode::HashMismatch,
                             "prepared erase commitment does not extend durable history"};
            }
            auto next = historyCommitments_;
            next[nodeId_] = intent.preparedCommitment;
            if (auto persisted = persistReplicationCheckpoint(next, durableQuarantinedWriters_);
                !persisted) {
                return persisted.error();
            }
            historyCommitments_ = std::move(next);
        } else {
            return Error{ErrorCode::InvalidState,
                         "prepared erase no longer extends local writer history"};
        }
        const auto recordBytes = serialize(*intent.record);
        if (auto ready = beforeRemoteWork(); !ready) {
            return ready.error();
        }
        if (auto stored =
                backend_->store(indexKey(intent.logicalKey, intent.recordHash), recordBytes);
            !stored) {
            return stored.error();
        }
        operations_[intent.record->operationId] = {intent.recordHash, intent.logicalKey};
        version_.merge(intent.record->vv);
        logicalClock_ = std::max(logicalClock_, intent.record->ts.logical);
        return backend_->remove(eraseOutboxKey(intent.logicalKey));
    }

    Result<void> replayReadyErasesAfterReconcile() {
        auto intents = loadEraseIntents();
        if (!intents) {
            return intents.error();
        }
        std::ranges::sort(intents.value(), [&](const EraseIntentState& lhs,
                                               const EraseIntentState& rhs) {
            const auto lhsCounter = lhs.record ? lhs.record->vv.get(nodeId_)
                                               : std::numeric_limits<std::uint64_t>::max();
            const auto rhsCounter = rhs.record ? rhs.record->vv.get(nodeId_)
                                               : std::numeric_limits<std::uint64_t>::max();
            return std::tuple{lhsCounter, lhs.logicalKey} < std::tuple{rhsCounter, rhs.logicalKey};
        });
        for (auto& intent : intents.value()) {
            if (!intent.ready || intent.readinessProbe != EraseReadinessProbe::Explicit) {
                continue;
            }
            if (auto committed = commitPreparedErase(intent); !committed) {
                return committed.error();
            }
        }
        return {};
    }

    Result<void> reconcileBeforeWrite(bool allowDeferredPreparedErase = false) {
        Result<std::map<std::string, MemoryIndexRecord>> reconciled =
            backend_->isRemote() ? sync() : syncFully();
        if (!reconciled) {
            return reconciled.error();
        }
        if (auto replayed = replayReadyErasesAfterReconcile(); !replayed) {
            return replayed.error();
        }
        if (allowDeferredPreparedErase) {
            return {};
        }
        auto intents = loadEraseIntents();
        if (!intents) {
            return intents.error();
        }
        const bool deferredPrepared =
            std::ranges::any_of(intents.value(), [](const EraseIntentState& intent) {
                return intent.ready && intent.record &&
                       intent.readinessProbe != EraseReadinessProbe::Explicit;
            });
        return deferredPrepared
                   ? Result<void>{Error{ErrorCode::InvalidState,
                                        "prepared document deletion requires local revalidation"}}
                   : Result<void>{};
    }

    [[nodiscard]] Result<void> beforeRemoteWork() const {
        if (control_.isCancelled && control_.isCancelled()) {
            return Error{ErrorCode::OperationCancelled, "memory sync reconciliation cancelled"};
        }
        if (backend_->isRemote() && control_.canAdmitRemoteWork && !control_.canAdmitRemoteWork()) {
            return Error{ErrorCode::ResourceExhausted,
                         "memory sync remote work denied by resource governor"};
        }
        return {};
    }

    static std::uint64_t nowMs() {
        return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                              std::chrono::system_clock::now().time_since_epoch())
                                              .count());
    }

    static std::string acknowledgementKey(std::string_view operationId, std::string_view peerId) {
        return "ack/" + escapeKeySegment(operationId) + "/" + escapeKeySegment(peerId);
    }

    static std::string blobKey(std::string_view hash) {
        return std::string("blob/") + std::string(hash);
    }

    static std::string indexKey(std::string_view logicalKey, std::string_view hash) {
        return std::string("index/") + std::string(logicalKey) + "/" + std::string(hash);
    }

    static std::string logicalKeyFromIndexKey(std::string_view key) {
        constexpr std::string_view prefix = "index/";
        if (!key.starts_with(prefix)) {
            return {};
        }
        const auto rest = key.substr(prefix.size());
        const auto slash = rest.rfind('/');
        return slash == std::string_view::npos ? std::string(rest)
                                               : std::string(rest.substr(0, slash));
    }

    static std::string recordHashFromIndexKey(std::string_view key) {
        const auto slash = key.rfind('/');
        if (slash == std::string_view::npos || slash + 1 >= key.size()) {
            return {};
        }
        return std::string(key.substr(slash + 1));
    }

    std::string replicationCheckpointKey() const {
        const std::string identity =
            nlohmann::json::array({"yams-replication-state-v1", corpusId_, corpusEpoch_, nodeId_})
                .dump();
        const auto digest = hashContent(std::span<const std::byte>{
            reinterpret_cast<const std::byte*>(identity.data()), identity.size()});
        return "checkpoint/replication-state-v1/" + digest;
    }

    Result<void> ensureDurableQuarantineLoaded() {
        if (durableQuarantineLoaded_) {
            return {};
        }
        const auto checkpointKey = replicationCheckpointKey();
        auto exists = backend_->exists(checkpointKey);
        if (!exists) {
            return exists.error();
        }
        if (!exists.value()) {
            durableQuarantineLoaded_ = true;
            return {};
        }
        auto encoded = backend_->retrieve(checkpointKey);
        if (!encoded) {
            return encoded.error();
        }
        if (encoded.value().size() > limits_.maxEnvelopeBytes) {
            return Error{ErrorCode::InvalidData, "replication checkpoint exceeds limit"};
        }
        try {
            const std::string_view text(reinterpret_cast<const char*>(encoded.value().data()),
                                        encoded.value().size());
            const auto json = nlohmann::json::parse(text);
            if (json.at("schema_version").get<std::uint32_t>() != 1 ||
                json.at("corpus_id").get<std::string>() != corpusId_ ||
                json.at("corpus_epoch").get<std::uint64_t>() != corpusEpoch_ ||
                !json.at("quarantined_writers").is_array() || !json.at("commitments").is_array()) {
                return Error{ErrorCode::InvalidData, "replication checkpoint is invalid"};
            }
            std::map<std::string, std::string> quarantined;
            for (const auto& entry : json.at("quarantined_writers")) {
                const auto writer = entry.at("writer_id").get<std::string>();
                const auto source = entry.at("source_node_id").get<std::string>();
                if (writer.empty() || source.empty() || quarantined.contains(writer) ||
                    quarantined.size() >= limits_.maxTrackedIdentities) {
                    return Error{ErrorCode::InvalidData,
                                 "replication checkpoint violates quarantine bounds"};
                }
                quarantined.emplace(writer, source);
            }
            std::map<NodeId, WriterHistoryCommitment> commitments;
            for (const auto& entry : json.at("commitments")) {
                const auto writer = entry.at("writer_id").get<std::string>();
                WriterHistoryCommitment commitment{.counter =
                                                       entry.at("counter").get<std::uint64_t>(),
                                                   .digest = entry.at("digest").get<std::string>()};
                if (writer.empty() || commitment.counter == 0 ||
                    !isSha256Digest(commitment.digest) || commitments.contains(writer) ||
                    commitments.size() >= limits_.maxTrackedIdentities) {
                    return Error{ErrorCode::InvalidData,
                                 "replication checkpoint violates commitment bounds"};
                }
                commitments.emplace(writer, std::move(commitment));
            }
            durableQuarantinedWriters_ = std::move(quarantined);
            historyCommitments_ = std::move(commitments);
            durableQuarantineLoaded_ = true;
            removeQuarantinedWinners();
            return {};
        } catch (const std::exception& error) {
            return Error{ErrorCode::InvalidData,
                         std::string("invalid replication checkpoint: ") + error.what()};
        }
    }

    Result<void>
    persistReplicationCheckpoint(const std::map<NodeId, WriterHistoryCommitment>& commitments,
                                 const std::map<std::string, std::string>& quarantinedWriters) {
        nlohmann::json quarantineJson = nlohmann::json::array();
        for (const auto& [writer, source] : quarantinedWriters) {
            quarantineJson.push_back({{"writer_id", writer}, {"source_node_id", source}});
        }
        nlohmann::json commitmentJson = nlohmann::json::array();
        for (const auto& [writer, commitment] : commitments) {
            commitmentJson.push_back({{"writer_id", writer},
                                      {"counter", commitment.counter},
                                      {"digest", commitment.digest}});
        }
        const std::string encoded = nlohmann::json{
            {"schema_version", 1},
            {"corpus_id", corpusId_},
            {"corpus_epoch", corpusEpoch_},
            {"quarantined_writers", std::move(quarantineJson)},
            {"commitments",
             std::move(commitmentJson)}}.dump();
        if (encoded.size() > limits_.maxEnvelopeBytes) {
            return Error{ErrorCode::ResourceExhausted,
                         "replication checkpoint exceeds envelope limit"};
        }
        return backend_->store(
            replicationCheckpointKey(),
            std::span<const std::byte>{reinterpret_cast<const std::byte*>(encoded.data()),
                                       encoded.size()});
    }

    std::string historySeed(std::string_view writerId) const {
        const std::string material = nlohmann::json::array({"yams-writer-history-v1", corpusId_,
                                                            corpusEpoch_, std::string(writerId)})
                                         .dump();
        return hashContent(std::span<const std::byte>{
            reinterpret_cast<const std::byte*>(material.data()), material.size()});
    }

    std::string advanceHistory(std::string_view writerId, const WriterHistoryCommitment& previous,
                               std::string_view recordHash, std::uint64_t counter) const {
        const std::string prior = previous.counter == 0 ? historySeed(writerId) : previous.digest;
        const std::string material =
            nlohmann::json::array({"yams-writer-history-link-v1", std::string(writerId), counter,
                                   prior, std::string(recordHash)})
                .dump();
        return hashContent(std::span<const std::byte>{
            reinterpret_cast<const std::byte*>(material.data()), material.size()});
    }

    Result<void> commitScannedHistory(const std::vector<ScanCandidate>& candidates) {
        auto next = historyCommitments_;
        std::vector<const ScanCandidate*> ordered;
        ordered.reserve(candidates.size());
        for (const auto& candidate : candidates) {
            ordered.push_back(&candidate);
        }
        std::ranges::sort(ordered, [](const ScanCandidate* lhs, const ScanCandidate* rhs) {
            return std::tuple{lhs->record.origin, lhs->record.vv.get(lhs->record.origin),
                              lhs->recordHash} < std::tuple{rhs->record.origin,
                                                            rhs->record.vv.get(rhs->record.origin),
                                                            rhs->recordHash};
        });
        for (const auto* candidate : ordered) {
            const auto& writer = candidate->record.origin;
            if (writerQuarantined(writer)) {
                continue;
            }
            if (forkedOperations_.contains(candidate->record.operationId)) {
                auto quarantined = quarantineWriter(writer, nodeId_);
                if (!quarantined) {
                    return quarantined.error();
                }
                continue;
            }
            const auto counter = candidate->record.vv.get(writer);
            auto previous = next.contains(writer) ? next.at(writer) : WriterHistoryCommitment{};
            if (counter <= previous.counter) {
                continue;
            }
            if (counter != previous.counter + 1) {
                // Shared/legacy stores may retain only a later winner. Never manufacture a
                // full-history commitment from that frontier; leave it uncommitted so direct
                // equal-counter handshakes fail closed until bounded history/bootstrap repairs it.
                continue;
            }
            next[writer] = WriterHistoryCommitment{
                .counter = counter,
                .digest = advanceHistory(writer, previous, candidate->recordHash, counter)};
        }
        for (const auto& [writer, _] : durableQuarantinedWriters_) {
            next.erase(writer);
        }
        if (next == historyCommitments_) {
            return {};
        }
        if (auto persisted = persistReplicationCheckpoint(next, durableQuarantinedWriters_);
            !persisted) {
            return persisted.error();
        }
        historyCommitments_ = std::move(next);
        return {};
    }

    Result<WriterHistoryCommitment> computeHistoryCommitmentAt(std::string_view writerId,
                                                               std::uint64_t targetCounter) {
        if (targetCounter > limits_.maxTrackedIdentities) {
            return Error{ErrorCode::ResourceExhausted,
                         "requested history prefix exceeds configured proof bound"};
        }
        const auto current = historyCommitments_.find(std::string(writerId));
        if (current == historyCommitments_.end() || targetCounter > current->second.counter) {
            return Error{ErrorCode::InvalidState, "requested writer history prefix is unavailable"};
        }
        if (targetCounter == current->second.counter) {
            return current->second;
        }

        std::map<std::uint64_t, std::string> recordHashes;
        std::optional<std::string> cursor;
        std::size_t observed = 0;
        for (;;) {
            auto listed = backend_->listPage(
                "index/", cursor ? std::optional<std::string_view>{*cursor} : std::nullopt,
                limits_.maxIndexObjectsPerSync);
            if (!listed) {
                return listed.error();
            }
            auto page = std::move(listed.value());
            for (const auto& key : page.keys) {
                if (++observed > limits_.maxTrackedIdentities) {
                    return Error{ErrorCode::ResourceExhausted,
                                 "history prefix proof scan exceeds configured bound"};
                }
                const auto logicalKey = logicalKeyFromIndexKey(key);
                const auto expectedHash = recordHashFromIndexKey(key);
                if (logicalKey.empty() || !isSha256Digest(expectedHash)) {
                    continue;
                }
                auto encoded = backend_->retrieve(key);
                if (!encoded || encoded.value().size() > limits_.maxEnvelopeBytes ||
                    hashContent(encoded.value()) != expectedHash) {
                    continue;
                }
                auto record = deserialize(encoded.value());
                if (!record || record.value().origin != writerId ||
                    !record.value().hasValidIdentity(corpusId_, corpusEpoch_, logicalKey)) {
                    continue;
                }
                const bool authenticated =
                    record.value().schemaVersion == kAuthenticatedMemoryIndexSchemaVersion;
                if ((authenticated || (writerAuth_ && writerAuth_->required())) &&
                    (!writerAuth_ || !writerAuth_->verify(record.value()))) {
                    continue;
                }
                const auto counter = record.value().vv.get(std::string(writerId));
                if (counter == 0 || counter > targetCounter) {
                    continue;
                }
                const auto [entry, inserted] = recordHashes.emplace(counter, expectedHash);
                if (!inserted && entry->second != expectedHash) {
                    return Error{ErrorCode::InvalidData,
                                 "writer history prefix contains a counter fork"};
                }
            }
            cursor = std::move(page.nextCursor);
            if (!cursor) {
                break;
            }
        }
        WriterHistoryCommitment commitment;
        for (std::uint64_t counter = 1; counter <= targetCounter; ++counter) {
            const auto record = recordHashes.find(counter);
            if (record == recordHashes.end()) {
                return Error{ErrorCode::InvalidState, "writer history prefix is incomplete"};
            }
            commitment = WriterHistoryCommitment{
                .counter = counter,
                .digest = advanceHistory(writerId, commitment, record->second, counter)};
        }
        return commitment;
    }

    Result<void> requireLocalHistoryCommitment() const {
        const auto counter = version_.get(nodeId_);
        const auto commitment = historyCommitments_.find(nodeId_);
        if (counter == 0) {
            return commitment == historyCommitments_.end()
                       ? Result<void>{}
                       : Result<void>{Error{ErrorCode::InvalidState,
                                            "zero-counter writer has a history commitment"}};
        }
        if (commitment == historyCommitments_.end() || commitment->second.counter != counter ||
            !isSha256Digest(commitment->second.digest)) {
            return Error{ErrorCode::InvalidState,
                         "local writer history is incomplete; bootstrap is required"};
        }
        return {};
    }

    Result<void> commitRecordHistory(const MemoryIndexRecord& record, std::string_view recordHash) {
        const auto& writer = record.origin;
        auto next = historyCommitments_;
        const auto previous = next.contains(writer) ? next.at(writer) : WriterHistoryCommitment{};
        const auto counter = record.vv.get(writer);
        if (counter != previous.counter + 1) {
            return Error{ErrorCode::InvalidState,
                         "record commitment does not extend contiguous history"};
        }
        next[writer] = WriterHistoryCommitment{
            .counter = counter, .digest = advanceHistory(writer, previous, recordHash, counter)};
        if (auto persisted = persistReplicationCheckpoint(next, durableQuarantinedWriters_);
            !persisted) {
            return persisted.error();
        }
        historyCommitments_ = std::move(next);
        return {};
    }

    Result<void> commitDirectHistory(const MemoryDelta& delta, const ValidatedDelta& validated) {
        return commitRecordHistory(delta.record, validated.recordHash);
    }

    Result<MemoryIndexRecord> makeQuarantineInvalidation(std::string_view logicalKey,
                                                         const MemoryIndexRecord& record) {
        if (record.isTombstone()) {
            return record;
        }
        std::vector<std::byte> payload;
        const auto cached = cachedBlobs_.find(record.entryHash);
        if (cached != cachedBlobs_.end()) {
            payload = cached->second;
        } else {
            auto loaded = backend_->retrieve(blobKey(record.entryHash));
            if (!loaded) {
                return loaded.error();
            }
            if (loaded.value().size() > limits_.maxValueBytes ||
                hashContent(loaded.value()) != record.entryHash) {
                return Error{ErrorCode::HashMismatch,
                             "quarantined winner payload failed integrity validation"};
            }
            payload = std::move(loaded.value());
        }

        const auto parsePayload = [&]<typename T>() -> Result<T> {
            try {
                const std::string_view text(reinterpret_cast<const char*>(payload.data()),
                                            payload.size());
                return nlohmann::json::parse(text).get<T>();
            } catch (const std::exception& error) {
                return Error{ErrorCode::InvalidData, error.what()};
            }
        };
        std::string tombstonePayload;
        const std::string documentPrefix =
            std::string(memoryStoreName(MemoryStore::Document)) + "/";
        const std::string embeddingPrefix =
            std::string(memoryStoreName(MemoryStore::Embedding)) + "/";
        const std::string nodePrefix =
            std::string(memoryStoreName(MemoryStore::TopologyNode)) + "/";
        const std::string edgePrefix =
            std::string(memoryStoreName(MemoryStore::TopologyEdge)) + "/";
        const std::string blobPrefix = std::string(memoryStoreName(MemoryStore::ContentBlob)) + "/";
        if (logicalKey.starts_with(documentPrefix)) {
            auto decoded = parsePayload.template operator()<MetadataDocumentRecord>();
            if (!decoded) {
                return decoded.error();
            }
            tombstonePayload = decoded.value().contentHash.empty() ? decoded.value().documentId
                                                                   : decoded.value().contentHash;
        } else if (logicalKey.starts_with(embeddingPrefix)) {
            auto decoded = parsePayload.template operator()<EmbeddingRecord>();
            if (!decoded) {
                return decoded.error();
            }
            tombstonePayload = decoded.value().chunkId;
        } else if (logicalKey.starts_with(nodePrefix)) {
            auto decoded = parsePayload.template operator()<TopologyNodeRecord>();
            if (!decoded) {
                return decoded.error();
            }
            tombstonePayload = decoded.value().nodeKey;
        } else if (logicalKey.starts_with(edgePrefix)) {
            auto decoded = parsePayload.template operator()<TopologyEdgeRecord>();
            if (!decoded) {
                return decoded.error();
            }
            tombstonePayload = nlohmann::json(decoded.value()).dump();
        } else if (logicalKey.starts_with(blobPrefix)) {
            tombstonePayload = std::string(logicalKey.substr(blobPrefix.size()));
            if (!isSha256Digest(tombstonePayload)) {
                return Error{ErrorCode::InvalidData, "quarantined content-blob key is invalid"};
            }
        } else {
            tombstonePayload = std::string(logicalKey);
        }

        auto tombstone = record;
        tombstone.entryHash.clear();
        tombstone.recordKind = std::string(kMemoryTombstoneRecordKind);
        tombstone.tombstonePayload = std::move(tombstonePayload);
        return tombstone;
    }

    Result<void> captureQuarantineInvalidation(std::string_view logicalKey,
                                               const MemoryIndexRecord& record) {
        const auto source = quarantineInvalidationSources_.find(std::string(logicalKey));
        if (source != quarantineInvalidationSources_.end() &&
            resolveLww(record, source->second) != LwwDecision::First) {
            return {};
        }
        auto tombstone = makeQuarantineInvalidation(logicalKey, record);
        if (!tombstone) {
            // The durable writer quarantine remains authoritative. A malformed payload could not
            // have passed the corresponding adapter's identity validation, so omit only that
            // downstream invalidation and continue reconstructing the remaining valid keys.
            return {};
        }
        quarantineInvalidationSources_[std::string(logicalKey)] = record;
        quarantineInvalidations_[std::string(logicalKey)] = std::move(tombstone.value());
        ++durableQuarantineGeneration_;
        return {};
    }

    void removeVisibleWinnersFrom(std::string_view writerId) {
        for (auto it = merged_.begin(); it != merged_.end();) {
            if (it->second.origin == writerId) {
                it = merged_.erase(it);
            } else {
                ++it;
            }
        }
        pruneUnreferencedCachedBlobs();
    }

    void removeQuarantinedWinners() {
        for (auto it = merged_.begin(); it != merged_.end();) {
            if (writerQuarantined(it->second.origin)) {
                it = merged_.erase(it);
            } else {
                ++it;
            }
        }
        pruneUnreferencedCachedBlobs();
    }

    void pruneUnreferencedCachedBlobs() {
        std::set<std::string> winningHashes;
        for (const auto& [_, record] : merged_) {
            if (!record.isTombstone()) {
                winningHashes.insert(record.entryHash);
            }
        }
        for (auto it = cachedBlobs_.begin(); it != cachedBlobs_.end();) {
            if (!winningHashes.contains(it->first)) {
                cachedBytes_ -= it->second.size();
                it = cachedBlobs_.erase(it);
            } else {
                ++it;
            }
        }
    }

    static std::string hashContent(std::span<const std::byte> content) {
        crypto::SHA256Hasher hasher;
        hasher.init();
        hasher.update(content);
        return hasher.finalize();
    }

    static std::vector<std::byte> serialize(const MemoryIndexRecord& record) {
        const std::string dump = nlohmann::json(record).dump();
        std::vector<std::byte> bytes(dump.size());
        std::memcpy(bytes.data(), dump.data(), dump.size());
        return bytes;
    }

    static Result<MemoryIndexRecord> deserialize(std::span<const std::byte> bytes) {
        try {
            const std::string_view text(reinterpret_cast<const char*>(bytes.data()), bytes.size());
            return nlohmann::json::parse(text).get<MemoryIndexRecord>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::InvalidData, e.what()};
        }
    }

    void quarantine(std::string_view key, std::string_view reason) {
        quarantined_[std::string(key)] = std::string(reason);
    }

    void recordAuthFailure() noexcept {
        if (authFailures_ < limits_.maxTrackedIdentities) {
            ++authFailures_;
        }
    }

    storage::IStorageBackend* backend_;
    NodeId nodeId_;
    std::string corpusId_;
    std::uint64_t corpusEpoch_{0};
    bool allowLegacyUnbound_{false};
    MemorySyncLimits limits_;
    MemorySyncControl control_;
    TombstoneGcPolicy tombstoneGc_;
    std::shared_ptr<const WriterAuthenticator> writerAuth_;
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
    std::uint64_t durableQuarantineGeneration_{0};
    std::size_t authFailures_{0};
};

} // namespace yams::memory_sync
