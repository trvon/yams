// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <nlohmann/json.hpp>

#include <yams/core/types.h>
#include <yams/crypto/hasher.h>
#include <yams/memory_sync/key_policy.h>
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
    std::chrono::milliseconds minRetention{std::chrono::milliseconds::max()};
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
        if (auto reconciled = sync(); !reconciled) {
            return reconciled.error();
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
        return {};
    }

    /// Publish a causally ordered deletion. Tombstones carry no content blob and
    /// are retained in index history so an offline peer cannot resurrect an older value.
    Result<void> erase(std::string_view logicalKey, std::string tombstonePayload = {}) {
        if (auto valid = validateLogicalKey(logicalKey); !valid) {
            return valid.error();
        }
        if (tombstonePayload.size() > limits_.maxEnvelopeBytes) {
            return Error{ErrorCode::InvalidArgument,
                         "memory sync tombstone payload exceeds envelope limit"};
        }
        if (auto reconciled = sync(); !reconciled) {
            return reconciled.error();
        }
        if (nodeId_.empty() || nodeId_ == "default" || corpusId_.empty() || corpusEpoch_ == 0) {
            return Error{ErrorCode::InvalidArgument, "memory sync identity is not configured"};
        }
        if (!version_.increment(nodeId_)) {
            return Error{ErrorCode::InvalidState, "memory sync writer counter exhausted"};
        }
        ++logicalClock_;

        MemoryIndexRecord record;
        record.ts.physicalMs = nowMs();
        record.ts.logical = logicalClock_;
        record.origin = nodeId_;
        record.vv = version_;
        record.corpusId = corpusId_;
        record.corpusEpoch = corpusEpoch_;
        record.operationId = makeOperationId(nodeId_, version_.get(nodeId_));
        record.logicalKey = logicalKey;
        record.recordKind = std::string(kMemoryTombstoneRecordKind);
        record.tombstonePayload = std::move(tombstonePayload);
        if (writerAuth_) {
            if (auto signedRecord = writerAuth_->sign(record); !signedRecord) {
                return signedRecord.error();
            }
        }

        const auto recordBytes = serialize(record);
        if (recordBytes.size() > limits_.maxEnvelopeBytes) {
            return Error{ErrorCode::InvalidArgument,
                         "memory sync tombstone envelope exceeds configured limit"};
        }
        const std::string recordHash = hashContent(recordBytes);
        if (auto ready = beforeRemoteWork(); !ready) {
            return ready.error();
        }
        return backend_->store(indexKey(logicalKey, recordHash), recordBytes);
    }

    /// Reconcile against the backend: list the index, pull every record, LWW-merge
    /// per logical key, and fold remote histories into this node's version vector.
    /// Returns the merged `logicalKey -> winning record` map.
    Result<std::map<std::string, MemoryIndexRecord>> sync() {
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
        const auto previousMerged = merged_;
        auto merged = merged_;
        for (const auto& candidate : candidates) {
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

        // Forks discovered in a later bounded window invalidate an earlier winner.
        for (auto it = merged.begin(); it != merged.end();) {
            if (forkedOperations_.contains(it->second.operationId)) {
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

    /// Collect tombstone history only when the complete configured replica set has
    /// acknowledged the exact delete operation and the retention horizon has elapsed.
    /// Empty replica sets disable collection. A history larger than one bounded page
    /// is retained rather than partially collected.
    Result<std::size_t> collectTombstoneGarbage(std::uint64_t now = nowMs()) {
        if (tombstoneGc_.requiredPeers.empty()) {
            return std::size_t{0};
        }
        std::size_t collected = 0;
        for (auto it = merged_.begin(); it != merged_.end();) {
            const auto& record = it->second;
            const auto retentionMs = static_cast<std::uint64_t>(
                std::max<std::chrono::milliseconds::rep>(0, tombstoneGc_.minRetention.count()));
            if (!record.isTombstone() || now < record.ts.physicalMs ||
                now - record.ts.physicalMs < retentionMs) {
                ++it;
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
                ++it;
                continue;
            }
            const std::string prefix = "index/" + it->first + "/";
            auto page = backend_->listPage(prefix, std::nullopt, limits_.maxIndexObjectsPerSync);
            if (!page) {
                return page.error();
            }
            if (page.value().nextCursor) {
                ++it;
                continue;
            }
            const auto tombstoneKey = indexKey(it->first, hashContent(serialize(record)));
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
            it = merged_.erase(it);
            ++collected;
        }
        return collected;
    }

    /// Return the number of records observed by the most recent reconciliation.
    /// This is intentionally side-effect free so status callers do not force sync.
    std::size_t mergedRecordCount() const noexcept { return merged_.size(); }
    std::size_t quarantinedRecordCount() const noexcept { return quarantined_.size(); }
    std::size_t authFailureCount() const noexcept { return authFailures_; }

    /// Read only the last periodically reconciled winner. Daemon IPC uses this
    /// path so polling a peer cannot manufacture convergence by forcing sync.
    Result<std::vector<std::byte>> readCached(std::string_view logicalKey) const {
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
    bool hasMergedRecord(std::string_view logicalKey) const {
        return merged_.contains(std::string(logicalKey));
    }
    std::size_t testingCachedBlobCount() const noexcept { return cachedBlobs_.size(); }
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
    struct ScanCandidate {
        std::string indexKey;
        std::string logicalKey;
        std::string recordHash;
        MemoryIndexRecord record;
    };

    Result<void> beforeRemoteWork() const {
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
        authFailures_ = std::min(authFailures_ + (authFailures_ < limits_.maxTrackedIdentities),
                                 limits_.maxTrackedIdentities);
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
    std::map<std::string, std::string> quarantined_;
    std::size_t authFailures_{0};
};

} // namespace yams::memory_sync
