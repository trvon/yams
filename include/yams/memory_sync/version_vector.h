// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <algorithm>
#include <cctype>
#include <compare>
#include <cstdint>
#include <limits>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>

#include <nlohmann/json.hpp>

namespace yams::memory_sync {

/// Unique writer identity used for version-vector and LWW tie-breaking.
using NodeId = std::string;

/// Hybrid logical timestamp. Record ownership is deliberately separate so the
/// memory-sync envelope has one canonical writer identity.
struct HybridTs {
    std::uint64_t physicalMs{0};
    std::uint64_t logical{0};

    auto operator<=>(const HybridTs&) const = default;
    bool operator==(const HybridTs&) const = default;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(HybridTs, physicalMs, logical)
};

/// Compatibility name for callers that adopted the pre-envelope timestamp type.
using HybridTimestamp = HybridTs;

/// Dotted version vector over writer identities. Merges element-wise (max) and
/// exposes the causal partial order (`dominates`) and its negation (`concurrent`).
class VersionVector {
public:
    /// Counter for `node`, or 0 when absent (a missing entry behaves as 0).
    std::uint64_t get(const NodeId& node) const {
        const auto it = counters_.find(node);
        return it == counters_.end() ? 0 : it->second;
    }

    /// Bump this writer's counter. Returns false rather than wrapping at UINT64_MAX.
    bool increment(const NodeId& node) {
        auto& counter = counters_[node];
        if (counter == std::numeric_limits<std::uint64_t>::max()) {
            return false;
        }
        ++counter;
        return true;
    }

    /// Observe a durable writer frontier without replaying every preceding increment.
    void observe(const NodeId& node, std::uint64_t counter) {
        if (counter > get(node)) {
            counters_[node] = counter;
        }
    }

    /// Element-wise max: keep the causal union of both histories.
    void merge(const VersionVector& other) {
        for (const auto& [node, count] : other.counters_) {
            auto& mine = counters_[node];
            if (count > mine) {
                mine = count;
            }
        }
    }

    /// True when this >= other for every node AND strictly greater somewhere.
    /// Equal vectors do not dominate.
    bool dominates(const VersionVector& other) const {
        bool anyGreater = false;
        for (const auto& [node, count] : other.counters_) {
            const std::uint64_t mine = get(node);
            if (mine < count) {
                return false;
            }
            if (mine > count) {
                anyGreater = true;
            }
        }
        for (const auto& [node, count] : counters_) {
            if (other.get(node) < count) {
                anyGreater = true;
            }
        }
        return anyGreater;
    }

    /// True when neither vector dominates the other (a concurrent update pair).
    bool concurrent(const VersionVector& other) const {
        return !dominates(other) && !other.dominates(*this);
    }

    bool empty() const { return counters_.empty(); }

    const std::map<NodeId, std::uint64_t>& counters() const { return counters_; }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(VersionVector, counters_)

private:
    std::map<NodeId, std::uint64_t> counters_;
};

/// True if `value` is a lowercase or uppercase hexadecimal SHA-256 digest.
inline bool isSha256Digest(std::string_view value) {
    return value.size() == 64 && std::all_of(value.begin(), value.end(),
                                             [](unsigned char ch) { return std::isxdigit(ch); });
}

inline constexpr std::uint32_t kLegacyBoundMemoryIndexSchemaVersion = 2;
inline constexpr std::uint32_t kMemoryIndexSchemaVersion = 3;
inline constexpr std::uint32_t kAuthenticatedMemoryIndexSchemaVersion = 4;
inline constexpr std::string_view kMemoryIndexSignatureAlgorithm = "Ed25519";
inline constexpr std::string_view kMemoryValueRecordKind = "value";
inline constexpr std::string_view kMemoryTombstoneRecordKind = "tombstone";

inline std::string makeOperationId(std::string_view origin, std::uint64_t counter) {
    return std::string(origin) + ":" + std::to_string(counter);
}

/// One mutable index entry: a content-addressed blob plus its LWW/causal metadata.
struct MemoryIndexRecord {
    std::uint32_t schemaVersion{kMemoryIndexSchemaVersion};
    std::string entryHash;          /// SHA-256 content-addressed blob key
    HybridTs ts;                    /// LWW scalar
    NodeId origin;                  /// writer identity and LWW tie-breaker
    VersionVector vv;               /// causal history at write time
    std::string corpusId;           /// stable replication-domain identifier
    std::uint64_t corpusEpoch{0};   /// incompatible corpus reset/migration generation
    std::string operationId;        /// canonical origin:counter identity
    std::string logicalKey;         /// key bound into this envelope
    std::string recordKind;         /// value or causally ordered tombstone
    std::string tombstonePayload;   /// optional adapter-specific stable delete identity
    std::string signatureAlgorithm; /// schema-v4 signature algorithm
    std::string signingKeyId;       /// configured writer-key generation identifier
    std::string signature;          /// lowercase hex Ed25519 signature

    bool isTombstone() const noexcept { return recordKind == kMemoryTombstoneRecordKind; }
    bool hasValidEntryHash() const {
        return isTombstone() ? entryHash.empty() : isSha256Digest(entryHash);
    }

    bool hasValidCausalIdentity(std::string_view expectedCorpus,
                                std::uint64_t expectedEpoch) const {
        const auto writerCounter = vv.get(origin);
        return !origin.empty() && origin != "default" && !corpusId.empty() && corpusEpoch != 0 &&
               corpusId == expectedCorpus && corpusEpoch == expectedEpoch && writerCounter != 0 &&
               operationId == makeOperationId(origin, writerCounter);
    }

    bool hasValidIdentity(std::string_view expectedCorpus, std::uint64_t expectedEpoch,
                          std::string_view expectedLogicalKey) const {
        const bool validKind =
            recordKind == kMemoryValueRecordKind || recordKind == kMemoryTombstoneRecordKind;
        return hasValidCausalIdentity(expectedCorpus, expectedEpoch) &&
               logicalKey == expectedLogicalKey && validKind && hasValidEntryHash();
    }
};

inline nlohmann::json boundEnvelopeJson(const MemoryIndexRecord& record) {
    return {{"schemaVersion", record.schemaVersion},
            {"entryHash", record.entryHash},
            {"ts", record.ts},
            {"origin", record.origin},
            {"vv", record.vv},
            {"corpusId", record.corpusId},
            {"corpusEpoch", record.corpusEpoch},
            {"operationId", record.operationId},
            {"logicalKey", record.logicalKey},
            {"recordKind", record.recordKind},
            {"tombstonePayload", record.tombstonePayload}};
}

inline nlohmann::json canonicalAuthenticatedEnvelopeJson(const MemoryIndexRecord& record) {
    auto json = boundEnvelopeJson(record);
    json["signatureAlgorithm"] = record.signatureAlgorithm;
    json["signingKeyId"] = record.signingKeyId;
    return json;
}

inline std::string canonicalEnvelopeBytes(const MemoryIndexRecord& record) {
    return canonicalAuthenticatedEnvelopeJson(record).dump();
}

inline void to_json(nlohmann::json& json, const MemoryIndexRecord& record) {
    if (!record.hasValidEntryHash()) {
        throw std::invalid_argument("memory index entryHash must be a SHA-256 digest");
    }
    if (!record.hasValidIdentity(record.corpusId, record.corpusEpoch, record.logicalKey)) {
        throw std::invalid_argument("memory index record has invalid causal identity");
    }
    if (record.schemaVersion != kMemoryIndexSchemaVersion &&
        record.schemaVersion != kAuthenticatedMemoryIndexSchemaVersion) {
        throw std::invalid_argument("memory index record has unsupported bound schema");
    }
    if (record.schemaVersion == kAuthenticatedMemoryIndexSchemaVersion &&
        (record.signatureAlgorithm != kMemoryIndexSignatureAlgorithm ||
         record.signingKeyId.empty() || record.signature.empty())) {
        throw std::invalid_argument("authenticated memory index record lacks signature fields");
    }
    json = boundEnvelopeJson(record);
    if (record.schemaVersion == kAuthenticatedMemoryIndexSchemaVersion) {
        json["signatureAlgorithm"] = record.signatureAlgorithm;
        json["signingKeyId"] = record.signingKeyId;
        json["signature"] = record.signature;
    }
}

inline void from_json(const nlohmann::json& json, MemoryIndexRecord& record) {
    record.entryHash = json.at("entryHash").get<std::string>();

    if (json.contains("schemaVersion")) {
        const auto schemaVersion = json.at("schemaVersion").get<std::uint32_t>();
        record.schemaVersion = schemaVersion;
        if (schemaVersion != kAuthenticatedMemoryIndexSchemaVersion &&
            schemaVersion != kMemoryIndexSchemaVersion &&
            schemaVersion != kLegacyBoundMemoryIndexSchemaVersion) {
            throw std::invalid_argument("unsupported memory index schema version: " +
                                        std::to_string(schemaVersion));
        }
        record.ts = json.at("ts").get<HybridTs>();
        record.origin = json.at("origin").get<NodeId>();
        record.vv = json.at("vv").get<VersionVector>();
        if (schemaVersion >= kMemoryIndexSchemaVersion) {
            record.corpusId = json.at("corpusId").get<std::string>();
            record.corpusEpoch = json.at("corpusEpoch").get<std::uint64_t>();
            record.operationId = json.at("operationId").get<std::string>();
            record.logicalKey = json.at("logicalKey").get<std::string>();
            record.recordKind = json.at("recordKind").get<std::string>();
            record.tombstonePayload = json.value("tombstonePayload", std::string{});
        }
        if (schemaVersion == kAuthenticatedMemoryIndexSchemaVersion) {
            record.signatureAlgorithm = json.at("signatureAlgorithm").get<std::string>();
            record.signingKeyId = json.at("signingKeyId").get<std::string>();
            record.signature = json.at("signature").get<std::string>();
        }
        if (!record.hasValidEntryHash()) {
            throw std::invalid_argument("memory index entryHash does not match record kind");
        }
        return;
    }

    // Compatibility with records written before explicit schema versioning.
    // The original format stored writer identity in ts.origin and used `version`;
    // the intermediate worktree format already used top-level origin + `vv`.
    record.schemaVersion = 1;
    const auto& timestamp = json.at("ts");
    record.ts.physicalMs = timestamp.at("physicalMs").get<std::uint64_t>();
    record.ts.logical = timestamp.at("logical").get<std::uint64_t>();
    if (!isSha256Digest(record.entryHash)) {
        throw std::invalid_argument("legacy memory index entryHash must be a SHA-256 digest");
    }
    if (json.contains("version")) {
        record.origin = timestamp.at("origin").get<NodeId>();
        record.vv = json.at("version").get<VersionVector>();
    } else {
        record.origin = json.at("origin").get<NodeId>();
        record.vv = json.at("vv").get<VersionVector>();
    }
}

enum class LwwDecision { First, Second, Fork };

/// Resolve two records for the same logical key. Causally-later writes win
/// regardless of timestamp; concurrent writes use `(physicalMs, logical, origin)`.
/// Reusing one causal vector for different payloads is a writer fork and must not
/// be converted into an arbitrary timestamp winner.
inline LwwDecision resolveLww(const MemoryIndexRecord& a, const MemoryIndexRecord& b) {
    if (a.vv.dominates(b.vv)) {
        return LwwDecision::First;
    }
    if (b.vv.dominates(a.vv)) {
        return LwwDecision::Second;
    }
    if (a.vv.counters() == b.vv.counters() &&
        std::tie(a.recordKind, a.entryHash, a.tombstonePayload) !=
            std::tie(b.recordKind, b.entryHash, b.tombstonePayload)) {
        return LwwDecision::Fork;
    }
    if (a.isTombstone() != b.isTombstone()) {
        return a.isTombstone() ? LwwDecision::First : LwwDecision::Second;
    }
    return std::tie(a.ts, a.origin, a.entryHash) > std::tie(b.ts, b.origin, b.entryHash)
               ? LwwDecision::First
               : LwwDecision::Second;
}

/// Compatibility predicate for callers that only need to know whether `a` wins.
/// Forks fail closed and therefore never select either operand.
inline bool lwwWins(const MemoryIndexRecord& a, const MemoryIndexRecord& b) {
    return resolveLww(a, b) == LwwDecision::First;
}

} // namespace yams::memory_sync
