// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <compare>
#include <cstdint>
#include <map>
#include <string>

#include <nlohmann/json.hpp>

namespace yams::memory_sync {

/// Unique writer identity used for version-vector and LWW tie-breaking.
using NodeId = std::string;

/// Hybrid logical-ish timestamp: physical wall-clock milliseconds, a Lamport-style
/// logical counter to order writes within the same millisecond, and the writer
/// identity as the final deterministic tie-breaker.
struct HybridTimestamp {
    std::uint64_t physicalMs{0};
    std::uint64_t logical{0};
    NodeId origin{};

    auto operator<=>(const HybridTimestamp&) const = default;
    bool operator==(const HybridTimestamp&) const = default;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(HybridTimestamp, physicalMs, logical, origin)
};

/// Dotted version vector over writer identities. Merges element-wise (max) and
/// exposes the causal partial order (`dominates`) and its negation (`concurrent`).
class VersionVector {
public:
    /// Counter for `node`, or 0 when absent (a missing entry behaves as 0).
    std::uint64_t get(const NodeId& node) const {
        const auto it = counters_.find(node);
        return it == counters_.end() ? 0 : it->second;
    }

    /// Bump this writer's counter. A writer increments only its own entry.
    void increment(const NodeId& node) { ++counters_[node]; }

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

/// One mutable index entry: a content-addressed blob plus its LWW/causal metadata.
struct MemoryIndexRecord {
    std::string entryHash; /// sha256 of the memory record (content-addressed)
    HybridTimestamp ts;    /// LWW scalar + origin tie-breaker
    VersionVector version; /// causal history at write time

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(MemoryIndexRecord, entryHash, ts, version)
};

/// Resolve two records for the same logical key. Causally-later writes win
/// regardless of timestamp; concurrent writes resolve deterministically by the
/// greater `(physicalMs, logical, origin)` tuple.
inline bool lwwWins(const MemoryIndexRecord& a, const MemoryIndexRecord& b) {
    if (a.version.dominates(b.version)) {
        return true;
    }
    if (b.version.dominates(a.version)) {
        return false;
    }
    return a.ts > b.ts;
}

} // namespace yams::memory_sync
