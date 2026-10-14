// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/topology/topology_artifacts.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace yams::topology {

/// Lookup from document hash to its input, as `computeClusterMemberOrder` expects. Documents with
/// an empty hash are skipped; on a duplicate hash the first one wins.
[[nodiscard]] std::unordered_map<std::string_view, const TopologyDocumentInput*>
indexTopologyDocumentsByHash(std::span<const TopologyDocumentInput> documents);

/// Walk order of one cluster as a permutation over `cluster.memberDocumentHashes` (see
/// RoadOrderKind for each kind's rule). Empty for None and for clusters with fewer than two
/// members. A member missing from `documentsByHash` has no time, no session and no embedding.
/// Deterministic: every tie resolves to the smaller member hash, i.e. the smaller index when the
/// members are hash-sorted, as every engine stores them.
[[nodiscard]] std::vector<uint32_t> computeClusterMemberOrder(
    RoadOrderKind kind, const ClusterArtifact& cluster,
    const std::unordered_map<std::string_view, const TopologyDocumentInput*>& documentsByHash);

/// Set `orderKind` / `orderedMembers` on every cluster of `batch` from `documents`. None clears
/// them. A cluster whose order comes out empty is stored as None, so `orderKind != None` holds
/// exactly when `orderedMembers` is non-empty.
void applyClusterMemberOrders(TopologyArtifactBatch& batch,
                              std::span<const TopologyDocumentInput> documents, RoadOrderKind kind);

/// Check that `permutation` is a permutation of `[0, memberCount)`. nullopt when it is; otherwise
/// the first violation as a stable token: `order_length_mismatch`, `order_index_out_of_range`,
/// `order_index_repeated`.
[[nodiscard]] std::optional<std::string_view>
validateMemberPermutation(std::span<const uint32_t> permutation, std::size_t memberCount);

} // namespace yams::topology
