// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/core/types.h>
#include <yams/topology/topology_artifacts.h>

#include <cstddef>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace yams::topology {

/**
 * @brief Serializes a TopologyArtifactBatch to a compact, endian-portable binary buffer.
 *
 * Uses an interned string table to eliminate duplicate document hashes and cluster IDs.
 */
Result<std::vector<std::byte>> serializeTopologyBatchBinary(const TopologyArtifactBatch& batch);

/**
 * @brief Deserializes a TopologyArtifactBatch from a binary buffer.
 */
Result<TopologyArtifactBatch> deserializeTopologyBatchBinary(std::span<const std::byte> bytes);

/**
 * @brief Serializes a TopologyArtifactBatch into a Zstd-compressed JSON envelope.
 *
 * The envelope stores high-level snapshot statistics (cluster/membership count, epoch)
 * directly in JSON for zero-decompression inspection, with the compressed binary payload
 * stored as base64 in "data_b64".
 */
Result<std::string> serializeTopologyBatchCompressed(const TopologyArtifactBatch& batch,
                                                     int compressionLevel = 3);

/**
 * @brief Deserializes a TopologyArtifactBatch from either a compressed envelope or legacy raw JSON.
 *
 * Automatically detects whether the input is in "zstd_binary_v1" format or legacy uncompressed
 * JSON.
 */
Result<TopologyArtifactBatch> deserializeTopologyBatchCompressed(std::string_view payload);

/**
 * @brief Road sidecar: the persisted form of `TopologyArtifactBatch::roads`.
 *
 * Roads live beside the main snapshot, not inside it, so the v1 snapshot payload is unchanged
 * with roads on or off and a decoder that predates roads never sees road bytes. The binary form
 * has its own magic ('Y','A','M','R') and version; the header records the snapshot the roads
 * belong to and the per-source top-K bound the builder applied, which load-time validation
 * checks against the stored road list. Version 2 adds the road provenance
 * (`TopologyArtifactBatch::roadsFullBuildEpoch`, `roadsIncrementalPasses`) after the snapshot
 * id; a version 1 sidecar decodes with both at 0, i.e. as full roads. Version 3 appends the
 * clusters' walk orders (`ClusterArtifact::orderKind` / `orderedMembers`) after the roads; a
 * version 1 or 2 sidecar decodes with no orders. The sidecar is written when the batch has roads
 * or at least one ordered cluster.
 */
struct ClusterMemberOrder {
    std::string clusterId;
    RoadOrderKind kind{RoadOrderKind::None};
    std::vector<uint32_t> permutation;

    friend bool operator==(const ClusterMemberOrder&, const ClusterMemberOrder&) = default;
};

struct TopologyRoadSidecar {
    std::string snapshotId;
    std::size_t maxPerCluster{0};
    std::vector<ClusterRoad> roads;
    uint64_t roadsFullBuildEpoch{0};
    uint32_t roadsIncrementalPasses{0};
    /// One entry per cluster with a non-empty order, in cluster-id order.
    std::vector<ClusterMemberOrder> memberOrders;
};

/// The batch's ordered clusters as sidecar entries (clusters with an empty order are skipped).
[[nodiscard]] std::vector<ClusterMemberOrder>
collectTopologyMemberOrders(const TopologyArtifactBatch& batch);

/// Binary road sidecar (magic, version, top-K, snapshot id, provenance, cluster-id table, roads,
/// member orders).
Result<std::vector<std::byte>>
serializeTopologyRoadSidecarBinary(const TopologyRoadSidecar& sidecar);
Result<TopologyRoadSidecar> deserializeTopologyRoadSidecarBinary(std::span<const std::byte> bytes);

/// Road sidecar in a Zstd JSON envelope (`format = "topology_roads_zstd_v1"`), the form stored in
/// the `topology:roads:<snapshotId>` knowledge-graph node.
Result<std::string> serializeTopologyRoadSidecarCompressed(const TopologyRoadSidecar& sidecar,
                                                           int compressionLevel = 3);
Result<TopologyRoadSidecar> deserializeTopologyRoadSidecarCompressed(std::string_view payload);

/**
 * @brief Soft validation of a loaded sidecar against the snapshot it is attached to.
 *
 * Returns the first violation as a stable reason token, or nullopt when the sidecar is usable:
 * `road_snapshot_mismatch`, `road_endpoint_not_cluster`, `road_self_loop`,
 * `road_weight_invalid` (negative or non-finite), `road_count_exceeds_max_per_cluster`. An empty
 * road list is always valid. Callers drop the roads and record the reason; a bad sidecar must
 * never make the snapshot itself unusable.
 */
std::optional<std::string> validateTopologyRoadSidecar(const TopologyArtifactBatch& batch,
                                                       const TopologyRoadSidecar& sidecar);

/**
 * @brief Attach the sidecar's member orders to the matching clusters of `batch`.
 *
 * Each entry is validated on its own: the cluster must exist, the kind must not be None, and the
 * permutation must cover `memberDocumentHashes` exactly once (see validateMemberPermutation). An
 * invalid entry is skipped and `<token>:<clusterId>` is appended to
 * `batch.orderUnavailableReason` (`;`-joined); tokens are `order_cluster_not_found`,
 * `order_kind_invalid`, `order_duplicate_cluster`, `order_length_mismatch`,
 * `order_index_out_of_range`, `order_index_repeated`. A snapshot id mismatch drops every entry
 * with the single token `order_snapshot_mismatch`. Roads are never touched. Every cluster's
 * order is reset before attaching. Returns the number of orders attached.
 */
std::size_t attachTopologyMemberOrders(TopologyArtifactBatch& batch,
                                       const TopologyRoadSidecar& sidecar);

/**
 * @brief Converts binary bytes to a base64 string.
 */
std::string encodeBase64(std::span<const std::byte> bytes);

/**
 * @brief Decodes a base64 string into binary bytes.
 */
Result<std::vector<std::byte>> decodeBase64(std::string_view base64Str);

} // namespace yams::topology
