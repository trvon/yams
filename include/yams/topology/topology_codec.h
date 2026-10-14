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
 * checks against the stored road list.
 */
struct TopologyRoadSidecar {
    std::string snapshotId;
    std::size_t maxPerCluster{0};
    std::vector<ClusterRoad> roads;
};

/// Binary road sidecar (magic, version, top-K, snapshot id, cluster-id table, roads).
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
 * @brief Converts binary bytes to a base64 string.
 */
std::string encodeBase64(std::span<const std::byte> bytes);

/**
 * @brief Decodes a base64 string into binary bytes.
 */
Result<std::vector<std::byte>> decodeBase64(std::string_view base64Str);

} // namespace yams::topology
