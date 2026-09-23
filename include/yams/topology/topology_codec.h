// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/core/types.h>
#include <yams/topology/topology_artifacts.h>

#include <cstddef>
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
 * @brief Converts binary bytes to a base64 string.
 */
std::string encodeBase64(std::span<const std::byte> bytes);

/**
 * @brief Decodes a base64 string into binary bytes.
 */
Result<std::vector<std::byte>> decodeBase64(std::string_view base64Str);

} // namespace yams::topology
