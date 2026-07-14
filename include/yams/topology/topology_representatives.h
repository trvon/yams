#pragma once

#include <yams/topology/topology_artifacts.h>

#include <span>
#include <vector>

namespace yams::topology {

/// Select at most routingRepresentativeCount - 1 document embeddings that diversify the
/// centroid route representative. Selection is deterministic farthest-first under cosine
/// distance; the centroid always remains the first (implicit) representative.
[[nodiscard]] std::vector<ClusterRoutingRepresentative> selectDiverseRoutingRepresentatives(
    std::span<const TopologyDocumentInput> documents, std::span<const std::size_t> members,
    std::span<const float> centroidEmbedding, std::size_t routingRepresentativeCount);

/// Add bounded SOAR-style secondary cluster assignments for documents near a partition boundary.
/// Primary memberships and centroids remain unchanged; admitted secondary assignments are recorded
/// in DocumentClusterMembership::overlapClusterIds and materialized in the secondary cluster's
/// memberDocumentHashes for routed retrieval.
[[nodiscard]] std::size_t applyOrthogonalBoundarySpill(
    std::span<const TopologyDocumentInput> documents, const TopologyBuildConfig& config,
    TopologyArtifactBatch& artifacts);

} // namespace yams::topology
