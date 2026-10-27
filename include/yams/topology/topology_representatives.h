#pragma once

#include <yams/topology/topology_artifacts.h>

#include <cstddef>
#include <optional>
#include <span>
#include <vector>

namespace yams::topology {

/// Select at most routingRepresentativeCount - 1 document embeddings that diversify the
/// centroid route representative. Selection is deterministic farthest-first under cosine
/// distance; the centroid always remains the first (implicit) representative.
[[nodiscard]] std::vector<ClusterRoutingRepresentative> selectDiverseRoutingRepresentatives(
    std::span<const TopologyDocumentInput> documents, std::span<const std::size_t> members,
    std::span<const float> centroidEmbedding, std::size_t routingRepresentativeCount);

/// As above, but the first farthest-first step measures distance from seedEmbedding instead of
/// the centroid. The Medoid representative rule seeds from the chosen medoid so the extra
/// representatives spread away from the document the cluster is labelled by. An empty or
/// dimension-mismatched seed falls back to the centroid.
[[nodiscard]] std::vector<ClusterRoutingRepresentative> selectDiverseRoutingRepresentatives(
    std::span<const TopologyDocumentInput> documents, std::span<const std::size_t> members,
    std::span<const float> centroidEmbedding, std::size_t routingRepresentativeCount,
    std::span<const float> seedEmbedding);

/// Members beyond this count are subsampled before the O(n^2) medoid search: an even stride over
/// the hash-sorted usable members (deterministic) supplies both the candidate and the reference
/// sets. Connected-component clusters stay under the 64-doc production cap; Louvain and k-means
/// clusters can be larger.
inline constexpr std::size_t kMedoidRepresentativeMaxMembers = 256;

struct MedoidRepresentativeSelection {
    /// Index into the documents span.
    std::size_t document{0};
    /// Sum over the other evaluated members of sqrt(max(0, 2(1 - cos))).
    double objective{0.0};
    /// Members that took part in the search (after the cap).
    std::size_t evaluatedMembers{0};
};

/// Choose the member minimising the summed chordal distance to the other members. Members with
/// an empty, non-finite, zero-norm or dimension-mismatched embedding are ignored. Ties within
/// 1e-9 go to the smaller document hash. nullopt when fewer than two members are usable.
[[nodiscard]] std::optional<MedoidRepresentativeSelection>
selectMedoidRepresentative(std::span<const TopologyDocumentInput> documents,
                           std::span<const std::size_t> members,
                           std::size_t maxMembers = kMedoidRepresentativeMaxMembers);

/// Shared representative-rule dispatch used by every topology engine. Each engine computes its
/// own weighted-degree choice (unchanged historical behaviour) and passes it as degreeChoice;
/// Degree returns it untouched, Medoid returns selectMedoidRepresentative() or degreeChoice when
/// the cluster has no usable geometry.
[[nodiscard]] std::size_t applyRepresentativeRule(RepresentativeRule rule,
                                                  std::span<const TopologyDocumentInput> documents,
                                                  std::span<const std::size_t> members,
                                                  std::size_t degreeChoice);

/// Add bounded SOAR-style secondary cluster assignments for documents near a partition boundary.
/// Primary memberships and centroids remain unchanged; admitted secondary assignments are recorded
/// in DocumentClusterMembership::overlapClusterIds and materialized in the secondary cluster's
/// memberDocumentHashes for routed retrieval.
[[nodiscard]] std::size_t
applyOrthogonalBoundarySpill(std::span<const TopologyDocumentInput> documents,
                             const TopologyBuildConfig& config, TopologyArtifactBatch& artifacts);

} // namespace yams::topology
