#pragma once

#include <yams/topology/topology_engine.h>

#include <memory>

namespace yams::vector {
class StaticCosineAnnIndex;
}

namespace yams::topology {

/// Immutable query-time index derived from a validated topology artifact batch.
/// Cluster indices refer to the corresponding position in TopologyArtifactBatch::clusters.
struct SparseRouteIndex {
    std::unordered_map<std::string, std::vector<std::size_t>> clustersByDocumentHash;
    std::vector<float> centroidNorms;
    std::vector<std::vector<float>> routingRepresentativeNorms;
    std::shared_ptr<const yams::vector::StaticCosineAnnIndex> centroidAnnIndex;
};

struct SparseRouteWork {
    std::size_t seedClusterLookups{0};
    std::size_t clusterMemberHashesScanned{0};
    std::size_t queryNormEvaluations{0};
    std::size_t representativeDistanceEvaluations{0};
    std::size_t exactRepresentativeDistanceEvaluations{0};
    std::size_t denseAnnDistanceEvaluations{0};
    std::size_t denseAnnCandidates{0};
    bool denseAnnUsed{false};
};

class ConnectedComponentTopologyEngine final : public ITopologyEngine {
public:
    Result<TopologyArtifactBatch> buildArtifacts(std::span<const TopologyDocumentInput> documents,
                                                 const TopologyBuildConfig& config) override;

    Result<TopologyDirtyRegion>
    defineDirtyRegion(const TopologyArtifactBatch& existing,
                      std::span<const TopologyDocumentInput> changedDocuments,
                      const TopologyBuildConfig& config) const override;

    Result<TopologyArtifactBatch>
    updateArtifacts(const TopologyArtifactBatch& existing,
                    std::span<const TopologyDocumentInput> changedDocuments,
                    const TopologyBuildConfig& config,
                    TopologyUpdateStats* stats = nullptr) override;
};

// Phase S: multi-modal router. Combines BM25-hit overlap with cluster
// membership (the "sparse signal") and cosine similarity to cluster centroids
// (the "dense signal") into a single cluster score:
//
//     score(c) = alpha * normalized_bm25_mass(c) + (1-alpha) * cosine(query_emb, centroid(c))
//
// where bm25_mass(c) = count of request.seedDocumentHashes whose primary or
// hierarchical membership is c. Secondary boundary-spill postings do not vote
// for routes; they enlarge retrieval only after a route is selected. Falls back
// gracefully when queryEmbedding is empty (degrades to pure seed scoring) or
// when cluster.centroidEmbedding is empty (skips dense leg for that cluster).
class SparseGuidedClusterRouter final {
public:
    /// Build immutable exact-routing structures and optionally the centroid ANN shortlist.
    [[nodiscard]] static SparseRouteIndex buildRouteIndex(const TopologyArtifactBatch& artifacts,
                                                          bool buildDenseAnnIndex = true);

    Result<std::vector<ClusterRoute>> route(const TopologyRouteRequest& request,
                                            const TopologyArtifactBatch& artifacts) const;

    Result<std::vector<ClusterRoute>> route(const TopologyRouteRequest& request,
                                            const TopologyArtifactBatch& artifacts,
                                            const SparseRouteIndex& index,
                                            SparseRouteWork* work = nullptr) const;
};

} // namespace yams::topology
