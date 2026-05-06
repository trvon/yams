#pragma once

#include <yams/topology/topology_engine.h>

namespace yams::topology {

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

class StableClusterTopologyRouter final : public ITopologyRouter {
public:
    Result<std::vector<ClusterRoute>> route(const TopologyRouteRequest& request,
                                            const TopologyArtifactBatch& artifacts) const override;
};

// Phase S: multi-modal router. Combines BM25-hit overlap with cluster
// membership (the "sparse signal") and cosine similarity to cluster centroids
// (the "dense signal") into a single cluster score:
//
//     score(c) = alpha * normalized_bm25_mass(c) + (1-alpha) * cosine(query_emb, centroid(c))
//
// where bm25_mass(c) = count of request.seedDocumentHashes that fall within
// cluster c's memberDocumentHashes. Falls back gracefully when queryEmbedding
// is empty (degrades to pure seed scoring) or when cluster.centroidEmbedding
// is empty (skips dense leg for that cluster).
class SparseGuidedClusterRouter final : public ITopologyRouter {
public:
    Result<std::vector<ClusterRoute>> route(const TopologyRouteRequest& request,
                                            const TopologyArtifactBatch& artifacts) const override;
};

} // namespace yams::topology
