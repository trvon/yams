#pragma once

#include <yams/topology/topology_engine.h>

namespace yams::topology {

// Phase S: Online KMeans engine.
//
// Designed for ingest-time bucket alignment. Unlike HDBSCAN/Louvain whose
// updateArtifacts() falls back to a full rebuild, OnlineKMeansTopologyEngine
// implements a genuinely incremental updateArtifacts: each new doc is assigned
// to the nearest existing centroid (or spawns a new bucket if no centroid
// exceeds config.minSimilarityToJoin), and the centroid is updated via running
// mean.
//
// buildArtifacts() bootstraps via KMeans++ initialization with K = config.kmeansK
// (or auto K = max(64, min(300, sqrt(n))) when kmeansK==0) and runs Lloyd
// iterations up to config.kmeansMaxIterations.
//
// Centroids are persisted in ClusterArtifact::centroidEmbedding. Members are
// stored as ClusterArtifact::memberDocumentHashes; medoid is the closest member
// to the centroid.
class OnlineKMeansTopologyEngine final : public ITopologyEngine {
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

} // namespace yams::topology
