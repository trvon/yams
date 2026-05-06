#pragma once

#include <yams/topology/topology_engine.h>

namespace yams::topology {

class HDBSCANTopologyEngine final : public ITopologyEngine {
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

// Phase H: Louvain modularity-greedy clustering on the reciprocal-edge graph.
// Reads `documents[i].neighbors` (built by buildPairWeights). Produces
// communities whose internal edge weight maximizes graph modularity.
// Algorithm: single-pass node-move greedy (Phase 1 of Blondel 2008's full
// multilevel Louvain). For each node, considers moving it to each neighbor's
// community; keeps the move with maximum ΔQ if positive. Repeats until a full
// pass produces no improvement OR the iteration cap is hit.
//
// No per-engine config knobs today; relies on `config.minEdgeScore` and
// `config.reciprocalOnly` from the shared graph-construction step.
class LouvainTopologyEngine final : public ITopologyEngine {
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
