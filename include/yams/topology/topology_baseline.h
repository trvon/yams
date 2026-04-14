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

} // namespace yams::topology
