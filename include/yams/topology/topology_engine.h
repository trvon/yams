#pragma once

#include <yams/topology/topology_artifacts.h>

#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace yams::topology {

class ITopologyArtifactStore {
public:
    virtual ~ITopologyArtifactStore() = default;

    virtual Result<void> storeBatch(const TopologyArtifactBatch& batch) = 0;

    virtual Result<std::optional<TopologyArtifactBatch>>
    loadLatest(std::string_view snapshotId = {}) const = 0;

    virtual Result<std::vector<DocumentClusterMembership>>
    loadMemberships(std::span<const std::string> documentHashes) const = 0;
};

class ITopologyEngine {
public:
    virtual ~ITopologyEngine() = default;

    virtual Result<TopologyArtifactBatch>
    buildArtifacts(std::span<const TopologyDocumentInput> documents,
                   const TopologyBuildConfig& config) = 0;

    virtual Result<TopologyArtifactBatch>
    updateArtifacts(const TopologyArtifactBatch& existing,
                    std::span<const TopologyDocumentInput> changedDocuments,
                    const TopologyBuildConfig& config, TopologyUpdateStats* stats = nullptr) = 0;
};

class ITopologyRouter {
public:
    virtual ~ITopologyRouter() = default;

    virtual Result<std::vector<ClusterRoute>>
    route(const TopologyRouteRequest& request, const TopologyArtifactBatch& artifacts) const = 0;
};

} // namespace yams::topology
