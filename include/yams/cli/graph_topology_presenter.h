#pragma once

#include <yams/core/types.h>
#include <yams/topology/topology_engine.h>

#include <filesystem>
#include <iosfwd>
#include <optional>
#include <string>

namespace yams::cli {

class GraphTopologySupport;

struct TopologyCommandRenderOptions {
    bool jsonOutput{false};
    bool verbose{false};
    bool scopeToCwd{false};
    std::filesystem::path cwd;
    std::string requestedSnapshotId;
    std::string clusterId;
};

Result<void>
renderTopologySnapshots(std::ostream& out,
                        const std::optional<yams::topology::TopologyArtifactBatch>& snapshot,
                        const TopologyCommandRenderOptions& options);

Result<void> renderTopologyClusters(std::ostream& out,
                                    const yams::topology::TopologyArtifactBatch& snapshot,
                                    const GraphTopologySupport& support,
                                    const TopologyCommandRenderOptions& options);

Result<void> renderTopologyClusterDetail(std::ostream& out,
                                         const yams::topology::TopologyArtifactBatch& snapshot,
                                         const GraphTopologySupport& support,
                                         const TopologyCommandRenderOptions& options);

} // namespace yams::cli
