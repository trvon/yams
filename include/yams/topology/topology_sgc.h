#pragma once

#include <yams/topology/topology_artifacts.h>

#include <cstddef>
#include <vector>

namespace yams::topology {

// Lean contract: formal/topology/Yams/Topology/SGC.lean.
// SGC smoothing is allowed to mutate embedding values only. It must preserve
// document count, index-aligned documentHash identity, filePath, metadata,
// neighbors, and each document's embedding dimensionality.
void applySGCSmoothing(std::vector<TopologyDocumentInput>& documents,
                       const TopologyBuildConfig& config, std::size_t hops);

} // namespace yams::topology
