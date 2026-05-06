#pragma once

#include <yams/topology/topology_artifacts.h>

#include <cstddef>
#include <vector>

namespace yams::topology {

void applySGCSmoothing(std::vector<TopologyDocumentInput>& documents,
                       const TopologyBuildConfig& config, std::size_t hops);

} // namespace yams::topology
