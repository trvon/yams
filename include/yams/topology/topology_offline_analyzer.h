#pragma once

#include <yams/topology/topology_engine.h>
#include <yams/topology/topology_input_extractor.h>

#include <memory>

namespace yams::topology {

struct TopologyOfflineAnalysis {
    TopologyArtifactBatch artifacts;
    TopologyExtractionStats extractionStats;
};

class TopologyOfflineAnalyzer {
public:
    TopologyOfflineAnalyzer(std::shared_ptr<TopologyInputExtractor> extractor,
                            std::shared_ptr<ITopologyEngine> engine);

    Result<TopologyOfflineAnalysis> analyze(const TopologyExtractionConfig& extractionConfig,
                                            const TopologyBuildConfig& buildConfig) const;

private:
    std::shared_ptr<TopologyInputExtractor> extractor_;
    std::shared_ptr<ITopologyEngine> engine_;
};

} // namespace yams::topology
