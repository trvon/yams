#include <yams/topology/topology_offline_analyzer.h>

namespace yams::topology {

TopologyOfflineAnalyzer::TopologyOfflineAnalyzer(std::shared_ptr<TopologyInputExtractor> extractor,
                                                 std::shared_ptr<ITopologyEngine> engine)
    : extractor_(std::move(extractor)), engine_(std::move(engine)) {}

Result<TopologyOfflineAnalysis>
TopologyOfflineAnalyzer::analyze(const TopologyExtractionConfig& extractionConfig,
                                 const TopologyBuildConfig& buildConfig) const {
    if (!extractor_) {
        return Error{ErrorCode::InvalidState, "topology offline analyzer requires extractor"};
    }
    if (!engine_) {
        return Error{ErrorCode::InvalidState, "topology offline analyzer requires engine"};
    }

    TopologyExtractionStats extractionStats;
    auto extracted = extractor_->extract(extractionConfig, &extractionStats);
    if (!extracted) {
        return extracted.error();
    }

    auto artifacts = engine_->buildArtifacts(extracted.value(), buildConfig);
    if (!artifacts) {
        return artifacts.error();
    }

    return TopologyOfflineAnalysis{.artifacts = std::move(artifacts.value()),
                                   .extractionStats = extractionStats};
}

} // namespace yams::topology
