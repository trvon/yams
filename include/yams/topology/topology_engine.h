#pragma once

#include <yams/topology/topology_artifacts.h>

#include <memory>
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

    /// Shared, immutable view of a snapshot; null when none exists. Stores that keep a resident
    /// snapshot override this to hand out that instance instead of a copy.
    virtual Result<std::shared_ptr<const TopologyArtifactBatch>>
    loadLatestShared(std::string_view snapshotId = {}) const {
        auto loaded = loadLatest(snapshotId);
        if (!loaded) {
            return loaded.error();
        }
        if (!loaded.value().has_value()) {
            return std::shared_ptr<const TopologyArtifactBatch>{};
        }
        return std::make_shared<const TopologyArtifactBatch>(std::move(*loaded.value()));
    }

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

    virtual Result<TopologyDirtyRegion>
    defineDirtyRegion(const TopologyArtifactBatch& existing,
                      std::span<const TopologyDocumentInput> changedDocuments,
                      const TopologyBuildConfig& config) const = 0;
};

} // namespace yams::topology
