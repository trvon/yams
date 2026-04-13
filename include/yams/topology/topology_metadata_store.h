#pragma once

#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/topology/topology_engine.h>

#include <memory>

namespace yams::topology {

class MetadataKgTopologyArtifactStore final : public ITopologyArtifactStore {
public:
    MetadataKgTopologyArtifactStore(
        std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
        std::shared_ptr<metadata::KnowledgeGraphStore> kgStore = nullptr);

    Result<void> storeBatch(const TopologyArtifactBatch& batch) override;

    Result<std::optional<TopologyArtifactBatch>>
    loadLatest(std::string_view snapshotId = {}) const override;

    Result<std::vector<DocumentClusterMembership>>
    loadMemberships(std::span<const std::string> documentHashes) const override;

private:
    std::shared_ptr<metadata::IMetadataRepository> metadataRepo_;
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore_;
    mutable std::optional<TopologyArtifactBatch> cachedLatest_;
};

} // namespace yams::topology
