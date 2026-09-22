#pragma once

#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/topology/topology_engine.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <string_view>
#include <unordered_map>

namespace yams::topology {

class MetadataKgTopologyArtifactStore final : public ITopologyArtifactStore {
public:
    /// Snapshot nodes kept in the knowledge graph; older compressed snapshots are removed.
    static constexpr std::size_t kRetainedSnapshots = 3;

    MetadataKgTopologyArtifactStore(
        std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
        std::shared_ptr<metadata::KnowledgeGraphStore> kgStore = nullptr);

    Result<void> storeBatch(const TopologyArtifactBatch& batch) override;

    Result<std::optional<TopologyArtifactBatch>>
    loadLatest(std::string_view snapshotId = {}) const override;

    Result<std::shared_ptr<const TopologyArtifactBatch>>
    loadLatestShared(std::string_view snapshotId = {}) const override;

    Result<std::vector<DocumentClusterMembership>>
    loadMemberships(std::span<const std::string> documentHashes) const override;

private:
    /// The latest snapshot plus its membership index, published as one immutable unit so
    /// readers never observe a batch/index mismatch.
    struct ResidentSnapshot {
        TopologyArtifactBatch batch;
        std::unordered_map<std::string_view, std::size_t> membershipIndex;
    };

    static std::shared_ptr<const ResidentSnapshot> makeResident(TopologyArtifactBatch batch);
    static std::shared_ptr<const TopologyArtifactBatch>
    batchView(const std::shared_ptr<const ResidentSnapshot>& resident);

    std::shared_ptr<const ResidentSnapshot> resident() const;
    Result<std::shared_ptr<const ResidentSnapshot>> loadResidentLatest() const;
    Result<std::optional<TopologyArtifactBatch>>
    loadSnapshotById(std::string_view snapshotId) const;

    std::shared_ptr<metadata::IMetadataRepository> metadataRepo_;
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore_;

    mutable std::mutex residentMutex_;
    mutable std::shared_ptr<const ResidentSnapshot> resident_;
    // Bumped by every successful storeBatch; a reader only installs a snapshot it loaded from
    // storage when no write landed in between.
    mutable std::uint64_t residentGeneration_ = 0;
    std::mutex writeMutex_;
};

} // namespace yams::topology
