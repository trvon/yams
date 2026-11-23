#pragma once

#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/topology/topology_engine.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

namespace yams::topology {

class MetadataKgTopologyArtifactStore final : public ITopologyArtifactStore {
public:
    /// Snapshot nodes kept in the knowledge graph; older compressed snapshots are removed, each
    /// together with its road sidecar.
    static constexpr std::size_t kRetainedSnapshots = 3;
    /// Road sidecar node: `topology:roads:<snapshotId>`, written only when the batch has roads
    /// and always before the latest pointer. Optional on load; see `attachRoadSidecar`.
    static constexpr std::string_view kRoadSidecarNodePrefix = "topology:roads:";
    static constexpr std::string_view kRoadSidecarNodeType = "topology_road_snapshot";
    /// Route-fit sidecar node: `topology:routefit:<snapshotId>`, written only when a cluster has
    /// a fitted route vector. Optional on load; see `attachRouteFitSidecar`.
    static constexpr std::string_view kRouteFitSidecarNodePrefix = "topology:routefit:";
    static constexpr std::string_view kRouteFitSidecarNodeType = "topology_route_fit_snapshot";

    /// Payload storage. The snapshot node and both sidecar nodes hold a small JSON manifest
    /// (`format = kChunkedPayloadFormat`, chunk count, chunk size, total bytes, CRC-32); the
    /// compressed payload itself is split across chunk nodes keyed
    /// `topology:chunk:<ownerNodeKey>:<index>` with the owner node key as label. A chunked node is
    /// written, replaced and deleted with its chunks in one KG transaction. A node whose
    /// properties are not a manifest is read as a legacy single-value payload.
    static constexpr std::string_view kPayloadChunkNodePrefix = "topology:chunk:";
    static constexpr std::string_view kPayloadChunkNodeType = "topology_payload_chunk";
    static constexpr std::string_view kChunkedPayloadFormat = "topology_chunked_v1";
    static constexpr std::size_t kDefaultPayloadChunkBytes = std::size_t{8} << 20U;
    /// Hard ceiling for any one value this store writes, far below SQLite's 1e9-byte default
    /// limit; a write that would exceed it fails before anything is stored.
    static constexpr std::size_t kMaxStoredValueBytes = std::size_t{64} << 20U;

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

    /// The payload stored under `nodeKey`: reassembled and checksum-verified when the node holds
    /// a chunk manifest, the properties verbatim otherwise (legacy single-value payloads).
    /// nullopt when the node or its properties are absent; InvalidData when chunks are missing
    /// or do not match the manifest.
    static Result<std::optional<std::string>>
    readStoredPayload(metadata::KnowledgeGraphStore& kgStore, std::string_view nodeKey);

    /// Test hook: chunk size for later writes, clamped to [1, kMaxStoredValueBytes].
    void setPayloadChunkBytesForTesting(std::size_t bytes);

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
    /// Fill `batch.roads` from the sidecar node of `batch.snapshotId`. A missing sidecar leaves
    /// roads empty with no error; an undecodable or invalid one leaves roads empty and sets
    /// `batch.roadUnavailableReason`. Only storage errors propagate.
    Result<void> attachRoadSidecar(TopologyArtifactBatch& batch) const;
    /// Fill the clusters' `fittedRouteVector` from the sidecar node of `batch.snapshotId`. A
    /// missing sidecar leaves them empty with no error; an undecodable or mismatched one leaves
    /// them empty and sets `batch.routeFitUnavailableReason`. Only storage errors propagate.
    Result<void> attachRouteFitSidecar(TopologyArtifactBatch& batch) const;

    std::shared_ptr<metadata::IMetadataRepository> metadataRepo_;
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore_;
    std::size_t payloadChunkBytes_ = kDefaultPayloadChunkBytes;

    mutable std::mutex residentMutex_;
    mutable std::shared_ptr<const ResidentSnapshot> resident_;
    // Bumped by every successful storeBatch; a reader only installs a snapshot it loaded from
    // storage when no write landed in between.
    mutable std::uint64_t residentGeneration_ = 0;
    std::mutex writeMutex_;
};

} // namespace yams::topology
