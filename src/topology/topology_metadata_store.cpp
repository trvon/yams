#include <yams/topology/topology_metadata_store.h>
#include <yams/topology/topology_codec.h>

#include <nlohmann/json.hpp>

#include <chrono>
#include <cstdint>
#include <string>
#include <tuple>
#include <utility>

namespace yams::topology {

namespace {

using json = nlohmann::json;

constexpr std::string_view kLatestSnapshotNodeKey = "topology:snapshot:latest";
constexpr std::string_view kSnapshotNodePrefix = "topology:snapshot:";
constexpr std::string_view kClusterIdKey = "topology.cluster_id";
constexpr std::string_view kParentClusterIdKey = "topology.parent_cluster_id";
constexpr std::string_view kClusterLevelKey = "topology.cluster_level";
constexpr std::string_view kPersistenceKey = "topology.persistence_score";
constexpr std::string_view kCohesionKey = "topology.cohesion_score";
constexpr std::string_view kBridgeKey = "topology.bridge_score";
constexpr std::string_view kRoleKey = "topology.role";
constexpr std::string_view kOverlapKey = "topology.overlap_cluster_ids_json";
constexpr std::string_view kSnapshotIdKey = "topology.snapshot_id";

const std::array<std::string_view, 9>& topologyMetadataKeys() {
    static const std::array<std::string_view, 9> keys = {
        kSnapshotIdKey,   kClusterIdKey,   kParentClusterIdKey,
        kClusterLevelKey, kPersistenceKey, kCohesionKey,
        kBridgeKey,       kRoleKey,        kOverlapKey};
    return keys;
}

const char* roleToString(DocumentTopologyRole role) {
    switch (role) {
        case DocumentTopologyRole::Core:
            return "core";
        case DocumentTopologyRole::Bridge:
            return "bridge";
        case DocumentTopologyRole::Medoid:
            return "medoid";
        case DocumentTopologyRole::Outlier:
            return "outlier";
    }
    return "core";
}

DocumentTopologyRole roleFromString(std::string_view value) {
    if (value == "bridge") {
        return DocumentTopologyRole::Bridge;
    }
    if (value == "medoid") {
        return DocumentTopologyRole::Medoid;
    }
    if (value == "outlier") {
        return DocumentTopologyRole::Outlier;
    }
    return DocumentTopologyRole::Core;
}

std::string snapshotNodeKey(std::string_view snapshotId) {
    return std::string(kSnapshotNodePrefix) + std::string(snapshotId);
}

} // namespace

MetadataKgTopologyArtifactStore::MetadataKgTopologyArtifactStore(
    std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore)
    : metadataRepo_(std::move(metadataRepo)), kgStore_(std::move(kgStore)) {}

Result<void> MetadataKgTopologyArtifactStore::storeBatch(const TopologyArtifactBatch& batch) {
    if (!metadataRepo_) {
        return Error{ErrorCode::InvalidState,
                     "topology metadata store requires metadata repository"};
    }
    if (batch.snapshotId.empty()) {
        return Error{ErrorCode::InvalidArgument, "topology batch requires non-empty snapshot id"};
    }

    std::optional<TopologyArtifactBatch> previousBatch;
    if (cachedLatest_.has_value()) {
        previousBatch = cachedLatest_;
    } else {
        auto latestResult = loadLatest();
        if (!latestResult) {
            return latestResult.error();
        }
        previousBatch = std::move(latestResult.value());
    }

    std::vector<std::tuple<int64_t, std::string, metadata::MetadataValue>> metadataEntries;
    metadataEntries.reserve(batch.memberships.size() * 9);
    for (const auto& membership : batch.memberships) {
        auto documentResult = metadataRepo_->getDocumentByHash(membership.documentHash);
        if (!documentResult) {
            return documentResult.error();
        }
        if (!documentResult.value().has_value()) {
            return Error{ErrorCode::NotFound, "topology membership document not found for hash=" +
                                                  membership.documentHash};
        }
        const auto documentId = documentResult.value()->id;
        metadataEntries.emplace_back(documentId, std::string(kSnapshotIdKey),
                                     metadata::MetadataValue(batch.snapshotId));
        metadataEntries.emplace_back(documentId, std::string(kClusterIdKey),
                                     metadata::MetadataValue(membership.clusterId));
        metadataEntries.emplace_back(
            documentId, std::string(kParentClusterIdKey),
            metadata::MetadataValue(membership.parentClusterId.value_or("")));
        metadataEntries.emplace_back(
            documentId, std::string(kClusterLevelKey),
            metadata::MetadataValue(static_cast<int64_t>(membership.clusterLevel)));
        metadataEntries.emplace_back(documentId, std::string(kPersistenceKey),
                                     metadata::MetadataValue(membership.persistenceScore));
        metadataEntries.emplace_back(documentId, std::string(kCohesionKey),
                                     metadata::MetadataValue(membership.cohesionScore));
        metadataEntries.emplace_back(documentId, std::string(kBridgeKey),
                                     metadata::MetadataValue(membership.bridgeScore));
        metadataEntries.emplace_back(documentId, std::string(kRoleKey),
                                     metadata::MetadataValue(roleToString(membership.role)));
        metadataEntries.emplace_back(
            documentId, std::string(kOverlapKey),
            metadata::MetadataValue(json(membership.overlapClusterIds).dump()));
    }

    if (!metadataEntries.empty()) {
        auto setResult = metadataRepo_->setMetadataBatch(metadataEntries);
        if (!setResult) {
            return setResult.error();
        }
    }

    if (previousBatch.has_value()) {
        std::unordered_set<std::string> currentDocumentHashes;
        currentDocumentHashes.reserve(batch.memberships.size());
        for (const auto& membership : batch.memberships) {
            currentDocumentHashes.insert(membership.documentHash);
        }

        for (const auto& previousMembership : previousBatch->memberships) {
            if (currentDocumentHashes.contains(previousMembership.documentHash)) {
                continue;
            }
            auto documentResult = metadataRepo_->getDocumentByHash(previousMembership.documentHash);
            if (!documentResult || !documentResult.value().has_value()) {
                continue;
            }
            for (const auto key : topologyMetadataKeys()) {
                auto removeResult =
                    metadataRepo_->removeMetadata(documentResult.value()->id, std::string(key));
                if (!removeResult) {
                    return removeResult.error();
                }
            }
        }
    }

    if (kgStore_) {
        const auto nowSecs = std::chrono::duration_cast<std::chrono::seconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
        metadata::KGNode snapshotNode;
        snapshotNode.nodeKey = snapshotNodeKey(batch.snapshotId);
        snapshotNode.label = batch.snapshotId;
        snapshotNode.type = std::string{"topology_snapshot"};
        snapshotNode.createdTime = nowSecs;
        snapshotNode.updatedTime = nowSecs;
        auto compRes = serializeTopologyBatchCompressed(batch);
        if (!compRes) {
            return compRes.error();
        }
        snapshotNode.properties = std::move(compRes.value());
        auto snapshotResult = kgStore_->upsertNode(snapshotNode);
        if (!snapshotResult) {
            return snapshotResult.error();
        }

        metadata::KGNode latestNode;
        latestNode.nodeKey = std::string{kLatestSnapshotNodeKey};
        latestNode.label = std::string{"latest_topology_snapshot"};
        latestNode.type = std::string{"topology_snapshot_pointer"};
        latestNode.createdTime = nowSecs;
        latestNode.updatedTime = nowSecs;
        latestNode.properties = json{{"snapshot_id", batch.snapshotId},
                                     {"generated_at_unix_seconds", batch.generatedAtUnixSeconds}}
                                    .dump();
        auto latestResult = kgStore_->upsertNode(latestNode);
        if (!latestResult) {
            return latestResult.error();
        }
    }

    cachedLatest_ = batch;
    return {};
}

Result<std::optional<TopologyArtifactBatch>>
MetadataKgTopologyArtifactStore::loadLatest(std::string_view snapshotId) const {
    if (snapshotId.empty()) {
        if (kgStore_) {
            auto latestNodeResult = kgStore_->getNodeByKey(kLatestSnapshotNodeKey);
            if (!latestNodeResult) {
                return latestNodeResult.error();
            }
            if (latestNodeResult.value().has_value() &&
                latestNodeResult.value()->properties.has_value()) {
                auto parsed = json::parse(*latestNodeResult.value()->properties, nullptr, false);
                if (!parsed.is_discarded()) {
                    const auto latestSnapshotId = parsed.value("snapshot_id", std::string{});
                    if (!latestSnapshotId.empty()) {
                        return loadLatest(latestSnapshotId);
                    }
                }
            }
        }
        if (cachedLatest_.has_value()) {
            return cachedLatest_;
        }
        return std::optional<TopologyArtifactBatch>{};
    }

    if (cachedLatest_.has_value() && cachedLatest_->snapshotId == snapshotId) {
        return cachedLatest_;
    }
    if (!kgStore_) {
        return std::optional<TopologyArtifactBatch>{};
    }

    auto snapshotNodeResult = kgStore_->getNodeByKey(snapshotNodeKey(snapshotId));
    if (!snapshotNodeResult) {
        return snapshotNodeResult.error();
    }
    if (!snapshotNodeResult.value().has_value() ||
        !snapshotNodeResult.value()->properties.has_value()) {
        return std::optional<TopologyArtifactBatch>{};
    }

    auto batchResult = deserializeTopologyBatchCompressed(*snapshotNodeResult.value()->properties);
    if (!batchResult) {
        return batchResult.error();
    }
    cachedLatest_ = batchResult.value();
    return cachedLatest_;
}

Result<std::vector<DocumentClusterMembership>> MetadataKgTopologyArtifactStore::loadMemberships(
    std::span<const std::string> documentHashes) const {
    if (!metadataRepo_) {
        return Error{ErrorCode::InvalidState,
                     "topology metadata store requires metadata repository"};
    }

    std::vector<DocumentClusterMembership> memberships;
    memberships.reserve(documentHashes.size());
    for (const auto& hash : documentHashes) {
        auto documentResult = metadataRepo_->getDocumentByHash(hash);
        if (!documentResult) {
            return documentResult.error();
        }
        if (!documentResult.value().has_value()) {
            continue;
        }

        auto allMetadataResult = metadataRepo_->getAllMetadata(documentResult.value()->id);
        if (!allMetadataResult) {
            return allMetadataResult.error();
        }
        const auto& allMetadata = allMetadataResult.value();
        const auto clusterIt = allMetadata.find(std::string(kClusterIdKey));
        if (clusterIt == allMetadata.end() || clusterIt->second.asString().empty()) {
            continue;
        }

        DocumentClusterMembership membership;
        membership.documentHash = hash;
        membership.clusterId = clusterIt->second.asString();
        if (auto parentIt = allMetadata.find(std::string(kParentClusterIdKey));
            parentIt != allMetadata.end() && !parentIt->second.asString().empty()) {
            membership.parentClusterId = parentIt->second.asString();
        }
        if (auto levelIt = allMetadata.find(std::string(kClusterLevelKey));
            levelIt != allMetadata.end()) {
            membership.clusterLevel = static_cast<std::size_t>(levelIt->second.asInteger());
        }
        if (auto persistenceIt = allMetadata.find(std::string(kPersistenceKey));
            persistenceIt != allMetadata.end()) {
            membership.persistenceScore = persistenceIt->second.asReal();
        }
        if (auto cohesionIt = allMetadata.find(std::string(kCohesionKey));
            cohesionIt != allMetadata.end()) {
            membership.cohesionScore = cohesionIt->second.asReal();
        }
        if (auto bridgeIt = allMetadata.find(std::string(kBridgeKey));
            bridgeIt != allMetadata.end()) {
            membership.bridgeScore = bridgeIt->second.asReal();
        }
        if (auto roleIt = allMetadata.find(std::string(kRoleKey)); roleIt != allMetadata.end()) {
            membership.role = roleFromString(roleIt->second.asString());
        }
        if (auto overlapIt = allMetadata.find(std::string(kOverlapKey));
            overlapIt != allMetadata.end() && !overlapIt->second.asString().empty()) {
            auto parsed = json::parse(overlapIt->second.asString(), nullptr, false);
            if (!parsed.is_discarded() && parsed.is_array()) {
                membership.overlapClusterIds = parsed.get<std::vector<std::string>>();
            }
        }
        memberships.push_back(std::move(membership));
    }

    return memberships;
}

} // namespace yams::topology
