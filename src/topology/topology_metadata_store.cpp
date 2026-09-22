#include <yams/topology/topology_codec.h>
#include <yams/topology/topology_metadata_store.h>

#include <nlohmann/json.hpp>

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
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

    std::lock_guard writeLock(writeMutex_);

    auto previousResult = loadResidentLatest();
    if (!previousResult) {
        return previousResult.error();
    }
    const std::shared_ptr<const ResidentSnapshot> previous = std::move(previousResult.value());

    std::vector<std::string> requestedHashes;
    requestedHashes.reserve(batch.memberships.size());
    for (const auto& membership : batch.memberships) {
        requestedHashes.push_back(membership.documentHash);
    }

    auto docsResult = metadataRepo_->batchGetDocumentsByHash(requestedHashes);
    if (!docsResult) {
        return docsResult.error();
    }
    const auto& docMap = docsResult.value();
    for (const auto& membership : batch.memberships) {
        if (!docMap.contains(membership.documentHash)) {
            return Error{ErrorCode::NotFound, "topology membership document not found for hash=" +
                                                  membership.documentHash};
        }
    }

    std::vector<std::tuple<int64_t, std::string, metadata::MetadataValue>> metadataEntries;
    metadataEntries.reserve(batch.memberships.size() * 2);
    for (const auto& membership : batch.memberships) {
        const auto it = docMap.find(membership.documentHash);
        const auto documentId = it->second.id;
        metadataEntries.emplace_back(documentId, std::string(kSnapshotIdKey),
                                     metadata::MetadataValue(batch.snapshotId));
        metadataEntries.emplace_back(documentId, std::string(kClusterIdKey),
                                     metadata::MetadataValue(membership.clusterId));
    }

    if (!metadataEntries.empty()) {
        auto setResult = metadataRepo_->setMetadataBatch(metadataEntries);
        if (!setResult) {
            return setResult.error();
        }
    }

    if (previous) {
        std::unordered_set<std::string> currentDocumentHashes(requestedHashes.begin(),
                                                              requestedHashes.end());
        std::vector<std::string> removedHashes;
        for (const auto& previousMembership : previous->batch.memberships) {
            if (!currentDocumentHashes.contains(previousMembership.documentHash)) {
                removedHashes.push_back(previousMembership.documentHash);
            }
        }

        if (!removedHashes.empty()) {
            auto removedDocsResult = metadataRepo_->batchGetDocumentsByHash(removedHashes);
            if (removedDocsResult) {
                for (const auto& [hash, docInfo] : removedDocsResult.value()) {
                    for (const auto key : topologyMetadataKeys()) {
                        auto removeResult =
                            metadataRepo_->removeMetadata(docInfo.id, std::string(key));
                        if (!removeResult) {
                            return removeResult.error();
                        }
                    }
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

    auto nextResident = makeResident(batch);
    {
        std::lock_guard lock(residentMutex_);
        resident_ = std::move(nextResident);
        ++residentGeneration_;
    }
    return {};
}

std::shared_ptr<const MetadataKgTopologyArtifactStore::ResidentSnapshot>
MetadataKgTopologyArtifactStore::makeResident(TopologyArtifactBatch batch) {
    auto resident = std::make_shared<ResidentSnapshot>();
    resident->batch = std::move(batch);
    resident->membershipIndex.reserve(resident->batch.memberships.size());
    for (std::size_t i = 0; i < resident->batch.memberships.size(); ++i) {
        resident->membershipIndex.emplace(resident->batch.memberships[i].documentHash, i);
    }
    return resident;
}

std::shared_ptr<const TopologyArtifactBatch> MetadataKgTopologyArtifactStore::batchView(
    const std::shared_ptr<const ResidentSnapshot>& resident) {
    if (!resident) {
        return {};
    }
    // Aliasing constructor: the batch shares ownership with its resident snapshot.
    return std::shared_ptr<const TopologyArtifactBatch>(resident, &resident->batch);
}

std::shared_ptr<const MetadataKgTopologyArtifactStore::ResidentSnapshot>
MetadataKgTopologyArtifactStore::resident() const {
    std::lock_guard lock(residentMutex_);
    return resident_;
}

Result<std::optional<TopologyArtifactBatch>>
MetadataKgTopologyArtifactStore::loadSnapshotById(std::string_view snapshotId) const {
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
    return std::optional<TopologyArtifactBatch>{std::move(batchResult.value())};
}

Result<std::shared_ptr<const MetadataKgTopologyArtifactStore::ResidentSnapshot>>
MetadataKgTopologyArtifactStore::loadResidentLatest() const {
    std::shared_ptr<const ResidentSnapshot> current;
    std::uint64_t generation = 0;
    {
        std::lock_guard lock(residentMutex_);
        current = resident_;
        generation = residentGeneration_;
    }
    if (!kgStore_) {
        return current;
    }

    auto latestNodeResult = kgStore_->getNodeByKey(kLatestSnapshotNodeKey);
    if (!latestNodeResult) {
        return latestNodeResult.error();
    }
    std::string latestSnapshotId;
    if (latestNodeResult.value().has_value() && latestNodeResult.value()->properties.has_value()) {
        auto parsed = json::parse(*latestNodeResult.value()->properties, nullptr, false);
        if (!parsed.is_discarded() && parsed.is_object()) {
            if (auto it = parsed.find("snapshot_id"); it != parsed.end() && it->is_string()) {
                latestSnapshotId = it->get<std::string>();
            }
        }
    }
    if (latestSnapshotId.empty()) {
        return current;
    }
    if (current && current->batch.snapshotId == latestSnapshotId) {
        return current;
    }

    auto loaded = loadSnapshotById(latestSnapshotId);
    if (!loaded) {
        return loaded.error();
    }
    if (!loaded.value().has_value()) {
        return current;
    }
    auto loadedResident = makeResident(std::move(*loaded.value()));
    std::lock_guard lock(residentMutex_);
    if (residentGeneration_ == generation) {
        resident_ = loadedResident;
        return loadedResident;
    }
    // A write landed while loading; it is newer than what storage returned.
    return resident_;
}

Result<std::shared_ptr<const TopologyArtifactBatch>>
MetadataKgTopologyArtifactStore::loadLatestShared(std::string_view snapshotId) const {
    if (snapshotId.empty()) {
        auto latest = loadResidentLatest();
        if (!latest) {
            return latest.error();
        }
        return batchView(latest.value());
    }

    if (auto current = resident(); current && current->batch.snapshotId == snapshotId) {
        return batchView(current);
    }
    // Historical snapshots are returned without displacing the resident latest snapshot.
    auto loaded = loadSnapshotById(snapshotId);
    if (!loaded) {
        return loaded.error();
    }
    if (!loaded.value().has_value()) {
        return std::shared_ptr<const TopologyArtifactBatch>{};
    }
    return std::make_shared<const TopologyArtifactBatch>(std::move(*loaded.value()));
}

Result<std::optional<TopologyArtifactBatch>>
MetadataKgTopologyArtifactStore::loadLatest(std::string_view snapshotId) const {
    auto shared = loadLatestShared(snapshotId);
    if (!shared) {
        return shared.error();
    }
    if (!shared.value()) {
        return std::optional<TopologyArtifactBatch>{};
    }
    return std::optional<TopologyArtifactBatch>{*shared.value()};
}

Result<std::vector<DocumentClusterMembership>> MetadataKgTopologyArtifactStore::loadMemberships(
    std::span<const std::string> documentHashes) const {
    auto current = resident();
    if (!current) {
        auto latest = loadResidentLatest();
        if (!latest) {
            return latest.error();
        }
        current = std::move(latest.value());
    }
    if (current && !current->batch.memberships.empty()) {
        std::vector<DocumentClusterMembership> memberships;
        memberships.reserve(documentHashes.size());
        for (const auto& hash : documentHashes) {
            if (auto it = current->membershipIndex.find(hash);
                it != current->membershipIndex.end()) {
                memberships.push_back(current->batch.memberships[it->second]);
            }
        }
        return memberships;
    }

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
