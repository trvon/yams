#include <yams/topology/topology_metadata_store.h>

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

const char* inputKindToString(TopologyInputKind kind) {
    switch (kind) {
        case TopologyInputKind::SemanticNeighborGraph:
            return "semantic_neighbor_graph";
        case TopologyInputKind::EmbeddingNeighborhood:
            return "embedding_neighborhood";
        case TopologyInputKind::Hybrid:
            return "hybrid";
    }
    return "hybrid";
}

TopologyInputKind inputKindFromString(std::string_view value) {
    if (value == "semantic_neighbor_graph") {
        return TopologyInputKind::SemanticNeighborGraph;
    }
    if (value == "embedding_neighborhood") {
        return TopologyInputKind::EmbeddingNeighborhood;
    }
    return TopologyInputKind::Hybrid;
}

json membershipToJson(const DocumentClusterMembership& membership) {
    json j = json::object();
    j["document_hash"] = membership.documentHash;
    j["cluster_id"] = membership.clusterId;
    if (membership.parentClusterId.has_value()) {
        j["parent_cluster_id"] = *membership.parentClusterId;
    } else {
        j["parent_cluster_id"] = nullptr;
    }
    j["cluster_level"] = membership.clusterLevel;
    j["persistence_score"] = membership.persistenceScore;
    j["cohesion_score"] = membership.cohesionScore;
    j["bridge_score"] = membership.bridgeScore;
    j["role"] = roleToString(membership.role);
    j["overlap_cluster_ids"] = membership.overlapClusterIds;
    return j;
}

DocumentClusterMembership membershipFromJson(const json& j) {
    DocumentClusterMembership membership;
    membership.documentHash = j.value("document_hash", "");
    membership.clusterId = j.value("cluster_id", "");
    if (j.contains("parent_cluster_id") && !j["parent_cluster_id"].is_null()) {
        membership.parentClusterId = j["parent_cluster_id"].get<std::string>();
    }
    membership.clusterLevel = j.value("cluster_level", std::size_t{0});
    membership.persistenceScore = j.value("persistence_score", 0.0);
    membership.cohesionScore = j.value("cohesion_score", 0.0);
    membership.bridgeScore = j.value("bridge_score", 0.0);
    membership.role = roleFromString(j.value("role", std::string{"core"}));
    if (j.contains("overlap_cluster_ids") && j["overlap_cluster_ids"].is_array()) {
        membership.overlapClusterIds = j["overlap_cluster_ids"].get<std::vector<std::string>>();
    }
    return membership;
}

json representativeToJson(const ClusterRepresentative& representative) {
    return json{{"cluster_id", representative.clusterId},
                {"document_hash", representative.documentHash},
                {"file_path", representative.filePath},
                {"representative_score", representative.representativeScore}};
}

ClusterRepresentative representativeFromJson(const json& j) {
    ClusterRepresentative representative;
    representative.clusterId = j.value("cluster_id", "");
    representative.documentHash = j.value("document_hash", "");
    representative.filePath = j.value("file_path", "");
    representative.representativeScore = j.value("representative_score", 0.0);
    return representative;
}

json clusterToJson(const ClusterArtifact& cluster) {
    json j = json::object();
    j["cluster_id"] = cluster.clusterId;
    if (cluster.parentClusterId.has_value()) {
        j["parent_cluster_id"] = *cluster.parentClusterId;
    } else {
        j["parent_cluster_id"] = nullptr;
    }
    j["level"] = cluster.level;
    j["member_count"] = cluster.memberCount;
    j["persistence_score"] = cluster.persistenceScore;
    j["cohesion_score"] = cluster.cohesionScore;
    j["bridge_mass"] = cluster.bridgeMass;
    j["member_document_hashes"] = cluster.memberDocumentHashes;
    j["overlap_cluster_ids"] = cluster.overlapClusterIds;
    if (cluster.medoid.has_value()) {
        j["medoid"] = representativeToJson(*cluster.medoid);
    }
    return j;
}

ClusterArtifact clusterFromJson(const json& j) {
    ClusterArtifact cluster;
    cluster.clusterId = j.value("cluster_id", "");
    if (j.contains("parent_cluster_id") && !j["parent_cluster_id"].is_null()) {
        cluster.parentClusterId = j["parent_cluster_id"].get<std::string>();
    }
    cluster.level = j.value("level", std::size_t{0});
    cluster.memberCount = j.value("member_count", std::size_t{0});
    cluster.persistenceScore = j.value("persistence_score", 0.0);
    cluster.cohesionScore = j.value("cohesion_score", 0.0);
    cluster.bridgeMass = j.value("bridge_mass", 0.0);
    if (j.contains("medoid") && j["medoid"].is_object()) {
        cluster.medoid = representativeFromJson(j["medoid"]);
    }
    if (j.contains("member_document_hashes") && j["member_document_hashes"].is_array()) {
        cluster.memberDocumentHashes = j["member_document_hashes"].get<std::vector<std::string>>();
    }
    if (j.contains("overlap_cluster_ids") && j["overlap_cluster_ids"].is_array()) {
        cluster.overlapClusterIds = j["overlap_cluster_ids"].get<std::vector<std::string>>();
    }
    return cluster;
}

json batchToJson(const TopologyArtifactBatch& batch) {
    json j{{"snapshot_id", batch.snapshotId},
           {"algorithm", batch.algorithm},
           {"input_kind", inputKindToString(batch.inputKind)},
           {"generated_at_unix_seconds", batch.generatedAtUnixSeconds}};
    j["clusters"] = json::array();
    for (const auto& cluster : batch.clusters) {
        j["clusters"].push_back(clusterToJson(cluster));
    }
    j["memberships"] = json::array();
    for (const auto& membership : batch.memberships) {
        j["memberships"].push_back(membershipToJson(membership));
    }
    return j;
}

Result<TopologyArtifactBatch> batchFromJson(const json& j) {
    if (!j.is_object()) {
        return Error{ErrorCode::InvalidData, "topology batch JSON must be an object"};
    }
    TopologyArtifactBatch batch;
    batch.snapshotId = j.value("snapshot_id", "");
    batch.algorithm = j.value("algorithm", "");
    batch.inputKind = inputKindFromString(j.value("input_kind", std::string{"hybrid"}));
    batch.generatedAtUnixSeconds = j.value("generated_at_unix_seconds", uint64_t{0});
    if (j.contains("clusters") && j["clusters"].is_array()) {
        for (const auto& clusterJson : j["clusters"]) {
            batch.clusters.push_back(clusterFromJson(clusterJson));
        }
    }
    if (j.contains("memberships") && j["memberships"].is_array()) {
        for (const auto& membershipJson : j["memberships"]) {
            batch.memberships.push_back(membershipFromJson(membershipJson));
        }
    }
    return batch;
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
        snapshotNode.properties = batchToJson(batch).dump();
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

    auto parsed = json::parse(*snapshotNodeResult.value()->properties, nullptr, false);
    if (parsed.is_discarded()) {
        return Error{ErrorCode::SerializationError, "failed to parse topology snapshot JSON"};
    }
    auto batchResult = batchFromJson(parsed);
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
