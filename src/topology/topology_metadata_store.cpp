#include <yams/profiling.h>
#include <yams/topology/topology_codec.h>
#include <yams/topology/topology_metadata_store.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>

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
constexpr std::string_view kSnapshotIdKey = "topology.snapshot_id";

// Only these per-document keys are written; migration 41 pruned the structural keys, which now
// live solely in the snapshot node.
const std::array<std::string_view, 2>& topologyMetadataKeys() {
    static const std::array<std::string_view, 2> keys = {kSnapshotIdKey, kClusterIdKey};
    return keys;
}

std::string snapshotNodeKey(std::string_view snapshotId) {
    return std::string(kSnapshotNodePrefix) + std::string(snapshotId);
}

constexpr std::string_view kClusterNodePrefix = "topology:cluster:";
constexpr std::string_view kClusterNodeType = "topology_cluster";
constexpr std::string_view kSnapshotNodeType = "topology_snapshot";
constexpr std::size_t kNodeScanPage = 512;

std::string clusterNodeKey(std::string_view clusterId) {
    return std::string(kClusterNodePrefix) + std::string(clusterId);
}

float unitWeight(double value) {
    return static_cast<float>(std::clamp(value, 0.0, 1.0));
}

/// Replace the cluster graph with the batch's clusters in one KG write batch: cluster nodes,
/// document member_of / medoid_of / overlaps edges, and cluster subcluster_of hierarchy edges.
/// Deleting the previous cluster nodes removes their edges through the foreign-key cascade.
Result<void> materializeClusterGraph(metadata::KnowledgeGraphStore& kgStore,
                                     const TopologyArtifactBatch& batch, std::int64_t nowSecs) {
    YAMS_ZONE_SCOPED_N("topology::store::materializeClusterGraph");
    std::vector<std::int64_t> staleClusterNodes;
    for (std::size_t offset = 0;; offset += kNodeScanPage) {
        auto page = kgStore.findNodesByType(kClusterNodeType, kNodeScanPage, offset);
        if (!page) {
            return page.error();
        }
        for (const auto& node : page.value()) {
            staleClusterNodes.push_back(node.id);
        }
        if (page.value().size() < kNodeScanPage) {
            break;
        }
    }

    std::vector<std::string> docKeys;
    docKeys.reserve(batch.memberships.size());
    for (const auto& membership : batch.memberships) {
        docKeys.push_back("doc:" + membership.documentHash);
    }
    std::unordered_map<std::string, std::int64_t> docNodeIds;
    if (!docKeys.empty()) {
        auto docNodes = kgStore.getNodesByKeys(docKeys);
        if (!docNodes) {
            return docNodes.error();
        }
        for (const auto& node : docNodes.value()) {
            docNodeIds.emplace(node.nodeKey.substr(4), node.id); // strip "doc:"
        }
    }

    auto writeBatch = kgStore.beginWriteBatch();
    if (!writeBatch) {
        return writeBatch.error();
    }
    auto& wb = *writeBatch.value();
    for (const auto nodeId : staleClusterNodes) {
        if (auto removed = wb.deleteNodeById(nodeId); !removed) {
            return removed.error();
        }
    }

    std::vector<metadata::KGNode> clusterNodes;
    clusterNodes.reserve(batch.clusters.size());
    for (const auto& cluster : batch.clusters) {
        metadata::KGNode node;
        node.nodeKey = clusterNodeKey(cluster.clusterId);
        node.label = cluster.clusterId;
        node.type = std::string(kClusterNodeType);
        node.createdTime = nowSecs;
        node.updatedTime = nowSecs;
        node.properties =
            json{{"snapshot_id", batch.snapshotId},     {"level", cluster.level},
                 {"member_count", cluster.memberCount}, {"persistence", cluster.persistenceScore},
                 {"cohesion", cluster.cohesionScore},   {"density", cluster.densityScore}}
                .dump();
        clusterNodes.push_back(std::move(node));
    }
    auto clusterIds = wb.upsertNodes(clusterNodes);
    if (!clusterIds) {
        return clusterIds.error();
    }
    std::unordered_map<std::string_view, std::int64_t> clusterNodeIds;
    for (std::size_t i = 0; i < batch.clusters.size() && i < clusterIds.value().size(); ++i) {
        clusterNodeIds.emplace(batch.clusters[i].clusterId, clusterIds.value()[i]);
    }

    std::vector<metadata::KGEdge> edges;
    edges.reserve(batch.memberships.size() + batch.clusters.size() * 2);
    const auto addEdge = [&](std::int64_t src, std::int64_t dst, std::string_view relation,
                             float weight) {
        edges.push_back(metadata::KGEdge{.srcNodeId = src,
                                         .dstNodeId = dst,
                                         .relation = std::string(relation),
                                         .weight = weight,
                                         .createdTime = nowSecs});
    };
    for (const auto& membership : batch.memberships) {
        const auto doc = docNodeIds.find(membership.documentHash);
        if (doc == docNodeIds.end()) {
            continue;
        }
        if (const auto cluster = clusterNodeIds.find(membership.clusterId);
            cluster != clusterNodeIds.end()) {
            addEdge(doc->second, cluster->second, "member_of",
                    unitWeight(membership.persistenceScore));
        }
        for (const auto& overlapId : membership.overlapClusterIds) {
            if (const auto overlap = clusterNodeIds.find(overlapId);
                overlap != clusterNodeIds.end()) {
                addEdge(doc->second, overlap->second, "overlaps",
                        unitWeight(membership.bridgeScore));
            }
        }
    }
    for (const auto& cluster : batch.clusters) {
        const auto self = clusterNodeIds.find(cluster.clusterId);
        if (self == clusterNodeIds.end()) {
            continue;
        }
        if (cluster.medoid) {
            if (const auto doc = docNodeIds.find(cluster.medoid->documentHash);
                doc != docNodeIds.end()) {
                addEdge(doc->second, self->second, "medoid_of",
                        unitWeight(cluster.medoid->representativeScore));
            }
        }
        if (cluster.parentClusterId) {
            if (const auto parent = clusterNodeIds.find(*cluster.parentClusterId);
                parent != clusterNodeIds.end()) {
                addEdge(self->second, parent->second, "subcluster_of", 1.0F);
            }
        }
    }
    if (!edges.empty()) {
        if (auto added = wb.addEdgesUnique(edges); !added) {
            return added.error();
        }
    }
    return wb.commit();
}

/// Keep the newest kRetainedSnapshots snapshot nodes (by insertion order) plus the one the
/// latest pointer names; older compressed snapshots are removed.
Result<void> pruneSnapshotNodes(metadata::KnowledgeGraphStore& kgStore, std::size_t retain,
                                std::string_view latestSnapshotId) {
    YAMS_ZONE_SCOPED_N("topology::store::pruneSnapshotNodes");
    std::vector<std::pair<std::int64_t, std::string>> snapshots;
    for (std::size_t offset = 0;; offset += kNodeScanPage) {
        auto page = kgStore.findNodesByType(kSnapshotNodeType, kNodeScanPage, offset);
        if (!page) {
            return page.error();
        }
        for (const auto& node : page.value()) {
            snapshots.emplace_back(node.id, node.nodeKey);
        }
        if (page.value().size() < kNodeScanPage) {
            break;
        }
    }
    if (snapshots.size() <= retain) {
        return {};
    }
    std::ranges::sort(snapshots, [](const auto& lhs, const auto& rhs) {
        return lhs.first > rhs.first; // newest first
    });
    const auto latestKey = snapshotNodeKey(latestSnapshotId);
    for (std::size_t i = retain; i < snapshots.size(); ++i) {
        if (snapshots[i].second == latestKey) {
            continue;
        }
        if (auto removed = kgStore.deleteNodeById(snapshots[i].first); !removed) {
            return removed.error();
        }
    }
    return {};
}

} // namespace

MetadataKgTopologyArtifactStore::MetadataKgTopologyArtifactStore(
    std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore)
    : metadataRepo_(std::move(metadataRepo)), kgStore_(std::move(kgStore)) {}

Result<void> MetadataKgTopologyArtifactStore::storeBatch(const TopologyArtifactBatch& batch) {
    YAMS_ZONE_SCOPED_N("topology::store::storeBatch");
    if (!metadataRepo_) {
        return Error{ErrorCode::InvalidState,
                     "topology metadata store requires metadata repository"};
    }
    if (batch.snapshotId.empty()) {
        return Error{ErrorCode::InvalidArgument, "topology batch requires non-empty snapshot id"};
    }

    std::lock_guard writeLock(writeMutex_);

    // The previous snapshot only identifies documents that left the topology. An undecodable
    // one must not block every future rebuild: skip that cleanup and let this batch replace it.
    std::shared_ptr<const ResidentSnapshot> previous;
    if (auto previousResult = loadResidentLatest(); previousResult) {
        previous = std::move(previousResult.value());
    } else if (previousResult.error().code == ErrorCode::InvalidData ||
               previousResult.error().code == ErrorCode::SerializationError) {
        spdlog::warn("[TopologyStore] previous topology snapshot is undecodable ({}); storing "
                     "snapshot {} without clearing keys of dropped documents",
                     previousResult.error().message, batch.snapshotId);
    } else {
        return previousResult.error();
    }

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

    // Write order makes the latest pointer the commit point: the snapshot node is written first
    // (an orphan is harmless if a later step fails), then per-document keys, then the pointer.
    // A failure before the pointer leaves the previous snapshot authoritative.
    const auto nowSecs = std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
    if (kgStore_) {
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
        YAMS_PLOT("topology::snapshot_payload_bytes", static_cast<int64_t>(compRes.value().size()));
        YAMS_PLOT("topology::snapshot_memberships", static_cast<int64_t>(batch.memberships.size()));
        snapshotNode.properties = std::move(compRes.value());
        auto snapshotResult = kgStore_->upsertNode(snapshotNode);
        if (!snapshotResult) {
            return snapshotResult.error();
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
                std::vector<std::pair<int64_t, std::string>> removals;
                removals.reserve(removedDocsResult.value().size() * topologyMetadataKeys().size());
                for (const auto& [hash, docInfo] : removedDocsResult.value()) {
                    for (const auto key : topologyMetadataKeys()) {
                        removals.emplace_back(docInfo.id, std::string(key));
                    }
                }
                auto removeResult = metadataRepo_->removeMetadataBatch(removals);
                if (!removeResult) {
                    return removeResult.error();
                }
            }
        }
    }

    if (kgStore_) {
        // Written before the pointer flips, so a failure leaves the previous snapshot current.
        if (auto graph = materializeClusterGraph(*kgStore_, batch, nowSecs); !graph) {
            return graph.error();
        }
    }

    if (kgStore_) {
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
        // Retention is best effort: an old snapshot left behind is only wasted space.
        if (auto pruned = pruneSnapshotNodes(*kgStore_, kRetainedSnapshots, batch.snapshotId);
            !pruned) {
            spdlog::warn("[topology] failed to prune old snapshot nodes: {}",
                         pruned.error().message);
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
    YAMS_ZONE_SCOPED_N("topology::store::loadResidentLatest");
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

        // Without a resident snapshot only the per-document cluster id survives (migration 41
        // moved the structural fields into the snapshot node), so the remaining fields keep
        // their defaults rather than reading keys that are no longer written.
        DocumentClusterMembership membership;
        membership.documentHash = hash;
        membership.clusterId = clusterIt->second.asString();
        memberships.push_back(std::move(membership));
    }

    return memberships;
}

} // namespace yams::topology
