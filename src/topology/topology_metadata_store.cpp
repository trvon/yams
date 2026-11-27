#include <yams/common/crc32.h>
#include <yams/profiling.h>
#include <yams/topology/topology_codec.h>
#include <yams/topology/topology_metadata_store.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <span>

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

std::string roadSidecarNodeKey(std::string_view snapshotId) {
    return std::string(MetadataKgTopologyArtifactStore::kRoadSidecarNodePrefix) +
           std::string(snapshotId);
}

std::string routeFitSidecarNodeKey(std::string_view snapshotId) {
    return std::string(MetadataKgTopologyArtifactStore::kRouteFitSidecarNodePrefix) +
           std::string(snapshotId);
}

using Store = MetadataKgTopologyArtifactStore;

// A manifest is a few hundred bytes; anything larger is a legacy payload and is never parsed
// twice to find that out.
constexpr std::size_t kMaxManifestBytes = 64 * 1024;

std::string chunkKeyPrefix(std::string_view ownerKey) {
    return std::string(Store::kPayloadChunkNodePrefix) + std::string(ownerKey) + ":";
}

std::string chunkNodeKey(std::string_view ownerKey, std::size_t index) {
    return chunkKeyPrefix(ownerKey) + std::to_string(index);
}

std::uint32_t payloadCrc32(std::string_view payload) {
    return common::crc32(std::as_bytes(std::span<const char>(payload.data(), payload.size())));
}

Result<void> checkStoredValueSize(std::string_view nodeKey, std::size_t bytes) {
    if (bytes > Store::kMaxStoredValueBytes) {
        return Error{ErrorCode::InvalidArgument,
                     "topology store refuses to write " + std::to_string(bytes) +
                         " bytes to node '" + std::string(nodeKey) + "': exceeds the " +
                         std::to_string(Store::kMaxStoredValueBytes) + "-byte value ceiling"};
    }
    return {};
}

struct ChunkedNode {
    std::string nodeKey;
    std::string label;
    std::string type;
    /// Inspection fields copied into the manifest (e.g. counts), readable without the payload.
    json inspection = json::object();
};

/// Write `payload` as the chunk nodes of `owner` plus its manifest node, in one transaction.
/// The owner's previous chunks are removed first, so replacing a node (or storing a snapshot id
/// again) never leaves stale chunks, and a failure rolls everything back to the prior state.
Result<void> storeChunkedNode(metadata::KnowledgeGraphStore& kgStore, const ChunkedNode& owner,
                              std::string_view payload, std::size_t chunkBytes,
                              std::int64_t nowSecs) {
    YAMS_ZONE_SCOPED_N("topology::store::storeChunkedNode");
    if (chunkBytes == 0) {
        return Error{ErrorCode::InvalidArgument, "topology payload chunk size must be positive"};
    }
    if (auto guarded = checkStoredValueSize(chunkNodeKey(owner.nodeKey, 0),
                                            std::min(chunkBytes, payload.size()));
        !guarded) {
        return guarded.error();
    }
    const std::size_t chunkCount = (payload.size() + chunkBytes - 1) / chunkBytes;
    json manifest = owner.inspection.is_object() ? owner.inspection : json::object();
    manifest["format"] = std::string(Store::kChunkedPayloadFormat);
    manifest["chunk_count"] = chunkCount;
    manifest["chunk_bytes"] = chunkBytes;
    manifest["total_bytes"] = payload.size();
    manifest["crc32"] = payloadCrc32(payload);
    std::string manifestText = manifest.dump();
    if (auto guarded = checkStoredValueSize(owner.nodeKey, manifestText.size()); !guarded) {
        return guarded.error();
    }

    auto writeBatch = kgStore.beginWriteBatch();
    if (!writeBatch) {
        return writeBatch.error();
    }
    auto& wb = *writeBatch.value();
    if (auto removed = wb.deleteNodesByKeyPrefix(chunkKeyPrefix(owner.nodeKey), owner.nodeKey);
        !removed) {
        return removed.error();
    }
    for (std::size_t i = 0; i < chunkCount; ++i) {
        metadata::KGNode chunk;
        chunk.nodeKey = chunkNodeKey(owner.nodeKey, i);
        chunk.label = owner.nodeKey;
        chunk.type = std::string(Store::kPayloadChunkNodeType);
        chunk.createdTime = nowSecs;
        chunk.updatedTime = nowSecs;
        chunk.properties = std::string(payload.substr(i * chunkBytes, chunkBytes));
        if (auto stored = wb.upsertNode(chunk); !stored) {
            return stored.error();
        }
    }
    metadata::KGNode node;
    node.nodeKey = owner.nodeKey;
    node.label = owner.label;
    node.type = owner.type;
    node.createdTime = nowSecs;
    node.updatedTime = nowSecs;
    node.properties = std::move(manifestText);
    if (auto stored = wb.upsertNode(node); !stored) {
        return stored.error();
    }
    return wb.commit();
}

/// Delete `nodeKey` and its chunk nodes, if any, in one transaction. Legacy single-value nodes
/// simply have no chunks.
Result<void> deleteChunkedNode(metadata::KnowledgeGraphStore& kgStore, const std::string& nodeKey) {
    auto writeBatch = kgStore.beginWriteBatch();
    if (!writeBatch) {
        return writeBatch.error();
    }
    auto& wb = *writeBatch.value();
    if (auto removed = wb.deleteNodesByKeyPrefix(chunkKeyPrefix(nodeKey), nodeKey); !removed) {
        return removed.error();
    }
    if (auto removed = wb.deleteNodeByKey(nodeKey); !removed) {
        return removed.error();
    }
    return wb.commit();
}

/// Delete the road sidecar of `snapshotId` if one exists. Used when a snapshot is pruned and
/// when a snapshot id is re-stored without roads.
Result<void> deleteRoadSidecar(metadata::KnowledgeGraphStore& kgStore,
                               std::string_view snapshotId) {
    return deleteChunkedNode(kgStore, roadSidecarNodeKey(snapshotId));
}

Result<std::size_t> manifestUnsigned(const json& manifest, const char* key) {
    auto it = manifest.find(key);
    if (it == manifest.end() || !it->is_number_unsigned()) {
        return Error{ErrorCode::InvalidData,
                     std::string("topology chunk manifest field '") + key + "' missing or invalid"};
    }
    return it->get<std::size_t>();
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
/// latest pointer names; older snapshots are removed together with their payload chunks and
/// their road and route-fit sidecars (and those sidecars' chunks).
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
        // The snapshot goes with its chunks in one transaction.
        if (auto removed = deleteChunkedNode(kgStore, snapshots[i].second); !removed) {
            return removed.error();
        }
        // The sidecars go with their snapshot; an orphaned one would only be wasted space, but a
        // snapshot id reused later would otherwise find stale roads.
        const std::string_view key = snapshots[i].second;
        if (key.starts_with(kSnapshotNodePrefix)) {
            if (auto removed = deleteRoadSidecar(kgStore, key.substr(kSnapshotNodePrefix.size()));
                !removed) {
                return removed.error();
            }
            if (auto removed = deleteChunkedNode(
                    kgStore, routeFitSidecarNodeKey(key.substr(kSnapshotNodePrefix.size())));
                !removed) {
                return removed.error();
            }
        }
    }
    return {};
}

} // namespace

MetadataKgTopologyArtifactStore::MetadataKgTopologyArtifactStore(
    std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore)
    : metadataRepo_(std::move(metadataRepo)), kgStore_(std::move(kgStore)) {}

void MetadataKgTopologyArtifactStore::setPayloadChunkBytesForTesting(std::size_t bytes) {
    std::lock_guard writeLock(writeMutex_);
    payloadChunkBytes_ = std::clamp<std::size_t>(bytes, 1, kMaxStoredValueBytes);
}

Result<std::optional<std::string>>
MetadataKgTopologyArtifactStore::readStoredPayload(metadata::KnowledgeGraphStore& kgStore,
                                                   std::string_view nodeKey) {
    YAMS_ZONE_SCOPED_N("topology::store::readStoredPayload");
    auto node = kgStore.getNodeByKey(nodeKey);
    if (!node) {
        return node.error();
    }
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked in order
    if (!node.value().has_value() || !node.value()->properties.has_value()) {
        return std::optional<std::string>{};
    }
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
    std::string& properties = *node.value()->properties;
    json manifest;
    if (properties.size() <= kMaxManifestBytes &&
        properties.find(kChunkedPayloadFormat) != std::string::npos) {
        manifest = json::parse(properties, nullptr, false);
    }
    const auto format = manifest.is_object() ? manifest.find("format") : manifest.end();
    const bool chunked = manifest.is_object() && format != manifest.end() && format->is_string() &&
                         format->get<std::string>() == kChunkedPayloadFormat;
    if (!chunked) {
        return std::optional<std::string>{std::move(properties)}; // legacy single value
    }

    auto chunkCount = manifestUnsigned(manifest, "chunk_count");
    if (!chunkCount) {
        return chunkCount.error();
    }
    auto totalBytes = manifestUnsigned(manifest, "total_bytes");
    if (!totalBytes) {
        return totalBytes.error();
    }
    auto expectedCrc = manifestUnsigned(manifest, "crc32");
    if (!expectedCrc) {
        return expectedCrc.error();
    }
    // Untrusted sizes: every chunk holds at least one byte and at most the value ceiling.
    if (chunkCount.value() > totalBytes.value() ||
        totalBytes.value() / kMaxStoredValueBytes > chunkCount.value()) {
        return Error{ErrorCode::InvalidData, "topology chunk manifest for '" +
                                                 std::string(nodeKey) + "' has inconsistent sizes"};
    }
    std::string payload;
    payload.reserve(totalBytes.value());
    for (std::size_t i = 0; i < chunkCount.value(); ++i) {
        auto chunk = kgStore.getNodeByKey(chunkNodeKey(nodeKey, i));
        if (!chunk) {
            return chunk.error();
        }
        // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked in order
        if (!chunk.value().has_value() || !chunk.value()->properties.has_value()) {
            return Error{ErrorCode::InvalidData, "topology payload '" + std::string(nodeKey) +
                                                     "' is missing chunk " + std::to_string(i) +
                                                     " of " + std::to_string(chunkCount.value())};
        }
        // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
        payload += *chunk.value()->properties;
        if (payload.size() > totalBytes.value()) {
            break;
        }
    }
    if (payload.size() != totalBytes.value() || payloadCrc32(payload) != expectedCrc.value()) {
        return Error{ErrorCode::InvalidData, "topology payload '" + std::string(nodeKey) +
                                                 "' does not match its chunk manifest"};
    }
    return std::optional<std::string>{std::move(payload)};
}

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

    // Write order makes the latest pointer the commit point: the snapshot node and its payload
    // chunks are written first, in one transaction (an orphan is harmless if a later step fails
    // and is pruned later), then the sidecars, per-document keys, and the pointer. A failure
    // before the pointer leaves the previous snapshot authoritative.
    const auto nowSecs = std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
    if (kgStore_) {
        auto compRes = serializeTopologyBatchCompressed(batch);
        if (!compRes) {
            return compRes.error();
        }
        YAMS_PLOT("topology::snapshot_payload_bytes", static_cast<int64_t>(compRes.value().size()));
        YAMS_PLOT("topology::snapshot_memberships", static_cast<int64_t>(batch.memberships.size()));
        ChunkedNode snapshotNode{
            .nodeKey = snapshotNodeKey(batch.snapshotId),
            .label = batch.snapshotId,
            .type = std::string{kSnapshotNodeType},
            .inspection = json{{"payload_format", "zstd_binary_v1"},
                               {"snapshot_id", batch.snapshotId},
                               {"algorithm", batch.algorithm},
                               {"topology_epoch", batch.topologyEpoch},
                               {"cluster_count", batch.clusters.size()},
                               {"membership_count", batch.memberships.size()},
                               {"generated_at_unix_seconds", batch.generatedAtUnixSeconds}}};
        if (auto stored = storeChunkedNode(*kgStore_, snapshotNode, compRes.value(),
                                           payloadChunkBytes_, nowSecs);
            !stored) {
            return stored.error();
        }
        compRes.value() = std::string{}; // release the payload before the sidecars are built

        // Roads and member orders ride in their own node so the snapshot payload above stays a
        // plain v1 payload. Written here, before the pointer, so a visible snapshot never lacks a
        // sidecar that was meant to accompany it; a snapshot id re-stored without either layer
        // sheds its old sidecar.
        auto memberOrders = collectTopologyMemberOrders(batch);
        if (batch.roads.empty() && memberOrders.empty()) {
            if (auto removed = deleteRoadSidecar(*kgStore_, batch.snapshotId); !removed) {
                return removed.error();
            }
        } else {
            TopologyRoadSidecar sidecar;
            sidecar.snapshotId = batch.snapshotId;
            sidecar.maxPerCluster = batch.roadMaxPerCluster;
            sidecar.roads = batch.roads;
            sidecar.roadsFullBuildEpoch = batch.roadsFullBuildEpoch;
            sidecar.roadsIncrementalPasses = batch.roadsIncrementalPasses;
            sidecar.memberOrders = std::move(memberOrders);
            auto sidecarPayload = serializeTopologyRoadSidecarCompressed(sidecar);
            if (!sidecarPayload) {
                return sidecarPayload.error();
            }
            YAMS_PLOT("topology::road_sidecar_payload_bytes",
                      static_cast<int64_t>(sidecarPayload.value().size()));
            ChunkedNode roadNode{.nodeKey = roadSidecarNodeKey(batch.snapshotId),
                                 .label = batch.snapshotId,
                                 .type = std::string{kRoadSidecarNodeType},
                                 .inspection = json{{"payload_format", "topology_roads_zstd_v1"},
                                                    {"road_count", batch.roads.size()}}};
            if (auto roadResult = storeChunkedNode(*kgStore_, roadNode, sidecarPayload.value(),
                                                   payloadChunkBytes_, nowSecs);
                !roadResult) {
                return roadResult.error();
            }
        }

        // Fitted route vectors follow the same rule: their own node, written before the
        // pointer, shed when a snapshot id is re-stored without them.
        auto routeFit = collectTopologyRouteFit(batch);
        if (routeFit.vectors.empty()) {
            if (auto removed =
                    deleteChunkedNode(*kgStore_, routeFitSidecarNodeKey(batch.snapshotId));
                !removed) {
                return removed.error();
            }
        } else {
            auto routeFitPayload = serializeTopologyRouteFitSidecarCompressed(routeFit);
            if (!routeFitPayload) {
                return routeFitPayload.error();
            }
            YAMS_PLOT("topology::route_fit_sidecar_payload_bytes",
                      static_cast<int64_t>(routeFitPayload.value().size()));
            ChunkedNode routeFitNode{.nodeKey = routeFitSidecarNodeKey(batch.snapshotId),
                                     .label = batch.snapshotId,
                                     .type = std::string{kRouteFitSidecarNodeType},
                                     .inspection = json{{"vector_count", routeFit.vectors.size()}}};
            if (auto stored = storeChunkedNode(*kgStore_, routeFitNode, routeFitPayload.value(),
                                               payloadChunkBytes_, nowSecs);
                !stored) {
                return stored.error();
            }
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
                                     {"topology_epoch", batch.topologyEpoch},
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

    // The resident copy is what was just written: no load-time road diagnostic applies to it.
    TopologyArtifactBatch residentBatch = batch;
    residentBatch.roadUnavailableReason.clear();
    residentBatch.orderUnavailableReason.clear();
    auto nextResident = makeResident(std::move(residentBatch));
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
    auto payload = readStoredPayload(*kgStore_, snapshotNodeKey(snapshotId));
    if (!payload) {
        return payload.error();
    }
    if (!payload.value().has_value()) {
        return std::optional<TopologyArtifactBatch>{};
    }
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
    auto batchResult = deserializeTopologyBatchCompressed(*payload.value());
    payload.value().reset();
    if (!batchResult) {
        return batchResult.error();
    }
    if (auto roads = attachRoadSidecar(batchResult.value()); !roads) {
        return roads.error();
    }
    if (auto routeFit = attachRouteFitSidecar(batchResult.value()); !routeFit) {
        return routeFit.error();
    }
    return std::optional<TopologyArtifactBatch>{std::move(batchResult.value())};
}

Result<void>
MetadataKgTopologyArtifactStore::attachRouteFitSidecar(TopologyArtifactBatch& batch) const {
    YAMS_ZONE_SCOPED_N("topology::store::attachRouteFitSidecar");
    for (auto& cluster : batch.clusters) {
        cluster.fittedRouteVector.clear();
    }
    batch.routeFit = RouteFitProvenance{};
    batch.routeFitUnavailableReason.clear();
    if (!kgStore_) {
        return {};
    }
    auto payload = readStoredPayload(*kgStore_, routeFitSidecarNodeKey(batch.snapshotId));
    if (!payload && payload.error().code != ErrorCode::InvalidData) {
        return payload.error();
    }
    if (payload && !payload.value().has_value()) {
        return {}; // this snapshot was never fitted
    }
    // A chunk set that fails its manifest is as undecodable as a corrupt payload.
    auto sidecar = payload
                       // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
                       ? deserializeTopologyRouteFitSidecarCompressed(*payload.value())
                       : Result<TopologyRouteFitSidecar>{payload.error()};
    if (!sidecar) {
        batch.routeFitUnavailableReason =
            "route_fit_sidecar_undecodable:" + sidecar.error().message;
        spdlog::warn("[TopologyStore] route-fit sidecar for snapshot {} is undecodable ({}); "
                     "routing continues on centroids",
                     batch.snapshotId, sidecar.error().message);
        return {};
    }
    const auto attached = attachTopologyRouteFit(batch, sidecar.value());
    if (!batch.routeFitUnavailableReason.empty()) {
        spdlog::warn("[TopologyStore] route-fit sidecar for snapshot {} attached {} of {} "
                     "vectors: {}",
                     batch.snapshotId, attached, sidecar.value().vectors.size(),
                     batch.routeFitUnavailableReason);
    }
    return {};
}

Result<void>
MetadataKgTopologyArtifactStore::attachRoadSidecar(TopologyArtifactBatch& batch) const {
    YAMS_ZONE_SCOPED_N("topology::store::attachRoadSidecar");
    batch.roads.clear();
    batch.roadMaxPerCluster = 0;
    batch.roadsFullBuildEpoch = 0;
    batch.roadsIncrementalPasses = 0;
    batch.roadUnavailableReason.clear();
    batch.orderUnavailableReason.clear();
    for (auto& cluster : batch.clusters) {
        cluster.orderKind = RoadOrderKind::None;
        cluster.orderedMembers.clear();
    }
    if (!kgStore_) {
        return {};
    }
    auto payload = readStoredPayload(*kgStore_, roadSidecarNodeKey(batch.snapshotId));
    if (!payload && payload.error().code != ErrorCode::InvalidData) {
        return payload.error();
    }
    if (payload && !payload.value().has_value()) {
        return {}; // neither roads nor orders were written for this snapshot
    }

    // A chunk set that fails its manifest is as undecodable as a corrupt payload.
    auto sidecar = payload
                       // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
                       ? deserializeTopologyRoadSidecarCompressed(*payload.value())
                       : Result<TopologyRoadSidecar>{payload.error()};
    if (!sidecar) {
        batch.roadUnavailableReason = "road_sidecar_undecodable:" + sidecar.error().message;
        batch.orderUnavailableReason = batch.roadUnavailableReason;
        spdlog::warn("[TopologyStore] road sidecar for snapshot {} is undecodable ({}); routing "
                     "continues without roads or member orders",
                     batch.snapshotId, sidecar.error().message);
        return {};
    }
    // The two layers are validated on their own: a bad road never costs an order and vice versa.
    const auto attachedOrders = attachTopologyMemberOrders(batch, sidecar.value());
    if (!batch.orderUnavailableReason.empty()) {
        spdlog::warn("[TopologyStore] road sidecar for snapshot {} carried {} usable member "
                     "orders; dropped: {}",
                     batch.snapshotId, attachedOrders, batch.orderUnavailableReason);
    }
    if (auto invalid = validateTopologyRoadSidecar(batch, sidecar.value()); invalid.has_value()) {
        batch.roadUnavailableReason = *invalid;
        spdlog::warn("[TopologyStore] road sidecar for snapshot {} rejected ({}); routing "
                     "continues without roads",
                     batch.snapshotId, *invalid);
        return {};
    }
    batch.roads = std::move(sidecar.value().roads);
    batch.roadMaxPerCluster = sidecar.value().maxPerCluster;
    batch.roadsFullBuildEpoch = sidecar.value().roadsFullBuildEpoch;
    batch.roadsIncrementalPasses = sidecar.value().roadsIncrementalPasses;
    return {};
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
    std::optional<std::uint64_t> latestTopologyEpoch;
    std::optional<std::uint64_t> latestGeneratedAt;
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
    if (latestNodeResult.value().has_value() && latestNodeResult.value()->properties.has_value()) {
        // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
        auto parsed = json::parse(*latestNodeResult.value()->properties, nullptr, false);
        if (!parsed.is_discarded() && parsed.is_object()) {
            if (auto it = parsed.find("snapshot_id"); it != parsed.end() && it->is_string()) {
                latestSnapshotId = it->get<std::string>();
            }
            if (auto it = parsed.find("topology_epoch");
                it != parsed.end() && it->is_number_unsigned()) {
                latestTopologyEpoch = it->get<std::uint64_t>();
            }
            if (auto it = parsed.find("generated_at_unix_seconds");
                it != parsed.end() && it->is_number_unsigned()) {
                latestGeneratedAt = it->get<std::uint64_t>();
            }
        }
    }
    if (latestSnapshotId.empty()) {
        std::lock_guard lock(residentMutex_);
        if (residentGeneration_ == generation) {
            resident_.reset();
            return nullptr;
        }
        return resident_;
    }
    if (current && current->batch.snapshotId == latestSnapshotId &&
        (!latestTopologyEpoch.has_value() ||
         current->batch.topologyEpoch == *latestTopologyEpoch) &&
        (!latestGeneratedAt.has_value() ||
         current->batch.generatedAtUnixSeconds == *latestGeneratedAt)) {
        return current;
    }

    auto loaded = loadSnapshotById(latestSnapshotId);
    if (!loaded) {
        return loaded.error();
    }
    if (!loaded.value().has_value()) {
        std::lock_guard lock(residentMutex_);
        if (residentGeneration_ == generation) {
            resident_.reset();
            return nullptr;
        }
        return resident_;
    }
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
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
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
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

        // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
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
