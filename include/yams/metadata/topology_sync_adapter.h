// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <cstring>
#include <string>
#include <string_view>
#include <unordered_map> // IWYU pragma: keep
#include <unordered_set>
#include <vector> // IWYU pragma: keep

#include <nlohmann/json.hpp>

#include <yams/core/types.h>
#include <yams/memory_sync/memory_sync_service.h>
#include <yams/memory_sync/records.h>
#include <yams/metadata/knowledge_graph_store.h>

namespace yams::metadata {

struct TopologySyncApplyStats {
    std::size_t nodesApplied{0};
    std::size_t edgesApplied{0};
    std::size_t nodesDeleted{0};
    std::size_t edgesDeleted{0};
    /// Edges not applied because an endpoint node is known to be deleted.
    std::size_t edgesDropped{0};
};

/// Replicates stable-key knowledge-graph nodes and edges. Numeric node IDs never
/// cross the wire; apply resolves fresh peer-local IDs before inserting edges.
///
/// The local graph is authoritative for this writer's own records: apply() never writes a record
/// this node published back into its own graph, and retractIfLocallyAbsent() tombstones an own
/// record once the local graph no longer holds it (a topology rebuild deleted the cluster node,
/// cascading its edges). Applying own records instead would resurrect what the rebuild deleted,
/// so the deletion could never be observed, let alone replicated.
class TopologySyncAdapter {
public:
    TopologySyncAdapter(KnowledgeGraphStore& store, memory_sync::MemorySyncService& sync)
        : store_(store), sync_(sync) {}

    Result<void> publishNode(const KGNode& node) {
        if (node.nodeKey.empty()) {
            return Error{ErrorCode::InvalidArgument, "topology node key must not be empty"};
        }
        memory_sync::TopologyNodeRecord record;
        record.nodeKey = node.nodeKey;
        record.label = node.label.value_or("");
        record.type = node.type.value_or("");
        if (node.createdTime) {
            record.createdTime = *node.createdTime;
            record.hasCreatedTime = true;
        }
        if (node.updatedTime) {
            record.updatedTime = *node.updatedTime;
            record.hasUpdatedTime = true;
        }
        if (node.properties) {
            record.propertiesJson = *node.properties;
            record.hasPropertiesJson = true;
        }
        return publish(nodeKey(record.nodeKey), record);
    }

    Result<void> publishEdge(std::string_view sourceNodeKey, const KGEdge& edge,
                             std::string_view targetNodeKey) {
        if (sourceNodeKey.empty() || targetNodeKey.empty() || edge.relation.empty()) {
            return Error{ErrorCode::InvalidArgument,
                         "topology edge requires source, relation, and target"};
        }
        memory_sync::TopologyEdgeRecord record;
        record.sourceNodeKey = sourceNodeKey;
        record.relation = edge.relation;
        record.targetNodeKey = targetNodeKey;
        record.weight = edge.weight;
        if (edge.createdTime) {
            record.createdTime = *edge.createdTime;
            record.hasCreatedTime = true;
        }
        if (edge.properties) {
            record.propertiesJson = *edge.properties;
            record.hasPropertiesJson = true;
        }
        return publish(edgeKey(record), record);
    }

    /// Publish an edge together with both endpoint nodes, endpoints first. A peer that receives
    /// this writer's edge has therefore received both endpoints from the same writer before it,
    /// whatever order the nodes and edges are swept in. Unchanged records publish nothing.
    Result<void> publishEdgeWithEndpoints(const KGNode& source, const KGEdge& edge,
                                          const KGNode& target) {
        if (auto published = publishNode(source); !published) {
            return published.error();
        }
        if (auto published = publishNode(target); !published) {
            return published.error();
        }
        return publishEdge(source.nodeKey, edge, target.nodeKey);
    }

    /// Tombstone this writer's own committed topology record when the local graph no longer
    /// holds it. Returns true when a tombstone was published. Records of other writers, deleted
    /// records, and records still present locally are left alone.
    Result<bool> retractIfLocallyAbsent(std::string_view key,
                                        const memory_sync::MemoryIndexRecord& winner) {
        if (winner.isTombstone() || winner.origin != sync_.localNodeId()) {
            return false;
        }
        const std::string nodePrefix = storePrefix(memory_sync::MemoryStore::TopologyNode);
        const std::string edgePrefix = storePrefix(memory_sync::MemoryStore::TopologyEdge);
        if (!key.starts_with(nodePrefix) && !key.starts_with(edgePrefix)) {
            return false;
        }
        auto payload = sync_.readCached(key);
        if (!payload) {
            // Not hydrated into the committed snapshot yet; the next pass checks it again.
            if (payload.error().code == ErrorCode::NotFound) {
                return false;
            }
            return payload.error();
        }
        const std::string_view text(reinterpret_cast<const char*>(payload.value().data()),
                                    payload.value().size());
        try {
            if (key.starts_with(nodePrefix)) {
                auto record = nlohmann::json::parse(text).get<memory_sync::TopologyNodeRecord>();
                if (nodeKey(record.nodeKey) != key) {
                    return Error{ErrorCode::InvalidData,
                                 "topology node identity does not match logical key"};
                }
                auto existing = store_.getNodeByKey(record.nodeKey);
                if (!existing) {
                    return existing.error();
                }
                if (existing.value()) {
                    return false;
                }
                if (auto erased = publishDeleteNode(record.nodeKey); !erased) {
                    return erased.error();
                }
                return true;
            }
            auto record = nlohmann::json::parse(text).get<memory_sync::TopologyEdgeRecord>();
            if (edgeKey(record) != key) {
                return Error{ErrorCode::InvalidData,
                             "topology edge identity does not match logical key"};
            }
            auto present = hasLocalEdge(record);
            if (!present) {
                return present.error();
            }
            if (present.value()) {
                return false;
            }
            if (auto erased =
                    publishDeleteEdge(record.sourceNodeKey, record.relation, record.targetNodeKey);
                !erased) {
                return erased.error();
            }
            return true;
        } catch (const std::exception& e) {
            return Error{ErrorCode::InvalidData, e.what()};
        }
    }

    Result<void> publishDeleteNode(std::string_view stableNodeKey) {
        if (stableNodeKey.empty()) {
            return Error{ErrorCode::InvalidArgument, "topology node key must not be empty"};
        }
        return sync_.erase(nodeKey(stableNodeKey), std::string(stableNodeKey));
    }

    Result<void> publishDeleteEdge(std::string_view sourceNodeKey, std::string_view relation,
                                   std::string_view targetNodeKey) {
        if (sourceNodeKey.empty() || relation.empty() || targetNodeKey.empty()) {
            return Error{ErrorCode::InvalidArgument,
                         "topology edge deletion requires source, relation, and target"};
        }
        memory_sync::TopologyEdgeRecord record;
        record.sourceNodeKey = sourceNodeKey;
        record.relation = relation;
        record.targetNodeKey = targetNodeKey;
        return sync_.erase(edgeKey(record), nlohmann::json(record).dump());
    }

    /// Apply deletions before nodes/edges, then resolve peer-local IDs for inserts. Records this
    /// node wrote itself are validated but never applied (see the class comment).
    ///
    /// An edge whose endpoint node is neither present locally nor arriving in this batch is
    /// deferred: it is skipped, the rest of the batch applies, and its logical key is listed in
    /// deferredKeys(). Publication order across peers does not put a target node ahead of the
    /// edges into it, so this is an ordering race, not an error; the edge stays a winner in the
    /// replicated index and the next apply() retries it.
    ///
    /// An edge whose endpoint is known to be deleted (its winner is a tombstone, or it is this
    /// node's own record and the local graph no longer holds it) is dropped instead: waiting
    /// cannot make the endpoint arrive. It is counted in edgesDropped and applies again if the
    /// endpoint is ever re-published.
    Result<TopologySyncApplyStats> apply() {
        deferredKeys_.clear();
        auto merged = sync_.syncOnce();
        if (!merged) {
            return merged.error();
        }

        const std::string nodePrefix = storePrefix(memory_sync::MemoryStore::TopologyNode);
        const std::string edgePrefix = storePrefix(memory_sync::MemoryStore::TopologyEdge);
        std::vector<memory_sync::TopologyNodeRecord> nodes;
        std::vector<memory_sync::TopologyEdgeRecord> edges;
        std::vector<std::string> deletedNodes;
        std::vector<memory_sync::TopologyEdgeRecord> deletedEdges;
        // Endpoint keys whose winner cannot be applied here: tombstones of any writer, and this
        // node's own records (authoritative only in the local graph).
        std::unordered_set<std::string> tombstonedNodes;
        std::unordered_set<std::string> ownNodes;
        const auto& localWriter = sync_.localNodeId();

        for (const auto& [key, envelope] : merged.value()) {
            if (!key.starts_with(nodePrefix) && !key.starts_with(edgePrefix)) {
                continue;
            }
            const bool own = envelope.origin == localWriter;
            if (envelope.isTombstone()) {
                try {
                    if (key.starts_with(nodePrefix)) {
                        if (nodeKey(envelope.tombstonePayload) != key) {
                            return Error{
                                ErrorCode::InvalidData,
                                "topology node tombstone identity does not match logical key"};
                        }
                        tombstonedNodes.insert(envelope.tombstonePayload);
                        if (!own) {
                            deletedNodes.push_back(envelope.tombstonePayload);
                        }
                    } else {
                        auto record = nlohmann::json::parse(envelope.tombstonePayload)
                                          .get<memory_sync::TopologyEdgeRecord>();
                        if (edgeKey(record) != key) {
                            return Error{
                                ErrorCode::InvalidData,
                                "topology edge tombstone identity does not match logical key"};
                        }
                        if (!own) {
                            deletedEdges.push_back(std::move(record));
                        }
                    }
                } catch (const std::exception& e) {
                    return Error{ErrorCode::InvalidData, e.what()};
                }
                continue;
            }
            auto payload = sync_.readCached(key);
            if (!payload) {
                return payload.error();
            }
            try {
                const std::string_view text(reinterpret_cast<const char*>(payload.value().data()),
                                            payload.value().size());
                if (key.starts_with(nodePrefix)) {
                    auto record =
                        nlohmann::json::parse(text).get<memory_sync::TopologyNodeRecord>();
                    if (nodeKey(record.nodeKey) != key) {
                        return Error{ErrorCode::InvalidData,
                                     "topology node identity does not match logical key"};
                    }
                    if (own) {
                        ownNodes.insert(record.nodeKey);
                    } else {
                        nodes.push_back(std::move(record));
                    }
                } else {
                    auto record =
                        nlohmann::json::parse(text).get<memory_sync::TopologyEdgeRecord>();
                    if (edgeKey(record) != key) {
                        return Error{ErrorCode::InvalidData,
                                     "topology edge identity does not match logical key"};
                    }
                    if (!own) {
                        edges.push_back(std::move(record));
                    }
                }
            } catch (const std::exception& e) {
                return Error{ErrorCode::InvalidData, e.what()};
            }
        }

        TopologySyncApplyStats stats;
        std::vector<std::int64_t> deletedEdgeIds;
        std::vector<std::int64_t> deletedNodeIds;
        std::unordered_map<std::string, std::int64_t> nodeIds;
        std::vector<KGNode> changedNodes;

        auto loadNode = [&](std::string_view key) -> Result<std::optional<KGNode>> {
            auto existing = store_.getNodeByKey(key);
            if (!existing) {
                return existing.error();
            }
            if (existing.value()) {
                nodeIds[std::string(key)] = existing.value()->id;
            }
            return existing.value();
        };

        // Complete all parsing and mutation planning before opening the write batch.
        for (const auto& record : deletedEdges) {
            auto sourceResult = loadNode(record.sourceNodeKey);
            if (!sourceResult) {
                return sourceResult.error();
            }
            auto targetResult = loadNode(record.targetNodeKey);
            if (!targetResult) {
                return targetResult.error();
            }
            const auto& source = sourceResult.value();
            const auto& target = targetResult.value();
            if (!source || !target) {
                continue;
            }
            auto existingResult = store_.getEdgesBetween(source->id, target->id, record.relation);
            if (!existingResult) {
                return existingResult.error();
            }
            for (const auto& edge : existingResult.value()) {
                deletedEdgeIds.push_back(edge.id);
            }
        }
        for (const auto& key : deletedNodes) {
            auto existingResult = loadNode(key);
            if (!existingResult) {
                return existingResult.error();
            }
            if (existingResult.value()) {
                deletedNodeIds.push_back(existingResult.value()->id);
            }
        }
        for (const auto& record : nodes) {
            auto desired = toNode(record);
            auto existingResult = loadNode(record.nodeKey);
            if (!existingResult) {
                return existingResult.error();
            }
            if (!existingResult.value() || !nodeMatches(*existingResult.value(), desired)) {
                changedNodes.push_back(std::move(desired));
            }
        }
        std::unordered_set<std::string> arrivingNodes;
        for (const auto& record : nodes) {
            arrivingNodes.insert(record.nodeKey);
        }
        const std::unordered_set<std::string> departingNodes(deletedNodes.begin(),
                                                             deletedNodes.end());
        std::vector<memory_sync::TopologyEdgeRecord> readyEdges;
        readyEdges.reserve(edges.size());
        for (auto& record : edges) {
            bool endpointsResolved = true;
            bool endpointDeleted = false;
            for (const auto* endpoint : {&record.sourceNodeKey, &record.targetNodeKey}) {
                auto existing = loadNode(*endpoint);
                if (!existing) {
                    return existing.error();
                }
                const bool present =
                    existing.value().has_value() && !departingNodes.contains(*endpoint);
                if (present || arrivingNodes.contains(*endpoint)) {
                    continue;
                }
                endpointsResolved = false;
                // Neither present nor arriving: a tombstoned endpoint, or an own record the local
                // graph no longer holds, was deleted and will not arrive.
                endpointDeleted = endpointDeleted || tombstonedNodes.contains(*endpoint) ||
                                  ownNodes.contains(*endpoint);
            }
            if (endpointDeleted) {
                ++stats.edgesDropped;
                continue;
            }
            if (!endpointsResolved) {
                deferredKeys_.push_back(edgeKey(record));
                continue;
            }
            readyEdges.push_back(std::move(record));
        }
        edges = std::move(readyEdges);

        if (deletedEdgeIds.empty() && deletedNodeIds.empty() && changedNodes.empty() &&
            edges.empty()) {
            return stats;
        }
        auto batchResult = store_.beginWriteBatch();
        if (!batchResult) {
            return batchResult.error();
        }
        auto batch = std::move(batchResult).value();
        for (const auto edgeId : deletedEdgeIds) {
            if (auto removed = batch->removeEdgeById(edgeId); !removed) {
                return removed.error();
            }
            ++stats.edgesDeleted;
        }
        for (const auto nodeId : deletedNodeIds) {
            if (auto removed = batch->deleteNodeById(nodeId); !removed) {
                return removed.error();
            }
            ++stats.nodesDeleted;
        }
        for (const auto& node : changedNodes) {
            auto replaced = batch->replaceNodeExact(node);
            if (!replaced) {
                return replaced.error();
            }
            nodeIds[node.nodeKey] = replaced.value();
            ++stats.nodesApplied;
        }

        for (const auto& record : edges) {
            const auto sourceIt = nodeIds.find(record.sourceNodeKey);
            const auto targetIt = nodeIds.find(record.targetNodeKey);
            if (sourceIt == nodeIds.end() || targetIt == nodeIds.end()) {
                // Planning deferred every edge without resolvable endpoints.
                return Error{ErrorCode::InternalError,
                             "topology edge endpoint was not resolved after planning"};
            }
            const auto desired = toEdge(record, sourceIt->second, targetIt->second);
            auto existing =
                store_.getEdgesBetween(desired.srcNodeId, desired.dstNodeId, desired.relation);
            if (!existing) {
                return existing.error();
            }
            bool exact = false;
            for (const auto& current : existing.value()) {
                if (current.weight == desired.weight &&
                    current.createdTime == desired.createdTime &&
                    current.properties == desired.properties) {
                    exact = true;
                    continue;
                }
                if (auto removed = batch->removeEdgeById(current.id); !removed) {
                    return removed.error();
                }
            }
            if (!exact) {
                auto inserted = batch->addEdge(desired);
                if (!inserted) {
                    return inserted.error();
                }
                ++stats.edgesApplied;
            }
        }
        if (auto committed = batch->commit(); !committed) {
            return committed.error();
        }
        return stats;
    }

    /// Logical keys of edges the last apply() deferred because an endpoint node is missing.
    const std::vector<std::string>& deferredKeys() const noexcept { return deferredKeys_; }

private:
    Result<bool> hasLocalEdge(const memory_sync::TopologyEdgeRecord& record) {
        auto source = store_.getNodeByKey(record.sourceNodeKey);
        if (!source) {
            return source.error();
        }
        auto target = store_.getNodeByKey(record.targetNodeKey);
        if (!target) {
            return target.error();
        }
        if (!source.value() || !target.value()) {
            return false;
        }
        auto edges =
            store_.getEdgesBetween(source.value()->id, target.value()->id, record.relation);
        if (!edges) {
            return edges.error();
        }
        return !edges.value().empty();
    }

    template <typename Record> Result<void> publish(const std::string& key, const Record& record) {
        const std::string dump = nlohmann::json(record).dump();
        std::vector<std::byte> bytes(dump.size());
        std::memcpy(bytes.data(), dump.data(), dump.size());
        auto published = sync_.publishIfChanged(key, bytes);
        if (!published) {
            return published.error();
        }
        return {};
    }

    static bool nodeMatches(const KGNode& current, const KGNode& desired) {
        return current.nodeKey == desired.nodeKey && current.label == desired.label &&
               current.type == desired.type && current.createdTime == desired.createdTime &&
               current.updatedTime == desired.updatedTime &&
               current.properties == desired.properties;
    }

    static KGNode toNode(const memory_sync::TopologyNodeRecord& record) {
        KGNode node;
        node.nodeKey = record.nodeKey;
        if (!record.label.empty()) {
            node.label = record.label;
        }
        if (!record.type.empty()) {
            node.type = record.type;
        }
        if (record.hasCreatedTime) {
            node.createdTime = record.createdTime;
        }
        if (record.hasUpdatedTime) {
            node.updatedTime = record.updatedTime;
        }
        if (record.hasPropertiesJson) {
            node.properties = record.propertiesJson;
        } else if (!record.properties.empty()) {
            node.properties = nlohmann::json(record.properties).dump();
        }
        return node;
    }

    static KGEdge toEdge(const memory_sync::TopologyEdgeRecord& record, std::int64_t sourceId,
                         std::int64_t targetId) {
        KGEdge edge;
        edge.srcNodeId = sourceId;
        edge.dstNodeId = targetId;
        edge.relation = record.relation;
        edge.weight = static_cast<float>(record.weight);
        if (record.hasCreatedTime) {
            edge.createdTime = record.createdTime;
        }
        if (record.hasPropertiesJson) {
            edge.properties = record.propertiesJson;
        }
        return edge;
    }

    static std::string storePrefix(memory_sync::MemoryStore store) {
        return std::string(memory_sync::memoryStoreName(store)) + "/";
    }

    static std::string nodeKey(std::string_view key) {
        return storePrefix(memory_sync::MemoryStore::TopologyNode) +
               memory_sync::escapeRecordKeySegment(key);
    }

    static std::string edgeKey(const memory_sync::TopologyEdgeRecord& edge) {
        return storePrefix(memory_sync::MemoryStore::TopologyEdge) + edge.id();
    }

    KnowledgeGraphStore& store_;
    memory_sync::MemorySyncService& sync_;
    std::vector<std::string> deferredKeys_;
};

} // namespace yams::metadata
