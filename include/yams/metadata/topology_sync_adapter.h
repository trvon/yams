// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <cstring>
#include <string>
#include <string_view>
#include <unordered_map> // IWYU pragma: keep
#include <vector>        // IWYU pragma: keep

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
};

/// Replicates stable-key knowledge-graph nodes and edges. Numeric node IDs never
/// cross the wire; apply resolves fresh peer-local IDs before inserting edges.
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

    /// Apply deletions before nodes/edges, then resolve peer-local IDs for inserts.
    Result<TopologySyncApplyStats> apply() {
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

        for (const auto& [key, envelope] : merged.value()) {
            if (!key.starts_with(nodePrefix) && !key.starts_with(edgePrefix)) {
                continue;
            }
            if (envelope.isTombstone()) {
                try {
                    if (key.starts_with(nodePrefix)) {
                        if (nodeKey(envelope.tombstonePayload) != key) {
                            return Error{
                                ErrorCode::InvalidData,
                                "topology node tombstone identity does not match logical key"};
                        }
                        deletedNodes.push_back(envelope.tombstonePayload);
                    } else {
                        auto record = nlohmann::json::parse(envelope.tombstonePayload)
                                          .get<memory_sync::TopologyEdgeRecord>();
                        if (edgeKey(record) != key) {
                            return Error{
                                ErrorCode::InvalidData,
                                "topology edge tombstone identity does not match logical key"};
                        }
                        deletedEdges.push_back(std::move(record));
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
                    nodes.push_back(std::move(record));
                } else {
                    auto record =
                        nlohmann::json::parse(text).get<memory_sync::TopologyEdgeRecord>();
                    if (edgeKey(record) != key) {
                        return Error{ErrorCode::InvalidData,
                                     "topology edge identity does not match logical key"};
                    }
                    edges.push_back(std::move(record));
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
            auto existingResult = store_.getEdgesFrom(source->id, record.relation);
            if (!existingResult) {
                return existingResult.error();
            }
            for (const auto& edge : existingResult.value()) {
                if (edge.dstNodeId == target->id) {
                    deletedEdgeIds.push_back(edge.id);
                }
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
        for (const auto& record : edges) {
            auto sourceResult = loadNode(record.sourceNodeKey);
            if (!sourceResult) {
                return sourceResult.error();
            }
            auto targetResult = loadNode(record.targetNodeKey);
            if (!targetResult) {
                return targetResult.error();
            }
            const bool sourceArrives = std::any_of(nodes.begin(), nodes.end(), [&](const auto& n) {
                return n.nodeKey == record.sourceNodeKey;
            });
            const bool targetArrives = std::any_of(nodes.begin(), nodes.end(), [&](const auto& n) {
                return n.nodeKey == record.targetNodeKey;
            });
            if ((!sourceResult.value() && !sourceArrives) ||
                (!targetResult.value() && !targetArrives)) {
                return Error{ErrorCode::NotFound,
                             "topology edge references a missing replicated node"};
            }
        }

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
                return Error{ErrorCode::NotFound,
                             "topology edge references a missing replicated node"};
            }
            const auto desired = toEdge(record, sourceIt->second, targetIt->second);
            auto existing = store_.getEdgesFrom(desired.srcNodeId, desired.relation);
            if (!existing) {
                return existing.error();
            }
            bool exact = false;
            for (const auto& current : existing.value()) {
                if (current.dstNodeId != desired.dstNodeId) {
                    continue;
                }
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

private:
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
};

} // namespace yams::metadata
