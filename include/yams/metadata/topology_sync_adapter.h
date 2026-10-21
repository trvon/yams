// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
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
    /// Exact deferred-edge count, independent of the bounded diagnostic key sample.
    std::size_t edgesDeferred{0};
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
        auto payload = sync_.readCommittedIfCurrent(key, winner);
        if (!payload) {
            return payload.error();
        }
        if (!payload.value()) {
            return false;
        }
        const auto& value = *payload.value();
        const std::string_view text(reinterpret_cast<const char*>(value.data()), value.size());
        try {
            if (key.starts_with(nodePrefix)) {
                auto record = nlohmann::json::parse(text).get<memory_sync::TopologyNodeRecord>();
                if (nodeKey(record.nodeKey) != key) {
                    return Error{ErrorCode::InvalidData,
                                 "topology node identity does not match logical key"};
                }
                auto existing = store_.hasNodeByKey(record.nodeKey);
                if (!existing) {
                    return existing.error();
                }
                if (existing.value()) {
                    return false;
                }
                return sync_.eraseIfCurrent(key, winner, record.nodeKey);
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
            memory_sync::TopologyEdgeRecord deleted;
            deleted.sourceNodeKey = record.sourceNodeKey;
            deleted.relation = record.relation;
            deleted.targetNodeKey = record.targetNodeKey;
            return sync_.eraseIfCurrent(key, winner, nlohmann::json(deleted).dump());
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
        using Service = memory_sync::MemorySyncService;
        using Envelope = memory_sync::MemoryIndexRecord;
        using Node = memory_sync::TopologyNodeRecord;
        using Edge = memory_sync::TopologyEdgeRecord;
        auto captured = sync_.captureAdapterView(Service::AdapterPressureMode::Defer);
        if (!captured) {
            return captured.error();
        }
        const auto& view = captured.value();
        const auto& localWriter = sync_.localNodeId();
        const auto nodePrefix = storePrefix(memory_sync::MemoryStore::TopologyNode);
        const auto edgePrefix = storePrefix(memory_sync::MemoryStore::TopologyEdge);
        auto nodes = [&](auto&& visitor) -> Result<void> {
            return sync_.visitAdapterWinners(
                view, nodePrefix,
                [&](const std::string& key, const Envelope& envelope) -> Result<void> {
                    auto record = readNode(view, key, envelope);
                    if (!record) {
                        return record.error();
                    }
                    return visitor(key, envelope, record.value());
                });
        };
        auto edges = [&](auto&& visitor) -> Result<void> {
            return sync_.visitAdapterWinners(
                view, edgePrefix,
                [&](const std::string& key, const Envelope& envelope) -> Result<void> {
                    auto record = readEdge(view, key, envelope);
                    if (!record) {
                        return record.error();
                    }
                    return visitor(key, envelope, record.value());
                });
        };
        // Complete source validation before ANY target write. The derived index
        // classifies unavailable endpoints; it never authenticates source records.
        bool hasRemoteRecords = false;
        bool hasRemoteEdges = false;
        if (auto checked =
                edges([&](std::string_view, const Envelope& envelope, const Edge&) -> Result<void> {
                    if (envelope.origin != localWriter) {
                        hasRemoteRecords = true;
                        hasRemoteEdges = hasRemoteEdges || !envelope.isTombstone();
                    }
                    return {};
                });
            !checked) {
            return checked.error();
        }
        std::shared_ptr<memory_sync::MemorySyncLoop::AdapterIndex> unavailable;
        if (auto checked = nodes([&](std::string_view, const Envelope& envelope,
                                     const Node& record) -> Result<void> {
                const bool own = envelope.origin == localWriter;
                hasRemoteRecords = hasRemoteRecords || !own;
                if (hasRemoteEdges && (own || envelope.isTombstone())) {
                    if (!unavailable) {
                        auto index = sync_.createAdapterIndex(view);
                        if (!index) {
                            return index.error();
                        }
                        unavailable = std::move(index.value());
                    }
                    return unavailable->put(record.nodeKey, "unavailable");
                }
                return {};
            });
            !checked) {
            return checked.error();
        }
        TopologySyncApplyStats stats;
        if (!hasRemoteRecords) {
            return stats;
        }
        if (auto ready = sync_.checkAdapterWork(view); !ready) {
            return ready.error();
        }
        auto opened = store_.beginWriteBatch();
        if (!opened) {
            return opened.error();
        }
        auto batch = std::move(opened).value();
        bool mutated = false;
        auto between = [&](std::int64_t source, std::int64_t target, std::string_view relation,
                           auto&& visitor) -> Result<void> {
            std::optional<std::int64_t> cursor;
            for (;;) {
                if (auto ready = sync_.checkAdapterWork(view); !ready) {
                    return ready.error();
                }
                auto page = batch->getEdgesBetweenPage(source, target, relation, cursor);
                if (!page) {
                    return page.error();
                }
                if (page.value().empty()) {
                    return {};
                }
                if (page.value().size() > KnowledgeGraphStore::WriteBatch::kEdgePageSize) {
                    return Error{ErrorCode::ResourceExhausted, "topology edge page exceeds budget"};
                }
                for (const auto& edge : page.value()) {
                    if ((cursor && edge.id <= *cursor) || edge.srcNodeId != source ||
                        edge.dstNodeId != target || edge.relation != relation) {
                        return Error{ErrorCode::InvalidData, "invalid topology edge cursor"};
                    }
                    if (auto ready = sync_.checkAdapterWork(view); !ready) {
                        return ready.error();
                    }
                    if (auto visited = visitor(edge); !visited) {
                        return visited.error();
                    }
                    cursor = edge.id;
                }
            }
        };
        // Explicit edge deletions precede cascading node deletions, preserving counters.
        if (auto deleted = edges([&](std::string_view, const Envelope& envelope,
                                     const Edge& record) -> Result<void> {
                if (!envelope.isTombstone() || envelope.origin == localWriter) {
                    return {};
                }
                auto source = batch->getNodeByKey(record.sourceNodeKey);
                if (!source) {
                    return source.error();
                }
                auto target = batch->getNodeByKey(record.targetNodeKey);
                if (!target) {
                    return target.error();
                }
                if (!source.value() || !target.value()) {
                    return {};
                }
                return between(source.value()->id, target.value()->id, record.relation,
                               [&](const KGEdge& edge) -> Result<void> {
                                   if (auto removed = batch->removeEdgeById(edge.id); !removed) {
                                       return removed.error();
                                   }
                                   mutated = true;
                                   ++stats.edgesDeleted;
                                   return {};
                               });
            });
            !deleted) {
            return deleted.error();
        }
        if (auto deleted = nodes([&](std::string_view, const Envelope& envelope,
                                     const Node& record) -> Result<void> {
                if (!envelope.isTombstone() || envelope.origin == localWriter) {
                    return {};
                }
                auto current = batch->getNodeByKey(record.nodeKey);
                if (!current) {
                    return current.error();
                }
                if (current.value()) {
                    if (auto removed = batch->deleteNodeById(current.value()->id); !removed) {
                        return removed.error();
                    }
                    mutated = true;
                    ++stats.nodesDeleted;
                }
                return {};
            });
            !deleted) {
            return deleted.error();
        }
        if (auto applied = nodes([&](std::string_view, const Envelope& envelope,
                                     const Node& record) -> Result<void> {
                if (envelope.isTombstone() || envelope.origin == localWriter) {
                    return {};
                }
                auto current = batch->getNodeByKey(record.nodeKey);
                if (!current) {
                    return current.error();
                }
                const auto desired = toNode(record);
                if (!current.value() || !nodeMatches(*current.value(), desired)) {
                    if (auto replaced = batch->replaceNodeExact(desired); !replaced) {
                        return replaced.error();
                    }
                    mutated = true;
                    ++stats.nodesApplied;
                }
                return {};
            });
            !applied) {
            return applied.error();
        }
        if (auto applied = edges([&](std::string_view key, const Envelope& envelope,
                                     const Edge& record) -> Result<void> {
                if (envelope.isTombstone() || envelope.origin == localWriter) {
                    return {};
                }
                auto source = batch->getNodeByKey(record.sourceNodeKey);
                if (!source) {
                    return source.error();
                }
                auto target = batch->getNodeByKey(record.targetNodeKey);
                if (!target) {
                    return target.error();
                }
                if (!source.value() || !target.value()) {
                    bool deleted = false;
                    for (const auto* missing : {source.value() ? nullptr : &record.sourceNodeKey,
                                                target.value() ? nullptr : &record.targetNodeKey}) {
                        if (missing && unavailable) {
                            auto marker = unavailable->lookup(*missing);
                            if (!marker) {
                                return marker.error();
                            }
                            if (marker.value()) {
                                if (*marker.value() != "unavailable") {
                                    return Error{ErrorCode::IOError,
                                                 "invalid topology endpoint index"};
                                }
                                deleted = true;
                            }
                        }
                    }
                    if (deleted) {
                        ++stats.edgesDropped;
                    } else {
                        ++stats.edgesDeferred;
                        // Protocol keys <=1024 bytes: <=64 diagnostic keys retain <=64 KiB.
                        if (deferredKeys_.size() < kDeferredKeySampleLimit) {
                            deferredKeys_.emplace_back(key);
                        }
                    }
                    return {};
                }
                const auto desired = toEdge(record, source.value()->id, target.value()->id);
                bool exact = false;
                if (auto checked = between(desired.srcNodeId, desired.dstNodeId, desired.relation,
                                           [&](const KGEdge& current) -> Result<void> {
                                               if (current.weight == desired.weight &&
                                                   current.createdTime == desired.createdTime &&
                                                   current.properties == desired.properties) {
                                                   exact = true;
                                                   return {};
                                               }
                                               if (auto removed = batch->removeEdgeById(current.id);
                                                   !removed) {
                                                   return removed.error();
                                               }
                                               mutated = true;
                                               return {};
                                           });
                    !checked) {
                    return checked.error();
                }
                if (!exact) {
                    if (auto inserted = batch->addEdge(desired); !inserted) {
                        return inserted.error();
                    }
                    mutated = true;
                    ++stats.edgesApplied;
                }
                return {};
            });
            !applied) {
            return applied.error();
        }
        if (auto ready = sync_.checkAdapterWork(view); !ready) {
            return ready.error();
        }
        // Empty commits also advance KG generation. Roll back read-only passes instead.
        if (mutated) {
            if (auto committed = batch->commit(); !committed) {
                return committed.error();
            }
        }
        return stats;
    }

    /// Bounded sample (at most 64 keys) of edges deferred because an endpoint is missing.
    /// TopologySyncApplyStats::edgesDeferred reports the exact total, not the sample size.
    const std::vector<std::string>& deferredKeys() const noexcept { return deferredKeys_; }

private:
    static constexpr std::size_t kDeferredKeySampleLimit = 64;
    using AdapterView = memory_sync::MemorySyncService::AdapterView;

    Result<memory_sync::TopologyNodeRecord>
    readNode(const AdapterView& view, std::string_view key,
             const memory_sync::MemoryIndexRecord& envelope) {
        memory_sync::TopologyNodeRecord record;
        if (envelope.isTombstone()) {
            record.nodeKey = envelope.tombstonePayload;
        } else {
            auto payload = sync_.readAdapterPayload(view, key);
            if (!payload) {
                return payload.error();
            }
            try {
                const std::string_view text(reinterpret_cast<const char*>(payload.value().data()),
                                            payload.value().size());
                record = nlohmann::json::parse(text).get<memory_sync::TopologyNodeRecord>();
            } catch (const std::exception& error) {
                return Error{ErrorCode::InvalidData, error.what()};
            }
        }
        if (nodeKey(record.nodeKey) != key) {
            return Error{ErrorCode::InvalidData,
                         "topology node identity does not match logical key"};
        }
        return record;
    }

    Result<memory_sync::TopologyEdgeRecord>
    readEdge(const AdapterView& view, std::string_view key,
             const memory_sync::MemoryIndexRecord& envelope) {
        memory_sync::TopologyEdgeRecord record;
        try {
            if (envelope.isTombstone()) {
                record = nlohmann::json::parse(envelope.tombstonePayload)
                             .get<memory_sync::TopologyEdgeRecord>();
            } else {
                auto payload = sync_.readAdapterPayload(view, key);
                if (!payload) {
                    return payload.error();
                }
                const std::string_view text(reinterpret_cast<const char*>(payload.value().data()),
                                            payload.value().size());
                record = nlohmann::json::parse(text).get<memory_sync::TopologyEdgeRecord>();
            }
        } catch (const std::exception& error) {
            return Error{ErrorCode::InvalidData, error.what()};
        }
        if (edgeKey(record) != key) {
            return Error{ErrorCode::InvalidData,
                         "topology edge identity does not match logical key"};
        }
        return record;
    }

    Result<bool> hasLocalEdge(const memory_sync::TopologyEdgeRecord& record) {
        return store_.hasEdgeByKeys(record.sourceNodeKey, record.targetNodeKey, record.relation);
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
