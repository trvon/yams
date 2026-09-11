// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Repair operation "graph" (wire code 9); see repair_operation.h.

#include <yams/daemon/components/repair/repair_operation.h>

#include "../repair_operations_internal.h"

#include <spdlog/spdlog.h>
#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/components/WriteCoordinator.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace yams::daemon::repair {

namespace {

struct KgCleanupStats {
    uint64_t nodesScanned{0};
    uint64_t orphanNodes{0};
    uint64_t nodesDeleted{0};
    uint64_t edgesDeleted{0};
    uint64_t docEntitiesDeleted{0};
    uint64_t skipped{0};
    uint64_t errors{0};
    std::vector<std::string> issues;
};

KgCleanupStats cleanOrphanedKgEntries(OperationEnv& env, bool dryRun, bool verbose,
                                      const RepairService::ProgressFn& progress) {
    (void)verbose;
    KgCleanupStats stats;
    auto meta = env.ctx.getMetadataRepo ? env.ctx.getMetadataRepo() : nullptr;
    auto kgStore = env.ctx.getKgStore ? env.ctx.getKgStore() : nullptr;
    if (!meta || !kgStore) {
        stats.errors = 1;
        stats.issues.push_back("Metadata or KG store unavailable");
        return stats;
    }

    auto emitProgress = [&](const std::string& message) {
        if (!progress)
            return;
        RepairEvent ev;
        ev.phase = "repairing";
        ev.operation = "graph";
        ev.processed = stats.nodesScanned;
        ev.succeeded = stats.nodesDeleted + stats.edgesDeleted + stats.docEntitiesDeleted;
        ev.failed = stats.errors;
        ev.skipped = stats.skipped;
        ev.message = message;
        progress(ev);
    };

    auto scanType = [&](const std::string& type, const std::string& prefix, bool deleteByHash) {
        constexpr std::size_t kBatchSize = 500;
        std::size_t offset = 0;
        std::unique_ptr<WriteBatch> orphanDeleteBatch;
        {
            auto* coord = env.ctx.getWriteCoordinator ? env.ctx.getWriteCoordinator() : nullptr;
            if (coord) {
                orphanDeleteBatch = std::make_unique<WriteBatch>();
                orphanDeleteBatch->source = "RepairService::orphanKgNodeDelete";
            }
        }
        while (true) {
            auto nodesRes = kgStore->findNodesByType(type, kBatchSize, offset);
            if (!nodesRes) {
                ++stats.errors;
                stats.issues.push_back("findNodesByType(" + type +
                                       ") failed: " + nodesRes.error().message);
                break;
            }

            const auto& nodes = nodesRes.value();
            if (nodes.empty())
                break;

            std::size_t deletedNodesInBatch = 0;
            for (const auto& node : nodes) {
                ++stats.nodesScanned;
                if (node.nodeKey.compare(0, prefix.size(), prefix) != 0)
                    continue;

                std::string hash = node.nodeKey.substr(prefix.size());
                if (hash.empty())
                    continue;

                auto docRes = meta->getDocumentByHash(hash);
                if (!docRes) {
                    ++stats.errors;
                    stats.issues.push_back("getDocumentByHash failed for " + hash + ": " +
                                           docRes.error().message);
                    continue;
                }
                if (!docRes.value().has_value()) {
                    ++stats.orphanNodes;
                    if (dryRun) {
                        ++stats.skipped;
                        continue;
                    }
                    if (orphanDeleteBatch) {
                        if (deleteByHash) {
                            orphanDeleteBatch->ops.emplace_back(DeleteNodesForDocumentHashOp{hash});
                        } else {
                            orphanDeleteBatch->ops.emplace_back(DeleteNodeByIdOp{node.id});
                        }
                    } else {
                        auto* coord =
                            env.ctx.getWriteCoordinator ? env.ctx.getWriteCoordinator() : nullptr;
                        if (!coord) {
                            ++stats.errors;
                            stats.issues.push_back(
                                "WriteCoordinator unavailable for orphan KG node delete");
                            continue;
                        }
                        auto wb = std::make_unique<WriteBatch>();
                        wb->source = "RepairService::orphanKgNodeDelete";
                        if (deleteByHash) {
                            wb->ops.emplace_back(DeleteNodesForDocumentHashOp{hash});
                        } else {
                            wb->ops.emplace_back(DeleteNodeByIdOp{node.id});
                        }
                        coord->enqueue(std::move(wb));
                        auto flushRes = coord->flush();
                        if (!flushRes) {
                            ++stats.errors;
                            stats.issues.push_back("orphan KG node delete flush failed for " +
                                                   node.nodeKey + ": " + flushRes.error().message);
                            continue;
                        }
                    }
                    ++stats.nodesDeleted;
                    ++deletedNodesInBatch;
                }

                if (stats.nodesScanned % 200 == 0) {
                    emitProgress("Scanned " + std::to_string(stats.nodesScanned) +
                                 " KG nodes (orphans=" + std::to_string(stats.orphanNodes) + ")");
                }
            }

            if (nodes.size() < kBatchSize)
                break;

            if (!dryRun && deletedNodesInBatch > 0) {
                if (offset >= deletedNodesInBatch) {
                    offset -= deletedNodesInBatch;
                } else {
                    offset = 0;
                }
            }
            offset += nodes.size();
        }

        // Flush accumulated orphan node deletes as a single batch.
        if (orphanDeleteBatch && !orphanDeleteBatch->ops.empty()) {
            auto* coord = env.ctx.getWriteCoordinator ? env.ctx.getWriteCoordinator() : nullptr;
            if (coord) {
                coord->enqueue(std::move(orphanDeleteBatch));
                auto flushRes = coord->flush();
                if (!flushRes) {
                    ++stats.errors;
                    stats.issues.push_back("orphan KG node delete batch flush failed: " +
                                           flushRes.error().message);
                }
            }
        }
    };

    scanType("document", "doc:", true);
    scanType("blob", "blob:", false);

    if (!dryRun) {
        auto* coord = env.ctx.getWriteCoordinator ? env.ctx.getWriteCoordinator() : nullptr;
        if (!coord) {
            ++stats.errors;
            stats.issues.push_back(
                "WriteCoordinator unavailable for orphan edge/doc_entities cleanup");
        } else {
            const auto before = coord->getStats();
            auto wb = std::make_unique<WriteBatch>();
            wb->source = "RepairService::kgOrphanCleanup";
            wb->ops.emplace_back(DeleteOrphanedEdgesOp{});
            wb->ops.emplace_back(DeleteOrphanedDocEntitiesOp{});
            coord->enqueue(std::move(wb));
            auto flushRes = coord->flush();
            if (!flushRes) {
                ++stats.errors;
                stats.issues.push_back("orphan edge/doc_entities cleanup flush failed: " +
                                       flushRes.error().message);
            } else {
                // flush() drains the queue, so the coordinator stats now reflect this batch.
                const auto after = coord->getStats();
                if (after.edgesDeleted >= before.edgesDeleted)
                    stats.edgesDeleted += after.edgesDeleted - before.edgesDeleted;
                if (after.docEntitiesDeleted >= before.docEntitiesDeleted)
                    stats.docEntitiesDeleted +=
                        after.docEntitiesDeleted - before.docEntitiesDeleted;
            }
        }
    } else {
        emitProgress("Dry-run: skipped orphan edge/doc_entities cleanup");
    }

    if (stats.edgesDeleted > 0 || stats.docEntitiesDeleted > 0) {
        emitProgress("Cleaned orphan edges=" + std::to_string(stats.edgesDeleted) +
                     ", stale doc_entities=" + std::to_string(stats.docEntitiesDeleted));
    }

    return stats;
}

RepairOperationResult repairKnowledgeGraph(OperationEnv& env, const RepairRequest& req,
                                           const RepairService::ProgressFn& progress,
                                           std::atomic<bool>* cancelRequested) {
    RepairOperationResult result;
    result.operation = "graph";

    auto graphComponent = env.ctx.getGraphComponent ? env.ctx.getGraphComponent() : nullptr;
    auto kgStore = env.ctx.getKgStore ? env.ctx.getKgStore() : nullptr;
    auto meta = env.ctx.getMetadataRepo ? env.ctx.getMetadataRepo() : nullptr;
    if (!graphComponent || !kgStore || !meta) {
        result.message = "Knowledge graph components not available";
        return result;
    }

    auto graphProgress = [&progress](uint64_t processed, uint64_t total,
                                     const GraphComponent::RepairStats& snap) {
        if (!progress)
            return;
        RepairEvent ev;
        ev.phase = "repairing";
        ev.operation = "graph";
        ev.processed = processed;
        ev.total = total;
        ev.succeeded = snap.nodesCreated + snap.nodesUpdated + snap.edgesCreated;
        ev.failed = snap.errors;
        ev.message = "graph repair scanning";
        progress(ev);
    };
    auto repairRes = graphComponent->repairGraph(req.dryRun, graphProgress, cancelRequested);
    if (!repairRes) {
        result.failed = 1;
        result.message = "Graph repair failed: " + repairRes.error().message;
        return result;
    }

    const auto& stats = repairRes.value();
    result.processed += stats.nodesCreated + stats.nodesUpdated + stats.edgesCreated;
    result.succeeded += stats.nodesCreated + stats.nodesUpdated + stats.edgesCreated;
    result.failed += stats.errors;
    for (const auto& issue : stats.issues) {
        spdlog::warn("[RepairService] graph repair issue: {}", issue);
    }

    auto cleanup = cleanOrphanedKgEntries(env, req.dryRun, req.verbose, progress);
    result.processed += cleanup.nodesScanned;
    result.succeeded += cleanup.nodesDeleted + cleanup.edgesDeleted + cleanup.docEntitiesDeleted;
    result.skipped += cleanup.skipped;
    result.failed += cleanup.errors;
    for (const auto& issue : cleanup.issues) {
        spdlog::warn("[RepairService] graph cleanup issue: {}", issue);
    }

    std::string message = "Rebuilt graph nodes/edges (nodes=" + std::to_string(stats.nodesCreated) +
                          ", edges=" + std::to_string(stats.edgesCreated) + ")";
    if (cleanup.nodesDeleted > 0 || cleanup.edgesDeleted > 0 || cleanup.docEntitiesDeleted > 0) {
        message += "; cleaned orphans (nodes=" + std::to_string(cleanup.nodesDeleted) +
                   ", edges=" + std::to_string(cleanup.edgesDeleted) +
                   ", doc_entities=" + std::to_string(cleanup.docEntitiesDeleted) + ")";
    }
    // Surface the graph repair summaries (reconciliation counts, orphaned-node removal, skips)
    // to the caller — previously these were only logged, so the CLI showed none of them.
    for (const auto& issue : stats.issues) {
        message += "; " + issue;
    }
    if (req.dryRun) {
        message += " (dry-run: changes were not committed)";
    }
    result.message = std::move(message);
    return result;
}

class KnowledgeGraphOperation final : public IRepairOperation {
public:
    std::string_view name() const noexcept override { return "graph"; }
    std::uint64_t code() const noexcept override { return 9; }

    RepairOperationResult run(OperationEnv& env, const RepairRequest& req,
                              const RepairService::ProgressFn& progress,
                              std::atomic<bool>* cancelRequested) override {
        return repairKnowledgeGraph(env, req, progress, cancelRequested);
    }
};

} // namespace

std::unique_ptr<IRepairOperation> makeKnowledgeGraphOperation() {
    return std::make_unique<KnowledgeGraphOperation>();
}

} // namespace yams::daemon::repair
