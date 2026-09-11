// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Repair operation "topology" (wire code 12); see repair_operation.h.

#include <yams/daemon/components/repair/repair_operation.h>

#include "../repair_operations_internal.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace yams::daemon::repair {

namespace {

RepairOperationResult rebuildTopologyArtifacts(OperationEnv& env, const RepairRequest& req,
                                               const RepairService::ProgressFn& progress) {
    (void)progress;

    RepairOperationResult result;
    result.operation = "topology";

    if (!env.ctx.rebuildTopologyArtifacts) {
        result.failed = 1;
        result.message = "Topology rebuild callback unavailable";
        return result;
    }

    auto rebuildResult = env.ctx.rebuildTopologyArtifacts(
        req.dryRun ? std::string{"repair.topology.dry_run"} : std::string{"repair.topology"},
        req.dryRun, {});
    if (!rebuildResult) {
        result.failed = 1;
        result.message = "Topology rebuild failed: " + rebuildResult.error().message;
        return result;
    }

    const auto& stats = rebuildResult.value();
    result.processed = stats.documentsProcessed;
    if (stats.skipped || req.dryRun) {
        result.skipped = stats.documentsProcessed;
    } else {
        result.succeeded = stats.documentsProcessed;
    }

    std::string message = (stats.skipped || req.dryRun) ? "Topology rebuild analyzed "
                                                        : "Topology rebuild stored artifacts for ";
    message += std::to_string(stats.documentsProcessed) +
               " docs (clusters=" + std::to_string(stats.clustersBuilt) +
               ", memberships=" + std::to_string(stats.membershipsBuilt) + ")";
    if (!stats.snapshotId.empty()) {
        message += ", snapshot=" + stats.snapshotId;
    }
    if (stats.documentsMissingEmbeddings > 0 || stats.documentsMissingGraphNodes > 0) {
        message += " [missing_embeddings=" + std::to_string(stats.documentsMissingEmbeddings) +
                   ", missing_graph_nodes=" + std::to_string(stats.documentsMissingGraphNodes) +
                   "]";
    }
    if (!stats.issues.empty()) {
        message += ": " + stats.issues.front();
    }
    result.message = std::move(message);
    return result;
}

class TopologyArtifactsOperation final : public IRepairOperation {
public:
    std::string_view name() const noexcept override { return "topology"; }
    std::uint64_t code() const noexcept override { return 12; }

    RepairOperationResult run(OperationEnv& env, const RepairRequest& req,
                              const RepairService::ProgressFn& progress,
                              std::atomic<bool>* cancelRequested) override {
        (void)cancelRequested;
        return rebuildTopologyArtifacts(env, req, progress);
    }
};

} // namespace

std::unique_ptr<IRepairOperation> makeTopologyArtifactsOperation() {
    return std::make_unique<TopologyArtifactsOperation>();
}

} // namespace yams::daemon::repair
