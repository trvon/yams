// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Repair operation "topology" (wire code 12); see repair_operation.h.

#include <yams/daemon/components/repair/repair_operation.h>

#include "../repair_operation_support.h"
#include "../repair_operations_internal.h"

#include <spdlog/spdlog.h>
#include <yams/core/repair_fsm.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/db_salvage.h>
#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/MetadataWriteFacade.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningManager.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/components/VectorIndexCoordinator.h>
#include <yams/daemon/components/WriteCoordinator.h>
#include <yams/daemon/metric_keys.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/content_extractor.h>
#include <yams/extraction/extraction_util.h>
#include <yams/integrity/repair_manager.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>
#include <yams/profiling.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_database.h>

#include <sqlite3.h>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/thread_pool.hpp>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <set>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

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
