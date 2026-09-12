// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/daemon/components/repair/repair_operation.h>

#include "repair_operations_internal.h"

namespace yams::daemon::repair {

boost::asio::awaitable<RepairOperationResult>
IRepairOperation::runAsync(OperationEnv& env, const RepairRequest& req,
                           const RepairService::ProgressFn& progress,
                           std::atomic<bool>* cancelRequested) {
    co_return run(env, req, progress, cancelRequested);
}

const std::vector<std::unique_ptr<IRepairOperation>>& repairOperations() {
    static const std::vector<std::unique_ptr<IRepairOperation>> registry = [] {
        std::vector<std::unique_ptr<IRepairOperation>> ops;
        ops.push_back(makeStuckDocumentsOperation());
        ops.push_back(makeOrphanedMetadataOperation());
        ops.push_back(makeMimeTypesOperation());
        ops.push_back(makeDownloadsOperation());
        ops.push_back(makePathTreeOperation());
        ops.push_back(makeSemanticDedupeOperation());
        ops.push_back(makeOrphanedChunksOperation());
        ops.push_back(makeBlockReferencesOperation());
        ops.push_back(makeKnowledgeGraphOperation());
        ops.push_back(makeFts5IndexOperation());
        ops.push_back(makeMissingEmbeddingsOperation());
        ops.push_back(makeTopologyArtifactsOperation());
        ops.push_back(makeOptimizeDatabaseOperation());
        return ops;
    }();
    return registry;
}

IRepairOperation* repairOperationForCode(std::uint64_t code) noexcept {
    for (const auto& op : repairOperations()) {
        if (op->code() == code) {
            return op.get();
        }
    }
    return nullptr;
}

} // namespace yams::daemon::repair
