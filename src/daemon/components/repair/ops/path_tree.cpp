// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Repair operation "path_tree" (wire code 5); see repair_operation.h.

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

RepairOperationResult rebuildPathTree(OperationEnv& env, bool dryRun, bool verbose,
                                      RepairService::ProgressFn progress) {
    RepairOperationResult result;
    result.operation = "path_tree";

    auto meta = env.ctx.getMetadataRepo ? env.ctx.getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata not available";
        return result;
    }

    // Use RepairManager for path tree repair
    auto concreteRepo = std::dynamic_pointer_cast<metadata::MetadataRepository>(meta);
    if (!concreteRepo) {
        result.message = "Could not get concrete MetadataRepository";
        return result;
    }

    if (dryRun) {
        auto countResult = concreteRepo->countDocsMissingPathTree();
        if (!countResult) {
            result.message = "Failed to count missing path tree entries";
            return result;
        }
        uint64_t missing = countResult.value();
        result.processed = missing;
        result.skipped = missing;
        result.message = "Would rebuild " + std::to_string(missing) + " path tree entries";
        return result;
    }

    integrity::RepairManager repairMgr(*concreteRepo);
    auto repairResult = repairMgr.repairPathTree([](uint64_t, uint64_t) {});
    if (!repairResult) {
        result.message = "Path tree repair failed: " + repairResult.error().message;
        result.failed = 1;
        return result;
    }

    const auto& stats = repairResult.value();
    result.processed = stats.documentsScanned;
    result.succeeded = stats.nodesCreated;
    result.failed = stats.errors;
    result.message = "Created " + std::to_string(stats.nodesCreated) + " path tree entries";
    return result;
}

class PathTreeOperation final : public IRepairOperation {
public:
    std::string_view name() const noexcept override { return "path_tree"; }
    std::uint64_t code() const noexcept override { return 5; }

    RepairOperationResult run(OperationEnv& env, const RepairRequest& req,
                              const RepairService::ProgressFn& progress,
                              std::atomic<bool>* cancelRequested) override {
        (void)cancelRequested;
        return rebuildPathTree(env, req.dryRun, req.verbose, progress);
    }
};

} // namespace

std::unique_ptr<IRepairOperation> makePathTreeOperation() {
    return std::make_unique<PathTreeOperation>();
}

} // namespace yams::daemon::repair
