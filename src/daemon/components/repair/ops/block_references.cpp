// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Repair operation "block_refs" (wire code 8); see repair_operation.h.

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

RepairOperationResult repairBlockReferences(OperationEnv& env, bool dryRun, bool verbose,
                                            const RepairService::ProgressFn& progress) {
    RepairOperationResult result;
    result.operation = "block_refs";

    namespace fs = std::filesystem;
    fs::path objectsPath = env.cfg.dataDir / "storage" / "objects";
    fs::path refsDbPath = env.cfg.dataDir / "storage" / "refs.db";

    if (!fs::exists(objectsPath) || !fs::exists(refsDbPath)) {
        result.message = "No objects directory or refs.db";
        return result;
    }

    auto repairResult = integrity::RepairManager::repairBlockReferences(
        objectsPath, refsDbPath, dryRun, [&progress](uint64_t processed, uint64_t total) {
            if (!progress)
                return;
            RepairEvent ev;
            ev.phase = "repairing";
            ev.operation = "block_refs";
            ev.processed = processed;
            ev.total = total;
            ev.message = "Scanning block references";
            progress(ev);
        });
    if (!repairResult) {
        result.message = "Block refs repair failed: " + repairResult.error().message;
        result.failed = 1;
        return result;
    }

    const auto& stats = repairResult.value();
    result.processed = stats.blocksScanned;
    result.succeeded = stats.blocksUpdated;
    result.skipped = stats.blocksSkipped;
    result.failed = stats.errors;
    result.message = "Scanned " + std::to_string(stats.blocksScanned) + ", updated " +
                     std::to_string(stats.blocksUpdated);
    return result;
}

class BlockReferencesOperation final : public IRepairOperation {
public:
    std::string_view name() const noexcept override { return "block_refs"; }
    std::uint64_t code() const noexcept override { return 8; }

    RepairOperationResult run(OperationEnv& env, const RepairRequest& req,
                              const RepairService::ProgressFn& progress,
                              std::atomic<bool>* cancelRequested) override {
        (void)cancelRequested;
        return repairBlockReferences(env, req.dryRun, req.verbose, progress);
    }
};

} // namespace

std::unique_ptr<IRepairOperation> makeBlockReferencesOperation() {
    return std::make_unique<BlockReferencesOperation>();
}

} // namespace yams::daemon::repair
