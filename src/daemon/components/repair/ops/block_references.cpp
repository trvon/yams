// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Repair operation "block_refs" (wire code 8); see repair_operation.h.

#include <yams/daemon/components/repair/repair_operation.h>

#include "../repair_operations_internal.h"

#include <yams/integrity/repair_manager.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>

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
