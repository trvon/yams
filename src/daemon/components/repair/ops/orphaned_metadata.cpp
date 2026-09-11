// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Repair operation "orphans" (wire code 2); see repair_operation.h.

#include <yams/daemon/components/repair/repair_operation.h>

#include "../repair_operations_internal.h"

#include <spdlog/spdlog.h>
#include <yams/api/content_store.h>
#include <yams/daemon/components/db_salvage.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace yams::daemon::repair {

namespace {

RepairOperationResult cleanOrphanedMetadata(OperationEnv& env, bool dryRun, bool verbose,
                                            bool removeCorrupt,
                                            RepairService::ProgressFn progress) {
    RepairOperationResult result;
    result.operation = "orphans";

    auto store = env.ctx.getContentStore ? env.ctx.getContentStore() : nullptr;
    auto meta = env.ctx.getMetadataRepo ? env.ctx.getMetadataRepo() : nullptr;
    if (!store || !meta) {
        result.message = "Store or metadata not available";
        return result;
    }

    // Phase 1: Quick-check if salvage is needed, then optionally salvage documents
    // from corrupt metadata DBs before scanning for orphans.  Skip salvage entirely
    // when all corrupt-DB documents are already present in the current DB.
    size_t salvagedCount = 0;
    bool salvageDidRun = false;
    if (!dryRun) {
        namespace fs = std::filesystem;
        fs::path dbPath = env.cfg.dataDir / "yams.db";
        if (fs::exists(dbPath)) {
            auto qc = quickCheckSalvageNeeded(env.cfg.dataDir, dbPath);
            if (qc.needsSalvage) {
                spdlog::info("[RepairService] Salvage needed: corrupt DB(s) have {} doc(s) "
                             "more than current DB ({})",
                             qc.maxCorruptCount - qc.currentDocCount, qc.currentDocCount);
                SalvageProgressFn salvageProgress;
                if (progress) {
                    salvageProgress = [&progress](const std::string& phase,
                                                  const std::string& message, uint64_t proc,
                                                  uint64_t total) {
                        RepairEvent ev;
                        ev.phase = phase;
                        ev.operation = "orphans";
                        ev.processed = proc;
                        ev.total = total;
                        ev.message = message;
                        progress(ev);
                    };
                }
                auto salvaged = salvageFromAllCorruptDbs(env.cfg.dataDir, dbPath, salvageProgress);
                salvagedCount = salvaged.combined.documentsSalvaged;
                salvageDidRun = true;
                if (salvagedCount > 0) {
                    spdlog::info("[RepairService] Salvage recovered {} document(s) "
                                 "from {} corrupt DB(s)",
                                 salvagedCount, salvaged.salvagedPaths.size());
                }
            } else {
                spdlog::info("[RepairService] Salvage not needed "
                             "(all {} docs already in current DB)",
                             qc.currentDocCount);
            }
        }
    }

    // Phase 2: Scan for orphaned metadata entries (docs without CAS blocks)
    auto docsResult = metadata::queryDocumentsByPattern(*meta, "%");
    if (!docsResult) {
        result.message = "Failed to query: " + docsResult.error().message;
        return result;
    }

    std::vector<int64_t> orphanedIds;
    for (const auto& doc : docsResult.value()) {
        auto existsResult = store->exists(doc.sha256Hash);
        if (!existsResult || !existsResult.value())
            orphanedIds.push_back(doc.id);
    }

    result.processed = docsResult.value().size();

    if (orphanedIds.empty()) {
        result.message = "No orphaned entries";
    } else if (dryRun) {
        result.skipped = orphanedIds.size();
        result.message = "Would clean " + std::to_string(orphanedIds.size()) + " orphans";
    } else {
        metadata::MetadataOpScope opScope("repair_orphan_cleanup");
        auto batchResult = meta->deleteDocumentsBatch(orphanedIds);
        if (batchResult) {
            result.succeeded = batchResult.value();
        } else {
            for (int64_t id : orphanedIds) {
                auto del = meta->deleteDocument(id);
                if (del)
                    result.succeeded++;
                else
                    result.failed++;
            }
        }
        result.message = "Cleaned " + std::to_string(result.succeeded) + " orphans";
    }

    if (salvagedCount > 0) {
        result.message +=
            ", salvaged " + std::to_string(salvagedCount) + " documents from corrupt DBs";
    }

    // Phase 3: If requested, remove the corrupt DB files now that salvage is complete
    if (!dryRun && removeCorrupt && salvageDidRun) {
        auto cleanup = removeCorruptDbFiles(env.cfg.dataDir);
        if (!cleanup.removed.empty()) {
            spdlog::info("[RepairService] Removed {} corrupt DB file(s)", cleanup.removed.size());
            result.message +=
                ", removed " + std::to_string(cleanup.removed.size()) + " corrupt DB file(s)";
        }
        for (const auto& err : cleanup.errors) {
            spdlog::warn("[RepairService] Corrupt DB removal error: {}", err);
        }
    }

    return result;
}

class OrphanedMetadataOperation final : public IRepairOperation {
public:
    std::string_view name() const noexcept override { return "orphans"; }
    std::uint64_t code() const noexcept override { return 2; }

    RepairOperationResult run(OperationEnv& env, const RepairRequest& req,
                              const RepairService::ProgressFn& progress,
                              std::atomic<bool>* cancelRequested) override {
        (void)cancelRequested;
        return cleanOrphanedMetadata(env, req.dryRun, req.verbose, req.removeCorrupt, progress);
    }
};

} // namespace

std::unique_ptr<IRepairOperation> makeOrphanedMetadataOperation() {
    return std::make_unique<OrphanedMetadataOperation>();
}

} // namespace yams::daemon::repair
