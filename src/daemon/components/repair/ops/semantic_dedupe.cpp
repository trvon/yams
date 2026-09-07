// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Repair operation "dedupe" (wire code 6); see repair_operation.h.

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

RepairOperationResult applySemanticDedupe(OperationEnv& env, const RepairRequest& req,
                                          const RepairService::ProgressFn& progress) {
    RepairOperationResult result;
    result.operation = "dedupe";

    auto meta = env.ctx.getMetadataRepo ? env.ctx.getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata not available";
        return result;
    }

    auto groupsResult = meta->listSemanticDuplicateGroups(1000);
    if (!groupsResult) {
        result.message =
            "Failed to load semantic duplicate groups: " + groupsResult.error().message;
        return result;
    }

    std::vector<metadata::SemanticDuplicateGroup> groups;
    groups.reserve(groupsResult.value().size());
    for (const auto& group : groupsResult.value()) {
        if (group.status == "suggested") {
            groups.push_back(group);
        }
    }

    result.processed = groups.size();
    if (groups.empty()) {
        result.message = "No semantic duplicate suggestions to apply";
        return result;
    }

    std::vector<int64_t> canonicalIds;
    canonicalIds.reserve(groups.size());
    for (const auto& group : groups) {
        if (group.canonicalDocumentId.has_value()) {
            canonicalIds.push_back(*group.canonicalDocumentId);
        }
    }

    auto detailsResult = meta->getSemanticDuplicateGroupsForDocuments(canonicalIds);
    if (!detailsResult) {
        result.message =
            "Failed to load semantic duplicate members: " + detailsResult.error().message;
        return result;
    }

    for (size_t i = 0; i < groups.size(); ++i) {
        const auto& group = groups[i];
        if (!group.canonicalDocumentId.has_value()) {
            result.failed++;
            continue;
        }

        auto detailIt = detailsResult.value().find(*group.canonicalDocumentId);
        if (detailIt == detailsResult.value().end()) {
            result.failed++;
            continue;
        }

        std::vector<int64_t> toDelete;
        for (const auto& member : detailIt->second.members) {
            if (member.role != "canonical") {
                toDelete.push_back(member.documentId);
            }
        }

        if (req.dryRun) {
            result.skipped += toDelete.size();
        } else {
            metadata::MetadataOpScope opScope("repair_dedupe_delete");
            if (!toDelete.empty()) {
                auto batchDelete = meta->deleteDocumentsBatch(toDelete);
                if (batchDelete) {
                    result.succeeded += batchDelete.value();
                    if (batchDelete.value() < toDelete.size()) {
                        result.failed += (toDelete.size() - batchDelete.value());
                    }
                } else {
                    for (int64_t docId : toDelete) {
                        auto del = meta->deleteDocument(docId);
                        if (del)
                            result.succeeded++;
                        else
                            result.failed++;
                    }
                }
            }

            auto statusResult = meta->updateSemanticDuplicateGroupStatus(group.groupKey, "applied");
            if (!statusResult) {
                result.failed++;
            }
        }

        if (progress) {
            RepairEvent ev;
            ev.phase = "repairing";
            ev.operation = "dedupe";
            ev.processed = i + 1;
            ev.total = groups.size();
            ev.succeeded = result.succeeded;
            ev.failed = result.failed;
            ev.skipped = result.skipped;
            ev.message = req.dryRun ? "Previewing semantic duplicate removals"
                                    : "Removing semantic duplicates";
            progress(ev);
        }
    }

    if (req.dryRun) {
        result.message =
            "Would remove " + std::to_string(result.skipped) + " semantic duplicate documents";
    } else {
        result.message =
            "Removed " + std::to_string(result.succeeded) + " semantic duplicate documents";
    }
    return result;
}

class SemanticDedupeOperation final : public IRepairOperation {
public:
    std::string_view name() const noexcept override { return "dedupe"; }
    std::uint64_t code() const noexcept override { return 6; }

    RepairOperationResult run(OperationEnv& env, const RepairRequest& req,
                              const RepairService::ProgressFn& progress,
                              std::atomic<bool>* cancelRequested) override {
        (void)cancelRequested;
        return applySemanticDedupe(env, req, progress);
    }
};

} // namespace

std::unique_ptr<IRepairOperation> makeSemanticDedupeOperation() {
    return std::make_unique<SemanticDedupeOperation>();
}

} // namespace yams::daemon::repair
