// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Repair operation "mime" (wire code 3); see repair_operation.h.

#include <yams/daemon/components/repair/repair_operation.h>

#include "../repair_operation_support.h"
#include "../repair_operations_internal.h"

#include <yams/daemon/components/MetadataWriteFacade.h>
#include <yams/detection/file_type_detector.h>
#include <yams/metadata/metadata_repository.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace yams::daemon::repair {

namespace {

RepairOperationResult repairMimeTypes(OperationEnv& env, bool dryRun, bool verbose,
                                      RepairService::ProgressFn progress) {
    RepairOperationResult result;
    result.operation = "mime";

    auto meta = env.ctx.getMetadataRepo ? env.ctx.getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata not available";
        return result;
    }

    auto* wc = env.ctx.getWriteCoordinator ? env.ctx.getWriteCoordinator() : nullptr;
    MetadataWriteFacade metaFacade(wc, meta.get());

    auto store = env.ctx.getContentStore ? env.ctx.getContentStore() : nullptr;
    (void)detection::FileTypeDetector::initializeWithMagicNumbers();
    auto& detector = detection::FileTypeDetector::instance();

    constexpr int kBatchSize = 5000;
    constexpr uint64_t kProgressStride = 100;
    const auto emit = [&](std::string phase, uint64_t processed, const std::string& message) {
        if (!progress)
            return;
        RepairEvent ev;
        ev.phase = std::move(phase);
        ev.operation = "mime";
        ev.processed = processed;
        ev.succeeded = result.succeeded;
        ev.failed = result.failed;
        ev.skipped = result.skipped;
        ev.message = message;
        progress(ev);
    };

    uint64_t totalScanned = 0;
    uint64_t totalCandidates = 0;
    int offset = 0;
    while (true) {
        metadata::DocumentQueryOptions opts;
        opts.limit = kBatchSize;
        opts.offset = offset;
        auto batchResult = meta->queryDocuments(opts);
        if (!batchResult) {
            result.message = "Failed to query: " + batchResult.error().message;
            return result;
        }
        const auto& batch = batchResult.value();
        if (batch.empty())
            break;

        std::vector<std::pair<int64_t, std::string>> toRepair;
        toRepair.reserve(batch.size());
        for (const auto& doc : batch) {
            ++totalScanned;
            if (shouldRedetectMime(doc)) {
                const auto detectedMime = bestEffortMimeForDocument(doc, store);
                if (!detectedMime.empty() && detectedMime != doc.mimeType) {
                    toRepair.push_back({doc.id, detectedMime});
                    ++totalCandidates;
                }
            }
            if (totalScanned % kProgressStride == 0) {
                emit("repairing", totalScanned, "Scanning documents");
            }
        }

        if (dryRun) {
            result.skipped += toRepair.size();
        } else {
            for (auto& [id, mimeType] : toRepair) {
                auto docResult = meta->getDocument(id);
                if (!(docResult && docResult.value())) {
                    result.failed++;
                    continue;
                }

                auto doc = *docResult.value();
                const bool oldWasText = detector.isTextMimeType(doc.mimeType);
                const bool newIsText = detector.isTextMimeType(mimeType);
                doc.mimeType = std::move(mimeType);

                metadata::MetadataOpScope opScope("repair_mime_update");
                if (meta->updateDocument(doc)) {
                    if (oldWasText && !newIsText) {
                        (void)meta->deleteContent(id);
                        (void)meta->removeFromIndex(id);
                        metaFacade.updateExtractionStatus(
                            id, false, metadata::ExtractionStatus::Pending,
                            "MIME repaired; stale text content cleared");
                    }
                    result.succeeded++;
                } else {
                    result.failed++;
                }
                if ((result.succeeded + result.failed) % kProgressStride == 0) {
                    emit("repairing", totalScanned, "Repairing MIME types");
                }
            }
        }

        emit("repairing", totalScanned, "Processed batch");

        if (static_cast<int>(batch.size()) < kBatchSize)
            break;
        offset += kBatchSize;
    }

    result.processed = totalScanned;

    if (totalCandidates == 0) {
        result.message = "All documents have valid MIME types";
    } else if (dryRun) {
        result.message = "Would repair " + std::to_string(result.skipped) + " MIME types";
    } else {
        result.message = "Repaired " + std::to_string(result.succeeded) + " MIME types";
    }
    metaFacade.flush();
    return result;
}

class MimeTypesOperation final : public IRepairOperation {
public:
    std::string_view name() const noexcept override { return "mime"; }
    std::uint64_t code() const noexcept override { return 3; }

    RepairOperationResult run(OperationEnv& env, const RepairRequest& req,
                              const RepairService::ProgressFn& progress,
                              std::atomic<bool>* cancelRequested) override {
        (void)cancelRequested;
        return repairMimeTypes(env, req.dryRun, req.verbose, progress);
    }
};

} // namespace

std::unique_ptr<IRepairOperation> makeMimeTypesOperation() {
    return std::make_unique<MimeTypesOperation>();
}

} // namespace yams::daemon::repair
