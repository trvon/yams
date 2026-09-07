// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Repair operation "fts5" (wire code 10); see repair_operation.h.

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

RepairOperationResult rebuildFts5Index(OperationEnv& env, const RepairRequest& req,
                                       const RepairService::ProgressFn& progress,
                                       std::atomic<bool>* cancelRequested) {
    RepairOperationResult result;
    result.operation = "fts5";

    auto isCanceled = [&]() {
        return cancelRequested && cancelRequested->load(std::memory_order_relaxed);
    };

    const bool dryRun = req.dryRun;
    const bool force = req.force;

    auto store = env.ctx.getContentStore ? env.ctx.getContentStore() : nullptr;
    auto meta = env.ctx.getMetadataRepo ? env.ctx.getMetadataRepo() : nullptr;
    if (!store || !meta) {
        result.message = "Store or metadata not available";
        return result;
    }

    auto* wc = env.ctx.getWriteCoordinator ? env.ctx.getWriteCoordinator() : nullptr;
    MetadataWriteFacade metaFacade(wc, meta.get());

    auto customExtractors = env.ctx.getContentExtractors
                                ? env.ctx.getContentExtractors()
                                : std::vector<std::shared_ptr<extraction::IContentExtractor>>{};

    // Pre-load all FTS5 rowids so the incremental skip check below is O(1)
    // per document instead of one SQL round-trip each.
    std::unordered_set<int64_t> ftsRowIds;
    if (!force) {
        auto ftsResult = meta->getFts5IndexedRowIdSet();
        if (ftsResult)
            ftsRowIds = std::move(ftsResult.value());
    }

    const RepairService::RepairBudget budget{std::max<std::size_t>(1, env.cfg.maxBatch),
                                             std::chrono::milliseconds(250)};
    std::int64_t cursorDocumentId = 0;
    std::size_t docsSeen = 0;
    bool finished = false;

    // Cursor pagination avoids materializing the full corpus and gives the daemon
    // regular scheduling/progress boundaries during large FTS5 repairs.
    if (progress) {
        RepairEvent ev;
        ev.phase = "repairing";
        ev.operation = "fts5";
        ev.processed = 0;
        ev.total = 0;
        ev.message =
            "Rebuilding FTS5 index in slices (batch=" + std::to_string(budget.maxDocuments) + ")";
        progress(ev);
    }

    while (!finished) {
        if (isCanceled()) {
            result.message =
                "FTS5 rebuild canceled after scanning " + std::to_string(docsSeen) + " documents";
            return result;
        }

        RepairService::RepairSlice slice;
        slice.cursorDocumentId = cursorDocumentId;

        while (!slice.exhausted(budget)) {
            metadata::DocumentQueryOptions opts;
            opts.idGreaterThan = cursorDocumentId;
            opts.orderByIdAsc = true;
            const auto remaining = budget.maxDocuments > slice.processed
                                       ? budget.maxDocuments - slice.processed
                                       : std::size_t{1};
            opts.limit = static_cast<int>(std::min<std::size_t>(remaining, 1024));

            auto docs = meta->queryDocuments(opts);
            if (!docs) {
                result.message = "Failed to enumerate after document id " +
                                 std::to_string(cursorDocumentId) + ": " + docs.error().message;
                result.failed++;
                return result;
            }
            if (docs.value().empty()) {
                finished = true;
                break;
            }

            for (const auto& d : docs.value()) {
                if (isCanceled()) {
                    result.message = "FTS5 rebuild canceled after scanning " +
                                     std::to_string(docsSeen) + " documents";
                    return result;
                }

                cursorDocumentId = std::max(cursorDocumentId, d.id);
                slice.cursorDocumentId = cursorDocumentId;
                ++slice.processed;
                ++docsSeen;
                ++result.processed;

                // Emit progress frequently so the streaming layer has events to send
                // (prevents client read timeouts during large rebuilds).
                if (progress && docsSeen % 25 == 0) {
                    RepairEvent ev;
                    ev.phase = "repairing";
                    ev.operation = "fts5";
                    ev.processed = docsSeen;
                    ev.total = 0;
                    ev.succeeded = result.succeeded;
                    ev.failed = result.failed;
                    ev.skipped = result.skipped;
                    ev.message = "Rebuilding FTS5 index (scanned " + std::to_string(docsSeen) + ")";
                    progress(ev);
                }

                auto extractionStatus = d.extractionStatus;
                bool successHasContent = false;
                if (extractionStatus == metadata::ExtractionStatus::Success) {
                    auto contentRes = meta->getContent(d.id);
                    successHasContent = contentRes && contentRes.value().has_value();
                    if (!successHasContent) {
                        metaFacade.updateExtractionStatus(d.id, false,
                                                          metadata::ExtractionStatus::Pending,
                                                          "Missing content row; reset by repair");
                        extractionStatus = metadata::ExtractionStatus::Pending;
                    }
                }

                const std::string extension = normalizedRepairExtension(d);

                // Re-detect MIME for unhelpful or clearly wrong types.
                std::string effectiveMime = d.mimeType;
                if (shouldRedetectMime(d)) {
                    effectiveMime = bestEffortMimeForDocument(d, store);
                    auto updated = d;
                    updated.mimeType = effectiveMime;
                    metadata::MetadataOpScope opScope("repair_mime_update");
                    (void)meta->updateDocument(updated);
                }

                if (dryRun) {
                    result.skipped++;
                    continue;
                }

                // Incremental mode: skip documents that already have successful extraction
                // with a valid content row AND a matching FTS5 index entry. Use --force to
                // unconditionally rebuild everything.
                if (!force && extractionStatus == metadata::ExtractionStatus::Success &&
                    successHasContent && ftsRowIds.count(d.id)) {
                    result.skipped++;
                    continue;
                }

                try {
                    auto extractedOpt = yams::extraction::util::extractDocumentText(
                        store, d.sha256Hash, effectiveMime, extension, customExtractors);
                    if (extractedOpt && !extractedOpt->empty()) {
                        metaFacade.updateExtractionStatus(d.id, false,
                                                          metadata::ExtractionStatus::Pending,
                                                          "repair fts5 processing");

                        metadata::DocumentContent content;
                        content.documentId = d.id;
                        if (extractedOpt->size() > kMaxTextToPersistInMetadataBytes) {
                            content.contentText =
                                extractedOpt->substr(0, kMaxTextToPersistInMetadataBytes);
                        } else {
                            content.contentText = *extractedOpt;
                        }
                        content.extractionMethod = "repair";
                        auto contentResult = meta->insertContent(content);

                        // Cap text for FTS5 indexing too — SQLite cannot bind strings > ~2 GB
                        // and very large documents cause "string or blob too big" errors.
                        const auto& textForIndex =
                            (extractedOpt->size() > kMaxTextToPersistInMetadataBytes)
                                ? content.contentText
                                : *extractedOpt;
                        auto ir = meta->indexDocumentContent(d.id, d.fileName, textForIndex,
                                                             effectiveMime);

                        if (ir && contentResult) {
                            metaFacade.updateExtractionStatus(
                                d.id, true, metadata::ExtractionStatus::Success, "");
                            result.succeeded++;
                        } else {
                            static const std::string kUnknownFailure = "unknown";
                            const std::string& failMsg =
                                !contentResult ? contentResult.error().message
                                               : (!ir ? ir.error().message : kUnknownFailure);
                            metaFacade.updateExtractionStatus(
                                d.id, false, metadata::ExtractionStatus::Failed, failMsg);
                            result.failed++;
                        }
                    } else {
                        metaFacade.updateExtractionStatus(d.id, false,
                                                          metadata::ExtractionStatus::Skipped,
                                                          "No extractable text");
                        result.skipped++;
                    }
                } catch (const std::exception& e) {
                    metaFacade.updateExtractionStatus(d.id, false,
                                                      metadata::ExtractionStatus::Failed, e.what());
                    result.failed++;
                }

                if (slice.exhausted(budget))
                    break;
            }

            if (docs.value().size() < static_cast<std::size_t>(opts.limit)) {
                finished = true;
                break;
            }
        }

        if (progress && slice.processed > 0) {
            RepairEvent ev;
            ev.phase = "repairing";
            ev.operation = "fts5";
            ev.processed = docsSeen;
            ev.total = 0;
            ev.succeeded = result.succeeded;
            ev.failed = result.failed;
            ev.skipped = result.skipped;
            ev.message = "Completed FTS5 repair slice through document id " +
                         std::to_string(slice.cursorDocumentId);
            progress(ev);
        }
        std::this_thread::yield();
    }

    result.message = "FTS5 rebuild: " + std::to_string(result.succeeded) + " ok, " +
                     std::to_string(result.failed) + " failed, " + std::to_string(result.skipped) +
                     " skipped";
    metaFacade.flush();
    return result;
}

class Fts5IndexOperation final : public IRepairOperation {
public:
    std::string_view name() const noexcept override { return "fts5"; }
    std::uint64_t code() const noexcept override { return 10; }

    RepairOperationResult run(OperationEnv& env, const RepairRequest& req,
                              const RepairService::ProgressFn& progress,
                              std::atomic<bool>* cancelRequested) override {
        return rebuildFts5Index(env, req, progress, cancelRequested);
    }
};

} // namespace

std::unique_ptr<IRepairOperation> makeFts5IndexOperation() {
    return std::make_unique<Fts5IndexOperation>();
}

} // namespace yams::daemon::repair
