// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Repair operation "stuck_docs" (wire code 1); see repair_operation.h.

#include <yams/daemon/components/repair/repair_operation.h>

#include "../repair_operation_support.h"
#include "../repair_operations_internal.h"

#include <spdlog/spdlog.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/MetadataWriteFacade.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningManager.h>
#include <yams/metadata/metadata_repository.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace yams::daemon::repair {

namespace {

struct StuckDocumentInfo {
    enum Category { FailedExtraction, GhostSuccess, StalledPending, StalledProcessing };
    Category category;
    int64_t docId{0};
    std::string hash;
    std::string path;
    int repairAttempts{0};
};

std::vector<StuckDocumentInfo> detectStuckDocuments(OperationEnv& env, int32_t maxRetries) {
    std::vector<StuckDocumentInfo> stuck;
    auto meta = env.ctx.getMetadataRepo ? env.ctx.getMetadataRepo() : nullptr;
    if (!meta)
        return stuck;

    auto now = std::chrono::system_clock::now();
    auto threshold = std::chrono::duration_cast<std::chrono::seconds>(env.cfg.stalledThreshold);
    auto cutoffEpoch =
        std::chrono::duration_cast<std::chrono::seconds>((now - threshold).time_since_epoch())
            .count();

    // Use targeted SQL queries for each stuck category instead of loading all docs.

    // Category 1: Failed extraction
    {
        metadata::DocumentQueryOptions opts;
        opts.extractionStatuses = {metadata::ExtractionStatus::Failed};
        opts.maxRepairAttempts = maxRetries;

        auto docsResult = meta->queryDocuments(opts);
        if (docsResult) {
            for (const auto& d : docsResult.value()) {
                stuck.push_back({StuckDocumentInfo::FailedExtraction, d.id, d.sha256Hash,
                                 d.filePath, d.repairAttempts});
            }
        }
    }

    // Category 2: Ghost success (Success but no content row)
    {
        metadata::DocumentQueryOptions opts;
        opts.extractionStatuses = {metadata::ExtractionStatus::Success};
        opts.maxRepairAttempts = maxRetries;
        opts.onlyMissingContent = true;

        auto docsResult = meta->queryDocuments(opts);
        if (docsResult) {
            for (const auto& d : docsResult.value()) {
                stuck.push_back({StuckDocumentInfo::GhostSuccess, d.id, d.sha256Hash, d.filePath,
                                 d.repairAttempts});
            }
        }
    }

    // Category 3: Stalled Pending (indexed/modified before cutoff)
    {
        metadata::DocumentQueryOptions opts;
        opts.extractionStatuses = {metadata::ExtractionStatus::Pending};
        opts.maxRepairAttempts = maxRetries;
        opts.stalledBefore = cutoffEpoch;

        auto docsResult = meta->queryDocuments(opts);
        if (docsResult) {
            for (const auto& d : docsResult.value()) {
                stuck.push_back({StuckDocumentInfo::StalledPending, d.id, d.sha256Hash, d.filePath,
                                 d.repairAttempts});
            }
        }
    }

    // Category 4: Stalled Processing (repairStatus stuck in Processing)
    {
        metadata::DocumentQueryOptions opts;
        opts.repairStatuses = {metadata::RepairStatus::Processing};
        opts.maxRepairAttempts = maxRetries;
        opts.repairAttemptedBefore = cutoffEpoch;

        auto docsResult = meta->queryDocuments(opts);
        if (docsResult) {
            for (const auto& d : docsResult.value()) {
                stuck.push_back({StuckDocumentInfo::StalledProcessing, d.id, d.sha256Hash,
                                 d.filePath, d.repairAttempts});
            }
        }
    }

    return stuck;
}

boost::asio::awaitable<RepairOperationResult>
recoverStuckDocumentsAsync(OperationEnv& env, const RepairRequest& req,
                           const RepairService::ProgressFn& progress,
                           std::atomic<bool>* cancelRequested) {
    RepairOperationResult result;
    result.operation = "stuck_docs";

    auto isCanceled = [&]() {
        return cancelRequested && cancelRequested->load(std::memory_order_relaxed);
    };

    auto meta = env.ctx.getMetadataRepo ? env.ctx.getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata repository not available";
        co_return result;
    }

    auto* wc = env.ctx.getWriteCoordinator ? env.ctx.getWriteCoordinator() : nullptr;
    MetadataWriteFacade metaFacade(wc, meta.get());

    auto stuckDocs = detectStuckDocuments(env, req.maxRetries);
    result.processed = stuckDocs.size();

    if (stuckDocs.empty()) {
        result.message = "No stuck documents found";
        co_return result;
    }

    spdlog::info("RepairService: found {} stuck documents", stuckDocs.size());

    if (req.dryRun) {
        result.message = "Would recover " + std::to_string(stuckDocs.size()) + " stuck documents";
        result.skipped = stuckDocs.size();
        co_return result;
    }

    const std::size_t rpcCapacity = static_cast<std::size_t>(TuneAdvisor::postIngestRpcQueueMax());
    const bool useRpcChannel = (rpcCapacity > 0);
    auto postIngestChannel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            useRpcChannel ? "post_ingest_rpc" : "post_ingest",
            useRpcChannel ? rpcCapacity : std::size_t(4096));
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    for (std::size_t i = 0; i < stuckDocs.size(); ++i) {
        if (isCanceled()) {
            result.failed += (stuckDocs.size() - i);
            result.message = "Repair canceled";
            co_return result;
        }

        const auto& s = stuckDocs[i];
        InternalEventBus::PostIngestTask task;
        task.hash = s.hash;
        task.mime = "";

        const std::string jobLabel =
            std::string(useRpcChannel ? "post_ingest_rpc" : "post_ingest") + " stuck-doc " +
            std::to_string(i + 1) + "/" + std::to_string(stuckDocs.size());
        auto notifyQueueFull = [&progress]() {
            if (!progress)
                return;
            RepairEvent event;
            event.phase = "repairing";
            event.operation = "stuck_docs";
            event.message = "Waiting for post-ingest queue capacity";
            progress(event);
        };
        const bool pushed =
            co_await queueWithBackoff(postIngestChannel, std::move(task), timer, env.running,
                                      jobLabel, 1, 500, 500, isCanceled, notifyQueueFull);
        if (pushed) {
            TuningManager::notifyWakeup();
            auto docRes = meta->getDocument(s.docId);
            if (docRes && docRes.value().has_value()) {
                auto doc = docRes.value().value();
                doc.repairAttempts = s.repairAttempts + 1;
                doc.repairAttemptedAt = std::chrono::time_point_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now());
                metadata::MetadataOpScope opScope("repair_attempt_update");
                (void)meta->updateDocument(doc);
            }

            metaFacade.updateExtractionStatus(s.docId, false, metadata::ExtractionStatus::Pending,
                                              "RepairService: recovery attempt");
            submitRepairStatusUpdate(env.ctx, meta, std::vector<std::string>{s.hash},
                                     metadata::RepairStatus::Pending,
                                     "RepairService::recovery/single");
            ++result.succeeded;
        } else if (isCanceled()) {
            result.failed += (stuckDocs.size() - i);
            result.message = "Repair canceled";
            co_return result;
        } else {
            ++result.failed;
            spdlog::warn("RepairService: PostIngest channel full after retries, could not "
                         "re-enqueue {} (consider increasing YAMS_POST_INGEST_RPC_QUEUE_MAX)",
                         s.hash.substr(0, 12));
        }

        if (progress && ((i + 1) % 10 == 0 || i + 1 == stuckDocs.size())) {
            RepairEvent ev;
            ev.phase = "repairing";
            ev.operation = "stuck_docs";
            ev.processed = i + 1;
            ev.total = stuckDocs.size();
            ev.succeeded = result.succeeded;
            ev.failed = result.failed;
            progress(ev);
        }
    }
    metaFacade.flush();

    result.message = "Recovered " + std::to_string(result.succeeded) + " stuck documents";
    if (result.failed > 0) {
        result.message += " (" + std::to_string(result.failed) +
                          " could not be enqueued - re-run repair to retry)";
    }
    co_return result;
}

RepairOperationResult recoverStuckDocuments(OperationEnv& env, const RepairRequest& req,
                                            const RepairService::ProgressFn& progress) {
    RepairOperationResult result;
    result.operation = "stuck_docs";

    auto meta = env.ctx.getMetadataRepo ? env.ctx.getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata repository not available";
        return result;
    }

    auto* wc = env.ctx.getWriteCoordinator ? env.ctx.getWriteCoordinator() : nullptr;
    MetadataWriteFacade metaFacade(wc, meta.get());

    auto stuckDocs = detectStuckDocuments(env, req.maxRetries);
    result.processed = stuckDocs.size();

    if (stuckDocs.empty()) {
        result.message = "No stuck documents found";
        return result;
    }

    spdlog::info("RepairService: found {} stuck documents", stuckDocs.size());

    if (req.dryRun) {
        result.message = "Would recover " + std::to_string(stuckDocs.size()) + " stuck documents";
        result.skipped = stuckDocs.size();
        return result;
    }

    // Re-enqueue stuck documents for re-extraction via a high-priority PostIngestQueue channel.
    // Use back-pressure retries with drain polling to avoid channel overflow and silent doc loss.
    const std::size_t rpcCapacity = static_cast<std::size_t>(TuneAdvisor::postIngestRpcQueueMax());
    const bool useRpcChannel = (rpcCapacity > 0);
    auto postIngestChannel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            useRpcChannel ? "post_ingest_rpc" : "post_ingest",
            useRpcChannel ? rpcCapacity : std::size_t(4096));

    constexpr int kMaxRetryRounds = 20; // 20 rounds × 500ms = 10s max wait per document
    constexpr auto kDrainPollInterval = std::chrono::milliseconds(500);

    for (size_t i = 0; i < stuckDocs.size(); ++i) {
        const auto& s = stuckDocs[i];

        // Re-enqueue for extraction (do not mutate metadata unless enqueue succeeds).
        InternalEventBus::PostIngestTask task;
        task.hash = s.hash;
        task.mime = ""; // will be re-detected

        // Attempt the push with back-pressure retries
        bool pushed = postIngestChannel->try_push(task);
        if (!pushed) {
            // Channel full — wait for consumer to drain, then retry
            for (int retry = 0; retry < kMaxRetryRounds && !pushed; ++retry) {
                std::this_thread::sleep_for(kDrainPollInterval);
                pushed = postIngestChannel->try_push(task);
            }
        }

        if (pushed) {
            TuningManager::notifyWakeup();
            // Increment repair attempts
            auto docRes = meta->getDocument(s.docId);
            if (docRes && docRes.value().has_value()) {
                auto doc = docRes.value().value();
                doc.repairAttempts = s.repairAttempts + 1;
                doc.repairAttemptedAt = std::chrono::time_point_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now());
                metadata::MetadataOpScope opScope("repair_attempt_update");
                (void)meta->updateDocument(doc);
            }

            // Reset extraction status to Pending
            metaFacade.updateExtractionStatus(s.docId, false, metadata::ExtractionStatus::Pending,
                                              "RepairService: recovery attempt");

            submitRepairStatusUpdate(env.ctx, meta, std::vector<std::string>{s.hash},
                                     metadata::RepairStatus::Pending,
                                     "RepairService::repairAttempt/reset");

            ++result.succeeded;
        } else {
            ++result.failed;
            spdlog::warn("RepairService: PostIngest channel full after retries, could not "
                         "re-enqueue {} (consider increasing YAMS_POST_INGEST_RPC_QUEUE_MAX)",
                         s.hash.substr(0, 12));
        }

        if (progress && (i + 1) % 10 == 0) {
            RepairEvent ev;
            ev.phase = "repairing";
            ev.operation = "stuck_docs";
            ev.processed = i + 1;
            ev.total = stuckDocs.size();
            ev.succeeded = result.succeeded;
            ev.failed = result.failed;
            progress(ev);
        }
    }

    result.message = "Recovered " + std::to_string(result.succeeded) + " stuck documents";
    if (result.failed > 0) {
        result.message += " (" + std::to_string(result.failed) +
                          " could not be enqueued — re-run repair to retry)";
    }
    metaFacade.flush();
    return result;
}

class StuckDocumentsOperation final : public IRepairOperation {
public:
    std::string_view name() const noexcept override { return "stuck_docs"; }
    std::uint64_t code() const noexcept override { return 1; }

    RepairOperationResult run(OperationEnv& env, const RepairRequest& req,
                              const RepairService::ProgressFn& progress,
                              std::atomic<bool>* cancelRequested) override {
        (void)cancelRequested;
        return recoverStuckDocuments(env, req, progress);
    }

    boost::asio::awaitable<RepairOperationResult>
    runAsync(OperationEnv& env, const RepairRequest& req, const RepairService::ProgressFn& progress,
             std::atomic<bool>* cancelRequested) override {
        co_return co_await recoverStuckDocumentsAsync(env, req, progress, cancelRequested);
    }
};

} // namespace

std::unique_ptr<IRepairOperation> makeStuckDocumentsOperation() {
    return std::make_unique<StuckDocumentsOperation>();
}

} // namespace yams::daemon::repair
