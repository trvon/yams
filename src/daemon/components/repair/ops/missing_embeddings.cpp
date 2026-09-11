// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Repair operation "embeddings" (wire code 11); see repair_operation.h.

#include <yams/daemon/components/repair/repair_operation.h>

#include "../repair_operation_support.h"
#include "../repair_operations_internal.h"

#include <spdlog/spdlog.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningManager.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/repair/embedding_repair_util.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace yams::daemon::repair {

namespace {

boost::asio::awaitable<RepairOperationResult>
generateMissingEmbeddingsAsync(OperationEnv& env, const RepairRequest& req,
                               const RepairService::ProgressFn& progress,
                               std::atomic<bool>* cancelRequested) {
    RepairOperationResult result;
    result.operation = "embeddings";

    auto isCanceled = [&]() {
        return cancelRequested && cancelRequested->load(std::memory_order_relaxed);
    };

    auto meta = env.ctx.getMetadataRepo ? env.ctx.getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata not available";
        co_return result;
    }

    if (progress) {
        RepairEvent ev;
        ev.phase = "repairing";
        ev.operation = "embeddings";
        ev.message = "Scanning metadata candidates for missing embeddings";
        progress(ev);
    }

    auto candidateScanResult =
        ::yams::repair::selectEmbeddingRepairCandidates(*meta, req.includeMime, req.force);
    if (!candidateScanResult) {
        result.message = "Failed to query";
        co_return result;
    }
    auto candidateScan = std::move(candidateScanResult.value());
    const auto& hashes = candidateScan.documentHashes;

    spdlog::info("RepairService::generateMissingEmbeddingsAsync candidates: scanned={} eligible={} "
                 "eligible_by_mime={} eligible_by_extracted_text={} excluded_samples=[{}] "
                 "force={} missing_only_query={}",
                 candidateScan.documentsScanned, hashes.size(), candidateScan.eligibleByMime,
                 candidateScan.eligibleByExtractedText, candidateScan.excludedSamples.size(),
                 req.force ? 1 : 0, req.force ? 0 : 1);

    if (progress) {
        RepairEvent ev;
        ev.phase = "repairing";
        ev.operation = "embeddings";
        ev.total = hashes.size();
        ev.message =
            "Found " + std::to_string(hashes.size()) + " eligible documents missing embeddings";
        progress(ev);
    }

    result.processed = hashes.size();
    result.skipped = 0;

    if (hashes.empty()) {
        result.message = "No eligible documents";
        co_return result;
    }

    if (req.dryRun) {
        result.skipped += hashes.size();
        result.message = "Would generate embeddings for " + std::to_string(hashes.size()) + " docs";
        co_return result;
    }

    std::string modelName = req.embeddingModel;
    if (modelName.empty() && env.ctx.resolvePreferredModel) {
        try {
            modelName = env.ctx.resolvePreferredModel();
        } catch (const std::exception& e) {
            spdlog::debug("RepairService: resolvePreferredModel failed: {}", e.what());
        } catch (...) {
            spdlog::debug("RepairService: resolvePreferredModel failed");
        }
    }
    if (modelName.empty() && env.ctx.getEmbeddingModelName) {
        try {
            modelName = env.ctx.getEmbeddingModelName();
        } catch (const std::exception& e) {
            spdlog::debug("RepairService: getEmbeddingModelName failed: {}", e.what());
        } catch (...) {
            spdlog::debug("RepairService: getEmbeddingModelName failed");
        }
    }
    if (isCanceled()) {
        result.failed = hashes.size();
        result.message = "Repair canceled";
        co_return result;
    }

    const uint32_t embedCap = TuneAdvisor::embedChannelCapacity();
    auto embedQ = InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
        "embed_jobs", embedCap);
    const std::size_t batchSize = std::max<std::size_t>(1u, env.cfg.maxBatch);
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    for (std::size_t i = 0; i < hashes.size(); i += batchSize) {
        if (isCanceled()) {
            result.failed += (hashes.size() - i);
            result.message = "Repair canceled";
            co_return result;
        }

        const std::size_t end = std::min(i + batchSize, hashes.size());
        std::vector<std::string> batch(hashes.begin() + static_cast<std::ptrdiff_t>(i),
                                       hashes.begin() + static_cast<std::ptrdiff_t>(end));
        const std::size_t batchIndex = (i / batchSize) + 1;
        const std::size_t totalBatches = (hashes.size() + batchSize - 1) / batchSize;

        submitRepairStatusUpdate(env.ctx, meta, batch, metadata::RepairStatus::Processing,
                                 "RepairService::executeBatch/processing");

        InternalEventBus::EmbedJob job{
            batch,
            static_cast<uint32_t>(batch.size()), // nosemgrep: yams.cpp.size-to-u32-cast (batch is
                                                 // capped by cfg.maxBatch)
            !req.force, modelName, std::vector<InternalEventBus::EmbedPreparedDoc>{}, nullptr};
        // Batch semantic graph maintenance outside the per-job embedding hot path.
        // Per-job corpus scans cause large transient heap spikes during repair.
        job.updateSemanticGraph = false;

        const std::string jobLabel =
            "embed batch " + std::to_string(batchIndex) + "/" + std::to_string(totalBatches);
        const bool queued = co_await queueWithBackoff(embedQ, std::move(job), timer, env.running,
                                                      jobLabel, batch.size(), 50, 1000, isCanceled);
        if (!queued) {
            submitRepairStatusUpdate(env.ctx, meta, batch, metadata::RepairStatus::Pending,
                                     "RepairService::executeBatch/rollback");
            InternalEventBus::instance().incEmbedDropped(batch.size());
            if (isCanceled()) {
                result.failed += (hashes.size() - i);
                result.message = "Repair canceled";
                co_return result;
            }
            result.failed += batch.size();
            continue;
        }

        InternalEventBus::instance().incEmbedQueued(batch.size());
        TuningManager::notifyWakeup();
        result.succeeded += batch.size();
        result.processed = result.succeeded + result.failed + result.skipped;

        if (progress) {
            RepairEvent ev;
            ev.phase = "repairing";
            ev.operation = "embeddings";
            ev.processed = std::min(end, hashes.size());
            ev.total = hashes.size();
            ev.succeeded = result.succeeded;
            ev.failed = result.failed;
            ev.skipped = result.skipped;
            ev.message = "queued daemon embed batch " + std::to_string(batchIndex) + "/" +
                         std::to_string(totalBatches);
            progress(ev);
        }
    }

    result.message = "Queued " + std::to_string(result.succeeded) + " docs for embedding";
    co_return result;
}

RepairOperationResult generateMissingEmbeddings(OperationEnv& env, const RepairRequest& req,
                                                const RepairService::ProgressFn& progress,
                                                std::atomic<bool>* cancelRequested) {
    RepairOperationResult result;
    result.operation = "embeddings";

    auto isCanceled = [&]() {
        return cancelRequested && cancelRequested->load(std::memory_order_relaxed);
    };

    auto meta = env.ctx.getMetadataRepo ? env.ctx.getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata not available";
        return result;
    }

    if (progress) {
        RepairEvent ev;
        ev.phase = "repairing";
        ev.operation = "embeddings";
        ev.message = "Scanning metadata candidates for missing embeddings";
        progress(ev);
    }

    auto candidateScanResult =
        ::yams::repair::selectEmbeddingRepairCandidates(*meta, req.includeMime, req.force);
    if (!candidateScanResult) {
        result.message = "Failed to query";
        return result;
    }
    auto candidateScan = std::move(candidateScanResult.value());
    const auto& hashes = candidateScan.documentHashes;

    spdlog::info("RepairService::generateMissingEmbeddings candidates: scanned={} eligible={} "
                 "eligible_by_mime={} eligible_by_extracted_text={} excluded_samples=[{}] "
                 "force={} missing_only_query={}",
                 candidateScan.documentsScanned, hashes.size(), candidateScan.eligibleByMime,
                 candidateScan.eligibleByExtractedText, candidateScan.excludedSamples.size(),
                 req.force ? 1 : 0, req.force ? 0 : 1);

    if (progress) {
        RepairEvent ev;
        ev.phase = "repairing";
        ev.operation = "embeddings";
        ev.total = hashes.size();
        ev.message =
            "Found " + std::to_string(hashes.size()) + " eligible documents missing embeddings";
        progress(ev);
    }

    size_t skippedExisting = 0;

    result.processed = hashes.size() + skippedExisting;
    result.skipped = skippedExisting;

    if (hashes.empty()) {
        if (skippedExisting > 0) {
            result.message = "All " + std::to_string(skippedExisting) +
                             " eligible documents already have embeddings";
        } else {
            result.message = "No eligible documents";
        }
        return result;
    }

    if (req.dryRun) {
        result.skipped += hashes.size();
        result.message = "Would generate embeddings for " + std::to_string(hashes.size()) + " docs";
        if (skippedExisting > 0) {
            result.message +=
                " (" + std::to_string(skippedExisting) + " already embedded, skipped)";
        }
        return result;
    }

    std::string modelName = req.embeddingModel;
    if (modelName.empty() && env.ctx.resolvePreferredModel) {
        try {
            modelName = env.ctx.resolvePreferredModel();
        } catch (const std::exception& e) {
            spdlog::debug("RepairService: resolvePreferredModel failed: {}", e.what());
        } catch (...) {
            spdlog::debug("RepairService: resolvePreferredModel failed");
        }
    }
    if (modelName.empty() && env.ctx.getEmbeddingModelName) {
        try {
            modelName = env.ctx.getEmbeddingModelName();
        } catch (const std::exception& e) {
            spdlog::debug("RepairService: getEmbeddingModelName failed: {}", e.what());
        } catch (...) {
            spdlog::debug("RepairService: getEmbeddingModelName failed");
        }
    }
    if (req.foreground && modelName.empty()) {
        result.failed = hashes.size();
        result.message = "No embedding model configured";
        return result;
    }
    if (isCanceled()) {
        result.failed = hashes.size();
        result.message = "Repair canceled";
        return result;
    }

    const uint32_t embedCap = TuneAdvisor::embedChannelCapacity();
    auto embedQ = InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
        "embed_jobs", embedCap);

    const std::size_t batchSize = std::max<std::size_t>(1u, env.cfg.maxBatch);
    const std::size_t totalDocs = hashes.size() + skippedExisting;
    const std::size_t totalBatches = (hashes.size() + batchSize - 1) / batchSize;
    std::size_t completedDocs = 0;
    std::size_t failedDocs = 0;
    std::size_t skippedDocs = skippedExisting;

    for (std::size_t i = 0; i < hashes.size(); i += batchSize) {
        if (isCanceled()) {
            result.failed += (hashes.size() - i);
            result.message = "Repair canceled";
            return result;
        }

        const std::size_t end = std::min(i + batchSize, hashes.size());
        std::vector<std::string> batch(hashes.begin() + i, hashes.begin() + end);
        const std::size_t batchIndex = i / batchSize;

        submitRepairStatusUpdate(env.ctx, meta, batch, metadata::RepairStatus::Processing,
                                 "RepairService::executeBatchMonitored/processing");

        std::shared_ptr<InternalEventBus::EmbedJobMonitor> monitor;
        if (req.foreground) {
            monitor = std::make_shared<InternalEventBus::EmbedJobMonitor>();
            monitor->totalDocs = batch.size();
            monitor->phase = "queued";
            monitor->detail = "queued daemon embed batch";
        }

        InternalEventBus::EmbedJob job{
            batch,
            static_cast<uint32_t>(batch.size()), // nosemgrep: yams.cpp.size-to-u32-cast (batch is
                                                 // capped by cfg.maxBatch)
            !req.force, modelName, std::vector<InternalEventBus::EmbedPreparedDoc>{}, monitor};
        // Batch semantic graph maintenance outside the per-job embedding hot path.
        // Per-job corpus scans cause large transient heap spikes during repair.
        job.updateSemanticGraph = false;

        int retries = 0;
        bool pushed = false;
        while (!pushed && retries < 20) {
            pushed = embedQ->try_push(job);
            if (!pushed) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(50 * (1 << std::min(retries, 5))));
                retries++;
            }
        }

        if (!pushed) {
            result.failed += batch.size();
            submitRepairStatusUpdate(env.ctx, meta, batch, metadata::RepairStatus::Pending,
                                     "RepairService::executeBatchMonitored/rollback");
            InternalEventBus::instance().incEmbedDropped();
            continue;
        }

        InternalEventBus::instance().incEmbedQueued();
        TuningManager::notifyWakeup();

        if (!req.foreground) {
            result.succeeded += batch.size();
            if (progress) {
                RepairEvent ev;
                ev.phase = "repairing";
                ev.operation = "embeddings";
                ev.processed = std::min(i + batchSize, hashes.size());
                ev.total = hashes.size();
                ev.succeeded = result.succeeded;
                ev.failed = result.failed;
                ev.skipped = result.skipped;
                progress(ev);
            }
            continue;
        }

        std::size_t lastReportedProcessed = static_cast<std::size_t>(-1);
        auto lastReport = std::chrono::steady_clock::time_point{};
        while (true) {
            if (isCanceled()) {
                if (monitor) {
                    monitor->cancelRequested.store(true, std::memory_order_relaxed);
                    monitor->cv.notify_all();
                }
                result.message = "Repair canceled";
                return result;
            }

            std::size_t batchCompleted = 0;
            std::size_t batchFailed = 0;
            std::size_t batchSkipped = 0;
            std::size_t batchProcessed = 0;
            std::string batchPhase = "queued";
            std::string batchDetail;
            bool batchDone = false;
            {
                std::unique_lock<std::mutex> lk(monitor->mutex);
                monitor->cv.wait_for(lk, std::chrono::milliseconds(250));
                batchCompleted = static_cast<std::size_t>(monitor->succeededDocs);
                batchFailed = static_cast<std::size_t>(monitor->failedDocs);
                batchSkipped = static_cast<std::size_t>(monitor->skippedDocs);
                batchProcessed = static_cast<std::size_t>(monitor->processedDocs);
                batchPhase = monitor->phase;
                batchDetail = monitor->detail;
                batchDone = monitor->done;
            }

            const auto now = std::chrono::steady_clock::now();
            if (progress && (batchProcessed != lastReportedProcessed ||
                             lastReport.time_since_epoch().count() == 0 ||
                             (now - lastReport) >= std::chrono::seconds(1))) {
                RepairEvent ev;
                ev.phase = "repairing";
                ev.operation = "embeddings";
                ev.processed = completedDocs + failedDocs + skippedDocs + batchProcessed;
                ev.total = totalDocs;
                ev.succeeded = completedDocs + batchCompleted;
                ev.failed = failedDocs + batchFailed;
                ev.skipped = skippedDocs + batchSkipped;
                if (batchDone) {
                    ev.message = "completed daemon embed batch " + std::to_string(batchIndex + 1) +
                                 "/" + std::to_string(totalBatches);
                } else {
                    ev.message = "daemon embed batch " + std::to_string(batchIndex + 1) + "/" +
                                 std::to_string(totalBatches) + " phase=" + batchPhase;
                    if (!batchDetail.empty()) {
                        ev.message += " " + batchDetail;
                    }
                }
                progress(ev);
                lastReportedProcessed = batchProcessed;
                lastReport = now;
            }

            if (batchDone) {
                completedDocs += batchCompleted;
                failedDocs += batchFailed;
                skippedDocs += batchSkipped;
                result.processed = completedDocs + failedDocs + skippedDocs;
                result.succeeded = completedDocs;
                result.failed = failedDocs;
                result.skipped = skippedDocs;
                break;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }
    }

    if (req.foreground) {
        // No explicit finalize needed — the coordinator manages the bulk window.
        if (req.repairTopology && result.succeeded > 0 && env.ctx.rebuildSemanticNeighborGraph) {
            auto semanticResult =
                env.ctx.rebuildSemanticNeighborGraph("repair_service.embedding_batch", modelName);
            if (!semanticResult) {
                spdlog::warn("RepairService: deferred semantic neighbor rebuild failed: {}",
                             semanticResult.error().message);
            } else if (progress) {
                RepairEvent ev;
                ev.phase = "repairing";
                ev.operation = "embeddings";
                ev.processed = result.processed;
                ev.total = totalDocs;
                ev.succeeded = result.succeeded;
                ev.failed = result.failed;
                ev.skipped = result.skipped;
                ev.message = "rebuilt semantic neighbor graph edges=" +
                             std::to_string(semanticResult.value());
                progress(ev);
            }
        }
        result.message = "Generated " + std::to_string(result.succeeded) + " embeddings";
        if (result.skipped > 0 || result.failed > 0) {
            result.message += ", skipped=" + std::to_string(result.skipped) +
                              ", failed=" + std::to_string(result.failed);
        }
        if (env.state) {
            env.state->stats.repairEmbeddingsGenerated.store(result.succeeded,
                                                             std::memory_order_relaxed);
            env.state->stats.repairEmbeddingsSkipped.store(result.skipped,
                                                           std::memory_order_relaxed);
            env.state->stats.repairFailedOperations.store(result.failed, std::memory_order_relaxed);
        }
    } else {
        result.message = "Queued " + std::to_string(result.succeeded) + " docs for embedding";
        if (result.skipped > 0) {
            result.message += " (" + std::to_string(result.skipped) + " already embedded, skipped)";
        }
    }
    return result;
}

class MissingEmbeddingsOperation final : public IRepairOperation {
public:
    std::string_view name() const noexcept override { return "embeddings"; }
    std::uint64_t code() const noexcept override { return 11; }

    RepairOperationResult run(OperationEnv& env, const RepairRequest& req,
                              const RepairService::ProgressFn& progress,
                              std::atomic<bool>* cancelRequested) override {
        return generateMissingEmbeddings(env, req, progress, cancelRequested);
    }

    boost::asio::awaitable<RepairOperationResult>
    runAsync(OperationEnv& env, const RepairRequest& req, const RepairService::ProgressFn& progress,
             std::atomic<bool>* cancelRequested) override {
        co_return co_await generateMissingEmbeddingsAsync(env, req, progress, cancelRequested);
    }
};

} // namespace

std::unique_ptr<IRepairOperation> makeMissingEmbeddingsOperation() {
    return std::make_unique<MissingEmbeddingsOperation>();
}

} // namespace yams::daemon::repair
