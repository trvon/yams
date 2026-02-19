// Split from RequestDispatcher.cpp: status handler
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <thread>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/dispatch_utils.hpp>
#include <yams/daemon/components/EmbeddingProviderFsm.h>
#include <yams/daemon/components/PluginHostFsm.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManagerFsm.h>
#include <yams/daemon/ipc/fsm_metrics_registry.h>
#include <yams/daemon/ipc/mux_metrics_registry.h>
#include <yams/daemon/ipc/stream_metrics_registry.h>
#include <yams/daemon/metric_keys.h>
#include <yams/storage/corpus_stats.h>
#include <yams/vector/vector_database.h>
#include <yams/version.hpp>

namespace yams::daemon {

boost::asio::awaitable<Response> RequestDispatcher::handleStatusRequest(const StatusRequest& req) {
    // Minimal and safe status path using centralized DaemonMetrics when available
    StatusResponse res;
    try {
        // Status path returns cached snapshot - NO I/O on request path
        // DaemonMetrics background thread keeps cache hot
        if (metrics_) {
            auto snap = metrics_->getSnapshot(req.detailed);
            if (!snap) {
                // Fallback: return minimal response if snapshot is null
                res.running = true;
                res.version = YAMS_VERSION_STRING;
                res.ready = false;
                res.overallStatus = "initializing";
                co_return res;
            }
            res.running = snap->running;
            res.version = snap->version;
            res.uptimeSeconds = snap->uptimeSeconds;
            res.requestsProcessed = snap->requestsProcessed;
            res.activeConnections = snap->activeConnections;
            res.maxConnections = snap->maxConnections;
            res.connectionSlotsFree = snap->connectionSlotsFree;
            res.oldestConnectionAge = snap->oldestConnectionAge;
            res.forcedCloseCount = snap->forcedCloseCount;
            res.proxyActiveConnections = snap->proxyActiveConnections;
            res.proxySocketPath = snap->proxySocketPath;
            res.memoryUsageMb = snap->memoryUsageMb;
            res.cpuUsagePercent = snap->cpuUsagePercent;
            // Resolved via DaemonMetrics: ready reflects lifecycle readiness
            res.ready = snap->ready;
            res.overallStatus = snap->overallStatus;   // normalized lowercase
            res.lifecycleState = snap->lifecycleState; // normalized lowercase
            res.lastError = snap->lastError;
            res.fsmTransitions = snap->fsmTransitions;
            res.fsmHeaderReads = snap->fsmHeaderReads;
            res.fsmPayloadReads = snap->fsmPayloadReads;
            res.fsmPayloadWrites = snap->fsmPayloadWrites;
            res.fsmBytesSent = snap->fsmBytesSent;
            res.fsmBytesReceived = snap->fsmBytesReceived;
            res.muxActiveHandlers = snap->muxActiveHandlers;
            res.muxQueuedBytes = snap->muxQueuedBytes;
            res.muxWriterBudgetBytes = snap->muxWriterBudgetBytes;
            // Pool sizes via FSM metrics (in DaemonMetrics snapshot)
            res.ipcPoolSize = snap->ipcPoolSize;
            res.ioPoolSize = snap->ioPoolSize;
            // Vector DB snapshot (best-effort)
            res.vectorDbInitAttempted = snap->vectorDbInitAttempted;
            res.vectorDbReady = snap->vectorDbReady;
            res.vectorDbDim = snap->vectorDbDim;
            res.readinessStates[std::string(readiness::kVectorDbInitAttempted)] =
                res.vectorDbInitAttempted;
            res.readinessStates[std::string(readiness::kVectorDbReady)] = res.vectorDbReady;
            res.readinessStates[std::string(readiness::kVectorDbDim)] = (res.vectorDbDim > 0);
            // Embedding runtime details (best-effort)
            res.embeddingAvailable = snap->embeddingAvailable;
            res.embeddingBackend = snap->embeddingBackend;
            res.embeddingModel = snap->embeddingModel;
            res.embeddingModelPath = snap->embeddingModelPath;
            res.embeddingDim = snap->embeddingDim;
            // Vector diagnostics (from background snapshot - no blocking)
            // Helpers to reduce binary bloat from repeated std::string construction
            auto setVal = [&](std::string_view key, size_t val) {
                res.requestCounts.emplace(key, val);
            };
            auto setReady = [&](std::string_view key, bool val) {
                res.readinessStates.emplace(key, val);
            };

            setReady(readiness::kVectorEmbeddingsAvailable, snap->vectorEmbeddingsAvailable);
            setReady(readiness::kVectorScoringEnabled, snap->vectorScoringEnabled);
            setVal(metrics::kVectorEmbeddingsAvailable, snap->vectorEmbeddingsAvailable ? 1 : 0);
            setVal(metrics::kVectorScoringEnabled, snap->vectorScoringEnabled ? 1 : 0);

            setReady(readiness::kSearchEngineBuildReasonInitial,
                     snap->searchEngineBuildReason == "initial");
            setReady(readiness::kSearchEngineBuildReasonRebuild,
                     snap->searchEngineBuildReason == "rebuild");
            setReady(readiness::kSearchEngineBuildReasonDegraded,
                     snap->searchEngineBuildReason == "degraded");

            setVal(metrics::kWorkerThreads, snap->workerThreads);
            setVal(metrics::kWorkerActive, snap->workerActive);
            setVal(metrics::kWorkerQueued, snap->workerQueued);
            setVal(metrics::kPostIngestThreads, snap->postIngestThreads);
            setVal(metrics::kPostIngestQueued, snap->postIngestQueued);
            setVal(metrics::kPostIngestRpcQueued, snap->postIngestRpcQueued);

            res.searchMetrics.active = snap->searchActive;
            res.searchMetrics.queued = snap->searchQueued;
            res.searchMetrics.executed = snap->searchExecuted;
            res.searchMetrics.cacheHitRate = snap->searchCacheHitRate;
            res.searchMetrics.avgLatencyUs = snap->searchAvgLatencyUs;
            res.searchMetrics.concurrencyLimit = snap->searchConcurrencyLimit;

            setVal(metrics::kSearchActive, snap->searchActive);
            setVal(metrics::kSearchQueued, snap->searchQueued);
            setVal(metrics::kSearchExecuted, static_cast<size_t>(snap->searchExecuted));
            setVal(metrics::kSearchCacheHitRatePct,
                   static_cast<size_t>(snap->searchCacheHitRate * 100.0));
            setVal(metrics::kSearchAvgLatencyUs, static_cast<size_t>(snap->searchAvgLatencyUs));
            setVal(metrics::kSearchConcurrencyLimit, snap->searchConcurrencyLimit);

            // PBI-040, task 040-1: Expose queue depth for FTS5 readiness checks
            res.postIngestQueueDepth = static_cast<uint32_t>(snap->postIngestQueued);

            // Deferred ingestion queue depth (adds queued under memory pressure)
            setVal(metrics::kDeferredQueueDepth, snap->deferredQueueDepth);
            setVal(metrics::kPostIngestInflight, snap->postIngestInflight);
            setVal(metrics::kPostIngestCapacity, snap->postIngestCapacity);
            setVal(metrics::kPostIngestDrained,
                   (snap->postIngestQueued == 0 && snap->postIngestInflight == 0) ? 1 : 0);

            // KG backpressure observability
            setVal(metrics::kPostIngestBackpressureRejects,
                   static_cast<size_t>(snap->postIngestBackpressureRejects));

            if (snap->kgJobsCapacity > 0) {
                setVal(metrics::kKgQueueDepth, snap->kgJobsDepth);
                setVal(metrics::kKgJobsCapacity, snap->kgJobsCapacity);
                setVal(metrics::kKgJobsFillPct,
                       static_cast<size_t>(std::clamp(snap->kgJobsFillRatio * 100.0, 0.0, 100.0)));
            }

            setVal(metrics::kPostIngestRpcCapacity, snap->postIngestRpcCapacity);
            setVal(metrics::kPostIngestRpcMaxPerBatch, snap->postIngestRpcMaxPerBatch);

            // Export selected tuning config values for clients (best-effort)
            try {
                if (serviceManager_) {
                    // Surface FSM states as numeric codes in requestCounts, and booleans in
                    // readinessStates
                    try {
                        auto ss = serviceManager_->getServiceManagerFsmSnapshot();
                        setVal(metrics::kServiceFsmState, static_cast<size_t>(ss.state));
                    } catch (...) {
                    }
                    try {
                        auto es = serviceManager_->getEmbeddingProviderFsmSnapshot();
                        setVal(metrics::kEmbeddingState, static_cast<size_t>(es.state));
                        setReady(readiness::kEmbeddingReady,
                                 es.state == EmbeddingProviderState::ModelReady);
                        // Provide an explicit degraded flag for clients/tools that
                        // distinguish readiness from degraded modes.
                        setReady(readiness::kEmbeddingDegraded,
                                 es.state == EmbeddingProviderState::Degraded);
                    } catch (...) {
                    }
                    // EmbeddingService metrics (jobs currently being processed)
                    try {
                        setVal(metrics::kEmbedInflight,
                               serviceManager_->getEmbeddingInFlightJobs());
                        setVal(metrics::kEmbedQueued, serviceManager_->getEmbeddingQueuedJobs());
                        setVal(metrics::kEmbedInferActive,
                               serviceManager_->getEmbeddingActiveInferSubBatches());
                        setVal(metrics::kEmbedInferOldestMs,
                               serviceManager_->getEmbeddingInferOldestMs());
                        setVal(metrics::kEmbedInferStarted,
                               serviceManager_->getEmbeddingInferStartedCount());
                        setVal(metrics::kEmbedInferCompleted,
                               serviceManager_->getEmbeddingInferCompletedCount());
                        setVal(metrics::kEmbedInferLastMs,
                               serviceManager_->getEmbeddingInferLastMs());
                        setVal(metrics::kEmbedInferMaxMs,
                               serviceManager_->getEmbeddingInferMaxMs());
                        setVal(metrics::kEmbedInferWarnCount,
                               serviceManager_->getEmbeddingInferWarnCount());
                    } catch (...) {
                    }
                    try {
                        auto ps = serviceManager_->getPluginHostFsmSnapshot();
                        setVal(metrics::kPluginHostState, static_cast<size_t>(ps.state));
                        setReady(readiness::kPluginsReady, ps.state == PluginHostState::Ready);
                        setReady(readiness::kPluginsDegraded, ps.state == PluginHostState::Failed);
                    } catch (...) {
                    }
                    const auto& tc = serviceManager_->getConfig().tuning;
                    setVal(metrics::kTuningPostIngestCapacity, tc.postIngestCapacity);
                    setVal(metrics::kTuningPostIngestThreadsMin, tc.postIngestThreadsMin);
                    setVal(metrics::kTuningPostIngestThreadsMax, tc.postIngestThreadsMax);
                    setVal(metrics::kTuningAdmitWarnThreshold, tc.admitWarnThreshold);
                    setVal(metrics::kTuningAdmitStopThreshold, tc.admitStopThreshold);

                    // WorkCoordinator metrics
                    setVal(metrics::kWorkCoordinatorActive, snap->workCoordinatorActiveWorkers);
                    setVal(metrics::kWorkCoordinatorRunning, snap->workCoordinatorRunning ? 1 : 0);
                }
            } catch (...) {
            }

            setVal(metrics::kPostIngestProcessed, snap->postIngestProcessed);
            setVal(metrics::kPostIngestFailed, snap->postIngestFailed);
            setVal(metrics::kPostIngestLatencyEma,
                   static_cast<size_t>(snap->postIngestLatencyMsEma));
            setVal(metrics::kPostIngestRateEma, static_cast<size_t>(snap->postIngestRateSecEma));

            setVal(metrics::kExtractionInflight, snap->extractionInFlight);

            setVal(metrics::kKgQueued, snap->kgQueued);
            setVal(metrics::kKgDropped, snap->kgDropped);
            setVal(metrics::kKgConsumed, snap->kgConsumed);
            setVal(metrics::kKgInflight, snap->kgInFlight);
            setVal(metrics::kKgQueueDepth, snap->kgQueueDepth);

            setVal(metrics::kSymbolInflight, snap->symbolInFlight);
            setVal(metrics::kSymbolQueueDepth, snap->symbolQueueDepth);

            // Entity extraction metrics (external plugins like Ghidra)
            setVal(metrics::kEntityQueued, snap->entityQueued);
            setVal(metrics::kEntityDropped, snap->entityDropped);
            setVal(metrics::kEntityConsumed, snap->entityConsumed);
            setVal(metrics::kEntityInflight, snap->entityInFlight);
            setVal(metrics::kEntityQueueDepth, snap->entityQueueDepth);

            setVal(metrics::kTitleQueueDepth, snap->titleQueueDepth);
            setVal(metrics::kTitleInflight, snap->titleInFlight);
            setVal(metrics::kPostTitleLimit, snap->titleConcurrencyLimit);
            setVal(metrics::kTitleQueued, snap->titleQueued);
            setVal(metrics::kTitleDropped, snap->titleDropped);
            setVal(metrics::kTitleConsumed, snap->titleConsumed);

            // FTS5 indexing metrics
            setVal(metrics::kFts5Queued, snap->fts5Queued);
            setVal(metrics::kFts5Dropped, snap->fts5Dropped);
            setVal(metrics::kFts5Consumed, snap->fts5Consumed);

            // Symbol extraction metrics
            setVal(metrics::kSymbolQueued, snap->symbolQueued);
            setVal(metrics::kSymbolDropped, snap->symbolDropped);
            setVal(metrics::kSymbolConsumed, snap->symbolConsumed);

            // Stream metrics
            setVal(metrics::kStreamTotal, snap->streamTotal);
            setVal(metrics::kStreamBatches, snap->streamBatches);
            setVal(metrics::kStreamKeepalives, snap->streamKeepalives);
            setVal(metrics::kStreamTtfbAvgMs, snap->streamTtfbAvgMs);

            // Repair service metrics
            setVal(metrics::kRepairQueueDepth, snap->repairQueueDepth);
            setVal(metrics::kRepairBatchesAttempted, snap->repairBatchesAttempted);
            setVal(metrics::kRepairEmbeddingsGenerated, snap->repairEmbeddingsGenerated);
            setVal(metrics::kRepairEmbeddingsSkipped, snap->repairEmbeddingsSkipped);
            setVal(metrics::kRepairFailedOperations, snap->repairFailedOperations);
            setVal(metrics::kRepairIdleTicks, snap->repairIdleTicks);
            setVal(metrics::kRepairBusyTicks, snap->repairBusyTicks);
            setVal(metrics::kRepairTotalBacklog, snap->repairTotalBacklog);
            setVal(metrics::kRepairProcessed, snap->repairProcessed);
            setVal(metrics::kRepairRunning, snap->repairRunning ? 1 : 0);
            setVal(metrics::kRepairInProgress, snap->repairInProgress ? 1 : 0);

            // File/directory add tracking
            setVal(metrics::kFilesAdded, static_cast<size_t>(snap->filesAdded));
            setVal(metrics::kDirectoriesAdded, static_cast<size_t>(snap->directoriesAdded));
            setVal(metrics::kFilesProcessed, static_cast<size_t>(snap->filesProcessed));
            setVal(metrics::kDirectoriesProcessed, static_cast<size_t>(snap->directoriesProcessed));

            // Dynamic concurrency limits (PBI-05a)
            setVal(metrics::kPostExtractionLimit, snap->postExtractionLimit);
            setVal(metrics::kPostKgLimit, snap->postKgLimit);
            setVal(metrics::kPostSymbolLimit, snap->postSymbolLimit);
            setVal(metrics::kPostEntityLimit, snap->postEntityLimit);

            // Surface whether the InternalEventBus is being used for post-ingest
            try {
                setVal(metrics::kPostIngestUseBus,
                       yams::daemon::TuneAdvisor::useInternalBusForPostIngest() ? 1 : 0);
            } catch (...) {
            }
            setVal(metrics::kPostEmbedLimit, snap->postEmbedLimit);

            // Internal bus metrics
            try {
                auto& bus = InternalEventBus::instance();
                setVal(metrics::kBusEmbedQueued, bus.embedQueued());
                setVal(metrics::kBusEmbedConsumed, bus.embedConsumed());
                setVal(metrics::kBusEmbedDropped, bus.embedDropped());
                setVal(metrics::kBusEmbedPreparedDocsQueued, bus.embedPreparedDocsQueued());
                setVal(metrics::kBusEmbedPreparedChunksQueued, bus.embedPreparedChunksQueued());
                setVal(metrics::kBusEmbedHashOnlyDocsQueued, bus.embedHashOnlyDocsQueued());
                setVal(metrics::kBusPostQueued, bus.postQueued());
                setVal(metrics::kBusPostConsumed, bus.postConsumed());
                setVal(metrics::kBusPostDropped, bus.postDropped());
            } catch (...) {
            }

            // Session watch status
            setVal(metrics::kWatchEnabled, snap->watchEnabled ? 1 : 0);
            if (snap->watchIntervalMs > 0) {
                setVal(metrics::kWatchIntervalMs, snap->watchIntervalMs);
            }
            res.retryAfterMs = snap->retryAfterMs;
            for (const auto& [k, v] : snap->readinessStates)
                res.readinessStates[k] = v;
            for (const auto& [k, v] : snap->initProgress)
                res.initProgress[k] = v;

            // Content store diagnostics
            res.contentStoreRoot = snap->contentStoreRoot;
            res.contentStoreError = snap->contentStoreError;
            // Search tuning state (from SearchTuner FSM - epic yams-7ez4)
            res.searchTuningState = snap->searchTuningState;
            res.searchTuningReason = snap->searchTuningReason;
            res.searchTuningParams = snap->searchTuningParams;
            // ResourceGovernor metrics (memory pressure management)
            res.governorRssBytes = snap->governorRssBytes;
            res.governorBudgetBytes = snap->governorBudgetBytes;
            res.governorPressureLevel = snap->governorPressureLevel;
            res.governorHeadroomPct = snap->governorHeadroomPct;
            setVal(metrics::kPressureLevel, static_cast<size_t>(snap->governorPressureLevel));

            // ONNX concurrency metrics
            res.onnxTotalSlots = snap->onnxTotalSlots;
            res.onnxUsedSlots = snap->onnxUsedSlots;
            res.onnxGlinerUsed = snap->onnxGlinerUsed;
            res.onnxEmbedUsed = snap->onnxEmbedUsed;
            res.onnxRerankerUsed = snap->onnxRerankerUsed;

            // Storage size summary (exposed via requestCounts for backwards compatible clients)
            if (snap->logicalBytes > 0)
                setVal(metrics::kStorageLogicalBytes, static_cast<size_t>(snap->logicalBytes));
            if (snap->physicalBytes > 0)
                setVal(metrics::kStoragePhysicalBytes, static_cast<size_t>(snap->physicalBytes));
            if (snap->storeObjects > 0)
                setVal(metrics::kStorageDocuments, static_cast<size_t>(snap->storeObjects));

            if (snap->logicalBytes > 0 && snap->physicalBytes > 0) {
                std::uint64_t saved = (snap->logicalBytes > snap->physicalBytes)
                                          ? (snap->logicalBytes - snap->physicalBytes)
                                          : 0ULL;
                setVal(metrics::kStorageSavedBytes, static_cast<size_t>(saved));
                std::uint64_t pct =
                    snap->logicalBytes ? (saved * 100ULL) / snap->logicalBytes : 0ULL;
                setVal(metrics::kStorageSavedPct, static_cast<size_t>(pct));
            } else {
                // Avoid signaling 100% savings when physical is unknown
                res.requestCounts.erase(std::string(metrics::kStorageSavedBytes));
                res.requestCounts.erase(std::string(metrics::kStorageSavedPct));
            }

            // New: detailed storage breakdown (when available)
            if (snap->casPhysicalBytes > 0)
                setVal(metrics::kCasPhysicalBytes, static_cast<size_t>(snap->casPhysicalBytes));
            if (snap->casUniqueRawBytes > 0)
                setVal(metrics::kCasUniqueRawBytes, static_cast<size_t>(snap->casUniqueRawBytes));
            if (snap->casDedupSavedBytes > 0)
                setVal(metrics::kCasDedupSavedBytes, static_cast<size_t>(snap->casDedupSavedBytes));
            if (snap->casCompressSavedBytes > 0)
                setVal(metrics::kCasCompressSavedBytes,
                       static_cast<size_t>(snap->casCompressSavedBytes));
            if (snap->metadataPhysicalBytes > 0)
                setVal(metrics::kMetadataPhysicalBytes,
                       static_cast<size_t>(snap->metadataPhysicalBytes));
            if (snap->indexPhysicalBytes > 0)
                setVal(metrics::kIndexPhysicalBytes, static_cast<size_t>(snap->indexPhysicalBytes));
            if (snap->vectorPhysicalBytes > 0)
                setVal(metrics::kVectorPhysicalBytes,
                       static_cast<size_t>(snap->vectorPhysicalBytes));
            if (snap->logsTmpPhysicalBytes > 0)
                setVal(metrics::kLogsTmpPhysicalBytes,
                       static_cast<size_t>(snap->logsTmpPhysicalBytes));
            if (snap->physicalTotalBytes > 0)
                setVal(metrics::kPhysicalTotalBytes, static_cast<size_t>(snap->physicalTotalBytes));

            // Route-separated DB pool telemetry for contention attribution
            setVal(metrics::kDbWritePoolAvailable, snap->dbWritePoolAvailable ? 1 : 0);
            setVal(metrics::kDbWritePoolTotalConnections, snap->dbWritePoolTotalConnections);
            setVal(metrics::kDbWritePoolAvailableConnections,
                   snap->dbWritePoolAvailableConnections);
            setVal(metrics::kDbWritePoolActiveConnections, snap->dbWritePoolActiveConnections);
            setVal(metrics::kDbWritePoolWaitingRequests, snap->dbWritePoolWaitingRequests);
            setVal(metrics::kDbWritePoolMaxObservedWaiting, snap->dbWritePoolMaxObservedWaiting);
            setVal(metrics::kDbWritePoolTotalWaitMicros,
                   static_cast<size_t>(snap->dbWritePoolTotalWaitMicros));
            setVal(metrics::kDbWritePoolTimeoutCount, snap->dbWritePoolTimeoutCount);
            setVal(metrics::kDbWritePoolFailedAcquisitions, snap->dbWritePoolFailedAcquisitions);

            setVal(metrics::kDbReadPoolAvailable, snap->dbReadPoolAvailable ? 1 : 0);
            setVal(metrics::kDbReadPoolTotalConnections, snap->dbReadPoolTotalConnections);
            setVal(metrics::kDbReadPoolAvailableConnections, snap->dbReadPoolAvailableConnections);
            setVal(metrics::kDbReadPoolActiveConnections, snap->dbReadPoolActiveConnections);
            setVal(metrics::kDbReadPoolWaitingRequests, snap->dbReadPoolWaitingRequests);
            setVal(metrics::kDbReadPoolMaxObservedWaiting, snap->dbReadPoolMaxObservedWaiting);
            setVal(metrics::kDbReadPoolTotalWaitMicros,
                   static_cast<size_t>(snap->dbReadPoolTotalWaitMicros));
            setVal(metrics::kDbReadPoolTimeoutCount, snap->dbReadPoolTimeoutCount);
            setVal(metrics::kDbReadPoolFailedAcquisitions, snap->dbReadPoolFailedAcquisitions);

            // Document/vector counters from cached metrics (no live DB queries on hot path!).
            // Always include these keys, even when 0, so clients/benchmarks can distinguish
            // "zero" from "missing" and avoid fragile presence checks.
            setVal(metrics::kDocumentsTotal, static_cast<size_t>(snap->documentsTotal));
            setVal(metrics::kDocumentsIndexed, static_cast<size_t>(snap->documentsIndexed));
            setVal(metrics::kDocumentsContentExtracted,
                   static_cast<size_t>(snap->documentsContentExtracted));
            setVal(metrics::kDocumentsEmbedded, static_cast<size_t>(snap->documentsEmbedded));

            // Vector count from cached metrics (for benchmarks/tools waiting for embeddings)
            setVal(metrics::kVectorCount, static_cast<size_t>(snap->vectorRowsExact));
            setVal(metrics::kIndexVisible, (snap->documentsIndexed > 0) ? 1 : 0);

            if (serviceManager_) {
                setVal(metrics::kSnapshotPersisted,
                       static_cast<size_t>(serviceManager_->getSnapshotsPersistedCount()));
            }
        } else {
            auto uptime = std::chrono::steady_clock::now() - state_->stats.startTime;
            res.running = true;
            res.version = YAMS_VERSION_STRING;
            res.uptimeSeconds = std::chrono::duration_cast<std::chrono::seconds>(uptime).count();
            res.requestsProcessed = state_->stats.requestsProcessed.load();
            res.activeConnections = state_->stats.activeConnections.load();
            try {
                auto lifecycleSnapshot = daemon_->getLifecycle().snapshot();
                if (lifecycleSnapshot.state == LifecycleState::Ready ||
                    lifecycleSnapshot.state == LifecycleState::Degraded) {
                    res.memoryUsageMb = 0.0;
                    res.cpuUsagePercent = 0.0;
                }
            } catch (...) {
            }
        }
        if (!metrics_) {
            // Align boolean readiness with lifecycle readiness in non-metrics path
            try {
                auto lifecycleSnapshot = daemon_->getLifecycle().snapshot();
                res.ready = (lifecycleSnapshot.state == LifecycleState::Ready);
            } catch (...) {
                res.ready = false;
            }
            res.readinessStates[std::string(readiness::kIpcServer)] =
                state_->readiness.ipcServerReady.load();
            res.readinessStates[std::string(readiness::kContentStore)] =
                state_->readiness.contentStoreReady.load();
            res.readinessStates[std::string(readiness::kDatabase)] =
                state_->readiness.databaseReady.load();
            res.readinessStates[std::string(readiness::kMetadataRepo)] =
                state_->readiness.metadataRepoReady.load();
            res.readinessStates[std::string(readiness::kSearchEngine)] =
                state_->readiness.searchEngineReady.load();
            res.readinessStates[std::string(readiness::kModelProvider)] =
                state_->readiness.modelProviderReady.load();
            res.readinessStates[std::string(readiness::kVectorIndex)] =
                state_->readiness.vectorIndexReady.load();
            res.readinessStates[std::string(readiness::kVectorDb)] =
                state_->readiness.vectorDbReady.load();
            res.readinessStates[std::string(readiness::kPlugins)] =
                state_->readiness.pluginsReady.load();
            try {
                if (state_) {
                    res.vectorDbInitAttempted =
                        state_->readiness.vectorDbInitAttempted.load(std::memory_order_relaxed);
                    res.vectorDbReady =
                        state_->readiness.vectorDbReady.load(std::memory_order_relaxed);
                    res.vectorDbDim = state_->readiness.vectorDbDim.load(std::memory_order_relaxed);
                    res.readinessStates[std::string(readiness::kVectorDbInitAttempted)] =
                        res.vectorDbInitAttempted;
                    res.readinessStates[std::string(readiness::kVectorDbReady)] = res.vectorDbReady;
                    res.readinessStates[std::string(readiness::kVectorDbDim)] =
                        (res.vectorDbDim > 0);
                }
            } catch (...) {
            }
            // Heal/mirror vector DB readiness from the live handle.
            // Readiness semantics: false while empty/building; true only when serving (has data).
            if (serviceManager_) {
                try {
                    // Only check if already initialized - never create/initialize here
                    if (auto vdb = serviceManager_->getVectorDatabase();
                        vdb && vdb->isInitialized()) {
                        const auto dim = vdb->getConfig().embedding_dim;
                        const auto rows = vdb->getVectorCount();

                        res.vectorDbReady = (rows > 0);
                        res.vectorDbInitAttempted = true;
                        if (dim > 0) {
                            res.vectorDbDim = static_cast<uint32_t>(dim);
                        }

                        if (state_) {
                            state_->readiness.vectorDbInitAttempted.store(
                                true, std::memory_order_relaxed);
                            state_->readiness.vectorDbReady.store(res.vectorDbReady,
                                                                  std::memory_order_relaxed);
                            if (dim > 0) {
                                state_->readiness.vectorDbDim.store(static_cast<uint32_t>(dim),
                                                                    std::memory_order_relaxed);
                            }
                        }
                    }
                } catch (...) {
                }
            }

            // Ensure readinessStates[vector_db] reflects the healed vectorDbReady.
            try {
                res.readinessStates[std::string(readiness::kVectorDb)] = res.vectorDbReady;
            } catch (...) {
            }
        }
        spdlog::debug("[StatusRequest] About to check search engine degradation");
        try {
            bool searchDegraded = true;
            if (serviceManager_) {
                spdlog::debug("[StatusRequest] Calling getSearchEngineSnapshot()");
                auto engine = serviceManager_->getSearchEngineSnapshot();
                spdlog::debug("[StatusRequest] getSearchEngineSnapshot() returned, engine={}",
                              static_cast<void*>(engine.get()));
                searchDegraded = (engine == nullptr);
            }
            res.readinessStates[std::string(readiness::kSearchEngineDegraded)] = searchDegraded;
        } catch (...) {
            res.readinessStates[std::string(readiness::kSearchEngineDegraded)] = true;
        }
        // NOTE: Vector diagnostics are now collected via DaemonMetrics background thread
        // and read from the snapshot above (lines 81-92). This avoids blocking the status
        // request path. See MetricsSnapshot::vectorEmbeddingsAvailable, vectorScoringEnabled,
        // and searchEngineBuildReason fields.
        try {
            auto lifecycleSnapshot = daemon_->getLifecycle().snapshot();
            switch (lifecycleSnapshot.state) {
                case LifecycleState::Ready:
                    res.overallStatus = "ready";
                    res.lifecycleState = "ready";
                    break;
                case LifecycleState::Degraded:
                    res.overallStatus = "degraded";
                    res.lifecycleState = "degraded";
                    break;
                case LifecycleState::Stopping:
                    res.overallStatus = "stopping";
                    res.lifecycleState = "stopping";
                    break;
                case LifecycleState::Stopped:
                    res.overallStatus = "stopped";
                    res.lifecycleState = "stopped";
                    break;
                case LifecycleState::Unknown:
                default:
                    res.overallStatus = "initializing";
                    res.lifecycleState = "initializing";
                    break;
            }
            if (!lifecycleSnapshot.lastError.empty()) {
                res.lastError = lifecycleSnapshot.lastError;
            }
        } catch (...) {
            // Fallback: preserve lowercase normalization
            res.overallStatus = state_->readiness.overallStatus();
            for (auto& c : res.overallStatus)
                c = static_cast<char>(std::tolower(c));
            res.lifecycleState = res.overallStatus;
        }
        // Populate typed provider details (always send, let UI filter if needed)
        try {
            res.providers = yams::daemon::dispatch::build_typed_providers(serviceManager_, state_);
            spdlog::debug("[StatusRequest] built {} providers", res.providers.size());
            for (const auto& p : res.providers) {
                spdlog::debug(
                    "[StatusRequest]   provider: name='{}' ready={} degraded={} isProvider={}",
                    p.name, p.ready, p.degraded, p.isProvider);
            }
        } catch (const std::exception& e) {
            spdlog::warn("[StatusRequest] Exception building providers: {}", e.what());
        } catch (...) {
            spdlog::warn("[StatusRequest] Unknown exception building providers");
        }
        // Populate skipped plugin diagnostics from last scan (if available)
        try {
            if (serviceManager_) {
                if (auto* abi = serviceManager_->getAbiPluginHost()) {
                    for (const auto& pr : abi->getLastScanSkips()) {
                        StatusResponse::PluginSkipInfo s;
                        s.path = pr.first.string();
                        s.reason = pr.second;
                        res.skippedPlugins.push_back(std::move(s));
                    }
                }
            }
        } catch (...) {
        }
        if (!metrics_) {
            try {
                auto lifecycleSnapshot = daemon_->getLifecycle().snapshot();
                if (lifecycleSnapshot.state == LifecycleState::Ready ||
                    lifecycleSnapshot.state == LifecycleState::Degraded) {
                    auto snap = FsmMetricsRegistry::instance().snapshot();
                    res.fsmTransitions = snap.transitions;
                    res.fsmHeaderReads = snap.headerReads;
                    res.fsmPayloadReads = snap.payloadReads;
                    res.fsmPayloadWrites = snap.payloadWrites;
                    res.fsmBytesSent = snap.bytesSent;
                    res.fsmBytesReceived = snap.bytesReceived;
                    auto msnap = MuxMetricsRegistry::instance().snapshot();
                    res.muxActiveHandlers = msnap.activeHandlers;
                    res.muxQueuedBytes = msnap.queuedBytes;
                    res.muxWriterBudgetBytes = msnap.writerBudgetBytes;
                    // Pool sizes via FSM metrics
                    try {
                        auto fs = FsmMetricsRegistry::instance().snapshot();
                        res.ipcPoolSize = fs.ipcPoolSize;
                        res.ioPoolSize = fs.ioPoolSize;
                    } catch (...) {
                    }
                }
            } catch (...) {
            }
        }
        if (!metrics_) {
            try {
                auto lifecycleSnapshot = daemon_->getLifecycle().snapshot();
                if (lifecycleSnapshot.state == LifecycleState::Ready ||
                    lifecycleSnapshot.state == LifecycleState::Degraded) {
                    std::size_t threads = 0, active = 0, queued = 0;
                    if (serviceManager_) {
                        threads = serviceManager_->getWorkerThreads();
                        active = serviceManager_->getWorkerActive();
                        queued = 0;
                    } else {
                        threads = std::max(1u, std::thread::hardware_concurrency());
                    }
                    res.requestCounts[std::string(metrics::kWorkerThreads)] = threads;
                    res.requestCounts[std::string(metrics::kWorkerActive)] = active;
                    res.requestCounts[std::string(metrics::kWorkerQueued)] = queued;
                    // PBI-040, task 040-1: Populate postIngestQueueDepth in non-metrics path
                    if (serviceManager_) {
                        if (auto piq = serviceManager_->getPostIngestQueue()) {
                            res.postIngestQueueDepth = static_cast<uint32_t>(piq->size());
                        }
                    }
                }
            } catch (...) {
            }
        }

        // Repair metrics should be visible even in fallback/non-metrics status path.
        try {
            if (state_) {
                const auto repairRunning =
                    state_->stats.repairRunning.load(std::memory_order_relaxed);
                res.requestCounts[std::string(metrics::kRepairQueueDepth)] =
                    state_->stats.repairQueueDepth.load(std::memory_order_relaxed);
                res.requestCounts[std::string(metrics::kRepairBatchesAttempted)] =
                    state_->stats.repairBatchesAttempted.load(std::memory_order_relaxed);
                res.requestCounts[std::string(metrics::kRepairEmbeddingsGenerated)] =
                    state_->stats.repairEmbeddingsGenerated.load(std::memory_order_relaxed);
                res.requestCounts[std::string(metrics::kRepairEmbeddingsSkipped)] =
                    state_->stats.repairEmbeddingsSkipped.load(std::memory_order_relaxed);
                res.requestCounts[std::string(metrics::kRepairFailedOperations)] =
                    state_->stats.repairFailedOperations.load(std::memory_order_relaxed);
                res.requestCounts[std::string(metrics::kRepairIdleTicks)] =
                    state_->stats.repairIdleTicks.load(std::memory_order_relaxed);
                res.requestCounts[std::string(metrics::kRepairBusyTicks)] =
                    state_->stats.repairBusyTicks.load(std::memory_order_relaxed);
                res.requestCounts[std::string(metrics::kRepairTotalBacklog)] =
                    state_->stats.repairTotalBacklog.load(std::memory_order_relaxed);
                res.requestCounts[std::string(metrics::kRepairProcessed)] =
                    state_->stats.repairProcessed.load(std::memory_order_relaxed);
                res.requestCounts[std::string(metrics::kRepairRunning)] = repairRunning ? 1 : 0;
                res.requestCounts[std::string(metrics::kRepairInProgress)] =
                    state_->stats.repairInProgress.load(std::memory_order_relaxed) ? 1 : 0;
                res.readinessStates[std::string(readiness::kRepairService)] = repairRunning;
            }
        } catch (...) {
        }

        // Keep canonical readiness keys stable for clients/tests even when
        // specific FSM snapshots are unavailable in a given code path.
        const std::pair<std::string_view, bool> defaultReadiness[] = {
            {readiness::kEmbeddingReady, false},
            {readiness::kEmbeddingDegraded, false},
            {readiness::kPluginsReady, false},
            {readiness::kPluginsDegraded, false},
            {readiness::kRepairService, false},
            {readiness::kSearchEngineBuildReasonInitial, false},
            {readiness::kSearchEngineBuildReasonRebuild, false},
            {readiness::kSearchEngineBuildReasonDegraded, false},
            {readiness::kVectorDbInitAttempted, false},
            {readiness::kVectorDbReady, false},
            {readiness::kVectorDbDim, false},
        };
        for (const auto& [key, value] : defaultReadiness) {
            auto it = res.readinessStates.find(std::string(key));
            if (it == res.readinessStates.end()) {
                res.readinessStates.emplace(std::string(key), value);
            }
        }
    } catch (...) {
        StatusResponse fallback;
        fallback.running = true;
        fallback.ready = false;
        fallback.uptimeSeconds = 0;
        fallback.requestsProcessed = 0;
        fallback.activeConnections = 0;
        fallback.memoryUsageMb = 0;
        fallback.cpuUsagePercent = 0;
        fallback.version = YAMS_VERSION_STRING;
        // Normalize lowercase to preserve client readiness derivations
        fallback.overallStatus = "starting";
        co_return fallback;
    }
    co_return res;
}

boost::asio::awaitable<Response>
RequestDispatcher::handleGetStatsRequest(const GetStatsRequest& req) {
    try {
        GetStatsResponse response;
        response.totalDocuments = 0;
        response.totalSize = 0;
        response.indexedDocuments = 0;
        response.vectorIndexSize = 0;
        response.compressionRatio = 0.0;
        // Always include defaults for keys expected by tests/clients
        response.additionalStats["wal_active_transactions"] = "0";
        response.additionalStats["wal_pending_entries"] = "0";
        // Populate plugins_json with actual plugin data including interfaces
        {
            auto [pluginsJson, pluginsCount] =
                yams::daemon::dispatch::build_plugins_json(serviceManager_);
            response.additionalStats["plugins_loaded"] = std::to_string(pluginsCount);
            response.additionalStats["plugins_json"] = pluginsJson;
        }
        // Internal bus + tuning toggles (doctor hints)
        try {
            response.additionalStats["tuning_use_internal_bus_for_repair"] =
                TuneAdvisor::useInternalBusForRepair() ? "true" : "false";
            response.additionalStats["tuning_use_internal_bus_for_post_ingest"] =
                TuneAdvisor::useInternalBusForPostIngest() ? "true" : "false";
        } catch (...) {
        }
        try {
            auto& bus = InternalEventBus::instance();
            response.additionalStats[std::string(metrics::kBusEmbedQueued)] =
                std::to_string(bus.embedQueued());
            response.additionalStats[std::string(metrics::kBusEmbedConsumed)] =
                std::to_string(bus.embedConsumed());
            response.additionalStats[std::string(metrics::kBusEmbedDropped)] =
                std::to_string(bus.embedDropped());
            response.additionalStats[std::string(metrics::kBusEmbedPreparedDocsQueued)] =
                std::to_string(bus.embedPreparedDocsQueued());
            response.additionalStats[std::string(metrics::kBusEmbedPreparedChunksQueued)] =
                std::to_string(bus.embedPreparedChunksQueued());
            response.additionalStats[std::string(metrics::kBusEmbedHashOnlyDocsQueued)] =
                std::to_string(bus.embedHashOnlyDocsQueued());
            response.additionalStats[std::string(metrics::kBusPostQueued)] =
                std::to_string(bus.postQueued());
            response.additionalStats[std::string(metrics::kBusPostConsumed)] =
                std::to_string(bus.postConsumed());
            response.additionalStats[std::string(metrics::kBusPostDropped)] =
                std::to_string(bus.postDropped());
        } catch (...) {
        }
        // Embedding service metrics (in-flight jobs being processed)
        try {
            if (serviceManager_) {
                response.additionalStats["embed_in_flight"] =
                    std::to_string(serviceManager_->getEmbeddingInFlightJobs());
                response.additionalStats["embed_svc_queued"] =
                    std::to_string(serviceManager_->getEmbeddingQueuedJobs());
            }
        } catch (...) {
        }
        // Minimal readiness hint (align to lifecycle readiness)
        bool notReady = true;
        try {
            auto lifecycleSnapshot = daemon_->getLifecycle().snapshot();
            if (lifecycleSnapshot.state == LifecycleState::Ready ||
                lifecycleSnapshot.state == LifecycleState::Degraded) {
                notReady = false;
            }
        } catch (...) {
            notReady = true;
        }
        response.additionalStats["not_ready"] =
            notReady ? std::string{"true"} : std::string{"false"};
        // IPC acceptor recovery counter (macOS AF_UNIX EINVAL recovery)
        try {
            if (state_) {
                auto v = state_->stats.ipcEinvalRebuilds.load(std::memory_order_relaxed);
                response.additionalStats["einval_rebuilds"] = std::to_string(v);
            }
        } catch (...) {
        }
        // Populate vector metrics from DaemonMetrics snapshot when available
        try {
            if (metrics_) {
                metrics_->refresh();
                auto snap = metrics_->getSnapshot();
                if (!snap) {
                    // Fallback: leave defaults if snapshot is null
                    co_return response;
                }
                response.vectorIndexSize = snap->vectorDbSizeBytes;
                if (snap->vectorRowsExact > 0)
                    response.additionalStats["vector_rows"] = std::to_string(snap->vectorRowsExact);

                response.additionalStats[std::string(metrics::kDbWritePoolAvailable)] =
                    std::to_string(static_cast<size_t>(snap->dbWritePoolAvailable ? 1 : 0));
                response.additionalStats[std::string(metrics::kDbWritePoolTotalConnections)] =
                    std::to_string(snap->dbWritePoolTotalConnections);
                response.additionalStats[std::string(metrics::kDbWritePoolAvailableConnections)] =
                    std::to_string(snap->dbWritePoolAvailableConnections);
                response.additionalStats[std::string(metrics::kDbWritePoolActiveConnections)] =
                    std::to_string(snap->dbWritePoolActiveConnections);
                response.additionalStats[std::string(metrics::kDbWritePoolWaitingRequests)] =
                    std::to_string(snap->dbWritePoolWaitingRequests);
                response.additionalStats[std::string(metrics::kDbWritePoolMaxObservedWaiting)] =
                    std::to_string(snap->dbWritePoolMaxObservedWaiting);
                response.additionalStats[std::string(metrics::kDbWritePoolTotalWaitMicros)] =
                    std::to_string(snap->dbWritePoolTotalWaitMicros);
                response.additionalStats[std::string(metrics::kDbWritePoolTimeoutCount)] =
                    std::to_string(snap->dbWritePoolTimeoutCount);
                response.additionalStats[std::string(metrics::kDbWritePoolFailedAcquisitions)] =
                    std::to_string(snap->dbWritePoolFailedAcquisitions);

                response.additionalStats[std::string(metrics::kDbReadPoolAvailable)] =
                    std::to_string(static_cast<size_t>(snap->dbReadPoolAvailable ? 1 : 0));
                response.additionalStats[std::string(metrics::kDbReadPoolTotalConnections)] =
                    std::to_string(snap->dbReadPoolTotalConnections);
                response.additionalStats[std::string(metrics::kDbReadPoolAvailableConnections)] =
                    std::to_string(snap->dbReadPoolAvailableConnections);
                response.additionalStats[std::string(metrics::kDbReadPoolActiveConnections)] =
                    std::to_string(snap->dbReadPoolActiveConnections);
                response.additionalStats[std::string(metrics::kDbReadPoolWaitingRequests)] =
                    std::to_string(snap->dbReadPoolWaitingRequests);
                response.additionalStats[std::string(metrics::kDbReadPoolMaxObservedWaiting)] =
                    std::to_string(snap->dbReadPoolMaxObservedWaiting);
                response.additionalStats[std::string(metrics::kDbReadPoolTotalWaitMicros)] =
                    std::to_string(snap->dbReadPoolTotalWaitMicros);
                response.additionalStats[std::string(metrics::kDbReadPoolTimeoutCount)] =
                    std::to_string(snap->dbReadPoolTimeoutCount);
                response.additionalStats[std::string(metrics::kDbReadPoolFailedAcquisitions)] =
                    std::to_string(snap->dbReadPoolFailedAcquisitions);

                // Compute storage/db sizes (best-effort, inexpensive on request)
                try {
                    namespace fs = std::filesystem;
                    if (!snap->contentStoreRoot.empty()) {
                        fs::path storageRoot{snap->contentStoreRoot};
                        fs::path dataRoot = storageRoot.parent_path();

                        auto fileSize = [](const fs::path& p) -> uint64_t {
                            std::error_code ec;
                            auto sz = fs::file_size(p, ec);
                            return ec ? 0ull : static_cast<uint64_t>(sz);
                        };
                        auto dirSize = [&](const fs::path& p, uint64_t& countOut) -> uint64_t {
                            uint64_t total = 0;
                            uint64_t cnt = 0;
                            std::error_code ec;
                            if (fs::exists(p, ec)) {
                                for (auto it = fs::recursive_directory_iterator(p, ec);
                                     !ec && it != fs::recursive_directory_iterator(); ++it) {
                                    if (it->is_regular_file(ec)) {
                                        total += fileSize(it->path());
                                        ++cnt;
                                    }
                                }
                            }
                            countOut = cnt;
                            return total;
                        };

                        // Objects directory
                        uint64_t objFiles = 0;
                        uint64_t objBytes = dirSize(storageRoot / "objects", objFiles);
                        response.additionalStats["storage_objects_bytes"] =
                            std::to_string(objBytes);
                        response.additionalStats["storage_objects_files"] =
                            std::to_string(objFiles);

                        // Refs database (if present)
                        uint64_t refsBytes = fileSize(storageRoot / "refs.db");
                        response.additionalStats["storage_refs_db_bytes"] =
                            std::to_string(refsBytes);

                        // Main DB and vector DB/index
                        uint64_t dbBytes = fileSize(dataRoot / "yams.db");
                        response.additionalStats["db_bytes"] = std::to_string(dbBytes);
                        uint64_t vecDbBytes = fileSize(dataRoot / "vectors.db");
                        response.additionalStats["vectors_db_bytes"] = std::to_string(vecDbBytes);
                        uint64_t vecIndexBytes = fileSize(dataRoot / "vector_index.bin");
                        response.additionalStats["vector_index_bytes"] =
                            std::to_string(vecIndexBytes);

                        // Aggregate storage usage (objects + refs)
                        uint64_t storageTotal = objBytes + refsBytes;
                        response.additionalStats["storage_total_bytes"] =
                            std::to_string(storageTotal);
                    }
                } catch (...) {
                }
            }
        } catch (...) {
        }
        // Collect corpus stats for search tuning (Phase 1: Adaptive Search Tuning)
        try {
            if (serviceManager_) {
                auto metaRepo = serviceManager_->getMetadataRepo();
                if (metaRepo) {
                    auto statsResult = metaRepo->getCorpusStats();
                    if (statsResult) {
                        auto corpusJson = statsResult.value().toJson();
                        response.additionalStats["corpus_stats"] = corpusJson.dump();
                    }
                }
            }
        } catch (...) {
        }
        (void)req; // unused otherwise
        co_return response;
    } catch (...) {
        GetStatsResponse response;
        response.additionalStats["wal_active_transactions"] = "0";
        response.additionalStats["wal_pending_entries"] = "0";
        response.additionalStats["plugins_loaded"] = "0";
        response.additionalStats["plugins_json"] = "[]";
        response.additionalStats["not_ready"] = "true";
        co_return response;
    }
}

} // namespace yams::daemon
