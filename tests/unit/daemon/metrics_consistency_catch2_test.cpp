// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Migrated from GTest: metrics_consistency_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <string_view>
#include <unordered_set>

#include <yams/daemon/metric_keys.h>

using namespace yams::daemon::metrics;

TEST_CASE("Metrics consistency: keys not empty", "[unit][daemon][metrics]") {
    static_assert(!kWorkerThreads.empty());
    static_assert(!kPostIngestQueued.empty());
    static_assert(!kKgQueueDepth.empty());

    CHECK_FALSE(kWorkerThreads.empty());
    CHECK_FALSE(kPostIngestQueued.empty());
    CHECK_FALSE(kKgQueueDepth.empty());
}

TEST_CASE("Metrics consistency: unique keys", "[unit][daemon][metrics]") {
    std::unordered_set<std::string_view> keys = {
        // Vector
        kVectorEmbeddingsAvailable, kVectorScoringEnabled,

        // Worker
        kWorkerThreads, kWorkerActive, kWorkerQueued,

        // Post-Ingest
        kPostIngestThreads, kPostIngestQueued, kPostIngestInflight, kPostIngestCapacity,
        kPostIngestProcessed, kPostIngestFailed, kPostIngestLatencyEma, kPostIngestRateEma,
        kPostIngestBackpressureRejects,

        // RPC
        kPostIngestRpcQueued, kPostIngestRpcCapacity, kPostIngestRpcMaxPerBatch,

        // Stages
        kExtractionInflight, kPostExtractionLimit, kKgQueued, kKgDropped, kKgConsumed, kKgInflight,
        kKgQueueDepth, kPostKgLimit, kKgJobsCapacity, kKgJobsFillPct, kSymbolQueued, kSymbolDropped,
        kSymbolConsumed, kSymbolInflight, kSymbolQueueDepth, kPostSymbolLimit, kEntityQueued,
        kEntityDropped, kEntityConsumed, kEntityInflight, kEntityQueueDepth, kPostEntityLimit,
        kTitleQueued, kTitleDropped, kTitleConsumed, kTitleInflight, kTitleQueueDepth,
        kPostTitleLimit, kEmbedQueued, kEmbedInflight, kPostEmbedLimit,

        // FTS5
        kFts5Queued, kFts5Dropped, kFts5Consumed,

        // Search
        kSearchActive, kSearchQueued, kSearchExecuted, kSearchCacheHitRatePct, kSearchAvgLatencyUs,
        kSearchConcurrencyLimit,

        // Ingestion
        kDeferredQueueDepth, kPostIngestUseBus,

        // Storage
        kStorageLogicalBytes, kStoragePhysicalBytes, kStorageDocuments, kStorageSavedBytes,
        kStorageSavedPct, kCasPhysicalBytes, kCasUniqueRawBytes, kCasDedupSavedBytes,
        kCasCompressSavedBytes, kMetadataPhysicalBytes, kIndexPhysicalBytes, kVectorPhysicalBytes,
        kLogsTmpPhysicalBytes, kPhysicalTotalBytes,

        // Counts
        kDocumentsTotal, kDocumentsIndexed, kDocumentsContentExtracted, kDocumentsEmbedded,
        kVectorCount,

        // Session
        kWatchEnabled, kWatchIntervalMs,

        // Tuning
        kTuningPostIngestCapacity, kTuningPostIngestThreadsMin, kTuningPostIngestThreadsMax,
        kTuningAdmitWarnThreshold, kTuningAdmitStopThreshold,

        // WorkCoordinator
        kWorkCoordinatorActive, kWorkCoordinatorRunning,

        // State
        kServiceFsmState, kEmbeddingState, kPluginHostState,

        // Stream
        kStreamTotal, kStreamBatches, kStreamKeepalives, kStreamTtfbAvgMs,

        // Add Tracking
        kFilesAdded, kDirectoriesAdded, kFilesProcessed, kDirectoriesProcessed,

        // Governor
        kPressureLevel,

        // Repair
        kRepairRunning, kRepairInProgress, kRepairQueueDepth, kRepairBatchesAttempted,
        kRepairEmbeddingsGenerated, kRepairEmbeddingsSkipped, kRepairFailedOperations,
        kRepairIdleTicks, kRepairBusyTicks, kRepairTotalBacklog, kRepairProcessed};

    CHECK(keys.size() >= 81);
}

TEST_CASE("Metrics consistency: specific values", "[unit][daemon][metrics]") {
    CHECK(kKgQueueDepth == "kg_queue_depth");
    CHECK(kPostIngestQueued == "post_ingest_queued");
    CHECK(kEmbedQueued == "embed_svc_queued");
    CHECK(kSearchActive == "search_active");
    CHECK(kStorageLogicalBytes == "storage_logical_bytes");
    CHECK(kRepairQueueDepth == "repair_queue_depth");
    CHECK(kRepairBatchesAttempted == "repair_batches_attempted");
    CHECK(kRepairInProgress == "repair_in_progress");
}

TEST_CASE("Metrics consistency: readiness keys not empty", "[unit][daemon][metrics]") {
    static_assert(!yams::daemon::readiness::kIpcServer.empty());
    static_assert(!yams::daemon::readiness::kVectorDb.empty());
    static_assert(!yams::daemon::readiness::kSearchEngineDegraded.empty());

    CHECK_FALSE(yams::daemon::readiness::kIpcServer.empty());
    CHECK_FALSE(yams::daemon::readiness::kVectorDb.empty());
    CHECK_FALSE(yams::daemon::readiness::kSearchEngineDegraded.empty());
}

TEST_CASE("Metrics consistency: readiness unique keys", "[unit][daemon][metrics]") {
    std::unordered_set<std::string_view> keys = {
        yams::daemon::readiness::kIpcServer,
        yams::daemon::readiness::kContentStore,
        yams::daemon::readiness::kDatabase,
        yams::daemon::readiness::kMetadataRepo,
        yams::daemon::readiness::kSearchEngine,
        yams::daemon::readiness::kModelProvider,
        yams::daemon::readiness::kVectorIndex,
        yams::daemon::readiness::kVectorDb,
        yams::daemon::readiness::kPlugins,
        yams::daemon::readiness::kVectorDbInitAttempted,
        yams::daemon::readiness::kVectorDbReady,
        yams::daemon::readiness::kVectorDbDim,
        yams::daemon::readiness::kSearchEngineDegraded,
        yams::daemon::readiness::kEmbeddingReady,
        yams::daemon::readiness::kEmbeddingDegraded,
        yams::daemon::readiness::kPluginsReady,
        yams::daemon::readiness::kPluginsDegraded,
        yams::daemon::readiness::kVectorEmbeddingsAvailable,
        yams::daemon::readiness::kVectorScoringEnabled,
        yams::daemon::readiness::kRepairService,
        yams::daemon::readiness::kSearchEngineBuildReasonInitial,
        yams::daemon::readiness::kSearchEngineBuildReasonRebuild,
        yams::daemon::readiness::kSearchEngineBuildReasonDegraded,
    };

    CHECK(keys.size() == 23);
}

TEST_CASE("Metrics consistency: readiness specific values", "[unit][daemon][metrics]") {
    CHECK(yams::daemon::readiness::kIpcServer == "ipc_server");
    CHECK(yams::daemon::readiness::kContentStore == "content_store");
    CHECK(yams::daemon::readiness::kMetadataRepo == "metadata_repo");
    CHECK(yams::daemon::readiness::kSearchEngine == "search_engine");
    CHECK(yams::daemon::readiness::kModelProvider == "model_provider");
    CHECK(yams::daemon::readiness::kVectorIndex == "vector_index");
    CHECK(yams::daemon::readiness::kVectorDb == "vector_db");
    CHECK(yams::daemon::readiness::kPlugins == "plugins");
    CHECK(yams::daemon::readiness::kRepairService == "repair_service");
    CHECK(yams::daemon::readiness::kVectorDbInitAttempted == "vector_db_init_attempted");
    CHECK(yams::daemon::readiness::kVectorDbReady == "vector_db_ready");
    CHECK(yams::daemon::readiness::kVectorDbDim == "vector_db_dim");
}
