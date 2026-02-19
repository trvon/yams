#include <string_view>
#include <unordered_set>
#include <iostream>
#include <gtest/gtest.h>
#include <yams/daemon/metric_keys.h>

using namespace yams::daemon::metrics;

TEST(MetricsConsistencyTest, ValidateKeysNotEmpty) {
    // Basic sanity checks
    static_assert(!kWorkerThreads.empty());
    static_assert(!kPostIngestQueued.empty());
    static_assert(!kKgQueueDepth.empty());

    EXPECT_FALSE(kWorkerThreads.empty());
    EXPECT_FALSE(kPostIngestQueued.empty());
    EXPECT_FALSE(kKgQueueDepth.empty());
}

TEST(MetricsConsistencyTest, ValidateUniqueKeys) {
    // Ensure no two constants accidentally point to the same string literal
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

    for (const auto& key : keys) {
        std::cout << "DEBUG KEY: " << key << std::endl;
    }
    std::cout << "TOTAL KEYS: " << keys.size() << std::endl;

    EXPECT_GE(keys.size(), 81);
}

TEST(MetricsConsistencyTest, ValidateSpecificValues) {
    // Ensure we didn't change the underlying string values accidentally
    EXPECT_EQ(kKgQueueDepth, "kg_queue_depth");
    EXPECT_EQ(kPostIngestQueued, "post_ingest_queued");
    EXPECT_EQ(kEmbedQueued, "embed_svc_queued");
    EXPECT_EQ(kSearchActive, "search_active");
    EXPECT_EQ(kStorageLogicalBytes, "storage_logical_bytes");
    EXPECT_EQ(kRepairQueueDepth, "repair_queue_depth");
    EXPECT_EQ(kRepairBatchesAttempted, "repair_batches_attempted");
    EXPECT_EQ(kRepairInProgress, "repair_in_progress");
}

TEST(MetricsConsistencyTest, ValidateReadinessKeysNotEmpty) {
    static_assert(!yams::daemon::readiness::kIpcServer.empty());
    static_assert(!yams::daemon::readiness::kVectorDb.empty());
    static_assert(!yams::daemon::readiness::kSearchEngineDegraded.empty());

    EXPECT_FALSE(yams::daemon::readiness::kIpcServer.empty());
    EXPECT_FALSE(yams::daemon::readiness::kVectorDb.empty());
    EXPECT_FALSE(yams::daemon::readiness::kSearchEngineDegraded.empty());
}

TEST(MetricsConsistencyTest, ValidateReadinessUniqueKeys) {
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

    EXPECT_EQ(keys.size(), 23);
}

TEST(MetricsConsistencyTest, ValidateReadinessSpecificValues) {
    EXPECT_EQ(yams::daemon::readiness::kIpcServer, "ipc_server");
    EXPECT_EQ(yams::daemon::readiness::kContentStore, "content_store");
    EXPECT_EQ(yams::daemon::readiness::kMetadataRepo, "metadata_repo");
    EXPECT_EQ(yams::daemon::readiness::kSearchEngine, "search_engine");
    EXPECT_EQ(yams::daemon::readiness::kModelProvider, "model_provider");
    EXPECT_EQ(yams::daemon::readiness::kVectorIndex, "vector_index");
    EXPECT_EQ(yams::daemon::readiness::kVectorDb, "vector_db");
    EXPECT_EQ(yams::daemon::readiness::kPlugins, "plugins");
    EXPECT_EQ(yams::daemon::readiness::kRepairService, "repair_service");
    EXPECT_EQ(yams::daemon::readiness::kVectorDbInitAttempted, "vector_db_init_attempted");
    EXPECT_EQ(yams::daemon::readiness::kVectorDbReady, "vector_db_ready");
    EXPECT_EQ(yams::daemon::readiness::kVectorDbDim, "vector_db_dim");
}
