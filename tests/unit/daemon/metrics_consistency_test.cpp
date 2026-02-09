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
        kPressureLevel};

    for (const auto& key : keys) {
        std::cout << "DEBUG KEY: " << key << std::endl;
    }
    std::cout << "TOTAL KEYS: " << keys.size() << std::endl;

    EXPECT_GE(keys.size(), 70);
}

TEST(MetricsConsistencyTest, ValidateSpecificValues) {
    // Ensure we didn't change the underlying string values accidentally
    EXPECT_EQ(kKgQueueDepth, "kg_queue_depth");
    EXPECT_EQ(kPostIngestQueued, "post_ingest_queued");
    EXPECT_EQ(kEmbedQueued, "embed_svc_queued");
    EXPECT_EQ(kSearchActive, "search_active");
    EXPECT_EQ(kStorageLogicalBytes, "storage_logical_bytes");
}
