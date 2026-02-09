#pragma once

#include <string_view>

namespace yams::daemon::metrics {

// Vector metrics
constexpr std::string_view kVectorEmbeddingsAvailable = "vector_embeddings_available";
constexpr std::string_view kVectorScoringEnabled = "vector_scoring_enabled";

// Ingest Worker metrics
constexpr std::string_view kWorkerThreads = "worker_threads";
constexpr std::string_view kWorkerActive = "worker_active";
constexpr std::string_view kWorkerQueued = "worker_queued";

// Post-Ingest Queue metrics
constexpr std::string_view kPostIngestThreads = "post_ingest_threads";
constexpr std::string_view kPostIngestQueued = "post_ingest_queued";
constexpr std::string_view kPostIngestInflight = "post_ingest_inflight";
constexpr std::string_view kPostIngestCapacity = "post_ingest_capacity";
constexpr std::string_view kPostIngestProcessed = "post_ingest_processed";
constexpr std::string_view kPostIngestFailed = "post_ingest_failed";
constexpr std::string_view kPostIngestLatencyEma = "post_ingest_latency_ms_ema";
constexpr std::string_view kPostIngestRateEma = "post_ingest_rate_sec_ema";
constexpr std::string_view kPostIngestBackpressureRejects = "post_ingest_backpressure_rejects";

// RPC Channel metrics
constexpr std::string_view kPostIngestRpcQueued = "post_ingest_rpc_queued";
constexpr std::string_view kPostIngestRpcCapacity = "post_ingest_rpc_capacity";
constexpr std::string_view kPostIngestRpcMaxPerBatch = "post_ingest_rpc_max_per_batch";

// Extraction Stage metrics
constexpr std::string_view kExtractionInflight = "extraction_inflight";
constexpr std::string_view kPostExtractionLimit = "post_extraction_limit";

// Knowledge Graph Stage metrics
constexpr std::string_view kKgQueued = "kg_queued";
constexpr std::string_view kKgDropped = "kg_dropped";
constexpr std::string_view kKgConsumed = "kg_consumed";
constexpr std::string_view kKgInflight = "kg_inflight";
constexpr std::string_view kKgQueueDepth = "kg_queue_depth";
constexpr std::string_view kPostKgLimit = "post_kg_limit";
constexpr std::string_view kKgJobsCapacity = "kg_jobs_capacity";
constexpr std::string_view kKgJobsFillPct = "kg_jobs_fill_pct";

// Symbol Stage metrics
constexpr std::string_view kSymbolQueued = "symbol_queued";
constexpr std::string_view kSymbolDropped = "symbol_dropped";
constexpr std::string_view kSymbolConsumed = "symbol_consumed";
constexpr std::string_view kSymbolInflight = "symbol_inflight";
constexpr std::string_view kSymbolQueueDepth = "symbol_queue_depth";
constexpr std::string_view kPostSymbolLimit = "post_symbol_limit";

// Entity Stage metrics
constexpr std::string_view kEntityQueued = "entity_queued";
constexpr std::string_view kEntityDropped = "entity_dropped";
constexpr std::string_view kEntityConsumed = "entity_consumed";
constexpr std::string_view kEntityInflight = "entity_in_flight";
constexpr std::string_view kEntityQueueDepth = "entity_queue_depth";
constexpr std::string_view kPostEntityLimit = "post_entity_limit";

// Title Stage metrics
constexpr std::string_view kTitleQueued = "title_queued";
constexpr std::string_view kTitleDropped = "title_dropped";
constexpr std::string_view kTitleConsumed = "title_consumed";
constexpr std::string_view kTitleInflight = "title_inflight";
constexpr std::string_view kTitleQueueDepth = "title_queue_depth";
constexpr std::string_view kPostTitleLimit = "post_title_limit";

// Embedding Service metrics
constexpr std::string_view kEmbedQueued = "embed_svc_queued";
constexpr std::string_view kEmbedInflight = "embed_in_flight";
constexpr std::string_view kPostEmbedLimit = "post_embed_limit";

// FTS5 indexing metrics
constexpr std::string_view kFts5Queued = "fts5_queued";
constexpr std::string_view kFts5Dropped = "fts5_dropped";
constexpr std::string_view kFts5Consumed = "fts5_consumed";

// Document counts
constexpr std::string_view kDocumentsTotal = "documents_total";
constexpr std::string_view kDocumentsIndexed = "documents_indexed"; // FTS5 indexed docs
constexpr std::string_view kDocumentsContentExtracted = "documents_content_extracted";
constexpr std::string_view kDocumentsEmbedded = "documents_embedded"; // Docs with vector embeddings
constexpr std::string_view kVectorCount = "vector_count";

// Search metrics
constexpr std::string_view kSearchActive = "search_active";
constexpr std::string_view kSearchQueued = "search_queued";
constexpr std::string_view kSearchExecuted = "search_executed";
constexpr std::string_view kSearchCacheHitRatePct = "search_cache_hit_rate_pct";
constexpr std::string_view kSearchAvgLatencyUs = "search_avg_latency_us";
constexpr std::string_view kSearchConcurrencyLimit = "search_concurrency_limit";

// Ingestion metrics
constexpr std::string_view kDeferredQueueDepth = "deferred_queue_depth";

// Storage metrics
constexpr std::string_view kStorageLogicalBytes = "storage_logical_bytes";
constexpr std::string_view kStoragePhysicalBytes = "storage_physical_bytes";
constexpr std::string_view kStorageDocuments = "storage_documents";
constexpr std::string_view kStorageSavedBytes = "storage_saved_bytes";
constexpr std::string_view kStorageSavedPct = "storage_saved_pct";

// Detailed storage breakdown
constexpr std::string_view kCasPhysicalBytes = "cas_physical_bytes";
constexpr std::string_view kCasUniqueRawBytes = "cas_unique_raw_bytes";
constexpr std::string_view kCasDedupSavedBytes = "cas_dedup_saved_bytes";
constexpr std::string_view kCasCompressSavedBytes = "cas_compress_saved_bytes";
constexpr std::string_view kMetadataPhysicalBytes = "metadata_physical_bytes";
constexpr std::string_view kIndexPhysicalBytes = "index_physical_bytes";
constexpr std::string_view kVectorPhysicalBytes = "vector_physical_bytes";
constexpr std::string_view kLogsTmpPhysicalBytes = "logs_tmp_physical_bytes";
constexpr std::string_view kPhysicalTotalBytes = "physical_total_bytes";

// Session metrics
constexpr std::string_view kWatchEnabled = "watch_enabled";
constexpr std::string_view kWatchIntervalMs = "watch_interval_ms";

// Tuning metrics
constexpr std::string_view kTuningPostIngestCapacity = "tuning_post_ingest_capacity";
constexpr std::string_view kTuningPostIngestThreadsMin = "tuning_post_ingest_threads_min";
constexpr std::string_view kTuningPostIngestThreadsMax = "tuning_post_ingest_threads_max";
constexpr std::string_view kTuningAdmitWarnThreshold = "tuning_admit_warn_threshold";
constexpr std::string_view kTuningAdmitStopThreshold = "tuning_admit_stop_threshold";

// WorkCoordinator metrics
constexpr std::string_view kWorkCoordinatorActive = "work_coordinator_active";
constexpr std::string_view kWorkCoordinatorRunning = "work_coordinator_running";

// Other request counts
constexpr std::string_view kServiceFsmState = "service_fsm_state";
constexpr std::string_view kEmbeddingState = "embedding_state";
constexpr std::string_view kPluginHostState = "plugin_host_state";
constexpr std::string_view kPostIngestUseBus = "post_ingest_use_bus";

// Stream metrics
constexpr std::string_view kStreamTotal = "stream_total";
constexpr std::string_view kStreamBatches = "stream_batches";
constexpr std::string_view kStreamKeepalives = "stream_keepalives";
constexpr std::string_view kStreamTtfbAvgMs = "stream_ttfb_avg_ms";

// File/Directory add tracking
constexpr std::string_view kFilesAdded = "files_added";
constexpr std::string_view kDirectoriesAdded = "directories_added";
constexpr std::string_view kFilesProcessed = "files_processed";
constexpr std::string_view kDirectoriesProcessed = "directories_processed";

// Event Bus metrics
constexpr std::string_view kBusEmbedQueued = "bus_embed_queued";
constexpr std::string_view kBusEmbedConsumed = "bus_embed_consumed";
constexpr std::string_view kBusEmbedDropped = "bus_embed_dropped";
constexpr std::string_view kBusPostQueued = "bus_post_queued";
constexpr std::string_view kBusPostConsumed = "bus_post_consumed";
constexpr std::string_view kBusPostDropped = "bus_post_dropped";

// Resource Governor metrics
constexpr std::string_view kPressureLevel = "pressure_level";

} // namespace yams::daemon::metrics
