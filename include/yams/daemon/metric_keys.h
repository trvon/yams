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
constexpr std::string_view kEmbedInferActive = "embed_infer_active";
constexpr std::string_view kEmbedInferOldestMs = "embed_infer_oldest_ms";
constexpr std::string_view kEmbedInferStarted = "embed_infer_started";
constexpr std::string_view kEmbedInferCompleted = "embed_infer_completed";
constexpr std::string_view kEmbedInferLastMs = "embed_infer_last_ms";
constexpr std::string_view kEmbedInferMaxMs = "embed_infer_max_ms";
constexpr std::string_view kEmbedInferWarnCount = "embed_infer_warn_count";
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

// Route-separated DB pool metrics (write/work vs read)
constexpr std::string_view kDbWritePoolAvailable = "db_write_pool_available";
constexpr std::string_view kDbWritePoolTotalConnections = "db_write_pool_total_connections";
constexpr std::string_view kDbWritePoolAvailableConnections =
	"db_write_pool_available_connections";
constexpr std::string_view kDbWritePoolActiveConnections = "db_write_pool_active_connections";
constexpr std::string_view kDbWritePoolWaitingRequests = "db_write_pool_waiting_requests";
constexpr std::string_view kDbWritePoolMaxObservedWaiting =
	"db_write_pool_max_observed_waiting";
constexpr std::string_view kDbWritePoolTotalWaitMicros = "db_write_pool_total_wait_micros";
constexpr std::string_view kDbWritePoolTimeoutCount = "db_write_pool_timeout_count";
constexpr std::string_view kDbWritePoolFailedAcquisitions =
	"db_write_pool_failed_acquisitions";

constexpr std::string_view kDbReadPoolAvailable = "db_read_pool_available";
constexpr std::string_view kDbReadPoolTotalConnections = "db_read_pool_total_connections";
constexpr std::string_view kDbReadPoolAvailableConnections =
	"db_read_pool_available_connections";
constexpr std::string_view kDbReadPoolActiveConnections = "db_read_pool_active_connections";
constexpr std::string_view kDbReadPoolWaitingRequests = "db_read_pool_waiting_requests";
constexpr std::string_view kDbReadPoolMaxObservedWaiting = "db_read_pool_max_observed_waiting";
constexpr std::string_view kDbReadPoolTotalWaitMicros = "db_read_pool_total_wait_micros";
constexpr std::string_view kDbReadPoolTimeoutCount = "db_read_pool_timeout_count";
constexpr std::string_view kDbReadPoolFailedAcquisitions = "db_read_pool_failed_acquisitions";

// Ingestion metrics
constexpr std::string_view kDeferredQueueDepth = "deferred_queue_depth";
constexpr std::string_view kSnapshotPersisted = "snapshot_persisted";
constexpr std::string_view kPostIngestDrained = "post_ingest_drained";
constexpr std::string_view kIndexVisible = "index_visible";

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
constexpr std::string_view kBusEmbedPreparedDocsQueued = "bus_embed_prepared_docs_queued";
constexpr std::string_view kBusEmbedPreparedChunksQueued = "bus_embed_prepared_chunks_queued";
constexpr std::string_view kBusEmbedHashOnlyDocsQueued = "bus_embed_hash_only_docs_queued";
constexpr std::string_view kBusPostQueued = "bus_post_queued";
constexpr std::string_view kBusPostConsumed = "bus_post_consumed";
constexpr std::string_view kBusPostDropped = "bus_post_dropped";

// Resource Governor metrics
constexpr std::string_view kPressureLevel = "pressure_level";

} // namespace yams::daemon::metrics

namespace yams::daemon::readiness {

// Core readiness gates
constexpr std::string_view kIpcServer = "ipc_server";
constexpr std::string_view kContentStore = "content_store";
constexpr std::string_view kDatabase = "database";
constexpr std::string_view kMetadataRepo = "metadata_repo";
constexpr std::string_view kSearchEngine = "search_engine";
constexpr std::string_view kModelProvider = "model_provider";
constexpr std::string_view kVectorIndex = "vector_index";
constexpr std::string_view kVectorDb = "vector_db";
constexpr std::string_view kPlugins = "plugins";

// Extended readiness details
constexpr std::string_view kVectorDbInitAttempted = "vector_db_init_attempted";
constexpr std::string_view kVectorDbReady = "vector_db_ready";
constexpr std::string_view kVectorDbDim = "vector_db_dim";
constexpr std::string_view kSearchEngineDegraded = "search_engine_degraded";
constexpr std::string_view kEmbeddingReady = "embedding_ready";
constexpr std::string_view kEmbeddingDegraded = "embedding_degraded";
constexpr std::string_view kPluginsReady = "plugins_ready";
constexpr std::string_view kPluginsDegraded = "plugins_degraded";
constexpr std::string_view kVectorEmbeddingsAvailable = "vector_embeddings_available";
constexpr std::string_view kVectorScoringEnabled = "vector_scoring_enabled";
constexpr std::string_view kSearchEngineBuildReasonInitial = "search_engine_build_reason_initial";
constexpr std::string_view kSearchEngineBuildReasonRebuild = "search_engine_build_reason_rebuild";
constexpr std::string_view kSearchEngineBuildReasonDegraded = "search_engine_build_reason_degraded";

} // namespace yams::daemon::readiness
