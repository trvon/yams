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
#include <yams/vector/vector_database.h>
#include <yams/version.hpp>

namespace yams::daemon {

// Forward declarations for free helpers defined in RequestDispatcher.cpp
double getMemoryUsage();
double getCpuUsage();

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
            // Embedding runtime details (best-effort)
            res.embeddingAvailable = snap->embeddingAvailable;
            res.embeddingBackend = snap->embeddingBackend;
            res.embeddingModel = snap->embeddingModel;
            res.embeddingModelPath = snap->embeddingModelPath;
            res.embeddingDim = snap->embeddingDim;
            // Vector diagnostics (from background snapshot - no blocking)
            res.readinessStates["vector_embeddings_available"] = snap->vectorEmbeddingsAvailable;
            res.readinessStates["vector_scoring_enabled"] = snap->vectorScoringEnabled;
            res.requestCounts["vector_embeddings_available"] =
                snap->vectorEmbeddingsAvailable ? 1 : 0;
            res.requestCounts["vector_scoring_enabled"] = snap->vectorScoringEnabled ? 1 : 0;
            res.readinessStates["search_engine_build_reason_initial"] =
                (snap->searchEngineBuildReason == "initial");
            res.readinessStates["search_engine_build_reason_rebuild"] =
                (snap->searchEngineBuildReason == "rebuild");
            res.readinessStates["search_engine_build_reason_degraded"] =
                (snap->searchEngineBuildReason == "degraded");
            res.requestCounts["worker_threads"] = snap->workerThreads;
            res.requestCounts["worker_active"] = snap->workerActive;
            res.requestCounts["worker_queued"] = snap->workerQueued;
            res.requestCounts["post_ingest_threads"] = snap->postIngestThreads;
            res.requestCounts["post_ingest_queued"] = snap->postIngestQueued;
            res.searchMetrics.active = snap->searchActive;
            res.searchMetrics.queued = snap->searchQueued;
            res.searchMetrics.executed = snap->searchExecuted;
            res.searchMetrics.cacheHitRate = snap->searchCacheHitRate;
            res.searchMetrics.avgLatencyUs = snap->searchAvgLatencyUs;
            res.searchMetrics.concurrencyLimit = snap->searchConcurrencyLimit;
            res.requestCounts["search_active"] = snap->searchActive;
            res.requestCounts["search_queued"] = snap->searchQueued;
            res.requestCounts["search_executed"] = static_cast<size_t>(snap->searchExecuted);
            res.requestCounts["search_cache_hit_rate_pct"] =
                static_cast<size_t>(snap->searchCacheHitRate * 100.0);
            res.requestCounts["search_avg_latency_us"] =
                static_cast<size_t>(snap->searchAvgLatencyUs);
            res.requestCounts["search_concurrency_limit"] = snap->searchConcurrencyLimit;
            // PBI-040, task 040-1: Expose queue depth for FTS5 readiness checks
            res.postIngestQueueDepth = static_cast<uint32_t>(snap->postIngestQueued);
            res.requestCounts["post_ingest_inflight"] = snap->postIngestInflight;
            res.requestCounts["post_ingest_capacity"] = snap->postIngestCapacity;
            // Export selected tuning config values for clients (best-effort)
            try {
                if (serviceManager_) {
                    // Surface FSM states as numeric codes in requestCounts, and booleans in
                    // readinessStates
                    try {
                        auto ss = serviceManager_->getServiceManagerFsmSnapshot();
                        res.requestCounts["service_fsm_state"] = static_cast<size_t>(ss.state);
                    } catch (...) {
                    }
                    try {
                        auto es = serviceManager_->getEmbeddingProviderFsmSnapshot();
                        res.requestCounts["embedding_state"] = static_cast<size_t>(es.state);
                        res.readinessStates["embedding_ready"] =
                            (es.state == EmbeddingProviderState::ModelReady);
                        // Provide an explicit degraded flag for clients/tools that
                        // distinguish readiness from degraded modes.
                        res.readinessStates["embedding_degraded"] =
                            (es.state == EmbeddingProviderState::Degraded);
                    } catch (...) {
                    }
                    try {
                        auto ps = serviceManager_->getPluginHostFsmSnapshot();
                        res.requestCounts["plugin_host_state"] = static_cast<size_t>(ps.state);
                        res.readinessStates["plugins_ready"] = (ps.state == PluginHostState::Ready);
                        res.readinessStates["plugins_degraded"] =
                            (ps.state == PluginHostState::Failed);
                    } catch (...) {
                    }
                    const auto& tc = serviceManager_->getConfig().tuning;
                    res.requestCounts["tuning_post_ingest_capacity"] = tc.postIngestCapacity;
                    res.requestCounts["tuning_post_ingest_threads_min"] = tc.postIngestThreadsMin;
                    res.requestCounts["tuning_post_ingest_threads_max"] = tc.postIngestThreadsMax;
                    res.requestCounts["tuning_admit_warn_threshold"] = tc.admitWarnThreshold;
                    res.requestCounts["tuning_admit_stop_threshold"] = tc.admitStopThreshold;
                }
            } catch (...) {
            }
            res.requestCounts["post_ingest_processed"] = snap->postIngestProcessed;
            res.requestCounts["post_ingest_failed"] = snap->postIngestFailed;
            res.requestCounts["post_ingest_latency_ms_ema"] =
                static_cast<size_t>(snap->postIngestLatencyMsEma);
            res.requestCounts["post_ingest_rate_sec_ema"] =
                static_cast<size_t>(snap->postIngestRateSecEma);
            res.requestCounts["extraction_inflight"] = snap->extractionInFlight;
            res.requestCounts["kg_queued"] = snap->kgQueued;
            res.requestCounts["kg_dropped"] = snap->kgDropped;
            res.requestCounts["kg_consumed"] = snap->kgConsumed;
            res.requestCounts["kg_inflight"] = snap->kgInFlight;
            res.requestCounts["symbol_inflight"] = snap->symbolInFlight;
            // Entity extraction metrics (external plugins like Ghidra)
            res.requestCounts["entity_queued"] = snap->entityQueued;
            res.requestCounts["entity_dropped"] = snap->entityDropped;
            res.requestCounts["entity_consumed"] = snap->entityConsumed;
            res.requestCounts["entity_inflight"] = snap->entityInFlight;
            // Dynamic concurrency limits (PBI-05a)
            res.requestCounts["post_extraction_limit"] = snap->postExtractionLimit;
            res.requestCounts["post_kg_limit"] = snap->postKgLimit;
            res.requestCounts["post_symbol_limit"] = snap->postSymbolLimit;
            res.requestCounts["post_entity_limit"] = snap->postEntityLimit;
            // Surface whether the InternalEventBus is being used for post-ingest
            try {
                res.requestCounts["post_ingest_use_bus"] =
                    yams::daemon::TuneAdvisor::useInternalBusForPostIngest() ? 1 : 0;
            } catch (...) {
            }
            res.retryAfterMs = snap->retryAfterMs;
            for (const auto& [k, v] : snap->readinessStates)
                res.readinessStates[k] = v;
            for (const auto& [k, v] : snap->initProgress)
                res.initProgress[k] = v;
            // Content store diagnostics
            res.contentStoreRoot = snap->contentStoreRoot;
            res.contentStoreError = snap->contentStoreError;
            // Storage size summary (exposed via requestCounts for backwards compatible clients)
            if (snap->logicalBytes > 0)
                res.requestCounts["storage_logical_bytes"] =
                    static_cast<size_t>(snap->logicalBytes);
            if (snap->physicalBytes > 0)
                res.requestCounts["storage_physical_bytes"] =
                    static_cast<size_t>(snap->physicalBytes);
            if (snap->storeObjects > 0)
                res.requestCounts["storage_documents"] = static_cast<size_t>(snap->storeObjects);
            if (snap->logicalBytes > 0 && snap->physicalBytes > 0) {
                std::uint64_t saved = (snap->logicalBytes > snap->physicalBytes)
                                          ? (snap->logicalBytes - snap->physicalBytes)
                                          : 0ULL;
                res.requestCounts["storage_saved_bytes"] = static_cast<size_t>(saved);
                std::uint64_t pct =
                    snap->logicalBytes ? (saved * 100ULL) / snap->logicalBytes : 0ULL;
                res.requestCounts["storage_saved_pct"] = static_cast<size_t>(pct);
            } else {
                // Avoid signaling 100% savings when physical is unknown
                res.requestCounts.erase("storage_saved_bytes");
                res.requestCounts.erase("storage_saved_pct");
            }
            // New: detailed storage breakdown (when available)
            if (snap->casPhysicalBytes > 0)
                res.requestCounts["cas_physical_bytes"] =
                    static_cast<size_t>(snap->casPhysicalBytes);
            if (snap->casUniqueRawBytes > 0)
                res.requestCounts["cas_unique_raw_bytes"] =
                    static_cast<size_t>(snap->casUniqueRawBytes);
            if (snap->casDedupSavedBytes > 0)
                res.requestCounts["cas_dedup_saved_bytes"] =
                    static_cast<size_t>(snap->casDedupSavedBytes);
            if (snap->casCompressSavedBytes > 0)
                res.requestCounts["cas_compress_saved_bytes"] =
                    static_cast<size_t>(snap->casCompressSavedBytes);
            if (snap->metadataPhysicalBytes > 0)
                res.requestCounts["metadata_physical_bytes"] =
                    static_cast<size_t>(snap->metadataPhysicalBytes);
            if (snap->indexPhysicalBytes > 0)
                res.requestCounts["index_physical_bytes"] =
                    static_cast<size_t>(snap->indexPhysicalBytes);
            if (snap->vectorPhysicalBytes > 0)
                res.requestCounts["vector_physical_bytes"] =
                    static_cast<size_t>(snap->vectorPhysicalBytes);
            if (snap->logsTmpPhysicalBytes > 0)
                res.requestCounts["logs_tmp_physical_bytes"] =
                    static_cast<size_t>(snap->logsTmpPhysicalBytes);
            if (snap->physicalTotalBytes > 0)
                res.requestCounts["physical_total_bytes"] =
                    static_cast<size_t>(snap->physicalTotalBytes);

            // Document counters from cached metrics (no live DB queries on hot path!)
            if (snap->documentsTotal > 0) {
                res.requestCounts["documents_total"] = static_cast<size_t>(snap->documentsTotal);
            }
            if (snap->documentsIndexed > 0) {
                res.requestCounts["documents_indexed"] =
                    static_cast<size_t>(snap->documentsIndexed);
            }
            if (snap->documentsContentExtracted > 0) {
                res.requestCounts["documents_content_extracted"] =
                    static_cast<size_t>(snap->documentsContentExtracted);
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
                    res.memoryUsageMb = getMemoryUsage();
                    res.cpuUsagePercent = getCpuUsage();
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
            res.readinessStates["ipc_server"] = state_->readiness.ipcServerReady.load();
            res.readinessStates["content_store"] = state_->readiness.contentStoreReady.load();
            res.readinessStates["database"] = state_->readiness.databaseReady.load();
            res.readinessStates["metadata_repo"] = state_->readiness.metadataRepoReady.load();
            res.readinessStates["search_engine"] = state_->readiness.searchEngineReady.load();
            res.readinessStates["model_provider"] = state_->readiness.modelProviderReady.load();
            res.readinessStates["vector_index"] = state_->readiness.vectorIndexReady.load();
            res.readinessStates["vector_db"] = state_->readiness.vectorDbReady.load();
            res.readinessStates["plugins"] = state_->readiness.pluginsReady.load();
            try {
                if (state_) {
                    res.vectorDbInitAttempted =
                        state_->readiness.vectorDbInitAttempted.load(std::memory_order_relaxed);
                    res.vectorDbReady =
                        state_->readiness.vectorDbReady.load(std::memory_order_relaxed);
                    res.vectorDbDim = state_->readiness.vectorDbDim.load(std::memory_order_relaxed);
                }
            } catch (...) {
            }
            if (!res.vectorDbReady && serviceManager_) {
                try {
                    // Only check if already initialized - never create/initialize here
                    if (auto vdb = serviceManager_->getVectorDatabase();
                        vdb && vdb->isInitialized()) {
                        res.vectorDbReady = true;
                        auto dim = vdb->getConfig().embedding_dim;
                        if (dim > 0) {
                            res.vectorDbDim = static_cast<uint32_t>(dim);
                        }
                        if (state_) {
                            state_->readiness.vectorDbReady.store(true, std::memory_order_relaxed);
                            if (dim > 0) {
                                state_->readiness.vectorDbDim.store(static_cast<uint32_t>(dim),
                                                                    std::memory_order_relaxed);
                            }
                        }
                    }
                } catch (...) {
                }
            }
        }
        spdlog::info("[StatusRequest] About to check search engine degradation");
        try {
            bool searchDegraded = true;
            if (serviceManager_) {
                spdlog::info("[StatusRequest] Calling getSearchEngineSnapshot()");
                auto engine = serviceManager_->getSearchEngineSnapshot();
                spdlog::info("[StatusRequest] getSearchEngineSnapshot() returned, engine={}",
                             (void*)engine.get());
                searchDegraded = (engine == nullptr);
            }
            res.readinessStates["search_engine_degraded"] = searchDegraded;
        } catch (...) {
            res.readinessStates["search_engine_degraded"] = true;
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
            spdlog::info("[StatusRequest] built {} providers", res.providers.size());
            for (const auto& p : res.providers) {
                spdlog::info(
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
                    res.requestCounts["worker_threads"] = threads;
                    res.requestCounts["worker_active"] = active;
                    res.requestCounts["worker_queued"] = queued;
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
            auto [pluginsJson, pluginsCount] = yams::daemon::dispatch::build_plugins_json(serviceManager_);
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
            response.additionalStats["bus_embed_queued"] = std::to_string(bus.embedQueued());
            response.additionalStats["bus_embed_consumed"] = std::to_string(bus.embedConsumed());
            response.additionalStats["bus_embed_dropped"] = std::to_string(bus.embedDropped());
            response.additionalStats["bus_post_queued"] = std::to_string(bus.postQueued());
            response.additionalStats["bus_post_consumed"] = std::to_string(bus.postConsumed());
            response.additionalStats["bus_post_dropped"] = std::to_string(bus.postDropped());
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
