// Split from RequestDispatcher.cpp: status handler
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <thread>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/dispatch_utils.hpp>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/ipc/fsm_metrics_registry.h>
#include <yams/daemon/ipc/mux_metrics_registry.h>
#include <yams/daemon/ipc/stream_metrics_registry.h>
#include <yams/version.hpp>

namespace yams::daemon {

// Forward declarations for free helpers defined in RequestDispatcher.cpp
double getMemoryUsage();
double getCpuUsage();

boost::asio::awaitable<Response>
RequestDispatcher::handleStatusRequest(const StatusRequest& /*req*/) {
    // Minimal and safe status path using centralized DaemonMetrics when available
    StatusResponse res;
    try {
        // fastMode flag removed; status path remains lightweight via DaemonMetrics
        if (metrics_) {
            metrics_->refresh();
            auto snap = metrics_->getSnapshot();
            res.running = snap.running;
            res.version = snap.version;
            res.uptimeSeconds = snap.uptimeSeconds;
            res.requestsProcessed = snap.requestsProcessed;
            res.activeConnections = snap.activeConnections;
            res.memoryUsageMb = snap.memoryUsageMb;
            res.cpuUsagePercent = snap.cpuUsagePercent;
            // Resolved via DaemonMetrics: ready reflects lifecycle readiness
            res.ready = snap.ready;
            res.overallStatus = snap.overallStatus;   // normalized lowercase
            res.lifecycleState = snap.lifecycleState; // normalized lowercase
            res.lastError = snap.lastError;
            res.fsmTransitions = snap.fsmTransitions;
            res.fsmHeaderReads = snap.fsmHeaderReads;
            res.fsmPayloadReads = snap.fsmPayloadReads;
            res.fsmPayloadWrites = snap.fsmPayloadWrites;
            res.fsmBytesSent = snap.fsmBytesSent;
            res.fsmBytesReceived = snap.fsmBytesReceived;
            res.muxActiveHandlers = snap.muxActiveHandlers;
            res.muxQueuedBytes = snap.muxQueuedBytes;
            res.muxWriterBudgetBytes = snap.muxWriterBudgetBytes;
            // Vector DB snapshot (best-effort)
            res.vectorDbInitAttempted = snap.vectorDbInitAttempted;
            res.vectorDbReady = snap.vectorDbReady;
            res.vectorDbDim = snap.vectorDbDim;
            // Embedding runtime details (best-effort)
            res.embeddingAvailable = snap.embeddingAvailable;
            res.embeddingBackend = snap.embeddingBackend;
            res.embeddingModel = snap.embeddingModel;
            res.embeddingModelPath = snap.embeddingModelPath;
            res.embeddingDim = snap.embeddingDim;
            res.requestCounts["worker_threads"] = snap.workerThreads;
            res.requestCounts["worker_active"] = snap.workerActive;
            res.requestCounts["worker_queued"] = snap.workerQueued;
            res.requestCounts["post_ingest_threads"] = snap.postIngestThreads;
            res.requestCounts["post_ingest_queued"] = snap.postIngestQueued;
            res.requestCounts["post_ingest_processed"] = snap.postIngestProcessed;
            res.requestCounts["post_ingest_failed"] = snap.postIngestFailed;
            res.requestCounts["post_ingest_latency_ms_ema"] =
                static_cast<size_t>(snap.postIngestLatencyMsEma);
            res.requestCounts["post_ingest_rate_sec_ema"] =
                static_cast<size_t>(snap.postIngestRateSecEma);
            res.retryAfterMs = snap.retryAfterMs;
            for (const auto& [k, v] : snap.readinessStates)
                res.readinessStates[k] = v;
            for (const auto& [k, v] : snap.initProgress)
                res.initProgress[k] = v;
            // Content store diagnostics
            res.contentStoreRoot = snap.contentStoreRoot;
            res.contentStoreError = snap.contentStoreError;
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
            res.readinessStates["plugins"] = state_->readiness.pluginsReady.load();
        }
        try {
            bool searchDegraded = true;
            if (serviceManager_) {
                auto engine = serviceManager_->getSearchEngineSnapshot();
                searchDegraded = (engine == nullptr);
            }
            res.readinessStates["search_engine_degraded"] = searchDegraded;
        } catch (...) {
            res.readinessStates["search_engine_degraded"] = true;
        }
        try {
            auto d = yams::daemon::dispatch::collect_vector_diag(serviceManager_);
            res.readinessStates["vector_embeddings_available"] = d.embeddingsAvailable;
            res.readinessStates["vector_scoring_enabled"] = d.scoringEnabled;
            // Also mirror in counters for MCP/CLI parity
            res.requestCounts["vector_embeddings_available"] = d.embeddingsAvailable ? 1 : 0;
            res.requestCounts["vector_scoring_enabled"] = d.scoringEnabled ? 1 : 0;
            res.readinessStates["search_engine_build_reason_initial"] =
                (d.buildReason == "initial");
            res.readinessStates["search_engine_build_reason_rebuild"] =
                (d.buildReason == "rebuild");
            res.readinessStates["search_engine_build_reason_degraded"] =
                (d.buildReason == "degraded");
        } catch (...) {
        }
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
        // Populate typed provider details (prefer clients to use this)
        try {
            res.providers = yams::daemon::dispatch::build_typed_providers(serviceManager_, state_);
        } catch (...) {
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
        response.additionalStats["plugins_loaded"] = "0";
        response.additionalStats["plugins_json"] = "[]";
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
                response.vectorIndexSize = snap.vectorDbSizeBytes;
                if (snap.vectorRowsExact > 0)
                    response.additionalStats["vector_rows"] = std::to_string(snap.vectorRowsExact);
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
