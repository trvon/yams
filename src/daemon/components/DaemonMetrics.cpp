#include <algorithm>
#include <fstream>
#include <sstream>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/ipc/fsm_metrics_registry.h>
#include <yams/daemon/ipc/mux_metrics_registry.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>
#include <yams/version.hpp>

namespace yams::daemon {

namespace {
// Read Proportional Set Size (PSS) in kB from smaps_rollup when available (Linux), else 0.
static std::uint64_t readPssKb() {
#ifdef __APPLE__
    return 0;
#else
    std::ifstream in("/proc/self/smaps_rollup");
    if (!in.is_open())
        return 0;
    std::string line;
    while (std::getline(in, line)) {
        if (line.rfind("Pss:", 0) == 0) {
            std::istringstream iss(line);
            std::string label;
            std::uint64_t kb = 0;
            iss >> label >> kb;
            return kb;
        }
    }
    return 0;
#endif
}

// Read Resident Set Size (VmRSS) in kB (Linux), else 0 on unsupported platforms
static std::uint64_t readRssKb() {
#ifdef __APPLE__
    return 0;
#else
    std::ifstream status("/proc/self/status");
    if (status.is_open()) {
        std::string line;
        while (std::getline(status, line)) {
            if (line.rfind("VmRSS:", 0) == 0) {
                std::istringstream iss(line);
                std::string label;
                std::uint64_t rss_kb = 0;
                iss >> label >> rss_kb;
                return rss_kb;
            }
        }
    }
    return 0;
#endif
}

[[maybe_unused]] double readMemoryUsageMb() {
#ifdef __APPLE__
    // Best-effort: not implementing macOS path here; return 0
    return 0.0;
#else
    // Prefer PSS if available (more accurate for shared pages), otherwise RSS
    std::uint64_t pss_kb = readPssKb();
    if (pss_kb > 0)
        return static_cast<double>(pss_kb) / 1024.0;
    std::uint64_t rss_kb = readRssKb();
    if (rss_kb > 0)
        return static_cast<double>(rss_kb) / 1024.0;
    return 0.0;
#endif
}

// Read CPU usage percent for the current process using /proc deltas.
// Percent is relative to total system capacity (all CPUs). A single fully utilized
// core on a 4-core system will be ~25%.
double readCpuUsagePercent(std::uint64_t& lastProcJiffies, std::uint64_t& lastTotalJiffies) {
#ifdef __APPLE__
    return 0.0;
#else
    // Read process jiffies from /proc/self/stat (utime + stime)
    std::ifstream pstat("/proc/self/stat");
    std::uint64_t procJiffies = 0;
    if (pstat.is_open()) {
        std::string content;
        std::getline(pstat, content);
        // stat fields: pid (1) comm (2) state (3) ... utime (14) stime (15)
        // Parse by splitting; comm may contain spaces in parentheses, so find closing ')'
        auto rparen = content.rfind(")");
        std::string tail = (rparen != std::string::npos && rparen + 2 < content.size())
                               ? content.substr(rparen + 2)
                               : content;
        std::istringstream iss(tail);
        // Skip fields 3..13
        for (int i = 0; i < 11; ++i) {
            std::string tmp;
            iss >> tmp;
        }
        std::uint64_t utime = 0, stime = 0;
        iss >> utime >> stime;
        procJiffies = utime + stime;
    }

    // Read total jiffies from /proc/stat aggregate cpu line
    std::ifstream sstat("/proc/stat");
    std::uint64_t totalJiffies = 0;
    if (sstat.is_open()) {
        std::string cpu;
        std::getline(sstat, cpu);
        if (cpu.rfind("cpu ", 0) == 0) {
            std::istringstream iss(cpu.substr(4));
            std::uint64_t user = 0, nice = 0, system = 0, idle = 0, iowait = 0, irq = 0,
                          softirq = 0, steal = 0, guest = 0, guest_nice = 0;
            iss >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal >> guest >>
                guest_nice;
            totalJiffies = user + nice + system + idle + iowait + irq + softirq + steal;
        }
    }

    if (lastProcJiffies == 0 || lastTotalJiffies == 0 || procJiffies < lastProcJiffies ||
        totalJiffies < lastTotalJiffies) {
        // Initialize baseline
        lastProcJiffies = procJiffies;
        lastTotalJiffies = totalJiffies;
        return 0.0;
    }
    std::uint64_t dProc = procJiffies - lastProcJiffies;
    std::uint64_t dTotal = totalJiffies - lastTotalJiffies;
    lastProcJiffies = procJiffies;
    lastTotalJiffies = totalJiffies;
    if (dTotal == 0)
        return 0.0;
    double pct = (static_cast<double>(dProc) / static_cast<double>(dTotal)) * 100.0;
    if (pct < 0.0)
        pct = 0.0;
    if (pct > 100.0)
        pct = 100.0; // cap at 100% of total system capacity
    return pct;
#endif
}

} // namespace

DaemonMetrics::DaemonMetrics(const DaemonLifecycleFsm* lifecycle, const StateComponent* state,
                             const ServiceManager* services)
    : lifecycle_(lifecycle), state_(state), services_(services) {
    cacheMs_ = TuneAdvisor::metricsCacheMs();
}

void DaemonMetrics::refresh() {
    (void)getSnapshot();
}

MetricsSnapshot DaemonMetrics::getSnapshot() const {
    // Return cached snapshot if fresh
    if (cacheMs_ > 0) {
        auto now = std::chrono::steady_clock::now();
        std::lock_guard<std::mutex> lk(cacheMutex_);
        if (lastUpdate_.time_since_epoch().count() != 0) {
            auto age =
                std::chrono::duration_cast<std::chrono::milliseconds>(now - lastUpdate_).count();
            if (age >= 0 && static_cast<uint32_t>(age) < cacheMs_) {
                return cached_;
            }
        }
    }

    MetricsSnapshot out;
    out.version = YAMS_VERSION_STRING;
    // Uptime and counters
    try {
        auto now = std::chrono::steady_clock::now();
        auto uptime = now - state_->stats.startTime;
        out.uptimeSeconds = static_cast<std::size_t>(
            std::chrono::duration_cast<std::chrono::seconds>(uptime).count());
        out.requestsProcessed = state_->stats.requestsProcessed.load();
        out.activeConnections = state_->stats.activeConnections.load();
    } catch (...) {
    }

    // Readiness flags and progress
    try {
        // Align boolean readiness with lifecycle readiness (authoritative)
        // rather than deprecated DaemonReadiness::fullyReady().
        try {
            auto lsnap = lifecycle_->snapshot();
            out.ready = (lsnap.state == LifecycleState::Ready);
        } catch (...) {
            out.ready = false;
        }
        out.readinessStates["ipc_server"] = state_->readiness.ipcServerReady.load();
        out.readinessStates["content_store"] = state_->readiness.contentStoreReady.load();
        out.readinessStates["database"] = state_->readiness.databaseReady.load();
        out.readinessStates["metadata_repo"] = state_->readiness.metadataRepoReady.load();
        out.readinessStates["search_engine"] = state_->readiness.searchEngineReady.load();
        out.readinessStates["model_provider"] = state_->readiness.modelProviderReady.load();
        out.readinessStates["vector_index"] = state_->readiness.vectorIndexReady.load();
        out.readinessStates["plugins"] = state_->readiness.pluginsReady.load();
        // Only include search init progress while not fully ready or when progress < 100%
        const bool searchReady = state_->readiness.searchEngineReady.load();
        const int searchPct = std::clamp<int>(state_->readiness.searchProgress.load(), 0, 100);
        if (!searchReady || searchPct < 100) {
            out.initProgress["search"] = static_cast<uint8_t>(searchPct);
        }
    } catch (...) {
    }

    // Lifecycle (authoritative overall status)
    try {
        auto s = lifecycle_->snapshot();
        switch (s.state) {
            case LifecycleState::Ready:
                out.overallStatus = "ready";
                out.lifecycleState = "ready";
                break;
            case LifecycleState::Degraded:
                out.overallStatus = "degraded";
                out.lifecycleState = "degraded";
                break;
            case LifecycleState::Initializing:
                out.overallStatus = "initializing";
                out.lifecycleState = "initializing";
                break;
            case LifecycleState::Starting:
                out.overallStatus = "starting";
                out.lifecycleState = "starting";
                break;
            case LifecycleState::Failed:
                out.overallStatus = "failed";
                out.lifecycleState = "failed";
                break;
            case LifecycleState::Stopping:
                out.overallStatus = "stopping";
                out.lifecycleState = "stopping";
                break;
            case LifecycleState::Stopped:
                out.overallStatus = "stopped";
                out.lifecycleState = "stopped";
                break;
            case LifecycleState::Unknown:
            default:
                out.overallStatus = "initializing";
                out.lifecycleState = "initializing";
                break;
        }
        out.lastError = s.lastError;
    } catch (...) {
        out.overallStatus = "initializing";
        out.lifecycleState = "initializing";
    }

    // Worker pool metrics
    try {
        if (services_) {
            out.workerThreads = services_->getWorkerThreads();
            out.workerActive = services_->getWorkerActive();
            out.workerQueued = services_->getWorkerQueueDepth();
            if (auto* pq = services_->getPostIngestQueue()) {
                out.postIngestThreads = pq->threads();
                out.postIngestQueued = pq->size();
                out.postIngestProcessed = pq->processed();
                out.postIngestFailed = pq->failed();
                out.postIngestLatencyMsEma = pq->latencyMsEma();
                out.postIngestRateSecEma = pq->ratePerSecEma();
            }
        } else {
            out.workerThreads = std::max(1u, std::thread::hardware_concurrency());
        }
    } catch (...) {
    }

    // FSM/MUX metrics (best-effort)
    try {
        auto fsnap = FsmMetricsRegistry::instance().snapshot();
        out.fsmTransitions = fsnap.transitions;
        out.fsmHeaderReads = fsnap.headerReads;
        out.fsmPayloadReads = fsnap.payloadReads;
        out.fsmPayloadWrites = fsnap.payloadWrites;
        out.fsmBytesSent = fsnap.bytesSent;
        out.fsmBytesReceived = fsnap.bytesReceived;
    } catch (...) {
    }
    int64_t muxQueuedBytesLocal = 0;
    try {
        auto msnap = MuxMetricsRegistry::instance().snapshot();
        out.muxActiveHandlers = msnap.activeHandlers;
        out.muxQueuedBytes = msnap.queuedBytes;
        out.muxWriterBudgetBytes = msnap.writerBudgetBytes;
        muxQueuedBytesLocal = msnap.queuedBytes;
    } catch (...) {
    }

    // OS resource hints (fast probes)
    try {
        // Base process memory footprint
        const std::uint64_t pss_kb = readPssKb();
        const std::uint64_t rss_kb = readRssKb();
        if (pss_kb > 0) {
            out.memoryUsageMb = static_cast<double>(pss_kb) / 1024.0;
            out.memoryBreakdownBytes["pss_bytes"] = pss_kb * 1024ull;
        } else {
            out.memoryUsageMb = static_cast<double>(rss_kb) / 1024.0;
        }
        if (rss_kb > 0)
            out.memoryBreakdownBytes["rss_bytes"] = rss_kb * 1024ull;

        // CPU percent uses deltas since last snapshot; stored in DaemonMetrics instance
        out.cpuUsagePercent = readCpuUsagePercent(lastProcJiffies_, lastTotalJiffies_);
    } catch (...) {
    }

    // Vector DB snapshot (size and exact rows when available)
    try {
        if (services_) {
            try {
                out.vectorDbInitAttempted = state_->readiness.vectorDbInitAttempted.load();
                out.vectorDbReady = state_->readiness.vectorDbReady.load();
                out.vectorDbDim = state_->readiness.vectorDbDim.load();
            } catch (...) {
            }
            // Size via filepath
            try {
                auto dd = services_->getResolvedDataDir();
                if (!dd.empty()) {
                    auto vdb = dd / "vectors.db";
                    if (std::filesystem::exists(vdb)) {
                        out.vectorDbSizeBytes = std::filesystem::file_size(vdb);
                    }
                }
            } catch (...) {
            }
            // Exact rows via initialized handle
            try {
                auto vdb = services_->getVectorDatabase();
                if (vdb && vdb->isInitialized()) {
                    out.vectorRowsExact = vdb->getVectorCount();
                }
            } catch (...) {
            }
            // Fallback: cheap temp handle if DB exists and rows still unknown AND
            // the daemon has not yet finished initializing its long-lived handle.
            // Avoid re-opening the DB on every metrics tick when the true row
            // count is legitimately 0 (empty DB) or when the main handle is
            // already available.
            try {
                if (out.vectorRowsExact == 0 && !out.vectorDbReady) {
                    auto dd = services_->getResolvedDataDir();
                    if (!dd.empty()) {
                        auto vdbp = dd / "vectors.db";
                        if (std::filesystem::exists(vdbp)) {
                            yams::vector::VectorDatabaseConfig cfg;
                            cfg.database_path = vdbp.string();
                            auto tmp = std::make_unique<yams::vector::VectorDatabase>(cfg);
                            if (tmp->initialize()) {
                                out.vectorRowsExact = tmp->getVectorCount();
                            }
                        }
                    }
                }
            } catch (...) {
            }
        }
    } catch (...) {
    }

    // Centralized service states
    try {
        if (services_) {
            // Content store / metadata repo
            auto cs = services_->getContentStore();
            auto mr = services_->getMetadataRepo();
            out.serviceContentStore = cs ? "running" : "unavailable";
            out.serviceMetadataRepo = mr ? "running" : "unavailable";
            // Content store diagnostics
            try {
                auto dd = services_->getResolvedDataDir();
                if (!dd.empty()) {
                    out.contentStoreRoot = (dd / "storage").string();
                }
            } catch (...) {
            }
            try {
                out.contentStoreError = services_->getContentStoreError();
            } catch (...) {
            }
            // Compression and store stats (optional)
            // Allow disabling via env to avoid instability on platforms with
            // fragile SQLite setups. When disabled, high-level metrics still
            // refresh but deep store stats remain unset.
            bool disableStoreStats = false;
            try {
                if (const char* env = std::getenv("YAMS_DISABLE_STORE_STATS")) {
                    std::string v(env);
                    std::transform(v.begin(), v.end(), v.begin(), ::tolower);
                    disableStoreStats = (v == "1" || v == "true" || v == "yes" || v == "on");
                }
            } catch (...) {
            }
            if (!disableStoreStats) {
                try {
                    if (cs) {
                        auto ss = cs->getStats();
                        out.storeObjects = ss.totalObjects;
                        out.uniqueBlocks = ss.uniqueBlocks;
                        out.deduplicatedBytes = ss.deduplicatedBytes;
                        out.compressionRatio = ss.dedupRatio();
                    }
                } catch (...) {
                }
            }
            // Search executor and reason when unavailable
            if (services_->getSearchExecutor()) {
                out.serviceSearchExecutor = "available";
            } else {
                out.serviceSearchExecutor = "unavailable";
                std::string reason;
                try {
                    if (!state_->readiness.databaseReady.load())
                        reason = "database_not_ready";
                    else if (!state_->readiness.metadataRepoReady.load())
                        reason = "metadata_repo_not_ready";
                    else
                        reason = "not_initialized";
                } catch (...) {
                }
                out.searchExecutorReason = reason;
            }
        }
    } catch (...) {
    }
    // Resolved data dir
    try {
        if (services_) {
            auto dd = services_->getResolvedDataDir();
            if (!dd.empty())
                out.dataDir = dd.string();
        }
    } catch (...) {
    }

    // Embedding runtime details (best-effort)
    try {
        if (services_) {
            auto gen = services_->getEmbeddingGenerator();
            if (gen) {
                try {
                    out.embeddingAvailable = gen->isInitialized();
                } catch (...) {
                    out.embeddingAvailable = true; // best-effort
                }
                try {
                    out.embeddingDim = static_cast<uint32_t>(gen->getEmbeddingDimension());
                } catch (...) {
                }
            }
            // Backend label heuristic and model name
            out.embeddingModel = services_->getEmbeddingModelName();
            try {
                auto prov = services_->getModelProvider();
                if (prov && prov->isAvailable()) {
                    out.embeddingBackend = "provider";
                    // Try to get model path via provider v1.2 JSON
                    if (!out.embeddingModel.empty()) {
                        try {
                            if (auto mi = prov->getModelInfo(out.embeddingModel)) {
                                out.embeddingModelPath = mi.value().path;
                                if (out.embeddingDim == 0 && mi.value().embeddingDim > 0)
                                    out.embeddingDim =
                                        static_cast<uint32_t>(mi.value().embeddingDim);
                            }
                        } catch (...) {
                        }
                    }
                } else if (gen) {
                    out.embeddingBackend = "local"; // generator-only path
                } else {
                    out.embeddingBackend = "unknown";
                }
            } catch (...) {
            }
        }
    } catch (...) {
    }

    // Add component-level memory where available (provider, vector index)
    try {
        if (services_) {
            std::uint64_t prov = 0;
            try {
                if (auto mp = services_->getModelProvider()) {
                    prov = static_cast<std::uint64_t>(mp->getMemoryUsage());
                }
            } catch (...) {
            }
            if (prov > 0)
                out.memoryBreakdownBytes["provider_bytes"] = prov;

            std::uint64_t vmem = 0;
            try {
                if (auto vim = services_->getVectorIndexManager()) {
                    auto s = vim->getStats();
                    vmem = static_cast<std::uint64_t>(s.memory_usage_bytes);
                }
            } catch (...) {
            }
            if (vmem > 0)
                out.memoryBreakdownBytes["vector_index_bytes"] = vmem;
        }
    } catch (...) {
    }

    // Backpressure threshold parsing and retry hint
    try {
        uint64_t maxWorkerQueue =
            services_ ? TuneAdvisor::maxWorkerQueue(services_->getWorkerThreads()) : 0;
        uint64_t maxMuxBytes = TuneAdvisor::maxMuxBytes();
        uint64_t maxActiveConn = TuneAdvisor::maxActiveConn();
        // Active conn default 0 = unlimited; we only compute hint, not gating here

        // Current load
        uint64_t queued = services_ ? services_->getWorkerQueueDepth() : 0;
        uint64_t activeConn = state_ ? state_->stats.activeConnections.load() : 0;

        bool bp_worker = (maxWorkerQueue > 0 && queued > maxWorkerQueue);
        bool bp_mux = (maxMuxBytes > 0 && muxQueuedBytesLocal > static_cast<int64_t>(maxMuxBytes));
        bool bp_conn = (maxActiveConn > 0 && activeConn > maxActiveConn);

        if (bp_worker || bp_mux || bp_conn) {
            // Simple retry suggestion: proportional to overload
            uint32_t base = 100; // 100ms base
            uint32_t extra = 0;
            if (bp_worker) {
                extra += static_cast<uint32_t>(std::min<uint64_t>(queued - maxWorkerQueue, 1000));
            }
            if (bp_mux) {
                // scale by MiB over budget
                uint64_t over = static_cast<uint64_t>(muxQueuedBytesLocal) - maxMuxBytes;
                extra += static_cast<uint32_t>(std::min<uint64_t>(over / (256 * 1024), 4000));
            }
            if (bp_conn) {
                extra += 200; // flat 200ms if over conn cap
            }
            out.retryAfterMs = base + extra;
        } else {
            out.retryAfterMs = 0;
        }
    } catch (...) {
        out.retryAfterMs = 0;
    }

    if (cacheMs_ > 0) {
        std::lock_guard<std::mutex> lk(cacheMutex_);
        cached_ = out;
        lastUpdate_ = std::chrono::steady_clock::now();
    }
    return out;
}

EmbeddingServiceInfo DaemonMetrics::getEmbeddingServiceInfo() const {
    EmbeddingServiceInfo info;
    try {
        if (services_) {
            auto provider = services_->getModelProvider();
            if (provider && provider->isAvailable()) {
                info.available = true;
                try {
                    auto loaded = provider->getLoadedModels();
                    info.modelsLoaded = static_cast<int>(loaded.size());
                } catch (...) {
                }
            }
        }
    } catch (...) {
    }
#ifdef YAMS_USE_ONNX_RUNTIME
    info.onnxRuntimeEnabled = true;
#else
    info.onnxRuntimeEnabled = false;
#endif
    return info;
}

} // namespace yams::daemon
