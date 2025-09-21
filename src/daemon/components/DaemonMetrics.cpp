#include <algorithm>
#include <fstream>
#include <sstream>
#include <yams/compression/compression_monitor.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/MetricsSnapshotRegistry.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/ipc/fsm_metrics_registry.h>
#include <yams/daemon/ipc/mux_metrics_registry.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>
#include <yams/version.hpp>
#ifdef __unix__
#include <sys/stat.h>
#endif
#if defined(TRACY_ENABLE)
#include <tracy/Tracy.hpp>
#endif

#ifdef __APPLE__
#include <unistd.h>
#include <mach/mach.h>
#endif

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
    task_vm_info_data_t info;
    mach_msg_type_number_t count = TASK_VM_INFO_COUNT;
    if (task_info(mach_task_self(), TASK_VM_INFO, (task_info_t)&info, &count) == KERN_SUCCESS) {
        return info.resident_size / 1024;
    }
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
    // On macOS, PSS is not easily available, so we rely on RSS.
    std::uint64_t rss_kb = readRssKb();
    if (rss_kb > 0)
        return static_cast<double>(rss_kb) / 1024.0;
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
    // Process CPU time
    task_thread_times_info_data_t thread_info;
    mach_msg_type_number_t count = TASK_THREAD_TIMES_INFO_COUNT;
    uint64_t procJiffies = 0;
    if (task_info(mach_task_self(), TASK_THREAD_TIMES_INFO, (task_info_t)&thread_info, &count) ==
        KERN_SUCCESS) {
        procJiffies =
            (thread_info.user_time.seconds + thread_info.system_time.seconds) * 100 +
            (thread_info.user_time.microseconds + thread_info.system_time.microseconds) / 10000;
    }

    // Total CPU time
    host_cpu_load_info_data_t cpu_info;
    count = HOST_CPU_LOAD_INFO_COUNT;
    uint64_t totalJiffies = 0;
    if (host_statistics(mach_host_self(), HOST_CPU_LOAD_INFO, (host_info_t)&cpu_info, &count) ==
        KERN_SUCCESS) {
        totalJiffies = cpu_info.cpu_ticks[CPU_STATE_USER] + cpu_info.cpu_ticks[CPU_STATE_SYSTEM] +
                       cpu_info.cpu_ticks[CPU_STATE_NICE] + cpu_info.cpu_ticks[CPU_STATE_IDLE];
    }

    if (lastProcJiffies == 0 || lastTotalJiffies == 0 || procJiffies < lastProcJiffies ||
        totalJiffies < lastTotalJiffies) {
        // Initialize baseline
        lastProcJiffies = procJiffies;
        lastTotalJiffies = totalJiffies;
        return 0.0;
    }

    uint64_t dProc = procJiffies - lastProcJiffies;
    uint64_t dTotal = totalJiffies - lastTotalJiffies;
    lastProcJiffies = procJiffies;
    lastTotalJiffies = totalJiffies;

    if (dTotal == 0) {
        return 0.0;
    }

    double pct = (static_cast<double>(dProc) / static_cast<double>(dTotal)) * 100.0;
    long nprocs = sysconf(_SC_NPROCESSORS_ONLN);
    if (nprocs < 1)
        nprocs = 1;
    return std::clamp(pct, 0.0, 100.0 * static_cast<double>(nprocs));
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
    (void)getSnapshot(false);
}

MetricsSnapshot DaemonMetrics::getSnapshot(bool detailed) const {
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
        if (lifecycle_) {
            try {
                auto lsnap = lifecycle_->snapshot();
                out.ready = (lsnap.state == LifecycleState::Ready);
            } catch (...) {
                out.ready = false;
            }
        } else {
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
        LifecycleState state = LifecycleState::Unknown;
        std::string lastErr;
        if (lifecycle_) {
            auto s = lifecycle_->snapshot();
            state = s.state;
            lastErr = s.lastError;
        }
        switch (state) {
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
        out.lastError = lastErr;
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
                // New gauges: inflight and capacity
                try {
                    auto g = pq->gauges();
                    out.postIngestInflight = g.inflight;
                    out.postIngestCapacity = g.cap;
                } catch (...) {
                }
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
        // Tuning pool sizes
        try {
            out.ipcPoolSize = fsnap.ipcPoolSize;
            out.ioPoolSize = fsnap.ioPoolSize;
        } catch (...) {
        }
    } catch (...) {
    }
    int64_t muxQueuedBytesLocal = 0;
    try {
        auto msnap = MuxMetricsRegistry::instance().snapshot();
        out.muxActiveHandlers = msnap.activeHandlers;
        out.muxQueuedBytes = msnap.queuedBytes;
        out.muxWriterBudgetBytes = msnap.writerBudgetBytes;
#if defined(TRACY_ENABLE)
        TracyPlot("mux.queued.bytes", static_cast<double>(out.muxQueuedBytes));
        TracyPlot("mux.writer.budget", static_cast<double>(out.muxWriterBudgetBytes));
#endif
        // Fallback to a sane non-zero default when snapshot hasn't been initialized yet.
        if (out.muxWriterBudgetBytes == 0) {
            try {
                out.muxWriterBudgetBytes = TuneAdvisor::writerBudgetBytesPerTurn();
            } catch (...) {
                out.muxWriterBudgetBytes = 4096; // last-resort default
            }
        }
        muxQueuedBytesLocal = msnap.queuedBytes;
    } catch (...) {
    }
    // Provide a best-effort retryAfter hint for clients when post-ingest queue is saturated.
    try {
        if (services_) {
            if (auto* pq = services_->getPostIngestQueue()) {
                auto g = pq->gauges();
                if (g.cap > 0 && g.queued >= g.cap) {
                    // Suggest a small backoff based on tuning control interval
                    auto cfg = services_->getTuningConfig();
                    out.retryAfterMs = std::max<uint32_t>(50, cfg.controlIntervalMs / 4);
                } else {
                    out.retryAfterMs = 0;
                }
            }
        }
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
#if defined(TRACY_ENABLE)
        TracyPlot("daemon.mem.mb", out.memoryUsageMb);
        TracyPlot("daemon.cpu.pct", out.cpuUsagePercent);
#endif
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
#if defined(TRACY_ENABLE)
            // Per-subsystem plots (vector DB rows and file size bytes)
            if (out.vectorRowsExact > 0)
                TracyPlot("vector.rows", static_cast<double>(out.vectorRowsExact));
            if (out.vectorDbSizeBytes > 0)
                TracyPlot("vector.db.bytes", static_cast<double>(out.vectorDbSizeBytes));
#endif
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
            // Content store stats and sizes (logical always, deep stats when detailed)
            bool disableStoreStats = false;
            try {
                if (const char* env = std::getenv("YAMS_DISABLE_STORE_STATS")) {
                    std::string v(env);
                    std::transform(v.begin(), v.end(), v.begin(), ::tolower);
                    disableStoreStats = (v == "1" || v == "true" || v == "yes" || v == "on");
                }
            } catch (...) {
            }
            if (cs) {
                try {
                    auto ss = cs->getStats();
                    // Lightweight fields
                    out.storeObjects = ss.totalObjects;
                    out.logicalBytes = ss.totalBytes;              // logical (ingested) bytes
                    out.casUniqueRawBytes = ss.totalBytes;         // unique raw bytes seen by CAS
                    out.casDedupSavedBytes = ss.deduplicatedBytes; // bytes avoided via dedup
                    if (detailed && !disableStoreStats) {
                        out.uniqueBlocks = ss.uniqueBlocks;
                        out.deduplicatedBytes = ss.deduplicatedBytes;
                        out.compressionRatio = ss.dedupRatio();
                    }
                } catch (...) {
                }
            }
            // Physical size scan (TTL cached) only when detailed
            if (detailed) {
                try {
                    // Populate compression savings (process-wide monitor)
                    try {
                        auto& g = yams::compression::CompressionMonitor::getGlobalStats();
                        // Access atomics via load to avoid data races
                        out.casCompressSavedBytes = g.totalSpaceSaved.load();
                    } catch (...) {
                    }
                    auto now = std::chrono::steady_clock::now();
                    bool expired = true;
                    {
                        std::lock_guard<std::mutex> lk(cacheMutex_);
                        if (lastPhysicalAt_.time_since_epoch().count() != 0) {
                            auto age = std::chrono::duration_cast<std::chrono::milliseconds>(
                                           now - lastPhysicalAt_)
                                           .count();
                            expired = (age < 0) || (static_cast<uint32_t>(age) >= physicalTtlMs_);
                        }
                    }
                    if (expired) {
                        std::uint64_t total = 0;
                        std::uint64_t casObjectsBytes = 0;
                        std::uint64_t refsDbBytes = 0;
                        std::uint64_t dbBytes = 0;
                        std::uint64_t dbWalBytes = 0;
                        std::uint64_t dbShmBytes = 0;
                        std::uint64_t vecDbBytes = 0;
                        std::uint64_t vecIdxBytes = 0;
                        std::uint64_t tmpBytes = 0;
                        std::uint64_t indexBytes = 0;
                        std::error_code ec;
                        namespace fs = std::filesystem;
                        fs::path root;
                        try {
                            root = services_ ? (services_->getResolvedDataDir() / "storage")
                                             : fs::path{};
                        } catch (...) {
                        }
                        if (!root.empty() && fs::exists(root, ec)) {
                            for (fs::recursive_directory_iterator
                                     it(root, fs::directory_options::skip_permission_denied, ec),
                                 end;
                                 it != end; it.increment(ec)) {
                                if (ec) {
                                    ec.clear();
                                    continue;
                                }
                                if (it->is_regular_file(ec)) {
                                    std::uint64_t add = 0;
                                    try {
#ifdef __unix__
                                        struct stat st;
                                        if (::stat(it->path().c_str(), &st) == 0 &&
                                            st.st_blocks > 0) {
                                            add = static_cast<std::uint64_t>(st.st_blocks) * 512ULL;
                                        } else {
                                            add = static_cast<std::uint64_t>(it->file_size(ec));
                                        }
#else
                                        add = static_cast<std::uint64_t>(it->file_size(ec));
#endif
                                    } catch (...) {
                                        add = 0;
                                    }
                                    total += add;
                                    // attribute within storage/ subdirs
                                    auto p = it->path();
                                    if (p.string().find((root / "objects").string()) == 0) {
                                        casObjectsBytes += add;
                                    } else if (p.filename() == "refs.db") {
                                        refsDbBytes += add;
                                    } else if (p.string().find((root / "temp").string()) == 0) {
                                        tmpBytes += add;
                                    }
                                }
                            }
                        }
                        // Data dir (siblings to storage): yams.db (+WAL/SHM), vectors.db,
                        // vector_index.bin
                        try {
                            auto dd = services_ ? services_->getResolvedDataDir() : fs::path{};
                            if (!dd.empty()) {
                                auto sizeOf = [&](const fs::path& p) -> std::uint64_t {
                                    std::error_code e2;
                                    return fs::exists(p, e2)
                                               ? static_cast<std::uint64_t>(fs::file_size(p, e2))
                                               : 0ULL;
                                };
                                dbBytes = sizeOf(dd / "yams.db");
                                dbWalBytes = sizeOf(dd / "yams.db-wal");
                                dbShmBytes = sizeOf(dd / "yams.db-shm");
                                vecDbBytes = sizeOf(dd / "vectors.db");
                                vecIdxBytes = sizeOf(dd / "vector_index.bin");
                                // If search index is externalized under dataDir/search_index,
                                // attribute here
                                std::uint64_t extIndex = 0;
                                std::error_code e3;
                                fs::path idxRoot = dd / "search_index";
                                if (fs::exists(idxRoot, e3)) {
                                    for (fs::recursive_directory_iterator it(idxRoot, e3), end;
                                         it != end; it.increment(e3)) {
                                        if (e3)
                                            break;
                                        if (it->is_regular_file(e3))
                                            extIndex += static_cast<std::uint64_t>(
                                                fs::file_size(it->path(), e3));
                                    }
                                }
                                indexBytes = extIndex;
                            }
                        } catch (...) {
                        }
                        std::uint64_t metaBytes = refsDbBytes + dbBytes + dbWalBytes + dbShmBytes;
                        std::uint64_t vecBytes = vecDbBytes + vecIdxBytes;
                        std::uint64_t totalComputed =
                            casObjectsBytes + metaBytes + indexBytes + vecBytes + tmpBytes;
                        std::lock_guard<std::mutex> lk(cacheMutex_);
                        lastPhysicalBytes_ = (totalComputed > 0) ? totalComputed : total;
                        lastPhysicalAt_ = now;
                        // Stash breakdown into cached_ as well for consumers using non-detailed
                        // snapshot later
                        cached_.casPhysicalBytes = casObjectsBytes;
                        cached_.metadataPhysicalBytes = metaBytes;
                        cached_.indexPhysicalBytes = indexBytes;
                        cached_.vectorPhysicalBytes = vecBytes;
                        cached_.logsTmpPhysicalBytes = tmpBytes;
                        cached_.physicalTotalBytes = (totalComputed > 0) ? totalComputed : total;
                    }
                    out.physicalBytes = lastPhysicalBytes_;
                    // Also mirror breakdown to current output
                    out.casPhysicalBytes = cached_.casPhysicalBytes;
                    out.metadataPhysicalBytes = cached_.metadataPhysicalBytes;
                    out.indexPhysicalBytes = cached_.indexPhysicalBytes;
                    out.vectorPhysicalBytes = cached_.vectorPhysicalBytes;
                    out.logsTmpPhysicalBytes = cached_.logsTmpPhysicalBytes;
                    out.physicalTotalBytes = cached_.physicalTotalBytes;
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
            // Backend label and model details
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
                    // Prefer generator's own backend name for clarity (e.g., LocalONNX(GenAI),
                    // Hybrid, Daemon)
                    try {
                        out.embeddingBackend = gen->getBackendName();
                    } catch (...) {
                        out.embeddingBackend = "local";
                    }
                    // Best-effort local model path resolution
                    if (out.embeddingModelPath.empty() && !out.embeddingModel.empty()) {
                        try {
                            if (const char* home = std::getenv("HOME")) {
                                namespace fs = std::filesystem;
                                fs::path p = fs::path(home) / ".yams" / "models" /
                                             out.embeddingModel / "model.onnx";
                                std::error_code ec;
                                if (fs::exists(p, ec))
                                    out.embeddingModelPath = p.string();
                            }
                        } catch (...) {
                        }
                    }
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
    // Publish as shared snapshot for zero-copy readers
    try {
        MetricsSnapshotRegistry::instance().set(std::make_shared<const MetricsSnapshot>(out));
    } catch (...) {
    }

#if defined(TRACY_ENABLE)
    // Emit InternalEventBus drop counters as plots for quick backpressure visibility
    try {
        auto& bus = InternalEventBus::instance();
        TracyPlot("bus.embed.dropped", static_cast<double>(bus.embedDropped()));
        TracyPlot("bus.post.dropped", static_cast<double>(bus.postDropped()));
    } catch (...) {
    }
#endif
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
