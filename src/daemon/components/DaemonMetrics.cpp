#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/compression/compression_monitor.h>
#include <yams/daemon/components/admission_control.h>
#include <yams/daemon/components/CheckpointManager.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/dispatch_utils.hpp>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/MetricsSnapshotRegistry.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/SocketServer.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/ipc/fsm_metrics_registry.h>
#include <yams/daemon/ipc/mux_metrics_registry.h>
#include <yams/daemon/ipc/stream_metrics_registry.h>
#include <yams/daemon/metric_keys.h>
#include <yams/daemon/resource/OnnxConcurrencyRegistry.h>
#include <yams/search/search_tuner.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>
#include <yams/version.hpp>
#ifdef __unix__
#include <sys/stat.h>
#endif
#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN 1
#endif
#ifndef NOMINMAX
#define NOMINMAX 1
#endif
#include <Windows.h>

#include <Psapi.h>

#endif
#if defined(TRACY_ENABLE)
#include <tracy/Tracy.hpp>
#endif

#ifdef __APPLE__
#include <unistd.h>
#include <mach/mach.h>
#include <malloc/malloc.h>
#endif

namespace yams::daemon {

namespace {
struct MemoryUsageSample {
    double memoryUsageMb{0.0};
    std::map<std::string, std::uint64_t> breakdownBytes;
    std::map<std::string, std::uint64_t> diagnosticCounters;
};

bool envFlagEnabled(const char* name) {
    const char* env = std::getenv(name);
    if (!env || *env == '\0') {
        return false;
    }
    const std::string_view value{env};
    return value != "0" && value != "false" && value != "FALSE" && value != "off" && value != "OFF";
}

bool allocatorBreakdownEnabled() {
#ifdef YAMS_TESTING
    return envFlagEnabled("YAMS_STATUS_ALLOCATOR_BREAKDOWN");
#else
    static const bool enabled = envFlagEnabled("YAMS_STATUS_ALLOCATOR_BREAKDOWN");
    return enabled;
#endif
}

bool mallocStackLoggingEnabled() {
#ifdef YAMS_TESTING
    return envFlagEnabled("MallocStackLogging") || envFlagEnabled("MallocStackLoggingNoCompact");
#else
    static const bool enabled =
        envFlagEnabled("MallocStackLogging") || envFlagEnabled("MallocStackLoggingNoCompact");
    return enabled;
#endif
}

bool mallocStackLoggingNoCompactEnabled() {
#ifdef YAMS_TESTING
    return envFlagEnabled("MallocStackLoggingNoCompact");
#else
    static const bool enabled = envFlagEnabled("MallocStackLoggingNoCompact");
    return enabled;
#endif
}

#if defined(__APPLE__)
std::pair<std::uint64_t, std::uint64_t> sampleMallocStackLogFootprint() {
    namespace fs = std::filesystem;
    std::uint64_t bytes = 0;
    std::uint64_t files = 0;
    std::error_code ec;
    const auto pid = static_cast<long long>(::getpid());
    const auto prefix = std::string{"stack-logs."} + std::to_string(pid) + ".";

    fs::path dir{"/private/tmp"};
    if (!fs::exists(dir, ec)) {
        dir = fs::temp_directory_path(ec);
    }
    if (dir.empty() || ec) {
        return {0, 0};
    }

    for (fs::directory_iterator it(dir, fs::directory_options::skip_permission_denied, ec), end;
         !ec && it != end; it.increment(ec)) {
        const auto name = it->path().filename().string();
        if (name.rfind(prefix, 0) != 0) {
            continue;
        }
        ++files;
        if (it->is_regular_file(ec)) {
            bytes += it->file_size(ec);
            if (ec) {
                ec.clear();
            }
        }
    }
    return {bytes, files};
}
#endif

// Read Proportional Set Size (PSS) in kB from smaps_rollup when available (Linux), else 0.
static std::uint64_t readPssKb() {
#if defined(_WIN32)
    // Windows does not expose PSS cheaply; fall back to RSS/working set
    return 0;
#elif defined(__APPLE__)
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

// Read Resident Set Size (VmRSS/working set) in kB
static std::uint64_t readRssKb() {
#if defined(_WIN32)
    PROCESS_MEMORY_COUNTERS_EX pmc{};
    if (GetProcessMemoryInfo(GetCurrentProcess(), reinterpret_cast<PROCESS_MEMORY_COUNTERS*>(&pmc),
                             sizeof(pmc))) {
        return static_cast<std::uint64_t>(pmc.WorkingSetSize / 1024ULL);
    }
    return 0;
#elif defined(__APPLE__)
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

static MemoryUsageSample sampleMemoryUsage(bool includeAllocatorBreakdown) {
    MemoryUsageSample sample;
    const auto sampleStart = std::chrono::steady_clock::now();

    const std::uint64_t pss_kb = readPssKb();
    const std::uint64_t rss_kb = readRssKb();
    if (pss_kb > 0) {
        sample.memoryUsageMb = static_cast<double>(pss_kb) / 1024.0;
        sample.breakdownBytes["pss_bytes"] = pss_kb * 1024ull;
    } else if (rss_kb > 0) {
        sample.memoryUsageMb = static_cast<double>(rss_kb) / 1024.0;
    }
    if (rss_kb > 0) {
        sample.breakdownBytes["rss_bytes"] = rss_kb * 1024ull;
    }

#if defined(__APPLE__)
    const bool mslEnabled = mallocStackLoggingEnabled();
    const bool mslNoCompact = mallocStackLoggingNoCompactEnabled();
    if (mslEnabled) {
        sample.diagnosticCounters["msl_enabled"] = 1;
    }
    if (mslNoCompact) {
        sample.diagnosticCounters["msl_no_compact_enabled"] = 1;
    }

    task_vm_info_data_t info{};
    mach_msg_type_number_t count = TASK_VM_INFO_COUNT;
    if (task_info(mach_task_self(), TASK_VM_INFO, reinterpret_cast<task_info_t>(&info), &count) ==
        KERN_SUCCESS) {
        if (info.resident_size > 0) {
            sample.breakdownBytes["rss_bytes"] = info.resident_size;
        }
        if (info.resident_size_peak > 0) {
            sample.breakdownBytes["rss_peak_bytes"] = info.resident_size_peak;
        }
        if (info.phys_footprint > 0) {
            sample.breakdownBytes["phys_footprint_bytes"] = info.phys_footprint;
            sample.memoryUsageMb = static_cast<double>(info.phys_footprint) / (1024.0 * 1024.0);
        }
        if (info.internal > 0) {
            sample.breakdownBytes["internal_bytes"] = info.internal;
        }
        if (info.external > 0) {
            sample.breakdownBytes["external_bytes"] = info.external;
        }
        if (info.reusable > 0) {
            sample.breakdownBytes["reusable_bytes"] = info.reusable;
        }
        if (info.compressed > 0) {
            sample.breakdownBytes["compressed_bytes"] = info.compressed;
        }
        if (info.compressed_peak > 0) {
            sample.breakdownBytes["compressed_peak_bytes"] = info.compressed_peak;
        }
    }

    if (includeAllocatorBreakdown) {
        const auto allocatorStart = std::chrono::steady_clock::now();
        malloc_statistics_t mallocStats{};
        if (auto* defaultZone = malloc_default_zone(); defaultZone != nullptr) {
            malloc_zone_statistics(defaultZone, &mallocStats);
            if (mallocStats.size_allocated > 0) {
                sample.breakdownBytes["malloc_allocated_bytes"] = mallocStats.size_allocated;
            }
            if (mallocStats.size_in_use > 0) {
                sample.breakdownBytes["malloc_in_use_bytes"] = mallocStats.size_in_use;
            }
            if (mallocStats.max_size_in_use > 0) {
                sample.breakdownBytes["malloc_peak_in_use_bytes"] = mallocStats.max_size_in_use;
            }
            if (mallocStats.blocks_in_use > 0) {
                sample.breakdownBytes["malloc_blocks_in_use"] = mallocStats.blocks_in_use;
            }
        }
        sample.diagnosticCounters["allocator_sample_us"] =
            static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                           std::chrono::steady_clock::now() - allocatorStart)
                                           .count());
    }

    if (mslEnabled) {
        const auto mslStart = std::chrono::steady_clock::now();
        const auto [stackLogBytes, stackLogFiles] = sampleMallocStackLogFootprint();
        sample.diagnosticCounters["msl_stack_log_bytes"] = stackLogBytes;
        sample.diagnosticCounters["msl_stack_log_files"] = stackLogFiles;
        sample.diagnosticCounters["msl_stack_log_sample_us"] =
            static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                           std::chrono::steady_clock::now() - mslStart)
                                           .count());
    }
#endif

    sample.diagnosticCounters["memory_sample_us"] =
        static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                       std::chrono::steady_clock::now() - sampleStart)
                                       .count());
    return sample;
}

template <typename NumeratorT, typename DenominatorT>
double safeRatio(NumeratorT numerator, DenominatorT denominator) {
    const auto denom = static_cast<double>(denominator);
    if (denom <= 0.0) {
        return 0.0;
    }
    return static_cast<double>(numerator) / denom;
}

// Read CPU usage percent for the current process using platform deltas.
// Percent is relative to total system capacity (all CPUs) and normalized to 0..100.
// A single fully utilized core on a 4-core system will be ~25%.
double readCpuUsagePercent(std::uint64_t& lastProcJiffies, std::uint64_t& lastTotalJiffies) {
#if defined(_WIN32)
    FILETIME idleFT{}, kernelFT{}, userFT{};
    if (!GetSystemTimes(&idleFT, &kernelFT, &userFT)) {
        return 0.0;
    }
    FILETIME createFT{}, exitFT{}, procKernelFT{}, procUserFT{};
    if (!GetProcessTimes(GetCurrentProcess(), &createFT, &exitFT, &procKernelFT, &procUserFT)) {
        return 0.0;
    }
    auto to64 = [](const FILETIME& ft) {
        ULARGE_INTEGER li{};
        li.LowPart = ft.dwLowDateTime;
        li.HighPart = ft.dwHighDateTime;
        return li.QuadPart;
    };

    const std::uint64_t procJiffies = to64(procKernelFT) + to64(procUserFT);
    const std::uint64_t totalJiffies = to64(kernelFT) + to64(userFT);

    if (lastProcJiffies == 0 || lastTotalJiffies == 0 || procJiffies < lastProcJiffies ||
        totalJiffies < lastTotalJiffies) {
        lastProcJiffies = procJiffies;
        lastTotalJiffies = totalJiffies;
        return 0.0;
    }

    const std::uint64_t dProc = procJiffies - lastProcJiffies;
    const std::uint64_t dTotal = totalJiffies - lastTotalJiffies;
    lastProcJiffies = procJiffies;
    lastTotalJiffies = totalJiffies;
    if (dTotal == 0)
        return 0.0;

    double pct = (static_cast<double>(dProc) / static_cast<double>(dTotal)) * 100.0;
    return std::clamp(pct, 0.0, 100.0);
#elif defined(__APPLE__)
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
    return std::clamp(pct, 0.0, 100.0);
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

std::uint64_t age_ms(std::chrono::steady_clock::time_point now,
                     std::chrono::steady_clock::time_point then) {
    if (then.time_since_epoch().count() == 0) {
        return 0;
    }
    auto age = std::chrono::duration_cast<std::chrono::milliseconds>(now - then).count();
    return age > 0 ? static_cast<std::uint64_t>(age) : 0ULL;
}

} // namespace

DaemonMetrics::DaemonMetrics(const DaemonLifecycleFsm* lifecycle, const StateComponent* state,
                             const ServiceManager* services, WorkCoordinator* coordinator,
                             const SocketServer* socketServer)
    : lifecycle_(lifecycle), state_(state), services_(services), socketServer_(socketServer),
      strand_(coordinator->getExecutor()) {
    cacheMs_ = TuneAdvisor::metricsCacheMs();
}

void DaemonMetrics::setSocketServer(const SocketServer* socketServer) {
    {
        std::unique_lock lock(cacheMutex_);
        socketServer_ = socketServer;

        // Invalidate snapshot cache so newly-attached SocketServer details (e.g. proxy socket path)
        // become visible immediately to status callers.
        cachedSnapshot_.reset();
        cachedDetailedSnapshot_.reset();
        lastUpdate_ = {};
        lastDetailedUpdate_ = {};
    }
}

DaemonMetrics::~DaemonMetrics() {
    stopPolling();
}

void DaemonMetrics::startPolling() {
    if (pollingActive_.exchange(true)) {
        return; // Already running
    }
    // Mark as not stopped before spawning the coroutine
    {
        std::lock_guard<std::mutex> lk(pollingMutex_);
        pollingStopped_ = false;
    }
    boost::asio::co_spawn(strand_, pollingLoop(), boost::asio::detached);
}

void DaemonMetrics::stopPolling() {
    // Check if polling was ever started
    {
        std::lock_guard<std::mutex> lk(pollingMutex_);
        if (pollingStopped_) {
            return; // Not running or already stopped
        }
    }

    // Signal the polling loop to stop
    pollingActive_.store(false, std::memory_order_release);

    // Cancel the timer via the strand to wake up the coroutine
    // This must be done on the strand to avoid racing with async_wait
    boost::asio::post(strand_, [this]() {
        if (pollingTimer_) {
            pollingTimer_->cancel();
        }
    });

    // Wait for the polling loop to finish
    std::unique_lock<std::mutex> lk(pollingMutex_);
    pollingCv_.wait(lk, [this]() { return pollingStopped_; });
}

boost::asio::awaitable<void> DaemonMetrics::pollingLoop() {
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
    // Store timer pointer for cancellation from stopPolling()
    pollingTimer_ = &timer;
    spdlog::info("DaemonMetrics: polling loop started (interval={}ms)", cacheMs_);

    while (pollingActive_.load(std::memory_order_relaxed)) {
        try {
            // Poll CPU and memory here, and cache the results.
            {
                double cpu = readCpuUsagePercent(lastProcJiffies_, lastTotalJiffies_);
                const bool includeAllocatorBreakdown =
                    allocatorBreakdownEnabled() || mallocStackLoggingEnabled();
                auto memSample = sampleMemoryUsage(includeAllocatorBreakdown);

                std::unique_lock lock(cacheMutex_);
                cached_.cpuUsagePercent = cpu;
                cached_.memoryUsageMb = memSample.memoryUsageMb;
                cached_.memoryBreakdownBytes = std::move(memSample.breakdownBytes);
                cached_.diagnosticCounters = std::move(memSample.diagnosticCounters);
            }

            // Update expensive DB counts in background (separate from snapshot cache)
            auto now = std::chrono::steady_clock::now();
            bool updateCounts = false;
            {
                std::shared_lock lk(cacheMutex_);
                if (lastDocCountsAt_.time_since_epoch().count() == 0) {
                    updateCounts = true; // First time
                } else {
                    auto age = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   now - lastDocCountsAt_)
                                   .count();
                    updateCounts = (age < 0) || (static_cast<uint32_t>(age) >= docCountsTtlMs_);
                }
            }

            if (updateCounts) {
                try {
                    auto mr = services_ ? services_->getMetadataRepo() : nullptr;
                    auto vdb = services_ ? services_->getVectorDatabase() : nullptr;

                    uint64_t total = 0, indexed = 0, extracted = 0, embedded = 0, vectorRows = 0;

                    // Read from component-owned metrics (no DB queries!)
                    if (mr) {
                        total = mr->getCachedDocumentCount();
                        indexed = mr->getCachedIndexedCount();
                        extracted = mr->getCachedExtractedCount();
                        embedded = mr->getCachedEmbeddedCount();
                    }

                    if (vdb && vdb->isInitialized()) {
                        vectorRows = vdb->getVectorCount(); // Returns cached count (no DB query)
                    }

                    std::unique_lock lock(cacheMutex_);
                    cachedDocumentsTotal_ = total;
                    cachedDocumentsIndexed_ = indexed;
                    cachedDocumentsExtracted_ = extracted;
                    cachedDocumentsEmbedded_ = embedded;
                    cachedVectorRows_ = vectorRows;
                    lastDocCountsAt_ = now;
                } catch (const std::exception& e) {
                    spdlog::debug("DaemonMetrics: failed to update document counts: {}", e.what());
                } catch (...) {
                    spdlog::debug("DaemonMetrics: failed to update document counts (unknown)");
                }

                // Check if search engine needs re-tuning based on corpus growth
                // Delegate to SearchComponent which handles rate limiting and atomic guards
                try {
                    if (services_) {
                        if (auto* searchComp = services_->getSearchComponent()) {
                            // Allow disabling rebuild checks for high-scale benchmarks.
                            // Rebuilds compete with ingestion/post-ingest and can dominate runtime.
                            bool disableRebuilds = false;
                            try {
                                if (const char* env = std::getenv("YAMS_DISABLE_SEARCH_REBUILDS")) {
                                    std::string v(env);
                                    std::transform(v.begin(), v.end(), v.begin(), ::tolower);
                                    disableRebuilds =
                                        (v == "1" || v == "true" || v == "yes" || v == "on");
                                }
                            } catch (...) {
                            }
                            if (!disableRebuilds) {
                                searchComp->checkAndTriggerRebuildIfNeeded();
                            }
                        }
                    }
                } catch (...) {
                }
            }

            // Update physical filesystem stats in background (TTL-based)
            bool updatePhysical = false;
            {
                std::shared_lock lk(cacheMutex_);
                if (lastPhysicalAt_.time_since_epoch().count() == 0) {
                    updatePhysical = true; // First time
                } else {
                    auto age =
                        std::chrono::duration_cast<std::chrono::milliseconds>(now - lastPhysicalAt_)
                            .count();
                    updatePhysical = (age < 0) || (static_cast<uint32_t>(age) >= physicalTtlMs_);
                }
            }

            if (updatePhysical) {
                try {
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
                        root =
                            services_ ? (services_->getResolvedDataDir() / "storage") : fs::path{};
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
                                    if (::stat(it->path().c_str(), &st) == 0 && st.st_blocks > 0) {
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
                            // If search index is externalized under dataDir/search_index, attribute
                            // here
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

                    std::unique_lock lock(cacheMutex_);
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
                } catch (const std::exception& e) {
                    spdlog::debug("DaemonMetrics: failed to update physical stats: {}", e.what());
                } catch (...) {
                    spdlog::debug("DaemonMetrics: failed to update physical stats (unknown)");
                }
            }

            // Refresh cached snapshots in background so request-time status calls can return
            // immediately without synchronous recompute.
            auto fastSnapshot = std::make_shared<MetricsSnapshot>(collectSnapshot(false));
            const auto publishedAt = std::chrono::steady_clock::now();
            bool refreshDetailed =
                detailedRefreshQueued_.exchange(false, std::memory_order_acq_rel);
            {
                std::shared_lock lock(cacheMutex_);
                if (!cachedDetailedSnapshot_ ||
                    lastDetailedUpdate_.time_since_epoch().count() == 0) {
                    refreshDetailed = true;
                } else if (!refreshDetailed) {
                    refreshDetailed = age_ms(publishedAt, lastDetailedUpdate_) >=
                                      static_cast<std::uint64_t>(tunerStateTtlMs_);
                }
            }
            std::shared_ptr<MetricsSnapshot> detailedSnapshot;
            if (refreshDetailed) {
                detailedSnapshot = std::make_shared<MetricsSnapshot>(collectSnapshot(true));
            }
            {
                std::unique_lock lock(cacheMutex_);
                cachedSnapshot_ = fastSnapshot;
                lastUpdate_ = publishedAt;
                if (detailedSnapshot) {
                    cachedDetailedSnapshot_ = detailedSnapshot;
                    lastDetailedUpdate_ = publishedAt;
                }
            }
            refreshQueued_.store(false, std::memory_order_release);
            try {
                MetricsSnapshotRegistry::instance().set(fastSnapshot);
            } catch (...) {
            }
        } catch (const std::exception& e) {
            spdlog::warn("DaemonMetrics: polling iteration failed: {}", e.what());
        } catch (...) {
            spdlog::warn("DaemonMetrics: polling iteration failed (unknown exception)");
        }

        // Sleep for cache interval using async timer
        timer.expires_after(std::chrono::milliseconds(cacheMs_));
        auto [ec] = co_await timer.async_wait(boost::asio::as_tuple(boost::asio::use_awaitable));
        // If timer was cancelled (e.g., by stopPolling), exit the loop
        if (ec == boost::asio::error::operation_aborted) {
            break;
        }
    }
    // Clear timer pointer and signal that polling has stopped
    pollingTimer_ = nullptr;
    {
        std::lock_guard<std::mutex> lk(pollingMutex_);
        pollingStopped_ = true;
    }
    pollingCv_.notify_one();
    spdlog::info("DaemonMetrics: polling loop stopped");
}

void DaemonMetrics::refresh() {
    scheduleBackgroundRefresh(false);
}

std::shared_ptr<const MetricsSnapshot> DaemonMetrics::getSnapshot(bool detailed) const {
    return buildDecoratedSnapshot(detailed);
}

DaemonMetrics::CachedSnapshotState DaemonMetrics::loadCachedSnapshotState() const {
    CachedSnapshotState state;
    std::shared_lock lock(cacheMutex_);
    state.fast = cachedSnapshot_;
    state.detailed = cachedDetailedSnapshot_;
    state.fastAt = lastUpdate_;
    state.detailedAt = lastDetailedUpdate_;
    return state;
}

DaemonMetrics::SnapshotFreshness
DaemonMetrics::evaluateSnapshotFreshness(const CachedSnapshotState& state,
                                         std::chrono::steady_clock::time_point now) const {
    SnapshotFreshness freshness;
    freshness.snapshotAgeMs = age_ms(now, state.fastAt);
    freshness.snapshotStale =
        state.fastAt.time_since_epoch().count() == 0 ||
        (cacheMs_ > 0 && freshness.snapshotAgeMs >= static_cast<std::uint64_t>(cacheMs_));

    freshness.detailAvailable = (state.detailed != nullptr);
    freshness.detailAgeMs = freshness.detailAvailable ? age_ms(now, state.detailedAt) : 0ULL;
    const auto freshnessBudgetMs = static_cast<std::uint64_t>(std::max<uint32_t>(cacheMs_, 1));
    freshness.detailStale = !freshness.detailAvailable ||
                            state.detailedAt.time_since_epoch().count() == 0 ||
                            (freshness.detailAgeMs >= freshnessBudgetMs);
    return freshness;
}

void DaemonMetrics::applySnapshotFreshness(MetricsSnapshot& snapshot,
                                           const SnapshotFreshness& freshness) const {
    snapshot.statusSnapshotAgeMs = freshness.snapshotAgeMs;
    snapshot.statusSnapshotStale = freshness.snapshotStale;
    snapshot.statusDetailAvailable = freshness.detailAvailable;
    snapshot.statusDetailAgeMs = freshness.detailAgeMs;
    snapshot.statusDetailStale = freshness.detailStale;
}

void DaemonMetrics::publishSnapshotPair(const std::shared_ptr<MetricsSnapshot>& fastSnapshot,
                                        const std::shared_ptr<MetricsSnapshot>& detailedSnapshot,
                                        std::chrono::steady_clock::time_point publishedAt) const {
    {
        std::unique_lock lock(cacheMutex_);
        cachedSnapshot_ = fastSnapshot;
        lastUpdate_ = publishedAt;
        if (detailedSnapshot) {
            cachedDetailedSnapshot_ = detailedSnapshot;
            lastDetailedUpdate_ = publishedAt;
        }
    }

    try {
        MetricsSnapshotRegistry::instance().set(fastSnapshot);
    } catch (...) {
    }
}

std::shared_ptr<const MetricsSnapshot>
DaemonMetrics::collectAndPublishSnapshot(bool detailed) const {
    auto publishedAt = std::chrono::steady_clock::now();
    auto fastSnapshot = std::make_shared<MetricsSnapshot>(collectSnapshot(false));
    std::shared_ptr<MetricsSnapshot> detailedSnapshot;
    if (detailed) {
        detailedSnapshot = std::make_shared<MetricsSnapshot>(collectSnapshot(true));
    }

    publishSnapshotPair(fastSnapshot, detailedSnapshot, publishedAt);

    MetricsSnapshot out = detailed ? *detailedSnapshot : *fastSnapshot;
    SnapshotFreshness freshness;
    freshness.snapshotAgeMs = 0;
    freshness.snapshotStale = false;
    freshness.detailAgeMs = 0;
    freshness.detailStale = !detailed;
    freshness.detailAvailable = detailed;
    applySnapshotFreshness(out, freshness);
    return std::make_shared<const MetricsSnapshot>(std::move(out));
}

MetricsSnapshot DaemonMetrics::buildMinimalSnapshot() const {
    MetricsSnapshot out;
    out.version = YAMS_VERSION_STRING;
    try {
        auto now = std::chrono::steady_clock::now();
        auto uptime = now - state_->stats.startTime;
        out.uptimeSeconds = static_cast<std::size_t>(
            std::chrono::duration_cast<std::chrono::seconds>(uptime).count());
        out.requestsProcessed = state_->stats.requestsProcessed.load();
        out.activeConnections = state_->stats.activeConnections.load();
        out.maxConnections = state_->stats.maxConnections.load();
        out.connectionSlotsFree = state_->stats.connectionSlotsFree.load();
        // Snapshot SocketServer-derived metrics under the same lock used by setSocketServer() so
        // stop/reset cannot race with these raw-pointer reads.
        {
            std::shared_lock lock(cacheMutex_);
            out.oldestConnectionAge =
                socketServer_ ? socketServer_->oldestConnectionAgeSeconds() : 0;
            if (socketServer_) {
                out.proxyActiveConnections = socketServer_->proxyActiveConnections();
                out.proxySocketPath = socketServer_->proxySocketPath().string();
            }
        }
        out.forcedCloseCount = state_->stats.forcedCloseCount.load();
        out.ipcTasksPending = state_->stats.ipcTasksPending.load();
        out.ipcTasksActive = state_->stats.ipcTasksActive.load();
        out.repairRunning = state_->stats.repairRunning.load(std::memory_order_relaxed);
        out.repairInProgress = state_->stats.repairInProgress.load(std::memory_order_relaxed);
        out.repairQueueDepth = state_->stats.repairQueueDepth.load(std::memory_order_relaxed);
        out.repairBatchesAttempted =
            state_->stats.repairBatchesAttempted.load(std::memory_order_relaxed);
        out.repairEmbeddingsGenerated =
            state_->stats.repairEmbeddingsGenerated.load(std::memory_order_relaxed);
        out.repairEmbeddingsSkipped =
            state_->stats.repairEmbeddingsSkipped.load(std::memory_order_relaxed);
        out.repairFailedOperations =
            state_->stats.repairFailedOperations.load(std::memory_order_relaxed);
        out.repairIdleTicks = state_->stats.repairIdleTicks.load(std::memory_order_relaxed);
        out.repairBusyTicks = state_->stats.repairBusyTicks.load(std::memory_order_relaxed);
        out.repairTotalBacklog = state_->stats.repairTotalBacklog.load(std::memory_order_relaxed);
        out.repairProcessed = state_->stats.repairProcessed.load(std::memory_order_relaxed);
        out.repairCurrentOperationCode =
            state_->stats.repairCurrentOperationCode.load(std::memory_order_relaxed);
        const auto repairStartedMs =
            state_->stats.repairCurrentOperationStartedMs.load(std::memory_order_relaxed);
        if (out.repairCurrentOperationCode > 0 && repairStartedMs > 0) {
            const auto nowMs =
                static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                               std::chrono::steady_clock::now().time_since_epoch())
                                               .count());
            out.repairCurrentOperationElapsedMs =
                nowMs > repairStartedMs ? nowMs - repairStartedMs : 0;
        }
        if (services_) {
            try {
                out.workerActive = services_->getWorkerActive();
                out.workerQueued = services_->getWorkerQueueDepth();
            } catch (...) {
            }
            try {
                if (auto pq = services_->getPostIngestQueue()) {
                    out.postIngestQueued = pq->size();
                    out.postIngestInflight = pq->totalInFlight();
                    out.postIngestCapacity = pq->capacity();
                    const std::size_t rpcCap =
                        static_cast<std::size_t>(TuneAdvisor::postIngestRpcQueueMax());
                    out.postIngestRpcCapacity = rpcCap;
                    if (rpcCap > 0) {
                        auto rpcCh =
                            InternalEventBus::instance()
                                .get_channel<InternalEventBus::PostIngestTask>("post_ingest_rpc");
                        if (rpcCh) {
                            out.postIngestRpcQueued = rpcCh->size_approx();
                        }
                    }
                }
            } catch (...) {
            }
            try {
                auto searchLoad = services_->getSearchLoadMetrics();
                out.searchActive = searchLoad.active;
                out.searchQueued = searchLoad.queued;
                out.searchExecuted = searchLoad.executed;
            } catch (...) {
            }
            auto topology = services_->getTopologyTelemetrySnapshot();
            out.topologyRebuildRunning = topology.rebuildRunning;
            out.topologyArtifactsFresh = topology.artifactsFresh;
            out.topologyLastRunSucceeded = topology.lastRunSucceeded;
            out.topologyLastRunSkipped = topology.lastRunSkipped;
            out.topologyLastRunFullRebuild = topology.lastRunFullRebuild;
            out.topologyLastRunStored = topology.lastRunStored;
            out.topologyDirtyDocuments = topology.dirtyDocumentCount;
            out.topologyLastSuccessAgeMs = topology.lastSuccessAgeMs;
            out.topologyRebuildLagMs = topology.rebuildLagMs;
            out.topologyRebuildRunningAgeMs = topology.rebuildRunningAgeMs;
            out.topologyLastDurationMs = topology.lastDurationMs;
            out.topologyRebuildsTotal = topology.rebuildsTotal;
            out.topologyRebuildFailuresTotal = topology.rebuildFailuresTotal;
            out.topologyLastDocumentsRequested = topology.lastDocumentsRequested;
            out.topologyLastDocumentsProcessed = topology.lastDocumentsProcessed;
            out.topologyLastDocumentsMissingEmbeddings = topology.lastDocumentsMissingEmbeddings;
            out.topologyLastDocumentsMissingGraphNodes = topology.lastDocumentsMissingGraphNodes;
            out.topologyLastClustersBuilt = topology.lastClustersBuilt;
            out.topologyLastMembershipsBuilt = topology.lastMembershipsBuilt;
            out.topologyLastDirtySeedCount = topology.lastDirtySeedCount;
            out.topologyLastDirtyRegionDocs = topology.lastDirtyRegionDocs;
            out.topologyLastCoalescedDirtySets = topology.lastCoalescedDirtySets;
            out.topologyLastFallbackFullRebuilds = topology.lastFallbackFullRebuilds;
            out.topologyLastReason = topology.lastReason;
            out.topologyLastSnapshotId = topology.lastSnapshotId;
            out.topologyLastAlgorithm = topology.lastAlgorithm;
        }
    } catch (...) {
    }
    return out;
}

std::shared_ptr<const MetricsSnapshot> DaemonMetrics::buildDecoratedSnapshot(bool detailed) const {
    auto now = std::chrono::steady_clock::now();

    const auto state = loadCachedSnapshotState();
    if (!state.fast) {
        return collectAndPublishSnapshot(detailed);
    }

    MetricsSnapshot out = *state.fast;
    if (detailed && state.detailed) {
        out = *state.detailed;
    }

    const auto freshness = evaluateSnapshotFreshness(state, now);
    if (freshness.needsSyncRefresh(detailed)) {
        return collectAndPublishSnapshot(detailed);
    }

    applySnapshotFreshness(out, freshness);
    return std::make_shared<const MetricsSnapshot>(std::move(out));
}

void DaemonMetrics::scheduleBackgroundRefresh(bool detailed) const {
    auto& queued = detailed ? detailedRefreshQueued_ : refreshQueued_;
    if (!pollingActive_.load(std::memory_order_acquire)) {
        return;
    }
    if (queued.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    boost::asio::post(strand_, [this]() {
        if (pollingTimer_) {
            pollingTimer_->cancel();
        }
    });
}

MetricsSnapshot DaemonMetrics::collectSnapshot(bool detailed) const {
    MetricsSnapshot out;
    out.version = YAMS_VERSION_STRING;
    populateCommonSnapshot(out, detailed);
    if (detailed) {
        enrichDetailedSnapshot(out);
    }
    return out;
}

void DaemonMetrics::populateCommonSnapshot(MetricsSnapshot& out, bool detailed) const {
    // Uptime and counters
    try {
        auto now = std::chrono::steady_clock::now();
        auto uptime = now - state_->stats.startTime;
        out.uptimeSeconds = static_cast<std::size_t>(
            std::chrono::duration_cast<std::chrono::seconds>(uptime).count());
        out.requestsProcessed = state_->stats.requestsProcessed.load();
        out.activeConnections = state_->stats.activeConnections.load();
        out.maxConnections = state_->stats.maxConnections.load();
        out.connectionSlotsFree = state_->stats.connectionSlotsFree.load();
        {
            std::shared_lock lock(cacheMutex_);
            out.oldestConnectionAge =
                socketServer_ ? socketServer_->oldestConnectionAgeSeconds() : 0;
            if (socketServer_) {
                out.proxyActiveConnections = socketServer_->proxyActiveConnections();
                out.proxySocketPath = socketServer_->proxySocketPath().string();
            }
        }
        out.forcedCloseCount = state_->stats.forcedCloseCount.load();
        out.ipcTasksPending = state_->stats.ipcTasksPending.load();
        out.ipcTasksActive = state_->stats.ipcTasksActive.load();
        out.repairRunning = state_->stats.repairRunning.load(std::memory_order_relaxed);
        out.repairInProgress = state_->stats.repairInProgress.load(std::memory_order_relaxed);
        out.repairQueueDepth = state_->stats.repairQueueDepth.load(std::memory_order_relaxed);
        out.repairBatchesAttempted =
            state_->stats.repairBatchesAttempted.load(std::memory_order_relaxed);
        out.repairEmbeddingsGenerated =
            state_->stats.repairEmbeddingsGenerated.load(std::memory_order_relaxed);
        out.repairEmbeddingsSkipped =
            state_->stats.repairEmbeddingsSkipped.load(std::memory_order_relaxed);
        out.repairFailedOperations =
            state_->stats.repairFailedOperations.load(std::memory_order_relaxed);
        out.repairIdleTicks = state_->stats.repairIdleTicks.load(std::memory_order_relaxed);
        out.repairBusyTicks = state_->stats.repairBusyTicks.load(std::memory_order_relaxed);
        out.repairTotalBacklog = state_->stats.repairTotalBacklog.load(std::memory_order_relaxed);
        out.repairProcessed = state_->stats.repairProcessed.load(std::memory_order_relaxed);
        out.repairCurrentOperationCode =
            state_->stats.repairCurrentOperationCode.load(std::memory_order_relaxed);
        const auto repairStartedMs =
            state_->stats.repairCurrentOperationStartedMs.load(std::memory_order_relaxed);
        if (out.repairCurrentOperationCode > 0 && repairStartedMs > 0) {
            const auto nowMs =
                static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                               std::chrono::steady_clock::now().time_since_epoch())
                                               .count());
            out.repairCurrentOperationElapsedMs =
                nowMs > repairStartedMs ? nowMs - repairStartedMs : 0;
        }
        if (services_) {
            auto topology = services_->getTopologyTelemetrySnapshot();
            out.topologyRebuildRunning = topology.rebuildRunning;
            out.topologyArtifactsFresh = topology.artifactsFresh;
            out.topologyLastRunSucceeded = topology.lastRunSucceeded;
            out.topologyLastRunSkipped = topology.lastRunSkipped;
            out.topologyLastRunFullRebuild = topology.lastRunFullRebuild;
            out.topologyLastRunStored = topology.lastRunStored;
            out.topologyDirtyDocuments = topology.dirtyDocumentCount;
            out.topologyLastSuccessAgeMs = topology.lastSuccessAgeMs;
            out.topologyRebuildLagMs = topology.rebuildLagMs;
            out.topologyRebuildRunningAgeMs = topology.rebuildRunningAgeMs;
            out.topologyLastDurationMs = topology.lastDurationMs;
            out.topologyRebuildsTotal = topology.rebuildsTotal;
            out.topologyRebuildFailuresTotal = topology.rebuildFailuresTotal;
            out.topologyLastDocumentsRequested = topology.lastDocumentsRequested;
            out.topologyLastDocumentsProcessed = topology.lastDocumentsProcessed;
            out.topologyLastDocumentsMissingEmbeddings = topology.lastDocumentsMissingEmbeddings;
            out.topologyLastDocumentsMissingGraphNodes = topology.lastDocumentsMissingGraphNodes;
            out.topologyLastClustersBuilt = topology.lastClustersBuilt;
            out.topologyLastMembershipsBuilt = topology.lastMembershipsBuilt;
            out.topologyLastDirtySeedCount = topology.lastDirtySeedCount;
            out.topologyLastDirtyRegionDocs = topology.lastDirtyRegionDocs;
            out.topologyLastCoalescedDirtySets = topology.lastCoalescedDirtySets;
            out.topologyLastFallbackFullRebuilds = topology.lastFallbackFullRebuilds;
            out.topologyLastReason = topology.lastReason;
            out.topologyLastSnapshotId = topology.lastSnapshotId;
            out.topologyLastAlgorithm = topology.lastAlgorithm;
        }
    } catch (...) {
    }

    // Readiness flags and progress
    try {
        // Align boolean readiness with lifecycle readiness rather than bootstrap-only summaries.
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
        out.readinessStates[std::string(readiness::kIpcServer)] =
            state_->readiness.ipcServerReady.load();
        out.readinessStates[std::string(readiness::kContentStore)] =
            state_->readiness.contentStoreReady.load();
        out.readinessStates[std::string(readiness::kDatabase)] =
            state_->readiness.databaseReady.load();
        out.readinessStates[std::string(readiness::kMetadataRepo)] =
            state_->readiness.metadataRepoReady.load();
        out.readinessStates[std::string(readiness::kSearchEngine)] =
            state_->readiness.searchEngineReady.load();
        out.readinessStates[std::string(readiness::kModelProvider)] =
            state_->readiness.modelProviderReady.load();
        out.readinessStates[std::string(readiness::kVectorIndex)] =
            state_->readiness.vectorIndexReady.load();
        out.readinessStates[std::string(readiness::kVectorDb)] =
            state_->readiness.vectorDbReady.load();
        out.readinessStates[std::string(readiness::kVectorDbInitAttempted)] =
            state_->readiness.vectorDbInitAttempted.load();
        out.readinessStates[std::string(readiness::kVectorDbReady)] =
            state_->readiness.vectorDbReady.load();
        out.readinessStates[std::string(readiness::kVectorDbDim)] =
            state_->readiness.vectorDbDim.load() > 0;
        out.readinessStates[std::string(readiness::kPlugins)] =
            state_->readiness.pluginsReady.load();
        out.readinessStates[std::string(readiness::kRepairService)] = out.repairRunning;
        // Topology freshness/rebuild state is useful telemetry, but not a daemon-readiness gate.
        // Keep it in the detailed metrics fields, not in the readiness summary.
        // Only include search init progress while not fully ready or when progress < 100%
        const bool searchReady = state_->readiness.searchEngineReady.load();
        const int searchPct = std::clamp<int>(state_->readiness.searchProgress.load(), 0, 100);
        if (!searchReady || searchPct < 100) {
            out.initProgress[std::string(readiness::kSearchEngine)] =
                static_cast<uint8_t>(searchPct);
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
            if (auto pq = services_->getPostIngestQueue()) {
                out.postIngestThreads = 1; // Strand-based now, conceptually 1 "thread"
                out.postIngestQueued = pq->size();
                out.postIngestInflight = pq->totalInFlight();
                out.postIngestCapacity = pq->capacity();

                // KG backpressure observability
                try {
                    out.postIngestBackpressureRejects = pq->backpressureRejects();
                    std::size_t kgDepth = 0;
                    std::size_t kgCap = 0;
                    out.kgJobsFillRatio = pq->kgFillRatio(&kgDepth, &kgCap);
                    out.kgJobsDepth = kgDepth;
                    out.kgJobsCapacity = kgCap;
                } catch (...) {
                }

                // High-priority post-ingest channel for repair/stuck-doc recovery.
                // Metrics are best-effort: if the channel is disabled (capacity=0) or
                // not created yet, values remain 0.
                try {
                    const std::size_t rpcCap =
                        static_cast<std::size_t>(TuneAdvisor::postIngestRpcQueueMax());
                    out.postIngestRpcCapacity = rpcCap;
                    out.postIngestRpcMaxPerBatch =
                        static_cast<std::size_t>(TuneAdvisor::postIngestRpcMaxPerBatch());
                    if (rpcCap > 0) {
                        auto rpcCh =
                            InternalEventBus::instance()
                                .get_channel<InternalEventBus::PostIngestTask>("post_ingest_rpc");
                        if (rpcCh) {
                            out.postIngestRpcQueued = rpcCh->size_approx();
                        }
                    }
                } catch (...) {
                }
                out.postIngestProcessed = pq->processed();
                out.postIngestFailed = pq->failed();
                out.postIngestLatencyMsEma = pq->latencyMsEma();
                out.postIngestRateSecEma = pq->ratePerSecEma();
                // Per-stage inflight counts
                out.extractionInFlight = pq->extractionInFlight();
                out.kgInFlight = pq->kgInFlight();
                out.symbolInFlight = pq->symbolInFlight();
                out.entityInFlight = pq->entityInFlight();
                // File/directory add tracking
                out.filesAdded = pq->filesAdded();
                out.directoriesAdded = pq->directoriesAdded();
                out.filesProcessed = pq->filesProcessed();
                out.directoriesProcessed = pq->directoriesProcessed();
                // Per-stage queue depths (approximate, from channel sizes)
                out.kgQueueDepth = pq->kgQueueDepth();
                out.symbolQueueDepth = pq->symbolQueueDepth();
                out.entityQueueDepth = pq->entityQueueDepth();
                out.titleQueueDepth = pq->titleQueueDepth();
                out.titleInFlight = pq->titleInFlight();
                out.titleConcurrencyLimit = PostIngestQueue::maxTitleConcurrent();
                // Dynamic concurrency limits (PBI-05a)
                out.postExtractionLimit = TuneAdvisor::postExtractionConcurrent();
                out.postKgLimit = TuneAdvisor::postKgConcurrent();
                out.postSymbolLimit = TuneAdvisor::postSymbolConcurrent();
                out.postEntityLimit = TuneAdvisor::postEntityConcurrent();
                out.postEmbedLimit = TuneAdvisor::postEmbedConcurrent();
                // Combined enrich (symbol+entity+title)
                out.postEnrichLimit = TuneAdvisor::postSymbolConcurrent() +
                                      TuneAdvisor::postEntityConcurrent() +
                                      TuneAdvisor::postTitleConcurrent();
                out.enrichInflight =
                    pq->symbolInFlight() + pq->entityInFlight() + pq->titleInFlight();
                out.enrichQueueDepth =
                    pq->symbolQueueDepth() + pq->entityQueueDepth() + pq->titleQueueDepth();

                // Gradient limiter per-stage metrics
                out.gradientLimitersEnabled = TuneAdvisor::enableGradientLimiters();
                if (out.gradientLimitersEnabled) {
                    auto readLimiter =
                        [](GradientLimiter* lim) -> MetricsSnapshot::GradientLimiterMetrics {
                        if (!lim)
                            return {};
                        auto m = lim->metrics();
                        return {m.limit,    m.smoothedRtt,  m.gradient,
                                m.inFlight, m.acquireCount, m.rejectCount};
                    };
                    out.glExtraction = readLimiter(pq->extractionLimiter());
                    out.glKg = readLimiter(pq->kgLimiter());
                    out.glSymbol = readLimiter(pq->symbolLimiter());
                    out.glEntity = readLimiter(pq->entityLimiter());
                    out.glTitle = readLimiter(pq->titleLimiter());
                    out.glEmbed = readLimiter(pq->embedLimiter());
                }
            }
            auto& bus = InternalEventBus::instance();
            out.kgQueued = bus.kgQueued();
            out.kgDropped = bus.kgDropped();
            out.kgConsumed = bus.kgConsumed();
            // Entity extraction metrics (from InternalEventBus counters)
            out.entityQueued = bus.entityQueued();
            out.entityDropped = bus.entityDropped();
            out.entityConsumed = bus.entityConsumed();
            // GC pipeline metrics (from InternalEventBus)
            out.gcQueued = bus.gcQueued();
            out.gcDropped = bus.gcDropped();
            out.gcConsumed = bus.gcConsumed();
            // Entity graph pipeline metrics (from InternalEventBus)
            out.entityGraphQueued = bus.entityGraphQueued();
            out.entityGraphDropped = bus.entityGraphDropped();
            out.entityGraphConsumed = bus.entityGraphConsumed();
        } else {
            out.workerThreads = std::max(1u, std::thread::hardware_concurrency());
        }
    } catch (...) {
    }

    // Session watch status (best-effort)
    try {
        if (services_) {
            auto appCtx = services_->getAppContext();
            auto sess = yams::app::services::makeSessionService(&appCtx);
            if (sess) {
                auto current = sess->current();
                if (current && !current->empty()) {
                    out.watchSession = *current;
                    out.watchEnabled = sess->watchEnabled(*current);
                    out.watchIntervalMs = sess->watchIntervalMs(*current);
                    if (out.watchEnabled) {
                        auto pinned = sess->getPinnedPatterns(*current);
                        if (!pinned.empty()) {
                            out.watchRoot = pinned.front();
                        }
                    }
                }
            }
        }
    } catch (...) {
    }

    // FSM transport metrics (best-effort)
    try {
        auto fsnap = FsmMetricsRegistry::instance().snapshot();
        out.fsmTransitions = fsnap.transitions;
        out.fsmHeaderReads = fsnap.headerReads;
        out.fsmPayloadReads = fsnap.payloadReads;
        out.fsmPayloadWrites = fsnap.payloadWrites;
        out.fsmBytesSent = fsnap.bytesSent;
        out.fsmBytesReceived = fsnap.bytesReceived;
        out.fsmTimeouts = fsnap.timeouts;
        out.fsmRetries = fsnap.retries;
        out.fsmErrors = fsnap.errors;
        out.ipcPoolSize = fsnap.ipcPoolSize;
        out.ioPoolSize = fsnap.ioPoolSize;
    } catch (...) {
    }

    // ResourceGovernor metrics (direct query)
    try {
        auto govSnap = ResourceGovernor::instance().getSnapshot();
        out.governorRssBytes = govSnap.rssBytes;
        out.governorBudgetBytes = govSnap.memoryBudgetBytes;
        out.governorPressureLevel = static_cast<uint8_t>(govSnap.level);
        out.governorHeadroomPct = static_cast<uint8_t>(govSnap.scalingHeadroom * 100.0);
    } catch (...) {
    }

    // ONNX concurrency metrics (direct query)
    try {
        auto onnxSnap = OnnxConcurrencyRegistry::instance().snapshot();
        out.onnxTotalSlots = onnxSnap.totalSlots;
        out.onnxUsedSlots = onnxSnap.usedSlots;
        out.onnxGlinerUsed = onnxSnap.lanes[static_cast<size_t>(OnnxLane::Gliner)].used;
        out.onnxEmbedUsed = onnxSnap.lanes[static_cast<size_t>(OnnxLane::Embedding)].used;
        out.onnxRerankerUsed = onnxSnap.lanes[static_cast<size_t>(OnnxLane::Reranker)].used;
    } catch (...) {
    }

    // DatabaseManager metrics
    try {
        if (services_) {
            if (auto* dbm = services_->getDatabaseManager()) {
                const auto& dbStats = dbm->getStats();
                out.dbOpenDurationMs = dbStats.openDurationMs.load();
                out.dbMigrationDurationMs = dbStats.migrationDurationMs.load();
                out.dbOpenErrors = dbStats.openErrors.load();
                out.dbMigrationErrors = dbStats.migrationErrors.load();
                out.dbRepositoryInitErrors = dbStats.repositoryInitErrors.load();
            }

            if (auto writePool = services_->getWriteConnectionPool()) {
                const auto stats = writePool->getStats();
                out.dbWritePoolAvailable = true;
                out.dbWritePoolTotalConnections = stats.totalConnections;
                out.dbWritePoolAvailableConnections = stats.availableConnections;
                out.dbWritePoolActiveConnections = stats.activeConnections;
                out.dbWritePoolWaitingRequests = stats.waitingRequests;
                out.dbWritePoolMaxObservedWaiting = stats.maxObservedWaiting;
                out.dbWritePoolTotalWaitMicros = stats.totalWaitMicros;
                out.dbWritePoolTimeoutCount = stats.timeoutCount;
                out.dbWritePoolFailedAcquisitions = stats.failedAcquisitions;
            }

            if (auto readPool = services_->getReadConnectionPool()) {
                const auto stats = readPool->getStats();
                out.dbReadPoolAvailable = true;
                out.dbReadPoolTotalConnections = stats.totalConnections;
                out.dbReadPoolAvailableConnections = stats.availableConnections;
                out.dbReadPoolActiveConnections = stats.activeConnections;
                out.dbReadPoolWaitingRequests = stats.waitingRequests;
                out.dbReadPoolMaxObservedWaiting = stats.maxObservedWaiting;
                out.dbReadPoolTotalWaitMicros = stats.totalWaitMicros;
                out.dbReadPoolTimeoutCount = stats.timeoutCount;
                out.dbReadPoolFailedAcquisitions = stats.failedAcquisitions;
            }
        }
    } catch (...) {
    }

    // WorkCoordinator metrics
    try {
        if (services_) {
            if (auto* wc = services_->getWorkCoordinator()) {
                auto wcStats = wc->getStats();
                out.workCoordinatorActiveWorkers = wcStats.activeWorkers;
                out.workCoordinatorRunning = wcStats.isRunning;
            }
        }
    } catch (...) {
    }

    // Stream metrics (from StreamMetricsRegistry)
    try {
        auto ssnap = StreamMetricsRegistry::instance().snapshot();
        out.streamTotal = ssnap.totalStreams;
        out.streamBatches = ssnap.batchesEmitted;
        out.streamKeepalives = ssnap.keepalives;
        if (ssnap.ttfbCount > 0) {
            out.streamTtfbAvgMs = ssnap.ttfbSumMs / ssnap.ttfbCount;
        }
    } catch (...) {
    }

    // InternalEventBus metrics (title extraction, FTS5, symbol)
    try {
        auto& bus = InternalEventBus::instance();
        // Embedding preparation fanout (doc-level observability)
        out.embedPreparedDocsQueued = bus.embedPreparedDocsQueued();
        out.embedPreparedChunksQueued = bus.embedPreparedChunksQueued();
        out.embedHashOnlyDocsQueued = bus.embedHashOnlyDocsQueued();
        out.embedPreparedAvgChunksPerDoc =
            safeRatio(out.embedPreparedChunksQueued, out.embedPreparedDocsQueued);
        // Title extraction
        out.titleQueued = bus.titleQueued();
        out.titleDropped = bus.titleDropped();
        out.titleConsumed = bus.titleConsumed();
        // FTS5 indexing (full picture)
        out.fts5Queued = bus.fts5Queued();
        out.fts5Dropped = bus.fts5Dropped();
        out.fts5Consumed = bus.fts5Consumed();
        // Symbol extraction
        out.symbolQueued = bus.symbolQueued();
        out.symbolDropped = bus.symbolDropped();
        out.symbolConsumed = bus.symbolConsumed();
        // Deferred ingestion queue depth
        try {
            auto ch = bus.get_or_create_channel<InternalEventBus::StoreDocumentTask>(
                "store_document_tasks",
                static_cast<std::size_t>(TuneAdvisor::storeDocumentChannelCapacity()));
            out.deferredQueueDepth = ch->size_approx();
        } catch (...) {
        }
    } catch (...) {
    }

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
    } catch (...) {
    }
    // Shared overload snapshot for retry-after and per-command admission metrics.
    try {
        out.retryAfterMs =
            admission::computeRetryAfterMs(admission::captureSnapshot(services_, state_));
        if (services_) {
            auto searchLoad = services_->getSearchLoadMetrics();
            out.searchActive = searchLoad.active;
            out.searchQueued = searchLoad.queued;
            out.searchExecuted = searchLoad.executed;
            out.searchCacheHitRate = searchLoad.cacheHitRate;
            out.searchAvgLatencyUs = searchLoad.avgLatencyUs;
            out.searchConcurrencyLimit = searchLoad.concurrencyLimit;
        }
        if (state_) {
            out.listActive = state_->stats.listRequestsActive.load(std::memory_order_relaxed);
            out.listRejected = state_->stats.listRequestsRejected.load(std::memory_order_relaxed);
            out.grepActive = state_->stats.grepRequestsActive.load(std::memory_order_relaxed);
            out.grepRejected = state_->stats.grepRequestsRejected.load(std::memory_order_relaxed);
            out.searchRejected =
                state_->stats.searchRequestsRejected.load(std::memory_order_relaxed);
            out.addDeferred = state_->stats.addRequestsDeferred.load(std::memory_order_relaxed);
            out.addRejected = state_->stats.addRequestsRejected.load(std::memory_order_relaxed);
        }
    } catch (...) {
    }

    // OS resource hints (fast probes)
    try {
        bool needMemoryFallback = false;
        {
            // Read from cache (no I/O on hot path) when the background polling loop has populated
            // it.
            std::shared_lock lock(cacheMutex_);
            out.memoryUsageMb = cached_.memoryUsageMb;
            out.cpuUsagePercent = cached_.cpuUsagePercent;
            out.memoryBreakdownBytes = cached_.memoryBreakdownBytes;
            out.diagnosticCounters = cached_.diagnosticCounters;
            needMemoryFallback = (out.memoryUsageMb <= 0.0 && out.memoryBreakdownBytes.empty());
        }
        if (needMemoryFallback) {
            const bool includeAllocatorBreakdown =
                (detailed && allocatorBreakdownEnabled()) || mallocStackLoggingEnabled();
            auto memSample = sampleMemoryUsage(includeAllocatorBreakdown);
            out.memoryUsageMb = memSample.memoryUsageMb;
            out.memoryBreakdownBytes = std::move(memSample.breakdownBytes);
            out.diagnosticCounters = std::move(memSample.diagnosticCounters);
        }
        if (services_) {
            const auto warnBytes = services_->getConfig().instrumentation.mslStackLogWarnBytes;
            if (warnBytes > 0) {
                out.diagnosticCounters["msl_stack_log_warn_bytes"] = warnBytes;
            }
        }
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
            // Do not touch the live VectorDatabase handle on the status path.
            // During startup the vector backend may be rebuilding HNSW under its internal mutex,
            // and even cheap getters can block health checks. Use only cached counters and
            // readiness atomics here so status remains responsive while startup progresses.
            std::size_t liveVectorRows = 0;
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
            // Exact rows via cached value ONLY (updated periodically in background)
            {
                std::shared_lock lock(cacheMutex_);
                out.vectorRowsExact = std::max<std::uint64_t>(cachedVectorRows_, liveVectorRows);
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

    // Ensure readinessStates[vector_db] reflects the final vectorDbReady after healing.
    try {
        out.readinessStates[std::string(readiness::kVectorDb)] = out.vectorDbReady;
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
                    out.dataDir = dd.string();
                    out.contentStoreRoot = (dd / "storage").string();
                }
            } catch (...) {
            }
            try {
                out.metadataDbPath = services_->getMetadataDatabasePath();
            } catch (...) {
            }
            try {
                out.vectorDbPath = services_->getVectorDatabasePath();
            } catch (...) {
            }
            try {
                out.contentStoreError = services_->getContentStoreError();
            } catch (...) {
            }
            {
                std::shared_lock lock(cacheMutex_);
                out.documentsTotal = cachedDocumentsTotal_;
                out.documentsIndexed = cachedDocumentsIndexed_;
                out.documentsContentExtracted = cachedDocumentsExtracted_;
                out.documentsEmbedded = cachedDocumentsEmbedded_;
                out.vectorRowsPerEmbeddedDoc =
                    safeRatio(cachedVectorRows_, cachedDocumentsEmbedded_);
            }
            // FTS5 orphan scan metrics from InternalEventBus
            try {
                auto& bus = InternalEventBus::instance();
                out.fts5OrphansDetected = bus.orphansDetected();
                out.fts5OrphansRemoved = bus.orphansRemoved();

                // FTS5 failure breakdown
                out.fts5FailNoDoc = bus.fts5FailNoDoc();
                out.fts5FailExtraction = bus.fts5FailExtraction();
                out.fts5FailIndex = bus.fts5FailIndex();
                out.fts5FailException = bus.fts5FailException();

                // Convert epoch ms to ISO8601 timestamp
                uint64_t epochMs = bus.lastOrphanScanEpochMs();
                if (epochMs > 0) {
                    auto tp =
                        std::chrono::system_clock::time_point(std::chrono::milliseconds(epochMs));
                    std::time_t tt = std::chrono::system_clock::to_time_t(tp);
                    std::tm tm;
#ifdef _WIN32
                    ::gmtime_s(&tm, &tt);
#else
                    ::gmtime_r(&tt, &tm);
#endif
                    std::ostringstream oss;
                    oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
                    out.lastOrphanScanTime = oss.str();
                }
            } catch (...) {
                // Metrics unavailable, leave defaults (0, 0, empty string)
            }
            // Checkpoint manager stats (PBI-090)
            try {
                if (auto* cm = services_->getCheckpointManager()) {
                    out.checkpointVectorCount = cm->vectorCheckpointCount();
                    out.checkpointHotzoneCount = cm->hotzoneCheckpointCount();
                    out.checkpointErrorCount = cm->checkpointErrorCount();

                    auto epochToIso = [](uint64_t epochMs) -> std::string {
                        if (epochMs == 0)
                            return "";
                        auto tp = std::chrono::system_clock::time_point(
                            std::chrono::milliseconds(epochMs));
                        std::time_t tt = std::chrono::system_clock::to_time_t(tp);
                        std::tm tm;
#ifdef _WIN32
                        ::gmtime_s(&tm, &tt);
#else
                        ::gmtime_r(&tt, &tm);
#endif
                        std::ostringstream oss;
                        oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
                        return oss.str();
                    };

                    out.lastVectorCheckpointTime = epochToIso(cm->lastVectorCheckpointEpoch());
                    out.lastHotzoneCheckpointTime = epochToIso(cm->lastHotzoneCheckpointEpoch());
                }
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

                    // Use persistent uncompressed bytes from reference counter stats
                    // (now stored in block_references table and persisted across restarts)
                    uint64_t compressSaved = ss.compressionSaved();

                    // Calculate logical bytes (uncompressed size):
                    // ss.totalUncompressedBytes comes from persistent ref_statistics
                    if (ss.totalUncompressedBytes > 0) {
                        out.logicalBytes = ss.totalUncompressedBytes;
                    } else {
                        // Fallback: no uncompressed data tracked yet
                        out.logicalBytes = ss.totalBytes;
                    }

                    // casUniqueRawBytes represents the unique bytes in CAS (on-disk)
                    // which is ss.totalBytes minus dedup savings. When compression is
                    // active, this is the compressed size.
                    out.casUniqueRawBytes = ss.totalBytes;
                    out.casDedupSavedBytes = ss.deduplicatedBytes;
                    out.casCompressSavedBytes = compressSaved;

                    if (detailed && !disableStoreStats) {
                        out.uniqueBlocks = ss.uniqueBlocks;
                        out.deduplicatedBytes = ss.deduplicatedBytes;
                        out.compressionRatio = ss.dedupRatio();
                    }
                } catch (...) {
                }
            }
            if (detailed) {
                try {
                    std::shared_lock lk(cacheMutex_);
                    out.physicalBytes = lastPhysicalBytes_;
                    out.casPhysicalBytes = cached_.casPhysicalBytes;
                    out.metadataPhysicalBytes = cached_.metadataPhysicalBytes;
                    out.indexPhysicalBytes = cached_.indexPhysicalBytes;
                    out.vectorPhysicalBytes = cached_.vectorPhysicalBytes;
                    out.logsTmpPhysicalBytes = cached_.logsTmpPhysicalBytes;
                    out.physicalTotalBytes = cached_.physicalTotalBytes;
                } catch (...) {
                }
            }
            // Search engine and reason when unavailable
            if (services_->getSearchEngineSnapshot()) {
                out.serviceSearchEngine = "available";
            } else {
                out.serviceSearchEngine = "unavailable";
                std::string reason;
                try {
                    if (!state_->readiness.databaseReady.load())
                        reason = "database_not_ready";
                    else if (!state_->readiness.metadataRepoReady.load())
                        reason = "metadata_repo_not_ready";
                    else if (!state_->readiness.searchEngineReady.load())
                        reason = "search_engine_not_built";
                    else
                        reason = "not_initialized";
                } catch (...) {
                }
                out.searchEngineReason = reason;
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
            auto provider = services_->getModelProvider();
            if (provider) {
                try {
                    out.embeddingAvailable = provider->isAvailable();
                } catch (...) {
                    out.embeddingAvailable = false;
                }
                try {
                    std::string modelName = services_->getEmbeddingModelName();
                    if (!modelName.empty()) {
                        out.embeddingDim =
                            static_cast<uint32_t>(provider->getEmbeddingDim(modelName));
                    }
                } catch (...) {
                }
            }
            // Backend label and model details
            out.embeddingModel = services_->getEmbeddingModelName();
            try {
                auto prov = services_->getModelProvider();
                if (prov && prov->isAvailable()) {
                    out.embeddingBackend = std::string("plugin:") + prov->getProviderName();
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
                    // Best-effort local model path resolution
                    if (out.embeddingModelPath.empty() && !out.embeddingModel.empty()) {
                        try {
                            if (services_) {
                                namespace fs = std::filesystem;
                                fs::path p = services_->getResolvedDataDir() / "models" /
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

            // Vector memory is reported by VectorDatabase (sqlite-vec).
        }
    } catch (...) {
    }

    // Vector diagnostics (uses non-blocking cached snapshots)
    try {
        auto d =
            yams::daemon::dispatch::collect_vector_diag(const_cast<ServiceManager*>(services_));
        out.vectorEmbeddingsAvailable = d.embeddingsAvailable;
        out.vectorScoringEnabled = d.scoringEnabled;
        out.searchEngineBuildReason = d.buildReason;
    } catch (...) {
    }
}

void DaemonMetrics::enrichDetailedSnapshot(MetricsSnapshot& out) const {
    try {
        if (services_) {
            auto metaRepo = services_->getMetadataRepo();
            if (metaRepo) {
                auto statsResult = metaRepo->getCorpusStats();
                if (statsResult) {
                    const auto& stats = statsResult.value();
                    auto now = std::chrono::steady_clock::now();

                    bool needsRefresh = false;
                    {
                        std::shared_lock tunerLock(tunerCacheMutex_);
                        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                           now - lastTunerStateAt_)
                                           .count();
                        needsRefresh =
                            (elapsed > tunerStateTtlMs_) ||
                            (static_cast<uint64_t>(stats.docCount) != cachedTunerDocCount_);
                    }

                    if (needsRefresh) {
                        yams::search::SearchTuner tuner(stats);
                        auto newState = yams::search::tuningStateToString(tuner.currentState());
                        auto newReason = tuner.stateReason();
                        std::map<std::string, double> newParams;

                        if (auto engine = services_->getSearchEngineSnapshot()) {
                            const auto& cfg = engine->getConfig();
                            newParams["rrf_k"] = static_cast<double>(cfg.rrfK);
                            newParams["text_weight"] = static_cast<double>(cfg.textWeight);
                            newParams["vector_weight"] = static_cast<double>(cfg.vectorWeight);
                            newParams["entity_vector_weight"] =
                                static_cast<double>(cfg.entityVectorWeight);
                            newParams["path_tree_weight"] = static_cast<double>(cfg.pathTreeWeight);
                            newParams["kg_weight"] = static_cast<double>(cfg.kgWeight);
                            newParams["tag_weight"] = static_cast<double>(cfg.tagWeight);
                            newParams["metadata_weight"] = static_cast<double>(cfg.metadataWeight);
                            newParams["similarity_threshold"] =
                                static_cast<double>(cfg.similarityThreshold);
                            newParams["vector_boost_factor"] =
                                static_cast<double>(cfg.vectorBoostFactor);
                            newParams["enable_graph_rerank"] = cfg.enableGraphRerank ? 1.0 : 0.0;
                            newParams["graph_rerank_topn"] =
                                static_cast<double>(cfg.graphRerankTopN);
                            newParams["graph_rerank_weight"] =
                                static_cast<double>(cfg.graphRerankWeight);
                            newParams["graph_rerank_max_boost"] =
                                static_cast<double>(cfg.graphRerankMaxBoost);
                            newParams["graph_rerank_min_signal"] =
                                static_cast<double>(cfg.graphRerankMinSignal);
                            newParams["graph_community_weight"] =
                                static_cast<double>(cfg.graphCommunityWeight);
                            newParams["kg_max_results"] = static_cast<double>(cfg.kgMaxResults);
                            newParams["graph_scoring_budget_ms"] =
                                static_cast<double>(cfg.graphScoringBudgetMs);
                        } else {
                            const auto& p = tuner.getParams();
                            newParams["rrf_k"] = static_cast<double>(p.rrfK);
                            newParams["text_weight"] = static_cast<double>(p.weights.text.value);
                            newParams["vector_weight"] =
                                static_cast<double>(p.weights.vector.value);
                            newParams["entity_vector_weight"] =
                                static_cast<double>(p.weights.entityVector.value);
                            newParams["path_tree_weight"] =
                                static_cast<double>(p.weights.pathTree.value);
                            newParams["kg_weight"] = static_cast<double>(p.weights.kg.value);
                            newParams["tag_weight"] = static_cast<double>(p.weights.tag.value);
                            newParams["metadata_weight"] =
                                static_cast<double>(p.weights.metadata.value);
                            newParams["similarity_threshold"] =
                                static_cast<double>(p.similarityThreshold);
                            newParams["vector_boost_factor"] =
                                static_cast<double>(p.vectorBoostFactor);
                            newParams["enable_graph_rerank"] = p.enableGraphRerank ? 1.0 : 0.0;
                            newParams["graph_rerank_topn"] = static_cast<double>(p.graphRerankTopN);
                            newParams["graph_rerank_weight"] =
                                static_cast<double>(p.graphRerankWeight);
                            newParams["graph_rerank_max_boost"] =
                                static_cast<double>(p.graphRerankMaxBoost);
                            newParams["graph_rerank_min_signal"] =
                                static_cast<double>(p.graphRerankMinSignal);
                            newParams["graph_community_weight"] =
                                static_cast<double>(p.graphCommunityWeight);
                            newParams["kg_max_results"] = static_cast<double>(p.kgMaxResults);
                            newParams["graph_scoring_budget_ms"] =
                                static_cast<double>(p.graphScoringBudgetMs);
                        }

                        std::unique_lock tunerLock(tunerCacheMutex_);
                        cachedTuningState_ = std::move(newState);
                        cachedTuningReason_ = std::move(newReason);
                        cachedTuningParams_ = std::move(newParams);
                        cachedTunerDocCount_ = stats.docCount;
                        lastTunerStateAt_ = now;
                    }

                    {
                        std::shared_lock tunerLock(tunerCacheMutex_);
                        out.searchTuningState = cachedTuningState_;
                        out.searchTuningReason = cachedTuningReason_;
                        out.searchTuningParams = cachedTuningParams_;
                    }

                    if (auto engine = services_->getSearchEngineSnapshot()) {
                        const auto& cfg = engine->getConfig();
                        out.searchTuningParams["rrf_k"] = static_cast<double>(cfg.rrfK);
                        out.searchTuningParams["text_weight"] = static_cast<double>(cfg.textWeight);
                        out.searchTuningParams["vector_weight"] =
                            static_cast<double>(cfg.vectorWeight);
                        out.searchTuningParams["entity_vector_weight"] =
                            static_cast<double>(cfg.entityVectorWeight);
                        out.searchTuningParams["path_tree_weight"] =
                            static_cast<double>(cfg.pathTreeWeight);
                        out.searchTuningParams["kg_weight"] = static_cast<double>(cfg.kgWeight);
                        out.searchTuningParams["tag_weight"] = static_cast<double>(cfg.tagWeight);
                        out.searchTuningParams["metadata_weight"] =
                            static_cast<double>(cfg.metadataWeight);
                        out.searchTuningParams["similarity_threshold"] =
                            static_cast<double>(cfg.similarityThreshold);
                        out.searchTuningParams["vector_boost_factor"] =
                            static_cast<double>(cfg.vectorBoostFactor);
                        out.searchTuningParams["enable_graph_rerank"] =
                            cfg.enableGraphRerank ? 1.0 : 0.0;
                        out.searchTuningParams["graph_rerank_topn"] =
                            static_cast<double>(cfg.graphRerankTopN);
                        out.searchTuningParams["graph_rerank_weight"] =
                            static_cast<double>(cfg.graphRerankWeight);
                        out.searchTuningParams["graph_rerank_max_boost"] =
                            static_cast<double>(cfg.graphRerankMaxBoost);
                        out.searchTuningParams["graph_rerank_min_signal"] =
                            static_cast<double>(cfg.graphRerankMinSignal);
                        out.searchTuningParams["graph_community_weight"] =
                            static_cast<double>(cfg.graphCommunityWeight);
                        out.searchTuningParams["kg_max_results"] =
                            static_cast<double>(cfg.kgMaxResults);
                        out.searchTuningParams["graph_scoring_budget_ms"] =
                            static_cast<double>(cfg.graphScoringBudgetMs);
                    }
                }
            }
        }
    } catch (...) {
    }
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
