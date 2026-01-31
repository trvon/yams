// ResourceGovernor implementation
// Monitors cumulative resource usage and coordinates graduated pressure responses.

#include <yams/daemon/components/ResourceGovernor.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/PoolManager.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/ipc/mux_metrics_registry.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/profiling.h>

// Platform-specific RSS and CPU reading
#if defined(_WIN32)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN 1
#endif
#ifndef NOMINMAX
#define NOMINMAX 1
#endif
// clang-format off
#include <Windows.h>
#include <Psapi.h>
// clang-format on
#elif defined(__APPLE__)
#include <unistd.h>
#include <mach/mach.h>
#else
#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>
#include <unistd.h>
#endif

namespace yams::daemon {

namespace {

/// Read current process RSS in bytes (cross-platform)
std::uint64_t readRssBytes() {
#if defined(_WIN32)
    PROCESS_MEMORY_COUNTERS pmc{};
    if (GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc))) {
        return pmc.WorkingSetSize;
    }
    return 0;

#elif defined(__APPLE__)
    mach_task_basic_info info{};
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info),
                  &count) == KERN_SUCCESS) {
        return info.resident_size;
    }
    return 0;

#else
    // Linux: read /proc/self/status for VmRSS
    std::FILE* file = std::fopen("/proc/self/status", "r");
    if (!file) {
        return 0;
    }

    std::uint64_t rssKb = 0;
    char line[256];
    while (std::fgets(line, sizeof(line), file)) {
        if (std::strncmp(line, "VmRSS:", 6) == 0) {
            unsigned long kb = 0;
            if (std::sscanf(line + 6, " %lu", &kb) == 1) {
                rssKb = kb;
            }
            break;
        }
    }
    std::fclose(file);
    return rssKb * 1024;
#endif
}

/// Read CPU usage percent for the current process using delta-based calculation.
/// Percent is relative to total system capacity (all CPUs). A single fully utilized
/// core on a 4-core system will report ~25%.
/// @param lastProcJiffies Previous process CPU ticks (updated on each call)
/// @param lastTotalJiffies Previous total system CPU ticks (updated on each call)
/// @return CPU usage percentage (0-100)
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

    SYSTEM_INFO sysInfo{};
    GetSystemInfo(&sysInfo);
    double pct = (static_cast<double>(dProc) / static_cast<double>(dTotal)) * 100.0;
    return std::clamp(pct, 0.0, 100.0);

#elif defined(__APPLE__)
    // Process CPU time via Mach task info
    task_thread_times_info_data_t thread_info{};
    mach_msg_type_number_t count = TASK_THREAD_TIMES_INFO_COUNT;
    std::uint64_t procJiffies = 0;
    if (task_info(mach_task_self(), TASK_THREAD_TIMES_INFO,
                  reinterpret_cast<task_info_t>(&thread_info), &count) == KERN_SUCCESS) {
        procJiffies =
            (thread_info.user_time.seconds + thread_info.system_time.seconds) * 100 +
            (thread_info.user_time.microseconds + thread_info.system_time.microseconds) / 10000;
    }

    // Total CPU time via host statistics
    host_cpu_load_info_data_t cpu_info{};
    count = HOST_CPU_LOAD_INFO_COUNT;
    std::uint64_t totalJiffies = 0;
    if (host_statistics(mach_host_self(), HOST_CPU_LOAD_INFO,
                        reinterpret_cast<host_info_t>(&cpu_info), &count) == KERN_SUCCESS) {
        totalJiffies = cpu_info.cpu_ticks[CPU_STATE_USER] + cpu_info.cpu_ticks[CPU_STATE_SYSTEM] +
                       cpu_info.cpu_ticks[CPU_STATE_NICE] + cpu_info.cpu_ticks[CPU_STATE_IDLE];
    }

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

#else
    // Linux: read from /proc/stat and /proc/self/stat
    std::uint64_t procJiffies = 0;
    std::uint64_t totalJiffies = 0;

    // Process jiffies from /proc/self/stat (fields 14=utime, 15=stime)
    std::ifstream procStat("/proc/self/stat");
    if (procStat.is_open()) {
        std::string line;
        if (std::getline(procStat, line)) {
            // Skip past comm field (which may contain spaces/parens)
            auto pos = line.rfind(')');
            if (pos != std::string::npos && pos + 2 < line.size()) {
                std::istringstream iss(line.substr(pos + 2));
                std::string field;
                // Fields after ')': state, ppid, pgrp, session, tty_nr, tpgid, flags,
                //                   minflt, cminflt, majflt, cmajflt, utime(14), stime(15)
                for (int i = 0; i < 11 && iss >> field; ++i) {
                } // Skip fields 3-13
                std::uint64_t utime = 0, stime = 0;
                if (iss >> utime >> stime) {
                    procJiffies = utime + stime;
                }
            }
        }
    }

    // Total system jiffies from /proc/stat (first "cpu" line)
    std::ifstream cpuStat("/proc/stat");
    if (cpuStat.is_open()) {
        std::string line;
        while (std::getline(cpuStat, line)) {
            if (line.rfind("cpu ", 0) == 0) {
                std::istringstream iss(line.substr(4));
                std::uint64_t user = 0, nice = 0, system = 0, idle = 0;
                std::uint64_t iowait = 0, irq = 0, softirq = 0, steal = 0;
                iss >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal;
                totalJiffies = user + nice + system + idle + iowait + irq + softirq + steal;
                break;
            }
        }
    }

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
#endif
}

} // anonymous namespace

// ============================================================================
// Singleton
// ============================================================================

ResourceGovernor& ResourceGovernor::instance() noexcept {
    static ResourceGovernor inst;
    return inst;
}

ResourceGovernor::ResourceGovernor() {
    // Initialize scaling caps to TuneAdvisor defaults
    updateScalingCaps(ResourcePressureLevel::Normal);
}

// ============================================================================
// Tick Interface
// ============================================================================

ResourceSnapshot ResourceGovernor::tick(ServiceManager* sm) {
    YAMS_ZONE_SCOPED_N("ResourceGovernor::tick");

    if (!TuneAdvisor::enableResourceGovernor()) {
        // Governor disabled - return empty snapshot with Normal level
        ResourceSnapshot snap{};
        snap.level = ResourcePressureLevel::Normal;
        snap.timestamp = std::chrono::steady_clock::now();
        return snap;
    }

    ResourceSnapshot snap{};
    snap.timestamp = std::chrono::steady_clock::now();

    // Collect metrics from all sources
    collectMetrics(sm, snap);

    // Update admission control decisions based on CPU with hysteresis
    updateCpuAdmissionControl(snap);

    // Compute pressure level with hysteresis
    ResourcePressureLevel newLevel = computeLevel(snap);
    ResourcePressureLevel oldLevel = currentLevel_.load(std::memory_order_relaxed);

    // Handle level transitions
    if (newLevel != oldLevel) {
        spdlog::info("[ResourceGovernor] Pressure level: {} -> {} (RSS={} MiB, budget={} MiB, "
                     "mem={:.1f}%, CPU={:.1f}%)",
                     pressureLevelName(oldLevel), pressureLevelName(newLevel),
                     snap.rssBytes / (1024ull * 1024ull),
                     snap.memoryBudgetBytes / (1024ull * 1024ull), snap.memoryPressure * 100.0,
                     snap.cpuUsagePercent);

        currentLevel_.store(newLevel, std::memory_order_relaxed);
        updateScalingCaps(newLevel);

        // Trigger level-specific responses
        switch (newLevel) {
            case ResourcePressureLevel::Normal:
                onNormalLevel(sm);
                break;
            case ResourcePressureLevel::Warning:
                onWarningLevel(sm);
                break;
            case ResourcePressureLevel::Critical:
                onCriticalLevel(sm);
                break;
            case ResourcePressureLevel::Emergency:
                onEmergencyLevel(sm);
                break;
        }
    }

    snap.level = newLevel;

    // Update stored snapshot
    {
        std::unique_lock lock(mutex_);
        lastSnapshot_ = snap;
    }

    return snap;
}

void ResourceGovernor::updateCpuAdmissionControl(const ResourceSnapshot& snap) {
    // Fast path: admission control disabled
    if (!TuneAdvisor::enableResourceGovernor() || !TuneAdvisor::enableAdmissionControl()) {
        cpuAdmissionBlocked_.store(false, std::memory_order_relaxed);
        cpuHighSince_ = {};
        cpuLowSince_ = {};
        return;
    }

    // cpuUsagePercent is 0..100 semantics (percent of total host capacity).
    double cpuPct = snap.cpuUsagePercent;

    const double cpuHighThresh = TuneAdvisor::cpuHighThresholdPercent();
    const auto highHold = std::chrono::milliseconds(TuneAdvisor::cpuAdmissionHighHoldMs());
    const auto lowHold = std::chrono::milliseconds(TuneAdvisor::cpuAdmissionLowHoldMs());
    auto now = snap.timestamp;

    // Above threshold: track high duration; clear low timer.
    if (cpuPct >= cpuHighThresh) {
        if (cpuHighSince_.time_since_epoch().count() == 0) {
            cpuHighSince_ = now;
        }
        cpuLowSince_ = {};

        if (!cpuAdmissionBlocked_.load(std::memory_order_relaxed) &&
            (now - cpuHighSince_) >= highHold) {
            cpuAdmissionBlocked_.store(true, std::memory_order_relaxed);
        }
        return;
    }

    // Below threshold: track low duration; clear high timer.
    cpuHighSince_ = {};
    if (cpuLowSince_.time_since_epoch().count() == 0) {
        cpuLowSince_ = now;
    }

    if (cpuAdmissionBlocked_.load(std::memory_order_relaxed) && (now - cpuLowSince_) >= lowHold) {
        cpuAdmissionBlocked_.store(false, std::memory_order_relaxed);
    }
}

// ============================================================================
// Metric Collection
// ============================================================================

void ResourceGovernor::collectMetrics(ServiceManager* sm, ResourceSnapshot& snap) {
    YAMS_ZONE_SCOPED_N("ResourceGovernor::collectMetrics");

    // Memory metrics
    snap.rssBytes = readRssBytes();
    snap.memoryBudgetBytes = TuneAdvisor::memoryBudgetBytes();

    // CPU metrics (delta-based, uses mutable state for jiffies tracking)
    snap.cpuUsagePercent = readCpuUsagePercent(lastProcJiffies_, lastTotalJiffies_);

    if (snap.memoryBudgetBytes > 0) {
        snap.memoryPressure =
            static_cast<double>(snap.rssBytes) / static_cast<double>(snap.memoryBudgetBytes);
    }

    // Compute scaling headroom (inverse of pressure, clamped 0-1)
    snap.scalingHeadroom = std::clamp(1.0 - snap.memoryPressure, 0.0, 1.0);

    if (!sm) {
        return;
    }

    // Model dimension
    if (auto provider = sm->getModelProvider()) {
        snap.loadedModels = static_cast<std::uint32_t>(provider->getLoadedModelCount());
        snap.modelMemoryBytes = provider->getMemoryUsage();
    }

    // Worker pool dimensions
    auto ingestMetrics = sm->getIngestMetricsSnapshot();
    snap.ingestWorkers = static_cast<std::uint32_t>(ingestMetrics.active);

    auto searchMetrics = sm->getSearchLoadMetrics();
    snap.searchConcurrency = searchMetrics.active;

    // PostIngestQueue dimensions
    if (auto* piq = sm->getPostIngestQueue()) {
        snap.postIngestQueued = piq->size();
        snap.extractionConcurrency = static_cast<std::uint32_t>(piq->extractionInFlight());
        snap.kgConcurrency = static_cast<std::uint32_t>(piq->kgInFlight());
    }

    // InternalEventBus queue depths
    auto& bus = InternalEventBus::instance();
    snap.embedQueued = bus.embedQueued();
    snap.kgQueued = bus.kgQueued();

    // Mux metrics
    auto muxSnap = MuxMetricsRegistry::instance().snapshot();
    snap.muxQueuedBytes =
        (muxSnap.queuedBytes > 0) ? static_cast<std::uint64_t>(muxSnap.queuedBytes) : 0;
    snap.activeIpcHandlers = static_cast<std::uint32_t>(muxSnap.activeHandlers);

    // Connection pool stats
    auto& pm = PoolManager::instance();
    auto ipcStats = pm.stats("ipc");
    snap.dbConnections = ipcStats.current_size;
}

// ============================================================================
// Pressure Level Computation
// ============================================================================

ResourcePressureLevel ResourceGovernor::computeLevel(const ResourceSnapshot& snap) {
    // Determine raw level based on thresholds
    ResourcePressureLevel rawLevel = ResourcePressureLevel::Normal;

    const double warningThresh = TuneAdvisor::memoryWarningThreshold();
    const double criticalThresh = TuneAdvisor::memoryCriticalThreshold();
    const double emergencyThresh = TuneAdvisor::memoryEmergencyThreshold();

    // Memory-based pressure level
    if (snap.memoryPressure >= emergencyThresh) {
        rawLevel = ResourcePressureLevel::Emergency;
    } else if (snap.memoryPressure >= criticalThresh) {
        rawLevel = ResourcePressureLevel::Critical;
    } else if (snap.memoryPressure >= warningThresh) {
        rawLevel = ResourcePressureLevel::Warning;
    }

    // CPU-based pressure escalation: if CPU is very high, escalate the pressure level
    // This prevents CPU saturation even when memory is fine
    const double cpuHighThresh = TuneAdvisor::cpuHighThresholdPercent();
    const double cpuCriticalThresh = cpuHighThresh + 25.0; // 25% above high

    if (snap.cpuUsagePercent >= cpuCriticalThresh) {
        // Escalate to at least Critical if CPU is very high
        rawLevel = std::max(rawLevel, ResourcePressureLevel::Critical);
    } else if (snap.cpuUsagePercent >= cpuHighThresh) {
        // Escalate to at least Warning if CPU is high
        rawLevel = std::max(rawLevel, ResourcePressureLevel::Warning);
    }

    // Apply hysteresis: require time at a level before transitioning
    // (decoupled from tick interval for consistent behavior at any tick rate)
    const auto hysteresisMs = std::chrono::milliseconds(TuneAdvisor::memoryHysteresisMs());
    auto now = std::chrono::steady_clock::now();

    if (rawLevel != proposedLevel_) {
        // Level changed - reset timer
        proposedLevel_ = rawLevel;
        proposedLevelSince_ = now;
    }

    auto elapsed = now - proposedLevelSince_;
    ResourcePressureLevel currentLvl = currentLevel_.load(std::memory_order_relaxed);

    if (rawLevel > currentLvl) {
        // Escalating - require hysteresisMs
        if (elapsed >= hysteresisMs) {
            return rawLevel;
        }
        return currentLvl; // Hold at current level
    } else if (rawLevel < currentLvl) {
        // De-escalating - require 2× hysteresisMs for stability
        if (elapsed >= hysteresisMs * 2) {
            return rawLevel;
        }
        return currentLvl; // Hold at current level
    }

    return currentLvl;
}

// ============================================================================
// Scaling Cap Updates
// ============================================================================

void ResourceGovernor::updateScalingCaps(ResourcePressureLevel level) {
    std::unique_lock lock(mutex_);

    // Get TuneAdvisor defaults
    const auto defaultIngest = TuneAdvisor::maxIngestWorkers();
    const auto defaultSearch = TuneAdvisor::searchConcurrencyLimit();
    const auto defaultExtract = TuneAdvisor::postExtractionConcurrent();
    const auto defaultKg = TuneAdvisor::postKgConcurrent();
    const auto defaultEmbed = TuneAdvisor::postEmbedConcurrent();

    switch (level) {
        case ResourcePressureLevel::Normal:
            // Full scaling allowed
            scalingCaps_ = ScalingCaps{
                .ingestWorkers = defaultIngest,
                .searchConcurrency = defaultSearch,
                .extractionConcurrency = defaultExtract,
                .kgConcurrency = defaultKg,
                .embedConcurrency = defaultEmbed,
                .allowModelLoads = true,
                .allowNewIngest = true,
            };
            break;

        case ResourcePressureLevel::Warning:
            // Cap at 50% of normal, block model loads
            scalingCaps_ = ScalingCaps{
                .ingestWorkers = std::max(2u, defaultIngest / 2),
                .searchConcurrency = std::max(2u, defaultSearch / 2),
                .extractionConcurrency = std::max(2u, defaultExtract / 2),
                .kgConcurrency = std::max(2u, defaultKg / 2),
                .embedConcurrency = std::max(1u, defaultEmbed / 2),
                .allowModelLoads = false,
                .allowNewIngest = true,
            };
            break;

        case ResourcePressureLevel::Critical:
            // Minimum concurrency, aggressive reduction
            scalingCaps_ = ScalingCaps{
                .ingestWorkers = 2,
                .searchConcurrency = 2,
                .extractionConcurrency = 2,
                .kgConcurrency = 2,
                .embedConcurrency = 1,
                .allowModelLoads = false,
                .allowNewIngest = true,
            };
            break;

        case ResourcePressureLevel::Emergency:
            // Halt everything possible
            scalingCaps_ = ScalingCaps{
                .ingestWorkers = 1,
                .searchConcurrency = 1,
                .extractionConcurrency = 0,
                .kgConcurrency = 0,
                .embedConcurrency = 0,
                .allowModelLoads = false,
                .allowNewIngest = false,
            };
            break;
    }
}

// ============================================================================
// Admission Control
// ============================================================================

bool ResourceGovernor::canScaleUp(std::string_view dimension, std::uint32_t delta) const {
    if (!TuneAdvisor::enableResourceGovernor()) {
        return true;
    }

    auto level = currentLevel_.load(std::memory_order_relaxed);

    // At Emergency, no scaling allowed
    if (level == ResourcePressureLevel::Emergency) {
        spdlog::debug("[ResourceGovernor] Scale-up denied for {}: emergency pressure", dimension);
        return false;
    }

    // At Critical, only allow if delta is small and we have some headroom
    if (level == ResourcePressureLevel::Critical) {
        std::shared_lock lock(mutex_);
        if (lastSnapshot_.scalingHeadroom < 0.05 || delta > 1) {
            spdlog::debug(
                "[ResourceGovernor] Scale-up denied for {}: critical pressure, headroom={:.1f}%",
                dimension, lastSnapshot_.scalingHeadroom * 100);
            return false;
        }
    }

    return true;
}

bool ResourceGovernor::canLoadModel(std::uint64_t modelSizeBytes) const {
    if (!TuneAdvisor::enableResourceGovernor()) {
        return true;
    }

    std::shared_lock lock(mutex_);

    if (!scalingCaps_.allowModelLoads) {
        spdlog::debug("[ResourceGovernor] Model load denied: pressure level forbids loads");
        return false;
    }

    // Check if model would push us over budget
    const auto currentRss = lastSnapshot_.rssBytes;
    const auto budget = lastSnapshot_.memoryBudgetBytes;
    const auto warningThresh = TuneAdvisor::memoryWarningThreshold();

    if (budget > 0) {
        const double projectedPressure =
            static_cast<double>(currentRss + modelSizeBytes) / static_cast<double>(budget);

        if (projectedPressure >= warningThresh) {
            spdlog::debug("[ResourceGovernor] Model load denied: would exceed warning threshold "
                          "(current={} MiB + model={} MiB = {:.1f}% of budget)",
                          currentRss / (1024ull * 1024ull), modelSizeBytes / (1024ull * 1024ull),
                          projectedPressure * 100);
            return false;
        }
    }

    return true;
}

bool ResourceGovernor::canAdmitWork() const {
    if (!TuneAdvisor::enableResourceGovernor() || !TuneAdvisor::enableAdmissionControl()) {
        return true;
    }

    std::shared_lock lock(mutex_);

    // Memory-based check (existing)
    if (!scalingCaps_.allowNewIngest) {
        return false;
    }

    // CPU-based throttling (with hysteresis): reject new work only after sustained high CPU.
    if (cpuAdmissionBlocked_.load(std::memory_order_relaxed)) {
        const double cpuHighThresh = TuneAdvisor::cpuHighThresholdPercent();
        spdlog::debug(
            "[ResourceGovernor] Throttling admission: sustained CPU at {:.1f}% (threshold {:.0f}%)",
            lastSnapshot_.cpuUsagePercent, cpuHighThresh);
        return false;
    }

    return true;
}

// ============================================================================
// Pressure Response Actions
// ============================================================================

void ResourceGovernor::onNormalLevel(ServiceManager* sm) {
    spdlog::info("[ResourceGovernor] Normal: restored full scaling capacity");

    // Restore TuneAdvisor queue limits
    TuneAdvisor::setPostIngestQueueMax(1000);

    // Resume all stages (in case coming down from Critical/Emergency)
    if (sm) {
        if (auto* piq = sm->getPostIngestQueue()) {
            piq->resumeAll();
        }
    }
}

void ResourceGovernor::onWarningLevel(ServiceManager* sm) {
    spdlog::info("[ResourceGovernor] Warning: capping scaling, blocking model loads");

    // Reduce post-ingest queue capacity to apply backpressure
    TuneAdvisor::setPostIngestQueueMax(500);

    // Resume stages (in case coming down from Critical/Emergency)
    if (sm) {
        if (auto* piq = sm->getPostIngestQueue()) {
            piq->resumeAll();
        }
    }
}

void ResourceGovernor::onCriticalLevel(ServiceManager* sm) {
    spdlog::warn("[ResourceGovernor] Critical: minimum concurrency, triggering model eviction");

    // Heavily reduce queue capacity
    TuneAdvisor::setPostIngestQueueMax(100);

    // Pause non-essential stages to reduce memory pressure
    if (sm) {
        if (auto* piq = sm->getPostIngestQueue()) {
            piq->pauseStage(PostIngestQueue::Stage::KnowledgeGraph);
            piq->pauseStage(PostIngestQueue::Stage::Symbol);
            piq->pauseStage(PostIngestQueue::Stage::Entity);
            piq->pauseStage(PostIngestQueue::Stage::Title);
            spdlog::info("[ResourceGovernor] Paused non-essential post-ingest stages");
        }
    }

    // Trigger model eviction if enabled
    if (TuneAdvisor::enableProactiveEviction() && sm && canEvict()) {
        if (auto provider = sm->getModelProvider()) {
            std::shared_lock lock(mutex_);
            const double pressure = lastSnapshot_.memoryPressure;
            lock.unlock();

            // evictUnderPressure is defined in IModelProvider but may not be
            // implemented by all providers. We'll add it to the interface.
            // For now, use releaseUnusedResources as a fallback.
            provider->releaseUnusedResources();
            recordEviction();

            spdlog::info("[ResourceGovernor] Released unused model resources (pressure={:.1f}%)",
                         pressure * 100);
        }
    }

    // Shrink connection pools
    // Note: PoolManager::shrinkAll() to be added in Phase 7
}

void ResourceGovernor::onEmergencyLevel(ServiceManager* sm) {
    spdlog::critical("[ResourceGovernor] EMERGENCY: halting new work, aggressive eviction");

    // Stop accepting new ingest items
    TuneAdvisor::setPostIngestQueueMax(0);

    // Pause ALL stages to stop all processing
    if (sm) {
        if (auto* piq = sm->getPostIngestQueue()) {
            piq->pauseAll();
            spdlog::warn("[ResourceGovernor] Paused ALL post-ingest stages (emergency)");
        }
    }

    // Aggressive eviction
    if (TuneAdvisor::enableProactiveEviction() && sm && canEvict()) {
        if (auto provider = sm->getModelProvider()) {
            // Release all non-essential resources
            provider->releaseUnusedResources();
            recordEviction();

            spdlog::warn(
                "[ResourceGovernor] Emergency eviction: released all unused model resources");
        }
    }
}

// ============================================================================
// Scaling Cap Getters
// ============================================================================

std::uint32_t ResourceGovernor::maxIngestWorkers() const {
    if (!TuneAdvisor::enableResourceGovernor()) {
        return TuneAdvisor::maxIngestWorkers();
    }
    std::shared_lock lock(mutex_);
    return scalingCaps_.ingestWorkers;
}

std::uint32_t ResourceGovernor::maxSearchConcurrency() const {
    if (!TuneAdvisor::enableResourceGovernor()) {
        return TuneAdvisor::searchConcurrencyLimit();
    }
    std::shared_lock lock(mutex_);
    return scalingCaps_.searchConcurrency;
}

std::uint32_t ResourceGovernor::maxEmbedConcurrency() const {
    if (!TuneAdvisor::enableResourceGovernor()) {
        return TuneAdvisor::postEmbedConcurrent();
    }
    std::shared_lock lock(mutex_);
    return scalingCaps_.embedConcurrency;
}

std::uint32_t ResourceGovernor::maxExtractionConcurrency() const {
    if (!TuneAdvisor::enableResourceGovernor()) {
        return TuneAdvisor::postExtractionConcurrent();
    }
    std::shared_lock lock(mutex_);
    return scalingCaps_.extractionConcurrency;
}

std::uint32_t ResourceGovernor::maxKgConcurrency() const {
    if (!TuneAdvisor::enableResourceGovernor()) {
        return TuneAdvisor::postKgConcurrent();
    }
    std::shared_lock lock(mutex_);
    return scalingCaps_.kgConcurrency;
}

// ============================================================================
// Observability
// ============================================================================

ResourceSnapshot ResourceGovernor::getSnapshot() const {
    std::shared_lock lock(mutex_);
    return lastSnapshot_;
}

ResourcePressureLevel ResourceGovernor::getPressureLevel() const noexcept {
    auto level = currentLevel_.load(std::memory_order_relaxed);
    // During startup grace period, cap at Warning to prevent false Emergency
    // from stalling pollers before metrics are populated.
    if (startupGraceActive() && level > ResourcePressureLevel::Warning) {
        return ResourcePressureLevel::Warning;
    }
    return level;
}

ScalingCaps ResourceGovernor::getScalingCaps() const {
    std::shared_lock lock(mutex_);
    return scalingCaps_;
}

// ============================================================================
// Eviction Cooldown
// ============================================================================

bool ResourceGovernor::canEvict() const {
    auto now = std::chrono::steady_clock::now();
    auto cooldownMs = TuneAdvisor::modelEvictionCooldownMs();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastEvictionTime_);
    return elapsed.count() >= cooldownMs;
}

void ResourceGovernor::recordEviction() {
    lastEvictionTime_ = std::chrono::steady_clock::now();
}

} // namespace yams::daemon
