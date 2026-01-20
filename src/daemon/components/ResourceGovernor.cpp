// ResourceGovernor implementation
// Monitors cumulative resource usage and coordinates graduated pressure responses.

#include <yams/daemon/components/ResourceGovernor.h>

#include <algorithm>
#include <spdlog/spdlog.h>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/PoolManager.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/ipc/mux_metrics_registry.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/profiling.h>

// Platform-specific RSS reading
#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <psapi.h>
#elif defined(__APPLE__)
#include <mach/mach.h>
#else
#include <cstdio>
#include <cstring>
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

    // Compute pressure level with hysteresis
    ResourcePressureLevel newLevel = computeLevel(snap);
    ResourcePressureLevel oldLevel = currentLevel_.load(std::memory_order_relaxed);

    // Handle level transitions
    if (newLevel != oldLevel) {
        spdlog::info("[ResourceGovernor] Pressure level: {} -> {} (RSS={} MiB, budget={} MiB, "
                     "pressure={:.1f}%)",
                     pressureLevelName(oldLevel), pressureLevelName(newLevel),
                     snap.rssBytes / (1024ull * 1024ull),
                     snap.memoryBudgetBytes / (1024ull * 1024ull), snap.memoryPressure * 100.0);

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

// ============================================================================
// Metric Collection
// ============================================================================

void ResourceGovernor::collectMetrics(ServiceManager* sm, ResourceSnapshot& snap) {
    YAMS_ZONE_SCOPED_N("ResourceGovernor::collectMetrics");

    // Memory metrics
    snap.rssBytes = readRssBytes();
    snap.memoryBudgetBytes = TuneAdvisor::memoryBudgetBytes();

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

    if (snap.memoryPressure >= emergencyThresh) {
        rawLevel = ResourcePressureLevel::Emergency;
    } else if (snap.memoryPressure >= criticalThresh) {
        rawLevel = ResourcePressureLevel::Critical;
    } else if (snap.memoryPressure >= warningThresh) {
        rawLevel = ResourcePressureLevel::Warning;
    }

    // Apply hysteresis: require consecutive ticks at a level before transitioning
    const std::uint32_t hysteresisTicks = TuneAdvisor::memoryHysteresisTicks();

    if (rawLevel == proposedLevel_) {
        ++ticksAtProposedLevel_;
    } else {
        proposedLevel_ = rawLevel;
        ticksAtProposedLevel_ = 1;
    }

    // For escalation (getting worse), apply hysteresis
    // For de-escalation (getting better), apply double hysteresis for stability
    ResourcePressureLevel currentLvl = currentLevel_.load(std::memory_order_relaxed);

    if (rawLevel > currentLvl) {
        // Escalating - require hysteresis ticks
        if (ticksAtProposedLevel_ >= hysteresisTicks) {
            return rawLevel;
        }
        return currentLvl; // Hold at current level
    } else if (rawLevel < currentLvl) {
        // De-escalating - require double hysteresis for stability
        if (ticksAtProposedLevel_ >= hysteresisTicks * 2) {
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
    return scalingCaps_.allowNewIngest;
}

// ============================================================================
// Pressure Response Actions
// ============================================================================

void ResourceGovernor::onNormalLevel(ServiceManager* sm) {
    spdlog::info("[ResourceGovernor] Normal: restored full scaling capacity");

    // Restore TuneAdvisor queue limits
    TuneAdvisor::setPostIngestQueueMax(1000);

    (void)sm; // May use in future for resuming paused stages
}

void ResourceGovernor::onWarningLevel(ServiceManager* sm) {
    spdlog::info("[ResourceGovernor] Warning: capping scaling, blocking model loads");

    // Reduce post-ingest queue capacity to apply backpressure
    TuneAdvisor::setPostIngestQueueMax(500);

    (void)sm;
}

void ResourceGovernor::onCriticalLevel(ServiceManager* sm) {
    spdlog::warn("[ResourceGovernor] Critical: minimum concurrency, triggering model eviction");

    // Heavily reduce queue capacity
    TuneAdvisor::setPostIngestQueueMax(100);

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
    return currentLevel_.load(std::memory_order_relaxed);
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
