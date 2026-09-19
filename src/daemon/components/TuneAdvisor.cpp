// TuneAdvisor platform-specific implementation
// Most of TuneAdvisor is header-only, but detectSystemMemory() requires
// platform headers that would pollute the public interface.

#include <yams/daemon/components/TuneAdvisor.h>

#include <cstdint>
#include <cstdio>
#include <cstring>

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#elif defined(__APPLE__)
#include <sys/sysctl.h>
#include <sys/types.h>
#endif

namespace yams::daemon {

uint64_t TuneAdvisor::detectSystemMemory() {
    static const uint64_t cached = []() -> uint64_t {
        constexpr uint64_t kFallback = 4ull * 1024ull * 1024ull * 1024ull; // 4 GiB

#if defined(_WIN32)
        MEMORYSTATUSEX memInfo{};
        memInfo.dwLength = sizeof(MEMORYSTATUSEX);
        if (GlobalMemoryStatusEx(&memInfo)) {
            return memInfo.ullTotalPhys;
        }
        return kFallback;

#elif defined(__APPLE__)
        uint64_t memSize = 0;
        size_t len = sizeof(memSize);
        // Use sysctlbyname which is more portable than mib array
        if (sysctlbyname("hw.memsize", &memSize, &len, nullptr, 0) == 0) {
            return memSize;
        }
        return kFallback;

#else
        // Linux: read /proc/meminfo
        std::FILE* f = std::fopen("/proc/meminfo", "r");
        if (f) {
            char line[256];
            while (std::fgets(line, sizeof(line), f)) {
                if (std::strncmp(line, "MemTotal:", 9) == 0) {
                    unsigned long kb = 0;
                    if (std::sscanf(line + 9, " %lu", &kb) == 1) {
                        std::fclose(f);
                        return static_cast<uint64_t>(kb) * 1024ull;
                    }
                }
            }
            std::fclose(f);
        }
        return kFallback;
#endif
    }();

    return cached;
}

// ---------------------------------------------------------------------------
// Method bodies moved out of TuneAdvisor.h. The header keeps declarations, the
// static inline override slots, and the YAMS_TESTING-conditional members.
// ---------------------------------------------------------------------------

TuneAdvisor::Profile TuneAdvisor::tuningProfile() {
    int ov = tuningProfileOverride_.load(std::memory_order_relaxed);
    if (ov == 1)
        return Profile::Efficient;
    if (ov == 2)
        return Profile::Balanced;
    if (ov == 3)
        return Profile::Aggressive;
    if (const char* s = compatibilityEnvironment("YAMS_TUNING_PROFILE")) {
        std::string v{s};
        for (auto& c : v)
            c = static_cast<char>(std::tolower(c));
        if (v == "efficient" || v == "conservative")
            return Profile::Efficient;
        if (v == "aggressive")
            return Profile::Aggressive;
    }
    return Profile::Balanced;
}

void TuneAdvisor::setTuningProfile(Profile p) {
    int code = 0;
    switch (p) {
        case Profile::Efficient:
            code = 1;
            break;
        case Profile::Balanced:
            code = 2;
            break;
        case Profile::Aggressive:
            code = 3;
            break;
    }
    tuningProfileOverride_.store(code, std::memory_order_relaxed);
}

double TuneAdvisor::profileScale() {
    switch (tuningProfile()) {
        case Profile::Efficient:
            return 0.0;
        case Profile::Aggressive:
            return 1.0;
        case Profile::Balanced:
        default:
            return 0.5;
    }
}

double TuneAdvisor::getEmbedSafety() {
    return embedSafety();
}

std::size_t TuneAdvisor::getEmbedDocCap() {
    return embedDocCap();
}

std::size_t TuneAdvisor::resolvedEmbedDocCap() {
    const std::size_t cap = getEmbedDocCap();
    return cap == 0 ? kDefaultEmbedDocCap : cap;
}

std::size_t TuneAdvisor::getEmbedJobDocCap() {
    return embedJobDocCap();
}

std::size_t TuneAdvisor::resolvedEmbedJobDocCap() {
    const std::size_t cap = getEmbedJobDocCap();
    if (cap != 0) {
        return cap;
    }
    const std::size_t inferCap = resolvedEmbedDocCap();
    return std::min(inferCap, kDefaultEmbedJobDocCap);
}

unsigned TuneAdvisor::getEmbedPauseMs() {
    return embedPauseMs();
}

uint32_t TuneAdvisor::getEmbedMaxConcurrency() {
    return embedMaxConcurrency();
}

bool TuneAdvisor::hasCompatibilityEnvironmentValue(const char* name) {
    return compatibilityEnvironment(name) != nullptr;
}

TuneAdvisor::ConfiguredOverridePublication TuneAdvisor::beginConfiguredOverridePublication() {
    return ConfiguredOverridePublication{};
}

TuneAdvisor::ConfiguredOverrideReloadGuard TuneAdvisor::beginConfiguredOverrideReload() {
    return ConfiguredOverrideReloadGuard{};
}

TuneAdvisor::ConfiguredOverrideUpdate TuneAdvisor::beginConfiguredOverrideUpdate() {
    return ConfiguredOverrideUpdate{};
}

std::uint64_t TuneAdvisor::configuredOverridesVersion() noexcept {
    return configuredOverrideSequence_.load(std::memory_order_acquire);
}

void TuneAdvisor::resetConfiguredOverrides() noexcept {
    tuningProfileOverride_.store(0, std::memory_order_relaxed);
    backpressureReadPauseMsOverride_.store(0, std::memory_order_relaxed);
    workerPollMsOverride_.store(0, std::memory_order_relaxed);
    workerPollMsPinned_.store(false, std::memory_order_relaxed);
    idleCpuPctOverride_.store(-1.0, std::memory_order_relaxed);
    idleMuxLowBytesOverride_.store(0, std::memory_order_relaxed);
    idleShrinkHoldMsOverride_.store(0, std::memory_order_relaxed);
    poolCooldownMsOverride_.store(0, std::memory_order_relaxed);
    poolScaleStepOverride_.store(0, std::memory_order_relaxed);
    poolMinSizeIpcOverride_.store(0, std::memory_order_relaxed);
    poolMaxSizeIpcOverride_.store(0, std::memory_order_relaxed);
    poolMinSizeIpcIoOverride_.store(0, std::memory_order_relaxed);
    poolMaxSizeIpcIoOverride_.store(0, std::memory_order_relaxed);
    ioConnPerThreadOverride_.store(0, std::memory_order_relaxed);
    postIngestThreads_.store(0, std::memory_order_relaxed);
    postIngestQueueMaxOverride_.store(0, std::memory_order_relaxed);
    postIngestPendingKgMaxOverride_.store(0, std::memory_order_relaxed);
    listInflightLimitOverride_.store(0, std::memory_order_relaxed);
    listAdmissionWaitMsOverride_.store(0, std::memory_order_relaxed);
    grepInflightLimitOverride_.store(0, std::memory_order_relaxed);
    grepAdmissionWaitMsOverride_.store(0, std::memory_order_relaxed);
    useInternalBusRepair_.store(true, std::memory_order_relaxed);
    useInternalBusPostIngest_.store(true, std::memory_order_relaxed);
    ipcTimeoutMsOverride_.store(0, std::memory_order_relaxed);
    streamChunkTimeoutMsOverride_.store(0, std::memory_order_relaxed);
    enableResourceGovernorOverride_.store(-1, std::memory_order_relaxed);
    enableAdmissionControlOverride_.store(-1, std::memory_order_relaxed);
    governorWarningScalePctOverride_.store(0, std::memory_order_relaxed);
    memoryBudgetBytesOverride_.store(0, std::memory_order_relaxed);
    memoryWarningPctOverride_.store(0.0, std::memory_order_relaxed);
    memoryCriticalPctOverride_.store(0.0, std::memory_order_relaxed);
    memoryEmergencyPctOverride_.store(0.0, std::memory_order_relaxed);
    memoryHysteresisMsOverride_.store(0, std::memory_order_relaxed);
    cpuLevelHysteresisMsOverride_.store(0, std::memory_order_relaxed);
    postIngestRpcQueueMaxOverride_.store(0, std::memory_order_relaxed);
    postIngestRpcMaxPerBatchOverride_.store(0, std::memory_order_relaxed);
    postIngestTotalConcurrentOverride_.store(0, std::memory_order_relaxed);
    postExtractionConcurrentOverride_.store(0, std::memory_order_relaxed);
    postKgConcurrentOverride_.store(0, std::memory_order_relaxed);
    postSymbolConcurrentOverride_.store(0, std::memory_order_relaxed);
    postEntityConcurrentOverride_.store(0, std::memory_order_relaxed);
    postTitleConcurrentOverride_.store(0, std::memory_order_relaxed);
    postEmbedConcurrentOverride_.store(0, std::memory_order_relaxed);
    postIngestBatchSizeOverride_.store(0, std::memory_order_relaxed);
    enableGradientLimitersOverride_.store(-1, std::memory_order_relaxed);
    gradientSmoothingAlphaOverride_.store(0.0, std::memory_order_relaxed);
    gradientLongAlphaOverride_.store(0.0, std::memory_order_relaxed);
    gradientWarmupSamplesOverride_.store(0, std::memory_order_relaxed);
    gradientToleranceOverride_.store(0.0, std::memory_order_relaxed);
    connectionSlotsMinOverride_.store(0, std::memory_order_relaxed);
    connectionSlotsMaxOverride_.store(0, std::memory_order_relaxed);
    connectionSlotsScaleStepOverride_.store(0, std::memory_order_relaxed);
    resetCpuHighThresholdPercentOverride();
    onnxMaxConcurrentOverride_.store(0, std::memory_order_relaxed);
    onnxGlinerReservedOverride_.store(UINT32_MAX, std::memory_order_relaxed);
    onnxEmbedReservedOverride_.store(UINT32_MAX, std::memory_order_relaxed);
    onnxRerankerReservedOverride_.store(UINT32_MAX, std::memory_order_relaxed);
    onnxSessionsPerModelOverride_.store(0, std::memory_order_relaxed);
    resetModelEvictThresholdOverrides();
    maxIngestWorkersOverride_.store(0, std::memory_order_relaxed);
    storeDocumentChannelCapacityOverride_.store(0, std::memory_order_relaxed);
    workCoordinatorThreadsOverride_.store(0, std::memory_order_relaxed);
    embedChannelCapacityOverride_.store(0, std::memory_order_relaxed);
    resetConnectionLifetimeSecondsOverride();
    gradientInitialLimitOverride_.store(0.0, std::memory_order_relaxed);
    gradientMinLimitOverride_.store(0.0, std::memory_order_relaxed);
    gradientMaxLimitOverride_.store(0.0, std::memory_order_relaxed);
}

std::recursive_mutex& TuneAdvisor::configuredOverridePublicationMutex() {
    static std::recursive_mutex mutex;
    return mutex;
}

std::mutex& TuneAdvisor::configuredOverrideWriteMutex() {
    static std::mutex mutex;
    return mutex;
}

std::mutex& TuneAdvisor::configuredOverrideLifecycleMutex() {
    static std::mutex mutex;
    return mutex;
}

std::condition_variable& TuneAdvisor::configuredOverrideLifecycleCv() {
    static std::condition_variable cv;
    return cv;
}

bool& TuneAdvisor::configuredOverrideLifecycleInitializing() {
    static bool initializing = false;
    return initializing;
}

std::size_t& TuneAdvisor::configuredOverrideLifecycleCount() {
    static std::size_t count = 0;
    return count;
}

std::mutex& TuneAdvisor::postIngestStageActivityMutex() {
    static std::mutex mutex;
    return mutex;
}

std::uint64_t& TuneAdvisor::nextPostIngestStageActivityToken() {
    static std::uint64_t token = 0;
    return token;
}

std::map<std::uint64_t, std::uint8_t>& TuneAdvisor::livePostIngestStageActivityTokens() {
    static std::map<std::uint64_t, std::uint8_t> tokens;
    return tokens;
}

void TuneAdvisor::resetPostIngestRuntimeStateForNewLifecycle() noexcept {
    {
        std::lock_guard activityLock(postIngestStageActivityMutex());
        livePostIngestStageActivityTokens().clear();
        postIngestStageActiveMaskOverride_.store(0, std::memory_order_release);
        for (auto& ownerCount : postIngestStageOwnerCounts_) {
            ownerCount.store(0, std::memory_order_release);
        }
    }
    beginDynamicCapWrite();
    setPostExtractionConcurrentDynamicCap(UINT32_MAX);
    setPostKgConcurrentDynamicCap(UINT32_MAX);
    setPostSymbolConcurrentDynamicCap(UINT32_MAX);
    setPostEntityConcurrentDynamicCap(UINT32_MAX);
    setPostTitleConcurrentDynamicCap(UINT32_MAX);
    setPostEmbedConcurrentDynamicCap(UINT32_MAX);
    endDynamicCapWrite();
}

void TuneAdvisor::ignoreInvalidEnvParseFailure() noexcept {}

std::mutex& TuneAdvisor::compatibilityEnvironmentSnapshotMutex() {
    static auto* mutex = new std::mutex();
    return *mutex;
}

std::map<std::string, std::string>& TuneAdvisor::compatibilityEnvironmentSnapshot() {
    static auto* snapshot = new std::map<std::string, std::string>();
    return *snapshot;
}

bool& TuneAdvisor::compatibilityEnvironmentSnapshotInitialized() {
    static auto* initialized = new bool(false);
    return *initialized;
}

std::optional<bool> TuneAdvisor::parseExplicitBoolEnvNow(const char* name) {
    if (const char* s = compatibilityEnvironment(name)) {
        std::string v{s};
        std::transform(v.begin(), v.end(), v.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (v == "0" || v == "false" || v == "off" || v == "no")
            return false;
        if (v == "1" || v == "true" || v == "on" || v == "yes")
            return true;
    }
    return std::nullopt;
}

std::optional<uint32_t> TuneAdvisor::parseBoundedUintEnvNow(const char* name, uint32_t minValue,
                                                            uint32_t maxValue) {
    if (const char* s = compatibilityEnvironment(name)) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= minValue && v <= maxValue)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return std::nullopt;
}

std::optional<uint64_t> TuneAdvisor::parseBoundedUint64EnvNow(const char* name, uint64_t minValue,
                                                              uint64_t maxValue) {
    if (const char* s = compatibilityEnvironment(name)) {
        try {
            uint64_t v = static_cast<uint64_t>(std::stoull(s));
            if (v >= minValue && v <= maxValue)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return std::nullopt;
}

std::optional<int> TuneAdvisor::parseBoundedIntEnvNow(const char* name, int minValue,
                                                      int maxValue) {
    if (const char* s = compatibilityEnvironment(name)) {
        try {
            int v = std::stoi(s);
            if (v >= minValue && v <= maxValue)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return std::nullopt;
}

uint32_t TuneAdvisor::readUint32Override(const std::atomic<uint32_t>& overrideValue,
                                         const char* envName, uint32_t defaultValue,
                                         uint32_t minValue, uint32_t maxValue) {
    uint32_t ov = overrideValue.load(std::memory_order_relaxed);
    if (ov != 0)
        return ov;
    if (auto envValue = parseBoundedUintEnvNow(envName, minValue, maxValue))
        return *envValue;
    return defaultValue;
}

std::uint64_t TuneAdvisor::readUint64Override(const std::atomic<std::uint64_t>& overrideValue,
                                              const char* envName, std::uint64_t defaultValue,
                                              std::uint64_t minValue, std::uint64_t maxValue) {
    std::uint64_t ov = overrideValue.load(std::memory_order_relaxed);
    if (ov != 0)
        return ov;
    if (auto envValue = parseBoundedUint64EnvNow(envName, minValue, maxValue))
        return *envValue;
    return defaultValue;
}

int TuneAdvisor::readPositiveIntOverride(const std::atomic<int>& overrideValue, const char* envName,
                                         int defaultValue, int minValue, int maxValue) {
    int ov = overrideValue.load(std::memory_order_relaxed);
    if (ov > 0)
        return ov;
    if (auto envValue = parseBoundedIntEnvNow(envName, minValue, maxValue))
        return *envValue;
    return defaultValue;
}

double TuneAdvisor::readPositiveDoubleOverride(const std::atomic<double>& overrideValue,
                                               const char* envName, double defaultValue,
                                               double minValue, double maxValue) {
    double ov = overrideValue.load(std::memory_order_relaxed);
    if (ov > 0.0)
        return ov;
    if (auto envValue = parseBoundedDoubleEnvNow(envName, minValue, maxValue))
        return *envValue;
    return defaultValue;
}

std::optional<double> TuneAdvisor::parseBoundedDoubleEnvNow(const char* name, double minValue,
                                                            double maxValue, double scale) {
    if (const char* s = compatibilityEnvironment(name)) {
        try {
            double v = std::stod(s) * scale;
            if (v >= minValue && v <= maxValue)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return std::nullopt;
}

std::optional<uint32_t> TuneAdvisor::postStageConcurrentEnvOverride(const char* env,
                                                                    uint32_t maxCap) {
    return parseBoundedUintEnvNow(env, 1u, maxCap);
}

double TuneAdvisor::cpuHighThresholdPercent() {
    double ov = cpuHighPct_.load(std::memory_order_relaxed);
    if (ov > 0.0)
        return ov;
    auto envValue = parseBoundedDoubleEnvNow("YAMS_CPU_HIGH_PCT", 10.0, 100.0);
    if (envValue)
        return *envValue;
    return 50.0 + profileScale() * 35.0;
}

void TuneAdvisor::setCpuHighThresholdPercent(double v) {
    if (std::isfinite(v) && v >= 10.0 && v <= 100.0) {
        cpuHighPct_.store(v, std::memory_order_relaxed);
    }
}

void TuneAdvisor::resetCpuHighThresholdPercentOverride() {
    cpuHighPct_.store(0.0, std::memory_order_relaxed);
}

double TuneAdvisor::cpuCriticalGapPercent() {
    auto envValue = parseBoundedDoubleEnvNow("YAMS_CPU_CRITICAL_GAP_PCT", 10.0, 50.0);
    if (envValue)
        return *envValue;
    return 40.0;
}

uint32_t TuneAdvisor::cpuAdmissionHighHoldMs() {
    uint32_t def = 250;
    auto envValue = parseBoundedUintEnvNow("YAMS_CPU_ADMIT_HIGH_HOLD_MS", 0u, 60000u);
    if (envValue)
        return *envValue;
    return def;
}

uint32_t TuneAdvisor::cpuAdmissionLowHoldMs() {
    uint32_t def = 500;
    auto envValue = parseBoundedUintEnvNow("YAMS_CPU_ADMIT_LOW_HOLD_MS", 0u, 60000u);
    if (envValue)
        return *envValue;
    return def;
}

int32_t TuneAdvisor::computeCpuThrottleDelayMs(double currentCpuPct) {
    double threshold = cpuHighThresholdPercent();
    if (currentCpuPct < threshold)
        return 0;
    double overage = currentCpuPct - threshold;
    int32_t delayMs = static_cast<int32_t>(std::llround(overage * 0.5));
    return std::clamp(delayMs, 2, 25);
}

double TuneAdvisor::embedSafety() {
    return embedSafety_.load(std::memory_order_relaxed);
}

std::size_t TuneAdvisor::embedDocCap() {
    std::size_t ov = embedDocCap_.load(std::memory_order_relaxed);
    if (ov != 0)
        return ov;
    auto envValue = parseBoundedUint64EnvNow("YAMS_EMBED_DOC_CAP", 1u, 4096u);
    if (envValue)
        return static_cast<std::size_t>(*envValue);
    return 0;
}

void TuneAdvisor::setEmbedDocCap(std::size_t v) {
    embedDocCap_.store(v, std::memory_order_relaxed);
}

std::size_t TuneAdvisor::embedJobDocCap() {
    std::size_t ov = embedJobDocCap_.load(std::memory_order_relaxed);
    if (ov != 0)
        return ov;
    auto envValue = parseBoundedUint64EnvNow("YAMS_EMBED_JOB_DOC_CAP", 1u, 4096u);
    if (envValue)
        return static_cast<std::size_t>(*envValue);
    return 0;
}

unsigned TuneAdvisor::embedPauseMs() {
    return embedPauseMs_.load(std::memory_order_relaxed);
}

uint32_t TuneAdvisor::chunkSize() {
    uint32_t def = 512u * 1024u;
    if (const char* cs = compatibilityEnvironment("YAMS_CHUNK_SIZE")) {
        try {
            auto v = static_cast<uint64_t>(std::stoull(cs));
            if (v >= 4ull * 1024ull && v <= 8ull * 1024ull * 1024ull)
                return static_cast<uint32_t>(v);
        } catch (const std::exception&) {
            return def;
        }
    }
    return def;
}

uint32_t TuneAdvisor::writerBudgetBytesPerTurn() {
    if (auto snap = TuningSnapshotRegistry::instance().get()) {
        return static_cast<uint32_t>(snap->writerBudgetBytesPerTurn);
    }
    uint32_t def = 3072u * 1024u; // 3 MiB
    if (const char* wb = compatibilityEnvironment("YAMS_WRITER_BUDGET_BYTES")) {
        try {
            auto v = static_cast<uint64_t>(std::stoull(wb));
            if (v >= 64ull * 1024ull && v <= 64ull * 1024ull * 1024ull)
                return static_cast<uint32_t>(v);
        } catch (const std::exception&) {
            return def;
        }
    }
    return def;
}

std::size_t TuneAdvisor::serverMaxInflightPerConn() {
    if (auto snap = TuningSnapshotRegistry::instance().get()) {
        return snap->serverMaxInflightPerConn;
    }
    if (const char* s = compatibilityEnvironment("YAMS_SERVER_MAX_INFLIGHT")) {
        try {
            std::size_t v = static_cast<std::size_t>(std::stoul(s));
            if (v > 0)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return static_cast<std::size_t>(64);
}

std::size_t TuneAdvisor::serverQueueFramesCap() {
    if (auto snap = TuningSnapshotRegistry::instance().get()) {
        return snap->serverQueueFramesCap;
    }
    if (const char* s = compatibilityEnvironment("YAMS_SERVER_QUEUE_FRAMES_CAP")) {
        try {
            std::size_t v = static_cast<std::size_t>(std::stoul(s));
            if (v > 0)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return static_cast<std::size_t>(1024);
}

std::size_t TuneAdvisor::serverQueueBytesCap() {
    if (auto snap = TuningSnapshotRegistry::instance().get()) {
        return snap->serverQueueBytesCap;
    }
    if (const char* s = compatibilityEnvironment("YAMS_SERVER_QUEUE_BYTES_CAP")) {
        try {
            std::size_t v = static_cast<std::size_t>(std::stoul(s));
            if (v >= 1024)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return static_cast<std::size_t>(128ull * 1024ull * 1024ull);
}

std::size_t TuneAdvisor::serverWriterBudgetBytesPerTurn() {
    if (auto snap = TuningSnapshotRegistry::instance().get()) {
        return snap->serverWriterBudgetBytesPerTurn;
    }
    if (const char* s = compatibilityEnvironment("YAMS_SERVER_WRITER_BUDGET_BYTES")) {
        try {
            std::size_t v = static_cast<std::size_t>(std::stoul(s));
            if (v >= 4096)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return static_cast<std::size_t>(8ull * 1024ull * 1024ull);
}

std::size_t TuneAdvisor::serverWriterBudgetMaxBytesPerTurn() {
    if (auto snap = TuningSnapshotRegistry::instance().get()) {
        return snap->serverWriterBudgetMaxBytesPerTurn;
    }
    std::size_t def = 8ull * 1024ull * 1024ull;
    if (const char* mb = compatibilityEnvironment("YAMS_SERVER_WRITER_BUDGET_MAX")) {
        try {
            auto v = static_cast<std::size_t>(std::stoul(mb));
            if (v >= 4096)
                return v;
        } catch (const std::exception&) {
            return def;
        }
    }
    return def;
}

uint64_t TuneAdvisor::maxWorkerQueue(size_t workerThreads) {
    if (const char* s = compatibilityEnvironment("YAMS_MAX_WORKER_QUEUE")) {
        try {
            return static_cast<uint64_t>(std::stoull(s));
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    if (workerThreads == 0)
        return 0; // unknown
    double scale = profileScale();
    if (scale < 0.5)
        scale = 0.5;
    double multiplier = 2.0 * scale;
    auto derived = static_cast<uint64_t>(
        std::max(1.0, std::ceil(static_cast<double>(workerThreads) * multiplier)));
    return derived;
}

uint64_t TuneAdvisor::maxMuxBytes() {
    constexpr uint64_t kBase = 256ull * 1024ull * 1024ull;
    double scale = profileScale();
    if (scale < 0.5)
        scale = 0.5;
    if (scale > 2.0)
        scale = 2.0;
    uint64_t def = static_cast<uint64_t>(std::llround(static_cast<double>(kBase) * scale));
    if (def < 64ull * 1024ull * 1024ull)
        def = 64ull * 1024ull * 1024ull;
    if (const char* s = compatibilityEnvironment("YAMS_MAX_MUX_BYTES")) {
        try {
            return static_cast<uint64_t>(std::stoull(s));
        } catch (const std::exception&) {
            return def;
        }
    }
    return def;
}

uint64_t TuneAdvisor::maxActiveConn() {
    if (const char* s = compatibilityEnvironment("YAMS_MAX_ACTIVE_CONN")) {
        try {
            return static_cast<uint64_t>(std::stoull(s));
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 0;
}

uint32_t TuneAdvisor::statusTickMs() {
    return 5;
}

uint32_t TuneAdvisor::idleTickMs() {
    return 1000;
}

uint32_t TuneAdvisor::repairMaxBatch() {
    if (const char* s = compatibilityEnvironment("YAMS_REPAIR_MAX_BATCH")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v > 0 && v <= 1000)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    // Profile-scaled: Efficient=8, Balanced=20, Aggressive=32
    uint32_t base = 8;
    uint32_t range = 24;
    return base + static_cast<uint32_t>(range * profileScale());
}

uint32_t TuneAdvisor::repairStartupBatchSize() {
    if (const char* s = compatibilityEnvironment("YAMS_REPAIR_STARTUP_BATCH")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v > 0 && v <= 1000)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    // Profile-scaled: Efficient=25, Balanced=62, Aggressive=100
    uint32_t base = 25;
    uint32_t range = 75;
    return base + static_cast<uint32_t>(range * profileScale());
}

uint32_t TuneAdvisor::repairTokensIdle() {
    uint32_t def = 1;
    double scale = profileScale();
    if (scale >= 1.0) {
        def = 4;
    } else if (scale >= 0.5) {
        def = 2;
    }

    if (const char* s = compatibilityEnvironment("YAMS_REPAIR_TOKENS_IDLE")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            return v;
        } catch (const std::exception&) {
            return def;
        }
    }
    return def;
}

uint32_t TuneAdvisor::repairTokensBusy() {
    uint32_t def = 0;
    if (profileScale() >= 1.0) {
        def = 1;
    }

    auto envValue = parseBoundedUintEnvNow("YAMS_REPAIR_TOKENS_BUSY", 0u, 256u);
    if (envValue)
        return *envValue;
    return def;
}

uint32_t TuneAdvisor::repairBusyConnThreshold() {
    uint32_t def = 1;
    auto envValue = parseBoundedUintEnvNow("YAMS_REPAIR_BUSY_CONN_THRESHOLD", 0u, 1024u);
    if (envValue)
        return *envValue;
    return def;
}

uint32_t TuneAdvisor::repairMaxBatchesPerSec() {
    uint32_t def = 1;
    if (const char* s = compatibilityEnvironment("YAMS_REPAIR_MAX_BATCHES_PER_SEC")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v <= 1000)
                return v;
        } catch (const std::exception&) {
            return def;
        }
    }
    return def;
}

uint32_t TuneAdvisor::orphanScanIntervalHours() {
    uint32_t def = 6;
    if (const char* s = compatibilityEnvironment("YAMS_ORPHAN_SCAN_INTERVAL_HOURS")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= 1 && v <= 48)
                return v;
        } catch (const std::exception&) {
            return def;
        }
    }
    return def;
}

uint32_t TuneAdvisor::repairDegradeHoldMs() {
    return repair_tuning::repairDegradeHoldMs();
}

uint32_t TuneAdvisor::repairReadyHoldMs() {
    return repair_tuning::repairReadyHoldMs();
}

uint32_t TuneAdvisor::repairAutoInitialDelayMinutes() {
    uint32_t def = 10;
    if (const char* s = compatibilityEnvironment("YAMS_REPAIR_AUTO_INITIAL_DELAY_MIN")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v <= 1440)
                return v;
        } catch (const std::exception&) {
            return def;
        }
    }
    return def;
}

uint32_t TuneAdvisor::repairAutoFastMinutes() {
    uint32_t def = 30;
    if (const char* s = compatibilityEnvironment("YAMS_REPAIR_AUTO_FAST_MIN")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v <= 1440)
                return v;
        } catch (const std::exception&) {
            return def;
        }
    }
    return def;
}

uint32_t TuneAdvisor::repairAutoWarmHours() {
    uint32_t def = 6;
    if (const char* s = compatibilityEnvironment("YAMS_REPAIR_AUTO_WARM_HOURS")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v <= 168)
                return v;
        } catch (const std::exception&) {
            return def;
        }
    }
    return def;
}

uint32_t TuneAdvisor::repairAutoColdHours() {
    uint32_t def = 168;
    if (const char* s = compatibilityEnvironment("YAMS_REPAIR_AUTO_COLD_HOURS")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v <= 720)
                return v;
        } catch (const std::exception&) {
            return def;
        }
    }
    return def;
}

uint32_t TuneAdvisor::fts5StartupDelayMs() {
    uint32_t def = 2000;
    if (const char* s = compatibilityEnvironment("YAMS_FTS5_STARTUP_DELAY_MS")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v <= 60000)
                return v;
        } catch (const std::exception&) {
            return def;
        }
    }
    return def;
}

uint32_t TuneAdvisor::fts5StartupThrottleMs() {
    uint32_t def = 100;
    if (const char* s = compatibilityEnvironment("YAMS_FTS5_STARTUP_THROTTLE_MS")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= 10 && v <= 1000)
                return v;
        } catch (const std::exception&) {
            return def;
        }
    }
    return def;
}

uint32_t TuneAdvisor::metricsCacheMs() {
    uint32_t def = 250;
    if (const char* s = compatibilityEnvironment("YAMS_METRICS_CACHE_MS")) {
        try {
            return static_cast<uint32_t>(std::stoul(s));
        } catch (const std::exception&) {
            return def;
        }
    }
    return def;
}

uint32_t TuneAdvisor::cpuBudgetPercent() {
    uint32_t def = static_cast<uint32_t>(40.0 + profileScale() * 20.0);
    if (const char* s = compatibilityEnvironment("YAMS_CPU_BUDGET_PERCENT")) {
        try {
            int v = std::stoi(s);
            if (v >= 10 && v <= 100)
                return static_cast<uint32_t>(v);
        } catch (const std::exception&) {
            return def;
        }
    }
    return def;
}

uint32_t TuneAdvisor::maxThreadsOverall() {
    if (const char* s = compatibilityEnvironment("YAMS_MAX_THREADS")) {
        try {
            int v = std::stoi(s);
            if (v >= 1 && v <= 1024)
                return static_cast<uint32_t>(v);
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 0;
}

uint32_t TuneAdvisor::hostThreadReserve(unsigned hw) {
    if (hw <= 1u)
        return 0u;

    switch (tuningProfile()) {
        case Profile::Efficient:
            return std::min(std::max<uint32_t>(2u, static_cast<uint32_t>(std::ceil(hw * 0.25))),
                            hw - 1u);
        case Profile::Aggressive:
            return std::min(std::max<uint32_t>(1u, static_cast<uint32_t>(std::ceil(hw * 0.10))),
                            hw - 1u);
        case Profile::Balanced:
        default:
            return std::min(std::max<uint32_t>(2u, static_cast<uint32_t>(std::ceil(hw * 0.15))),
                            hw - 1u);
    }
}

uint32_t TuneAdvisor::daemonThreadCapacity(unsigned hw) {
    if (hw == 0u)
        hw = 1u;
    const uint32_t reserve = hostThreadReserve(hw);
    return std::max<uint32_t>(1u, hw - reserve);
}

uint64_t TuneAdvisor::autoMemoryBudgetBytes(uint64_t systemMem) {
    if (systemMem == 0) {
        return 256ull * 1024ull * 1024ull;
    }

    switch (tuningProfile()) {
        case Profile::Efficient:
            return std::clamp((systemMem * 45ull) / 100ull, 256ull * 1024ull * 1024ull,
                              systemMem > 256ull * 1024ull * 1024ull
                                  ? (systemMem - 256ull * 1024ull * 1024ull)
                                  : 256ull * 1024ull * 1024ull);
        case Profile::Aggressive:
            return std::clamp((systemMem * 75ull) / 100ull, 256ull * 1024ull * 1024ull,
                              systemMem > 256ull * 1024ull * 1024ull
                                  ? (systemMem - 256ull * 1024ull * 1024ull)
                                  : 256ull * 1024ull * 1024ull);
        case Profile::Balanced:
        default:
            break;
    }

    const uint64_t budget = (systemMem * 60ull) / 100ull;
    const uint64_t minBudget = 256ull * 1024ull * 1024ull;
    const uint64_t maxBudget = (systemMem > minBudget) ? (systemMem - minBudget) : minBudget;
    return std::clamp<uint64_t>(budget, minBudget, maxBudget);
}

uint32_t TuneAdvisor::workCoordinatorThreads() {
    uint32_t ov = workCoordinatorThreadsOverride_.load(std::memory_order_relaxed);
    if (ov != 0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_WORK_COORDINATOR_THREADS")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= 1 && v <= 512)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return defaultReadPathCapacityModel(hardwareConcurrency()).workerThreads;
}

void TuneAdvisor::setWorkCoordinatorThreads(uint32_t n) {
    workCoordinatorThreadsOverride_.store(std::clamp<uint32_t>(n, 1u, 512u),
                                          std::memory_order_relaxed);
}

uint32_t TuneAdvisor::recommendedThreads(double backgroundFactor, uint32_t hardMax) {
    return recommendedThreadsForHw(hardwareConcurrency(), backgroundFactor, hardMax);
}

unsigned TuneAdvisor::hardwareConcurrency() {
    unsigned v = hwCached_.load(std::memory_order_relaxed);
    if (v == 0) {
        unsigned m = std::thread::hardware_concurrency();
        if (m == 0)
            m = 4;
        hwCached_.store(m, std::memory_order_relaxed);
        v = m;
    }
    return v;
}

void TuneAdvisor::setHardwareConcurrencyForTests(unsigned v) {
    if (v == 0)
        v = 1;
    hwCached_.store(v, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::embedMaxConcurrencyBase() {
    if (const char* s = compatibilityEnvironment("YAMS_EMBED_MAX_CONCURRENCY")) {
        try {
            int v = std::stoi(s);
            if (v >= 1 && v <= 1024)
                return static_cast<uint32_t>(v);
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    // Use a conservative fraction (25% of budgeted threads) for embeddings
    uint32_t rec = recommendedThreads(0.25);
    return std::max(1u, rec);
}

uint32_t TuneAdvisor::embedMaxConcurrency() {
    uint32_t dyn = embedMaxConcurrencyOverride_.load(std::memory_order_relaxed);
    if (dyn > 0)
        return dyn;
    return embedMaxConcurrencyBase();
}

void TuneAdvisor::setEmbedMaxConcurrencyDynamicCap(uint32_t v) {
    if (v == 0) {
        embedMaxConcurrencyOverride_.store(0u, std::memory_order_relaxed);
        return;
    }
    embedMaxConcurrencyOverride_.store(std::clamp<uint32_t>(v, 1u, 1024u),
                                       std::memory_order_relaxed);
}

uint32_t TuneAdvisor::postIngestThreads() {
    // 1) Explicit override set by config/daemon_main
    uint32_t configured = postIngestThreads_.load(std::memory_order_relaxed);
    if (configured != 0)
        return configured;
    // 2) Environment variable override for quick experiments
    if (const char* s = compatibilityEnvironment("YAMS_POST_INGEST_THREADS")) {
        try {
            int v = std::stoi(s);
            if (v >= 1 && v <= 64)
                return static_cast<uint32_t>(v);
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    // 3) Conservative default: single background worker; users can raise via config/env
    return 1u;
}

void TuneAdvisor::setPostIngestThreads(uint32_t n) {
    postIngestThreads_.store(n, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::postIngestQueueMax() {
    uint32_t ov = postIngestQueueMaxOverride_.load(std::memory_order_relaxed);
    if (ov != 0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_POST_INGEST_QUEUE_MAX")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= 10 && v <= 1'000'000)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 1000;
}

void TuneAdvisor::setPostIngestQueueMax(uint32_t v) {
    postIngestQueueMaxOverride_.store(v, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::postIngestPendingKgMax() {
    uint32_t ov = postIngestPendingKgMaxOverride_.load(std::memory_order_relaxed);
    if (ov != 0)
        return ov;
    return 16384;
}

void TuneAdvisor::setPostIngestPendingKgMax(uint32_t v) {
    postIngestPendingKgMaxOverride_.store(v, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::postIngestRpcQueueMax() {
    uint32_t ov = postIngestRpcQueueMaxOverride_.load(std::memory_order_relaxed);
    if (ov != 0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_POST_INGEST_RPC_QUEUE_MAX")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= 10 && v <= 1'000'000)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 256;
}

void TuneAdvisor::setPostIngestRpcQueueMax(uint32_t v) {
    postIngestRpcQueueMaxOverride_.store(v, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::postIngestRpcMaxPerBatch() {
    uint32_t ov = postIngestRpcMaxPerBatchOverride_.load(std::memory_order_relaxed);
    if (ov != 0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_POST_INGEST_RPC_MAX_PER_BATCH")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= 1 && v <= 1024)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 4;
}

void TuneAdvisor::setPostIngestRpcMaxPerBatch(uint32_t value) {
    postIngestRpcMaxPerBatchOverride_.store(value, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::postIngestBatchSize() {
    uint32_t ov = postIngestBatchSizeOverride_.load(std::memory_order_relaxed);
    if (ov != 0)
        return ov;

    // Keeps foreground retrieval responsive under concurrent ingestion while reducing
    // content-index transaction overhead. Contention feedback below still scales to 4/2/1.
    constexpr uint32_t kDefaultBatchSize = 32;
    uint32_t baseBatchSize = kDefaultBatchSize;
    auto embedCap = static_cast<uint32_t>(getEmbedDocCap());
    if (embedCap == 0) {
        embedCap = 64;
    }
    baseBatchSize = std::min({baseBatchSize, embedCap, 256u});
    auto envValue = parseBoundedUintEnvNow("YAMS_POST_INGEST_BATCH_SIZE", 1u, 256u);
    if (envValue)
        baseBatchSize = *envValue;

    // Adaptive scaling: reduce batch size when lock contention is high
    uint64_t recentErrors = metadata::dbLockErrorCount();
    if (recentErrors > 10) {
        return 1; // Maximum contention: single-document transactions
    } else if (recentErrors > 5) {
        return std::min(baseBatchSize, 2u);
    } else if (recentErrors > 2) {
        return std::min(baseBatchSize, 4u);
    }
    return baseBatchSize;
}

void TuneAdvisor::setPostIngestBatchSize(uint32_t v) {
    postIngestBatchSizeOverride_.store(v, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::ipcTimeoutMs() {
    return readUint32Override(ipcTimeoutMsOverride_, "YAMS_IPC_TIMEOUT_MS", 15000u, 500u, 600000u);
}

void TuneAdvisor::setIpcTimeoutMs(uint32_t value) {
    ipcTimeoutMsOverride_.store(value, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::streamChunkTimeoutMs() {
    return readUint32Override(streamChunkTimeoutMsOverride_, "YAMS_STREAM_CHUNK_TIMEOUT_MS", 30000u,
                              1000u, 600000u);
}

void TuneAdvisor::setStreamChunkTimeoutMs(uint32_t value) {
    streamChunkTimeoutMsOverride_.store(value, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::backpressureReadPauseMs() {
    return readUint32Override(backpressureReadPauseMsOverride_, "YAMS_BACKPRESSURE_READ_PAUSE_MS",
                              10u, 0u, 1000u);
}

void TuneAdvisor::setBackpressureReadPauseMs(uint32_t ms) {
    backpressureReadPauseMsOverride_.store(ms, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::workerPollMs() {
    return readUint32Override(workerPollMsOverride_, "YAMS_WORKER_POLL_MS", 150u, 50u, 2000u);
}

void TuneAdvisor::setWorkerPollMs(uint32_t ms) {
    workerPollMsOverride_.store(ms, std::memory_order_relaxed);
    workerPollMsPinned_.store(ms != 0, std::memory_order_relaxed);
}

void TuneAdvisor::setWorkerPollMsDynamic(uint32_t ms) {
    if (ms == 0 || workerPollMsPinned())
        return;
    workerPollMsOverride_.store(ms, std::memory_order_relaxed);
}

bool TuneAdvisor::workerPollMsPinned() {
    if (workerPollMsPinned_.load(std::memory_order_relaxed))
        return true;
    return parseBoundedUintEnvNow("YAMS_WORKER_POLL_MS", 50u, 2000u).has_value();
}

double TuneAdvisor::idleCpuThresholdPercent() {
    return readPositiveDoubleOverride(idleCpuPctOverride_, "YAMS_IDLE_CPU_PCT", 10.0, 0.0, 100.0);
}

void TuneAdvisor::setIdleCpuThresholdPercent(double pct) {
    idleCpuPctOverride_.store(pct, std::memory_order_relaxed);
}

std::uint64_t TuneAdvisor::idleMuxLowBytes() {
    return readUint64Override(idleMuxLowBytesOverride_, "YAMS_IDLE_MUX_LOW_BYTES",
                              4ull * 1024ull * 1024ull, 0ull, UINT64_MAX);
}

void TuneAdvisor::setIdleMuxLowBytes(std::uint64_t b) {
    idleMuxLowBytesOverride_.store(b, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::idleShrinkHoldMs() {
    return readUint32Override(idleShrinkHoldMsOverride_, "YAMS_IDLE_SHRINK_HOLD_MS", 5000u, 500u,
                              60000u);
}

void TuneAdvisor::setIdleShrinkHoldMs(uint32_t ms) {
    idleShrinkHoldMsOverride_.store(ms, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::poolCooldownMs() {
    uint32_t ov = poolCooldownMsOverride_.load(std::memory_order_relaxed);
    if (ov != 0)
        return ov;
    switch (tuningProfile()) {
        case Profile::Efficient:
            return 750;
        case Profile::Aggressive:
            return 250;
        case Profile::Balanced:
        default:
            break;
    }
    return parseBoundedUintEnvNow("YAMS_POOL_COOLDOWN_MS", 0u, 60000u).value_or(500u);
}

void TuneAdvisor::setPoolCooldownMs(uint32_t ms) {
    poolCooldownMsOverride_.store(ms, std::memory_order_relaxed);
}

int TuneAdvisor::poolScaleStep() {
    return readPositiveIntOverride(poolScaleStepOverride_, "YAMS_POOL_SCALE_STEP", 1, 1, 16);
}

void TuneAdvisor::setPoolScaleStep(int step) {
    poolScaleStepOverride_.store(step, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::poolMinSizeIpc() {
    return readUint32Override(poolMinSizeIpcOverride_, "YAMS_POOL_IPC_MIN", 1u, 1u, 1024u);
}

void TuneAdvisor::setPoolMinSizeIpc(uint32_t v) {
    poolMinSizeIpcOverride_.store(v, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::poolMaxSizeIpc() {
    return readUint32Override(poolMaxSizeIpcOverride_, "YAMS_POOL_IPC_MAX", 32u, 1u, 4096u);
}

void TuneAdvisor::setPoolMaxSizeIpc(uint32_t v) {
    poolMaxSizeIpcOverride_.store(v, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::poolMinSizeIpcIo() {
    return readUint32Override(poolMinSizeIpcIoOverride_, "YAMS_POOL_IO_MIN", 1u, 1u, 1024u);
}

void TuneAdvisor::setPoolMinSizeIpcIo(uint32_t v) {
    poolMinSizeIpcIoOverride_.store(v, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::poolMaxSizeIpcIo() {
    return readUint32Override(poolMaxSizeIpcIoOverride_, "YAMS_POOL_IO_MAX", 32u, 1u, 4096u);
}

void TuneAdvisor::setPoolMaxSizeIpcIo(uint32_t v) {
    poolMaxSizeIpcIoOverride_.store(v, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::poolLowWatermarkPercent() {
    return readUint32Override(poolLowWatermarkPctOverride_, "YAMS_POOL_LOW_WATERMARK_PCT", 25u, 0u,
                              100u);
}

uint32_t TuneAdvisor::poolHighWatermarkPercent() {
    return readUint32Override(poolHighWatermarkPctOverride_, "YAMS_POOL_HIGH_WATERMARK_PCT", 85u,
                              0u, 100u);
}

uint32_t TuneAdvisor::connectionSlotsMin() {
    return readUint32Override(connectionSlotsMinOverride_, "YAMS_CONN_SLOTS_MIN", 256u, 1u, 1024u);
}

void TuneAdvisor::setConnectionSlotsMin(uint32_t v) {
    connectionSlotsMinOverride_.store(v, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::connectionSlotsMax() {
    return readUint32Override(connectionSlotsMaxOverride_, "YAMS_CONN_SLOTS_MAX", 4096u, 64u,
                              16384u);
}

void TuneAdvisor::setConnectionSlotsMax(uint32_t v) {
    connectionSlotsMaxOverride_.store(v, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::connectionSlotsScaleStep() {
    return readUint32Override(connectionSlotsScaleStepOverride_, "YAMS_CONN_SLOTS_STEP", 16u, 1u,
                              128u);
}

void TuneAdvisor::setConnectionSlotsScaleStep(uint32_t v) {
    connectionSlotsScaleStepOverride_.store(v, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::connectionSlotsTarget() {
    uint32_t ov = connectionSlotsTargetOverride_.load(std::memory_order_relaxed);
    if (ov != 0)
        return ov;

    uint32_t rec = recommendedThreads();
    uint32_t per = ioConnPerThread();
    double scale = 0.5 + profileScale(); // 0.5 (Efficient) to 1.5 (Aggressive)

    uint64_t computed = static_cast<uint64_t>(rec) * static_cast<uint64_t>(per) * 4ull;
    computed = static_cast<uint64_t>(static_cast<double>(computed) * scale);

    uint32_t minSlots = connectionSlotsMin();
    if (computed < static_cast<uint64_t>(minSlots))
        computed = minSlots;

    uint32_t maxSlots = connectionSlotsMax();
    if (computed > static_cast<uint64_t>(maxSlots))
        computed = maxSlots;

    return static_cast<uint32_t>(computed);
}

uint32_t TuneAdvisor::searchConcurrencyLimit() {
    return readUint32Override(
        searchConcurrencyOverride_, "YAMS_SEARCH_MAX_CONCURRENT",
        defaultReadPathCapacityModel(hardwareConcurrency()).searchConcurrencyLimit, 1u, 512u);
}

uint32_t TuneAdvisor::readPoolMaxConnections(uint32_t configuredMax) {
    return defaultReadPoolMaxConnectionsForHw(hardwareConcurrency(), configuredMax);
}

uint32_t TuneAdvisor::listInflightLimit() {
    uint32_t ov = listInflightLimitOverride_.load(std::memory_order_relaxed);
    if (ov != 0)
        return ov;
    switch (tuningProfile()) {
        case Profile::Efficient:
            return 4;
        case Profile::Aggressive:
            return 16;
        case Profile::Balanced:
        default:
            return 8;
    }
}

void TuneAdvisor::setListInflightLimit(uint32_t v) {
    listInflightLimitOverride_.store(std::clamp<uint32_t>(v, 1u, 1024u), std::memory_order_relaxed);
}

uint32_t TuneAdvisor::listAdmissionWaitMs() {
    uint32_t ov = listAdmissionWaitMsOverride_.load(std::memory_order_relaxed);
    if (ov != 0)
        return ov;
    return 200;
}

void TuneAdvisor::setListAdmissionWaitMs(uint32_t v) {
    listAdmissionWaitMsOverride_.store(std::clamp<uint32_t>(v, 1u, 120000u),
                                       std::memory_order_relaxed);
}

uint32_t TuneAdvisor::grepInflightLimit() {
    uint32_t ov = grepInflightLimitOverride_.load(std::memory_order_relaxed);
    if (ov != 0)
        return ov;
    switch (tuningProfile()) {
        case Profile::Efficient:
            return 1;
        case Profile::Aggressive:
            return 3;
        case Profile::Balanced:
        default:
            return 1;
    }
}

void TuneAdvisor::setGrepInflightLimit(uint32_t v) {
    grepInflightLimitOverride_.store(std::clamp<uint32_t>(v, 1u, 1024u), std::memory_order_relaxed);
}

uint32_t TuneAdvisor::grepAdmissionWaitMs() {
    uint32_t ov = grepAdmissionWaitMsOverride_.load(std::memory_order_relaxed);
    if (ov != 0)
        return ov;
    return 20000;
}

void TuneAdvisor::setGrepAdmissionWaitMs(uint32_t v) {
    grepAdmissionWaitMsOverride_.store(std::clamp<uint32_t>(v, 1u, 120000u),
                                       std::memory_order_relaxed);
}

uint32_t TuneAdvisor::writerActiveLow1Threshold() {
    return 2;
}

uint32_t TuneAdvisor::writerActiveLow2Threshold() {
    return 4;
}

uint32_t TuneAdvisor::writerActiveHigh1Threshold() {
    return 8;
}

uint32_t TuneAdvisor::writerActiveHigh2Threshold() {
    return 32;
}

double TuneAdvisor::writerScaleActiveLow1Mul() {
    return 2.0;
}

double TuneAdvisor::writerScaleActiveLow2Mul() {
    return 1.5;
}

double TuneAdvisor::writerScaleActiveHigh1Mul() {
    return 2.0;
}

double TuneAdvisor::writerScaleActiveHigh2Mul() {
    return 2.0;
}

double TuneAdvisor::writerQueuedHalfThresholdFraction() {
    return 0.5;
}

double TuneAdvisor::writerQueuedThreeQuarterThresholdFraction() {
    return 0.75;
}

double TuneAdvisor::writerScaleQueuedHalfMul() {
    return 1.5;
}

double TuneAdvisor::writerScaleQueuedThreeQuarterMul() {
    return 2.0;
}

std::uint64_t TuneAdvisor::streamMuxVeryHighBytes() {
    return 256ull * 1024ull * 1024ull;
}

std::uint64_t TuneAdvisor::streamMuxHighBytes() {
    return 128ull * 1024ull * 1024ull;
}

std::uint64_t TuneAdvisor::streamMuxLight1Bytes() {
    return 8ull * 1024ull * 1024ull;
}

std::uint64_t TuneAdvisor::streamMuxLight2Bytes() {
    return 32ull * 1024ull * 1024ull;
}

std::uint64_t TuneAdvisor::streamMuxLight3Bytes() {
    return 64ull * 1024ull * 1024ull;
}

double TuneAdvisor::streamPageFactorVeryHighDiv() {
    return 0.25;
}

double TuneAdvisor::streamPageFactorHighDiv() {
    return 0.5;
}

double TuneAdvisor::streamPageFactorLight1Mul() {
    return 3.0;
}

double TuneAdvisor::streamPageFactorLight2Mul() {
    return 2.0;
}

double TuneAdvisor::streamPageFactorLight3Mul() {
    return 1.5;
}

std::size_t TuneAdvisor::streamPageClampMin() {
    return 5;
}

std::size_t TuneAdvisor::streamPageClampMax() {
    return 50000;
}

uint32_t TuneAdvisor::ioConnPerThread() {
    uint32_t ov = ioConnPerThreadOverride_.load(std::memory_order_relaxed);
    if (ov != 0)
        return ov;
    uint32_t def = 8;
    if (const char* s = compatibilityEnvironment("YAMS_IO_CONN_PER_THREAD")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= 1 && v <= 1024)
                return v;
        } catch (const std::exception&) {
            return def;
        }
    }
    return def;
}

void TuneAdvisor::setIoConnPerThread(uint32_t v) {
    ioConnPerThreadOverride_.store(v, std::memory_order_relaxed);
}

bool TuneAdvisor::enableParallelIngest() {
    int ov = enableParallelIngestOverride_.load(std::memory_order_relaxed);
    if (ov >= 0)
        return ov > 0;
    if (const char* s = compatibilityEnvironment("YAMS_ENABLE_PARALLEL_INGEST")) {
        std::string v{s};
        std::transform(v.begin(), v.end(), v.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (v == "0" || v == "false" || v == "off" || v == "no")
            return false;
        return true;
    }
    return true;
}

void TuneAdvisor::setEnableParallelIngest(bool en) {
    enableParallelIngestOverride_.store(en ? 1 : 0, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::maxIngestWorkers() {
    uint32_t ov = maxIngestWorkersOverride_.load(std::memory_order_relaxed);
    if (ov > 0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_INDEXING_WORKERS_MAX")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= 1)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return std::max(1u, recommendedThreads(1.0));
}

void TuneAdvisor::setMaxIngestWorkers(uint32_t v) {
    maxIngestWorkersOverride_.store(v, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::storagePoolSize() {
    uint32_t ov = storagePoolSizeOverride_.load(std::memory_order_relaxed);
    if (ov > 0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_STORAGE_POOL_SIZE")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= 1)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 0;
}

void TuneAdvisor::setStoragePoolSize(uint32_t v) {
    storagePoolSizeOverride_.store(v, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::ingestBacklogPerWorker() {
    uint32_t ov = ingestBacklogPerWorkerOverride_.load(std::memory_order_relaxed);
    if (ov > 0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_INGEST_BACKLOG_PER_WORKER")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= 1)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 32;
}

bool TuneAdvisor::useInternalBusForRepair() {
    return useInternalBusRepair_.load(std::memory_order_relaxed);
}

void TuneAdvisor::setUseInternalBusForRepair(bool en) {
    useInternalBusRepair_.store(en, std::memory_order_relaxed);
}

bool TuneAdvisor::useInternalBusForPostIngest() {
    return useInternalBusPostIngest_.load(std::memory_order_relaxed);
}

void TuneAdvisor::setUseInternalBusForPostIngest(bool en) {
    useInternalBusPostIngest_.store(en, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::ioThreadCount() {
    uint32_t ov = ioThreadCountOverride_.load(std::memory_order_relaxed);
    if (ov > 0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_IO_THREADS")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= 1 && v <= 16)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 10;
}

uint32_t TuneAdvisor::connectionLifetimeSeconds() {
    int32_t ov = connectionLifetimeSecondsOverride_.load(std::memory_order_relaxed);
    if (ov >= 0)
        return static_cast<uint32_t>(ov);
    if (const char* s = compatibilityEnvironment("YAMS_CONNECTION_LIFETIME_S")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v <= 86400)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 300;
}

void TuneAdvisor::setConnectionLifetimeSeconds(uint32_t v) {
    if (v <= 86400) {
        connectionLifetimeSecondsOverride_.store(static_cast<int32_t>(v),
                                                 std::memory_order_relaxed);
    }
}

void TuneAdvisor::resetConnectionLifetimeSecondsOverride() {
    connectionLifetimeSecondsOverride_.store(-1, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::maxIdleTimeouts() {
    uint32_t ov = maxIdleTimeoutsOverride_.load(std::memory_order_relaxed);
    if (ov > 0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_MAX_IDLE_TIMEOUTS")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= 1 && v <= 100)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 12;
}

uint32_t TuneAdvisor::checkpointIntervalSeconds() {
    uint32_t ov = checkpointIntervalSecondsOverride_.load(std::memory_order_relaxed);
    if (ov > 0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_CHECKPOINT_INTERVAL_SECONDS")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= 10 && v <= 3600)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 300;
}

uint32_t TuneAdvisor::checkpointInsertThreshold() {
    uint32_t ov = checkpointInsertThresholdOverride_.load(std::memory_order_relaxed);
    if (ov > 0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_CHECKPOINT_INSERT_THRESHOLD")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= 1 && v <= 100000)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 1000;
}

bool TuneAdvisor::enableHotzoneCheckpoint() {
    int ov = enableHotzoneCheckpointOverride_.load(std::memory_order_relaxed);
    if (ov >= 0)
        return ov > 0;
    if (const char* s = compatibilityEnvironment("YAMS_ENABLE_HOTZONE_PERSISTENCE")) {
        std::string v{s};
        std::transform(v.begin(), v.end(), v.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (v == "1" || v == "true" || v == "on" || v == "yes")
            return true;
        return false;
    }
    return false;
}

void TuneAdvisor::setPostIngestStageActive(PostIngestStage stage, bool active) {
    const uint32_t bit = 1u << static_cast<uint8_t>(stage);
    if (active) {
        postIngestStageActiveMaskOverride_.fetch_or(bit, std::memory_order_release);
    } else {
        postIngestStageActiveMaskOverride_.fetch_and(~bit, std::memory_order_release);
    }
}

TuneAdvisor::PostIngestStageActivityToken
TuneAdvisor::acquirePostIngestStageActivity(PostIngestStage stage) {
    std::lock_guard lock(postIngestStageActivityMutex());
    auto& nextToken = nextPostIngestStageActivityToken();
    auto& liveTokens = livePostIngestStageActivityTokens();
    do {
        ++nextToken;
    } while (nextToken == 0 || liveTokens.contains(nextToken));
    liveTokens.emplace(nextToken, static_cast<std::uint8_t>(stage));
    postIngestStageOwnerCounts_[static_cast<std::size_t>(stage)].fetch_add(
        1, std::memory_order_release);
    return nextToken;
}

void TuneAdvisor::releasePostIngestStageActivity(PostIngestStage stage,
                                                 PostIngestStageActivityToken token) {
    std::lock_guard lock(postIngestStageActivityMutex());
    auto& liveTokens = livePostIngestStageActivityTokens();
    const auto tokenIt = liveTokens.find(token);
    if (tokenIt == liveTokens.end() || tokenIt->second != static_cast<std::uint8_t>(stage)) {
        return;
    }
    liveTokens.erase(tokenIt);
    auto& owners = postIngestStageOwnerCounts_[static_cast<std::size_t>(stage)];
    const uint32_t current = owners.load(std::memory_order_relaxed);
    if (current > 0) {
        owners.store(current - 1, std::memory_order_release);
    }
}

uint32_t TuneAdvisor::postIngestStageActiveMask() {
    uint32_t mask = postIngestStageActiveMaskOverride_.load(std::memory_order_acquire);
    for (std::size_t i = 0; i < postIngestStageOwnerCounts_.size(); ++i) {
        if (postIngestStageOwnerCounts_[i].load(std::memory_order_acquire) > 0) {
            mask |= 1u << i;
        }
    }
    return mask;
}

uint32_t TuneAdvisor::postIngestTotalConcurrent() {
    uint32_t ov = postIngestTotalConcurrentOverride_.load(std::memory_order_relaxed);
    if (ov > 0)
        return ov;
    auto envValue = parseBoundedUintEnvNow("YAMS_POST_INGEST_TOTAL_CONCURRENT", 1u, 256u);
    if (envValue)
        return *envValue;
    uint32_t hw = daemonThreadCapacity(hardwareConcurrency());

    // Use round-up division to avoid integer truncation starving small systems.
    // Without rounding: (8*20)/100 = 1, base = max(2,1) = 2, no scaling benefit.
    // With rounding:    (8*20+99)/100 = 2, base = max(2,2) = 2, scaleRange grows too.
    uint32_t base = std::max(2u, (hw * 20 + 99) / 100);
    uint32_t scaleRange = std::max(1u, (hw * 15 + 99) / 100);
    uint32_t total = base + static_cast<uint32_t>(scaleRange * profileScale());

    // Ensure the budget can support at least 1 slot per active stage so no
    // pipeline stage is starved.  Count active bits from the stage mask.
    uint32_t mask = postIngestStageActiveMask();
    uint32_t activeStages = 0;
    for (uint32_t m = mask; m != 0; m >>= 1) {
        activeStages += (m & 1u);
    }

    // Clamp to hardware capacity, then re-apply a per-stage floor only
    // when the host has enough capacity. Small systems should not be
    // inflated to six post-ingest slots merely because all stages are
    // enabled.
    total = std::clamp(total, 2u, std::max(2u, hw));
    if (activeStages > 0) {
        total = std::max(total, std::min(activeStages, std::max(2u, hw)));
    }
    return total;
}

void TuneAdvisor::setPostIngestTotalConcurrent(uint32_t v) {
    if (v == 0) {
        postIngestTotalConcurrentOverride_.store(0u, std::memory_order_relaxed);
        return;
    }
    postIngestTotalConcurrentOverride_.store(std::clamp(v, 1u, 256u), std::memory_order_relaxed);
}

uint32_t TuneAdvisor::postExtractionDefaultConcurrent() {
    return postIngestBudgetedConcurrency(/*includeDynamicCaps=*/false).extraction;
}

uint32_t TuneAdvisor::postExtractionConcurrent() {
    return postIngestBudgetedConcurrency(/*includeDynamicCaps=*/true).extraction;
}

void TuneAdvisor::setPostExtractionConcurrent(uint32_t v) {
    postExtractionConcurrentOverride_.store(std::min(v, 64u), std::memory_order_relaxed);
}

void TuneAdvisor::setPostExtractionConcurrentDynamicCap(uint32_t v) {
    postExtractionConcurrentDynamicCap_.store(v == UINT32_MAX ? UINT32_MAX : std::min(v, 64u),
                                              std::memory_order_relaxed);
}

uint32_t TuneAdvisor::postKgDefaultConcurrent() {
    return postIngestBudgetedConcurrency(/*includeDynamicCaps=*/false).kg;
}

uint32_t TuneAdvisor::postKgConcurrent() {
    return postIngestBudgetedConcurrency(/*includeDynamicCaps=*/true).kg;
}

void TuneAdvisor::setPostKgConcurrent(uint32_t v) {
    postKgConcurrentOverride_.store(std::min(v, 64u), std::memory_order_relaxed);
}

void TuneAdvisor::setPostKgConcurrentDynamicCap(uint32_t v) {
    postKgConcurrentDynamicCap_.store(v == UINT32_MAX ? UINT32_MAX : std::min(v, 64u),
                                      std::memory_order_relaxed);
}

uint32_t TuneAdvisor::postSymbolDefaultConcurrent() {
    return postIngestBudgetedConcurrency(/*includeDynamicCaps=*/false).symbol;
}

uint32_t TuneAdvisor::postSymbolConcurrent() {
    return postIngestBudgetedConcurrency(/*includeDynamicCaps=*/true).symbol;
}

void TuneAdvisor::setPostSymbolConcurrent(uint32_t v) {
    postSymbolConcurrentOverride_.store(std::min(v, 32u), std::memory_order_relaxed);
}

void TuneAdvisor::setPostSymbolConcurrentDynamicCap(uint32_t v) {
    postSymbolConcurrentDynamicCap_.store(v == UINT32_MAX ? UINT32_MAX : std::min(v, 32u),
                                          std::memory_order_relaxed);
}

uint32_t TuneAdvisor::postEntityDefaultConcurrent() {
    return postIngestBudgetedConcurrency(/*includeDynamicCaps=*/false).entity;
}

uint32_t TuneAdvisor::postEntityConcurrent() {
    return postIngestBudgetedConcurrency(/*includeDynamicCaps=*/true).entity;
}

void TuneAdvisor::setPostEntityConcurrent(uint32_t v) {
    postEntityConcurrentOverride_.store(std::min(v, 16u), std::memory_order_relaxed);
}

void TuneAdvisor::setPostEntityConcurrentDynamicCap(uint32_t v) {
    postEntityConcurrentDynamicCap_.store(v == UINT32_MAX ? UINT32_MAX : std::min(v, 16u),
                                          std::memory_order_relaxed);
}

uint32_t TuneAdvisor::postTitleDefaultConcurrent() {
    return postIngestBudgetedConcurrency(/*includeDynamicCaps=*/false).title;
}

uint32_t TuneAdvisor::postTitleConcurrent() {
    return postIngestBudgetedConcurrency(/*includeDynamicCaps=*/true).title;
}

void TuneAdvisor::setPostTitleConcurrent(uint32_t v) {
    postTitleConcurrentOverride_.store(std::min(v, 16u), std::memory_order_relaxed);
}

void TuneAdvisor::setPostTitleConcurrentDynamicCap(uint32_t v) {
    postTitleConcurrentDynamicCap_.store(v == UINT32_MAX ? UINT32_MAX : std::min(v, 16u),
                                         std::memory_order_relaxed);
}

uint32_t TuneAdvisor::postEmbedDefaultConcurrent() {
    return postIngestBudgetedConcurrency(/*includeDynamicCaps=*/false).embed;
}

uint32_t TuneAdvisor::postEmbedConcurrent() {
    return postIngestBudgetedConcurrency(/*includeDynamicCaps=*/true).embed;
}

void TuneAdvisor::setPostEmbedConcurrent(uint32_t v) {
    postEmbedConcurrentOverride_.store(std::min(v, 32u), std::memory_order_relaxed);
}

void TuneAdvisor::setPostEmbedConcurrentDynamicCap(uint32_t v) {
    postEmbedConcurrentDynamicCap_.store(v == UINT32_MAX ? UINT32_MAX : std::min(v, 32u),
                                         std::memory_order_relaxed);
}

void TuneAdvisor::beginDynamicCapWrite() {
    // Publish an odd sequence before any following relaxed cap stores.
    dynamicCapSeq_.fetch_add(1, std::memory_order_acq_rel);
}

void TuneAdvisor::endDynamicCapWrite() {
    // Increment to even — signals "write complete"
    dynamicCapSeq_.fetch_add(1, std::memory_order_release);
}

std::array<uint32_t, 6> TuneAdvisor::readDynamicCapsConsistent() {
    std::array<uint32_t, 6> vals{};
    for (int attempt = 0; attempt < 64; ++attempt) {
        uint64_t seq1 = dynamicCapSeq_.load(std::memory_order_acquire);
        if (seq1 & 1u) {
            // Write in progress, spin briefly
            continue;
        }
        vals[0] = postExtractionConcurrentDynamicCap_.load(std::memory_order_relaxed);
        vals[1] = postKgConcurrentDynamicCap_.load(std::memory_order_relaxed);
        vals[2] = postSymbolConcurrentDynamicCap_.load(std::memory_order_relaxed);
        vals[3] = postEntityConcurrentDynamicCap_.load(std::memory_order_relaxed);
        vals[4] = postTitleConcurrentDynamicCap_.load(std::memory_order_relaxed);
        vals[5] = postEmbedConcurrentDynamicCap_.load(std::memory_order_relaxed);
        uint64_t seq2 = dynamicCapSeq_.load(std::memory_order_acquire);
        if (seq1 == seq2) {
            return vals; // Consistent read
        }
        // Sequence changed mid-read, retry
    }
    // Fallback after too many retries: return whatever we got (best-effort)
    return vals;
}

uint32_t TuneAdvisor::onnxSessionsPerModel(bool gpuEnabled) {
    uint32_t ov = onnxSessionsPerModelOverride_.load(std::memory_order_relaxed);
    if (ov > 0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_ONNX_SESSIONS_PER_MODEL")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= 1 && v <= 32)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    // GPU-aware default sizing
    uint32_t hw = hardwareConcurrency();
    if (gpuEnabled) {
        // GPU mode: GPU handles heavy lifting, more sessions useful for throughput
        return std::max<uint32_t>(2, std::min<uint32_t>(hw / 2, 8));
    } else {
        // CPU-only mode: conservative to prevent CPU saturation during inference
        // Each ONNX session uses multiple threads internally (intra-op parallelism)
        return std::max<uint32_t>(1, std::min<uint32_t>(hw / 4, 4));
    }
}

void TuneAdvisor::setOnnxSessionsPerModel(uint32_t v) {
    onnxSessionsPerModelOverride_.store(std::clamp(v, 1u, 32u), std::memory_order_relaxed);
}

uint32_t TuneAdvisor::embedChannelCapacity() {
    uint32_t ov = embedChannelCapacityOverride_.load(std::memory_order_relaxed);
    if (ov > 0)
        return ov;
    if (const char* val = compatibilityEnvironment("YAMS_EMBED_CHANNEL_CAPACITY")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(val));
            if (v >= 256 && v <= 65536)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 8192; // Increased from 2048 to handle bulk ingest
}

void TuneAdvisor::setEmbedChannelCapacity(uint32_t v) {
    embedChannelCapacityOverride_.store(std::clamp(v, 256u, 65536u), std::memory_order_relaxed);
}

uint32_t TuneAdvisor::storeDocumentChannelCapacity() {
    uint32_t ov = storeDocumentChannelCapacityOverride_.load(std::memory_order_relaxed);
    if (ov != 0)
        return ov;

    uint32_t base = 4096;
    if (const char* s = compatibilityEnvironment("YAMS_STORE_DOCUMENT_CHANNEL_CAPACITY")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= 64 && v <= 1'000'000)
                base = v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }

    bool correctnessMode = true;
    if (const char* s = compatibilityEnvironment("YAMS_INGEST_CORRECTNESS_MODE")) {
        std::string v(s);
        std::transform(v.begin(), v.end(), v.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        correctnessMode = !(v == "0" || v == "false" || v == "no" || v == "off");
    }

    uint32_t cap = postIngestQueueMax();
    if (cap == 0)
        cap = base;

    if (correctnessMode) {
        // Favor correctness under bursty producers by allowing a deeper ingest queue.
        // Still bounded to avoid unbounded memory growth.
        uint32_t target = std::max<uint32_t>(base, 4096u);
        return std::clamp(target, 64u, 65536u);
    }

    uint32_t bounded = std::min<uint32_t>(base, cap);
    return std::max<uint32_t>(64u, bounded);
}

void TuneAdvisor::setStoreDocumentChannelCapacity(uint32_t v) {
    storeDocumentChannelCapacityOverride_.store(std::clamp(v, 64u, 1'000'000u),
                                                std::memory_order_relaxed);
}

uint32_t TuneAdvisor::dbLockErrorThreshold() {
    if (const char* s = compatibilityEnvironment("YAMS_DB_LOCK_THRESHOLD")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= 1 && v <= 100)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 5;
}

void TuneAdvisor::reportDbLockError() {
    metadata::reportDbLockError();
}

uint64_t TuneAdvisor::getAndResetDbLockErrors() {
    return metadata::getAndResetDbLockErrors();
}

TuneAdvisor::PostIngestBudget TuneAdvisor::postIngestBudgetAll(bool includeDynamicCaps) {
    return readConfiguredOverridesSnapshot([includeDynamicCaps] {
        auto b = postIngestBudgetedConcurrency(includeDynamicCaps);
        return PostIngestBudget{b.extraction, b.kg, b.symbol, b.entity, b.title, b.embed};
    });
}

TuneAdvisor::PostIngestConcurrencyBudget
TuneAdvisor::postIngestBudgetedConcurrency(bool includeDynamicCaps) {
    constexpr std::size_t kStageCount = 6;
    constexpr std::size_t kExtractionIdx = 0;
    constexpr std::size_t kEmbedIdx = 5;
    constexpr std::array<uint32_t, kStageCount> kWeights{1u, 1u, 1u, 1u, 1u, 2u};
    constexpr std::array<uint32_t, kStageCount> kMaxCaps{64u, 64u, 32u, 16u, 16u, 32u};
    const uint32_t totalBudget = std::max<uint32_t>(1, postIngestTotalConcurrent());
    const uint32_t activeMask = postIngestStageActiveMask();

    auto resolveOverride = [](std::atomic<uint32_t>& overrideSlot, const char* env,
                              uint32_t maxCap) -> std::optional<uint32_t> {
        uint32_t ov = overrideSlot.load(std::memory_order_relaxed);
        if (ov > 0)
            return std::min(ov, maxCap);
        return postStageConcurrentEnvOverride(env, maxCap);
    };

    auto allocate = [&](const std::array<uint32_t, kStageCount>& desired,
                        const std::array<uint32_t, kStageCount>& caps,
                        const std::array<uint32_t, kStageCount>& weights) {
        std::array<uint32_t, kStageCount> alloc{};
        std::array<bool, kStageCount> locked{};
        uint32_t used = 0;
        uint32_t activeStages = 0;

        for (std::size_t i = 0; i < kStageCount; ++i) {
            if (caps[i] > 0)
                activeStages += 1;
        }

        if (activeStages > 0 && totalBudget >= activeStages) {
            for (std::size_t i = 0; i < kStageCount; ++i) {
                if (caps[i] > 0) {
                    alloc[i] = 1;
                    used += 1;
                }
            }
        } else if (totalBudget >= 2) {
            if (caps[kExtractionIdx] > 0) {
                alloc[kExtractionIdx] = 1;
                used += 1;
            }
            if (caps[kEmbedIdx] > 0) {
                alloc[kEmbedIdx] = 1;
                used += 1;
            }
        } else if (totalBudget == 1 && caps[kExtractionIdx] > 0) {
            alloc[kExtractionIdx] = 1;
            used += 1;
        }

        for (std::size_t i = 0; i < kStageCount; ++i) {
            uint32_t target = std::min(desired[i], caps[i]);
            if (target > alloc[i]) {
                uint32_t gap = target - alloc[i];
                uint32_t room = caps[i] - alloc[i];
                uint32_t add = std::min(gap, room);
                alloc[i] += add;
                used += add;
            }
            locked[i] = desired[i] == caps[i];
        }

        if (used > totalBudget) {
            double ratio =
                static_cast<double>(totalBudget) / static_cast<double>(std::max<uint32_t>(1, used));
            used = 0;
            for (std::size_t i = 0; i < kStageCount; ++i) {
                if (!locked[i]) {
                    alloc[i] =
                        std::min(caps[i], static_cast<uint32_t>(std::floor(alloc[i] * ratio)));
                }
                used += alloc[i];
            }
        }

        if (totalBudget >= 2) {
            if (alloc[kExtractionIdx] == 0 && caps[kExtractionIdx] > 0) {
                alloc[kExtractionIdx] = 1;
            }
            if (alloc[kEmbedIdx] == 0 && caps[kEmbedIdx] > 0) {
                alloc[kEmbedIdx] = 1;
            }
        } else if (totalBudget == 1 && caps[kExtractionIdx] > 0) {
            alloc[kExtractionIdx] = 1;
            alloc[kEmbedIdx] = 0;
        }

        used = 0;
        for (auto value : alloc) {
            used += value;
        }

        if (used > totalBudget) {
            std::array<std::size_t, kStageCount> reduceOrder{};
            for (std::size_t i = 0; i < kStageCount; ++i) {
                reduceOrder[i] = i;
            }
            std::sort(reduceOrder.begin(), reduceOrder.end(), [&](std::size_t a, std::size_t b) {
                if (weights[a] != weights[b])
                    return weights[a] < weights[b];
                return a < b;
            });
            while (used > totalBudget) {
                bool progressed = false;
                for (auto idx : reduceOrder) {
                    if (alloc[idx] == 0)
                        continue;
                    if (locked[idx])
                        continue;
                    if (idx == kExtractionIdx || idx == kEmbedIdx) {
                        if (totalBudget >= 2 && alloc[idx] <= 1)
                            continue;
                    }
                    alloc[idx] -= 1;
                    used -= 1;
                    progressed = true;
                    if (used <= totalBudget)
                        break;
                }
                if (!progressed)
                    break;
            }
        }

        if (used < totalBudget) {
            uint32_t remaining = totalBudget - used;

            // Weighted sort: distribute remaining budget by weight (higher weight first).
            std::array<std::size_t, kStageCount> order{};
            for (std::size_t i = 0; i < kStageCount; ++i) {
                order[i] = i;
            }
            std::sort(order.begin(), order.end(), [&](std::size_t a, std::size_t b) {
                if (weights[a] != weights[b])
                    return weights[a] > weights[b];
                return a < b;
            });
            while (remaining > 0) {
                bool progressed = false;
                for (auto idx : order) {
                    if (alloc[idx] >= caps[idx])
                        continue;
                    alloc[idx] += 1;
                    remaining -= 1;
                    progressed = true;
                    if (remaining == 0)
                        break;
                }
                if (!progressed)
                    break;
            }

            // Fairness correction: if any stage with cap > 0 still has 0 allocation,
            // steal 1 slot from the lowest-weight stage that has alloc > 1 AND
            // whose weight does not exceed the needy stage's weight.
            // This prevents starvation without penalizing higher-weight stages.
            constexpr std::array<std::size_t, 4> kZeroFillOrder{
                4, // Title
                1, // KnowledgeGraph
                2, // Symbol
                3  // Entity
            };
            for (auto needIdx : kZeroFillOrder) {
                if (caps[needIdx] == 0 || alloc[needIdx] != 0)
                    continue;
                // Find lowest-weight donor with alloc > 1 and weight <= needy stage
                std::size_t donor = kStageCount;
                for (auto it = order.rbegin(); it != order.rend(); ++it) {
                    if (*it != needIdx && alloc[*it] > 1 && weights[*it] <= weights[needIdx]) {
                        donor = *it;
                        break;
                    }
                }
                if (donor < kStageCount) {
                    alloc[donor] -= 1;
                    alloc[needIdx] = 1;
                }
            }
        }

        return alloc;
    };

    std::array<uint32_t, kStageCount> caps = kMaxCaps;
    std::array<uint32_t, kStageCount> weights = kWeights;
    for (std::size_t i = 0; i < kStageCount; ++i) {
        if ((activeMask & (1u << i)) == 0u) {
            caps[i] = 0;
            weights[i] = 0;
        }
    }

    uint32_t weightSum = 0;
    for (auto w : weights) {
        weightSum += w;
    }
    if (weightSum == 0) {
        weights = kWeights;
        weightSum = 0;
        for (auto w : weights) {
            weightSum += w;
        }
        caps = kMaxCaps;
    }

    std::array<uint32_t, kStageCount> defaults{};
    for (std::size_t i = 0; i < kStageCount; ++i) {
        defaults[i] = static_cast<uint32_t>(
            std::floor(static_cast<double>(totalBudget) * weights[i] / weightSum));
        defaults[i] = std::min(defaults[i], caps[i]);
    }
    auto allocDefaults = allocate(defaults, caps, weights);

    std::array<uint32_t, kStageCount> desired = allocDefaults;
    bool hasOverride = false;
    auto clampLocked = [&](std::size_t idx, uint32_t value) {
        if (caps[idx] == 0) {
            desired[idx] = 0;
            return;
        }
        desired[idx] = value;
        caps[idx] = value;
        hasOverride = true;
    };

    if (auto v = resolveOverride(postExtractionConcurrentOverride_,
                                 "YAMS_POST_EXTRACTION_CONCURRENT", kMaxCaps[0])) {
        clampLocked(0, *v);
    }
    if (auto v =
            resolveOverride(postKgConcurrentOverride_, "YAMS_POST_KG_CONCURRENT", kMaxCaps[1])) {
        clampLocked(1, *v);
    }
    if (auto v = resolveOverride(postSymbolConcurrentOverride_, "YAMS_POST_SYMBOL_CONCURRENT",
                                 kMaxCaps[2])) {
        clampLocked(2, *v);
    }
    if (auto v = resolveOverride(postEntityConcurrentOverride_, "YAMS_POST_ENTITY_CONCURRENT",
                                 kMaxCaps[3])) {
        clampLocked(3, *v);
    }
    if (auto v = resolveOverride(postTitleConcurrentOverride_, "YAMS_POST_TITLE_CONCURRENT",
                                 kMaxCaps[4])) {
        clampLocked(4, *v);
    }
    if (auto v = resolveOverride(postEmbedConcurrentOverride_, "YAMS_POST_EMBED_CONCURRENT",
                                 kMaxCaps[5])) {
        clampLocked(5, *v);
    }

    bool hasDynamicCap = false;
    if (includeDynamicCaps) {
        const auto dyn = readDynamicCapsConsistent();
        for (std::size_t i = 0; i < kStageCount; ++i) {
            if (dyn[i] != UINT32_MAX) {
                caps[i] = std::min(caps[i], std::min(dyn[i], kMaxCaps[i]));
                hasDynamicCap = true;
            }
        }
    }

    auto alloc = (hasOverride || hasDynamicCap) ? allocate(desired, caps, weights) : allocDefaults;
    return PostIngestConcurrencyBudget{alloc[0], alloc[1], alloc[2], alloc[3], alloc[4], alloc[5]};
}

bool TuneAdvisor::enableResourceGovernor() {
    int ov = enableResourceGovernorOverride_.load(std::memory_order_relaxed);
    if (ov >= 0)
        return ov > 0;
    auto envValue = parseExplicitBoolEnvNow("YAMS_ENABLE_RESOURCE_GOVERNOR");
    if (envValue)
        return *envValue;
    return true;
}

void TuneAdvisor::setEnableResourceGovernor(bool en) {
    enableResourceGovernorOverride_.store(en ? 1 : 0, std::memory_order_relaxed);
}

bool TuneAdvisor::enableProactiveEviction() {
    return parseExplicitBoolEnvNow("YAMS_PROACTIVE_EVICTION").value_or(true);
}

bool TuneAdvisor::enableAdmissionControl() {
    int ov = enableAdmissionControlOverride_.load(std::memory_order_relaxed);
    if (ov >= 0)
        return ov > 0;
    auto envValue = parseExplicitBoolEnvNow("YAMS_ADMISSION_CONTROL");
    if (envValue)
        return *envValue;
    return true;
}

void TuneAdvisor::setEnableAdmissionControl(bool en) {
    enableAdmissionControlOverride_.store(en ? 1 : 0, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::governorWarningScalePercent() {
    uint32_t ov = governorWarningScalePctOverride_.load(std::memory_order_relaxed);
    if (ov >= 10 && ov <= 100)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_GOV_WARNING_SCALE_PCT")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= 10 && v <= 100)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 85;
}

void TuneAdvisor::setGovernorWarningScalePercent(uint32_t pct) {
    if (pct == 0) {
        governorWarningScalePctOverride_.store(0, std::memory_order_relaxed);
        return;
    }
    governorWarningScalePctOverride_.store(std::clamp<uint32_t>(pct, 10u, 100u),
                                           std::memory_order_relaxed);
}

void TuneAdvisor::resetGovernorWarningScalePercentOverride() {
    governorWarningScalePctOverride_.store(0, std::memory_order_relaxed);
}

uint64_t TuneAdvisor::memoryBudgetBytes() {
    uint64_t ov = memoryBudgetBytesOverride_.load(std::memory_order_relaxed);
    if (ov > 0)
        return ov;
    auto envValue =
        parseBoundedUint64EnvNow("YAMS_MEMORY_BUDGET_BYTES", 64ull * 1024ull * 1024ull, ULLONG_MAX);
    if (envValue)
        return *envValue;
    return autoMemoryBudgetBytes(detectSystemMemory());
}

void TuneAdvisor::setMemoryBudgetBytes(uint64_t bytes) {
    memoryBudgetBytesOverride_.store(bytes, std::memory_order_relaxed);
}

double TuneAdvisor::memoryWarningThreshold() {
    double ov = memoryWarningPctOverride_.load(std::memory_order_relaxed);
    if (ov > 0.0)
        return ov;
    auto envValue = parseBoundedDoubleEnvNow("YAMS_MEMORY_WARNING_PCT", 0.5, 0.99, 0.01);
    if (envValue)
        return *envValue;
    return 0.70 + profileScale() * 0.10;
}

void TuneAdvisor::setMemoryWarningThreshold(double pct) {
    memoryWarningPctOverride_.store(pct, std::memory_order_relaxed);
}

double TuneAdvisor::memoryCriticalThreshold() {
    double ov = memoryCriticalPctOverride_.load(std::memory_order_relaxed);
    if (ov > 0.0)
        return ov;
    auto envValue = parseBoundedDoubleEnvNow("YAMS_MEMORY_CRITICAL_PCT", 0.5, 0.99, 0.01);
    if (envValue)
        return *envValue;
    return 0.85 + profileScale() * 0.07;
}

void TuneAdvisor::setMemoryCriticalThreshold(double pct) {
    memoryCriticalPctOverride_.store(pct, std::memory_order_relaxed);
}

double TuneAdvisor::memoryEmergencyThreshold() {
    double ov = memoryEmergencyPctOverride_.load(std::memory_order_relaxed);
    if (ov > 0.0)
        return ov;
    auto envValue = parseBoundedDoubleEnvNow("YAMS_MEMORY_EMERGENCY_PCT", 0.5, 0.99, 0.01);
    if (envValue)
        return *envValue;
    return 0.92 + profileScale() * 0.05;
}

void TuneAdvisor::setMemoryEmergencyThreshold(double pct) {
    memoryEmergencyPctOverride_.store(pct, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::memoryHysteresisMs() {
    uint32_t ov = memoryHysteresisMsOverride_.load(std::memory_order_relaxed);
    if (ov > 0)
        return ov;
    auto envValue = parseBoundedUintEnvNow("YAMS_MEMORY_HYSTERESIS_MS", 10u, 10000u);
    if (envValue)
        return *envValue;
    return 500; // 500ms default
}

void TuneAdvisor::setMemoryHysteresisMs(uint32_t ms) {
    memoryHysteresisMsOverride_.store(ms, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::cpuLevelHysteresisMs() {
    uint32_t ov = cpuLevelHysteresisMsOverride_.load(std::memory_order_relaxed);
    if (ov > 0)
        return ov;
    auto envValue = parseBoundedUintEnvNow("YAMS_CPU_LEVEL_HYSTERESIS_MS", 10u, 10000u);
    if (envValue)
        return *envValue;
    return 150;
}

void TuneAdvisor::setCpuLevelHysteresisMs(uint32_t ms) {
    cpuLevelHysteresisMsOverride_.store(ms, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::modelEvictionCooldownMs() {
    uint32_t ov = modelEvictionCooldownMsOverride_.load(std::memory_order_relaxed);
    if (ov > 0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_MODEL_EVICTION_COOLDOWN_MS")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= 100 && v <= 10000)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 500;
}

bool TuneAdvisor::enableSemanticNeighborBackfill() {
    return parseExplicitBoolEnvNow("YAMS_ENABLE_SEMANTIC_NEIGHBOR_BACKFILL").value_or(true);
}

bool TuneAdvisor::enableGradientLimiters() {
    int ov = enableGradientLimitersOverride_.load(std::memory_order_relaxed);
    if (ov >= 0)
        return ov > 0;
    return parseExplicitBoolEnvNow("YAMS_ENABLE_GRADIENT_LIMITERS").value_or(true);
}

void TuneAdvisor::setEnableGradientLimiters(bool en) {
    enableGradientLimitersOverride_.store(en ? 1 : 0, std::memory_order_relaxed);
}

double TuneAdvisor::gradientSmoothingAlpha() {
    double ov = gradientSmoothingAlphaOverride_.load(std::memory_order_relaxed);
    if (ov > 0.0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_GRADIENT_SMOOTHING_ALPHA")) {
        try {
            double v = std::stod(s);
            if (v >= 0.01 && v <= 0.99)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 0.2;
}

void TuneAdvisor::setGradientSmoothingAlpha(double alpha) {
    gradientSmoothingAlphaOverride_.store(alpha, std::memory_order_relaxed);
}

double TuneAdvisor::gradientLongAlpha() {
    double ov = gradientLongAlphaOverride_.load(std::memory_order_relaxed);
    if (ov > 0.0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_GRADIENT_LONG_ALPHA")) {
        try {
            double v = std::stod(s);
            if (v >= 0.01 && v <= 0.5)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 0.05;
}

void TuneAdvisor::setGradientLongAlpha(double alpha) {
    gradientLongAlphaOverride_.store(alpha, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::gradientWarmupSamples() {
    uint32_t ov = gradientWarmupSamplesOverride_.load(std::memory_order_relaxed);
    if (ov > 0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_GRADIENT_WARMUP_SAMPLES")) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(s));
            if (v >= 1 && v <= 100)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 10;
}

void TuneAdvisor::setGradientWarmupSamples(uint32_t samples) {
    gradientWarmupSamplesOverride_.store(samples, std::memory_order_relaxed);
}

double TuneAdvisor::gradientTolerance() {
    double ov = gradientToleranceOverride_.load(std::memory_order_relaxed);
    if (ov > 0.0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_GRADIENT_TOLERANCE")) {
        try {
            double v = std::stod(s);
            if (v >= 1.0 && v <= 5.0)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 1.5;
}

void TuneAdvisor::setGradientTolerance(double tolerance) {
    gradientToleranceOverride_.store(tolerance, std::memory_order_relaxed);
}

double TuneAdvisor::gradientInitialLimit() {
    double ov = gradientInitialLimitOverride_.load(std::memory_order_relaxed);
    if (ov > 0.0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_GRADIENT_INITIAL_LIMIT")) {
        try {
            double v = std::stod(s);
            if (v >= 1.0 && v <= 128.0)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 4.0;
}

void TuneAdvisor::setGradientInitialLimit(double limit) {
    gradientInitialLimitOverride_.store(limit, std::memory_order_relaxed);
}

double TuneAdvisor::gradientMinLimit() {
    double ov = gradientMinLimitOverride_.load(std::memory_order_relaxed);
    if (ov > 0.0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_GRADIENT_MIN_LIMIT")) {
        try {
            double v = std::stod(s);
            if (v >= 0.0 && v <= 64.0)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 1.0;
}

void TuneAdvisor::setGradientMinLimit(double limit) {
    gradientMinLimitOverride_.store(limit, std::memory_order_relaxed);
}

double TuneAdvisor::gradientMaxLimit() {
    double ov = gradientMaxLimitOverride_.load(std::memory_order_relaxed);
    if (ov > 0.0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_GRADIENT_MAX_LIMIT")) {
        try {
            double v = std::stod(s);
            if (v >= 1.0 && v <= 256.0)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    return 32.0;
}

void TuneAdvisor::setGradientMaxLimit(double limit) {
    gradientMaxLimitOverride_.store(limit, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::onnxMaxConcurrent() {
    uint32_t ov = onnxMaxConcurrentOverride_.load(std::memory_order_relaxed);
    if (ov > 0)
        return ov;
    auto envValue = parseBoundedUintEnvNow("YAMS_ONNX_MAX_CONCURRENT", 1u, 64u);
    if (envValue)
        return static_cast<uint32_t>(*envValue * profileScale());
    uint32_t hw = daemonThreadCapacity(hardwareConcurrency());
    uint32_t reserved = onnxGlinerReserved() + onnxEmbedReserved() + onnxRerankerReserved();

    // Use round-up division to avoid integer truncation on small systems.
    uint32_t base = std::max(2u, (hw * 10 + 99) / 100);
    uint32_t scaleRange = std::max(1u, (hw * 15 + 99) / 100);
    uint32_t total = base + static_cast<uint32_t>(scaleRange * profileScale());
    // Ensure at least 1 shared slot beyond total reserved.
    total = std::max(total, reserved + 1);

    total = std::max(total, reserved + 1u);
    return std::clamp(total, 2u, 12u);
}

void TuneAdvisor::setOnnxMaxConcurrent(uint32_t n) {
    onnxMaxConcurrentOverride_.store(n, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::onnxGlinerReserved() {
    uint32_t ov = onnxGlinerReservedOverride_.load(std::memory_order_relaxed);
    if (ov <= 8)
        return ov;
    auto envValue = parseBoundedUintEnvNow("YAMS_ONNX_GLINER_RESERVED", 0u, 8u);
    if (envValue)
        return *envValue;
    return 1;
}

void TuneAdvisor::setOnnxGlinerReserved(uint32_t n) {
    onnxGlinerReservedOverride_.store(n, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::onnxEmbedReserved() {
    uint32_t ov = onnxEmbedReservedOverride_.load(std::memory_order_relaxed);
    if (ov <= 8)
        return ov;
    auto envValue = parseBoundedUintEnvNow("YAMS_ONNX_EMBED_RESERVED", 0u, 8u);
    if (envValue)
        return *envValue;
    return 1;
}

void TuneAdvisor::setOnnxEmbedReserved(uint32_t n) {
    onnxEmbedReservedOverride_.store(n, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::onnxRerankerReserved() {
    uint32_t ov = onnxRerankerReservedOverride_.load(std::memory_order_relaxed);
    if (ov <= 8)
        return ov;
    auto envValue = parseBoundedUintEnvNow("YAMS_ONNX_RERANKER_RESERVED", 0u, 8u);
    if (envValue)
        return *envValue;
    // Efficient profile with tight budget: reranker gets no reserved slot
    if (profileScale() == 0.0)
        return 0;
    return 1;
}

void TuneAdvisor::setOnnxRerankerReserved(uint32_t n) {
    onnxRerankerReservedOverride_.store(n, std::memory_order_relaxed);
}

uint32_t TuneAdvisor::modelMaintenanceConnThreshold() {
    if (auto value = parseBoundedUintEnvNow("YAMS_MODEL_MAINT_CONN_THRESHOLD", 0, 100))
        return *value;
    switch (tuningProfile()) {
        case Profile::Efficient:
            return 2;
        case Profile::Aggressive:
            return 0;
        default: // Balanced
            return 1;
    }
}

uint32_t TuneAdvisor::modelMaintenanceSearchThreshold() {
    if (auto value = parseBoundedUintEnvNow("YAMS_MODEL_MAINT_SEARCH_THRESHOLD", 0, 100))
        return *value;
    switch (tuningProfile()) {
        case Profile::Efficient:
            return 2;
        case Profile::Aggressive:
            return 0;
        default: // Balanced
            return 1;
    }
}

uint32_t TuneAdvisor::modelMaintenanceQueueThreshold() {
    if (auto value = parseBoundedUintEnvNow("YAMS_MODEL_MAINT_QUEUE_THRESHOLD", 0, 10000))
        return *value;
    switch (tuningProfile()) {
        case Profile::Efficient:
            return 20;
        case Profile::Aggressive:
            return 0;
        default: // Balanced
            return 10;
    }
}

double TuneAdvisor::modelEvictWarningThreshold() {
    double ov = modelEvictWarningOverride_.load(std::memory_order_relaxed);
    if (ov > 0.0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_MODEL_EVICT_WARNING_THRESHOLD")) {
        try {
            double v = std::stod(s);
            if (v > 0.0 && v < 1.0)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    switch (tuningProfile()) {
        case Profile::Efficient:
            return 0.30;
        case Profile::Aggressive:
            return 0.75;
        default: // Balanced
            return 0.60;
    }
}

void TuneAdvisor::setModelEvictWarningThreshold(double v) {
    if (v > 0.0 && v < 1.0 && std::isfinite(v))
        modelEvictWarningOverride_.store(v, std::memory_order_relaxed);
}

double TuneAdvisor::modelEvictCriticalThreshold() {
    double ov = modelEvictCriticalOverride_.load(std::memory_order_relaxed);
    if (ov > 0.0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_MODEL_EVICT_CRITICAL_THRESHOLD")) {
        try {
            double v = std::stod(s);
            if (v > 0.0 && v < 1.0)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    switch (tuningProfile()) {
        case Profile::Efficient:
            return 0.50;
        case Profile::Aggressive:
            return 0.85;
        default: // Balanced
            return 0.75;
    }
}

void TuneAdvisor::setModelEvictCriticalThreshold(double v) {
    if (v > 0.0 && v < 1.0 && std::isfinite(v))
        modelEvictCriticalOverride_.store(v, std::memory_order_relaxed);
}

double TuneAdvisor::modelEvictEmergencyThreshold() {
    double ov = modelEvictEmergencyOverride_.load(std::memory_order_relaxed);
    if (ov > 0.0)
        return ov;
    if (const char* s = compatibilityEnvironment("YAMS_MODEL_EVICT_EMERGENCY_THRESHOLD")) {
        try {
            double v = std::stod(s);
            if (v > 0.0 && v < 1.0)
                return v;
        } catch (const std::exception&) {
            ignoreInvalidEnvParseFailure();
        }
    }
    switch (tuningProfile()) {
        case Profile::Efficient:
            return 0.70;
        case Profile::Aggressive:
            return 0.95;
        default: // Balanced
            return 0.90;
    }
}

void TuneAdvisor::setModelEvictEmergencyThreshold(double v) {
    if (v > 0.0 && v < 1.0 && std::isfinite(v))
        modelEvictEmergencyOverride_.store(v, std::memory_order_relaxed);
}

void TuneAdvisor::resetModelEvictThresholdOverrides() {
    modelEvictWarningOverride_.store(0.0, std::memory_order_relaxed);
    modelEvictCriticalOverride_.store(0.0, std::memory_order_relaxed);
    modelEvictEmergencyOverride_.store(0.0, std::memory_order_relaxed);
}

double TuneAdvisor::workCoordinatorIoBias() {
    switch (tuningProfile()) {
        case Profile::Efficient:
            return 1.0;
        case Profile::Aggressive:
            return 1.75;
        case Profile::Balanced:
        default:
            return 1.5;
    }
}

uint32_t TuneAdvisor::recommendedThreadsForHw(unsigned hw, double backgroundFactor,
                                              uint32_t hardMax) {
    double budget = static_cast<double>(cpuBudgetPercent()) / 100.0;
    if (backgroundFactor <= 0.0)
        backgroundFactor = 0.5;
    double eff = std::clamp(budget * backgroundFactor, 0.1, 1.0);
    uint32_t budgetCap =
        static_cast<uint32_t>(std::max(1.0, std::floor(eff * static_cast<double>(hw))));
    uint32_t cap = std::min(budgetCap, daemonThreadCapacity(hw));
    uint32_t absMax = maxThreadsOverall();
    if (absMax > 0)
        cap = std::min(cap, absMax);
    if (hardMax > 0)
        cap = std::min(cap, hardMax);
    return std::max(1u, cap);
}

TuneAdvisor::ReadPathCapacityModel TuneAdvisor::defaultReadPathCapacityModel(unsigned hw) {
    ReadPathCapacityModel model;
    model.workerThreads = std::max(4u, recommendedThreadsForHw(hw, workCoordinatorIoBias()));

    auto derived = std::max<uint32_t>(2u, recommendedThreadsForHw(hw, 0.5)) * 2u;
    switch (tuningProfile()) {
        case Profile::Efficient:
            model.searchConcurrencyLimit = derived;
            break;
        case Profile::Aggressive:
            model.searchConcurrencyLimit = std::max<uint32_t>(6u, derived);
            break;
        case Profile::Balanced:
        default:
            model.searchConcurrencyLimit = std::max<uint32_t>(5u, derived);
            break;
    }
    return model;
}

uint32_t TuneAdvisor::defaultReadPoolMaxConnectionsForHw(unsigned hw, uint32_t configuredMax) {
    const auto model = defaultReadPathCapacityModel(hw);
    uint32_t derived = std::max<uint32_t>(
        {4u, model.workerThreads, model.searchConcurrencyLimit, listInflightLimit()});
    const uint32_t capped = std::clamp<uint32_t>(derived, 4u, 8u);
    return std::min(capped, std::max<uint32_t>(1u, configuredMax));
}

} // namespace yams::daemon
