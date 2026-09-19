#pragma once

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#endif

#ifdef min
#undef min
#endif
#ifdef max
#undef max
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <climits>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <yams/config/config_helpers.h>
#include <yams/daemon/components/RepairTuning.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/metadata/db_lock_telemetry.h>

// Platform-specific includes for memory detection (used by detectSystemMemory)
// Note: These are only used in the implementation of detectSystemMemory()
// Windows: GlobalMemoryStatusEx requires windows.h
// macOS: sysctl requires sys/sysctl.h
// Linux: reads /proc/meminfo (no special headers)

namespace yams::daemon {

// Centralized, lightweight tuning accessors. Reads env vars with sane defaults
// and basic range clamps. Header-only to avoid init-order issues.
class TuneAdvisor {
public:
    enum class Profile { Efficient, Balanced, Aggressive };

    // Resolve tuning profile (override -> env -> default Balanced).
    static Profile tuningProfile();

    static void setTuningProfile(Profile p);

    // Scale factor applied to several heuristics
    // Efficient  -> 0.0 (minimal resource use)
    // Balanced   -> 0.5 (moderate resource use)
    // Aggressive -> 1.0 (maximum throughput)
    static double profileScale();

    // Public accessors for embedding-related knobs (used outside daemon module)
    // These forward to internal tunables while keeping implementation details private.
    static constexpr std::size_t kDefaultEmbedDocCap = 64;
    static constexpr std::size_t kDefaultEmbedJobDocCap = 64;
    static double getEmbedSafety();
    static std::size_t getEmbedDocCap();
    static std::size_t resolvedEmbedDocCap();
    static std::size_t getEmbedJobDocCap();
    static std::size_t resolvedEmbedJobDocCap();
    static unsigned getEmbedPauseMs();
    static uint32_t getEmbedMaxConcurrency();
    static inline std::atomic<int> tuningProfileOverride_{0};

    // Refresh every YAMS_* compatibility input as one lifecycle snapshot. Production getters use
    // this immutable map instead of capturing individual keys on first use, so an ambient mutation
    // cannot alter a not-yet-read knob midway through daemon startup.
    static void refreshCompatibilityEnvironmentSnapshot() {
#ifndef YAMS_TESTING
        auto current = yams::config::snapshot_environment_prefix("YAMS_");
        std::lock_guard lock(compatibilityEnvironmentSnapshotMutex());
        compatibilityEnvironmentSnapshot() = std::move(current);
        compatibilityEnvironmentSnapshotInitialized() = true;
#endif
    }

    /// Report whether the immutable lifecycle compatibility snapshot contains a key. This lets
    /// status provenance describe the snapshotted source without re-reading ambient state.
    static bool hasCompatibilityEnvironmentValue(const char* name);

    // Serializes process-wide value publication with the matching instance config/status update.
    // The mutex is recursive so ConfigResolver's aggregate update can nest inside a daemon reload
    // publication transaction on the same thread.
    class ConfiguredOverridePublication {
    public:
        ConfiguredOverridePublication()
            : lock_(TuneAdvisor::configuredOverridePublicationMutex()) {}
        ConfiguredOverridePublication(const ConfiguredOverridePublication&) = delete;
        ConfiguredOverridePublication& operator=(const ConfiguredOverridePublication&) = delete;

    private:
        std::unique_lock<std::recursive_mutex> lock_;
    };

    static ConfiguredOverridePublication beginConfiguredOverridePublication();

    /// Hold lifecycle membership stable while deciding and publishing a process-global reload.
    /// Without the retained lock, a new embedded manager could join after validation and receive a
    /// snapshot that becomes stale before construction returns.
    class ConfiguredOverrideReloadGuard {
    public:
        ConfiguredOverrideReloadGuard()
            : lock_(TuneAdvisor::configuredOverrideLifecycleMutex()),
              allowed_(TuneAdvisor::configuredOverrideLifecycleCount() <= 1) {}
        ConfiguredOverrideReloadGuard(const ConfiguredOverrideReloadGuard&) = delete;
        ConfiguredOverrideReloadGuard& operator=(const ConfiguredOverrideReloadGuard&) = delete;
        explicit operator bool() const noexcept { return allowed_; }

    private:
        std::unique_lock<std::mutex> lock_;
        bool allowed_{false};
    };

    static ConfiguredOverrideReloadGuard beginConfiguredOverrideReload();

    // Serializes one ConfigResolver update and publishes an even sequence when all related atomics
    // are installed. Readers that need a coherent multi-field policy use
    // readConfiguredOverridesSnapshot() and retry if a reload overlaps their read.
    class ConfiguredOverrideUpdate {
    public:
        ConfiguredOverrideUpdate()
            : publicationLock_(TuneAdvisor::configuredOverridePublicationMutex()),
              writeLock_(TuneAdvisor::configuredOverrideWriteMutex()) {
            TuneAdvisor::configuredOverrideSequence_.fetch_add(1, std::memory_order_acq_rel);
        }
        ~ConfiguredOverrideUpdate() {
            TuneAdvisor::configuredOverrideSequence_.fetch_add(1, std::memory_order_release);
        }

        ConfiguredOverrideUpdate(const ConfiguredOverrideUpdate&) = delete;
        ConfiguredOverrideUpdate& operator=(const ConfiguredOverrideUpdate&) = delete;

    private:
        std::unique_lock<std::recursive_mutex> publicationLock_;
        std::unique_lock<std::mutex> writeLock_;
    };

    static ConfiguredOverrideUpdate beginConfiguredOverrideUpdate();

    // Process-wide TuneAdvisor compatibility remains transitional authority. Concurrent embedded
    // construction waits for the primary lifecycle to commit initialization. If the primary
    // constructor fails before commit, one waiter takes ownership and initializes instead.
    class ConfiguredOverrideLifecycleLease {
    public:
        ConfiguredOverrideLifecycleLease() {
            std::unique_lock lock(TuneAdvisor::configuredOverrideLifecycleMutex());
            TuneAdvisor::configuredOverrideLifecycleCv().wait(
                lock, [] { return !TuneAdvisor::configuredOverrideLifecycleInitializing(); });
            auto& count = TuneAdvisor::configuredOverrideLifecycleCount();
            primary_ = count == 0;
            if (primary_) {
                TuneAdvisor::configuredOverrideLifecycleInitializing() = true;
                TuneAdvisor::resetPostIngestRuntimeStateForNewLifecycle();
            }
            ++count;
        }
        ~ConfiguredOverrideLifecycleLease() { release(); }

        ConfiguredOverrideLifecycleLease(const ConfiguredOverrideLifecycleLease&) = delete;
        ConfiguredOverrideLifecycleLease&
        operator=(const ConfiguredOverrideLifecycleLease&) = delete;

        [[nodiscard]] bool ownsInitialization() const noexcept { return primary_; }
        void release() noexcept {
            std::lock_guard lock(TuneAdvisor::configuredOverrideLifecycleMutex());
            if (!active_) {
                return;
            }
            active_ = false;
            auto& count = TuneAdvisor::configuredOverrideLifecycleCount();
            if (count > 0) {
                --count;
            }
            if (primary_ && !committed_) {
                TuneAdvisor::configuredOverrideLifecycleInitializing() = false;
                TuneAdvisor::configuredOverrideLifecycleCv().notify_all();
            }
        }
        void commitInitialization() noexcept {
            if (!primary_ || committed_) {
                return;
            }
            std::lock_guard lock(TuneAdvisor::configuredOverrideLifecycleMutex());
            committed_ = true;
            TuneAdvisor::configuredOverrideLifecycleInitializing() = false;
            TuneAdvisor::configuredOverrideLifecycleCv().notify_all();
        }

    private:
        bool primary_{false};
        bool committed_{false};
        bool active_{true};
    };

    static std::uint64_t configuredOverridesVersion() noexcept;

    // Template: must stay in the header (instantiated from ServiceManager, TuningManager,
    // ResourceGovernor).
    template <typename Reader> static auto readConfiguredOverridesSnapshot(Reader reader) {
        for (;;) {
            const auto before = configuredOverridesVersion();
            if ((before & 1U) != 0U) {
                std::this_thread::yield();
                continue;
            }
            auto result = reader();
            const auto after = configuredOverridesVersion();
            if (before == after && (after & 1U) == 0U) {
                return result;
            }
        }
    }

    // Start a fresh typed-config lifecycle without inheriting overrides installed while resolving
    // an earlier daemon in the same process. Dynamic caps and runtime observations are deliberately
    // excluded: this resets only fields written by ConfigResolver::applyRuntimeTuning().
    static void resetConfiguredOverrides() noexcept;

private:
    static std::recursive_mutex& configuredOverridePublicationMutex();
    static std::mutex& configuredOverrideWriteMutex();
    static std::mutex& configuredOverrideLifecycleMutex();
    static std::condition_variable& configuredOverrideLifecycleCv();
    static bool& configuredOverrideLifecycleInitializing();
    static std::size_t& configuredOverrideLifecycleCount();
    static std::mutex& postIngestStageActivityMutex();
    static std::uint64_t& nextPostIngestStageActivityToken();
    static std::map<std::uint64_t, std::uint8_t>& livePostIngestStageActivityTokens();
    static void resetPostIngestRuntimeStateForNewLifecycle() noexcept;
    static inline std::atomic<std::uint64_t> configuredOverrideSequence_{0};

    static void ignoreInvalidEnvParseFailure() noexcept;

    static std::mutex& compatibilityEnvironmentSnapshotMutex();

    static std::map<std::string, std::string>& compatibilityEnvironmentSnapshot();

    static bool& compatibilityEnvironmentSnapshotInitialized();

    static const char* compatibilityEnvironment(const char* name) {
        thread_local std::optional<std::string> copiedValue;
#ifdef YAMS_TESTING
        copiedValue = yams::config::getenv_optional(name);
#else
        std::unique_lock lock(compatibilityEnvironmentSnapshotMutex());
        if (!compatibilityEnvironmentSnapshotInitialized()) {
            // Keep one lock order: environment boundary first, compatibility snapshot second.
            // refreshCompatibilityEnvironmentSnapshot() uses the same order.
            lock.unlock();
            auto current = yams::config::snapshot_environment_prefix("YAMS_");
            lock.lock();
            if (!compatibilityEnvironmentSnapshotInitialized()) {
                compatibilityEnvironmentSnapshot() = std::move(current);
                compatibilityEnvironmentSnapshotInitialized() = true;
            }
        }
        const auto value = compatibilityEnvironmentSnapshot().find(name);
        copiedValue = value == compatibilityEnvironmentSnapshot().end()
                          ? std::nullopt
                          : std::optional<std::string>{value->second};
#endif
        return copiedValue ? copiedValue->c_str() : nullptr;
    }

    static std::optional<bool> parseExplicitBoolEnvNow(const char* name);

    static std::optional<uint32_t> parseBoundedUintEnvNow(const char* name, uint32_t minValue,
                                                          uint32_t maxValue);

    static std::optional<uint64_t> parseBoundedUint64EnvNow(const char* name, uint64_t minValue,
                                                            uint64_t maxValue);

    static std::optional<int> parseBoundedIntEnvNow(const char* name, int minValue, int maxValue);

    static uint32_t readUint32Override(const std::atomic<uint32_t>& overrideValue,
                                       const char* envName, uint32_t defaultValue,
                                       uint32_t minValue, uint32_t maxValue);

    static std::uint64_t readUint64Override(const std::atomic<std::uint64_t>& overrideValue,
                                            const char* envName, std::uint64_t defaultValue,
                                            std::uint64_t minValue, std::uint64_t maxValue);

    static int readPositiveIntOverride(const std::atomic<int>& overrideValue, const char* envName,
                                       int defaultValue, int minValue, int maxValue);

    static double readPositiveDoubleOverride(const std::atomic<double>& overrideValue,
                                             const char* envName, double defaultValue,
                                             double minValue, double maxValue);

    static std::optional<double> parseBoundedDoubleEnvNow(const char* name, double minValue,
                                                          double maxValue, double scale = 1.0);

    static std::optional<uint32_t> postStageConcurrentEnvOverride(const char* env, uint32_t maxCap);

public:
    // -------- Runtime-tunable policy (defaults chosen conservatively) --------

    /// CPU high threshold (%) for admission control. Profile-adjusted defaults:
    ///   Efficient:  50% (early throttling for usability)
    ///   Balanced:   67% (clamped but reasonable throughput)
    ///   Aggressive: 85% (late throttling, max throughput)
    /// Environment: YAMS_CPU_HIGH_PCT (0-100)
    static double cpuHighThresholdPercent();
    static void setCpuHighThresholdPercent(double v);
    static void resetCpuHighThresholdPercentOverride();

    /// Gap between CPU high and CPU critical thresholds (%).
    /// Default 40% (up from 25%) to avoid false Critical during ONNX inference.
    /// Environment: YAMS_CPU_CRITICAL_GAP_PCT (10-50)
    static double cpuCriticalGapPercent();

    // CPU admission control hysteresis.
    // These time windows help avoid rejecting work on brief CPU spikes.
    // Envs:
    // - YAMS_CPU_ADMIT_HIGH_HOLD_MS (0..60000) default 250ms
    // - YAMS_CPU_ADMIT_LOW_HOLD_MS  (0..60000) default 500ms
    static uint32_t cpuAdmissionHighHoldMs();
    static uint32_t cpuAdmissionLowHoldMs();

    /// Compute CPU-aware throttling delay in milliseconds.
    /// Returns 0 if CPU is below threshold, otherwise a small delay based on severity.
    ///
    /// Notes:
    /// - Daemon tick is typically very small (see statusTickMs()); large sleeps here can
    ///   dominate throughput and create a feedback loop where work never catches up.
    /// - cpuHighThresholdPercent() is defined in 0..100 semantics (percent of total host).
    ///
    /// Delay formula: (cpuPct - threshold) * 0.5ms, clamped to [2, 25]ms.
    static int32_t computeCpuThrottleDelayMs(double currentCpuPct);

    // Embedding batch tuning knobs (used by vector::EmbeddingService)
    static double embedSafety();
    static std::size_t embedDocCap();
    static void setEmbedDocCap(std::size_t v);
    // Maximum number of document hashes grouped into a single EmbedJob.
    // This is intentionally separate from embedDocCap (inference sub-batch size)
    // to keep individual jobs bounded while preserving model-efficient infer batches.
    static std::size_t embedJobDocCap();
    static unsigned embedPauseMs();

    // Chunk size for IPC streaming (bytes). Default 512 KiB.
    static uint32_t chunkSize();

    // Writer budget per turn for multiplexed writer (bytes). Default 3 MiB.
    static uint32_t writerBudgetBytesPerTurn();

    // -------- Server-side IPC/mux controls (centralized) --------
    // Max inflight requests per connection (server). Default tuned for fairness under
    // multi-client load.
    static std::size_t serverMaxInflightPerConn();

    // Per-request queued frames cap (server). Default 1024.
    static std::size_t serverQueueFramesCap();

    // Total queued bytes per connection cap (server). Default 128 MiB.
    static std::size_t serverQueueBytesCap();

    // Server writer budget per turn (bytes). Falls back to 8 MiB default for balanced
    // throughput if unset.
    static std::size_t serverWriterBudgetBytesPerTurn();

    // Server writer maximum budget clamp per turn (bytes). Centralized here for consistency.
    // Default 8 MiB; env YAMS_SERVER_WRITER_BUDGET_MAX may override (min 4 KiB).
    static std::size_t serverWriterBudgetMaxBytesPerTurn();

    // Suggested maximum worker queue depth before backpressure (0=auto). Default auto scales with
    // profile.
    static uint64_t maxWorkerQueue(size_t workerThreads);

    // Suggested mux queued-bytes budget before backpressure. Default scales with profile (Balanced:
    // 256 MiB).
    static uint64_t maxMuxBytes();

    // Suggested max active connections. Default 0 = unlimited.
    static uint64_t maxActiveConn();

    // Status/metrics tick cadence for daemon main loop. Default 5 ms.
    // ResourceGovernor /proc caching (100ms min interval) prevents excessive
    // filesystem I/O even at 200 ticks/sec.
    static uint32_t statusTickMs();

    // Idle-mode tick cadence for daemon tuning loop. Default 1000 ms.
    // When the daemon has no real work (zero non-health connections, empty queues),
    // the tuning loop sleeps for this duration instead of the active-mode 5 ms.
    // Dramatically reduces CPU wake-ups during idle periods.
    static uint32_t idleTickMs();

#ifdef YAMS_TESTING
    /// Test-only accessor: returns idle tick cadence.
    static uint32_t testing_idleTickMs() { return idleTickMs(); }
#endif

    // -------- Repair coordinator tuning (env-driven) --------
    // Max repair batch size per cycle.
    // Profile-scaled: Efficient=24, Balanced=32, Aggressive=48
    static uint32_t repairMaxBatch();

    // Batch size for repair operations during startup phase.
    // Profile-scaled: Efficient=25, Balanced=62, Aggressive=100
    // Smaller batches reduce startup load. Normal operation uses repairMaxBatch().
    static uint32_t repairStartupBatchSize();

    // Maintenance tokens (concurrency) when daemon is idle. Default 1.
    // Maintenance tokens (concurrency) when daemon is idle. Default scales with profile.
    // Efficient: 1, Balanced: 2, Aggressive: 4
    static uint32_t repairTokensIdle();
    // Maintenance tokens (concurrency) when daemon is busy (has active connections).
    // Default 0, except Aggressive mode which keeps 1 worker to ensure catch-up.
    static uint32_t repairTokensBusy();
    // Threshold of active connections to consider the daemon busy. Default 1.
    static uint32_t repairBusyConnThreshold();

    // Max allowed repair batches per second (rate limiter). Default 1.
    static uint32_t repairMaxBatchesPerSec();

    // Orphan scan interval (hours). Default 6h. Range 1-48h.
    static uint32_t orphanScanIntervalHours();

    static uint32_t repairDegradeHoldMs();
    static uint32_t repairReadyHoldMs();

    // Auto-repair tick scheduling (tiered). Set to 0 to disable a tier.
    static uint32_t repairAutoInitialDelayMinutes();

    static uint32_t repairAutoFastMinutes();

    static uint32_t repairAutoWarmHours();

    static uint32_t repairAutoColdHours();

    // Fts5Job consumer startup delay (ms). Default 2000ms.
    // Gives time for daemon to fully initialize before processing FTS5 jobs.
    static uint32_t fts5StartupDelayMs();

    // Fts5Job consumer throttle during startup (ms). Default 100ms.
    // Higher value reduces startup load. Normal operation uses 10ms.
    static uint32_t fts5StartupThrottleMs();

    // Metrics snapshot cache window (ms). Default 250 ms.
    static uint32_t metricsCacheMs();

    // -------- Central CPU budget and thread caps --------
    // Global CPU budget percent (10..100). Defaults adapt to profile posture.
    static uint32_t cpuBudgetPercent();

    // Absolute hard cap across subsystems (0 = no cap). Env: YAMS_MAX_THREADS
    static uint32_t maxThreadsOverall();

    // Profile-aware host reserve so daemon auto-sizing leaves room for other workloads.
    static uint32_t hostThreadReserve(unsigned hw);

    static uint32_t daemonThreadCapacity(unsigned hw);

    static uint64_t autoMemoryBudgetBytes(uint64_t systemMem);

    // WorkCoordinator threads (override, env, or derived).
    // Default: slightly I/O-biased relative to the general CPU budget because metadata and
    // retrieval paths frequently block on SQLite/disk work even when CPU usage is moderate.
    // Environment: YAMS_WORK_COORDINATOR_THREADS
    static uint32_t workCoordinatorThreads();
    static void setWorkCoordinatorThreads(uint32_t n);

    // Recommended thread count based on CPU budget. backgroundFactor in (0,1].
    static uint32_t recommendedThreads(double backgroundFactor = 1.0, uint32_t hardMax = 0);

#ifdef YAMS_TESTING
    static uint32_t testing_reservedHostThreads(unsigned hw) { return hostThreadReserve(hw); }
    static uint64_t testing_autoMemoryBudgetBytes(uint64_t systemMem) {
        return autoMemoryBudgetBytes(systemMem);
    }
#endif

    // Cached hardware concurrency (process-wide)
    static unsigned hardwareConcurrency();

    static void setHardwareConcurrencyForTests(unsigned v);

    // Embedding max concurrency (global). Env YAMS_EMBED_MAX_CONCURRENCY wins; else budgeted 25%.
    static uint32_t embedMaxConcurrencyBase();
    static uint32_t embedMaxConcurrency();
    // Runtime (daemon-only) dynamic cap. 0 = unset (use base/env).
    static void setEmbedMaxConcurrencyDynamicCap(uint32_t v);

    // -------- Code-controlled worker sizing (no env steering) --------
    // When non-zero, components should prefer these values over heuristics.
    static uint32_t postIngestThreads();
    static void setPostIngestThreads(uint32_t n);
    // Post-ingest queue capacity (bounded queue). Env override: YAMS_POST_INGEST_QUEUE_MAX
    static uint32_t postIngestQueueMax();
    static void setPostIngestQueueMax(uint32_t v);

    // Descriptor-count bound for the overflow FIFO behind kg_jobs. Admission releases raw
    // document bytes; variable-size paths/tags mean this is not a total-byte or RSS bound.
    // Typed key only: tuning.post_ingest_pending_kg_max.
    static uint32_t postIngestPendingKgMax();
    static void setPostIngestPendingKgMax(uint32_t v);

    // Post-ingest RPC queue capacity (high-priority channel). Env override:
    // YAMS_POST_INGEST_RPC_QUEUE_MAX
    static uint32_t postIngestRpcQueueMax();
    static void setPostIngestRpcQueueMax(uint32_t v);

    // Maximum number of high-priority (RPC) post-ingest tasks to drain per batch.
    // Env override: YAMS_POST_INGEST_RPC_MAX_PER_BATCH
    static uint32_t postIngestRpcMaxPerBatch();
    static void setPostIngestRpcMaxPerBatch(uint32_t value);
    // Post-ingest batching size. Env override: YAMS_POST_INGEST_BATCH_SIZE.
    // This is a cap: partial batches still dispatch after the bounded coalesce window.
    // Dynamically scales down when DB lock contention is detected.
    static uint32_t postIngestBatchSize();
    static void setPostIngestBatchSize(uint32_t v);

    // Override store for IPC timeout (ms)
    static inline std::atomic<uint32_t> ipcTimeoutMsOverride_{0};

    // IPC timeouts (ms) for read/write operations. Default 15000ms; env: YAMS_IPC_TIMEOUT_MS.
    // Range clamp [500, 600000] — wide enough for large-corpus benchmarks under
    // sanitizer builds where each op can take 30-60s.
    static uint32_t ipcTimeoutMs();
    static void setIpcTimeoutMs(uint32_t value);
    // Timeout for streaming chunk production (ms). When nonzero, a streaming
    // response will be failed with a Timeout error if next_chunk() exceeds this
    // limit. Default 30000ms; env: YAMS_STREAM_CHUNK_TIMEOUT_MS. Range clamp
    // [1000, 600000].
    static uint32_t streamChunkTimeoutMs();
    static void setStreamChunkTimeoutMs(uint32_t value);
    // -------- New centralized tuning getters (env-driven) --------
    // Backpressure read pause when receiver is backpressured (ms). Default 10.
    static uint32_t backpressureReadPauseMs();
    static void setBackpressureReadPauseMs(uint32_t ms);
    // Worker pool poll/sleep cadence (ms) for run loop. Default 150.
    static uint32_t workerPollMs();
    static void setWorkerPollMs(uint32_t ms);
    static void setWorkerPollMsDynamic(uint32_t ms);
    static bool workerPollMsPinned();

    // Idle shrink policy
    static double idleCpuThresholdPercent();
    static void setIdleCpuThresholdPercent(double pct);
    static std::uint64_t idleMuxLowBytes();
    static void setIdleMuxLowBytes(std::uint64_t b);
    static uint32_t idleShrinkHoldMs();
    static void setIdleShrinkHoldMs(uint32_t ms);
    static uint32_t poolCooldownMs();
    static void setPoolCooldownMs(uint32_t ms);
    static int poolScaleStep();
    static void setPoolScaleStep(int step);

    // Pool defaults (IPC CPU and IO pools)
    static uint32_t poolMinSizeIpc();
    static void setPoolMinSizeIpc(uint32_t v);
    static uint32_t poolMaxSizeIpc();
    static void setPoolMaxSizeIpc(uint32_t v);
    static uint32_t poolMinSizeIpcIo();
    static void setPoolMinSizeIpcIo(uint32_t v);
    static uint32_t poolMaxSizeIpcIo();
    static void setPoolMaxSizeIpcIo(uint32_t v);
    static uint32_t poolLowWatermarkPercent();
    static uint32_t poolHighWatermarkPercent();
    // -------- Connection slot dynamic sizing (PBI-085) --------
    // Minimum connection slots (floor for dynamic resizing). Default 256.
    // Environment: YAMS_CONN_SLOTS_MIN (range 1..1024)
    static uint32_t connectionSlotsMin();
    static void setConnectionSlotsMin(uint32_t v);

    // Maximum connection slots (ceiling for dynamic resizing). Default 4096.
    // Environment: YAMS_CONN_SLOTS_MAX (range 64..16384)
    static uint32_t connectionSlotsMax();
    static void setConnectionSlotsMax(uint32_t v);

    // Scale step for connection slot resizing. Default 16.
    // Environment: YAMS_CONN_SLOTS_STEP (range 1..128)
    static uint32_t connectionSlotsScaleStep();
    static void setConnectionSlotsScaleStep(uint32_t v);

    // Initial/target connection slots based on hardware and profile.
    // Formula: recommendedThreads * ioConnPerThread * 4 * (0.5 + profileScale)
    // With minimum of 256 slots. Profile-scaled: Efficient=lower, Aggressive=higher.
    static uint32_t connectionSlotsTarget();
    static uint32_t searchConcurrencyLimit();
    static uint32_t readPoolMaxConnections(uint32_t configuredMax);

    // Dedicated daemon-side list admission controls.
    // Defaults are profile-aware and can be overridden via tuning config.
    static uint32_t listInflightLimit();
    static void setListInflightLimit(uint32_t v);

    static uint32_t listAdmissionWaitMs();
    static void setListAdmissionWaitMs(uint32_t v);

    // Dedicated daemon-side grep admission controls.
    // Defaults are profile-aware and can be overridden via tuning config.
    static uint32_t grepInflightLimit();
    static void setGrepInflightLimit(uint32_t v);

    static uint32_t grepAdmissionWaitMs();
    static void setGrepAdmissionWaitMs(uint32_t v);

    // Writer drain ramp thresholds and multipliers
    static uint32_t writerActiveLow1Threshold();
    static uint32_t writerActiveLow2Threshold();
    static uint32_t writerActiveHigh1Threshold();
    static uint32_t writerActiveHigh2Threshold();
    static double writerScaleActiveLow1Mul();
    static double writerScaleActiveLow2Mul();
    static double writerScaleActiveHigh1Mul();
    static double writerScaleActiveHigh2Mul();
    static double writerQueuedHalfThresholdFraction();
    static double writerQueuedThreeQuarterThresholdFraction();
    static double writerScaleQueuedHalfMul();
    static double writerScaleQueuedThreeQuarterMul();

    // Streaming page sizing thresholds and clamps
    static std::uint64_t streamMuxVeryHighBytes();
    static std::uint64_t streamMuxHighBytes();
    static std::uint64_t streamMuxLight1Bytes();
    static std::uint64_t streamMuxLight2Bytes();
    static std::uint64_t streamMuxLight3Bytes();
    static double streamPageFactorVeryHighDiv(); // divide by 4
    static double streamPageFactorHighDiv();     // divide by 2
    static double streamPageFactorLight1Mul();
    static double streamPageFactorLight2Mul();
    static double streamPageFactorLight3Mul();
    static std::size_t streamPageClampMin();
    static std::size_t streamPageClampMax();

    // IO: desired average connections per thread before scaling up IO pool.
    // Default 8; override via YAMS_IO_CONN_PER_THREAD (range 1..1024).
    static uint32_t ioConnPerThread();
    static void setIoConnPerThread(uint32_t v);

    static bool enableParallelIngest();
    static void setEnableParallelIngest(bool en);

    static uint32_t maxIngestWorkers();
    static void setMaxIngestWorkers(uint32_t v);

    static uint32_t storagePoolSize();
    static void setStoragePoolSize(uint32_t v);

    static uint32_t ingestBacklogPerWorker();
    // Internal Event Bus toggles (config-driven)
    static bool useInternalBusForRepair();
    static void setUseInternalBusForRepair(bool en);
    static bool useInternalBusForPostIngest();
    static void setUseInternalBusForPostIngest(bool en);

    /// Number of dedicated I/O threads (default 10)
    /// Environment: YAMS_IO_THREADS
    static uint32_t ioThreadCount();
    /// Main-socket absolute connection lifetime in seconds (default 300).
    /// 0 disables lifetime-based forced close.
    /// Environment: YAMS_CONNECTION_LIFETIME_S
    static uint32_t connectionLifetimeSeconds();
    static void setConnectionLifetimeSeconds(uint32_t v);
    static void resetConnectionLifetimeSecondsOverride();

    static uint32_t maxIdleTimeouts();
    static uint32_t checkpointIntervalSeconds();
    static uint32_t checkpointInsertThreshold();
    static bool enableHotzoneCheckpoint();
    // =========================================================================
    // PBI-05a: PostIngestQueue Dynamic Concurrency Scaling
    // =========================================================================

    enum class PostIngestStage : uint8_t {
        Extraction = 0,
        KnowledgeGraph = 1,
        Symbol = 2,
        Entity = 3,
        Title = 4,
        Embed = 5
    };
    using PostIngestStageActivityToken = std::uint64_t;

    static void setPostIngestStageActive(PostIngestStage stage, bool active);
    static PostIngestStageActivityToken acquirePostIngestStageActivity(PostIngestStage stage);
    static void releasePostIngestStageActivity(PostIngestStage stage,
                                               PostIngestStageActivityToken token);
    static uint32_t postIngestStageActiveMask();

    /// Total post-ingest concurrency budget (shared across stages).
    /// Default uses cpuBudgetPercent() via recommendedThreads().
    /// Environment: YAMS_POST_INGEST_TOTAL_CONCURRENT
    static uint32_t postIngestTotalConcurrent();
    static void setPostIngestTotalConcurrent(uint32_t v);

    /// Maximum concurrent extraction tasks (profile-scaled, max 64)
    /// Profile-scaled: Efficient=2, Balanced=3, Aggressive=4
    /// Environment: YAMS_POST_EXTRACTION_CONCURRENT
    static uint32_t postExtractionDefaultConcurrent();
    static uint32_t postExtractionConcurrent();
    static void setPostExtractionConcurrent(uint32_t v);
    // Runtime (daemon-only) dynamic cap. UINT32_MAX = unset; 0 = zero concurrency.
    static void setPostExtractionConcurrentDynamicCap(uint32_t v);

    /// Maximum concurrent KG ingestion tasks (profile-scaled, max 64)
    /// Profile-scaled: Efficient=4, Balanced=6, Aggressive=8
    /// Environment: YAMS_POST_KG_CONCURRENT
    static uint32_t postKgDefaultConcurrent();
    static uint32_t postKgConcurrent();
    static void setPostKgConcurrent(uint32_t v);
    static void setPostKgConcurrentDynamicCap(uint32_t v);

    /// Maximum concurrent symbol extraction tasks (profile-scaled, max 32)
    /// Profile-scaled: Efficient=2, Balanced=3, Aggressive=4
    /// Environment: YAMS_POST_SYMBOL_CONCURRENT
    static uint32_t postSymbolDefaultConcurrent();
    static uint32_t postSymbolConcurrent();
    static void setPostSymbolConcurrent(uint32_t v);
    static void setPostSymbolConcurrentDynamicCap(uint32_t v);

    /// Maximum concurrent entity extraction tasks (profile-scaled, max 16)
    /// Entity extraction is CPU-heavy, so lower defaults
    /// Profile-scaled: Efficient=1, Balanced=2, Aggressive=2
    /// Environment: YAMS_POST_ENTITY_CONCURRENT
    static uint32_t postEntityDefaultConcurrent();
    static uint32_t postEntityConcurrent();
    static void setPostEntityConcurrent(uint32_t v);
    static void setPostEntityConcurrentDynamicCap(uint32_t v);

    /// Maximum concurrent title extraction tasks (profile-scaled, max 16)
    /// Environment: YAMS_POST_TITLE_CONCURRENT
    static uint32_t postTitleDefaultConcurrent();
    static uint32_t postTitleConcurrent();
    static void setPostTitleConcurrent(uint32_t v);
    static void setPostTitleConcurrentDynamicCap(uint32_t v);

    // PBI-05b: EmbeddingService concurrency (parallel embedding workers)
    // Embeddings are compute-heavy (ONNX inference) so we need parallelism to keep up with ingest
    // Profile-scaled: Efficient=2, Balanced=3, Aggressive=4
    static uint32_t postEmbedDefaultConcurrent();
    static uint32_t postEmbedConcurrent();
    static void setPostEmbedConcurrent(uint32_t v);
    static void setPostEmbedConcurrentDynamicCap(uint32_t v);

    // Seqlock helpers for DynamicCap batch writes.
    // Writer must call beginDynamicCapWrite() before and endDynamicCapWrite() after
    // storing all 6 DynamicCap atomics to prevent torn reads on the reader side.
    static void beginDynamicCapWrite();
    static void endDynamicCapWrite();

    // Read all 6 DynamicCap values atomically w.r.t. the seqlock.
    // Returns a consistent snapshot of all DynamicCap values.
    static std::array<uint32_t, 6> readDynamicCapsConsistent();

    // =========================================================================
    // ONNX Model Pool Sizing (GPU-aware)
    // =========================================================================

    /// Maximum concurrent ONNX sessions per model.
    /// Adjusted based on whether GPU is available:
    ///   GPU mode: max(2, min(hw_threads/2, 8)) - high throughput, GPU handles inference
    ///   CPU mode: max(1, min(hw_threads/4, 4)) - conservative to avoid CPU saturation
    /// Environment: YAMS_ONNX_SESSIONS_PER_MODEL
    static uint32_t onnxSessionsPerModel(bool gpuEnabled);
    static void setOnnxSessionsPerModel(uint32_t v);

    // Get the current embed channel capacity (for sizing the queue)
    static uint32_t embedChannelCapacity();
    static void setEmbedChannelCapacity(uint32_t v);

    // Ingest channel capacity (store_document_tasks). Clamp to post-ingest queue max to avoid
    // unbounded buffering of document payloads under governor backpressure.
    static uint32_t storeDocumentChannelCapacity();
    static void setStoreDocumentChannelCapacity(uint32_t v);

    // =========================================================================
    // DB Contention Management (adaptive concurrency based on lock errors)
    // =========================================================================

    /// Lock error threshold for scaling down concurrency (default 5)
    /// When recent lock errors exceed this, TuningManager reduces KG/embed concurrency
    /// Environment: YAMS_DB_LOCK_THRESHOLD
    static uint32_t dbLockErrorThreshold();
    /// Increment DB lock error counter (call this when "database is locked" error occurs)
    static void reportDbLockError();

    /// Get and reset DB lock error window count (called by TuningManager per tick)
    static uint64_t getAndResetDbLockErrors();

    /// Bulk result struct for postIngestBudgetAll() — avoids 6x redundant computation.
    struct PostIngestBudget {
        uint32_t extraction;
        uint32_t kg;
        uint32_t symbol;
        uint32_t entity;
        uint32_t title;
        uint32_t embed;
    };

    /// Compute the full post-ingest concurrency budget in a single call.
    /// Each individual getter (postExtractionConcurrent(), etc.) internally calls
    /// postIngestBudgetedConcurrency(), which is expensive. When you need all 6
    /// values in the same tick, use this method instead.
    static PostIngestBudget postIngestBudgetAll(bool includeDynamicCaps);

#ifdef YAMS_TESTING
    // ========================================================================
    // Test Hooks
    // ========================================================================

    /// Expose PostIngestConcurrencyBudget for deterministic unit testing
    struct TestBudget {
        uint32_t extraction;
        uint32_t kg;
        uint32_t symbol;
        uint32_t entity;
        uint32_t title;
        uint32_t embed;
    };

    /// Compute post-ingest budget in a single call (avoids 6x redundant computation)
    static TestBudget testing_postIngestBudget(bool includeDynamicCaps) {
        const auto b = postIngestBudgetAll(includeDynamicCaps);
        return TestBudget{b.extraction, b.kg, b.symbol, b.entity, b.title, b.embed};
    }
#endif

private:
    struct PostIngestConcurrencyBudget {
        uint32_t extraction;
        uint32_t kg;
        uint32_t symbol;
        uint32_t entity;
        uint32_t title;
        uint32_t embed;
    };

    static PostIngestConcurrencyBudget postIngestBudgetedConcurrency(bool includeDynamicCaps);

    // Runtime policy storage (single process); defaults chosen to reduce CPU when busy
    static inline std::atomic<double> cpuHighPct_{0.0};
    static inline std::atomic<double> embedSafety_{0.90};
    static inline std::atomic<std::size_t> embedDocCap_{0};    // 0 = no extra cap
    static inline std::atomic<std::size_t> embedJobDocCap_{0}; // 0 = use derived default
    static inline std::atomic<unsigned> embedPauseMs_{0};      // 0 = no pause
    static inline std::atomic<uint32_t> postIngestThreads_{0};

    // Overrides for config-driven tuning (0 or negative = unset)
    static inline std::atomic<uint32_t> backpressureReadPauseMsOverride_{0};
    static inline std::atomic<uint32_t> workerPollMsOverride_{0};
    static inline std::atomic<bool> workerPollMsPinned_{false};
    static inline std::atomic<double> idleCpuPctOverride_{-1.0};
    static inline std::atomic<std::uint64_t> idleMuxLowBytesOverride_{0};
    static inline std::atomic<uint32_t> idleShrinkHoldMsOverride_{0};
    static inline std::atomic<uint32_t> poolCooldownMsOverride_{0};
    static inline std::atomic<int> poolScaleStepOverride_{0};
    static inline std::atomic<uint32_t> poolMinSizeIpcOverride_{0};
    static inline std::atomic<uint32_t> poolMaxSizeIpcOverride_{0};
    static inline std::atomic<uint32_t> poolMinSizeIpcIoOverride_{0};
    static inline std::atomic<uint32_t> poolMaxSizeIpcIoOverride_{0};
    static inline std::atomic<uint32_t> poolLowWatermarkPctOverride_{0};
    static inline std::atomic<uint32_t> poolHighWatermarkPctOverride_{0};
    static inline std::atomic<uint32_t> searchConcurrencyOverride_{0};
    static inline std::atomic<uint32_t> listInflightLimitOverride_{0};
    static inline std::atomic<uint32_t> listAdmissionWaitMsOverride_{0};
    static inline std::atomic<uint32_t> grepInflightLimitOverride_{0};
    static inline std::atomic<uint32_t> grepAdmissionWaitMsOverride_{0};
    static inline std::atomic<unsigned> hwCached_{0};
    static inline std::atomic<uint32_t> postIngestTotalConcurrentOverride_{0};
    static inline std::atomic<uint32_t> postIngestStageActiveMaskOverride_{0};
    static inline std::array<std::atomic<uint32_t>, 6> postIngestStageOwnerCounts_{};
    static inline std::atomic<uint32_t> postIngestQueueMaxOverride_{0};
    static inline std::atomic<uint32_t> postIngestPendingKgMaxOverride_{0};
    static inline std::atomic<uint32_t> postIngestBatchSizeOverride_{0};
    static inline std::atomic<uint32_t> postIngestRpcQueueMaxOverride_{0};
    static inline std::atomic<uint32_t> storeDocumentChannelCapacityOverride_{0};
    static inline std::atomic<uint32_t> postIngestRpcMaxPerBatchOverride_{0};
    static inline std::atomic<uint32_t> ioConnPerThreadOverride_{0};
    static inline std::atomic<uint32_t> connectionSlotsMinOverride_{0};
    static inline std::atomic<uint32_t> connectionSlotsMaxOverride_{0};
    static inline std::atomic<uint32_t> connectionSlotsScaleStepOverride_{0};
    static inline std::atomic<uint32_t> connectionSlotsTargetOverride_{0};
    static inline std::atomic<int> enableParallelIngestOverride_{-1};
    static inline std::atomic<uint32_t> maxIngestWorkersOverride_{0};
    static inline std::atomic<uint32_t> storagePoolSizeOverride_{0};
    static inline std::atomic<uint32_t> ingestBacklogPerWorkerOverride_{0};
    static inline std::atomic<uint32_t> workCoordinatorThreadsOverride_{0};
    // Defaults: prefer internal event bus by default; config/env can override
    static inline std::atomic<bool> useInternalBusRepair_{true};
    static inline std::atomic<bool> useInternalBusPostIngest_{true};

    // PBI-089: Request Queue and IOCoordinator overrides
    static inline std::atomic<uint32_t> ioThreadCountOverride_{0};
    static inline std::atomic<int32_t> connectionLifetimeSecondsOverride_{-1};
    static inline std::atomic<uint32_t> maxIdleTimeoutsOverride_{0};
    static inline std::atomic<uint32_t> streamChunkTimeoutMsOverride_{0};

    // PBI-090: CheckpointManager overrides
    static inline std::atomic<uint32_t> checkpointIntervalSecondsOverride_{0};
    static inline std::atomic<uint32_t> checkpointInsertThresholdOverride_{0};
    static inline std::atomic<int> enableHotzoneCheckpointOverride_{-1};

    // PBI-05a: PostIngestQueue concurrency overrides
    static inline std::atomic<uint32_t> postExtractionConcurrentOverride_{0};
    static inline std::atomic<uint32_t> postKgConcurrentOverride_{0};
    static inline std::atomic<uint32_t> postSymbolConcurrentOverride_{0};
    static inline std::atomic<uint32_t> postEntityConcurrentOverride_{0};
    static inline std::atomic<uint32_t> postTitleConcurrentOverride_{0};

    // Runtime (daemon-only) dynamic caps for post-ingest stages (UINT32_MAX = unset)
    static inline std::atomic<uint32_t> postExtractionConcurrentDynamicCap_{UINT32_MAX};
    static inline std::atomic<uint32_t> postKgConcurrentDynamicCap_{UINT32_MAX};
    static inline std::atomic<uint32_t> postSymbolConcurrentDynamicCap_{UINT32_MAX};
    static inline std::atomic<uint32_t> postEntityConcurrentDynamicCap_{UINT32_MAX};
    static inline std::atomic<uint32_t> postTitleConcurrentDynamicCap_{UINT32_MAX};

    // PBI-05b: EmbeddingService concurrency overrides
    static inline std::atomic<uint32_t> embedMaxConcurrencyOverride_{0};
    static inline std::atomic<uint32_t> postEmbedConcurrentOverride_{0};
    static inline std::atomic<uint32_t> postEmbedConcurrentDynamicCap_{UINT32_MAX};

    // Seqlock counter for DynamicCap batch writes.
    // Writer increments to odd before stores, even after. Reader retries if odd or changed.
    // Prevents torn reads when 6 DynamicCap atomics are updated individually.
    static inline std::atomic<uint64_t> dynamicCapSeq_{0};
    static inline std::atomic<uint32_t> embedChannelCapacityOverride_{0};

    // ONNX Model Pool overrides
    static inline std::atomic<uint32_t> onnxSessionsPerModelOverride_{0};
    // =========================================================================
    // Resource Governor Configuration (Memory Pressure Management)
    // =========================================================================

public:
    /// Enable/disable the resource governor. When disabled, no memory pressure
    /// monitoring or adaptive scaling occurs. Default: true.
    /// Environment: YAMS_ENABLE_RESOURCE_GOVERNOR
    static bool enableResourceGovernor();
    static void setEnableResourceGovernor(bool en);

    /// Enable proactive model eviction under memory pressure. Default: true.
    /// Environment: YAMS_PROACTIVE_EVICTION
    static bool enableProactiveEviction();

    /// Enable admission control (refuse new work when at emergency pressure). Default: true.
    /// Environment: YAMS_ADMISSION_CONTROL
    static bool enableAdmissionControl();
    static void setEnableAdmissionControl(bool en);

    /// Percent of normal concurrency retained at Warning pressure (10-100).
    /// Used by ResourceGovernor to apply a gradual slowdown instead of abrupt halving.
    /// Environment: YAMS_GOV_WARNING_SCALE_PCT
    static uint32_t governorWarningScalePercent();
    static void setGovernorWarningScalePercent(uint32_t pct);
    static void resetGovernorWarningScalePercentOverride();

    /// Memory budget in bytes. 0 = auto-detect based on profile while leaving
    /// headroom for other system workloads.
    ///   Efficient:  45% system RAM
    ///   Balanced:   60% system RAM
    ///   Aggressive: 75% system RAM
    /// Environment: YAMS_MEMORY_BUDGET_BYTES
    static uint64_t memoryBudgetBytes();
    static void setMemoryBudgetBytes(uint64_t bytes);

    /// Memory warning threshold (0.0-1.0). Profile-adjusted defaults:
    ///   Efficient:  0.70 (70%)
    ///   Balanced:   0.75 (75%)
    ///   Aggressive: 0.80 (80%)
    /// Environment: YAMS_MEMORY_WARNING_PCT (0-100)
    static double memoryWarningThreshold();
    static void setMemoryWarningThreshold(double pct);

    /// Memory critical threshold (0.0-1.0). Profile-adjusted defaults:
    ///   Efficient:  0.85 (85%)
    ///   Balanced:   0.90 (90%)
    ///   Aggressive: 0.92 (92%)
    /// Environment: YAMS_MEMORY_CRITICAL_PCT (0-100)
    static double memoryCriticalThreshold();
    static void setMemoryCriticalThreshold(double pct);

    /// Memory emergency threshold (0.0-1.0). Profile-adjusted defaults:
    ///   Efficient:  0.92 (92%)
    ///   Balanced:   0.95 (95%)
    ///   Aggressive: 0.97 (97%)
    /// Environment: YAMS_MEMORY_EMERGENCY_PCT (0-100)
    static double memoryEmergencyThreshold();
    static void setMemoryEmergencyThreshold(double pct);

    /// Hysteresis duration before changing pressure level (milliseconds).
    /// Prevents rapid oscillation between levels. Default: 500ms.
    /// Environment: YAMS_MEMORY_HYSTERESIS_MS
    static uint32_t memoryHysteresisMs();
    static void setMemoryHysteresisMs(uint32_t ms);

    static uint32_t cpuLevelHysteresisMs();
    static void setCpuLevelHysteresisMs(uint32_t ms);

    /// Cooldown period between model evictions to prevent thrashing (ms). Default: 500.
    /// Environment: YAMS_MODEL_EVICTION_COOLDOWN_MS
    static uint32_t modelEvictionCooldownMs();
    // =========================================================================
    // Gradient Limiter Configuration (Netflix Gradient2 Algorithm)
    // =========================================================================

    /// Enable gradient-based adaptive concurrency limiters.
    /// When enabled, post-ingest stages automatically tune their concurrency
    /// based on measured latency feedback (replaces static thresholds).
    /// Environment: YAMS_ENABLE_GRADIENT_LIMITERS
    static bool enableSemanticNeighborBackfill();

    static bool enableGradientLimiters();
    static void setEnableGradientLimiters(bool en);

    /// Gradient limiter EMA smoothing alpha (short window).
    /// Higher = more responsive to latency changes. Range: 0.0-1.0. Default: 0.2.
    /// Environment: YAMS_GRADIENT_SMOOTHING_ALPHA
    static double gradientSmoothingAlpha();
    static void setGradientSmoothingAlpha(double alpha);

    /// Gradient limiter long-window EMA alpha (drift correction).
    /// Lower = slower drift correction, more stable. Range: 0.0-1.0. Default: 0.05.
    /// Environment: YAMS_GRADIENT_LONG_ALPHA
    static double gradientLongAlpha();
    static void setGradientLongAlpha(double alpha);

    /// Gradient limiter warmup samples before adjusting limits.
    /// Minimum samples collected before limit adjustment begins. Default: 10.
    /// Environment: YAMS_GRADIENT_WARMUP_SAMPLES
    static uint32_t gradientWarmupSamples();
    static void setGradientWarmupSamples(uint32_t samples);

    /// Gradient limiter tolerance multiplier.
    /// Maximum growth multiplier when RTT is improving. Default: 1.5.
    /// Environment: YAMS_GRADIENT_TOLERANCE
    static double gradientTolerance();
    static void setGradientTolerance(double tolerance);

    /// Gradient limiter initial concurrency limit.
    /// Starting concurrency per stage before gradient algorithm adjusts. Default: 4.0.
    /// Environment: YAMS_GRADIENT_INITIAL_LIMIT
    static double gradientInitialLimit();
    static void setGradientInitialLimit(double limit);

    /// Gradient limiter minimum concurrency limit (floor).
    /// Limit will never drop below this value. Default: 1.0.
    /// Environment: YAMS_GRADIENT_MIN_LIMIT
    static double gradientMinLimit();
    static void setGradientMinLimit(double limit);

    /// Gradient limiter maximum concurrency limit (ceiling).
    /// Limit will never exceed this value (per stage; overridden by stage cap). Default: 32.0.
    /// Environment: YAMS_GRADIENT_MAX_LIMIT
    static double gradientMaxLimit();
    static void setGradientMaxLimit(double limit);

    // =========================================================================
    // ONNX Concurrency Configuration (Global Slot Coordination)
    // =========================================================================

    /// Maximum concurrent ONNX operations (global across GLiNER, embeddings, reranking).
    /// 0 = auto (hw_threads/2, clamped 4-16). Default: auto.
    /// Environment: YAMS_ONNX_MAX_CONCURRENT
    static uint32_t onnxMaxConcurrent();
    static void setOnnxMaxConcurrent(uint32_t n);

    /// Reserved ONNX slots for GLiNER operations (entity/title extraction).
    /// Guarantees GLiNER gets at least this many slots even under contention. Default: 1.
    /// Environment: YAMS_ONNX_GLINER_RESERVED
    static uint32_t onnxGlinerReserved();
    static void setOnnxGlinerReserved(uint32_t n);

    /// Reserved ONNX slots for embedding operations.
    /// Guarantees embeddings get at least this many slots even under contention. Default: 1.
    /// Environment: YAMS_ONNX_EMBED_RESERVED
    static uint32_t onnxEmbedReserved();
    static void setOnnxEmbedReserved(uint32_t n);

    /// Reserved ONNX slots for reranking operations. Default: 1.
    /// Environment: YAMS_ONNX_RERANKER_RESERVED
    static uint32_t onnxRerankerReserved();
    static void setOnnxRerankerReserved(uint32_t n);

    // =========================================================================
    // Model Idle Maintenance Thresholds (Profile-Aware)
    // =========================================================================

    /// Maximum active connections before skipping model idle maintenance.
    /// Profile-adjusted: Efficient=2, Balanced=1, Aggressive=0.
    /// Environment: YAMS_MODEL_MAINT_CONN_THRESHOLD
    static uint32_t modelMaintenanceConnThreshold();
    /// Maximum active searches before skipping model idle maintenance.
    /// Profile-adjusted: Efficient=2, Balanced=1, Aggressive=0.
    /// Environment: YAMS_MODEL_MAINT_SEARCH_THRESHOLD
    static uint32_t modelMaintenanceSearchThreshold();
    /// Maximum post-ingest queue depth before skipping model idle maintenance.
    /// Profile-adjusted: Efficient=20, Balanced=10, Aggressive=0.
    /// Environment: YAMS_MODEL_MAINT_QUEUE_THRESHOLD
    static uint32_t modelMaintenanceQueueThreshold();
    // =========================================================================
    // Model Eviction Pressure Thresholds (Profile-Aware)
    // =========================================================================

    /// Pressure level to start warning-level model eviction (evict 1 model).
    /// Profile-adjusted: Efficient=0.30, Balanced=0.60, Aggressive=0.75.
    /// Environment: YAMS_MODEL_EVICT_WARNING_THRESHOLD
    static double modelEvictWarningThreshold();
    static void setModelEvictWarningThreshold(double v);

    /// Pressure level for critical-level model eviction (evict 2 models).
    /// Profile-adjusted: Efficient=0.50, Balanced=0.75, Aggressive=0.85.
    /// Environment: YAMS_MODEL_EVICT_CRITICAL_THRESHOLD
    static double modelEvictCriticalThreshold();
    static void setModelEvictCriticalThreshold(double v);

    /// Pressure level for emergency-level model eviction (evict all).
    /// Profile-adjusted: Efficient=0.70, Balanced=0.90, Aggressive=0.95.
    /// Environment: YAMS_MODEL_EVICT_EMERGENCY_THRESHOLD
    static double modelEvictEmergencyThreshold();
    static void setModelEvictEmergencyThreshold(double v);

    /// Clear all eviction threshold overrides, restoring profile/env var defaults.
    /// Primarily intended for testing to ensure test isolation.
    static void resetModelEvictThresholdOverrides();

private:
    struct ReadPathCapacityModel {
        uint32_t workerThreads{4};
        uint32_t searchConcurrencyLimit{4};
    };

    static double workCoordinatorIoBias();

    static uint32_t recommendedThreadsForHw(unsigned hw, double backgroundFactor,
                                            uint32_t hardMax = 0);

    static ReadPathCapacityModel defaultReadPathCapacityModel(unsigned hw);

    static uint32_t defaultReadPoolMaxConnectionsForHw(unsigned hw, uint32_t configuredMax);

    /// Detect system memory (cross-platform). Returns bytes.
    /// Implementation uses platform-specific APIs:
    ///   Windows: GlobalMemoryStatusEx
    ///   macOS: sysctlbyname("hw.memsize")
    ///   Linux: /proc/meminfo
    static uint64_t detectSystemMemory();

    // Resource Governor overrides
    static inline std::atomic<int> enableResourceGovernorOverride_{-1};
    static inline std::atomic<int> enableAdmissionControlOverride_{-1};
    static inline std::atomic<uint64_t> memoryBudgetBytesOverride_{0};
    static inline std::atomic<double> memoryWarningPctOverride_{0.0};
    static inline std::atomic<double> memoryCriticalPctOverride_{0.0};
    static inline std::atomic<double> memoryEmergencyPctOverride_{0.0};
    static inline std::atomic<uint32_t> memoryHysteresisMsOverride_{0};
    static inline std::atomic<uint32_t> cpuLevelHysteresisMsOverride_{0};
    static inline std::atomic<uint32_t> modelEvictionCooldownMsOverride_{0};
    static inline std::atomic<uint32_t> governorWarningScalePctOverride_{0};

    // Gradient limiter overrides
    static inline std::atomic<int> enableGradientLimitersOverride_{-1};
    static inline std::atomic<double> gradientSmoothingAlphaOverride_{0.0};
    static inline std::atomic<double> gradientLongAlphaOverride_{0.0};
    static inline std::atomic<uint32_t> gradientWarmupSamplesOverride_{0};
    static inline std::atomic<double> gradientToleranceOverride_{0.0};
    static inline std::atomic<double> gradientInitialLimitOverride_{0.0};
    static inline std::atomic<double> gradientMinLimitOverride_{0.0};
    static inline std::atomic<double> gradientMaxLimitOverride_{0.0};

    // ONNX concurrency overrides
    static inline std::atomic<uint32_t> onnxMaxConcurrentOverride_{0};
    // Zero is a valid reservation; UINT32_MAX restores profile/environment defaults.
    static inline std::atomic<uint32_t> onnxGlinerReservedOverride_{UINT32_MAX};
    static inline std::atomic<uint32_t> onnxEmbedReservedOverride_{UINT32_MAX};
    static inline std::atomic<uint32_t> onnxRerankerReservedOverride_{UINT32_MAX};

    // Model eviction pressure threshold overrides
    static inline std::atomic<double> modelEvictWarningOverride_{0.0};
    static inline std::atomic<double> modelEvictCriticalOverride_{0.0};
    static inline std::atomic<double> modelEvictEmergencyOverride_{0.0};
};

} // namespace yams::daemon
