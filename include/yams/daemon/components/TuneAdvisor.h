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
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <thread>

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
    enum class AutoEmbedPolicy { Never, Idle, Always };

    enum class Profile { Efficient, Balanced, Aggressive };

    // Resolve tuning profile (override -> env -> default Balanced).
    static Profile tuningProfile() {
        int ov = tuningProfileOverride_.load(std::memory_order_relaxed);
        if (ov == 1)
            return Profile::Efficient;
        if (ov == 2)
            return Profile::Balanced;
        if (ov == 3)
            return Profile::Aggressive;
        if (const char* s = std::getenv("YAMS_TUNING_PROFILE")) {
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

    static void setTuningProfile(Profile p) {
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

    // Scale factor applied to several heuristics
    // Efficient  -> 0.0 (minimal resource use)
    // Balanced   -> 0.5 (moderate resource use)
    // Aggressive -> 1.0 (maximum throughput)
    static double profileScale() {
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

    /// Profile scale for PostIngestQueue operations.
    /// Efficient  -> 0.0 (minimal resource use)
    /// Balanced   -> 0.5 (moderate resource use)
    /// Aggressive -> 1.0 (maximum throughput)
    static double postIngestProfileScale() { return profileScale(); }

    // Public accessors for embedding-related knobs (used outside daemon module)
    // These forward to internal tunables while keeping implementation details private.
    static double getEmbedSafety() { return embedSafety(); }
    static std::size_t getEmbedDocCap() { return embedDocCap(); }
    static unsigned getEmbedPauseMs() { return embedPauseMs(); }
    static uint32_t getEmbedMaxConcurrency() { return embedMaxConcurrency(); }

public:
    static inline std::atomic<int> tuningProfileOverride_{0};

    // -------- Runtime-tunable policy (defaults chosen conservatively) --------
    static AutoEmbedPolicy autoEmbedPolicy() {
        return autoEmbedPolicy_.load(std::memory_order_relaxed);
    }
    static void setAutoEmbedPolicy(AutoEmbedPolicy p) {
        autoEmbedPolicy_.store(p, std::memory_order_relaxed);
    }

    static double cpuIdleThresholdPercent() { return cpuIdlePct_.load(std::memory_order_relaxed); }
    static void setCpuIdleThresholdPercent(double v) {
        cpuIdlePct_.store(v, std::memory_order_relaxed);
    }

    /// CPU high threshold (%) for admission control. Profile-adjusted defaults:
    ///   Efficient:  50% (early throttling for usability)
    ///   Balanced:   67% (clamped but reasonable throughput)
    ///   Aggressive: 85% (late throttling, max throughput)
    /// Environment: YAMS_CPU_HIGH_PCT (0-100)
    static double cpuHighThresholdPercent() {
        if (const char* s = std::getenv("YAMS_CPU_HIGH_PCT")) {
            try {
                double v = std::stod(s);
                if (v >= 10.0 && v <= 100.0)
                    return v;
            } catch (...) {
            }
        }
        return 50.0 + profileScale() * 35.0;
    }
    static void setCpuHighThresholdPercent(double v) {
        cpuHighPct_.store(v, std::memory_order_relaxed);
    }

    // CPU admission control hysteresis.
    // These time windows help avoid rejecting work on brief CPU spikes.
    // Envs:
    // - YAMS_CPU_ADMIT_HIGH_HOLD_MS (0..60000) default 250ms
    // - YAMS_CPU_ADMIT_LOW_HOLD_MS  (0..60000) default 500ms
    static uint32_t cpuAdmissionHighHoldMs() {
        uint32_t def = 250;
        if (const char* s = std::getenv("YAMS_CPU_ADMIT_HIGH_HOLD_MS")) {
            try {
                auto v = static_cast<uint64_t>(std::stoull(s));
                if (v <= 60000)
                    return static_cast<uint32_t>(v);
            } catch (...) {
            }
        }
        return def;
    }
    static uint32_t cpuAdmissionLowHoldMs() {
        uint32_t def = 500;
        if (const char* s = std::getenv("YAMS_CPU_ADMIT_LOW_HOLD_MS")) {
            try {
                auto v = static_cast<uint64_t>(std::stoull(s));
                if (v <= 60000)
                    return static_cast<uint32_t>(v);
            } catch (...) {
            }
        }
        return def;
    }

    /// Compute CPU-aware throttling delay in milliseconds.
    /// Returns 0 if CPU is below threshold, otherwise a small delay based on severity.
    ///
    /// Notes:
    /// - Daemon tick is typically very small (see statusTickMs()); large sleeps here can
    ///   dominate throughput and create a feedback loop where work never catches up.
    /// - cpuHighThresholdPercent() is defined in 0..100 semantics (percent of total host).
    ///
    /// Delay formula: (cpuPct - threshold) * 0.5ms, clamped to [2, 25]ms.
    static int32_t computeCpuThrottleDelayMs(double currentCpuPct) {
        double threshold = cpuHighThresholdPercent();
        if (currentCpuPct < threshold)
            return 0;
        double overage = currentCpuPct - threshold;
        int32_t delayMs = static_cast<int32_t>(std::llround(overage * 0.5));
        return std::clamp(delayMs, 2, 25);
    }

    static std::uint64_t muxBacklogHighBytes() {
        return muxHighBytes_.load(std::memory_order_relaxed);
    }
    static void setMuxBacklogHighBytes(std::uint64_t v) {
        muxHighBytes_.store(v, std::memory_order_relaxed);
    }

    // Embedding batch tuning knobs (used by vector::EmbeddingService)
    static double embedSafety() { return embedSafety_.load(std::memory_order_relaxed); }
    static void setEmbedSafety(double v) {
        if (v < 0.5)
            v = 0.5;
        if (v > 0.95)
            v = 0.95;
        embedSafety_.store(v, std::memory_order_relaxed);
    }
    static std::size_t embedDocCap() {
        std::size_t ov = embedDocCap_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        if (const char* s = std::getenv("YAMS_EMBED_DOC_CAP")) {
            try {
                std::size_t v = static_cast<std::size_t>(std::stoull(s));
                if (v >= 1 && v <= 4096)
                    return v;
            } catch (...) {
            }
        }
        return 0;
    }
    static void setEmbedDocCap(std::size_t v) { embedDocCap_.store(v, std::memory_order_relaxed); }
    static unsigned embedPauseMs() { return embedPauseMs_.load(std::memory_order_relaxed); }
    static void setEmbedPauseMs(unsigned v) { embedPauseMs_.store(v, std::memory_order_relaxed); }

    // Chunk size for IPC streaming (bytes). Default 512 KiB.
    static uint32_t chunkSize() {
        uint32_t def = 512u * 1024u;
        if (const char* cs = std::getenv("YAMS_CHUNK_SIZE")) {
            try {
                auto v = static_cast<uint64_t>(std::stoull(cs));
                if (v >= 4ull * 1024ull && v <= 8ull * 1024ull * 1024ull)
                    return static_cast<uint32_t>(v);
            } catch (...) {
            }
        }
        return def;
    }

    // Writer budget per turn for multiplexed writer (bytes). Default 3 MiB.
    static uint32_t writerBudgetBytesPerTurn() {
        uint32_t def = 3072u * 1024u; // 3 MiB
        if (const char* wb = std::getenv("YAMS_WRITER_BUDGET_BYTES")) {
            try {
                auto v = static_cast<uint64_t>(std::stoull(wb));
                if (v >= 64ull * 1024ull && v <= 64ull * 1024ull * 1024ull)
                    return static_cast<uint32_t>(v);
            } catch (...) {
            }
        }
        return def;
    }

    // -------- Server-side IPC/mux controls (centralized) --------
    // Max inflight requests per connection (server). Default aligns with handler default (2048).
    static std::size_t serverMaxInflightPerConn() {
        if (const char* s = std::getenv("YAMS_SERVER_MAX_INFLIGHT")) {
            try {
                std::size_t v = static_cast<std::size_t>(std::stoul(s));
                if (v > 0)
                    return v;
            } catch (...) {
            }
        }
        return static_cast<std::size_t>(2048);
    }

    // Per-request queued frames cap (server). Default 1024.
    static std::size_t serverQueueFramesCap() {
        if (const char* s = std::getenv("YAMS_SERVER_QUEUE_FRAMES_CAP")) {
            try {
                std::size_t v = static_cast<std::size_t>(std::stoul(s));
                if (v > 0)
                    return v;
            } catch (...) {
            }
        }
        return static_cast<std::size_t>(1024);
    }

    // Total queued bytes per connection cap (server). Default 256 MiB.
    static std::size_t serverQueueBytesCap() {
        if (const char* s = std::getenv("YAMS_SERVER_QUEUE_BYTES_CAP")) {
            try {
                std::size_t v = static_cast<std::size_t>(std::stoul(s));
                if (v >= 1024)
                    return v;
            } catch (...) {
            }
        }
        return static_cast<std::size_t>(256ull * 1024ull * 1024ull);
    }

    // Server writer budget per turn (bytes). Falls back to client/general writer budget if unset.
    static std::size_t serverWriterBudgetBytesPerTurn() {
        if (const char* s = std::getenv("YAMS_SERVER_WRITER_BUDGET_BYTES")) {
            try {
                std::size_t v = static_cast<std::size_t>(std::stoul(s));
                if (v >= 4096)
                    return v;
            } catch (...) {
            }
        }
        return static_cast<std::size_t>(writerBudgetBytesPerTurn());
    }

    // Server writer maximum budget clamp per turn (bytes). Centralized here for consistency.
    // Default 8 MiB; env YAMS_SERVER_WRITER_BUDGET_MAX may override (min 4 KiB).
    static std::size_t serverWriterBudgetMaxBytesPerTurn() {
        std::size_t def = 8ull * 1024ull * 1024ull;
        if (const char* mb = std::getenv("YAMS_SERVER_WRITER_BUDGET_MAX")) {
            try {
                auto v = static_cast<std::size_t>(std::stoul(mb));
                if (v >= 4096)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }

    // Suggested maximum worker queue depth before backpressure (0=auto). Default auto scales with
    // profile.
    static uint64_t maxWorkerQueue(size_t workerThreads) {
        if (const char* s = std::getenv("YAMS_MAX_WORKER_QUEUE")) {
            try {
                return static_cast<uint64_t>(std::stoull(s));
            } catch (...) {
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

    // Suggested mux queued-bytes budget before backpressure. Default scales with profile (Balanced:
    // 256 MiB).
    static uint64_t maxMuxBytes() {
        constexpr uint64_t kBase = 256ull * 1024ull * 1024ull;
        double scale = profileScale();
        if (scale < 0.5)
            scale = 0.5;
        if (scale > 2.0)
            scale = 2.0;
        uint64_t def = static_cast<uint64_t>(std::llround(static_cast<double>(kBase) * scale));
        if (def < 64ull * 1024ull * 1024ull)
            def = 64ull * 1024ull * 1024ull;
        if (const char* s = std::getenv("YAMS_MAX_MUX_BYTES")) {
            try {
                return static_cast<uint64_t>(std::stoull(s));
            } catch (...) {
            }
        }
        return def;
    }

    // Suggested max active connections. Default 0 = unlimited.
    static uint64_t maxActiveConn() {
        if (const char* s = std::getenv("YAMS_MAX_ACTIVE_CONN")) {
            try {
                return static_cast<uint64_t>(std::stoull(s));
            } catch (...) {
            }
        }
        return 0;
    }

    // Status/metrics tick cadence for daemon main loop. Default 5 ms.
    // Reduced from 250ms -> 50ms -> 5ms for real-time governor metrics.
    // Timing behaviors (hysteresis, startup) are now decoupled via ms-based constants.
    static uint32_t statusTickMs() {
        uint32_t def = 5;
        if (const char* s = std::getenv("YAMS_STATUS_TICK_MS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v > 0 && v < 10000)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }

    // -------- Repair coordinator tuning (env-driven) --------
    // Max repair batch size per cycle.
    // Profile-scaled: Efficient=24, Balanced=32, Aggressive=48
    static uint32_t repairMaxBatch() {
        if (const char* s = std::getenv("YAMS_REPAIR_MAX_BATCH")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v > 0 && v <= 1000)
                    return v;
            } catch (...) {
            }
        }
        // Profile-scaled: Efficient=8, Balanced=20, Aggressive=32
        uint32_t base = 8;
        uint32_t range = 24;
        return base + static_cast<uint32_t>(range * profileScale());
    }

    // Batch size for repair operations during startup phase.
    // Profile-scaled: Efficient=25, Balanced=62, Aggressive=100
    // Smaller batches reduce startup load. Normal operation uses repairMaxBatch().
    static uint32_t repairStartupBatchSize() {
        if (const char* s = std::getenv("YAMS_REPAIR_STARTUP_BATCH")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v > 0 && v <= 1000)
                    return v;
            } catch (...) {
            }
        }
        // Profile-scaled: Efficient=25, Balanced=62, Aggressive=100
        uint32_t base = 25;
        uint32_t range = 75;
        return base + static_cast<uint32_t>(range * profileScale());
    }

    // Maintenance tokens (concurrency) when daemon is idle. Default 1.
    static uint32_t repairTokensIdle() {
        uint32_t def = 1;
        if (const char* s = std::getenv("YAMS_REPAIR_TOKENS_IDLE")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                return v;
            } catch (...) {
            }
        }
        return def;
    }
    // Maintenance tokens (concurrency) when daemon is busy (has active connections). Default 0.
    static uint32_t repairTokensBusy() {
        uint32_t def = 0;
        if (const char* s = std::getenv("YAMS_REPAIR_TOKENS_BUSY")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                return v;
            } catch (...) {
            }
        }
        return def;
    }
    // Threshold of active connections to consider the daemon busy. Default 1.
    static uint32_t repairBusyConnThreshold() {
        uint32_t def = 1;
        if (const char* s = std::getenv("YAMS_REPAIR_BUSY_CONN_THRESHOLD")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                return v;
            } catch (...) {
            }
        }
        return def;
    }

    // Max allowed repair batches per second (rate limiter). Default 1.
    static uint32_t repairMaxBatchesPerSec() {
        uint32_t def = 1;
        if (const char* s = std::getenv("YAMS_REPAIR_MAX_BATCHES_PER_SEC")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v <= 1000)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }

    // Orphan scan interval (hours). Default 6h. Range 1-48h.
    static uint32_t orphanScanIntervalHours() {
        uint32_t def = 6;
        if (const char* s = std::getenv("YAMS_ORPHAN_SCAN_INTERVAL_HOURS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 48)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }

    // Hysteresis: ms to hold busy before degrading. Default 750 ms.
    static uint32_t repairDegradeHoldMs() {
        uint32_t def = 750;
        if (const char* s = std::getenv("YAMS_REPAIR_DEGRADE_HOLD_MS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                return v;
            } catch (...) {
            }
        }
        return def;
    }
    // Hysteresis: ms to hold idle/clear before returning to ready. Default 1500 ms.
    static uint32_t repairReadyHoldMs() {
        uint32_t def = 1500;
        if (const char* s = std::getenv("YAMS_REPAIR_READY_HOLD_MS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                return v;
            } catch (...) {
            }
        }
        return def;
    }

    // Fts5Job consumer startup delay (ms). Default 2000ms.
    // Gives time for daemon to fully initialize before processing FTS5 jobs.
    static uint32_t fts5StartupDelayMs() {
        uint32_t def = 2000;
        if (const char* s = std::getenv("YAMS_FTS5_STARTUP_DELAY_MS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v <= 60000)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }

    // Fts5Job consumer throttle during startup (ms). Default 100ms.
    // Higher value reduces startup load. Normal operation uses 10ms.
    static uint32_t fts5StartupThrottleMs() {
        uint32_t def = 100;
        if (const char* s = std::getenv("YAMS_FTS5_STARTUP_THROTTLE_MS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 10 && v <= 1000)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }

    // Metrics snapshot cache window (ms). Default 250 ms.
    static uint32_t metricsCacheMs() {
        uint32_t def = 250;
        if (const char* s = std::getenv("YAMS_METRICS_CACHE_MS")) {
            try {
                return static_cast<uint32_t>(std::stoul(s));
            } catch (...) {
            }
        }
        return def;
    }

    // Max inflight per connection on client/adapter. Default 128.
    static size_t maxInflight() {
        size_t def = 128;
        if (const char* s = std::getenv("YAMS_MAX_INFLIGHT")) {
            try {
                size_t v = static_cast<size_t>(std::stoul(s));
                if (v > 0)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }

    // -------- Central CPU budget and thread caps --------
    // Global CPU budget percent (10..100). Defaults adapt to profile posture.
    static uint32_t cpuBudgetPercent() {
        uint32_t def = static_cast<uint32_t>(40.0 + profileScale() * 20.0);
        if (const char* s = std::getenv("YAMS_CPU_BUDGET_PERCENT")) {
            try {
                int v = std::stoi(s);
                if (v >= 10 && v <= 100)
                    return static_cast<uint32_t>(v);
            } catch (...) {
            }
        }
        return def;
    }

    // Absolute hard cap across subsystems (0 = no cap). Env: YAMS_MAX_THREADS
    static uint32_t maxThreadsOverall() {
        if (const char* s = std::getenv("YAMS_MAX_THREADS")) {
            try {
                int v = std::stoi(s);
                if (v >= 1 && v <= 1024)
                    return static_cast<uint32_t>(v);
            } catch (...) {
            }
        }
        return 0;
    }

    // Recommended thread count based on CPU budget. backgroundFactor in (0,1].
    static uint32_t recommendedThreads(double backgroundFactor = 1.0, uint32_t hardMax = 0) {
        unsigned hw = hardwareConcurrency();
        double budget = static_cast<double>(cpuBudgetPercent()) / 100.0;
        if (backgroundFactor <= 0.0)
            backgroundFactor = 0.5;
        double eff = std::clamp(budget * backgroundFactor, 0.1, 1.0);
        uint32_t cap =
            static_cast<uint32_t>(std::max(1.0, std::floor(eff * static_cast<double>(hw))));
        uint32_t absMax = maxThreadsOverall();
        if (absMax > 0)
            cap = std::min(cap, absMax);
        if (hardMax > 0)
            cap = std::min(cap, hardMax);
        return std::max(1u, cap);
    }

    // Cached hardware concurrency (process-wide)
    static unsigned hardwareConcurrency() {
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

    static void setHardwareConcurrencyForTests(unsigned v) {
        if (v == 0)
            v = 1;
        hwCached_.store(v, std::memory_order_relaxed);
    }

    // Embedding max concurrency (global). Env YAMS_EMBED_MAX_CONCURRENCY wins; else budgeted 25%.
    static uint32_t embedMaxConcurrency() {
        if (const char* s = std::getenv("YAMS_EMBED_MAX_CONCURRENCY")) {
            try {
                int v = std::stoi(s);
                if (v >= 1 && v <= 1024)
                    return static_cast<uint32_t>(v);
            } catch (...) {
            }
        }
        // Use a conservative fraction (25% of budgeted threads) for embeddings
        uint32_t rec = recommendedThreads(0.25);
        return std::max(1u, rec);
    }

    // -------- Code-controlled worker sizing (no env steering) --------
    // When non-zero, components should prefer these values over heuristics.
    static uint32_t postIngestThreads() {
        // 1) Explicit override set by config/daemon_main
        uint32_t configured = postIngestThreads_.load(std::memory_order_relaxed);
        if (configured != 0)
            return configured;
        // 2) Environment variable override for quick experiments
        if (const char* s = std::getenv("YAMS_POST_INGEST_THREADS")) {
            try {
                int v = std::stoi(s);
                if (v >= 1 && v <= 64)
                    return static_cast<uint32_t>(v);
            } catch (...) {
            }
        }
        // 3) Conservative default: single background worker; users can raise via config/env
        return 1u;
    }
    static void setPostIngestThreads(uint32_t n) {
        postIngestThreads_.store(n, std::memory_order_relaxed);
    }
    // Post-ingest queue capacity (bounded queue). Env override: YAMS_POST_INGEST_QUEUE_MAX
    static uint32_t postIngestQueueMax() {
        uint32_t ov = postIngestQueueMaxOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        if (const char* s = std::getenv("YAMS_POST_INGEST_QUEUE_MAX")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 10 && v <= 1'000'000)
                    return v;
            } catch (...) {
            }
        }
        return 1000;
    }
    static void setPostIngestQueueMax(uint32_t v) {
        postIngestQueueMaxOverride_.store(v, std::memory_order_relaxed);
    }

    // Post-ingest batching size. Env override: YAMS_POST_INGEST_BATCH_SIZE
    // Profile-scaled: Efficient=4, Balanced=6, Aggressive=8
    // Dynamically scales down when DB lock contention is detected.
    static uint32_t postIngestBatchSize() {
        uint32_t ov = postIngestBatchSizeOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;

        // Profile-scaled base: Efficient=4, Balanced=6, Aggressive=8
        uint32_t baseBatchSize =
            std::max(1u, static_cast<uint32_t>(8.0 * postIngestProfileScale()));
        auto embedCap = static_cast<uint32_t>(getEmbedDocCap());
        if (embedCap == 0) {
            embedCap = 64;
        }
        baseBatchSize = std::min({baseBatchSize, embedCap, 256u});
        if (const char* s = std::getenv("YAMS_POST_INGEST_BATCH_SIZE")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 256)
                    baseBatchSize = v;
            } catch (...) {
            }
        }

        // Adaptive scaling: reduce batch size when lock contention is high
        uint64_t recentErrors = dbLockErrorCount_.load(std::memory_order_relaxed);
        if (recentErrors > 10) {
            return 1; // Maximum contention: single-document transactions
        } else if (recentErrors > 5) {
            return std::min(baseBatchSize, 2u);
        } else if (recentErrors > 2) {
            return std::min(baseBatchSize, 4u);
        }
        return baseBatchSize;
    }
    static void setPostIngestBatchSize(uint32_t v) {
        postIngestBatchSizeOverride_.store(v, std::memory_order_relaxed);
    }

    // Override store for IPC timeout (ms)
    static inline std::atomic<uint32_t> ipcTimeoutMsOverride_{0};

    // IPC timeouts (ms) for read/write operations. Default 5000ms; env: YAMS_IPC_TIMEOUT_MS.
    static uint32_t ipcTimeoutMs() {
        uint32_t ov = ipcTimeoutMsOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        uint32_t def = 5000;
        if (const char* s = std::getenv("YAMS_IPC_TIMEOUT_MS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 500 && v <= 60000)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }
    static void setIpcTimeoutMs(uint32_t ms) {
        ipcTimeoutMsOverride_.store(ms, std::memory_order_relaxed);
    }

    // Timeout for streaming chunk production (ms). When nonzero, a streaming
    // response will be failed with a Timeout error if next_chunk() exceeds this
    // limit. Default 30000ms; env: YAMS_STREAM_CHUNK_TIMEOUT_MS. Range clamp
    // [1000, 600000].
    static uint32_t streamChunkTimeoutMs() {
        uint32_t ov = streamChunkTimeoutMsOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        uint32_t def = 30000;
        if (const char* s = std::getenv("YAMS_STREAM_CHUNK_TIMEOUT_MS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1000 && v <= 600000)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }
    static void setStreamChunkTimeoutMs(uint32_t ms) {
        streamChunkTimeoutMsOverride_.store(ms, std::memory_order_relaxed);
    }
    static uint32_t mcpWorkerThreads() { return mcpWorkerThreads_.load(std::memory_order_relaxed); }
    static void setMcpWorkerThreads(uint32_t n) {
        mcpWorkerThreads_.store(n, std::memory_order_relaxed);
    }

    // KG batching control (code-level, default enabled)
    static bool kgBatchEdgesEnabled() { return kgBatchEdges_.load(std::memory_order_relaxed); }
    static void setKgBatchEdgesEnabled(bool e) {
        kgBatchEdges_.store(e, std::memory_order_relaxed);
    }
    static bool kgBatchNodesEnabled() { return kgBatchNodes_.load(std::memory_order_relaxed); }
    static void setKgBatchNodesEnabled(bool e) {
        kgBatchNodes_.store(e, std::memory_order_relaxed);
    }

    // Analyzer toggles and caps
    static bool analyzerUrls() { return analyzerUrls_.load(std::memory_order_relaxed); }
    static void setAnalyzerUrls(bool e) { analyzerUrls_.store(e, std::memory_order_relaxed); }
    static bool analyzerEmails() { return analyzerEmails_.load(std::memory_order_relaxed); }
    static void setAnalyzerEmails(bool e) { analyzerEmails_.store(e, std::memory_order_relaxed); }
    static bool analyzerFilePaths() { return analyzerFilePaths_.load(std::memory_order_relaxed); }
    static void setAnalyzerFilePaths(bool e) {
        analyzerFilePaths_.store(e, std::memory_order_relaxed);
    }
    static std::size_t maxEntitiesPerDoc() {
        return maxEntitiesPerDoc_.load(std::memory_order_relaxed);
    }
    static void setMaxEntitiesPerDoc(std::size_t n) {
        maxEntitiesPerDoc_.store(n, std::memory_order_relaxed);
    }

    // -------- New centralized tuning getters (env-driven) --------
    // Backpressure read pause when receiver is backpressured (ms). Default 10.
    static uint32_t backpressureReadPauseMs() {
        uint32_t ov = backpressureReadPauseMsOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        uint32_t def = 10;
        if (const char* s = std::getenv("YAMS_BACKPRESSURE_READ_PAUSE_MS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v <= 1000)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }
    static void setBackpressureReadPauseMs(uint32_t ms) {
        backpressureReadPauseMsOverride_.store(ms, std::memory_order_relaxed);
    }
    // Worker pool poll/sleep cadence (ms) for run loop. Default 150.
    static uint32_t workerPollMs() {
        uint32_t ov = workerPollMsOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        uint32_t def = 150;
        if (const char* s = std::getenv("YAMS_WORKER_POLL_MS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 50 && v <= 2000)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }
    static void setWorkerPollMs(uint32_t ms) {
        workerPollMsOverride_.store(ms, std::memory_order_relaxed);
        workerPollMsPinned_.store(ms != 0, std::memory_order_relaxed);
    }
    static void setWorkerPollMsDynamic(uint32_t ms) {
        if (ms == 0 || workerPollMsPinned())
            return;
        workerPollMsOverride_.store(ms, std::memory_order_relaxed);
    }
    static bool workerPollMsPinned() {
        if (workerPollMsPinned_.load(std::memory_order_relaxed))
            return true;
        if (const char* s = std::getenv("YAMS_WORKER_POLL_MS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                return (v >= 50 && v <= 2000);
            } catch (...) {
            }
        }
        return false;
    }

    // Idle shrink policy
    static double idleCpuThresholdPercent() {
        double ov = idleCpuPctOverride_.load(std::memory_order_relaxed);
        if (ov > 0.0)
            return ov;
        double def = 10.0;
        if (const char* s = std::getenv("YAMS_IDLE_CPU_PCT")) {
            try {
                double v = std::stod(s);
                if (v >= 0.0 && v <= 100.0)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }
    static void setIdleCpuThresholdPercent(double pct) {
        idleCpuPctOverride_.store(pct, std::memory_order_relaxed);
    }
    static std::uint64_t idleMuxLowBytes() {
        std::uint64_t ov = idleMuxLowBytesOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        std::uint64_t def = 4ull * 1024ull * 1024ull;
        if (const char* s = std::getenv("YAMS_IDLE_MUX_LOW_BYTES")) {
            try {
                return static_cast<std::uint64_t>(std::stoull(s));
            } catch (...) {
            }
        }
        return def;
    }
    static void setIdleMuxLowBytes(std::uint64_t b) {
        idleMuxLowBytesOverride_.store(b, std::memory_order_relaxed);
    }
    static uint32_t idleShrinkHoldMs() {
        uint32_t ov = idleShrinkHoldMsOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        uint32_t def = 5000;
        if (const char* s = std::getenv("YAMS_IDLE_SHRINK_HOLD_MS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 500 && v <= 60000)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }
    static void setIdleShrinkHoldMs(uint32_t ms) {
        idleShrinkHoldMsOverride_.store(ms, std::memory_order_relaxed);
    }
    static bool aggressiveIdleShrinkEnabled() {
        int ov = aggressiveIdleShrinkOverride_.load(std::memory_order_relaxed);
        if (ov >= 0)
            return ov != 0;
        if (const char* s = std::getenv("YAMS_AGGRESSIVE_IDLE_SHRINK")) {
            std::string v(s);
            for (auto& c : v)
                c = static_cast<char>(std::tolower(c));
            return (v == "1" || v == "true" || v == "on");
        }
        return false;
    }
    static void setAggressiveIdleShrinkEnabled(bool en) {
        aggressiveIdleShrinkOverride_.store(en ? 1 : 0, std::memory_order_relaxed);
    }
    static uint32_t poolCooldownMs() {
        uint32_t ov = poolCooldownMsOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        uint32_t def = 500;
        switch (tuningProfile()) {
            case Profile::Efficient:
                def = 750;
                break;
            case Profile::Aggressive:
                def = 250;
                break;
            case Profile::Balanced:
            default:
                def = 500;
                break;
        }
        if (const char* s = std::getenv("YAMS_POOL_COOLDOWN_MS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                // v is unsigned; the lower-bound check is redundant on some compilers
                if (v <= 60000)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }
    static void setPoolCooldownMs(uint32_t ms) {
        poolCooldownMsOverride_.store(ms, std::memory_order_relaxed);
    }
    static int poolScaleStep() {
        int ov = poolScaleStepOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        int def = 1;
        if (const char* s = std::getenv("YAMS_POOL_SCALE_STEP")) {
            try {
                int v = std::stoi(s);
                if (v >= 1 && v <= 16)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }
    static void setPoolScaleStep(int step) {
        poolScaleStepOverride_.store(step, std::memory_order_relaxed);
    }

    // Pool defaults (IPC CPU and IO pools)
    static uint32_t poolMinSizeIpc() {
        uint32_t ov = poolMinSizeIpcOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        uint32_t def = 1;
        if (const char* s = std::getenv("YAMS_POOL_IPC_MIN")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 1024)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }
    static void setPoolMinSizeIpc(uint32_t v) {
        poolMinSizeIpcOverride_.store(v, std::memory_order_relaxed);
    }
    static uint32_t poolMaxSizeIpc() {
        uint32_t ov = poolMaxSizeIpcOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        uint32_t def = 32;
        if (const char* s = std::getenv("YAMS_POOL_IPC_MAX")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 4096)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }
    static void setPoolMaxSizeIpc(uint32_t v) {
        poolMaxSizeIpcOverride_.store(v, std::memory_order_relaxed);
    }
    static uint32_t poolMinSizeIpcIo() {
        uint32_t ov = poolMinSizeIpcIoOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        uint32_t def = 1;
        if (const char* s = std::getenv("YAMS_POOL_IO_MIN")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 1024)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }
    static void setPoolMinSizeIpcIo(uint32_t v) {
        poolMinSizeIpcIoOverride_.store(v, std::memory_order_relaxed);
    }
    static uint32_t poolMaxSizeIpcIo() {
        uint32_t ov = poolMaxSizeIpcIoOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        uint32_t def = 32;
        if (const char* s = std::getenv("YAMS_POOL_IO_MAX")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 4096)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }
    static void setPoolMaxSizeIpcIo(uint32_t v) {
        poolMaxSizeIpcIoOverride_.store(v, std::memory_order_relaxed);
    }
    static uint32_t poolLowWatermarkPercent() {
        uint32_t ov = poolLowWatermarkPctOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        uint32_t def = 25;
        if (const char* s = std::getenv("YAMS_POOL_LOW_WATERMARK_PCT")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v <= 100)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }
    static void setPoolLowWatermarkPercent(uint32_t v) {
        poolLowWatermarkPctOverride_.store(v, std::memory_order_relaxed);
    }
    static uint32_t poolHighWatermarkPercent() {
        uint32_t ov = poolHighWatermarkPctOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        uint32_t def = 85;
        if (const char* s = std::getenv("YAMS_POOL_HIGH_WATERMARK_PCT")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v <= 100)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }
    static void setPoolHighWatermarkPercent(uint32_t v) {
        poolHighWatermarkPctOverride_.store(v, std::memory_order_relaxed);
    }

    // -------- Connection slot dynamic sizing (PBI-085) --------
    // Minimum connection slots (floor for dynamic resizing). Default 64.
    // Environment: YAMS_CONN_SLOTS_MIN (range 1..1024)
    static uint32_t connectionSlotsMin() {
        uint32_t ov = connectionSlotsMinOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        uint32_t def = 64;
        if (const char* s = std::getenv("YAMS_CONN_SLOTS_MIN")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 1024)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }
    static void setConnectionSlotsMin(uint32_t v) {
        connectionSlotsMinOverride_.store(v, std::memory_order_relaxed);
    }

    // Maximum connection slots (ceiling for dynamic resizing). Default 4096.
    // Environment: YAMS_CONN_SLOTS_MAX (range 64..16384)
    static uint32_t connectionSlotsMax() {
        uint32_t ov = connectionSlotsMaxOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        uint32_t def = 4096;
        if (const char* s = std::getenv("YAMS_CONN_SLOTS_MAX")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 64 && v <= 16384)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }
    static void setConnectionSlotsMax(uint32_t v) {
        connectionSlotsMaxOverride_.store(v, std::memory_order_relaxed);
    }

    // Scale step for connection slot resizing. Default 16.
    // Environment: YAMS_CONN_SLOTS_STEP (range 1..128)
    static uint32_t connectionSlotsScaleStep() {
        uint32_t ov = connectionSlotsScaleStepOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        uint32_t def = 16;
        if (const char* s = std::getenv("YAMS_CONN_SLOTS_STEP")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 128)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }
    static void setConnectionSlotsScaleStep(uint32_t v) {
        connectionSlotsScaleStepOverride_.store(v, std::memory_order_relaxed);
    }

    // Initial/target connection slots based on hardware and profile.
    // Formula: recommendedThreads * ioConnPerThread * 4 * (0.5 + profileScale)
    // With minimum of 256 slots. Profile-scaled: Efficient=lower, Aggressive=higher.
    static uint32_t connectionSlotsTarget() {
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
    static void setConnectionSlotsTarget(uint32_t v) {
        connectionSlotsTargetOverride_.store(v, std::memory_order_relaxed);
    }

    static uint32_t searchConcurrencyLimit() {
        uint32_t ov = searchConcurrencyOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        if (const char* s = std::getenv("YAMS_SEARCH_MAX_CONCURRENT")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 512)
                    return v;
            } catch (...) {
            }
        }
        auto derived = std::max<uint32_t>(2, recommendedThreads(0.5));
        return derived * 2;
    }
    static void setSearchConcurrencyLimit(uint32_t v) {
        searchConcurrencyOverride_.store(v, std::memory_order_relaxed);
    }

    // Writer drain ramp thresholds and multipliers
    static uint32_t writerActiveLow1Threshold() { return 2; }
    static uint32_t writerActiveLow2Threshold() { return 4; }
    static uint32_t writerActiveHigh1Threshold() { return 8; }
    static uint32_t writerActiveHigh2Threshold() { return 32; }
    static double writerScaleActiveLow1Mul() { return 2.0; }
    static double writerScaleActiveLow2Mul() { return 1.5; }
    static double writerScaleActiveHigh1Mul() { return 2.0; }
    static double writerScaleActiveHigh2Mul() { return 2.0; }
    static double writerQueuedHalfThresholdFraction() { return 0.5; }
    static double writerQueuedThreeQuarterThresholdFraction() { return 0.75; }
    static double writerScaleQueuedHalfMul() { return 1.5; }
    static double writerScaleQueuedThreeQuarterMul() { return 2.0; }

    // Streaming page sizing thresholds and clamps
    static std::uint64_t streamMuxVeryHighBytes() { return 256ull * 1024ull * 1024ull; }
    static std::uint64_t streamMuxHighBytes() { return 128ull * 1024ull * 1024ull; }
    static std::uint64_t streamMuxLight1Bytes() { return 8ull * 1024ull * 1024ull; }
    static std::uint64_t streamMuxLight2Bytes() { return 32ull * 1024ull * 1024ull; }
    static std::uint64_t streamMuxLight3Bytes() { return 64ull * 1024ull * 1024ull; }
    static double streamPageFactorVeryHighDiv() { return 0.25; } // divide by 4
    static double streamPageFactorHighDiv() { return 0.5; }      // divide by 2
    static double streamPageFactorLight1Mul() { return 3.0; }
    static double streamPageFactorLight2Mul() { return 2.0; }
    static double streamPageFactorLight3Mul() { return 1.5; }
    static std::size_t streamPageClampMin() { return 5; }
    static std::size_t streamPageClampMax() { return 50000; }

    // General mux backlog fallback when no cap configured
    static std::uint64_t muxBacklogHighFallbackBytes() {
        std::uint64_t def = 64ull * 1024ull * 1024ull; // 64 MiB
        if (const char* s = std::getenv("YAMS_MUX_HIGH_FALLBACK_BYTES")) {
            try {
                return static_cast<std::uint64_t>(std::stoull(s));
            } catch (...) {
            }
        }
        return def;
    }

    // IO: desired average connections per thread before scaling up IO pool.
    // Default 8; override via YAMS_IO_CONN_PER_THREAD (range 1..1024).
    static uint32_t ioConnPerThread() {
        uint32_t ov = ioConnPerThreadOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        uint32_t def = 8;
        if (const char* s = std::getenv("YAMS_IO_CONN_PER_THREAD")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 1024)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }
    static void setIoConnPerThread(uint32_t v) {
        ioConnPerThreadOverride_.store(v, std::memory_order_relaxed);
    }

    static bool enableParallelIngest() {
        int ov = enableParallelIngestOverride_.load(std::memory_order_relaxed);
        if (ov >= 0)
            return ov > 0;
        if (const char* s = std::getenv("YAMS_ENABLE_PARALLEL_INGEST")) {
            std::string v{s};
            std::transform(v.begin(), v.end(), v.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            if (v == "0" || v == "false" || v == "off" || v == "no")
                return false;
            return true;
        }
        return true;
    }
    static void setEnableParallelIngest(bool en) {
        enableParallelIngestOverride_.store(en ? 1 : 0, std::memory_order_relaxed);
    }

    static uint32_t maxIngestWorkers() {
        uint32_t ov = maxIngestWorkersOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_INDEXING_WORKERS_MAX")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1)
                    return v;
            } catch (...) {
            }
        }
        uint32_t hw = static_cast<uint32_t>(std::thread::hardware_concurrency());
        if (hw == 0)
            hw = 1;
        return hw;
    }
    static void setMaxIngestWorkers(uint32_t v) {
        maxIngestWorkersOverride_.store(v, std::memory_order_relaxed);
    }

    static uint32_t storagePoolSize() {
        uint32_t ov = storagePoolSizeOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_STORAGE_POOL_SIZE")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1)
                    return v;
            } catch (...) {
            }
        }
        return 0;
    }
    static void setStoragePoolSize(uint32_t v) {
        storagePoolSizeOverride_.store(v, std::memory_order_relaxed);
    }

    static uint32_t ingestBacklogPerWorker() {
        uint32_t ov = ingestBacklogPerWorkerOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_INGEST_BACKLOG_PER_WORKER")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1)
                    return v;
            } catch (...) {
            }
        }
        return 32;
    }
    static void setIngestBacklogPerWorker(uint32_t v) {
        ingestBacklogPerWorkerOverride_.store(v == 0 ? 1 : v, std::memory_order_relaxed);
    }

    // Internal Event Bus toggles (config-driven)
    static bool useInternalBusForRepair() {
        return useInternalBusRepair_.load(std::memory_order_relaxed);
    }
    static void setUseInternalBusForRepair(bool en) {
        useInternalBusRepair_.store(en, std::memory_order_relaxed);
    }
    static bool useInternalBusForPostIngest() {
        return useInternalBusPostIngest_.load(std::memory_order_relaxed);
    }
    static void setUseInternalBusForPostIngest(bool en) {
        useInternalBusPostIngest_.store(en, std::memory_order_relaxed);
    }

    // =========================================================================
    // PBI-089: Request Queue and IOCoordinator Configuration
    // =========================================================================

    /// Maximum request queue size (default 4096)
    /// Environment: YAMS_REQUEST_QUEUE_SIZE
    static uint32_t requestQueueSize() {
        uint32_t ov = requestQueueSizeOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_REQUEST_QUEUE_SIZE")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 16 && v <= 65536)
                    return v;
            } catch (...) {
            }
        }
        return 4096;
    }
    static void setRequestQueueSize(uint32_t v) {
        requestQueueSizeOverride_.store(v, std::memory_order_relaxed);
    }

    /// Queue high watermark percentage (default 80%)
    /// Environment: YAMS_QUEUE_HIGH_WATERMARK
    static uint32_t queueHighWatermarkPercent() {
        uint32_t ov = queueHighWatermarkOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_QUEUE_HIGH_WATERMARK")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 10 && v <= 99)
                    return v;
            } catch (...) {
            }
        }
        return 80;
    }
    static void setQueueHighWatermarkPercent(uint32_t v) {
        queueHighWatermarkOverride_.store(v, std::memory_order_relaxed);
    }

    /// Queue low watermark percentage (default 20%)
    /// Environment: YAMS_QUEUE_LOW_WATERMARK
    static uint32_t queueLowWatermarkPercent() {
        uint32_t ov = queueLowWatermarkOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_QUEUE_LOW_WATERMARK")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 50)
                    return v;
            } catch (...) {
            }
        }
        return 20;
    }
    static void setQueueLowWatermarkPercent(uint32_t v) {
        queueLowWatermarkOverride_.store(v, std::memory_order_relaxed);
    }

    /// Request timeout in queue (default 30000ms)
    /// Environment: YAMS_REQUEST_TIMEOUT_MS
    static uint32_t requestQueueTimeoutMs() {
        uint32_t ov = requestQueueTimeoutMsOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_REQUEST_TIMEOUT_MS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1000 && v <= 300000)
                    return v;
            } catch (...) {
            }
        }
        return 30000;
    }
    static void setRequestQueueTimeoutMs(uint32_t v) {
        requestQueueTimeoutMsOverride_.store(v, std::memory_order_relaxed);
    }

    /// Number of dedicated I/O threads (default 2)
    /// Environment: YAMS_IO_THREADS
    static uint32_t ioThreadCount() {
        uint32_t ov = ioThreadCountOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_IO_THREADS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 16)
                    return v;
            } catch (...) {
            }
        }
        return 2;
    }
    static void setIoThreadCount(uint32_t v) {
        ioThreadCountOverride_.store(v, std::memory_order_relaxed);
    }

    /// Whether to enable priority queuing (default true)
    /// Environment: YAMS_ENABLE_PRIORITY_QUEUE
    static bool enablePriorityQueue() {
        int ov = enablePriorityQueueOverride_.load(std::memory_order_relaxed);
        if (ov >= 0)
            return ov > 0;
        if (const char* s = std::getenv("YAMS_ENABLE_PRIORITY_QUEUE")) {
            std::string v{s};
            std::transform(v.begin(), v.end(), v.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            if (v == "0" || v == "false" || v == "off" || v == "no")
                return false;
            return true;
        }
        return true;
    }
    static void setEnablePriorityQueue(bool en) {
        enablePriorityQueueOverride_.store(en ? 1 : 0, std::memory_order_relaxed);
    }

    static uint32_t maxIdleTimeouts() {
        uint32_t ov = maxIdleTimeoutsOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_MAX_IDLE_TIMEOUTS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 100)
                    return v;
            } catch (...) {
            }
        }
        return 3;
    }
    static void setMaxIdleTimeouts(uint32_t v) {
        maxIdleTimeoutsOverride_.store(v, std::memory_order_relaxed);
    }

    static uint32_t requestQueueDepth() {
        return requestQueueDepth_.load(std::memory_order_relaxed);
    }
    static void setRequestQueueDepth(uint32_t v) {
        requestQueueDepth_.store(v, std::memory_order_relaxed);
    }

    static bool requestQueueBackpressure() {
        return requestQueueBackpressure_.load(std::memory_order_relaxed);
    }
    static void setRequestQueueBackpressure(bool v) {
        requestQueueBackpressure_.store(v, std::memory_order_relaxed);
    }

    static uint32_t checkpointIntervalSeconds() {
        uint32_t ov = checkpointIntervalSecondsOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_CHECKPOINT_INTERVAL_SECONDS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 10 && v <= 3600)
                    return v;
            } catch (...) {
            }
        }
        return 300;
    }
    static void setCheckpointIntervalSeconds(uint32_t v) {
        checkpointIntervalSecondsOverride_.store(v, std::memory_order_relaxed);
    }

    static uint32_t checkpointInsertThreshold() {
        uint32_t ov = checkpointInsertThresholdOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_CHECKPOINT_INSERT_THRESHOLD")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 100000)
                    return v;
            } catch (...) {
            }
        }
        return 1000;
    }
    static void setCheckpointInsertThreshold(uint32_t v) {
        checkpointInsertThresholdOverride_.store(v, std::memory_order_relaxed);
    }

    static bool enableHotzoneCheckpoint() {
        int ov = enableHotzoneCheckpointOverride_.load(std::memory_order_relaxed);
        if (ov >= 0)
            return ov > 0;
        if (const char* s = std::getenv("YAMS_ENABLE_HOTZONE_PERSISTENCE")) {
            std::string v{s};
            std::transform(v.begin(), v.end(), v.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            if (v == "1" || v == "true" || v == "on" || v == "yes")
                return true;
            return false;
        }
        return false;
    }
    static void setEnableHotzoneCheckpoint(bool en) {
        enableHotzoneCheckpointOverride_.store(en ? 1 : 0, std::memory_order_relaxed);
    }

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

    static void setPostIngestStageActive(PostIngestStage stage, bool active) {
        const uint32_t bit = 1u << static_cast<uint8_t>(stage);
        uint32_t mask = postIngestStageActiveMaskOverride_.load(std::memory_order_relaxed);
        if (active) {
            mask |= bit;
        } else {
            mask &= ~bit;
        }
        postIngestStageActiveMaskOverride_.store(mask, std::memory_order_relaxed);
    }
    static uint32_t postIngestStageActiveMask() {
        uint32_t mask = postIngestStageActiveMaskOverride_.load(std::memory_order_relaxed);
        if (mask == 0)
            return 0x3Fu;
        return mask;
    }

    /// Total post-ingest concurrency budget (shared across stages).
    /// Default uses cpuBudgetPercent() via recommendedThreads().
    /// Environment: YAMS_POST_INGEST_TOTAL_CONCURRENT
    static uint32_t postIngestTotalConcurrent() {
        uint32_t ov = postIngestTotalConcurrentOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_POST_INGEST_TOTAL_CONCURRENT")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 256)
                    return v;
            } catch (...) {
            }
        }
        uint32_t hw = hardwareConcurrency();

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
        if (activeStages > 0) {
            total = std::max(total, activeStages);
        }

        return std::clamp(total, 2u, hw);
    }
    static void setPostIngestTotalConcurrent(uint32_t v) {
        if (v == 0) {
            postIngestTotalConcurrentOverride_.store(0u, std::memory_order_relaxed);
            return;
        }
        postIngestTotalConcurrentOverride_.store(std::clamp(v, 1u, 256u),
                                                 std::memory_order_relaxed);
    }

    /// Maximum concurrent extraction tasks (profile-scaled, max 64)
    /// Profile-scaled: Efficient=2, Balanced=3, Aggressive=4
    /// Environment: YAMS_POST_EXTRACTION_CONCURRENT
    static uint32_t postExtractionConcurrent() {
        return postIngestBudgetedConcurrency().extraction;
    }
    static void setPostExtractionConcurrent(uint32_t v) {
        postExtractionConcurrentOverride_.store(std::min(v, 64u), std::memory_order_relaxed);
    }

    /// Maximum concurrent KG ingestion tasks (profile-scaled, max 64)
    /// Profile-scaled: Efficient=4, Balanced=6, Aggressive=8
    /// Environment: YAMS_POST_KG_CONCURRENT
    static uint32_t postKgConcurrent() { return postIngestBudgetedConcurrency().kg; }
    static void setPostKgConcurrent(uint32_t v) {
        postKgConcurrentOverride_.store(std::min(v, 64u), std::memory_order_relaxed);
    }

    /// Maximum concurrent symbol extraction tasks (profile-scaled, max 32)
    /// Profile-scaled: Efficient=2, Balanced=3, Aggressive=4
    /// Environment: YAMS_POST_SYMBOL_CONCURRENT
    static uint32_t postSymbolConcurrent() { return postIngestBudgetedConcurrency().symbol; }
    static void setPostSymbolConcurrent(uint32_t v) {
        postSymbolConcurrentOverride_.store(std::min(v, 32u), std::memory_order_relaxed);
    }

    /// Maximum concurrent entity extraction tasks (profile-scaled, max 16)
    /// Entity extraction is CPU-heavy, so lower defaults
    /// Profile-scaled: Efficient=1, Balanced=2, Aggressive=2
    /// Environment: YAMS_POST_ENTITY_CONCURRENT
    static uint32_t postEntityConcurrent() { return postIngestBudgetedConcurrency().entity; }
    static void setPostEntityConcurrent(uint32_t v) {
        postEntityConcurrentOverride_.store(std::min(v, 16u), std::memory_order_relaxed);
    }

    /// Maximum concurrent title extraction tasks (profile-scaled, max 16)
    /// Environment: YAMS_POST_TITLE_CONCURRENT
    static uint32_t postTitleConcurrent() { return postIngestBudgetedConcurrency().title; }
    static void setPostTitleConcurrent(uint32_t v) {
        postTitleConcurrentOverride_.store(std::min(v, 16u), std::memory_order_relaxed);
    }

    // PBI-05b: EmbeddingService concurrency (parallel embedding workers)
    // Embeddings are compute-heavy (ONNX inference) so we need parallelism to keep up with ingest
    // Profile-scaled: Efficient=2, Balanced=3, Aggressive=4
    static uint32_t postEmbedConcurrent() { return postIngestBudgetedConcurrency().embed; }
    static void setPostEmbedConcurrent(uint32_t v) {
        postEmbedConcurrentOverride_.store(std::min(v, 32u), std::memory_order_relaxed);
    }

    // =========================================================================
    // ONNX Model Pool Sizing (GPU-aware)
    // =========================================================================

    /// Maximum concurrent ONNX sessions per model.
    /// Adjusted based on whether GPU is available:
    ///   GPU mode: max(2, min(hw_threads/2, 8)) - high throughput, GPU handles inference
    ///   CPU mode: max(1, min(hw_threads/4, 4)) - conservative to avoid CPU saturation
    /// Environment: YAMS_ONNX_SESSIONS_PER_MODEL
    static uint32_t onnxSessionsPerModel(bool gpuEnabled) {
        uint32_t ov = onnxSessionsPerModelOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_ONNX_SESSIONS_PER_MODEL")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 32)
                    return v;
            } catch (...) {
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
    static void setOnnxSessionsPerModel(uint32_t v) {
        onnxSessionsPerModelOverride_.store(std::clamp(v, 1u, 32u), std::memory_order_relaxed);
    }

    /// Whether ONNX pool is currently in startup mode (reduced resources during warmup).
    /// Set to true during initialization, cleared after preload completes.
    static bool onnxStartupMode() { return onnxStartupMode_.load(std::memory_order_relaxed); }
    static void setOnnxStartupMode(bool v) { onnxStartupMode_.store(v, std::memory_order_relaxed); }

    // Get the current embed channel capacity (for sizing the queue)
    static uint32_t embedChannelCapacity() {
        uint32_t ov = embedChannelCapacityOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* val = std::getenv("YAMS_EMBED_CHANNEL_CAPACITY")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(val));
                if (v >= 256 && v <= 65536)
                    return v;
            } catch (...) {
            }
        }
        return 8192; // Increased from 2048 to handle bulk ingest
    }
    static void setEmbedChannelCapacity(uint32_t v) {
        embedChannelCapacityOverride_.store(std::clamp(v, 256u, 65536u), std::memory_order_relaxed);
    }

    // =========================================================================
    // DB Contention Management (adaptive concurrency based on lock errors)
    // =========================================================================

    /// SQLite connection pool minimum size (default 4)
    /// Environment: YAMS_DB_POOL_MIN
    static uint32_t dbConnectionPoolMin() {
        uint32_t ov = dbPoolMinOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_DB_POOL_MIN")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 64)
                    return v;
            } catch (...) {
            }
        }
        return 4;
    }
    static void setDbConnectionPoolMin(uint32_t v) {
        dbPoolMinOverride_.store(std::clamp(v, 1u, 64u), std::memory_order_relaxed);
    }

    /// SQLite connection pool maximum size (default 20)
    /// Environment: YAMS_DB_POOL_MAX
    static uint32_t dbConnectionPoolMax() {
        uint32_t ov = dbPoolMaxOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_DB_POOL_MAX")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 128)
                    return v;
            } catch (...) {
            }
        }
        return 20;
    }
    static void setDbConnectionPoolMax(uint32_t v) {
        dbPoolMaxOverride_.store(std::clamp(v, 1u, 128u), std::memory_order_relaxed);
    }

    /// SQLite busy timeout in milliseconds (default 30000)
    /// Environment: YAMS_DB_BUSY_TIMEOUT_MS
    static uint32_t dbBusyTimeoutMs() {
        uint32_t ov = dbBusyTimeoutMsOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_DB_BUSY_TIMEOUT_MS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1000 && v <= 120000)
                    return v;
            } catch (...) {
            }
        }
        return 30000;
    }
    static void setDbBusyTimeoutMs(uint32_t v) {
        dbBusyTimeoutMsOverride_.store(std::clamp(v, 1000u, 120000u), std::memory_order_relaxed);
    }

    /// Lock error threshold for scaling down concurrency (default 5)
    /// When dbLockErrorsWindow exceeds this, TuningManager reduces KG/embed concurrency
    /// Environment: YAMS_DB_LOCK_THRESHOLD
    static uint32_t dbLockErrorThreshold() {
        uint32_t ov = dbLockThresholdOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_DB_LOCK_THRESHOLD")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 100)
                    return v;
            } catch (...) {
            }
        }
        return 5;
    }
    static void setDbLockErrorThreshold(uint32_t v) {
        dbLockThresholdOverride_.store(std::clamp(v, 1u, 100u), std::memory_order_relaxed);
    }

    /// Increment DB lock error counter (call this when "database is locked" error occurs)
    static void reportDbLockError() { dbLockErrorCount_.fetch_add(1, std::memory_order_relaxed); }

    /// Get and reset DB lock error window count (called by TuningManager per tick)
    static uint64_t getAndResetDbLockErrors() {
        return dbLockErrorCount_.exchange(0, std::memory_order_relaxed);
    }

    /// Get current DB lock error count (for monitoring)
    static uint64_t dbLockErrorCount() { return dbLockErrorCount_.load(std::memory_order_relaxed); }

private:
    struct PostIngestConcurrencyBudget {
        uint32_t extraction;
        uint32_t kg;
        uint32_t symbol;
        uint32_t entity;
        uint32_t title;
        uint32_t embed;
    };

    static PostIngestConcurrencyBudget postIngestBudgetedConcurrency() {
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
            if (const char* s = std::getenv(env)) {
                try {
                    uint32_t v = static_cast<uint32_t>(std::stoul(s));
                    if (v >= 1 && v <= maxCap)
                        return v;
                } catch (...) {
                }
            }
            return std::nullopt;
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
                double ratio = static_cast<double>(totalBudget) /
                               static_cast<double>(std::max<uint32_t>(1, used));
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
                std::sort(reduceOrder.begin(), reduceOrder.end(),
                          [&](std::size_t a, std::size_t b) {
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
        if (auto v = resolveOverride(postKgConcurrentOverride_, "YAMS_POST_KG_CONCURRENT",
                                     kMaxCaps[1])) {
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

        auto alloc = hasOverride ? allocate(desired, caps, weights) : allocDefaults;
        return PostIngestConcurrencyBudget{alloc[0], alloc[1], alloc[2],
                                           alloc[3], alloc[4], alloc[5]};
    }

    // Runtime policy storage (single process); defaults chosen to reduce CPU when busy
    static inline std::atomic<AutoEmbedPolicy> autoEmbedPolicy_{AutoEmbedPolicy::Idle};
    static inline std::atomic<double> cpuIdlePct_{25.0};
    static inline std::atomic<double> cpuHighPct_{70.0};
    static inline std::atomic<std::uint64_t> muxHighBytes_{256ull * 1024ull * 1024ull};
    static inline std::atomic<double> embedSafety_{0.90};
    static inline std::atomic<std::size_t> embedDocCap_{0}; // 0 = no extra cap
    static inline std::atomic<unsigned> embedPauseMs_{0};   // 0 = no pause
    static inline std::atomic<uint32_t> postIngestThreads_{0};
    static inline std::atomic<uint32_t> mcpWorkerThreads_{0};
    static inline std::atomic<bool> kgBatchEdges_{true};
    static inline std::atomic<bool> kgBatchNodes_{true};
    static inline std::atomic<bool> analyzerUrls_{true};
    static inline std::atomic<bool> analyzerEmails_{true};
    static inline std::atomic<bool> analyzerFilePaths_{false};
    static inline std::atomic<std::size_t> maxEntitiesPerDoc_{32};

    // Overrides for config-driven tuning (0 or negative = unset)
    static inline std::atomic<uint32_t> backpressureReadPauseMsOverride_{0};
    static inline std::atomic<uint32_t> workerPollMsOverride_{0};
    static inline std::atomic<bool> workerPollMsPinned_{false};
    static inline std::atomic<double> idleCpuPctOverride_{-1.0};
    static inline std::atomic<std::uint64_t> idleMuxLowBytesOverride_{0};
    static inline std::atomic<uint32_t> idleShrinkHoldMsOverride_{0};
    static inline std::atomic<int> aggressiveIdleShrinkOverride_{-1};
    static inline std::atomic<uint32_t> poolCooldownMsOverride_{0};
    static inline std::atomic<int> poolScaleStepOverride_{0};
    static inline std::atomic<uint32_t> poolMinSizeIpcOverride_{0};
    static inline std::atomic<uint32_t> poolMaxSizeIpcOverride_{0};
    static inline std::atomic<uint32_t> poolMinSizeIpcIoOverride_{0};
    static inline std::atomic<uint32_t> poolMaxSizeIpcIoOverride_{0};
    static inline std::atomic<uint32_t> poolLowWatermarkPctOverride_{0};
    static inline std::atomic<uint32_t> poolHighWatermarkPctOverride_{0};
    static inline std::atomic<uint32_t> searchConcurrencyOverride_{0};
    static inline std::atomic<unsigned> hwCached_{0};
    static inline std::atomic<uint32_t> postIngestTotalConcurrentOverride_{0};
    static inline std::atomic<uint32_t> postIngestStageActiveMaskOverride_{0x3Fu};
    static inline std::atomic<uint32_t> postIngestQueueMaxOverride_{0};
    static inline std::atomic<uint32_t> postIngestBatchSizeOverride_{0};
    static inline std::atomic<uint32_t> ioConnPerThreadOverride_{0};
    static inline std::atomic<uint32_t> connectionSlotsMinOverride_{0};
    static inline std::atomic<uint32_t> connectionSlotsMaxOverride_{0};
    static inline std::atomic<uint32_t> connectionSlotsScaleStepOverride_{0};
    static inline std::atomic<uint32_t> connectionSlotsTargetOverride_{0};
    static inline std::atomic<int> enableParallelIngestOverride_{-1};
    static inline std::atomic<uint32_t> maxIngestWorkersOverride_{0};
    static inline std::atomic<uint32_t> storagePoolSizeOverride_{0};
    static inline std::atomic<uint32_t> ingestBacklogPerWorkerOverride_{0};
    // Defaults: prefer internal event bus by default; config/env can override
    static inline std::atomic<bool> useInternalBusRepair_{true};
    static inline std::atomic<bool> useInternalBusPostIngest_{true};

    // PBI-089: Request Queue and IOCoordinator overrides
    static inline std::atomic<uint32_t> requestQueueSizeOverride_{0};
    static inline std::atomic<uint32_t> queueHighWatermarkOverride_{0};
    static inline std::atomic<uint32_t> queueLowWatermarkOverride_{0};
    static inline std::atomic<uint32_t> requestQueueTimeoutMsOverride_{0};
    static inline std::atomic<uint32_t> ioThreadCountOverride_{0};
    static inline std::atomic<int> enablePriorityQueueOverride_{-1};
    static inline std::atomic<uint32_t> maxIdleTimeoutsOverride_{0};
    static inline std::atomic<uint32_t> streamChunkTimeoutMsOverride_{0};
    static inline std::atomic<uint32_t> requestQueueDepth_{0};
    static inline std::atomic<bool> requestQueueBackpressure_{false};

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

    // PBI-05b: EmbeddingService concurrency overrides
    static inline std::atomic<uint32_t> postEmbedConcurrentOverride_{0};
    static inline std::atomic<uint32_t> embedChannelCapacityOverride_{0};

    // ONNX Model Pool overrides
    static inline std::atomic<uint32_t> onnxSessionsPerModelOverride_{0};
    static inline std::atomic<bool> onnxStartupMode_{false};

    // DB contention management overrides
    static inline std::atomic<uint32_t> dbPoolMinOverride_{0};
    static inline std::atomic<uint32_t> dbPoolMaxOverride_{0};
    static inline std::atomic<uint32_t> dbBusyTimeoutMsOverride_{0};
    static inline std::atomic<uint32_t> dbLockThresholdOverride_{0};

    // DB lock error tracking (rolling window counter)
    static inline std::atomic<uint64_t> dbLockErrorCount_{0};

    // =========================================================================
    // Resource Governor Configuration (Memory Pressure Management)
    // =========================================================================

public:
    /// Enable/disable the resource governor. When disabled, no memory pressure
    /// monitoring or adaptive scaling occurs. Default: true.
    /// Environment: YAMS_ENABLE_RESOURCE_GOVERNOR
    static bool enableResourceGovernor() {
        int ov = enableResourceGovernorOverride_.load(std::memory_order_relaxed);
        if (ov >= 0)
            return ov > 0;
        if (const char* s = std::getenv("YAMS_ENABLE_RESOURCE_GOVERNOR")) {
            std::string v{s};
            std::transform(v.begin(), v.end(), v.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            if (v == "0" || v == "false" || v == "off" || v == "no")
                return false;
        }
        return true;
    }
    static void setEnableResourceGovernor(bool en) {
        enableResourceGovernorOverride_.store(en ? 1 : 0, std::memory_order_relaxed);
    }

    /// Enable proactive model eviction under memory pressure. Default: true.
    /// Environment: YAMS_PROACTIVE_EVICTION
    static bool enableProactiveEviction() {
        int ov = enableProactiveEvictionOverride_.load(std::memory_order_relaxed);
        if (ov >= 0)
            return ov > 0;
        if (const char* s = std::getenv("YAMS_PROACTIVE_EVICTION")) {
            std::string v{s};
            std::transform(v.begin(), v.end(), v.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            if (v == "0" || v == "false" || v == "off" || v == "no")
                return false;
        }
        return true;
    }
    static void setEnableProactiveEviction(bool en) {
        enableProactiveEvictionOverride_.store(en ? 1 : 0, std::memory_order_relaxed);
    }

    /// Enable admission control (refuse new work when at emergency pressure). Default: true.
    /// Environment: YAMS_ADMISSION_CONTROL
    static bool enableAdmissionControl() {
        int ov = enableAdmissionControlOverride_.load(std::memory_order_relaxed);
        if (ov >= 0)
            return ov > 0;
        if (const char* s = std::getenv("YAMS_ADMISSION_CONTROL")) {
            std::string v{s};
            std::transform(v.begin(), v.end(), v.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            if (v == "0" || v == "false" || v == "off" || v == "no")
                return false;
        }
        return true;
    }
    static void setEnableAdmissionControl(bool en) {
        enableAdmissionControlOverride_.store(en ? 1 : 0, std::memory_order_relaxed);
    }

    /// Memory budget in bytes. 0 = auto-detect based on profile:
    ///   Efficient:  60% system RAM
    ///   Balanced:   80% system RAM
    ///   Aggressive: 90% system RAM
    /// Environment: YAMS_MEMORY_BUDGET_BYTES
    static uint64_t memoryBudgetBytes() {
        uint64_t ov = memoryBudgetBytesOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_MEMORY_BUDGET_BYTES")) {
            try {
                uint64_t v = static_cast<uint64_t>(std::stoull(s));
                if (v >= 64ull * 1024ull * 1024ull) // min 64 MiB
                    return v;
            } catch (...) {
            }
        }
        // Auto-detect based on profile
        uint64_t systemMem = detectSystemMemory();
        switch (tuningProfile()) {
            case Profile::Efficient:
                return systemMem * 60 / 100;
            case Profile::Aggressive:
                return systemMem * 90 / 100;
            default:
                return systemMem * 80 / 100;
        }
    }
    static void setMemoryBudgetBytes(uint64_t bytes) {
        memoryBudgetBytesOverride_.store(bytes, std::memory_order_relaxed);
    }

    /// Memory warning threshold (0.0-1.0). Profile-adjusted defaults:
    ///   Efficient:  0.70 (70%)
    ///   Balanced:   0.75 (75%)
    ///   Aggressive: 0.80 (80%)
    /// Environment: YAMS_MEMORY_WARNING_PCT (0-100)
    static double memoryWarningThreshold() {
        double ov = memoryWarningPctOverride_.load(std::memory_order_relaxed);
        if (ov > 0.0)
            return ov;
        if (const char* s = std::getenv("YAMS_MEMORY_WARNING_PCT")) {
            try {
                double v = std::stod(s) / 100.0;
                if (v >= 0.5 && v <= 0.99)
                    return v;
            } catch (...) {
            }
        }
        return 0.70 + profileScale() * 0.10;
    }
    static void setMemoryWarningThreshold(double pct) {
        memoryWarningPctOverride_.store(pct, std::memory_order_relaxed);
    }

    /// Memory critical threshold (0.0-1.0). Profile-adjusted defaults:
    ///   Efficient:  0.85 (85%)
    ///   Balanced:   0.90 (90%)
    ///   Aggressive: 0.92 (92%)
    /// Environment: YAMS_MEMORY_CRITICAL_PCT (0-100)
    static double memoryCriticalThreshold() {
        double ov = memoryCriticalPctOverride_.load(std::memory_order_relaxed);
        if (ov > 0.0)
            return ov;
        if (const char* s = std::getenv("YAMS_MEMORY_CRITICAL_PCT")) {
            try {
                double v = std::stod(s) / 100.0;
                if (v >= 0.5 && v <= 0.99)
                    return v;
            } catch (...) {
            }
        }
        return 0.85 + profileScale() * 0.07;
    }
    static void setMemoryCriticalThreshold(double pct) {
        memoryCriticalPctOverride_.store(pct, std::memory_order_relaxed);
    }

    /// Memory emergency threshold (0.0-1.0). Profile-adjusted defaults:
    ///   Efficient:  0.92 (92%)
    ///   Balanced:   0.95 (95%)
    ///   Aggressive: 0.97 (97%)
    /// Environment: YAMS_MEMORY_EMERGENCY_PCT (0-100)
    static double memoryEmergencyThreshold() {
        double ov = memoryEmergencyPctOverride_.load(std::memory_order_relaxed);
        if (ov > 0.0)
            return ov;
        if (const char* s = std::getenv("YAMS_MEMORY_EMERGENCY_PCT")) {
            try {
                double v = std::stod(s) / 100.0;
                if (v >= 0.5 && v <= 0.99)
                    return v;
            } catch (...) {
            }
        }
        return 0.92 + profileScale() * 0.05;
    }
    static void setMemoryEmergencyThreshold(double pct) {
        memoryEmergencyPctOverride_.store(pct, std::memory_order_relaxed);
    }

    /// Hysteresis ticks before changing pressure level (each tick ~250ms).
    /// Prevents rapid oscillation between levels. Default: 2 (~500ms).
    /// Environment: YAMS_MEMORY_HYSTERESIS_TICKS
    static uint32_t memoryHysteresisTicks() {
        uint32_t ov = memoryHysteresisTicksOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_MEMORY_HYSTERESIS_TICKS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 20)
                    return v;
            } catch (...) {
            }
        }
        return 2;
    }
    static void setMemoryHysteresisTicks(uint32_t ticks) {
        memoryHysteresisTicksOverride_.store(ticks, std::memory_order_relaxed);
    }

    /// Hysteresis duration before changing pressure level (milliseconds).
    /// Prevents rapid oscillation between levels. Default: 500ms.
    /// Environment: YAMS_MEMORY_HYSTERESIS_MS
    static uint32_t memoryHysteresisMs() {
        uint32_t ov = memoryHysteresisMsOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_MEMORY_HYSTERESIS_MS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 10 && v <= 10000)
                    return v;
            } catch (...) {
            }
        }
        return 500; // 500ms default
    }
    static void setMemoryHysteresisMs(uint32_t ms) {
        memoryHysteresisMsOverride_.store(ms, std::memory_order_relaxed);
    }

    /// Cooldown period between model evictions to prevent thrashing (ms). Default: 500.
    /// Environment: YAMS_MODEL_EVICTION_COOLDOWN_MS
    static uint32_t modelEvictionCooldownMs() {
        uint32_t ov = modelEvictionCooldownMsOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_MODEL_EVICTION_COOLDOWN_MS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 100 && v <= 10000)
                    return v;
            } catch (...) {
            }
        }
        return 500;
    }
    static void setModelEvictionCooldownMs(uint32_t ms) {
        modelEvictionCooldownMsOverride_.store(ms, std::memory_order_relaxed);
    }

    // =========================================================================
    // ONNX Concurrency Configuration (Global Slot Coordination)
    // =========================================================================

    /// Maximum concurrent ONNX operations (global across GLiNER, embeddings, reranking).
    /// 0 = auto (hw_threads/2, clamped 4-16). Default: auto.
    /// Environment: YAMS_ONNX_MAX_CONCURRENT
    static uint32_t onnxMaxConcurrent() {
        uint32_t ov = onnxMaxConcurrentOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_ONNX_MAX_CONCURRENT")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 64)
                    return v;
            } catch (...) {
            }
        }
        uint32_t hw = hardwareConcurrency();
        uint32_t reserved = onnxGlinerReserved() + onnxEmbedReserved() + onnxRerankerReserved();

        // Use round-up division to avoid integer truncation on small systems.
        uint32_t base = std::max(2u, (hw * 10 + 99) / 100);
        uint32_t scaleRange = std::max(1u, (hw * 15 + 99) / 100);
        uint32_t total = base + static_cast<uint32_t>(scaleRange * profileScale());
        // Ensure at least 1 shared slot beyond total reserved.
        total = std::max(total, reserved + 1);

        return std::clamp(total, 2u, 12u);
    }
    static void setOnnxMaxConcurrent(uint32_t n) {
        onnxMaxConcurrentOverride_.store(n, std::memory_order_relaxed);
    }

    /// Reserved ONNX slots for GLiNER operations (entity/title extraction).
    /// Guarantees GLiNER gets at least this many slots even under contention. Default: 1.
    /// Environment: YAMS_ONNX_GLINER_RESERVED
    static uint32_t onnxGlinerReserved() {
        uint32_t ov = onnxGlinerReservedOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_ONNX_GLINER_RESERVED")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v <= 8)
                    return v;
            } catch (...) {
            }
        }
        return 1;
    }
    static void setOnnxGlinerReserved(uint32_t n) {
        onnxGlinerReservedOverride_.store(n, std::memory_order_relaxed);
    }

    /// Reserved ONNX slots for embedding operations.
    /// Guarantees embeddings get at least this many slots even under contention. Default: 1.
    /// Environment: YAMS_ONNX_EMBED_RESERVED
    static uint32_t onnxEmbedReserved() {
        uint32_t ov = onnxEmbedReservedOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_ONNX_EMBED_RESERVED")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v <= 8)
                    return v;
            } catch (...) {
            }
        }
        return 1;
    }
    static void setOnnxEmbedReserved(uint32_t n) {
        onnxEmbedReservedOverride_.store(n, std::memory_order_relaxed);
    }

    /// Reserved ONNX slots for reranking operations. Default: 1.
    /// Environment: YAMS_ONNX_RERANKER_RESERVED
    static uint32_t onnxRerankerReserved() {
        uint32_t ov = onnxRerankerReservedOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_ONNX_RERANKER_RESERVED")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v <= 8)
                    return v;
            } catch (...) {
            }
        }
        // Efficient profile with tight budget: reranker gets no reserved slot
        if (profileScale() == 0.0)
            return 0;
        return 1;
    }
    static void setOnnxRerankerReserved(uint32_t n) {
        onnxRerankerReservedOverride_.store(n, std::memory_order_relaxed);
    }

    // =========================================================================
    // Model Idle Maintenance Thresholds (Profile-Aware)
    // =========================================================================

    /// Maximum active connections before skipping model idle maintenance.
    /// Profile-adjusted: Efficient=2, Balanced=1, Aggressive=0.
    /// Environment: YAMS_MODEL_MAINT_CONN_THRESHOLD
    static uint32_t modelMaintenanceConnThreshold() {
        uint32_t ov = modelMaintConnThresholdOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov - 1; // stored as value+1 to distinguish 0 from unset
        if (const char* s = std::getenv("YAMS_MODEL_MAINT_CONN_THRESHOLD")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v <= 100)
                    return v;
            } catch (...) {
            }
        }
        switch (tuningProfile()) {
            case Profile::Efficient:
                return 2;
            case Profile::Aggressive:
                return 0;
            default: // Balanced
                return 1;
        }
    }
    static void setModelMaintenanceConnThreshold(uint32_t n) {
        modelMaintConnThresholdOverride_.store(n + 1, std::memory_order_relaxed);
    }

    /// Maximum active searches before skipping model idle maintenance.
    /// Profile-adjusted: Efficient=2, Balanced=1, Aggressive=0.
    /// Environment: YAMS_MODEL_MAINT_SEARCH_THRESHOLD
    static uint32_t modelMaintenanceSearchThreshold() {
        uint32_t ov = modelMaintSearchThresholdOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov - 1;
        if (const char* s = std::getenv("YAMS_MODEL_MAINT_SEARCH_THRESHOLD")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v <= 100)
                    return v;
            } catch (...) {
            }
        }
        switch (tuningProfile()) {
            case Profile::Efficient:
                return 2;
            case Profile::Aggressive:
                return 0;
            default: // Balanced
                return 1;
        }
    }
    static void setModelMaintenanceSearchThreshold(uint32_t n) {
        modelMaintSearchThresholdOverride_.store(n + 1, std::memory_order_relaxed);
    }

    /// Maximum post-ingest queue depth before skipping model idle maintenance.
    /// Profile-adjusted: Efficient=20, Balanced=10, Aggressive=0.
    /// Environment: YAMS_MODEL_MAINT_QUEUE_THRESHOLD
    static uint32_t modelMaintenanceQueueThreshold() {
        uint32_t ov = modelMaintQueueThresholdOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov - 1;
        if (const char* s = std::getenv("YAMS_MODEL_MAINT_QUEUE_THRESHOLD")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v <= 10000)
                    return v;
            } catch (...) {
            }
        }
        switch (tuningProfile()) {
            case Profile::Efficient:
                return 20;
            case Profile::Aggressive:
                return 0;
            default: // Balanced
                return 10;
        }
    }
    static void setModelMaintenanceQueueThreshold(uint32_t n) {
        modelMaintQueueThresholdOverride_.store(n + 1, std::memory_order_relaxed);
    }

    // =========================================================================
    // Model Eviction Pressure Thresholds (Profile-Aware)
    // =========================================================================

    /// Pressure level to start warning-level model eviction (evict 1 model).
    /// Profile-adjusted: Efficient=0.30, Balanced=0.60, Aggressive=0.75.
    /// Environment: YAMS_MODEL_EVICT_WARNING_THRESHOLD
    static double modelEvictWarningThreshold() {
        double ov = modelEvictWarningOverride_.load(std::memory_order_relaxed);
        if (ov > 0.0)
            return ov;
        if (const char* s = std::getenv("YAMS_MODEL_EVICT_WARNING_THRESHOLD")) {
            try {
                double v = std::stod(s);
                if (v > 0.0 && v < 1.0)
                    return v;
            } catch (...) {
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
    static void setModelEvictWarningThreshold(double v) {
        if (v > 0.0 && v < 1.0 && std::isfinite(v))
            modelEvictWarningOverride_.store(v, std::memory_order_relaxed);
    }

    /// Pressure level for critical-level model eviction (evict 2 models).
    /// Profile-adjusted: Efficient=0.50, Balanced=0.75, Aggressive=0.85.
    /// Environment: YAMS_MODEL_EVICT_CRITICAL_THRESHOLD
    static double modelEvictCriticalThreshold() {
        double ov = modelEvictCriticalOverride_.load(std::memory_order_relaxed);
        if (ov > 0.0)
            return ov;
        if (const char* s = std::getenv("YAMS_MODEL_EVICT_CRITICAL_THRESHOLD")) {
            try {
                double v = std::stod(s);
                if (v > 0.0 && v < 1.0)
                    return v;
            } catch (...) {
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
    static void setModelEvictCriticalThreshold(double v) {
        if (v > 0.0 && v < 1.0 && std::isfinite(v))
            modelEvictCriticalOverride_.store(v, std::memory_order_relaxed);
    }

    /// Pressure level for emergency-level model eviction (evict all).
    /// Profile-adjusted: Efficient=0.70, Balanced=0.90, Aggressive=0.95.
    /// Environment: YAMS_MODEL_EVICT_EMERGENCY_THRESHOLD
    static double modelEvictEmergencyThreshold() {
        double ov = modelEvictEmergencyOverride_.load(std::memory_order_relaxed);
        if (ov > 0.0)
            return ov;
        if (const char* s = std::getenv("YAMS_MODEL_EVICT_EMERGENCY_THRESHOLD")) {
            try {
                double v = std::stod(s);
                if (v > 0.0 && v < 1.0)
                    return v;
            } catch (...) {
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
    static void setModelEvictEmergencyThreshold(double v) {
        if (v > 0.0 && v < 1.0 && std::isfinite(v))
            modelEvictEmergencyOverride_.store(v, std::memory_order_relaxed);
    }

    /// Clear all eviction threshold overrides, restoring profile/env var defaults.
    /// Primarily intended for testing to ensure test isolation.
    static void resetModelEvictThresholdOverrides() {
        modelEvictWarningOverride_.store(0.0, std::memory_order_relaxed);
        modelEvictCriticalOverride_.store(0.0, std::memory_order_relaxed);
        modelEvictEmergencyOverride_.store(0.0, std::memory_order_relaxed);
    }

private:
    /// Detect system memory (cross-platform). Returns bytes.
    /// Implementation uses platform-specific APIs:
    ///   Windows: GlobalMemoryStatusEx
    ///   macOS: sysctlbyname("hw.memsize")
    ///   Linux: /proc/meminfo
    static uint64_t detectSystemMemory();

    // Resource Governor overrides
    static inline std::atomic<int> enableResourceGovernorOverride_{-1};
    static inline std::atomic<int> enableProactiveEvictionOverride_{-1};
    static inline std::atomic<int> enableAdmissionControlOverride_{-1};
    static inline std::atomic<uint64_t> memoryBudgetBytesOverride_{0};
    static inline std::atomic<double> memoryWarningPctOverride_{0.0};
    static inline std::atomic<double> memoryCriticalPctOverride_{0.0};
    static inline std::atomic<double> memoryEmergencyPctOverride_{0.0};
    static inline std::atomic<uint32_t> memoryHysteresisTicksOverride_{0};
    static inline std::atomic<uint32_t> memoryHysteresisMsOverride_{0};
    static inline std::atomic<uint32_t> modelEvictionCooldownMsOverride_{0};

    // ONNX concurrency overrides
    static inline std::atomic<uint32_t> onnxMaxConcurrentOverride_{0};
    static inline std::atomic<uint32_t> onnxGlinerReservedOverride_{0};
    static inline std::atomic<uint32_t> onnxEmbedReservedOverride_{0};
    static inline std::atomic<uint32_t> onnxRerankerReservedOverride_{0};

    // Model idle maintenance threshold overrides (stored as value+1 to distinguish 0 from unset)
    static inline std::atomic<uint32_t> modelMaintConnThresholdOverride_{0};
    static inline std::atomic<uint32_t> modelMaintSearchThresholdOverride_{0};
    static inline std::atomic<uint32_t> modelMaintQueueThresholdOverride_{0};

    // Model eviction pressure threshold overrides
    static inline std::atomic<double> modelEvictWarningOverride_{0.0};
    static inline std::atomic<double> modelEvictCriticalOverride_{0.0};
    static inline std::atomic<double> modelEvictEmergencyOverride_{0.0};
};

} // namespace yams::daemon
