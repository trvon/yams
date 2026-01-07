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
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <thread>

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
    // Efficient  -> 0.75 (slower growth, lower resource use)
    // Balanced   -> 1.0
    // Aggressive -> 1.5 (faster growth, lower thresholds)
    static double profileScale() {
        switch (tuningProfile()) {
            case Profile::Efficient:
                return 0.75;
            case Profile::Aggressive:
                return 1.5;
            case Profile::Balanced:
            default:
                return 1.0;
        }
    }

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

    static double cpuHighThresholdPercent() { return cpuHighPct_.load(std::memory_order_relaxed); }
    static void setCpuHighThresholdPercent(double v) {
        cpuHighPct_.store(v, std::memory_order_relaxed);
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
    static std::size_t embedDocCap() { return embedDocCap_.load(std::memory_order_relaxed); }
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

    // Status/metrics tick cadence for daemon main loop. Default 250 ms.
    static uint32_t statusTickMs() {
        uint32_t def = 250;
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
    // Max repair batch size per cycle. Default 32.
    static uint32_t repairMaxBatch() {
        uint32_t def = 32;
        if (const char* s = std::getenv("YAMS_REPAIR_MAX_BATCH")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v > 0 && v <= 1000)
                    return v;
            } catch (...) {
            }
        }
        return def;
    }

    // Batch size for repair operations during startup phase. Default 100.
    // Smaller batches reduce startup load. Normal operation uses repairMaxBatch (32).
    static uint32_t repairStartupBatchSize() { return 100; }

    // Duration of startup phase in ticks (each tick is statusTickMs, default 250ms).
    // During startup, smaller batch sizes are used to reduce load.
    // Default 20 ticks = 5 seconds (20 * 250ms = 5000ms).
    static uint32_t repairStartupDurationTicks() { return 20; }

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
        uint32_t def = 50;
        switch (tuningProfile()) {
            case Profile::Efficient:
                def = 40;
                break;
            case Profile::Aggressive:
                def = 80;
                break;
            case Profile::Balanced:
            default:
                def = 50;
                break;
        }
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
    static uint32_t postIngestBatchSize() {
        uint32_t ov = postIngestBatchSizeOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        if (const char* s = std::getenv("YAMS_POST_INGEST_BATCH_SIZE")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 256)
                    return v;
            } catch (...) {
            }
        }
        return 8;
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

    /// Maximum concurrent extraction tasks (default 4, max 64)
    /// Environment: YAMS_POST_EXTRACTION_CONCURRENT
    static uint32_t postExtractionConcurrent() {
        uint32_t ov = postExtractionConcurrentOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_POST_EXTRACTION_CONCURRENT")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 64)
                    return v;
            } catch (...) {
            }
        }
        return 4; // Default matches original kMaxConcurrent_
    }
    static void setPostExtractionConcurrent(uint32_t v) {
        postExtractionConcurrentOverride_.store(std::min(v, 64u), std::memory_order_relaxed);
    }

    /// Maximum concurrent KG ingestion tasks (default 8, max 64)
    /// Environment: YAMS_POST_KG_CONCURRENT
    static uint32_t postKgConcurrent() {
        uint32_t ov = postKgConcurrentOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_POST_KG_CONCURRENT")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 64)
                    return v;
            } catch (...) {
            }
        }
        return 8; // Default matches original kMaxKgConcurrent_
    }
    static void setPostKgConcurrent(uint32_t v) {
        postKgConcurrentOverride_.store(std::min(v, 64u), std::memory_order_relaxed);
    }

    /// Maximum concurrent symbol extraction tasks (default 4, max 32)
    /// Environment: YAMS_POST_SYMBOL_CONCURRENT
    static uint32_t postSymbolConcurrent() {
        uint32_t ov = postSymbolConcurrentOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_POST_SYMBOL_CONCURRENT")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 32)
                    return v;
            } catch (...) {
            }
        }
        return 4; // Default matches original kMaxSymbolConcurrent_
    }
    static void setPostSymbolConcurrent(uint32_t v) {
        postSymbolConcurrentOverride_.store(std::min(v, 32u), std::memory_order_relaxed);
    }

    /// Maximum concurrent entity extraction tasks (default 2, max 16)
    /// Entity extraction is heavy (binary analysis) so lower default
    /// Environment: YAMS_POST_ENTITY_CONCURRENT
    static uint32_t postEntityConcurrent() {
        uint32_t ov = postEntityConcurrentOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        if (const char* s = std::getenv("YAMS_POST_ENTITY_CONCURRENT")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 1 && v <= 16)
                    return v;
            } catch (...) {
            }
        }
        return 2; // Default matches original kMaxEntityConcurrent_
    }
    static void setPostEntityConcurrent(uint32_t v) {
        postEntityConcurrentOverride_.store(std::min(v, 16u), std::memory_order_relaxed);
    }

    // PBI-05b: EmbeddingService concurrency (parallel embedding workers)
    // Embeddings are compute-heavy (ONNX inference) so we need parallelism to keep up with ingest
    static uint32_t postEmbedConcurrent() {
        uint32_t ov = postEmbedConcurrentOverride_.load(std::memory_order_relaxed);
        if (ov > 0)
            return ov;
        // Check environment
        if (const char* val = std::getenv("YAMS_POST_EMBED_CONCURRENT")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(val));
                if (v >= 1 && v <= 32)
                    return v;
            } catch (...) {
            }
        }
        // Default: scale with hardware up to 8 workers
        // Embeddings are GPU/CPU intensive, so we cap lower than other stages
        uint32_t hw = hardwareConcurrency();
        return std::min<uint32_t>(std::max<uint32_t>(hw / 4, 2), 8);
    }
    static void setPostEmbedConcurrent(uint32_t v) {
        postEmbedConcurrentOverride_.store(std::min(v, 32u), std::memory_order_relaxed);
    }

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

private:
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
    static inline std::atomic<uint32_t> postIngestQueueMaxOverride_{0};
    static inline std::atomic<uint32_t> postIngestBatchSizeOverride_{0};
    static inline std::atomic<uint32_t> ioConnPerThreadOverride_{0};
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

    // PBI-05b: EmbeddingService concurrency overrides
    static inline std::atomic<uint32_t> postEmbedConcurrentOverride_{0};
    static inline std::atomic<uint32_t> embedChannelCapacityOverride_{0};
};

} // namespace yams::daemon
