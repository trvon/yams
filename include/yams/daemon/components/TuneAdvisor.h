#pragma once

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

    // Suggested maximum worker queue depth before backpressure (0=auto). Default auto: 2x threads.
    static uint64_t maxWorkerQueue(size_t workerThreads) {
        if (const char* s = std::getenv("YAMS_MAX_WORKER_QUEUE")) {
            try {
                return static_cast<uint64_t>(std::stoull(s));
            } catch (...) {
            }
        }
        if (workerThreads == 0)
            return 0; // unknown
        return static_cast<uint64_t>(workerThreads) * 2ull;
    }

    // Suggested mux queued-bytes budget before backpressure. Default 256 MiB.
    static uint64_t maxMuxBytes() {
        uint64_t def = 256ull * 1024ull * 1024ull;
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
    // Global CPU budget percent (10..100). Default 50.
    static uint32_t cpuBudgetPercent() {
        uint32_t def = 50;
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
        uint32_t configured = postIngestThreads_.load(std::memory_order_relaxed);
        if (configured != 0)
            return configured;
        // Background workers: prefer small cap (12.5% of CPU budget), hard max 4
        return TuneAdvisor::recommendedThreads(0.125, /*hardMax=*/4);
    }
    static void setPostIngestThreads(uint32_t n) {
        postIngestThreads_.store(n, std::memory_order_relaxed);
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
    // Backpressure read pause when receiver is backpressured (ms). Default 5.
    static uint32_t backpressureReadPauseMs() {
        uint32_t ov = backpressureReadPauseMsOverride_.load(std::memory_order_relaxed);
        if (ov != 0)
            return ov;
        uint32_t def = 5;
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
        if (const char* s = std::getenv("YAMS_POOL_COOLDOWN_MS")) {
            try {
                uint32_t v = static_cast<uint32_t>(std::stoul(s));
                if (v >= 0 && v <= 60000)
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
    static inline std::atomic<unsigned> hwCached_{0};
};

} // namespace yams::daemon
