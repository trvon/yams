#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>

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

    // Metrics snapshot cache window (ms). Default 200 ms.
    static uint32_t metricsCacheMs() {
        uint32_t def = 200;
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

    // -------- Code-controlled worker sizing (no env steering) --------
    // When non-zero, components should prefer these values over heuristics.
    static uint32_t postIngestThreads() {
        return postIngestThreads_.load(std::memory_order_relaxed);
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
};

} // namespace yams::daemon
