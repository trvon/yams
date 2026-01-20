#pragma once

// FsmMetricsRegistry
// -------------------
// Centralized, daemon-scoped registry for light-weight aggregation of
// transport FSM metrics. Designed for very low overhead in hot paths.
// - Sharded counters to minimize contention (false sharing avoided with cache alignment)
// - Relaxed atomics for increment-only fast paths
// - Snapshot API to surface metrics (e.g., StatusResponse) with near-zero coordination
//
// Typical usage:
//   using yams::daemon::FsmMetricsRegistry;
//   auto& reg = FsmMetricsRegistry::instance();
//   reg.incrementTransitions();
//   reg.incrementHeaderReads();
//   reg.incrementPayloadReads();
//   reg.incrementPayloadWrites();
//   reg.addBytesSent(n);
//   reg.addBytesReceived(n);
//   auto snap = reg.snapshot();
//
// Notes:
// - Enabling/disabling collection is cheap. When disabled, increments are no-ops.
// - Snapshot sums all shards (O(num_shards)); intended for infrequent use (e.g., status).
// - Reset can be used for testing or epoch-based metrics windows if desired.
//
// Thread-safety: increments are wait-free and lock-free; snapshot/reset are safe with atomics.

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <type_traits>

namespace yams {
namespace daemon {

class FsmMetricsRegistry {
public:
    struct Snapshot {
        uint64_t transitions{0};
        uint64_t headerReads{0};
        uint64_t payloadReads{0};
        uint64_t payloadWrites{0};
        uint64_t bytesSent{0};
        uint64_t bytesReceived{0};
        uint32_t ipcPoolSize{0};
        uint32_t ioPoolSize{0};
        uint64_t writerBudgetBytes{0};

        // ResourceGovernor metrics (from TuningManager)
        uint64_t governorRssBytes{0};     // Current RSS
        uint64_t governorBudgetBytes{0};  // Memory budget
        uint8_t governorPressureLevel{0}; // 0=Normal, 1=Warning, 2=Critical, 3=Emergency
        uint8_t governorHeadroomPct{100}; // Scaling headroom (0-100%)

        // ONNX concurrency metrics
        uint32_t onnxTotalSlots{0};
        uint32_t onnxUsedSlots{0};
        uint32_t onnxGlinerUsed{0};
        uint32_t onnxEmbedUsed{0};
        uint32_t onnxRerankerUsed{0};

        // Merge another snapshot into this one
        inline void merge(const Snapshot& other) noexcept {
            transitions += other.transitions;
            headerReads += other.headerReads;
            payloadReads += other.payloadReads;
            payloadWrites += other.payloadWrites;
            bytesSent += other.bytesSent;
            bytesReceived += other.bytesReceived;
            ipcPoolSize = other.ipcPoolSize; // last-writer wins (snapshots are periodic)
            ioPoolSize = other.ioPoolSize;
            writerBudgetBytes = other.writerBudgetBytes;
            // Governor metrics - last-writer wins
            governorRssBytes = other.governorRssBytes;
            governorBudgetBytes = other.governorBudgetBytes;
            governorPressureLevel = other.governorPressureLevel;
            governorHeadroomPct = other.governorHeadroomPct;
            // ONNX metrics - last-writer wins
            onnxTotalSlots = other.onnxTotalSlots;
            onnxUsedSlots = other.onnxUsedSlots;
            onnxGlinerUsed = other.onnxGlinerUsed;
            onnxEmbedUsed = other.onnxEmbedUsed;
            onnxRerankerUsed = other.onnxRerankerUsed;
        }
    };

    // Singleton accessor (daemon-scoped)
    static inline FsmMetricsRegistry& instance() noexcept {
        static FsmMetricsRegistry g;
        return g;
    }

    // Configure whether metrics collection is enabled
    void setEnabled(bool enabled) noexcept { enabled_.store(enabled, std::memory_order_relaxed); }

    // Enable/disable metrics collection (disabled = fast no-op on increments)
    inline void set_enabled(bool on) noexcept { enabled_.store(on, std::memory_order_relaxed); }
    inline bool enabled() const noexcept { return enabled_.load(std::memory_order_relaxed); }

    // Incremental counters (fast-path; relaxed atomics)
    inline void incrementTransitions(uint64_t n = 1) noexcept {
        if (!enabled())
            return;
        shard().transitions.fetch_add(n, std::memory_order_relaxed);
    }
    inline void incrementHeaderReads(uint64_t n = 1) noexcept {
        if (!enabled())
            return;
        shard().headerReads.fetch_add(n, std::memory_order_relaxed);
    }
    inline void incrementPayloadReads(uint64_t n = 1) noexcept {
        if (!enabled())
            return;
        shard().payloadReads.fetch_add(n, std::memory_order_relaxed);
    }
    inline void incrementPayloadWrites(uint64_t n = 1) noexcept {
        if (!enabled())
            return;
        shard().payloadWrites.fetch_add(n, std::memory_order_relaxed);
    }
    inline void addBytesSent(uint64_t n) noexcept {
        if (!enabled())
            return;
        shard().bytesSent.fetch_add(n, std::memory_order_relaxed);
    }
    inline void addBytesReceived(uint64_t n) noexcept {
        if (!enabled())
            return;
        shard().bytesReceived.fetch_add(n, std::memory_order_relaxed);
    }

    // Aggregate a snapshot across shards (read-only)
    inline Snapshot snapshot() const noexcept {
        Snapshot out{};
        // Sum with acquire to ensure a consistent view of each shard's atomics
        for (const auto& s : shards_) {
            out.transitions += s.transitions.load(std::memory_order_acquire);
            out.headerReads += s.headerReads.load(std::memory_order_acquire);
            out.payloadReads += s.payloadReads.load(std::memory_order_acquire);
            out.payloadWrites += s.payloadWrites.load(std::memory_order_acquire);
            out.bytesSent += s.bytesSent.load(std::memory_order_acquire);
            out.bytesReceived += s.bytesReceived.load(std::memory_order_acquire);
        }
        out.ipcPoolSize = ipcPoolSize_.load(std::memory_order_acquire);
        out.ioPoolSize = ioPoolSize_.load(std::memory_order_acquire);
        out.writerBudgetBytes = writerBudgetBytes_.load(std::memory_order_acquire);
        // ResourceGovernor metrics
        out.governorRssBytes = governorRssBytes_.load(std::memory_order_acquire);
        out.governorBudgetBytes = governorBudgetBytes_.load(std::memory_order_acquire);
        out.governorPressureLevel = governorPressureLevel_.load(std::memory_order_acquire);
        out.governorHeadroomPct = governorHeadroomPct_.load(std::memory_order_acquire);
        // ONNX concurrency metrics
        out.onnxTotalSlots = onnxTotalSlots_.load(std::memory_order_acquire);
        out.onnxUsedSlots = onnxUsedSlots_.load(std::memory_order_acquire);
        out.onnxGlinerUsed = onnxGlinerUsed_.load(std::memory_order_acquire);
        out.onnxEmbedUsed = onnxEmbedUsed_.load(std::memory_order_acquire);
        out.onnxRerankerUsed = onnxRerankerUsed_.load(std::memory_order_acquire);
        return out;
    }

    // Reset all counters to zero (intended for tests or epoch rollover)
    inline void reset() noexcept {
        for (auto& s : shards_) {
            s.transitions.store(0, std::memory_order_release);
            s.headerReads.store(0, std::memory_order_release);
            s.payloadReads.store(0, std::memory_order_release);
            s.payloadWrites.store(0, std::memory_order_release);
            s.bytesSent.store(0, std::memory_order_release);
            s.bytesReceived.store(0, std::memory_order_release);
        }
        ipcPoolSize_.store(0, std::memory_order_release);
        ioPoolSize_.store(0, std::memory_order_release);
        writerBudgetBytes_.store(0, std::memory_order_release);
        // Reset ResourceGovernor metrics
        governorRssBytes_.store(0, std::memory_order_release);
        governorBudgetBytes_.store(0, std::memory_order_release);
        governorPressureLevel_.store(0, std::memory_order_release);
        governorHeadroomPct_.store(100, std::memory_order_release);
        // Reset ONNX metrics
        onnxTotalSlots_.store(0, std::memory_order_release);
        onnxUsedSlots_.store(0, std::memory_order_release);
        onnxGlinerUsed_.store(0, std::memory_order_release);
        onnxEmbedUsed_.store(0, std::memory_order_release);
        onnxRerankerUsed_.store(0, std::memory_order_release);
    }

    // Tuning/Pool stats exposed to FSM consumers (status/doctor)
    inline void setIpcPoolSize(uint32_t n) noexcept {
        ipcPoolSize_.store(n, std::memory_order_relaxed);
    }
    inline void setIoPoolSize(uint32_t n) noexcept {
        ioPoolSize_.store(n, std::memory_order_relaxed);
    }
    inline void setWriterBudgetBytes(uint64_t n) noexcept {
        writerBudgetBytes_.store(n, std::memory_order_relaxed);
    }

    // ResourceGovernor metrics setters
    inline void setGovernorRssBytes(uint64_t bytes) noexcept {
        governorRssBytes_.store(bytes, std::memory_order_relaxed);
    }
    inline void setGovernorBudgetBytes(uint64_t bytes) noexcept {
        governorBudgetBytes_.store(bytes, std::memory_order_relaxed);
    }
    inline void setGovernorPressureLevel(uint8_t level) noexcept {
        governorPressureLevel_.store(level, std::memory_order_relaxed);
    }
    inline void setGovernorHeadroomPct(uint8_t pct) noexcept {
        governorHeadroomPct_.store(pct, std::memory_order_relaxed);
    }

    // ONNX concurrency metrics setters
    inline void setOnnxTotalSlots(uint32_t n) noexcept {
        onnxTotalSlots_.store(n, std::memory_order_relaxed);
    }
    inline void setOnnxUsedSlots(uint32_t n) noexcept {
        onnxUsedSlots_.store(n, std::memory_order_relaxed);
    }
    inline void setOnnxGlinerUsed(uint32_t n) noexcept {
        onnxGlinerUsed_.store(n, std::memory_order_relaxed);
    }
    inline void setOnnxEmbedUsed(uint32_t n) noexcept {
        onnxEmbedUsed_.store(n, std::memory_order_relaxed);
    }
    inline void setOnnxRerankerUsed(uint32_t n) noexcept {
        onnxRerankerUsed_.store(n, std::memory_order_relaxed);
    }

private:
    // Cache-line-aligned shard to avoid false sharing under contention
    struct alignas(64) Shard {
        std::atomic<uint64_t> transitions{0};
        std::atomic<uint64_t> headerReads{0};
        std::atomic<uint64_t> payloadReads{0};
        std::atomic<uint64_t> payloadWrites{0};
        std::atomic<uint64_t> bytesSent{0};
        std::atomic<uint64_t> bytesReceived{0};

        // Pad to full line (compiler may already do this with alignas)
        char _pad[64 - (6 * sizeof(std::atomic<uint64_t>) % 64)]{};
    };

    // Choose a reasonable default shard count; power-of-two preferred for fast modulo
    static constexpr std::size_t kDefaultShards = 64;

    FsmMetricsRegistry() noexcept
        : enabled_(true) // Default ON; can be toggled by config/env at startup
    {
        // Nothing else to initialize
    }

    // Map current thread to a shard index in [0, kDefaultShards)
    static inline std::size_t shard_index() noexcept {
        // Assign each thread a stable index the first time it's seen.
        static std::atomic<uint32_t> next{0};
        thread_local uint32_t idx = 0xFFFFFFFFu;
        if (idx == 0xFFFFFFFFu) {
            // Incremental assignment; wrap naturally with modulo
            idx = next.fetch_add(1, std::memory_order_relaxed);
        }
        return static_cast<std::size_t>(idx) &
               (kDefaultShards - 1); // kDefaultShards is power-of-two
    }

    inline Shard& shard() noexcept { return shards_[shard_index()]; }
    inline const Shard& shard() const noexcept { return shards_[shard_index()]; }

private:
    std::atomic<bool> enabled_;
    std::array<Shard, kDefaultShards> shards_{};
    std::atomic<uint32_t> ipcPoolSize_{0};
    std::atomic<uint32_t> ioPoolSize_{0};
    std::atomic<uint64_t> writerBudgetBytes_{0};

    // ResourceGovernor metrics
    std::atomic<uint64_t> governorRssBytes_{0};
    std::atomic<uint64_t> governorBudgetBytes_{0};
    std::atomic<uint8_t> governorPressureLevel_{0};
    std::atomic<uint8_t> governorHeadroomPct_{100};

    // ONNX concurrency metrics
    std::atomic<uint32_t> onnxTotalSlots_{0};
    std::atomic<uint32_t> onnxUsedSlots_{0};
    std::atomic<uint32_t> onnxGlinerUsed_{0};
    std::atomic<uint32_t> onnxEmbedUsed_{0};
    std::atomic<uint32_t> onnxRerankerUsed_{0};
};

} // namespace daemon
} // namespace yams
