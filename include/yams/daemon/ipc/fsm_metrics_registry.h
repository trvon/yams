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

        // Merge another snapshot into this one
        inline void merge(const Snapshot& other) noexcept {
            transitions += other.transitions;
            headerReads += other.headerReads;
            payloadReads += other.payloadReads;
            payloadWrites += other.payloadWrites;
            bytesSent += other.bytesSent;
            bytesReceived += other.bytesReceived;
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
};

} // namespace daemon
} // namespace yams