#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace yams::daemon {

// A lightweight manager for dynamic resource pools (threads, workers, IO contexts).
// This is a stub interface to anchor Phase 6 work without affecting current logic.
// Implementation applies deltas with hysteresis and clamps to configured bounds.
class PoolManager {
public:
    struct Config {
        std::uint32_t min_size{1};
        std::uint32_t max_size{32};
        std::uint32_t cooldown_ms{500};
        std::uint32_t low_watermark{25};  // percent
        std::uint32_t high_watermark{85}; // percent
    };

    struct Delta {
        std::string component; // e.g., "search", "embedding", "ipc"
        int change{0};         // +N or -N
        std::string reason;    // e.g., "fsm:streaming_high_load"
        std::uint32_t cooldown_ms{0};
    };

    struct Stats {
        std::uint32_t current_size{0};
        std::uint64_t resize_events{0};
        std::uint64_t rejected_on_cap{0};
        std::uint64_t throttled_on_cooldown{0};
    };

    static PoolManager& instance();

    void configure(const std::string& component, const Config& cfg);
    // Apply a single delta; returns the resulting size for the component.
    std::uint32_t apply_delta(const Delta& d);
    // Snapshot stats for a component.
    Stats stats(const std::string& component) const;

    /// Shrink all pools to their minimum size (called under memory pressure)
    /// @return Number of pools that were shrunk
    std::size_t shrinkAll();

private:
    PoolManager() = default;
    struct Entry {
        Config cfg{};
        std::uint32_t size{0};
        std::uint64_t last_resize_ns{0};
        Stats s{};
    };

    // Find or create an entry for component.
    Entry& entry_for(const std::string& component);
    const Entry* entry_for_const(const std::string& component) const;

private:
    mutable std::mutex mutex_;
    std::vector<std::pair<std::string, Entry>> pools_;
};

} // namespace yams::daemon
