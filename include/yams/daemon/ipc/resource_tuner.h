#pragma once

#include <cstdint>
#include <string>

namespace yams::daemon {

// Lightweight adapter that translates FSM signals into PoolManager deltas
// and updates TuneAdvisor hints. Lives in IPC library to avoid cross-target deps.
class ResourceTuner {
public:
    static ResourceTuner& instance();

    // Called when backpressure flips ON/OFF for a component (e.g., "ipc")
    void onBackpressureFlip(const std::string& component, bool on);

    // Called when an operation timeout occurs; may suggest scaling up
    void onTimeout(const std::string& component);

    // Periodic load hint: use centralized metrics to gently scale pools up/down.
    // workerThreads informs queue threshold heuristics.
    void updateLoadHints(double cpuPercent, std::uint64_t muxQueuedBytes,
                         std::uint64_t workerQueued, std::uint64_t workerThreads,
                         std::uint64_t activeConnections);

    // Optional: adjust internal sensitivity (future)
    void setCooldownMs(std::uint32_t ms) { cooldown_ms_ = ms; }
    void setScaleStep(int step) { scale_step_ = step; }

private:
    ResourceTuner() = default;
    std::uint32_t cooldown_ms_{500};
    int scale_step_{1};
};

} // namespace yams::daemon
